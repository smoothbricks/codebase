//! Two kinds of span on the crate's stderr diagnostics path.
//!
//! Lifecycle spans ([`timed`], [`timed_async`]) wrap the slow steps of a lifecycle verb and are
//! unconditional: a span prints `cowshed: <label> start` when its step begins and
//! `cowshed: <label> done elapsed=<duration> status=ok|err` when it ends, so a slow verb reads
//! as timestamps down to the step that spent the time instead of one unexplained wait. The run
//! that is slow in production is the one that has to be readable, and it costs two stderr lines
//! per step. A wrong label only mislabels a log line, never behavior.
//!
//! Latency spans ([`span`]) time the steps of the verbs whose stderr belongs to someone else —
//! `path` answers a script, `exec` relays its child's stderr byte for byte — so they print only
//! when `COWSHED_TIMING` is set, one line per finished step:
//! `cowshed: timing +<since start> <scope> <step> <elapsed>`. The offset is measured from the
//! process's first span, which `main` takes before parsing arguments, so the lines of one
//! command read as a waterfall and the gap before a step is time no span covered.

use std::borrow::Cow;
use std::fmt;
use std::sync::LazyLock;
use std::time::Instant;

/// The environment variable that turns latency spans on.
pub const TIMING_ENV: &str = "COWSHED_TIMING";

/// When latency spans are on, the instant the clock started.
static ORIGIN: LazyLock<Option<Instant>> = LazyLock::new(|| {
    std::env::var_os(TIMING_ENV)
        .is_some_and(|value| !value.is_empty())
        .then(Instant::now)
});

/// Anchor the latency-span clock at process start. `main` calls this first, so a later span's
/// offset includes argument parsing and runtime start.
pub fn start_clock() {
    LazyLock::force(&ORIGIN);
}

/// One latency step; its line prints when it is dropped. Inert unless `COWSHED_TIMING` is set.
#[must_use = "a span measures until it is dropped"]
pub struct Span {
    scope: &'static str,
    step: Cow<'static, str>,
    started: Option<Instant>,
}

/// Start timing `step` of `scope`.
pub fn span(scope: &'static str, step: &'static str) -> Span {
    Span {
        scope,
        step: Cow::Borrowed(step),
        started: ORIGIN.map(|_| Instant::now()),
    }
}

/// Start timing a step whose name is only known at run time; `step` runs only when spans are on.
pub fn span_named(scope: &'static str, step: impl FnOnce() -> String) -> Span {
    let started = ORIGIN.map(|_| Instant::now());
    Span {
        scope,
        step: match started {
            Some(_) => Cow::Owned(step()),
            None => Cow::Borrowed(""),
        },
        started,
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        let (Some(started), Some(origin)) = (self.started, *ORIGIN) else {
            return;
        };
        eprintln!(
            "cowshed: timing +{:?} {} {} {:?}",
            started.duration_since(origin),
            self.scope,
            self.step,
            started.elapsed()
        );
    }
}

/// Say what happened at this point of the latency waterfall — a decision, not a step — when
/// spans are on. `what` runs only then.
pub fn event(scope: &'static str, what: impl FnOnce() -> String) {
    if let Some(origin) = *ORIGIN {
        eprintln!("cowshed: timing +{:?} {scope} {}", origin.elapsed(), what());
    }
}

/// Run `work` inside a latency span.
pub async fn spanned<T>(
    scope: &'static str,
    step: &'static str,
    work: impl Future<Output = T>,
) -> T {
    let _span = span(scope, step);
    work.await
}

/// Run one synchronous step inside a span.
pub(crate) fn timed<T, E>(
    label: fmt::Arguments<'_>,
    step: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    eprintln!("cowshed: {label} start");
    let started = Instant::now();
    let result = step();
    done(label, started, result.is_ok());
    result
}

/// Run one asynchronous step inside a span labelled `<scope> <step>`.
///
/// The label is two static parts rather than formatted arguments because formatted arguments
/// borrow what they format and cannot be held across the await.
pub(crate) async fn timed_async<T, E>(
    scope: &'static str,
    step: &'static str,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    eprintln!("cowshed: {scope} {step} start");
    let started = Instant::now();
    let result = work.await;
    done(format_args!("{scope} {step}"), started, result.is_ok());
    result
}

fn done(label: fmt::Arguments<'_>, started: Instant, ok: bool) {
    eprintln!(
        "cowshed: {label} done elapsed={:?} status={}",
        started.elapsed(),
        if ok { "ok" } else { "err" }
    );
}
