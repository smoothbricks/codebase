//! Two kinds of span on the crate's stderr diagnostics path.
//!
//! Lifecycle spans ([`timed`], [`timed_async`]) wrap the slow steps of a lifecycle verb and are
//! unconditional: a span prints `cowshed: <label> start` when its step begins and
//! `cowshed: <label> done elapsed=<duration> status=ok|err` when it ends, so a slow verb reads
//! as timestamps down to the step that spent the time instead of one unexplained wait. The run
//! that is slow in production is the one that has to be readable, and it costs two stderr lines
//! per step. A wrong label only mislabels a log line, never behavior. A controller call that asks
//! for its steps ([`reporting`]) also hears each one start and end, nested under the step it runs
//! inside, so an embedder can trace a call it does not share a stderr with.
//!
//! Latency spans ([`span`]) time the steps of the verbs whose stderr belongs to someone else —
//! `path` answers a script, `exec` relays its child's stderr byte for byte — so they print only
//! when `COWSHED_TIMING` is set, one line per finished step:
//! `cowshed: timing +<since start> <scope> <step> <elapsed>`. The offset is measured from the
//! process's first span, which `main` takes before parsing arguments, so the lines of one
//! command read as a waterfall and the gap before a step is time no span covered.

use std::borrow::Cow;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use tokio::sync::mpsc;

use crate::api::dto::StepReport;

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

/// Run one synchronous step inside a span labelled `<scope> <step>`, reported to the call's
/// [`StepSink`] when the call asked for its steps.
pub fn timed<T, E: fmt::Display>(
    scope: &'static str,
    step: fmt::Arguments<'_>,
    work: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    eprintln!("cowshed: {scope} {step} start");
    let started = Instant::now();
    let result = match OpenStep::start(scope, || step.to_string()) {
        Some(open) => {
            let result = STEPS.sync_scope(open.inside(), work);
            open.end(&result);
            result
        }
        None => work(),
    };
    done(format_args!("{scope} {step}"), started, result.is_ok());
    result
}

/// Run one asynchronous step inside a span labelled `<scope> <step>`, reported to the call's
/// [`StepSink`] when the call asked for its steps.
///
/// The step is owned rather than formatted arguments because formatted arguments borrow what
/// they format and cannot be held across the await; a static step costs no allocation.
pub async fn timed_async<T, E: fmt::Display>(
    scope: &'static str,
    step: impl Into<Cow<'static, str>>,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let step = step.into();
    eprintln!("cowshed: {scope} {step} start");
    let started = Instant::now();
    let result = match OpenStep::start(scope, || (*step).to_owned()) {
        Some(open) => {
            let result = STEPS.scope(open.inside(), work).await;
            open.end(&result);
            result
        }
        None => work.await,
    };
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

/// Attach one fact to a lifecycle step: printed unconditionally as
/// `cowshed: <scope> <step> <key>=<value>`, next to the step's own `start`/`done` lines, so a slow
/// or refused step says why in the same trace that says how long it took.
pub fn attribute(
    scope: &'static str,
    step: fmt::Arguments<'_>,
    key: &'static str,
    value: impl fmt::Display,
) {
    eprintln!("cowshed: {scope} {step} {key}={value}");
}

/// Where one call's lifecycle steps are reported while it runs. A caller that asked for its
/// steps hears each [`timed`] and [`timed_async`] step start and end as it happens, so it can say
/// which step a slow or hung call is in while the call is still in it.
#[derive(Clone, Debug)]
pub struct StepSink {
    reports: mpsc::UnboundedSender<StepReport>,
    next: Arc<AtomicU32>,
}

impl StepSink {
    pub fn new(reports: mpsc::UnboundedSender<StepReport>) -> Self {
        Self {
            reports,
            next: Arc::new(AtomicU32::new(0)),
        }
    }

    /// A caller that stopped listening loses only the reports, never the step.
    fn report(&self, report: StepReport) {
        let _ = self.reports.send(report);
    }
}

/// The call a step reports to and the step it runs inside.
#[derive(Clone)]
struct StepScope {
    sink: StepSink,
    parent: Option<u32>,
}

tokio::task_local! {
    /// Set by [`reporting`] for a call that asked for its steps, and narrowed by every step to
    /// itself while it runs. Task-local, so concurrent calls on one runtime never hear each
    /// other's steps; [`carried`] takes it across to a blocking thread.
    static STEPS: StepScope;
}

/// Run `work`, reporting every lifecycle step it runs to `sink`.
pub async fn reporting<F: Future>(sink: StepSink, work: F) -> F::Output {
    STEPS.scope(StepScope { sink, parent: None }, work).await
}

/// `job`, to run on another thread inside the step it is handed off from: the blocking lanes
/// wrap every job in this, so a storage step on a blocking thread reports to its call.
pub fn carried<T>(job: impl FnOnce() -> T) -> impl FnOnce() -> T {
    let scope = STEPS.try_with(StepScope::clone).ok();
    move || match scope {
        Some(scope) => STEPS.sync_scope(scope, job),
        None => job(),
    }
}

/// A step reported started and not yet ended.
struct OpenStep {
    sink: StepSink,
    step: u32,
}

impl OpenStep {
    /// Report the step started when its call asked for steps; `name` runs only then.
    fn start(scope: &'static str, name: impl FnOnce() -> String) -> Option<Self> {
        let outer = STEPS.try_with(StepScope::clone).ok()?;
        let step = outer.sink.next.fetch_add(1, Ordering::Relaxed);
        outer.sink.report(StepReport::Started {
            step,
            parent: outer.parent,
            scope: scope.to_owned(),
            name: name(),
        });
        Some(Self {
            sink: outer.sink,
            step,
        })
    }

    /// The scope the step's own work runs in: its steps hang from this one.
    fn inside(&self) -> StepScope {
        StepScope {
            sink: self.sink.clone(),
            parent: Some(self.step),
        }
    }

    fn end<T, E: fmt::Display>(self, result: &Result<T, E>) {
        self.sink.report(StepReport::Ended {
            step: self.step,
            error: result.as_ref().err().map(ToString::to_string),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::dto::StepReport;

    fn started(step: u32, parent: Option<u32>, scope: &str, name: &str) -> StepReport {
        StepReport::Started {
            step,
            parent,
            scope: scope.to_owned(),
            name: name.to_owned(),
        }
    }

    fn ended(step: u32, error: Option<&str>) -> StepReport {
        StepReport::Ended {
            step,
            error: error.map(str::to_owned),
        }
    }

    fn reports(receiver: &mut mpsc::UnboundedReceiver<StepReport>) -> Vec<StepReport> {
        std::iter::from_fn(|| receiver.try_recv().ok()).collect()
    }

    /// A step reports under the step it runs inside, on whichever thread it runs: an async step,
    /// a synchronous one inside it, and one dispatched to a blocking thread from inside it all
    /// hang from the async step, and a failure names its cause on the step that failed and on
    /// every step it failed.
    #[tokio::test]
    async fn a_reported_call_hears_each_step_under_the_step_that_ran_it() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let result = reporting(
            StepSink::new(sender),
            timed_async("new", "init", async {
                timed("apfs", format_args!("canonical/{}", "mount"), || {
                    Ok::<_, String>(())
                })?;
                tokio::task::spawn_blocking(carried(|| {
                    timed("apfs", format_args!("canonical/creds"), || {
                        Err::<(), _>("no signing key".to_owned())
                    })
                }))
                .await
                .expect("blocking step")
            }),
        )
        .await;

        assert_eq!(result, Err("no signing key".to_owned()));
        assert_eq!(
            reports(&mut receiver),
            [
                started(0, None, "new", "init"),
                started(1, Some(0), "apfs", "canonical/mount"),
                ended(1, None),
                started(2, Some(0), "apfs", "canonical/creds"),
                ended(2, Some("no signing key")),
                ended(0, Some("no signing key")),
            ]
        );
    }

    /// Two calls reported at once each hear only their own steps, even interleaved on one
    /// runtime, and a step outside any reported call is heard by neither.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_calls_hear_only_their_own_steps() {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let call = |name: &'static str| {
            let barrier = std::sync::Arc::clone(&barrier);
            let (sender, receiver) = mpsc::unbounded_channel();
            let task = tokio::spawn(reporting(
                StepSink::new(sender),
                timed_async("new", name, async move {
                    barrier.wait().await;
                    timed("apfs", format_args!("{name}/inner"), || Ok::<_, String>(()))
                }),
            ));
            (task, receiver)
        };
        let (first, mut first_reports) = call("first");
        let (second, mut second_reports) = call("second");
        timed("apfs", format_args!("unreported"), || Ok::<_, String>(())).expect("step");
        first.await.expect("first").expect("first step");
        second.await.expect("second").expect("second step");

        for (reports, name) in [
            (&mut first_reports, "first"),
            (&mut second_reports, "second"),
        ] {
            assert_eq!(
                super::tests::reports(reports),
                [
                    started(0, None, "new", name),
                    started(1, Some(0), "apfs", &format!("{name}/inner")),
                    ended(1, None),
                    ended(0, None),
                ]
            );
        }
    }
}
