//! Start/done spans around the slow steps of a lifecycle verb, on the crate's stderr
//! diagnostics path.
//!
//! A span prints `cowshed: <label> start` when its step begins and
//! `cowshed: <label> done elapsed=<duration> status=ok|err` when it ends, so a slow verb reads
//! as timestamps down to the step that spent the time instead of one unexplained wait.
//! Unconditional by design: the run that is slow in production is the one that has to be
//! readable, and it costs two stderr lines per step. A wrong label only mislabels a log line,
//! never behavior.

use std::fmt;
use std::future::Future;
use std::time::Instant;

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
