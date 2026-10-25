//! Deadlines for waits on another process, per smoothbricks `specs/cowshed/05_gateway.md`
//! ("Deadlines") and `specs/lmao/01r_transient_errors_and_retry.md`.
//!
//! A wait on a peer is one of two shapes, and the shape decides its bound:
//!
//! - A **request** (a control RPC, a handshake, reaping a child that already closed its pipes, a
//!   system-service call) answers in a time its [`LatencyClass`] fixes. Its [`Deadline`] is one
//!   monotonic budget from the request to the answer; past it the wait ends with [`Expired`]
//!   (code `TIMEOUT`, as an Op's engine-written timeout and lmao's `Code('TIMEOUT')`).
//! - A **long wait** (a job's terminal, a held lease, a token or gate queue, a stream) lasts as
//!   long as its work. It has no total ceiling; its holder must show progress. [`Idle`] measures
//!   the silence since the last progress; past its bound the wait ends with [`Silent`] (code
//!   `SILENT`, a workflow engine's `idle` fact), naming the holder.
//!
//! Every expiry is terminal: [`RetryClass::Terminal`]. Nothing retries around it and nothing
//! raises the bound to pass; the error is the diagnosis. These are pure values over the injected
//! [`Clock`]; the shell that maps sockets, child exits and timers onto them lives with each
//! repository's host primitives.

use std::fmt;
use std::time::Duration;

use crate::clock::Clock;
use crate::result::{BlockedReason, RetryPolicy};

/// How long a request across a process boundary takes when its peer is well. Each class has one
/// bound, recorded with its measurement in the owning spec's table; no call site picks a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyClass {
    /// One request and its one answer from a running daemon over a local socket.
    Control,
    /// Connecting to a peer that should already be running, and its hello.
    Handshake,
    /// A child's exit after its pipes closed or after it was killed.
    Reap,
    /// One call into a system service.
    Ffi,
}

impl LatencyClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Handshake => "handshake",
            Self::Reap => "reap",
            Self::Ffi => "ffi",
        }
    }
}

impl fmt::Display for LatencyClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A class and the bound its spec table records for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound {
    pub class: LatencyClass,
    pub within: Duration,
}

impl Bound {
    pub const fn new(class: LatencyClass, within: Duration) -> Self {
        Self { class, within }
    }
}

/// The last thing a waiter saw its peer do. An expiry names it, so the reader knows whether the
/// peer never answered the connect, took the request and went silent, or stopped mid-answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    Nothing,
    Connected,
    Sent {
        bytes: u64,
    },
    Received {
        bytes: u64,
    },
    Heartbeat {
        seq: u64,
    },
    Signalled {
        signal: i32,
    },
    /// A state the holder reported, named by a static label (`queued`, `running`, `draining`).
    State(&'static str),
}

impl Progress {
    /// `Sent`/`Received` with `bytes` more: the counters a stream's reads and writes advance.
    pub const fn sent(self, bytes: u64) -> Self {
        match self {
            Self::Sent { bytes: before } => Self::Sent {
                bytes: before.saturating_add(bytes),
            },
            _ => Self::Sent { bytes },
        }
    }

    pub const fn received(self, bytes: u64) -> Self {
        match self {
            Self::Received { bytes: before } => Self::Received {
                bytes: before.saturating_add(bytes),
            },
            _ => Self::Received { bytes },
        }
    }
}

impl fmt::Display for Progress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nothing => formatter.write_str("nothing"),
            Self::Connected => formatter.write_str("connected"),
            Self::Sent { bytes } => write!(formatter, "sent {bytes} bytes"),
            Self::Received { bytes } => write!(formatter, "received {bytes} bytes"),
            Self::Heartbeat { seq } => write!(formatter, "heartbeat {seq}"),
            Self::Signalled { signal } => write!(formatter, "signalled {signal}"),
            Self::State(state) => formatter.write_str(state),
        }
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// One request's budget: started once, before the connect, and never restarted by a retry or a
/// fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline {
    op: &'static str,
    bound: Bound,
    started_ns: u64,
}

impl Deadline {
    pub fn start(op: &'static str, bound: Bound, clock: &dyn Clock) -> Self {
        Self {
            op,
            bound,
            started_ns: clock.monotonic_nanos(),
        }
    }

    pub const fn op(&self) -> &'static str {
        self.op
    }

    pub const fn bound(&self) -> Bound {
        self.bound
    }

    pub fn elapsed(&self, clock: &dyn Clock) -> Duration {
        Duration::from_nanos(clock.monotonic_nanos().saturating_sub(self.started_ns))
    }

    /// What is left of the budget; `None` once it is spent.
    pub fn remaining(&self, clock: &dyn Clock) -> Option<Duration> {
        let left = nanos(self.bound.within)
            .saturating_sub(clock.monotonic_nanos().saturating_sub(self.started_ns));
        (left != 0).then(|| Duration::from_nanos(left))
    }

    pub fn expired<P>(&self, peer: P, last: Progress, clock: &dyn Clock) -> Expired<P> {
        Expired {
            op: self.op,
            peer,
            class: self.bound.class,
            bound: self.bound.within,
            elapsed: self.elapsed(clock),
            last,
        }
    }
}

/// A request that got no answer within its bound: code `TIMEOUT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expired<P> {
    pub op: &'static str,
    pub peer: P,
    pub class: LatencyClass,
    pub bound: Duration,
    pub elapsed: Duration,
    pub last: Progress,
}

impl<P: fmt::Display> fmt::Display for Expired<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "TIMEOUT: {} on {}: no answer within its {} bound of {:?}; waited {:?}, last progress: {}",
            self.op, self.peer, self.class, self.bound, self.elapsed, self.last
        )
    }
}

impl<P: fmt::Debug + fmt::Display> std::error::Error for Expired<P> {}

/// The silence of a long wait's holder, measured from its last progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Idle {
    op: &'static str,
    idle: Duration,
    started_ns: u64,
    last_ns: u64,
    last: Progress,
}

impl Idle {
    pub fn start(op: &'static str, idle: Duration, clock: &dyn Clock) -> Self {
        let now = clock.monotonic_nanos();
        Self {
            op,
            idle,
            started_ns: now,
            last_ns: now,
            last: Progress::Nothing,
        }
    }

    pub const fn op(&self) -> &'static str {
        self.op
    }

    pub const fn last(&self) -> Progress {
        self.last
    }

    /// The whole wait so far, progress included.
    pub fn elapsed(&self, clock: &dyn Clock) -> Duration {
        Duration::from_nanos(clock.monotonic_nanos().saturating_sub(self.started_ns))
    }

    /// Allow the holder `idle` of silence from here on, keeping the wait's start and its last
    /// progress: a holder that has just shown it heartbeats is held to the heartbeat's bound.
    pub fn set_silence(&mut self, idle: Duration) {
        self.idle = idle;
    }

    /// The holder showed `progress`: the silence starts over.
    pub fn observe(&mut self, progress: Progress, clock: &dyn Clock) {
        self.last_ns = clock.monotonic_nanos();
        self.last = progress;
    }

    /// How much more silence the holder is allowed; `None` once it has been silent too long.
    pub fn remaining(&self, clock: &dyn Clock) -> Option<Duration> {
        let left =
            nanos(self.idle).saturating_sub(clock.monotonic_nanos().saturating_sub(self.last_ns));
        (left != 0).then(|| Duration::from_nanos(left))
    }

    pub fn silent<P>(&self, holder: P, clock: &dyn Clock) -> Silent<P> {
        let now = clock.monotonic_nanos();
        Silent {
            op: self.op,
            holder,
            idle: self.idle,
            silent_for: Duration::from_nanos(now.saturating_sub(self.last_ns)),
            elapsed: Duration::from_nanos(now.saturating_sub(self.started_ns)),
            last: self.last,
        }
    }
}

/// A long wait whose holder showed no progress for its idle bound: code `SILENT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Silent<P> {
    pub op: &'static str,
    pub holder: P,
    pub idle: Duration,
    pub silent_for: Duration,
    pub elapsed: Duration,
    pub last: Progress,
}

impl<P: fmt::Display> fmt::Display for Silent<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "SILENT: {}: {} showed no progress for {:?} (idle bound {:?}); waited {:?} in all, last progress: {}",
            self.op, self.holder, self.silent_for, self.idle, self.elapsed, self.last
        )
    }
}

impl<P: fmt::Debug + fmt::Display> std::error::Error for Silent<P> {}

/// What may run the operation again after a failure (`01r` "Error Type Hierarchy"): the span's
/// own retry loop, the engine that re-executes a blocked operation, or nobody. A wait's error
/// derives its class from its variant, so an expiry cannot be marked retryable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryClass {
    Transient(RetryPolicy),
    Blocked(BlockedReason),
    Terminal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A clock that moves only when the test moves it.
    struct Manual(AtomicU64);

    impl Manual {
        fn at(nanos: u64) -> Self {
            Self(AtomicU64::new(nanos))
        }
        fn advance(&self, by: Duration) {
            self.0.fetch_add(nanos(by), Ordering::Relaxed);
        }
    }

    impl Clock for Manual {
        fn wall_nanos(&self) -> i64 {
            0
        }
        fn monotonic_nanos(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    const CONTROL: Bound = Bound::new(LatencyClass::Control, Duration::from_secs(5));

    #[test]
    fn a_deadline_spends_one_budget_from_its_start() {
        let clock = Manual::at(1_000);
        let deadline = Deadline::start("status", CONTROL, &clock);
        for (advance, remaining) in [
            (Duration::ZERO, Some(Duration::from_secs(5))),
            (Duration::from_secs(2), Some(Duration::from_secs(3))),
            (Duration::from_millis(2_999), Some(Duration::from_millis(1))),
            (Duration::from_millis(1), None),
            (Duration::from_secs(60), None),
        ] {
            clock.advance(advance);
            assert_eq!(deadline.remaining(&clock), remaining);
        }
        assert_eq!(deadline.elapsed(&clock), Duration::from_secs(65));
    }

    #[test]
    fn an_expiry_names_op_peer_class_bound_elapsed_and_last_progress() {
        let clock = Manual::at(0);
        let deadline = Deadline::start("status", CONTROL, &clock);
        clock.advance(Duration::from_secs(5));
        let expired = deadline.expired(
            "gateway /run/control.sock",
            Progress::Sent { bytes: 18 },
            &clock,
        );
        assert_eq!(
            expired,
            Expired {
                op: "status",
                peer: "gateway /run/control.sock",
                class: LatencyClass::Control,
                bound: Duration::from_secs(5),
                elapsed: Duration::from_secs(5),
                last: Progress::Sent { bytes: 18 },
            }
        );
        assert_eq!(
            expired.to_string(),
            "TIMEOUT: status on gateway /run/control.sock: no answer within its control bound of 5s; waited 5s, last progress: sent 18 bytes"
        );
    }

    #[test]
    fn progress_restarts_the_silence_but_never_the_total() {
        let clock = Manual::at(0);
        let mut idle = Idle::start("job terminal", Duration::from_secs(15), &clock);
        for (advance, progress, remaining) in [
            (Duration::from_secs(10), None, Some(Duration::from_secs(5))),
            (
                Duration::ZERO,
                Some(Progress::Heartbeat { seq: 1 }),
                Some(Duration::from_secs(15)),
            ),
            (
                Duration::from_secs(14),
                Some(Progress::Heartbeat { seq: 2 }),
                Some(Duration::from_secs(15)),
            ),
            (Duration::from_secs(15), None, None),
        ] {
            clock.advance(advance);
            if let Some(progress) = progress {
                idle.observe(progress, &clock);
            }
            assert_eq!(idle.remaining(&clock), remaining);
        }
        let silent = idle.silent("supervisor pid 81200", &clock);
        assert_eq!(silent.silent_for, Duration::from_secs(15));
        assert_eq!(silent.elapsed, Duration::from_secs(39));
        assert_eq!(silent.last, Progress::Heartbeat { seq: 2 });
        assert_eq!(
            silent.to_string(),
            "SILENT: job terminal: supervisor pid 81200 showed no progress for 15s (idle bound 15s); waited 39s in all, last progress: heartbeat 2"
        );
    }

    #[test]
    fn stream_counters_accumulate_within_one_direction() {
        let progress = Progress::Connected.sent(10).sent(8);
        assert_eq!(progress, Progress::Sent { bytes: 18 });
        assert_eq!(
            progress.received(4).received(1),
            Progress::Received { bytes: 5 }
        );
    }
}
