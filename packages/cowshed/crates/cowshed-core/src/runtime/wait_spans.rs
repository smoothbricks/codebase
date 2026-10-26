//! Expired waits on another process as lmao spans (05_gateway.md, "Deadlines", Spans;
//! 13_telemetry.md, "Wait spans").
//!
//! A wait that ends `TIMEOUT`, `SILENT` or `PEER_LOST` reaches its caller as a value: the
//! [`WaitFailure`] the caller's [`crate::CowshedError`] carries
//! ([`crate::CowshedError::wait_source`]), in this process only. The boundary that owns the wait
//! records it. A boundary with a span of its own ends that span `span-err`; one with none, the
//! CLI, seals the failure as its own one-span segment `wait-<uuid>.arrow` under
//! `<telemetry root>/<yyyy-mm-dd>/` through [`WaitSpans::seal`], in the trace it adopted from
//! `TRACEPARENT`, by the same [`crate::storage::trace_segment`] owner the job and image spans use.
//! A wait that is answered writes nothing: the command that waited owns that time.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use cowshed_os::deadline::{Expired, LatencyClass, Peer, PeerPid, Progress, Silent, WaitError};
use thiserror::Error;
use uuid::Uuid;

use crate::api::dto::{SpanId, TraceContext, TraceId};
use crate::storage::trace_columns::trace_columns;
use crate::storage::trace_columns::{Enum, Optional, Required, U32, U64, Utf8};
use crate::storage::trace_segment::{
    EntryType, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError,
    seal_segment, trace_batch,
};
use cowshed_types::trace_enum;

/// The name a wait span's start row carries; its `span-err` row carries the failure's sentence.
pub const WAIT_SPAN_MESSAGE: &str = "cowshed.wait";
/// A wait span is the only span of its own lmao thread.
const WAIT_SPAN_ID: u32 = 1;

trace_enum! {
    /// The code an expired wait is reported under (05_gateway.md, "Deadlines", Codes).
    pub enum WaitCode {
        /// A request past its latency class's bound.
        Timeout = "TIMEOUT",
        /// A long wait whose holder showed no progress for its silence bound.
        Silent = "SILENT",
        /// The peer ended the connection before it answered.
        PeerLost = "PEER_LOST",
    }
}

/// An expired wait as its owning boundary records it: the facts its sentence states, as values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitFailure {
    pub code: WaitCode,
    pub op: &'static str,
    /// The peer or holder, named as the failure names it.
    pub peer: String,
    /// The pid the kernel reported for the peer; `None` when it reported none.
    pub peer_pid: Option<i32>,
    /// A `TIMEOUT`'s latency class; a silence or a loss has none.
    pub class: Option<LatencyClass>,
    /// A `TIMEOUT`'s request bound or a `SILENT`'s silence bound; a loss has none.
    pub bound: Option<Duration>,
    /// The whole wait, from its start to its end.
    pub elapsed: Duration,
    /// The last progress the waiter saw.
    pub last: Progress,
    /// The failure's own sentence, as the caller's error states it.
    pub message: String,
}

impl WaitFailure {
    /// `failure` as an expired wait; `None` for a peer that is absent or a call that failed,
    /// neither of which is an expiry.
    pub fn of(failure: &WaitError) -> Option<Self> {
        match failure {
            WaitError::Expired(expired) => Some(Self::expired(expired)),
            WaitError::Silent(silent) => Some(Self::silent(silent, peer_pid(&silent.holder))),
            WaitError::PeerLost {
                op,
                peer,
                elapsed,
                last,
            } => Some(Self {
                code: WaitCode::PeerLost,
                op: *op,
                peer: peer.to_string(),
                peer_pid: peer_pid(peer),
                class: None,
                bound: None,
                elapsed: *elapsed,
                last: *last,
                message: failure.to_string(),
            }),
            WaitError::Absent { .. } | WaitError::Io { .. } => None,
        }
    }

    fn expired(expired: &Expired<Peer>) -> Self {
        Self {
            code: WaitCode::Timeout,
            op: expired.op,
            peer: expired.peer.to_string(),
            peer_pid: peer_pid(&expired.peer),
            class: Some(expired.class),
            bound: Some(expired.bound),
            elapsed: expired.elapsed,
            last: expired.last,
            message: expired.to_string(),
        }
    }

    /// A long wait whose holder showed no progress for its silence bound. `holder_pid` is the pid
    /// the kernel reported for the process the holder is reached through, if it reported one.
    pub fn silent<P: fmt::Display>(silent: &Silent<P>, holder_pid: Option<i32>) -> Self {
        Self {
            code: WaitCode::Silent,
            op: silent.op,
            peer: silent.holder.to_string(),
            peer_pid: holder_pid,
            class: None,
            bound: Some(silent.idle),
            elapsed: silent.elapsed,
            last: silent.last,
            message: silent.to_string(),
        }
    }
}

/// The pid the kernel reported for `peer`, if it reported one.
pub fn peer_pid(peer: &Peer) -> Option<i32> {
    match peer.pid {
        PeerPid::Known(pid) => Some(pid),
        PeerPid::NotReported | PeerPid::Unread(_) => None,
    }
}

trace_columns! {
    /// One row of a wait span (13_telemetry.md, "Wait spans"). The start row states the wait as it
    /// began; the `span-err` row also states how it ended, which the start row leaves null.
    pub struct WaitColumns, fields: wait_fields, arrays: wait_column_arrays;
    wait_op: Required<Utf8>,
    wait_peer: Required<Utf8>,
    wait_peer_pid: Optional<U32>,
    wait_class: Optional<Utf8>,
    wait_bound_us: Optional<U64>,
    wait_code: Optional<Enum<WaitCode>>,
    wait_elapsed_us: Optional<U64>,
    wait_last: Optional<Utf8>,
    process_id: Required<U32>,
    w3c_parent_span_id: Optional<U64>,
}

/// Why a wait span was not sealed. The command's outcome never depends on it.
#[derive(Debug, Error)]
pub enum WaitSpanError {
    #[error("the wait's span would start before the Unix epoch")]
    BeforeEpoch,
    #[error("the wait's span would end beyond i64 nanoseconds")]
    BeyondNanoseconds,
    #[error(transparent)]
    Segment(#[from] TraceSegmentError),
}
/// Where a boundary with no span of its own seals the waits that expired under it, and the trace
/// those spans are in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitSpans {
    root: PathBuf,
    trace_id: TraceId,
    /// The adopted context's span, which a wait span hangs from; `None` in a minted trace.
    parent: Option<SpanId>,
}

impl WaitSpans {
    /// Seal under the telemetry `root`, in the trace `traceparent` names: a W3C `traceparent`, as
    /// a traced caller exports it in `TRACEPARENT`. With none, or one that does not parse, each
    /// wait span is the root of a fresh trace, as a W3C participant restarts one.
    pub fn adopting(root: PathBuf, traceparent: Option<&str>) -> Self {
        let (trace_id, parent) = match traceparent.and_then(TraceContext::from_traceparent) {
            Some(adopted) => (adopted.trace_id, Some(adopted.span_id)),
            None => (TraceContext::mint().trace_id, None),
        };
        Self {
            root,
            trace_id,
            parent,
        }
    }

    /// The telemetry root the spans are sealed under.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Seal `failure`, which expired at `ended`, as one span of a fresh lmao thread: its
    /// `span-start` row when the wait began (`ended` less the time it waited) and its `span-err`
    /// row at `ended`. The partition is the UTC date the wait began. The sealed segment's path.
    pub fn seal(&self, failure: &WaitFailure, ended: SystemTime) -> Result<PathBuf, WaitSpanError> {
        let ended = ended
            .duration_since(UNIX_EPOCH)
            .map_err(|_| WaitSpanError::BeforeEpoch)?;
        let began = ended
            .checked_sub(failure.elapsed)
            .ok_or(WaitSpanError::BeforeEpoch)?;
        let segment = Uuid::new_v4();
        let batch = self.batch(failure, began, ended, segment.as_u64_pair().0)?;
        seal_segment(
            &self.root,
            TelemetryDate::from_unix_seconds(began.as_secs()),
            &format!("wait-{}", segment.simple()),
            &batch,
        )
        .map_err(WaitSpanError::from)
    }

    fn batch(
        &self,
        failure: &WaitFailure,
        began: Duration,
        ended: Duration,
        thread_id: u64,
    ) -> Result<RecordBatch, WaitSpanError> {
        let span = SpanAddress {
            thread_id,
            span_id: WAIT_SPAN_ID,
        };
        let mut system = SystemColumns::with_capacity(2);
        for (at, entry_type, message) in [
            (began, EntryType::SpanStart, WAIT_SPAN_MESSAGE),
            (ended, EntryType::SpanErr, failure.message.as_str()),
        ] {
            system.push(SystemRow {
                timestamp_ns: i64::try_from(at.as_nanos())
                    .map_err(|_| WaitSpanError::BeyondNanoseconds)?,
                trace_id: self.trace_id.as_str(),
                span,
                parent: None,
                entry_type,
                message: Some(message),
            })?;
        }
        let start = WaitColumns {
            wait_op: failure.op.to_owned(),
            wait_peer: failure.peer.clone(),
            wait_peer_pid: failure.peer_pid.and_then(|pid| u32::try_from(pid).ok()),
            wait_class: failure.class.map(|class| class.as_str().to_owned()),
            wait_bound_us: failure.bound.map(micros),
            wait_code: None,
            wait_elapsed_us: None,
            wait_last: None,
            process_id: std::process::id(),
            w3c_parent_span_id: self.parent.as_ref().map(|parent| {
                u64::from_str_radix(parent.as_str(), 16)
                    .expect("a SpanId is exactly 16 lowercase hex digits")
            }),
        };
        let end = WaitColumns {
            wait_code: Some(failure.code),
            wait_elapsed_us: Some(micros(failure.elapsed)),
            wait_last: Some(failure.last.to_string()),
            ..start.clone()
        };
        Ok(trace_batch(
            system,
            wait_column_arrays([&start, &end]).map_err(TraceSegmentError::from)?,
        )?)
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).expect("a wait fits in u64 microseconds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_peer_or_a_failed_call_is_no_expiry() {
        let peer = Box::new(Peer::unix("workspace supervisor", Path::new("/s.sock")));
        let absent = WaitError::Absent {
            op: "hello",
            peer: peer.clone(),
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        };
        let failed = WaitError::Io {
            op: "hello",
            peer: peer.clone(),
            elapsed: Duration::from_millis(3),
            last: Progress::Connected,
            source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        };
        assert_eq!(WaitFailure::of(&absent), None);
        assert_eq!(WaitFailure::of(&failed), None);
        let lost = WaitError::PeerLost {
            op: "status",
            peer,
            elapsed: Duration::from_millis(7),
            last: Progress::Sent { bytes: 12 },
        };
        let lost = WaitFailure::of(&lost).expect("a loss is recorded");
        assert_eq!(
            (lost.code, lost.op, lost.class, lost.bound),
            (WaitCode::PeerLost, "status", None, None)
        );
        assert!(
            lost.message
                .starts_with("PEER_LOST: status on workspace supervisor at /s.sock")
        );
    }

    #[test]
    fn only_a_well_formed_traceparent_is_adopted() {
        let root = PathBuf::from("/telemetry");
        let adopted = WaitSpans::adopting(
            root.clone(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        assert_eq!(
            adopted.trace_id.as_str(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(
            adopted.parent.as_ref().map(SpanId::as_str),
            Some("00f067aa0ba902b7")
        );
        for refused in [
            "",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-00",
        ] {
            let minted = WaitSpans::adopting(root.clone(), Some(refused));
            assert_eq!(minted.parent, None, "{refused:?}");
            assert_ne!(minted.trace_id, adopted.trace_id, "{refused:?}");
        }
    }
}
