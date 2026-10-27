//! Expired waits on another process as lmao spans (05_gateway.md, "Deadlines", Spans;
//! 13_telemetry.md, "Wait spans").
//!
//! A wait that ends `TIMEOUT`, `SILENT` or `PEER_LOST` reaches its caller as a value: the
//! [`WaitFailure`] the caller's [`crate::CowshedError`] carries
//! ([`crate::CowshedError::wait_source`]), in this process only. The boundary that owns the wait
//! records it. A boundary with a span of its own ends that span `span-err`; one with none, the
//! CLI, seals the failure as its own one-span segment `wait-<uuid>.arrow` under
//! `<telemetry root>/<yyyy-mm-dd>/` through [`WaitSpans::record`], in the trace it adopted from
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
use crate::storage::trace_columns::{Enum, Optional, Required, U32, U64, Utf8};
use crate::storage::trace_columns::{trace_columns, trace_enum};
use crate::storage::trace_segment::{
    EntryType, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError,
    seal_segment, trace_batch,
};

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
    /// The transport's role, not parsed back out of the peer's display string.
    pub peer_role: &'static str,
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
    /// One local recording attempt, retained when the command boundary sees this error again.
    pub receipt: Option<WaitReceipt>,
}

/// The one recording attempt for a wait; neither a failed sample nor a failed seal is retried.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitReceipt {
    pub span: std::result::Result<PathBuf, String>,
    pub sample: Option<std::result::Result<PathBuf, String>>,
}
impl WaitFailure {
    fn interval(&self, ended: SystemTime) -> Result<(Duration, Duration), WaitSpanError> {
        let ended = ended
            .duration_since(UNIX_EPOCH)
            .map_err(|_| WaitSpanError::BeforeEpoch)?;
        let began = ended
            .checked_sub(self.elapsed)
            .ok_or(WaitSpanError::BeforeEpoch)?;
        Ok((began, ended))
    }
    /// A silent peer the recording samples: a workspace supervisor or a cowshed controller, each
    /// with a kernel- or hello-reported pid.
    pub fn stack_pid(&self) -> Option<i32> {
        match (self.code, self.peer_role, self.peer_pid) {
            (WaitCode::Silent, "workspace supervisor" | "cowshed controller", Some(pid))
                if pid > 0 =>
            {
                Some(pid)
            }
            _ => None,
        }
    }
    /// `failure` as an expired wait; `None` for a peer that is absent or a call that failed,
    /// neither of which is an expiry.
    pub fn of(failure: &WaitError) -> Option<Self> {
        match failure {
            WaitError::Expired(expired) => Some(Self::expired(expired)),
            WaitError::Silent(silent) => Some(Self::silent(
                silent,
                peer_pid(&silent.holder),
                silent.holder.role,
            )),
            WaitError::PeerLost {
                op,
                peer,
                elapsed,
                last,
            } => Some(Self {
                code: WaitCode::PeerLost,
                op: *op,
                peer: peer.to_string(),
                peer_role: peer.role,
                peer_pid: peer_pid(peer),
                class: None,
                bound: None,
                elapsed: *elapsed,
                last: *last,
                message: failure.to_string(),
                receipt: None,
            }),
            WaitError::Absent { .. } | WaitError::Io { .. } => None,
        }
    }

    fn expired(expired: &Expired<Peer>) -> Self {
        Self {
            code: WaitCode::Timeout,
            op: expired.op,
            peer: expired.peer.to_string(),
            peer_role: expired.peer.role,
            peer_pid: peer_pid(&expired.peer),
            class: Some(expired.class),
            bound: Some(expired.bound),
            elapsed: expired.elapsed,
            last: expired.last,
            message: expired.to_string(),
            receipt: None,
        }
    }

    /// A long wait whose holder showed no progress for its silence bound. `holder_pid` is the pid
    /// the kernel reported for the process the holder is reached through, if it reported one.
    pub fn silent<P: fmt::Display>(
        silent: &Silent<P>,
        holder_pid: Option<i32>,
        peer_role: &'static str,
    ) -> Self {
        Self {
            code: WaitCode::Silent,
            op: silent.op,
            peer: silent.holder.to_string(),
            peer_role,
            peer_pid: holder_pid,
            class: None,
            bound: Some(silent.idle),
            elapsed: silent.elapsed,
            last: silent.last,
            message: silent.to_string(),
            receipt: None,
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
    wait_stack_sample: Optional<Utf8>,
    wait_stack_error: Optional<Utf8>,
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

/// The OS sampler a recording takes a silent peer's stacks with: the peer's pid in, the report's
/// bytes or why there is none out.
pub type StackCapture = fn(i32) -> std::result::Result<Vec<u8>, String>;

/// Where a client seals the waits its own calls expire in, and how it samples the stacks of the
/// peer that fell silent: one recording attempt per failure (`WaitSpans::record`).
#[derive(Clone, Debug)]
pub struct WaitRecording {
    spans: WaitSpans,
    capture: StackCapture,
}

impl WaitRecording {
    pub fn new(spans: WaitSpans, capture: StackCapture) -> Self {
        Self { spans, capture }
    }

    /// The host's: its telemetry root, the trace the caller exported in `TRACEPARENT`, and the
    /// OS's unprivileged sampler.
    pub fn host() -> Self {
        Self::new(
            WaitSpans::adopting(
                crate::storage::bootstrap::CanonicalRoots::global()
                    .telemetry()
                    .to_path_buf(),
                std::env::var("TRACEPARENT").ok().as_deref(),
            ),
            super::stack_sample::capture,
        )
    }

    /// The telemetry root the spans are sealed under.
    pub fn root(&self) -> &Path {
        self.spans.root()
    }

    /// Record `error`'s expired wait once, sampling a silent peer first: the error comes back
    /// naming the sample and carrying the receipt, so a later boundary that sees it records
    /// nothing again. Capture and seal failures never replace the original failure or its exit
    /// code. An error with no expired wait comes back as it was.
    pub fn record(&self, mut error: crate::CowshedError) -> crate::CowshedError {
        let Some(failure) = error.wait_source() else {
            return error;
        };
        if failure.receipt.is_some() {
            return error;
        }
        let mut failure = failure.clone();
        let original_message_len = failure.message.len();
        self.spans
            .record(&mut failure, SystemTime::now(), self.capture);
        if let Some(receipt) = &failure.receipt
            && let Err(reason) = &receipt.span
        {
            cowshed_os::eprintln!(
                "cowshed: the expired wait's span could not be sealed under {}: {reason}",
                self.spans.root().display()
            );
        }
        error
            .message
            .push_str(&failure.message[original_message_len..]);
        error.with_wait(Some(failure))
    }
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

    /// Record once, sampling a silent supervisor before sealing its receipt. The caller injects
    /// the expiry clock and the OS sampler; sampling cannot move the wait's timestamps or bounds.
    pub fn record(
        &self,
        failure: &mut WaitFailure,
        ended: SystemTime,
        capture: impl FnOnce(i32) -> std::result::Result<Vec<u8>, String>,
    ) {
        if failure.receipt.is_some() {
            return;
        }
        let (began, ended) = match failure.interval(ended) {
            Ok(times) => times,
            Err(error) => {
                failure.receipt = Some(WaitReceipt {
                    span: Err(error.to_string()),
                    sample: None,
                });
                return;
            }
        };
        let segment = Uuid::new_v4();
        let stem = format!("wait-{}", segment.simple());
        let date = TelemetryDate::from_unix_seconds(began.as_secs());
        let sample = failure.stack_pid().map(|pid| {
            capture(pid).and_then(|bytes| {
                crate::storage::trace_segment::seal_bytes(
                    &self.root,
                    date,
                    &format!("{stem}.sample.txt"),
                    &bytes,
                )
                .map_err(|error| error.to_string())
            })
        });
        match &sample {
            Some(Ok(path)) => failure.message.push_str(&format!(
                "; {} stacks: {}",
                failure.peer_role,
                path.display()
            )),
            Some(Err(error)) => failure.message.push_str(&format!(
                "; {} stack capture failed: {error}",
                failure.peer_role
            )),
            None => {}
        }
        let span = self
            .batch(
                failure,
                began,
                ended,
                segment.as_u64_pair().0,
                sample.as_ref(),
            )
            .and_then(|batch| {
                seal_segment(&self.root, date, &stem, &batch).map_err(WaitSpanError::from)
            })
            .map_err(|error| error.to_string());
        failure.receipt = Some(WaitReceipt { span, sample });
    }
    fn batch(
        &self,
        failure: &WaitFailure,
        began: Duration,
        ended: Duration,
        thread_id: u64,
        sample: Option<&std::result::Result<PathBuf, String>>,
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
            wait_stack_sample: None,
            wait_stack_error: None,
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
            wait_stack_sample: sample
                .and_then(|sample| sample.as_ref().ok())
                .map(|path| path.display().to_string()),
            wait_stack_error: sample.and_then(|sample| sample.as_ref().err()).cloned(),
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
    fn stack_targets_and_receipt_times_follow_the_failure_not_diagnostic_work() {
        use arrow_array::{Array as _, StringArray, TimestampNanosecondArray, UInt64Array};
        let waits = WaitSpans::adopting(
            PathBuf::from("/telemetry"),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        let clock = || UNIX_EPOCH + Duration::from_secs(300);
        for (code, role, pid, expected) in [
            (WaitCode::Silent, "workspace supervisor", Some(42), Some(42)),
            (WaitCode::Silent, "workspace supervisor", None, None),
            (WaitCode::Silent, "workspace supervisor", Some(0), None),
            (WaitCode::Silent, "gateway", Some(42), None),
            (WaitCode::Silent, "cowshed controller", Some(42), Some(42)),
            (WaitCode::Timeout, "workspace supervisor", Some(42), None),
            (WaitCode::PeerLost, "workspace supervisor", Some(42), None),
        ] {
            let silent = Silent {
                op: "wait",
                holder: "peer",
                idle: Duration::from_secs(15),
                silent_for: Duration::from_secs(15),
                elapsed: Duration::from_secs(265),
                last: Progress::Received { bytes: 3841 },
            };
            let mut failure = WaitFailure::silent(&silent, pid, role);
            failure.code = code;
            assert_eq!(failure.stack_pid(), expected, "{code:?} {role} {pid:?}");
            let (began, ended) = failure.interval(clock()).unwrap();
            assert_eq!((began.as_secs(), ended.as_secs()), (35, 300));
            for sample in [
                Ok(PathBuf::from("/telemetry/wait.sample.txt")),
                Err("sampler refused".to_owned()),
            ] {
                let batch = waits
                    .batch(&failure, began, ended, 7, Some(&sample))
                    .unwrap();
                let timestamp = batch
                    .column_by_name("timestamp")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                assert_eq!(
                    (timestamp.value(0), timestamp.value(1)),
                    (35_000_000_000, 300_000_000_000)
                );
                let elapsed = batch
                    .column_by_name("wait_elapsed_us")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                assert!(elapsed.is_null(0));
                assert_eq!(elapsed.value(1), 265_000_000);
                for (name, expected) in [
                    (
                        "wait_stack_sample",
                        sample.as_ref().ok().map(|path| path.display().to_string()),
                    ),
                    ("wait_stack_error", sample.as_ref().err().cloned()),
                ] {
                    let column = batch
                        .column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert!(column.is_null(0));
                    assert_eq!(
                        (!column.is_null(1)).then(|| column.value(1).to_owned()),
                        expected
                    );
                }
            }
        }
    }

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
