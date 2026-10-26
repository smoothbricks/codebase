//! An expired wait on another process, as exact values (05_gateway.md, "Deadlines").
//!
//! The conversion from the shell's wait errors stays with the wait-span owner that records
//! the failure; only the recorded facts live here.

use std::time::Duration;

use lmao_core::deadline::{LatencyClass, Progress};

use crate::trace_enum;

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
