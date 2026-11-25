//! Wire protocol for a served supervisor's bidirectional watch on the host gateway's land-gate
//! state (05_gateway.md "A land's gate takes tokens back").
//!
//! Each served supervisor opens one persistent connection to `<store>/gateway.sock` with
//! [`GateWatchOpen`], naming its `.groups` ledger path. The gateway immediately writes the
//! current [`GateWatchState`], writes it again on every active/inactive transition, and repeats
//! it every heartbeat (`deadline::HEARTBEAT`, 5 s). On the same connection, a supervisor that
//! admits a land's gate job (`ExecClass::Gate` with `lander: Some(_)`) writes
//! [`GateWatchEvent::GateStart`] at admission and [`GateWatchEvent::GateEnd`] when the job ends.
//!
//! Dropping the connection drops all active gate registrations held by that supervisor and tells
//! the gateway to clear `PRIO_DARWIN_BG` on every recorded life in that supervisor's `.groups`
//! ledger. On the supervisor side, EOF or silence past `deadline::HEARTBEAT_SILENCE` (15 s)
//! treats gate state as off, restoring withdrawn tokens and clearing `PRIO_DARWIN_BG`.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// The gateway control-plane operation name for the bidirectional supervisor gate watch.
pub const GATE_WATCH_OP: &str = "gate-watch";

/// Opening frame sent by a served supervisor on `<store>/gateway.sock`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GateWatchOpen {
    pub op: String,
    /// Path to this supervisor's `.groups` ledger under `<store>/run/`. The gateway verifies it
    /// lies directly under `<store>/run/` before using it on disconnect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger: Option<PathBuf>,
}

impl GateWatchOpen {
    /// Construct a valid `gate-watch` opening request.
    pub fn new(ledger: Option<PathBuf>) -> Self {
        Self {
            op: GATE_WATCH_OP.to_owned(),
            ledger,
        }
    }
}

/// Frames sent from a supervisor to the gateway on an open `gate-watch` connection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum GateWatchEvent {
    /// A land's gate job was admitted on this supervisor.
    GateStart {
        #[serde(rename = "jobId")]
        job_id: u64,
    },
    /// A previously registered land's gate job ended on this supervisor.
    GateEnd {
        #[serde(rename = "jobId")]
        job_id: u64,
    },
}

/// State and heartbeat frame pushed by the gateway to every watching supervisor on open, on every
/// active/inactive transition, and every `deadline::HEARTBEAT` (5 s).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GateWatchState {
    pub ok: bool,
    pub active: bool,
    pub seq: u64,
}
