//! What the daemon reports about itself, as plain data.
//!
//! A controller reconciles its own inventory against this snapshot, so the shapes live below both
//! the daemon that produces them and the controller that consumes them.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayStatus {
    /// Version of the daemon process that answered the control request.
    pub version: String,
    pub draining: bool,
    /// Why the daemon is draining, while it is. A draining daemon refuses every new session, so a
    /// status that answers is not a healthy one until this is `None`.
    #[serde(default)]
    pub drain_cause: Option<String>,
    /// SHA-256 of the executable the daemon process runs, when its supervisor recorded it. A
    /// version string cannot tell two builds of one release apart; these bytes can.
    #[serde(default)]
    pub executable_sha256: Option<String>,
    pub sessions: Vec<SessionStatus>,
    pub active: usize,
    pub queued: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionStatus {
    pub workspace_id: String,
    pub revision: u64,
    /// Exactly the [`crate::WorkspaceEndpoint`] `Display` rendering, so a controller can compare
    /// the endpoint it intends to install against what the daemon reports without reparsing.
    pub endpoint: String,
    pub active: usize,
    pub queued: usize,
}
