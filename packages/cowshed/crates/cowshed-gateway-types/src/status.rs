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
