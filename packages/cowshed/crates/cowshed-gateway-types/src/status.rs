//! What the daemon reports about itself, as plain data.
//!
//! A controller reconciles its own inventory against this snapshot, so the shapes live below both
//! the daemon that produces them and the controller that consumes them.

use std::num::NonZeroUsize;

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
    /// Present while the daemon's startup pass still mounts the recorded projects and restores
    /// their sessions. The control socket answers throughout; every request that depends on those
    /// mounts or sessions is refused until this is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healing: Option<StartupHeal>,
    /// Present while the daemon is still recovering the workspace supervisors that served before
    /// it started. It serves throughout; only a command for one of those workspaces is refused
    /// until that workspace's supervisor is recovered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovering: Option<SupervisorRecovery>,
    pub sessions: Vec<SessionStatus>,
    pub active: usize,
    pub queued: usize,
}

/// Where the daemon's startup pass is (05_gateway.md "Startup contract"): mounting the recorded
/// projects, then restoring the attached workspaces' sessions from what it mounted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum StartupHeal {
    /// `projects` recorded projects are not mounted yet; never zero.
    Mounting { projects: NonZeroUsize },
    /// Every project is mounted, or reported as unmountable; the sessions are being restored.
    RestoringSessions,
}

/// The pass in the user's words — mounting their projects, restoring their sessions — which is
/// how every message that reports it reads.
impl std::fmt::Display for StartupHeal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mounting { projects } if projects.get() == 1 => {
                formatter.write_str("mounting 1 adopted project")
            }
            Self::Mounting { projects } => {
                write!(formatter, "mounting {projects} adopted projects")
            }
            Self::RestoringSessions => formatter.write_str("restoring workspace sessions"),
        }
    }
}

/// The workspace supervisors that served before the daemon started and that it has not finished
/// recovering: not yet asked who they are, or, of another cowshed build, not yet answered `drain`.
/// Never zero — a daemon with nothing left to recover reports no recovery at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SupervisorRecovery {
    pub supervisors: NonZeroUsize,
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
