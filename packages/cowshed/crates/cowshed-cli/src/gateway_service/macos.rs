//! What the gateway's install and daemon do on macOS only: carry the TCP workspace listeners
//! across a restart, and attach RAM disks.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cowshed_os::namespace::HostNamespace;

use super::Downgrade;

pub const RELEASE_CLI_TARGET: &str = if cfg!(target_arch = "aarch64") {
    "cli-arm64-macos"
} else {
    "cli-x64-macos"
};

/// The socket an install serves while it restarts the gateway, which the daemon takes its
/// listeners from before it binds anything.
pub(super) fn listener_handoff(control_socket: &Path) -> Option<PathBuf> {
    Some(cowshed_gateway::handoff_path(control_socket))
}

/// The install's side of the listener handoff (05_gateway.md "An install never refuses a client
/// connection"): serving it from before the restart until the restarted gateway serves carries
/// every workspace listener across the restart. A downgrade serves nothing: an older gateway may
/// not take the listeners, and its own bind would then meet this install's copies. A handoff that
/// cannot serve is said, and the restart refuses connections, as it did before.
pub(super) fn carry_listeners(
    control_socket: &Path,
    downgrade: Downgrade,
) -> Option<cowshed_gateway::HandoffServer> {
    if downgrade == Downgrade::Allow {
        return None;
    }
    match cowshed_gateway::HandoffServer::start(&cowshed_gateway::handoff_path(control_socket)) {
        Ok(server) => Some(server),
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: the handoff socket cannot serve, so this restart carries no workspace \
             listener: {error}"
            );
            None
        }
    }
}

/// The RAM temp volume's disk is attached in the daemon, so no job owns its helper
/// (05_gateway.md, "RAM disks").
pub(super) fn ram_disks(
    namespace: HostNamespace,
) -> Option<Arc<dyn cowshed_gateway_types::RamDisks>> {
    let disks: Arc<dyn cowshed_gateway_types::RamDisks> =
        Arc::new(cowshed_apfs::ram_disk::SystemRamDisks::new(namespace));
    Some(disks)
}
