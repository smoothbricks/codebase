//! What the gateway's install and daemon do not do on Linux: there is no TCP workspace listener
//! to carry across a restart and no RAM disk to attach. A held ZFS dataset can be unmounted
//! under the gateway as an APFS volume can, so the lost-mount watch is the parent's, on both.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cowshed_os::namespace::HostNamespace;

use super::Downgrade;

pub const RELEASE_CLI_TARGET: &str = if cfg!(target_arch = "aarch64") {
    "cli-arm64-linux"
} else {
    "cli-x64-linux"
};

/// The daemon takes no listener from an install.
pub(super) const fn listener_handoff(_control_socket: &Path) -> Option<PathBuf> {
    None
}

/// Linux binds no TCP workspace listener, so a restart has none to carry and serves no handoff.
pub(super) const fn carry_listeners(_control_socket: &Path, _downgrade: Downgrade) -> Option<()> {
    None
}

/// The daemon attaches no RAM disk: the RAM temp volume is an APFS one.
pub(super) fn ram_disks(
    _namespace: HostNamespace,
) -> Option<Arc<dyn cowshed_gateway_types::RamDisks>> {
    None
}
