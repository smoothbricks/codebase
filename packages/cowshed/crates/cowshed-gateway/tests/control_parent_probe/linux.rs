use super::*;

use std::{path::PathBuf, sync::LazyLock};

/// On Linux `GatewayConfig::validate` demands a data-socket root, and it checks
/// it BEFORE the control-socket parent this file exists to falsify. Without one,
/// `Gateway::start` refuses with `Config(MissingDataSocketRoot)` and the probes
/// assert against an error they never reached — green on macOS, red on Linux.
///
/// It is a precondition of the probe, never its subject, so one owned 0o700
/// directory per process is enough: `validate_data_socket_root` requires an
/// absolute, non-symlink, caller-owned directory with no group or other bits.
static DATA_SOCKET_ROOT: LazyLock<PathBuf> = LazyLock::new(|| secure_root("data"));

pub(super) fn socket_root() -> PathBuf {
    DATA_SOCKET_ROOT.clone()
}
