//! Controller Git on macOS: the developer directory's own `git`, which Seatbelt's
//! `sandbox-exec` starts under the profile [`super::confined`] renders for its checkout.

use std::ffi::OsString;
use std::path::PathBuf;

/// The Git a confined launch starts. The system selector names the active developer tools
/// without consulting `PATH`; `/usr/bin/git` is an `xcrun` shim that writes host caches even for
/// a read.
pub(super) fn git_program() -> PathBuf {
    PathBuf::from(format!("{}/usr/bin/git", crate::sandbox::GIT_DEVELOPER_DIR))
}

/// The `PATH` a launch of [`git_program`] that inherits no environment is handed: the system's
/// own directories ([`crate::host_path`]), never the controller's. The program is named by its
/// path, so this is only where Git finds what it starts itself.
pub(super) fn git_search_path() -> Option<OsString> {
    Some(OsString::from(crate::host_path::service_path()))
}

/// What to do when Git cannot be started ([`super::git_spawn_error`]).
pub(super) const INSTALL_GIT: &str = "install the macOS command line developer tools, then retry";
