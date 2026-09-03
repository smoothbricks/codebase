//! Platform-owned cleanup for daemon state inherited by a cloned workspace.

#[cfg(target_os = "macos")]
pub(crate) mod macos;
