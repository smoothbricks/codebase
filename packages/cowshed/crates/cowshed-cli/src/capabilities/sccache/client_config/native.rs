#[cfg(not(target_os = "macos"))]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(not(target_os = "macos"))]
pub(super) use linux::config_directories;
#[cfg(target_os = "macos")]
pub(super) use macos::config_directories;
