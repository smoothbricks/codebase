//! The unprivileged stack sampler a wait recording takes a silent workspace supervisor's or
//! cowshed controller's stacks with ([`super::wait_spans::WaitRecording`]): `sample` on macOS
//! (`macos.rs`); Linux has none, and its capture says so (`linux.rs`).

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// The stacks of process `pid` as the host's sampler reports them, or why none were taken.
pub use platform::capture;
