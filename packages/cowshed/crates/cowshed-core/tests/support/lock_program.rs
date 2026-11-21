//! The host's program that takes a lock as Nx and cowshed do (`flock(2)`) and runs a command
//! while it holds it: BSD `lockf(1)` on macOS (`lock_program/macos.rs`), util-linux `flock(1)`
//! on Linux (`lock_program/linux.rs`). A test spawns it to be a foreign holder of a lock.

#[cfg(target_os = "linux")]
#[path = "lock_program/linux.rs"]
mod linux;
#[cfg(target_os = "macos")]
#[path = "lock_program/macos.rs"]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// What the program's command line opens with, as a process census names its process.
pub(crate) use platform::NAME;
/// The program taking `lock`: the caller appends the command it runs while it holds the lock.
/// The command inherits the locked descriptor, so the lock lasts until both have ended.
pub(crate) use platform::holding;
