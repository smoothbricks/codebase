//! `apfs-benchmark`: timings of APFS disk images (`macos.rs`). Cargo builds a package's binaries
//! for every target, so a Linux build has this one too; there it measures nothing and says so
//! (`linux.rs`).

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

fn main() {
    platform::run();
}
