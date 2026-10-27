//! Runs the existing dead-namespace sweep to completion before a real-substrate lane starts.

#[cfg(target_os = "macos")]
#[path = "../../tests/support/scratch_apfs.rs"]
pub mod scratch_apfs;
#[cfg(target_os = "macos")]
#[path = "../../tests/support/scratch_namespace.rs"]
pub mod scratch_namespace;
#[cfg(target_os = "macos")]
#[path = "../../tests/support/scratch_sweep.rs"]
mod scratch_sweep;

#[cfg(target_os = "macos")]
fn main() -> std::io::Result<()> {
    let mut arguments = std::env::args().skip(1);
    let run = arguments
        .next()
        .filter(|run| !run.is_empty())
        .ok_or_else(|| std::io::Error::other("usage: cowshed-test-sweep <run-id>"))?;
    if arguments.next().is_some() {
        return Err(std::io::Error::other("usage: cowshed-test-sweep <run-id>"));
    }
    let root = scratch_apfs::ScratchRoot::new("lane-sweep")?;
    scratch_sweep::sweep_dead_runs(root.namespace(), &run)
}

#[cfg(not(target_os = "macos"))]
fn main() {}
