//! Runs the existing dead-namespace sweep to completion before a real-substrate lane starts, and
//! keeps one scratch root for its owner (`--keep <root>`, scratch_apfs.rs `Keeper`).

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
    let usage = || std::io::Error::other("usage: cowshed-test-sweep <run-id> | --keep <root>");
    let mut arguments = std::env::args_os().skip(1);
    let first = arguments.next().ok_or_else(usage)?;
    if first == scratch_apfs::KEEP_ARGUMENT {
        let root = arguments.next().ok_or_else(usage)?;
        if arguments.next().is_some() {
            return Err(usage());
        }
        return keep(std::path::Path::new(&root));
    }
    let run = first
        .into_string()
        .ok()
        .filter(|run| !run.is_empty())
        .ok_or_else(usage)?;
    if arguments.next().is_some() {
        return Err(usage());
    }
    let root = scratch_apfs::ScratchRoot::new("lane-sweep")?;
    scratch_sweep::sweep_dead_runs(root.namespace(), &run)
}

/// Keep `root` for the owner on the other end of standard input (scratch_apfs.rs `Keeper`): once
/// the root is a test namespace's ([`scratch_namespace::root_namespace`]), say so on standard
/// output, then wait. The owner's release says it tore the root down itself; end of input without
/// one says it died, and this reclaims exactly that root ([`scratch_namespace::reclaim_kept_root`]).
/// The pipe is the proof: its owner was its only writer, and an exited owner still holds its pid
/// until it is reaped, so its pid proves nothing yet.
#[cfg(target_os = "macos")]
fn keep(root: &std::path::Path) -> std::io::Result<()> {
    use std::io::{Read as _, Write as _};

    if scratch_namespace::root_namespace(root).is_none() {
        return Err(std::io::Error::other(format!(
            "{} is no test namespace's scratch root",
            root.display()
        )));
    }
    {
        let mut owner = std::io::stdout().lock();
        owner.write_all(scratch_apfs::KEEPER_KEEPING)?;
        owner.flush()?;
    }
    let mut said = Vec::new();
    // Ends when the owner releases the root or dies: its descriptor is the pipe's only writer.
    std::io::stdin().read_to_end(&mut said)?;
    if said == scratch_apfs::KEEPER_RELEASED {
        return Ok(());
    }
    scratch_namespace::reclaim_kept_root(root).map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!(
                "reclaim scratch root {} after its owner died: {error}",
                root.display()
            ),
        )
    })?;
    cowshed_os::eprintln!(
        "cowshed-test-sweep: reclaimed scratch root {} after its owner died",
        root.display()
    );
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn main() {}
