//! The real-substrate lane's synchronous setup, never a fixture destructor.

use super::scratch_namespace::processes::{end_processes_in, processes_in};
use super::scratch_namespace::{USER_ROOT, dead_owner, detach_images};
use cowshed_apfs::{CommandRunner as _, DiskImageSource, SystemCommandRunner};
use cowshed_os::namespace::{HostNamespace, TestNamespace};
use cowshed_os::test_backstop::lock_exclusive;
use std::fs;
use std::time::Instant;

#[path = "scratch_reclaim.rs"]
mod reclaim;

#[path = "scratch_directory_cleanup.rs"]
mod cleanup;
use cleanup::{ROOT_PREFIX, reclaim};

/// Reclaim dead predecessors once before the lane starts fixtures. The setup process owns the
/// whole pass synchronously; a killed setup records nothing and starts no tests. Another setup
/// takes the existing lock and completes the work. No fixture starts or joins this sweep.
///
/// A dead run's root is the directory of a test namespace: an entry of the per-user root that
/// parses as one and whose owner is gone ([`dead_owner`]): nobody holds its minting pid, or a
/// process that started at another instant than its `.owner` records does. Nothing else in the
/// per-user root is ever a run's, so nothing else is selected, ended in, released or removed:
/// not `t`, not
/// production's `mnt` (below it the workspaces mount, the one a gate runs in among them) or
/// `ram<revision>`, not a name no run made. Each dead run's images are released as its own
/// namespace; the setup's namespace only reads the kernel's attachment table.
///
/// The sweep is driven off the process and attachment tables rather than the directory listing,
/// because the residues outlive each other independently: a root directory can be deleted (by
/// hand, or by a tmp reaper) while its images stay attached, and an attached image keeps working
/// from a deleted backing file, as a process keeps working in a deleted directory. Ending and
/// detaching therefore select on the path the kernel still holds, not on what is on disk now.
/// Processes go first: one working in a dead run's volume would hold that volume attached.
pub(super) fn sweep_dead_runs(namespace: TestNamespace, run: &str) -> std::io::Result<()> {
    let lock = cowshed_os::scratch::macos_scratch_sweep_lock()?;
    // The completion record shares the canonical lock's directory, never a fixture's tree.
    let record = lock.with_file_name("sweep.run");
    let _sweeping = lock_exclusive(&lock).inspect_err(|error| {
        cowshed_os::eprintln!("abandoned scratch roots: lock {}: {error}", lock.display());
    })?;
    let recorded = match fs::read(&record) {
        Ok(recorded) => Some(recorded),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            cowshed_os::eprintln!("read completed scratch sweep {}: {error}", record.display());
            return Err(error);
        }
    };
    let started = Instant::now();
    let completed = reclaim::sweep_once(
        run,
        recorded.as_deref(),
        || {
            let directory_roots = sweep_dead_roots()?;
            end_processes_in(|path| dead_owner(path).is_some()).inspect_err(|error| {
                cowshed_os::eprintln!("abandoned scratch roots: end workers: {error}");
            })?;
            let unreleased = detach_images(namespace, dead_owner).inspect_err(|error| {
                cowshed_os::eprintln!(
                    "abandoned scratch roots remain intact because image release could not be inventoried: {error}"
                );
            })?;
            let mut counts = reclaim::Reclaimed {
                removed: directory_roots,
                retained: 0,
            };
            let entries = fs::read_dir(USER_ROOT.as_path()).inspect_err(|error| {
                cowshed_os::eprintln!(
                    "abandoned scratch roots: list {}: {error}",
                    USER_ROOT.display()
                );
            })?;
            for entry in entries {
                let path = entry
                    .inspect_err(|error| {
                        cowshed_os::eprintln!(
                            "abandoned scratch roots: read entry in {}: {error}",
                            USER_ROOT.display()
                        );
                    })?
                    .path();
                if dead_owner(&path).is_none() {
                    continue;
                }
                let root = reclaim::reclaim_dead_roots(
                    [path],
                    &unreleased,
                    |path| fs::remove_dir_all(path),
                    |path, error| {
                        cowshed_os::eprintln!(
                            "abandoned scratch root {} was not removed: {error}",
                            path.display()
                        );
                    },
                );
                counts.removed += root.removed;
                counts.retained += root.retained;
            }
            Ok(counts)
        },
        |run| {
            fs::write(&record, run).inspect_err(|error| {
                cowshed_os::eprintln!(
                    "record completed scratch sweep {}: {error}",
                    record.display()
                );
            })
        },
    )?;
    if let Some(counts) = completed {
        cowshed_os::eprintln!(
            "scratch lane setup {run}: swept dead runs in {:?}; removed={} retained={}",
            started.elapsed(),
            counts.removed,
            counts.retained
        );
    }
    Ok(())
}
fn sweep_dead_roots() -> std::io::Result<usize> {
    let dead = dead_directory_fixtures()?;
    for path in &dead {
        reclaim(path).inspect_err(|error| {
            cowshed_os::eprintln!(
                "abandoned directory fixture {} was not removed: {error}",
                path.display()
            );
        })?;
    }
    Ok(dead.len())
}

/// The directory fixtures under `/private/tmp` whose minting pid is gone.
fn dead_directory_fixtures() -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut dead = Vec::new();
    for entry in fs::read_dir("/private/tmp")? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(pid) = name
            .to_str()
            .and_then(|name| name.strip_prefix(ROOT_PREFIX))
            .and_then(|suffix| suffix.split('-').next())
            .and_then(|pid| pid.parse::<libc::pid_t>().ok())
            .filter(|pid| *pid > 0)
        else {
            continue;
        };
        // SAFETY: signal zero checks one positive pid without delivering a signal.
        if unsafe { libc::kill(pid, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            && entry.file_type()?.is_dir()
        {
            dead.push(entry.path());
        }
    }
    Ok(dead)
}

/// What a sweep would reclaim, counted without acting.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Census {
    pub(super) directory_fixtures: usize,
    pub(super) roots: usize,
    pub(super) processes: usize,
    pub(super) images: usize,
}

impl Census {
    pub(super) fn is_clean(&self) -> bool {
        *self == Self::default()
    }
}

/// The census `cowshed:apfs-test-sweep` is cached on (`cowshed-test-sweep --census`): the same selections
/// [`sweep_dead_runs`] acts on, read and counted. Dead directory fixtures, per-user-root entries whose owner is
/// gone ([`dead_owner`]), processes working in a dead run's tree, and the kernel's attachments of a dead run's
/// images; `inventory` only reads the attachment table. A clean census means a sweep has nothing to do.
pub(super) fn census(inventory: TestNamespace) -> std::io::Result<Census> {
    let mut roots = 0;
    for entry in fs::read_dir(USER_ROOT.as_path())? {
        if dead_owner(&entry?.path()).is_some() {
            roots += 1;
        }
    }
    let images = SystemCommandRunner::new(HostNamespace::Test(inventory))
        .attached_disk_images()?
        .iter()
        .filter(|attached| match &attached.source {
            DiskImageSource::File(path) => dead_owner(path).is_some(),
            DiskImageSource::Url(_) => false,
        })
        .count();
    Ok(Census {
        directory_fixtures: dead_directory_fixtures()?.len(),
        roots,
        processes: processes_in(|path| dead_owner(path).is_some())?.len(),
        images,
    })
}
