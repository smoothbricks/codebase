//! The real-substrate lane's synchronous setup, never a fixture destructor.

use super::scratch_namespace::processes::end_processes_in;
use super::scratch_namespace::{USER_ROOT, dead_owner, detach_images, tree};
use cowshed_os::namespace::TestNamespace;
use cowshed_os::test_backstop::FileBackstop as _;
use std::fs;
use std::os::unix::fs::FileExt as _;
use std::path::Path;
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
///
/// The pass says on stderr each step it starts and how long it took, each dead root by name, and
/// records the step on the lock for a pass queued behind it ([`SweepLock`]): a setup the runner
/// ends mid-pass names the work it was in, and a wait for the lock names its holder.
pub(super) fn sweep_dead_runs(namespace: TestNamespace, run: &str) -> std::io::Result<()> {
    let lock = cowshed_os::scratch::macos_scratch_sweep_lock()?;
    // The completion record shares the canonical lock's directory, never a fixture's tree.
    let record = lock.with_file_name("sweep.run");
    let sweeping = SweepLock::take(&lock, run).inspect_err(|error| {
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
            let directory_roots = sweeping.phase(
                "removing dead directory fixtures and templates in /private/tmp",
                || sweep_dead_roots(&sweeping),
            )?;
            sweeping
                .phase("ending the processes working in dead roots", || {
                    end_processes_in(|path| dead_owner(path).is_some())
                })
                .inspect_err(|error| {
                    cowshed_os::eprintln!("abandoned scratch roots: end workers: {error}");
                })?;
            let unreleased = sweeping
                .phase("releasing dead roots' images", || {
                    detach_images(namespace, dead_owner)
                })
                .inspect_err(|error| {
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
                    |path| {
                        sweeping.phase(
                            &format!("removing abandoned scratch root {}", path.display()),
                            || tree::remove(path),
                        )
                    },
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
fn sweep_dead_roots(sweeping: &SweepLock) -> std::io::Result<usize> {
    let mut removed = 0;
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
            let path = entry.path();
            sweeping
                .phase(
                    &format!("removing abandoned directory fixture {}", path.display()),
                    || reclaim(&path),
                )
                .inspect_err(|error| {
                    cowshed_os::eprintln!(
                        "abandoned directory fixture {} was not removed: {error}",
                        path.display()
                    );
                })?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// The sweep lock, held with its holder's record written on it: the pass's pid, its run and the
/// step it is in, rewritten at each step ([`Self::phase`]). A pass that finds the lock taken says
/// so on stderr, naming that holder, and names it again once per
/// [`cowshed_os::flock::HOLDER_REPORT`] while it keeps its place in the kernel's queue. Dropping it
/// empties the record before the lock goes.
struct SweepLock {
    file: fs::File,
    run: String,
}

impl SweepLock {
    fn take(lock: &Path, run: &str) -> std::io::Result<Self> {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock)?;
        if !cowshed_os::flock::try_exclusive(&file)? {
            let started = Instant::now();
            cowshed_os::eprintln!(
                "scratch lane setup {run}: waiting for {}, held by {}",
                lock.display(),
                holder(lock)
            );
            // Queued, not silent: the holder's release at the end of its pass, or the kernel's at
            // its exit, ends the wait, and the test backstop bounds the lock call itself.
            cowshed_os::flock::queued(lock, cowshed_os::flock::HOLDER_REPORT, holder, || {
                file.lock_bounded()
            })?;
            cowshed_os::eprintln!(
                "scratch lane setup {run}: took {} after {:?}",
                lock.display(),
                started.elapsed()
            );
        }
        let held = Self {
            file,
            run: run.to_owned(),
        };
        held.doing("starting its pass");
        Ok(held)
    }

    /// Rewrite the holder record to name `what` this pass is doing. One that cannot be written is
    /// said and leaves the pass running.
    fn doing(&self, what: &str) {
        let record = format!(
            "pid {} (cowshed-test-sweep, run {}): {what}\n",
            std::process::id(),
            self.run
        );
        if let Err(error) = self
            .file
            .set_len(0)
            .and_then(|()| self.file.write_all_at(record.as_bytes(), 0))
        {
            cowshed_os::eprintln!(
                "scratch lane setup {}: cannot record `{what}` on the sweep lock: {error}",
                self.run
            );
        }
    }

    /// Say on stderr, and on the lock's holder record, that this pass starts `what`, then how it
    /// ended and how long it took.
    fn phase<T>(
        &self,
        what: &str,
        work: impl FnOnce() -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        let started = Instant::now();
        cowshed_os::eprintln!("scratch lane setup {}: {what}", self.run);
        self.doing(what);
        let result = work();
        cowshed_os::eprintln!(
            "scratch lane setup {}: {what}: {} after {:?}",
            self.run,
            if result.is_ok() { "done" } else { "failed" },
            started.elapsed()
        );
        result
    }
}

impl Drop for SweepLock {
    fn drop(&mut self) {
        // One left unemptied names this pass until the next holder rewrites it.
        if let Err(error) = self.file.set_len(0) {
            cowshed_os::eprintln!(
                "scratch lane setup {}: cannot empty the sweep lock's holder record: {error}",
                self.run
            );
        }
    }
}

/// Who holds the sweep lock, as its holder's record names it.
fn holder(lock: &Path) -> String {
    match fs::read_to_string(lock) {
        Ok(record) if !record.trim().is_empty() => record.trim().to_owned(),
        Ok(_) => "a holder that wrote no record on it (a test holding the lock itself)".to_owned(),
        Err(error) => format!("a holder whose record cannot be read: {error}"),
    }
}
