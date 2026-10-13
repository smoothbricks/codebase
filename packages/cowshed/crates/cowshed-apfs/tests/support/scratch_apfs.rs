//! Scratch roots for tests that drive the host's real APFS stack, or start processes that outlive
//! their starter (a stock Nx daemon).
//!
//! Shared by every test binary that attaches real images (`#[path]`-included, not a crate): the
//! cleanup protocol must be one protocol, or one binary's sweep reclaims what another still uses.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Instant;

use super::{
    CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend, SystemCommandRunner,
    kernel_mounts, verify_no_symlinks,
};

#[path = "scratch_reclaim.rs"]
mod reclaim;

#[path = "../../../cowshed-os/tests/support/scratch_processes.rs"]
pub(crate) mod processes;

use processes::end_processes_in;

/// Every scratch root lives directly under this prefix and spells out the pid of the run that
/// owns it. The pid is the whole ownership protocol: a later run can tell a live root from an
/// abandoned one without any shared state of its own.
pub(crate) const ROOT_PREFIX: &str = "/private/tmp/cowshed-itest-";

/// Held while a sweep runs. Unserialized, concurrent sweeps raced each other for the same
/// abandoned images: every sweeper waited on the image lease another held for its detach (11.5 s
/// measured), then found the volume already unmounted under it and failed. Its name spells no
/// pid, so no sweep ever reclaims it. Other checkouts' sweeps share it and write their own
/// records into it, so it records nothing of this one's.
const SWEEP_LOCK: &str = "/private/tmp/cowshed-itest-sweep.lock";

/// The run whose sweep last finished ([`run_id`]), written under [`SWEEP_LOCK`]: every run
/// sweeps its dead predecessors once, not once per test process. Its name spells no pid either.
const SWEEP_RECORD: &str = "/private/tmp/cowshed-itest-sweep.run";

/// One test's disposable root: `/private/tmp/cowshed-itest-<pid>-<n>-<label>`.
///
/// Dropping it ends every process working in its tree, detaches every image attached from below
/// it, then removes the tree. Only the root that started a predecessor sweep joins that sweep;
/// another root's teardown never inherits that unrelated work.
pub struct ScratchRoot {
    path: PathBuf,
    sweep: Option<JoinHandle<()>>,
}

impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        static SWEEP: Once = Once::new();
        Self::new_with_sweep(label, &SWEEP, sweep_dead_runs)
    }

    /// A child fixture whose parent owns recovery if the child is killed. It cleans its own
    /// root on drop, but never starts or waits for a sweep of unrelated host runs.
    pub(crate) fn new_owned(label: &str) -> std::io::Result<Self> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = PathBuf::from(format!(
            "{ROOT_PREFIX}{}-{}-{label}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path)?;
        Ok(Self { path, sweep: None })
    }

    fn new_with_sweep(label: &str, once: &Once, sweep: fn()) -> std::io::Result<Self> {
        let mut root = Self::new_owned(label)?;
        // The starter owns this work through completion. Other roots can come and go without
        // waiting on its handle, while the live starter keeps its process's sweep from being
        // silently cut off on the normal path.
        once.call_once(|| {
            match std::thread::Builder::new()
                .name("scratch-sweep".to_owned())
                .spawn(sweep)
            {
                Ok(sweep) => root.sweep = Some(sweep),
                Err(error) => {
                    eprintln!("scratch root: no sweep thread ({error}); sweeping dead runs now");
                    sweep();
                }
            }
        });
        Ok(root)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Recover exactly the scratch root an owned child reported, after that child has been reaped.
/// This does not take the host-wide sweep lock, enumerate unrelated dead roots, or rewrite the
/// suite's sweep record. The parent's panic/kill witness uses the same cleanup as root drop.
pub(crate) fn reclaim_dead_root(path: &Path) -> std::io::Result<()> {
    if path.parent() != Some(Path::new("/private/tmp"))
        || !owner_pid(&path.to_string_lossy()).is_some_and(process_is_gone)
    {
        return Err(std::io::Error::other(format!(
            "refusing to reclaim a scratch root without a dead owner: {}",
            path.display()
        )));
    }
    tear_down_root(path)
}

fn tear_down_root(path: &Path) -> std::io::Result<()> {
    // End the root's workers before releasing any image they may hold.
    if let Err(error) = end_processes_in(|cwd| cwd.starts_with(path)) {
        eprintln!("scratch root {}: {error}", path.display());
    }
    let unreleased = detach_images(|image| image.starts_with(path))?;
    if !unreleased.is_empty() {
        return Err(std::io::Error::other(format!(
            "scratch root {} remains intact because its images could not be released: {unreleased:?}",
            path.display()
        )));
    }
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        let started = Instant::now();
        if let Err(error) = tear_down_root(&self.path) {
            eprintln!(
                "scratch root {}: {error}; a later sweep reclaims it",
                self.path.display()
            );
        }
        let torn_down = started.elapsed();
        let swept = Instant::now();
        if let Some(sweep) = self.sweep.take()
            && sweep.join().is_err()
        {
            eprintln!(
                "scratch root: its sweep of dead runs panicked before recording it; the next run sweeps"
            );
        }
        eprintln!(
            "scratch root {}: torn down in {torn_down:?}, then waited {:?} for its own sweep",
            self.path.display(),
            swept.elapsed()
        );
    }
}

/// Reclaim what runs that can no longer clean up after themselves left behind. Nothing runs after
/// a SIGKILL — a harness timeout, a bounded-exec force-kill, a `cargo test` killed mid-mount — so
/// the only protocol that always converges is that every run sweeps its dead predecessors. One
/// process of a run sweeps while the others wait on the lock, then find the run's sweep recorded
/// and return. A sweep killed with its process records nothing, so the next process to take the
/// lock, of this run or the next, sweeps.
///
/// The sweep is driven off the process and attachment tables rather than the directory listing,
/// because the residues outlive each other independently: a root directory can be deleted (by
/// hand, or by a tmp reaper) while its images stay attached, and an attached image keeps working
/// from a deleted backing file, as a process keeps working in a deleted directory. Ending and
/// detaching therefore select on the path the kernel still holds, not on what is on disk now.
/// Processes go first: one working in a dead run's volume would hold that volume attached.
fn sweep_dead_runs() {
    let run = run_id();
    let _sweeping = match lock_exclusive(Path::new(SWEEP_LOCK)) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("abandoned scratch roots stay until a later run: lock {SWEEP_LOCK}: {error}");
            return;
        }
    };
    if fs::read(SWEEP_RECORD).is_ok_and(|recorded| recorded == run.as_bytes()) {
        return;
    }
    let started = Instant::now();
    let dead = |path: &Path| owner_pid(&path.to_string_lossy()).is_some_and(process_is_gone);
    if let Err(error) = end_processes_in(dead) {
        eprintln!("abandoned scratch roots: {error}");
    }
    let unreleased = match detach_images(dead) {
        Ok(unreleased) => unreleased,
        Err(error) => {
            eprintln!(
                "abandoned scratch roots remain intact because image release could not be inventoried: {error}"
            );
            return;
        }
    };
    let Ok(entries) = fs::read_dir("/private/tmp") else {
        return;
    };
    reclaim::reclaim_dead_roots(
        entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| dead(path)),
        &unreleased,
        |path| fs::remove_dir_all(path),
        |path, error| {
            eprintln!(
                "abandoned scratch root {} was not removed: {error}",
                path.display()
            );
        },
    );
    if let Err(error) = fs::write(SWEEP_RECORD, run.as_bytes()) {
        eprintln!("this run sweeps again: record its sweep in {SWEEP_RECORD}: {error}");
    }
    eprintln!("scratch root: swept dead runs in {:?}", started.elapsed());
}

/// This run's identity, as [`SWEEP_RECORD`] records it: nextest gives every test process of one
/// run the same `NEXTEST_RUN_ID`; `cargo test` runs a binary's tests in one process, which sweeps
/// once, so its pid and the instant it swept name its run, where a pid alone could name an
/// earlier run that had the pid before.
fn run_id() -> String {
    std::env::var("NEXTEST_RUN_ID").unwrap_or_else(|_| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        format!("pid {} at {now}", std::process::id())
    })
}

/// `path` opened (created if absent) and `flock`ed exclusively, waiting for any holder: the lock
/// lives as long as the returned file, and dies with its process however that process ends.
pub(crate) fn lock_exclusive(path: &Path) -> std::io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    loop {
        // SAFETY: `flock` takes a descriptor `file` keeps open for the call and touches no memory.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
            return Ok(file);
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// The pid of the run that owns a scratch root, given the root itself or any path below it.
fn owner_pid(path: &str) -> Option<i32> {
    path.strip_prefix(ROOT_PREFIX)?
        .split(['-', '/'])
        .next()?
        .parse()
        .ok()
}

/// Whether no process holds `pid` any more. Signal 0 runs the existence and permission checks
/// without delivering anything, and `ESRCH` is the single answer that proves the owner is gone —
/// `EPERM` means it is alive under another user, so a run must not reclaim its root.
fn process_is_gone(pid: i32) -> bool {
    // SAFETY: `kill` with signal 0 only probes; it delivers nothing and touches no memory.
    let probe = unsafe { libc::kill(pid, 0) };
    probe != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

/// Select images by the backing path the kernel's I/O Registry holds, then let the production
/// backend reread and release every attachment of each one under its lease/pin/native-unmount
/// protocol. Never act on cached diskN, and never select from `hdiutil info`, which omits attached
/// images while another image attaches or detaches. A path the kernel holds twice is one image
/// with two attachments: it is released once, both detached, never refused as ambiguous.
///
/// The images are released concurrently, one thread each. Every release is an unmount under the
/// host's namespace lease and then a detach under its storage lease, and the two classes exclude
/// each other while a phase admits up to eight of one class (05_gateway.md, "Disk-lifecycle
/// lease"). One after another, every unmount and every detach of a root waited for a phase of its
/// own class on a loaded host; released together, their unmounts can share a namespace phase and
/// their detaches a storage phase. Each image still has exactly one holder.
///
/// Returns the backing paths whose individual release failed; an inventory failure is an error
/// because no root can then be proved safe to remove.
fn detach_images(select: impl Fn(&Path) -> bool) -> std::io::Result<BTreeSet<PathBuf>> {
    let attached = SystemCommandRunner.attached_disk_images()?;
    let images = attached
        .iter()
        .filter_map(|attached| match &attached.source {
            DiskImageSource::File(path) => Some(path.as_path()),
            DiskImageSource::Url(_) => None,
        })
        .filter(|image| select(image))
        .collect::<std::collections::BTreeSet<_>>();
    let release = release_image;
    let mut unreleased = BTreeSet::new();
    let mut report = |image: &Path, released: std::thread::Result<std::io::Result<()>>| {
        let error = match released {
            Ok(Ok(())) => return,
            Ok(Err(error)) => error,
            Err(_) => std::io::Error::other("its release thread panicked"),
        };
        eprintln!(
            "scratch image {} could not be released: {error}",
            image.display()
        );
        unreleased.insert(image.to_owned());
    };
    std::thread::scope(|scope| {
        let mut releasing = Vec::with_capacity(images.len());
        for image in images {
            match std::thread::Builder::new()
                .name("scratch-release".to_owned())
                .spawn_scoped(scope, move || release(image))
            {
                Ok(thread) => releasing.push((image, thread)),
                Err(error) => {
                    eprintln!(
                        "scratch image {}: no thread to release it beside the others ({error}); \
                         releasing it in turn",
                        image.display()
                    );
                    report(image, Ok(release(image)));
                }
            }
        }
        for (image, thread) in releasing {
            report(image, thread.join());
        }
    });
    Ok(unreleased)
}

/// Release every attachment the kernel holds for `image` as the store host releases an image
/// under its root: refuse a path a symlink redirects, read the kernel's mount sources now, then
/// the production backend unmounts each mounted volume through its pinned identity and detaches
/// each whole device once.
pub(crate) fn release_image(image: &Path) -> std::io::Result<()> {
    let root = Path::new("/private/tmp");
    if image.starts_with(root) {
        verify_no_symlinks(root, image).map_err(std::io::Error::other)?;
    }
    let mounted = kernel_mounts()?
        .into_iter()
        .map(|mount| mount.source_device)
        .collect();
    MacOsApfsBackend::new(SystemCommandRunner)
        .release_image(image, &mounted, DetachIntent::Release)
        .map_err(std::io::Error::other)
}

#[cfg(test)]
mod sweep_ownership_tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::mpsc::{self, Receiver, Sender};

    static RENDEZVOUS: Mutex<Option<(Sender<()>, Receiver<()>)>> = Mutex::new(None);

    fn blocked_sweep() {
        let (started, release) = RENDEZVOUS.lock().unwrap().take().unwrap();
        started.send(()).unwrap();
        release.recv().unwrap();
    }

    #[test]
    fn scoped_recovery_refuses_a_live_owner_and_non_root_paths() {
        let root = ScratchRoot::new_owned("live-recovery-refusal").unwrap();
        let sentinel = root.path().join("keep");
        fs::write(&sentinel, b"live owner's bytes").unwrap();
        for path in [root.path(), sentinel.as_path(), Path::new("/private/tmp")] {
            assert!(
                reclaim_dead_root(path).is_err(),
                "refuse {}",
                path.display()
            );
        }
        assert_eq!(fs::read(sentinel).unwrap(), b"live owner's bytes");
    }

    #[test]
    fn dropping_another_root_does_not_join_the_starters_sweep() {
        let (started, running) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        *RENDEZVOUS.lock().unwrap() = Some((started, blocked));
        let once = Once::new();
        let starter = ScratchRoot::new_with_sweep("sweep-owner", &once, blocked_sweep).unwrap();
        running.recv().unwrap();
        let other =
            ScratchRoot::new_with_sweep("not-sweep-owner", &once, || panic!("one sweep per run"))
                .unwrap();
        let other_path = other.path().to_owned();
        let (done, finished) = mpsc::channel();
        let dropping = std::thread::spawn(move || {
            drop(other);
            done.send(()).unwrap();
        });
        let independent = finished.recv_timeout(std::time::Duration::from_secs(2));
        // Always release and join the threads, including on RED: the assertion is not a hang.
        release.send(()).unwrap();
        dropping.join().unwrap();
        drop(starter);
        assert!(
            independent.is_ok(),
            "another root's teardown waited for the blocked sweep it did not start"
        );
        assert!(!other_path.exists(), "the root still cleans its own tree");
    }
}
