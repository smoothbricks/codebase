//! Scratch roots for tests that drive the host's real APFS stack.
//!
//! Shared by every test binary that attaches real images (`#[path]`-included, not a crate): the
//! cleanup protocol must be one protocol, or one binary's sweep reclaims what another still uses.

use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{
    ApfsSubstrateConfig, CommandRunner, DetachIntent, DiskImageSource, MacOsApfsExecutionHost,
    SystemCommandRunner,
};

/// Every scratch root lives directly under this prefix and spells out the pid of the run that
/// owns it. The pid is the whole ownership protocol: a later run can tell a live root from an
/// abandoned one without any shared state of its own.
pub(crate) const ROOT_PREFIX: &str = "/private/tmp/cowshed-itest-";

/// Held while a run sweeps. Every test process sweeps once, and nextest gives every test its own
/// process, so a run opens dozens of sweeps at once. Unserialized, each of them raced the others
/// for the same abandoned images: every sweeper waited on the image lease another held for its
/// detach (11.5 s measured), then found the volume already unmounted under it and failed. One
/// sweep at a time does the work once. The file records when the last finished sweep started,
/// and a sweep that started after a process asked to sweep has already seen every run that was
/// dead when it asked, so the queue behind a sweep returns without sweeping again. Its name
/// spells no pid, so no sweep ever reclaims it.
const SWEEP_LOCK: &str = "/private/tmp/cowshed-itest-sweep.lock";

/// One test's disposable root: `/private/tmp/cowshed-itest-<pid>-<n>-<label>`.
///
/// Dropping it detaches every image attached from below it, then removes the tree. A run that
/// could not drop it (a SIGKILL, a harness timeout) leaves it to the next run's sweep.
pub struct ScratchRoot {
    path: PathBuf,
}

impl ScratchRoot {
    pub fn new(label: &str) -> std::io::Result<Self> {
        static SWEEP: Once = Once::new();
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        SWEEP.call_once(sweep_dead_runs);
        let path = PathBuf::from(format!(
            "{ROOT_PREFIX}{}-{}-{label}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        // A failed test unwinds with its volumes still attached; removing the tree without
        // detaching first strands kernel attachments pointing into a half-deleted root, and
        // leaves their volumes showing up in Finder and `mount` until the machine reboots.
        if let Err(error) = detach_images(|image| image.starts_with(&self.path)) {
            eprintln!(
                "scratch root {} remains intact because its images could not be released: {error}",
                self.path.display()
            );
            return;
        }
        if let Err(error) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "scratch root {} was not removed ({error}); the next run's sweep reclaims it",
                self.path.display()
            );
        }
    }
}

/// Reclaim what runs that can no longer clean up after themselves left behind. Nothing runs after
/// a SIGKILL — a harness timeout, a bounded-exec force-kill, a `cargo test` killed mid-mount — so
/// the only protocol that always converges is that every run sweeps its dead predecessors.
///
/// The sweep is driven off the attachment table rather than the directory listing, because the
/// two residues outlive each other independently: a root directory can be deleted (by hand, or by
/// a tmp reaper) while its images stay attached, and an attached image keeps working from a
/// deleted backing file. Detaching therefore selects on the image path the kernel still holds,
/// not on what is on disk now.
fn sweep_dead_runs() {
    let asked = since_epoch();
    let sweeping = match lock_exclusive(Path::new(SWEEP_LOCK)) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("abandoned scratch roots stay until a later run: lock {SWEEP_LOCK}: {error}");
            return;
        }
    };
    let mut recorded = [0; 16];
    if sweeping.read_at(&mut recorded, 0).ok() == Some(recorded.len())
        && u128::from_be_bytes(recorded) >= asked
    {
        return;
    }
    let started = since_epoch();
    if let Err(error) =
        detach_images(|image| owner_pid(&image.to_string_lossy()).is_some_and(process_is_gone))
    {
        eprintln!("abandoned scratch roots remain intact after image-release failure: {error}");
        return;
    }
    let Ok(entries) = fs::read_dir("/private/tmp") else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if owner_pid(&path.to_string_lossy()).is_some_and(process_is_gone)
            && let Err(error) = fs::remove_dir_all(&path)
        {
            eprintln!(
                "abandoned scratch root {} was not removed: {error}",
                path.display()
            );
        }
    }
    if let Err(error) = sweeping.write_all_at(&started.to_be_bytes(), 0) {
        eprintln!("the next run sweeps again: record the sweep in {SWEEP_LOCK}: {error}");
    }
}

/// Nanoseconds since the Unix epoch, which orders sweeps across processes.
fn since_epoch() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
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
/// host reread and release every attachment of each one under its lease/pin/native-unmount
/// protocol. Never act on cached diskN, and never select from `hdiutil info`, which omits attached
/// images while another image attaches or detaches. A path the kernel holds twice is one image
/// with two attachments: it is released once, both detached, never refused as ambiguous.
fn detach_images(select: impl Fn(&Path) -> bool) -> std::io::Result<()> {
    let attached = SystemCommandRunner.attached_disk_images()?;
    let root = Path::new("/private/tmp");
    let host =
        MacOsApfsExecutionHost::new(SystemCommandRunner, ApfsSubstrateConfig::new(root, root))
            .map_err(std::io::Error::other)?;
    let mut first_error = None;
    let images = attached
        .iter()
        .filter_map(|attached| match &attached.source {
            DiskImageSource::File(path) => Some(path.as_path()),
            DiskImageSource::Url(_) => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    for image in images {
        if select(image)
            && let Err(error) = host.detach_existing_image(image, DetachIntent::Release)
        {
            eprintln!(
                "scratch image {} could not be released: {error}",
                image.display()
            );
            if first_error.is_none() {
                first_error = Some(std::io::Error::other(error));
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
