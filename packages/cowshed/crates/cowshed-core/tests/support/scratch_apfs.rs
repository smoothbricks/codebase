//! Scratch roots for tests that drive the host's real APFS stack, or start processes that outlive
//! their starter (a stock Nx daemon).
//!
//! Shared by every test binary that attaches real images (`#[path]`-included, not a crate): the
//! cleanup protocol must be one protocol, or one binary's sweep reclaims what another still uses.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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
/// spells no pid, so no sweep ever reclaims it. Every build's tests share it, old and new side by
/// side on one host, so a record this build cannot read is no sweep ([`swept_since`]).
const SWEEP_LOCK: &str = "/private/tmp/cowshed-itest-sweep.lock";

/// How long the processes left working under a scratch root get to exit on `SIGTERM` before
/// they are sent `SIGKILL`: the grace `nx daemon --stop` gives a daemon (`build_volume::nx`),
/// which a stock Nx daemon meets in well under a second.
const EXIT_GRACE: Duration = Duration::from_secs(10);

/// One test's disposable root: `/private/tmp/cowshed-itest-<pid>-<n>-<label>`.
///
/// Dropping it ends every process working in its tree, detaches every image attached from below
/// it, then removes the tree. A run that could not drop it (a SIGKILL, a harness timeout) leaves
/// it to the next run's sweep.
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
        // A failed test unwinds with the processes it started still working in the tree. An Nx
        // daemon detaches from whatever started it, so nothing else ever ends it: it outlived
        // every failed test in a deleted directory, watching files for good. Ended first, no
        // such process holds a volume the detach below releases.
        if let Err(error) = end_processes_in(|cwd| cwd.starts_with(&self.path)) {
            eprintln!("scratch root {}: {error}", self.path.display());
        }
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
/// The sweep is driven off the process and attachment tables rather than the directory listing,
/// because the residues outlive each other independently: a root directory can be deleted (by
/// hand, or by a tmp reaper) while its images stay attached, and an attached image keeps working
/// from a deleted backing file, as a process keeps working in a deleted directory. Ending and
/// detaching therefore select on the path the kernel still holds, not on what is on disk now.
/// Processes go first: one working in a dead run's volume would hold that volume attached.
fn sweep_dead_runs() {
    let asked = since_epoch();
    let sweeping = match lock_exclusive(Path::new(SWEEP_LOCK)) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("abandoned scratch roots stay until a later run: lock {SWEEP_LOCK}: {error}");
            return;
        }
    };
    // One byte past a record, so a longer one reads as no record; a short read sweeps.
    let mut recorded = [0; 17];
    if sweeping
        .read_at(&mut recorded, 0)
        .is_ok_and(|read| swept_since(&recorded[..read], asked, since_epoch()))
    {
        return;
    }
    let started = since_epoch();
    let dead = |path: &Path| owner_pid(&path.to_string_lossy()).is_some_and(process_is_gone);
    if let Err(error) = end_processes_in(dead) {
        eprintln!("abandoned scratch roots: {error}");
    }
    if let Err(error) = detach_images(dead) {
        eprintln!("abandoned scratch roots remain intact after image-release failure: {error}");
        return;
    }
    let Ok(entries) = fs::read_dir("/private/tmp") else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if dead(&path)
            && let Err(error) = fs::remove_dir_all(&path)
        {
            eprintln!(
                "abandoned scratch root {} was not removed: {error}",
                path.display()
            );
        }
    }
    // Truncated first: a longer record another build left would otherwise keep its tail, and the
    // file would never again be exactly this build's record.
    if let Err(error) = sweeping
        .set_len(0)
        .and_then(|()| sweeping.write_all_at(&started.to_be_bytes(), 0))
    {
        eprintln!("the next run sweeps again: record the sweep in {SWEEP_LOCK}: {error}");
    }
}

/// Whether `record`, what [`SWEEP_LOCK`] holds, names a sweep that started once this process had
/// asked to sweep (`asked`) and no later than `now`, so it has already seen every run dead when
/// this one asked. Only exactly this build's record does: sixteen big-endian bytes of
/// nanoseconds. Anything else -- no record, a partial one, an instant from the future, or what
/// another build's sweep writes into the same file -- is no sweep, and the asking process sweeps.
/// Sparing a sweep only saves time; trusting a record wrongly leaves dead runs' images attached.
fn swept_since(record: &[u8], asked: u128, now: u128) -> bool {
    <[u8; 16]>::try_from(record)
        .is_ok_and(|record| (asked..=now).contains(&u128::from_be_bytes(record)))
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

/// End every process but this one whose working directory `select` picks: `SIGTERM`, as `nx
/// daemon --stop` ends an Nx daemon, then `SIGKILL` to any that outlives [`EXIT_GRACE`]. Each is
/// named on stderr. The selection is asked again once they are gone, because a process can start
/// another in the tree while it ends -- a dying shell's job, a daemon's worker -- and the tree is
/// clear only once a selection finds none.
fn end_processes_in(select: impl Fn(&Path) -> bool) -> std::io::Result<()> {
    const ROUNDS: usize = 3;
    for _ in 0..ROUNDS {
        let found = processes_in(&select)?;
        if found.is_empty() {
            return Ok(());
        }
        for (pid, cwd) in &found {
            eprintln!(
                "scratch root: ending pid {pid} ({}), working in {}",
                executable(*pid),
                cwd.display()
            );
        }
        let running = found.into_iter().map(|(pid, _)| pid).collect();
        let stayed = signal_and_wait(signal_and_wait(running, libc::SIGTERM)?, libc::SIGKILL)?;
        if !stayed.is_empty() {
            return Err(std::io::Error::other(format!(
                "pids {stayed:?} still run {EXIT_GRACE:?} after SIGKILL"
            )));
        }
    }
    let found = processes_in(&select)?;
    if found.is_empty() {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "processes still start in the tree after {ROUNDS} rounds of ending them: {found:?}"
    )))
}

/// Every process but this one whose working directory `select` picks, with that directory, by
/// the kernel's own answer (`proc_pidinfo` `PROC_PIDVNODEPATHINFO`), which still names a
/// directory deleted since. A process gone since the listing, a zombie and another user's process
/// name no directory, and are left out.
pub(crate) fn processes_in(
    select: impl Fn(&Path) -> bool,
) -> std::io::Result<Vec<(libc::pid_t, PathBuf)>> {
    // SAFETY: `getpid` has no preconditions and cannot fail.
    let this = unsafe { libc::getpid() };
    Ok(all_pids()?
        .into_iter()
        .filter(|&pid| pid > 0 && pid != this)
        .filter_map(|pid| {
            working_directory(pid)
                .filter(|cwd| select(cwd))
                .map(|cwd| (pid, cwd))
        })
        .collect())
}

fn working_directory(pid: libc::pid_t) -> Option<PathBuf> {
    let size = libc::c_int::try_from(size_of::<libc::proc_vnodepathinfo>()).ok()?;
    // SAFETY: `proc_vnodepathinfo` is plain data, for which all zeroes is a value.
    let mut info: libc::proc_vnodepathinfo = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is writable for `size` bytes, the size passed.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            (&raw mut info).cast(),
            size,
        )
    };
    if filled != size {
        return None;
    }
    let path = info.pvi_cdir.vip_path.as_flattened();
    // SAFETY: `c_char` and `u8` have one size and alignment; the slice is `path` itself.
    let path = unsafe { std::slice::from_raw_parts(path.as_ptr().cast::<u8>(), path.len()) };
    let path = std::ffi::CStr::from_bytes_until_nul(path).ok()?.to_bytes();
    (!path.is_empty()).then(|| PathBuf::from(std::ffi::OsStr::from_bytes(path)))
}

/// `pid`'s executable, to name it by, or why it could not be read.
fn executable(pid: libc::pid_t) -> String {
    let mut path = [0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: `path` is writable for its whole length, the size passed.
    let length = unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), path.len() as u32) };
    match usize::try_from(length) {
        Ok(length @ 1..) => String::from_utf8_lossy(&path[..length]).into_owned(),
        _ => format!("executable unreadable: {}", std::io::Error::last_os_error()),
    }
}

/// Send `signal` to every pid of `running` and wait up to [`EXIT_GRACE`] for their exits on the
/// kernel's exit events, each registered before its signal so that no exit is missed. Answers
/// the pids still running then.
fn signal_and_wait(
    running: BTreeSet<libc::pid_t>,
    signal: libc::c_int,
) -> std::io::Result<BTreeSet<libc::pid_t>> {
    let exit = |pid: libc::pid_t| libc::kevent {
        ident: pid as libc::uintptr_t,
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: libc::NOTE_EXIT,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    if running.is_empty() {
        return Ok(running);
    }
    // SAFETY: `kqueue` takes no arguments; a negative answer is an error.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `queue` is a fresh descriptor this function alone owns.
    let queue = unsafe { OwnedFd::from_raw_fd(queue) };
    let mut waiting = BTreeSet::new();
    for pid in running {
        let change = exit(pid);
        // SAFETY: one valid change, no events requested back.
        let registered = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        } == 0;
        // SAFETY: `pid` is positive, so it names one process and never a group.
        if !registered || unsafe { libc::kill(pid, signal) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                continue;
            }
            return Err(error);
        }
        waiting.insert(pid);
    }
    let deadline = Instant::now() + EXIT_GRACE;
    let mut events = [exit(0); 16];
    while !waiting.is_empty() {
        let Some(left) = deadline.checked_duration_since(Instant::now()) else {
            break;
        };
        let timeout = libc::timespec {
            tv_sec: left.as_secs() as libc::time_t,
            tv_nsec: libc::c_long::from(left.subsec_nanos()),
        };
        // SAFETY: room for `events.len()` events; `timeout` outlives the call.
        let ready = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                std::ptr::null(),
                0,
                events.as_mut_ptr(),
                events.len() as libc::c_int,
                &timeout,
            )
        };
        let Ok(ready) = usize::try_from(ready) else {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        };
        for event in &events[..ready] {
            waiting.remove(&(event.ident as libc::pid_t));
        }
    }
    Ok(waiting)
}

/// Every pid the kernel lists. The sizing call answers the process count with headroom; a
/// listing that fills the whole buffer may have been cut short by processes started since, so
/// it is asked again with the larger count.
fn all_pids() -> std::io::Result<Vec<libc::pid_t>> {
    let listed =
        |count: libc::c_int| usize::try_from(count).map_err(|_| std::io::Error::last_os_error());
    // SAFETY: a null buffer asks only for the number of pids to allot room for.
    let mut capacity = listed(unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) })?;
    loop {
        capacity += 64;
        let mut pids: Vec<libc::pid_t> = vec![0; capacity];
        let bytes = libc::c_int::try_from(capacity * size_of::<libc::pid_t>())
            .map_err(std::io::Error::other)?;
        // SAFETY: `pids` is writable for `bytes` bytes, the size passed.
        let count = listed(unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), bytes) })?;
        if count < capacity {
            pids.truncate(count);
            return Ok(pids);
        }
        capacity = count;
    }
}

#[cfg(test)]
mod tests {
    use super::swept_since;

    /// Only this build's record of a sweep that started after the asking process asked, and not
    /// later than now, spares that process its sweep. Another build's sweep shares the lock file
    /// and wrote its nextest run id there; read as an instant, its first sixteen bytes lie far in
    /// the future, and trusting them silenced every later sweep while the images of 37 dead runs
    /// stayed attached.
    #[test]
    fn a_record_this_build_did_not_write_is_no_sweep() {
        let asked: u128 = 1_791_000_000_000_000_000;
        let now = asked + 5_000_000;
        assert!(swept_since(&(asked + 1).to_be_bytes(), asked, now));
        assert!(swept_since(&now.to_be_bytes(), asked, now));
        assert!(!swept_since(&(asked - 1).to_be_bytes(), asked, now));
        assert!(!swept_since(&(now + 1).to_be_bytes(), asked, now));
        assert!(!swept_since(
            b"ff0f447a-0fc0-4b31-b5c7-32e43b0b7b41",
            asked,
            now
        ));
        assert!(!swept_since(b"", asked, now));
        let mut longer = (asked + 1).to_be_bytes().to_vec();
        longer.push(0);
        assert!(!swept_since(&longer, asked, now));
    }
}
