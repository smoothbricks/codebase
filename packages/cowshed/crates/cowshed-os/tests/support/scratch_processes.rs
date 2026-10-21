//! End processes working beneath a test-owned tree before reclaiming its filesystem state.

use std::collections::BTreeSet;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The grace used by `nx daemon --stop`, before force-ending a surviving worker.
const EXIT_GRACE: Duration = Duration::from_secs(10);

/// End every process but this one whose working directory `select` picks: `SIGTERM`, as `nx
/// daemon --stop` ends an Nx daemon, then `SIGKILL` to any that outlives [`EXIT_GRACE`]. Each is
/// named on stderr. The selection is asked again once they are gone, because a process can start
/// another in the tree while it ends -- a dying shell's job, a daemon's worker -- and the tree is
/// clear only once a selection finds none.
pub(crate) fn end_processes_in(select: impl Fn(&Path) -> bool) -> std::io::Result<()> {
    const ROUNDS: usize = 3;
    for _ in 0..ROUNDS {
        let found = processes_in(&select)?;
        if found.is_empty() {
            return Ok(());
        }
        for (pid, cwd) in &found {
            cowshed_os::eprintln!(
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
