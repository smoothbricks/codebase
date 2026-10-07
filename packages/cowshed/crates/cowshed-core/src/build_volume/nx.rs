//! Nx's state inside a build volume: who holds its task database open, its daemon, and the
//! run summary stock Nx writes after every run (16_build_volumes.md, "The adoption needs the
//! target's Nx database closed", Land steps 4 to 7).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::Deserialize;

use super::BuildVolumeState;
use crate::api::dto::UnattributedRun;

/// Where Nx keeps its daemon's record inside `workspace-data` (pinned Nx 23.2.1,
/// `daemon/tmp-dir.js`). It names one checkout's process and socket, so it never travels.
pub const DAEMON_DIRECTORY: &str = "d";
const DAEMON_RECORD: &str = "server-process.json";
/// The run summary Nx's `StoreRunInformationLifeCycle` writes into its cache directory after
/// every `run`/`run-many` (pinned Nx 23.2.1, `tasks-runner/life-cycles/store-run-information-life-cycle.js`).
pub const RUN_SUMMARY: &str = "run.json";
/// How long a stopped daemon gets to exit after `SIGTERM`, as `nx daemon --stop` sends it.
const DAEMON_EXIT_GRACE: Duration = Duration::from_secs(10);

/// A process that holds a task database open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Holder {
    pub pid: i32,
    pub command: String,
}

impl std::fmt::Display for Holder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "pid {} ({})", self.pid, self.command)
    }
}

/// Every Nx task database in the volume rooted at `volume`: each `*.db` file directly in a
/// `workspace-data` directory the volume holds.
pub fn task_databases(volume: &Path, state: &BuildVolumeState) -> io::Result<Vec<PathBuf>> {
    let mut databases = Vec::new();
    for data in state.nx_workspace_data() {
        databases.extend(task_databases_in(&volume.join(data))?);
    }
    Ok(databases)
}

/// Delete every daemon record in the volume: a record names another checkout's process and
/// socket (rule "One Nx state per checkout").
pub fn discard_daemon_records(volume: &Path, state: &BuildVolumeState) -> io::Result<()> {
    for data in state.nx_workspace_data() {
        match fs::remove_dir_all(volume.join(data).join(DAEMON_DIRECTORY)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Copy into the volume rooted at `into` the record of each live daemon the volume rooted at
/// `from` names, when that daemon runs outside every sandbox, for a checkout whose build link
/// moves from `from` to `into` with no task database of `from` open ([`held`], under
/// [`hold_opens`]). Nx's daemon checks every 20 ms that the record names it and exits once it
/// does not (pinned Nx 23.2.1, `daemon/server/server.js`), so a daemon the move left without
/// its record dies under its clients, and a client that writes to it then fails
/// (`write EPIPE`). A host daemon with no task database open resolves every other path of its
/// state through the link when it opens it, as its clients do, so it serves the checkout on
/// `into` as it did on `from`. A sandboxed daemon (a workspace keeper's) is left to exit: its
/// grant names `from` alone, and its keeper starts the next one with a grant naming `into`.
#[cfg(target_os = "macos")]
pub fn hand_over_daemon_records(
    from: &Path,
    state: &BuildVolumeState,
    into: &Path,
) -> io::Result<()> {
    for data in state.nx_workspace_data() {
        let record = from.join(data).join(DAEMON_DIRECTORY).join(DAEMON_RECORD);
        let Some(live) = crate::runtime::nx_daemon::live_record(&record) else {
            continue;
        };
        if crate::process::sandboxed(live.pid)? {
            continue;
        }
        let directory = into.join(data).join(DAEMON_DIRECTORY);
        fs::create_dir_all(&directory)?;
        fs::write(directory.join(DAEMON_RECORD), &live.bytes)?;
    }
    Ok(())
}

/// Why a volume's Nx state could not be closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Busy {
    /// Processes other than the daemon hold a task database open.
    Held {
        database: PathBuf,
        holders: Vec<Holder>,
    },
    /// The daemon did not exit within its hang guard after `SIGTERM`.
    DaemonStayed { daemon: Holder },
}

impl std::fmt::Display for Busy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Held { database, holders } => {
                write!(formatter, "{} is open in ", database.display())?;
                for (index, holder) in holders.iter().enumerate() {
                    if index > 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(formatter, "{holder}")?;
                }
                Ok(())
            }
            Self::DaemonStayed { daemon } => write!(
                formatter,
                "the Nx daemon {daemon} did not exit within {}s of SIGTERM",
                DAEMON_EXIT_GRACE.as_secs()
            ),
        }
    }
}

/// Close the Nx state of the volume rooted at `volume`: when no process but the checkout's live
/// daemon holds a task database open, stop that daemon as stock `nx daemon --stop` does
/// (`SIGTERM` to the pid its record names, `daemon/client/client.js` `stop`), wait for its exit,
/// and ask once more. Any holder then is [`Busy`]; nothing is stopped except a daemon already
/// asked to.
#[cfg(target_os = "macos")]
pub fn close(volume: &Path, state: &BuildVolumeState) -> io::Result<Result<(), Busy>> {
    for data in state.nx_workspace_data() {
        if let Err(busy) = close_workspace_data(&volume.join(data))? {
            return Ok(Err(busy));
        }
    }
    Ok(Ok(()))
}

/// [`close`] for one `workspace-data` directory, wherever it lives: in a build volume, or in a
/// checkout that links none. A directory that does not exist holds nothing.
pub fn close_workspace_data(data: &Path) -> io::Result<Result<(), Busy>> {
    let daemon = live_daemon(data);
    let databases = task_databases_in(data)?;
    if let Some(busy) = first_held(&databases, daemon)? {
        return Ok(Err(busy));
    }
    if let Some(pid) = daemon
        && !stop_daemon(pid)?
    {
        return Ok(Err(Busy::DaemonStayed {
            daemon: Holder {
                pid,
                command: command_line(pid),
            },
        }));
    }
    Ok(first_held(&databases, None)?.map_or(Ok(()), Err))
}

/// Whether a process other than the live daemon of the `workspace-data` directory `data` holds
/// one of its task databases open, as [`close_workspace_data`] first asks: an Nx client at work,
/// which a host shell is as much as a job. Nothing is stopped. Nx's graph and hash passes open
/// no database, so this is the signal of a run's tasks, not of every client.
pub fn in_use(data: &Path) -> io::Result<Option<Busy>> {
    first_held(&task_databases_in(data)?, live_daemon(data))
}

/// The first of `databases` that a process other than `daemon` holds open, with its holders.
fn first_held(databases: &[PathBuf], daemon: Option<i32>) -> io::Result<Option<Busy>> {
    for database in databases {
        let holders: Vec<Holder> = holders(database)?
            .into_iter()
            .filter(|holder| Some(holder.pid) != daemon)
            .collect();
        if !holders.is_empty() {
            return Ok(Some(Busy::Held {
                database: database.clone(),
                holders,
            }));
        }
    }
    Ok(None)
}

/// Whether any process holds one of the volume's task databases open, without stopping
/// anything: the second look [`close`] takes, for a caller that has to know nothing opened one
/// since.
#[cfg(target_os = "macos")]
pub fn held(volume: &Path, state: &BuildVolumeState) -> io::Result<Result<(), Busy>> {
    for data in state.nx_workspace_data() {
        for database in task_databases_in(&volume.join(data))? {
            let holders = holders(&database)?;
            if !holders.is_empty() {
                return Ok(Err(Busy::Held { database, holders }));
            }
        }
    }
    Ok(Ok(()))
}

/// Nx's open locks on a volume's task databases, held by this process until dropped. Stock Nx
/// opens a task database only while it holds that database's open lock: the database's path
/// with the `lock` extension, locked exclusively (`flock`) before the database is opened and let
/// go once it is (pinned Nx 23.2.1, `native/db/mod.rs` `connect_to_nx_db`,
/// `native/db/initialize.rs` `create_lock_file`). So while these are held no Nx process opens
/// one of the databases: an opener waits on the lock, then opens the database by its path, so
/// through whatever the checkout's build link names once they are let go.
#[cfg(target_os = "macos")]
#[must_use]
pub struct Opens {
    _locks: Vec<fs::File>,
}

/// A task database an Nx process is opening: the processes that have its open lock open, the
/// one holding it and any waiting on it.
#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Opening {
    pub database: PathBuf,
    pub holders: Vec<Holder>,
}

/// Take, without waiting, the open lock of each task database the volume rooted at `volume`
/// holds and of each one `next` holds at the same place, which an Nx process opening through
/// the checkout's link would create in `volume` while the link still names it. Answers the held
/// locks, or the first database an Nx process is opening right now, after letting go of the
/// rest. While the locks are held, [`held`] covers every Nx process: one that opened a database
/// before holds it, and one that had not waits until the locks are let go.
#[cfg(target_os = "macos")]
pub fn hold_opens(
    volume: &Path,
    state: &BuildVolumeState,
    next: &Path,
) -> io::Result<Result<Opens, Opening>> {
    let mut locks = Vec::new();
    for data in state.nx_workspace_data() {
        let mut names = std::collections::BTreeSet::new();
        for root in [volume, next] {
            names.extend(
                task_databases_in(&root.join(data))?
                    .into_iter()
                    .filter_map(|database| database.file_name().map(ToOwned::to_owned)),
            );
        }
        if names.is_empty() {
            continue;
        }
        let directory = volume.join(data);
        fs::create_dir_all(&directory)?;
        for name in names {
            let database = directory.join(name);
            let lock = database.with_extension("lock");
            let file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock)?;
            match file.try_lock() {
                Ok(()) => locks.push(file),
                Err(fs::TryLockError::WouldBlock) => {
                    drop(file);
                    drop(locks);
                    return Ok(Err(Opening {
                        database,
                        holders: holders(&lock)?,
                    }));
                }
                Err(fs::TryLockError::Error(error)) => return Err(error),
            }
        }
    }
    Ok(Ok(Opens { _locks: locks }))
}

fn task_databases_in(data: &Path) -> io::Result<Vec<PathBuf>> {
    let mut databases = Vec::new();
    let entries = match fs::read_dir(data) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(databases),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "db") && path.is_file() {
            databases.push(path);
        }
    }
    databases.sort();
    Ok(databases)
}

/// The pid of the daemon `workspace-data`'s record names, when that daemon is live, from the one
/// read of the record that verified it: its process runs and its socket accepts a connection.
fn live_daemon(data: &Path) -> Option<i32> {
    crate::runtime::nx_daemon::live_pid(&data.join(DAEMON_DIRECTORY).join(DAEMON_RECORD))
}

/// `SIGTERM` `pid` and wait for its exit on the kernel's exit event: `true` once it has exited,
/// `false` when it outlived [`DAEMON_EXIT_GRACE`], which guards only against a hang. The
/// daemon's own termination handler removes its socket and record.
#[cfg(target_os = "macos")]
fn stop_daemon(pid: i32) -> io::Result<bool> {
    use std::os::fd::{FromRawFd, OwnedFd};
    // SAFETY: `kqueue` takes no arguments; a negative answer is an error.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `queue` is a fresh descriptor this function alone owns.
    let queue = unsafe { OwnedFd::from_raw_fd(queue) };
    let exit = libc::kevent {
        ident: pid as libc::uintptr_t,
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: libc::NOTE_EXIT,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    use std::os::fd::AsRawFd;
    // Registered before the signal, so an exit between the two is never missed.
    // SAFETY: one valid change, no events requested back.
    if unsafe {
        libc::kevent(
            queue.as_raw_fd(),
            &exit,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    } < 0
    {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(true),
            _ => Err(error),
        };
    }
    // SAFETY: `pid` is positive (a live daemon's), so it names one process and never a group.
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(true),
            _ => Err(error),
        };
    }
    let timeout = libc::timespec {
        tv_sec: DAEMON_EXIT_GRACE.as_secs() as libc::time_t,
        tv_nsec: 0,
    };
    let mut event = exit;
    loop {
        // SAFETY: room for one event; `timeout` outlives the call.
        let ready = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                std::ptr::null(),
                0,
                &mut event,
                1,
                &timeout,
            )
        };
        match ready {
            1 => return Ok(true),
            0 => return Ok(false),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

/// [`stop_daemon`] through a pidfd: opened before the signal, so an exit between the two is never
/// missed, and the signal goes through the descriptor, so it can only ever reach the process the
/// descriptor was opened on, never another that reuses the pid.
#[cfg(target_os = "linux")]
fn stop_daemon(pid: i32) -> io::Result<bool> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    // SAFETY: pidfd_open takes plain integers; a non-negative answer is a new descriptor.
    let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if opened < 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(true),
            _ => Err(error),
        };
    }
    let descriptor = i32::try_from(opened).map_err(io::Error::other)?;
    // SAFETY: `descriptor` is a fresh descriptor this function alone owns.
    let process = unsafe { OwnedFd::from_raw_fd(descriptor) };
    // SAFETY: a pidfd, SIGTERM, no siginfo and no flags: the call reads nothing else.
    let signalled = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            process.as_raw_fd(),
            libc::SIGTERM,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    };
    if signalled != 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(true),
            _ => Err(error),
        };
    }
    let mut poll = libc::pollfd {
        fd: process.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout = libc::c_int::try_from(DAEMON_EXIT_GRACE.as_millis()).map_err(io::Error::other)?;
    loop {
        // SAFETY: `poll` describes one live descriptor this function owns.
        match unsafe { libc::poll(&mut poll, 1, timeout) } {
            1 => return Ok(true),
            0 => return Ok(false),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

/// Every process that has `path` open, from procfs: each `/proc/<pid>/fd` link that names the
/// file. A process of another user, or one that exited since the listing, shows none.
#[cfg(target_os = "linux")]
pub fn holders(path: &Path) -> io::Result<Vec<Holder>> {
    let file = match fs::canonicalize(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    procfs_holders(|process| {
        fs::read_dir(process.join("fd")).is_ok_and(|descriptors| {
            descriptors
                .flatten()
                .any(|descriptor| fs::read_link(descriptor.path()).is_ok_and(|link| link == file))
        })
    })
}

/// Every process holding anything on the volume mounted at `mount` -- an open file, a mapped
/// file, its working directory, its root, its executable -- which is what keeps `umount`
/// answering `EBUSY`. From procfs, so a process of another user, or one that exited since the
/// listing, shows none.
#[cfg(target_os = "linux")]
pub fn volume_holders(mount: &Path) -> io::Result<Vec<Holder>> {
    let mount = match fs::canonicalize(mount) {
        Ok(mount) => mount,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let on_volume = |link: PathBuf| link.starts_with(&mount);
    procfs_holders(|process| {
        ["cwd", "root", "exe"]
            .iter()
            .any(|name| fs::read_link(process.join(name)).is_ok_and(on_volume))
            || fs::read_dir(process.join("fd")).is_ok_and(|descriptors| {
                descriptors
                    .flatten()
                    .any(|descriptor| fs::read_link(descriptor.path()).is_ok_and(on_volume))
            })
            // A mapping's path is its line's last field and the line's first `/`: the
            // address range, permissions, offset, device and inode before it hold none.
            || fs::read_to_string(process.join("maps")).is_ok_and(|maps| {
                maps.lines().any(|mapping| {
                    mapping
                        .find('/')
                        .is_some_and(|start| Path::new(&mapping[start..]).starts_with(&mount))
                })
            })
    })
}

/// Every process under `/proc` that `held` answers yes for, given its `/proc/<pid>` directory.
#[cfg(target_os = "linux")]
fn procfs_holders(held: impl Fn(&Path) -> bool) -> io::Result<Vec<Holder>> {
    let mut holders = Vec::new();
    for process in fs::read_dir("/proc")? {
        let process = process?;
        let Some(pid) = process
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        if held(&process.path()) {
            holders.push(Holder {
                pid,
                command: command_line(pid),
            });
        }
    }
    Ok(holders)
}

/// `pid`'s argument vector joined by spaces, or its executable path, or a placeholder naming that
/// neither could be read.
#[cfg(target_os = "linux")]
fn command_line(pid: libc::pid_t) -> String {
    if let Ok(arguments) = fs::read(format!("/proc/{pid}/cmdline")) {
        let command = arguments
            .split(|&byte| byte == 0)
            .filter(|argument| !argument.is_empty())
            .map(|argument| String::from_utf8_lossy(argument).into_owned())
            .collect::<Vec<_>>()
            .join(" ");
        if !command.is_empty() {
            return command;
        }
    }
    fs::read_link(format!("/proc/{pid}/exe")).map_or_else(
        |_| "command unreadable".to_owned(),
        |executable| executable.display().to_string(),
    )
}

/// Every process that has `path` open: one open-file query (`proc_listpidspath`), the kernel's
/// own answer, never a scan of `lsof` output.
#[cfg(target_os = "macos")]
pub fn holders(path: &Path) -> io::Result<Vec<Holder>> {
    listpidspath(path, 0)
}

/// Every process holding anything on the volume mounted at `mount` -- an open file, its working
/// directory, its executable -- which is what keeps `umount` answering `Resource busy`.
/// Event-only opens (FSEvents, Spotlight) never block an unmount and are left out.
#[cfg(target_os = "macos")]
pub fn volume_holders(mount: &Path) -> io::Result<Vec<Holder>> {
    const PROC_LISTPIDSPATH_PATH_IS_VOLUME: u32 = 1;
    const PROC_LISTPIDSPATH_EXCLUDE_EVTONLY: u32 = 2;
    listpidspath(
        mount,
        PROC_LISTPIDSPATH_PATH_IS_VOLUME | PROC_LISTPIDSPATH_EXCLUDE_EVTONLY,
    )
}

#[cfg(target_os = "macos")]
fn listpidspath(path: &Path, pathflags: u32) -> io::Result<Vec<Holder>> {
    use std::os::unix::ffi::OsStrExt;
    const PROC_ALL_PIDS: u32 = 1;
    unsafe extern "C" {
        fn proc_listpidspath(
            kind: u32,
            typeinfo: u32,
            path: *const libc::c_char,
            pathflags: u32,
            buffer: *mut libc::c_void,
            buffersize: libc::c_int,
        ) -> libc::c_int;
    }
    let path_c = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    let mut capacity = 256usize;
    loop {
        let mut pids = vec![0 as libc::pid_t; capacity];
        let bytes = libc::c_int::try_from(capacity * size_of::<libc::pid_t>())
            .map_err(|_| io::Error::other("process table too large"))?;
        // SAFETY: `path_c` is NUL-terminated and `pids` is writable for `bytes` bytes.
        let filled = unsafe {
            proc_listpidspath(
                PROC_ALL_PIDS,
                0,
                path_c.as_ptr(),
                pathflags,
                pids.as_mut_ptr().cast(),
                bytes,
            )
        };
        if filled < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = usize::try_from(filled).unwrap_or(0) / size_of::<libc::pid_t>();
        if count == capacity {
            capacity *= 4;
            continue;
        }
        return Ok(pids[..count]
            .iter()
            .copied()
            .filter(|&pid| pid > 0)
            .map(|pid| Holder {
                pid,
                command: command_line(pid),
            })
            .collect());
    }
}

/// `pid`'s argument vector joined by spaces (`KERN_PROCARGS2`), or its executable path, or a
/// placeholder naming why neither could be read.
#[cfg(target_os = "macos")]
fn command_line(pid: libc::pid_t) -> String {
    let mut mib = [libc::CTL_KERN, libc::KERN_ARGMAX];
    let mut argmax: libc::c_int = 0;
    let mut argmax_size = size_of::<libc::c_int>();
    // SAFETY: `mib` names KERN_ARGMAX, whose value is one c_int written into `argmax`.
    let ok = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            (&raw mut argmax).cast(),
            &mut argmax_size,
            std::ptr::null_mut(),
            0,
        )
    } == 0;
    if ok && let Ok(capacity) = usize::try_from(argmax) {
        let mut buffer = vec![0u8; capacity];
        let mut size: libc::size_t = capacity;
        let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
        // SAFETY: `buffer` is writable for `size` bytes; the kernel writes at most that many.
        let read = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                3,
                buffer.as_mut_ptr().cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        } == 0;
        if read && let Some(command) = parse_procargs(&buffer[..size]) {
            return command;
        }
    }
    let mut path = [0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    // SAFETY: `path` is writable for its whole length.
    let length = unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), path.len() as u32) };
    if length > 0 {
        return String::from_utf8_lossy(&path[..length as usize]).into_owned();
    }
    "command unreadable".to_owned()
}

/// `KERN_PROCARGS2`: a native-endian `argc`, the executable path, NUL padding, then `argc`
/// NUL-terminated arguments (then the environment, which is never read).
#[cfg(target_os = "macos")]
fn parse_procargs(buffer: &[u8]) -> Option<String> {
    let argc = i32::from_ne_bytes(buffer.get(..4)?.try_into().ok()?);
    let mut rest = &buffer[4..];
    let executable_end = rest.iter().position(|&byte| byte == 0)?;
    rest = &rest[executable_end..];
    let first = rest.iter().position(|&byte| byte != 0)?;
    rest = &rest[first..];
    let mut arguments = Vec::new();
    for _ in 0..argc {
        let end = rest.iter().position(|&byte| byte == 0)?;
        arguments.push(String::from_utf8_lossy(&rest[..end]).into_owned());
        rest = &rest[end + 1..];
    }
    (!arguments.is_empty()).then(|| arguments.join(" "))
}

/// The last Nx run recorded in a cache directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Run {
    /// `nx <arguments>`, as Nx records its own command line.
    pub command: String,
    pub tasks: Vec<RunTask>,
}

/// One task of a run, as Nx's run summary records it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunTask {
    pub task_id: String,
    pub project: String,
    pub target: String,
    pub hash: String,
    pub cache: CacheStatus,
    pub code: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheStatus {
    LocalHit,
    RemoteHit,
    Miss,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunSummaryWire {
    run: RunWire,
    tasks: Vec<RunTaskWire>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunWire {
    command: String,
    start_time: String,
    end_time: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunTaskWire {
    task_id: String,
    target: String,
    project_name: String,
    hash: String,
    cache_status: String,
    status: i32,
}

/// Whether the run summary in `cache` is the one `check` wrote.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Attribution {
    Ours(Run),
    Unattributed(UnattributedRun),
}

/// Attribute the run summary in `cache` to `check`, spawned at `spawned` and exited at `exited`
/// (16_build_volumes.md, Land step 7). Stock Nx writes the summary once, at the end of a run, and
/// a check's Nx writes it before the check exits, so the summary is the check's when Nx's own
/// `run.startTime` is not before the spawn, its `run.endTime` is not after the exit, its command
/// is one the check spells, and it has a task for every target that command names. A run that
/// ended inside the window before the check's own was overwritten by it; any other summary is
/// [`Attribution::Unattributed`] with the reason, never counted.
pub fn attribute(
    cache: &Path,
    check: &str,
    spawned: SystemTime,
    exited: SystemTime,
) -> Attribution {
    let path = cache.join(RUN_SUMMARY);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Attribution::Unattributed(UnattributedRun::NoSummary);
        }
        Err(error) => return unreadable(format!("{}: {error}", path.display())),
    };
    let summary: RunSummaryWire = match serde_json::from_slice(&bytes) {
        Ok(summary) => summary,
        Err(error) => return unreadable(format!("{}: {error}", path.display())),
    };
    let (Some(run_started), Some(run_ended)) = (
        parse_utc_millis(&summary.run.start_time),
        parse_utc_millis(&summary.run.end_time),
    ) else {
        return unreadable(format!(
            "{} records unreadable times {:?}..{:?}",
            path.display(),
            summary.run.start_time,
            summary.run.end_time
        ));
    };
    // Nx stamps milliseconds; the window is widened to the millisecond it truncates to.
    let millis = |at: SystemTime| {
        at.duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or(0)
    };
    if run_started < millis(spawned) {
        return Attribution::Unattributed(UnattributedRun::BeganBeforeCheck {
            start: summary.run.start_time,
        });
    }
    if run_ended > millis(exited) + 1 || run_ended < run_started {
        return Attribution::Unattributed(UnattributedRun::EndedAfterCheck {
            end: summary.run.end_time,
        });
    }
    let Some(named) = spelled_by(check, &summary.run.command) else {
        return Attribution::Unattributed(UnattributedRun::ForeignCommand {
            command: summary.run.command,
        });
    };
    let mut tasks = Vec::with_capacity(summary.tasks.len());
    for task in summary.tasks {
        let cache = match task.cache_status.as_str() {
            "local-cache-hit" => CacheStatus::LocalHit,
            "remote-cache-hit" => CacheStatus::RemoteHit,
            "cache-miss" => CacheStatus::Miss,
            other => {
                return unreadable(format!(
                    "{} records unknown cache status {other:?}",
                    path.display()
                ));
            }
        };
        tasks.push(RunTask {
            task_id: task.task_id,
            project: task.project_name,
            target: task.target,
            hash: task.hash,
            cache,
            code: task.status,
        });
    }
    let uncovered: Vec<String> = named
        .into_iter()
        .filter(|target| !tasks.iter().any(|task| task.target == *target))
        .collect();
    if !uncovered.is_empty() {
        return Attribution::Unattributed(UnattributedRun::Uncovered { targets: uncovered });
    }
    Attribution::Ours(Run {
        command: summary.run.command,
        tasks,
    })
}

fn unreadable(error: String) -> Attribution {
    Attribution::Unattributed(UnattributedRun::Unreadable { error })
}

/// The targets `command` (as Nx records it: the stem of its script, then its arguments joined by
/// spaces) names, when `check` spells that command: the script's stem as a word of the check,
/// followed by exactly those arguments and then the end of a shell command. `None` when the
/// check does not spell it.
fn spelled_by(check: &str, command: &str) -> Option<Vec<String>> {
    const SEPARATORS: [&str; 6] = [";", "&&", "||", "|", ")", "&"];
    let mut recorded = command.split_whitespace();
    let script = recorded.next()?;
    let arguments: Vec<&str> = recorded.collect();
    let mut words: Vec<&str> = Vec::new();
    for word in check.split_whitespace() {
        let word = word.trim_matches(|c| c == '\'' || c == '"');
        match word.strip_suffix(';') {
            Some(word) => words.extend([word, ";"]),
            None => words.push(word),
        }
    }
    let stem = |word: &str| {
        let name = word.rsplit('/').next().unwrap_or(word);
        name.strip_suffix(".js").unwrap_or(name).to_owned()
    };
    let end = arguments.len() + 1;
    let spelled = (0..words.len()).any(|at| {
        stem(words[at]) == script
            && words.get(at + 1..at + end) == Some(arguments.as_slice())
            && words
                .get(at + end)
                .is_none_or(|next| SEPARATORS.contains(next))
    });
    if !spelled {
        return None;
    }
    let mut targets = Vec::new();
    let mut naming = false;
    for argument in &arguments {
        if let Some(value) = ["--targets=", "--target=", "-t="]
            .iter()
            .find_map(|flag| argument.strip_prefix(flag))
        {
            targets.extend(
                value
                    .split(',')
                    .filter(|t| !t.is_empty())
                    .map(str::to_owned),
            );
            naming = false;
        } else if matches!(*argument, "-t" | "--targets" | "--target") {
            naming = true;
        } else if argument.starts_with('-') {
            naming = false;
        } else if naming {
            targets.extend(
                argument
                    .split(',')
                    .filter(|t| !t.is_empty())
                    .map(str::to_owned),
            );
        }
    }
    Some(targets)
}

/// What `nx show target inputs <task> --json` prints (pinned Nx 23.2.1,
/// `command-line/show/show-target/inputs.js` `renderInputs`): the task's project and target and
/// the raw hash inputs of its plan, by category.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TaskInputsWire {
    project: String,
    target: String,
    external: Vec<String>,
    runtime: Vec<String>,
    environment: Vec<String>,
    files: Vec<String>,
    dep_outputs: Vec<String>,
}

/// The hash inputs of `task` (`project:target` or `project:target:configuration`) by Nx's own
/// category names, from what `nx show target inputs <task> --json` printed. Anything but that
/// exact shape, for that task, is an error naming what was read: a custom hasher's warning
/// object, a missing or unknown category, another task.
pub fn task_inputs(
    task: &str,
    stdout: &[u8],
) -> Result<std::collections::BTreeMap<String, Vec<String>>, String> {
    let wire: TaskInputsWire = serde_json::from_slice(stdout).map_err(|error| {
        format!(
            "nx show target inputs {task} printed no hash inputs ({error}): {}",
            String::from_utf8_lossy(&stdout[..stdout.len().min(512)])
        )
    })?;
    let named = format!("{}:{}", wire.project, wire.target);
    if task != named && !task.starts_with(&format!("{named}:")) {
        return Err(format!(
            "nx show target inputs {task} described {named} instead"
        ));
    }
    Ok([
        ("external", wire.external),
        ("runtime", wire.runtime),
        ("environment", wire.environment),
        ("files", wire.files),
        ("depOutputs", wire.dep_outputs),
    ]
    .into_iter()
    .map(|(category, entries)| (category.to_owned(), entries))
    .collect())
}

/// Milliseconds since the epoch of `YYYY-MM-DDTHH:MM:SS.mmmZ`, as JavaScript's
/// `Date.prototype.toISOString` writes it.
fn parse_utc_millis(value: &str) -> Option<u128> {
    let bytes = value.as_bytes();
    if bytes.len() != 24
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'.'
        || bytes[23] != b'Z'
    {
        return None;
    }
    let number = |range: std::ops::Range<usize>| value.get(range)?.parse::<u64>().ok();
    let days = crate::storage::days_from_civil(number(0..4)?, number(5..7)?, number(8..10)?)?;
    let (hour, minute, second, millis) = (
        number(11..13)?,
        number(14..16)?,
        number(17..19)?,
        number(20..23)?,
    );
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    Some(u128::from(seconds) * 1_000 + u128::from(millis))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::BuildStatePath;
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    use crate::fork_lock::Spawn as _;

    fn scratch(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-nx-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn state() -> BuildVolumeState {
        BuildVolumeState {
            paths: vec![
                BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
                BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
            ],
            fingerprint: None,
        }
    }

    fn summary(command: &str, start: &str, end: &str) -> String {
        format!(
            r#"{{"run":{{"command":"{command}","startTime":"{start}","endTime":"{end}","inner":false}},
               "tasks":[
                 {{"taskId":"a:build","target":"build","projectName":"a","hash":"1","startTime":"x","endTime":"y","params":"","cacheStatus":"local-cache-hit","status":0}},
                 {{"taskId":"b:test","target":"test","projectName":"b","hash":"2","startTime":"x","endTime":"y","params":"","cacheStatus":"cache-miss","status":0}}]}}"#
        )
    }

    fn at(millis: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(millis)
    }

    /// The check's own summary is attributed to it; every summary a foreign run planted in the
    /// target's cache is unattributed with its reason and counts nothing.
    #[test]
    fn only_the_checks_own_run_summary_is_attributed_to_it() {
        let root = scratch("run");
        let check = "bun nx run-many -t build test --outputStyle=static-failures-only";
        let ours = "nx run-many -t build test --outputStyle=static-failures-only";
        // 2026-10-03T16:19:43.794Z .. 16:19:44.292Z
        let (spawned, exited) = (at(1_791_044_383_794), at(1_791_044_384_292));
        let plant = |command: &str, start: &str, end: &str| {
            fs::write(root.join(RUN_SUMMARY), summary(command, start, end)).unwrap();
            attribute(&root, check, spawned, exited)
        };
        assert_eq!(
            attribute(&root, check, spawned, exited),
            Attribution::Unattributed(UnattributedRun::NoSummary)
        );
        let Attribution::Ours(run) =
            plant(ours, "2026-10-03T16:19:43.794Z", "2026-10-03T16:19:44.292Z")
        else {
            panic!("the check's own summary is its own");
        };
        assert_eq!(run.command, ours);
        assert_eq!(
            run.tasks.iter().map(|task| task.cache).collect::<Vec<_>>(),
            [CacheStatus::LocalHit, CacheStatus::Miss]
        );
        assert_eq!(run.tasks[1].task_id, "b:test");
        // A foreign run of the same command that began before the check wrote last: the check
        // ran no Nx here, or its summary was overwritten.
        assert_eq!(
            plant(ours, "2026-10-03T16:19:43.793Z", "2026-10-03T16:19:44.000Z"),
            Attribution::Unattributed(UnattributedRun::BeganBeforeCheck {
                start: "2026-10-03T16:19:43.793Z".to_owned()
            })
        );
        // One that began inside the check and ended after it.
        assert_eq!(
            plant(ours, "2026-10-03T16:19:43.900Z", "2026-10-03T16:19:44.294Z"),
            Attribution::Unattributed(UnattributedRun::EndedAfterCheck {
                end: "2026-10-03T16:19:44.294Z".to_owned()
            })
        );
        // A different command wholly inside the check.
        assert_eq!(
            plant(
                "nx run-many -t lint",
                "2026-10-03T16:19:43.900Z",
                "2026-10-03T16:19:44.000Z"
            ),
            Attribution::Unattributed(UnattributedRun::ForeignCommand {
                command: "nx run-many -t lint".to_owned()
            })
        );
        // The check's command, but the summary has no task of a target it names.
        let wider = "nx run-many -t build lint";
        fs::write(
            root.join(RUN_SUMMARY),
            summary(
                wider,
                "2026-10-03T16:19:43.900Z",
                "2026-10-03T16:19:44.000Z",
            ),
        )
        .unwrap();
        assert_eq!(
            attribute(&root, "nx run-many -t build lint", spawned, exited),
            Attribution::Unattributed(UnattributedRun::Uncovered {
                targets: vec!["lint".to_owned()]
            })
        );
        assert!(matches!(
            plant(ours, "yesterday", "today"),
            Attribution::Unattributed(UnattributedRun::Unreadable { .. })
        ));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_check_spells_the_command_nx_records_and_names_its_targets() {
        let targets = |check, command| spelled_by(check, command);
        assert_eq!(
            targets(
                "direnv exec . bun nx run-many -t lint test build -p 'a,b' && echo done",
                "nx run-many -t lint test build -p a,b"
            ),
            Some(vec!["lint".into(), "test".into(), "build".into()])
        );
        assert_eq!(
            targets(
                "node node_modules/nx/bin/nx.js run-many --targets=lint,test",
                "nx run-many --targets=lint,test"
            ),
            Some(vec!["lint".into(), "test".into()])
        );
        assert_eq!(
            targets("nx run a:build; cargo test", "nx run a:build"),
            Some(vec![])
        );
        assert_eq!(targets("nx run-many -t build", "nx run-many -t lint"), None);
        assert_eq!(
            targets("nx run-many -t build -p a", "nx run-many -t build"),
            None,
            "a run of fewer arguments than the check spells is another command"
        );
        assert_eq!(targets("cargo test", "nx run-many -t build"), None);
    }

    #[test]
    fn task_inputs_read_only_stock_nxs_exact_shape_for_that_task() {
        let stock = br#"{"project":"a","target":"build","external":["npm:x"],"runtime":["node -v"],
            "environment":["HOME"],"files":["a/src/x.ts"],"depOutputs":["dist/b"]}"#;
        let inputs = task_inputs("a:build", stock).unwrap();
        assert_eq!(inputs["files"], ["a/src/x.ts"]);
        assert_eq!(inputs["depOutputs"], ["dist/b"]);
        assert_eq!(inputs.len(), 5);
        assert!(task_inputs("a:build:production", stock).is_ok());
        assert!(
            task_inputs("b:build", stock)
                .unwrap_err()
                .contains("described a:build")
        );
        let hasher =
            br#"{"project":"a","target":"build","warning":"This target uses a custom hasher."}"#;
        assert!(task_inputs("a:build", hasher).is_err());
        assert!(task_inputs("a:build", b"{}").is_err());
        let unknown = br#"{"project":"a","target":"build","external":[],"runtime":[],
            "environment":[],"files":[],"depOutputs":[],"extra":[]}"#;
        assert!(task_inputs("a:build", unknown).is_err());
        assert!(
            task_inputs(
                "a:build",
                br#"{"project":"a","target":"build","files":[{"x":1}]}"#
            )
            .is_err()
        );
    }

    #[test]
    fn daemon_records_are_discarded_and_databases_found() {
        let root = scratch("records");
        fs::create_dir_all(root.join("nx/workspace-data/d")).unwrap();
        fs::write(root.join("nx/workspace-data/d/server-process.json"), b"{}").unwrap();
        fs::write(root.join("nx/workspace-data/ABC-v3.db"), b"").unwrap();
        fs::write(root.join("nx/workspace-data/ABC-v3.db-wal"), b"").unwrap();
        assert_eq!(
            task_databases(&root, &state()).unwrap(),
            [root.join("nx/workspace-data/ABC-v3.db")]
        );
        discard_daemon_records(&root, &state()).unwrap();
        assert!(!root.join("nx/workspace-data/d").exists());
        discard_daemon_records(&root, &state()).unwrap();
        fs::remove_dir_all(&root).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn procargs_yield_the_argument_vector() {
        let mut buffer = 2i32.to_ne_bytes().to_vec();
        buffer.extend_from_slice(b"/usr/bin/node\0\0\0node\0nx.js\0HOME=/x\0");
        assert_eq!(parse_procargs(&buffer).as_deref(), Some("node nx.js"));
        assert_eq!(parse_procargs(&[1, 0]), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn an_open_database_names_its_holder_and_a_closed_one_none() {
        let root = scratch("holders");
        fs::create_dir_all(root.join("nx/workspace-data")).unwrap();
        let database = root.join("nx/workspace-data/T-v3.db");
        fs::write(&database, b"").unwrap();
        assert_eq!(holders(&database).unwrap(), []);
        assert_eq!(close(&root, &state()).unwrap(), Ok(()));
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 3<\"$0\"; read _")
            .arg(&database)
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = child.id() as i32;
        let started = std::time::Instant::now();
        let held = loop {
            let held = holders(&database).unwrap();
            if !held.is_empty() || started.elapsed() > Duration::from_secs(10) {
                break held;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        assert_eq!(
            held.iter().map(|holder| holder.pid).collect::<Vec<_>>(),
            [pid]
        );
        assert!(held[0].command.contains("sh"), "{held:?}");
        match close(&root, &state()).unwrap() {
            Err(Busy::Held { holders, .. }) => assert_eq!(holders[0].pid, pid),
            other => panic!("expected the open database to refuse: {other:?}"),
        }
        drop(child.stdin.take());
        child.wait().unwrap();
        assert_eq!(close(&root, &state()).unwrap(), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }

    /// A busy detach names what holds the volume. The holder a checkout most often has is a
    /// process whose working directory is in it -- an Nx daemon, a shell -- with no file open,
    /// which a per-file query never sees.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn a_process_whose_cwd_is_on_a_volume_is_one_of_its_holders() {
        let root = scratch("volume-holders");
        // Deeper than the queried path: only the volume, not the path itself, is in common.
        let nested = root.join("checkout/packages/tool");
        fs::create_dir_all(&nested).unwrap();
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("read _")
            .current_dir(&nested)
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = child.id() as i32;
        let started = std::time::Instant::now();
        let held = loop {
            let held = volume_holders(&root).unwrap();
            if held.iter().any(|holder| holder.pid == pid)
                || started.elapsed() > Duration::from_secs(10)
            {
                break held;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        drop(child.stdin.take());
        child.wait().unwrap();
        let ours = held.iter().find(|holder| holder.pid == pid);
        assert!(
            ours.is_some_and(|holder| holder.command.contains("sh")),
            "pid {pid} (cwd {}) is not named among the {} holders of {}'s volume",
            nested.display(),
            held.len(),
            root.display()
        );
        fs::remove_dir_all(&root).unwrap();
    }

    /// A checkout of the repository's own stock Nx, its daemon enabled, under a scratch root:
    /// whatever ends the test, the root's teardown ends every process working in it, the daemon
    /// with the rest.
    #[cfg(target_os = "macos")]
    struct StockNx {
        nx: PathBuf,
        checkout: PathBuf,
    }

    #[cfg(target_os = "macos")]
    impl StockNx {
        fn new(root: &crate::scratch_apfs::ScratchRoot) -> Self {
            // Read at run time: `env!` would compile this checkout's path into the test binary,
            // and a test binary built in one checkout must not reach another's files.
            let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
                .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
            let package = PathBuf::from(manifest)
                .join("../../../../node_modules/nx")
                .canonicalize()
                .expect("the repository's own Nx is installed");
            let checkout = root.path().to_path_buf();
            fs::write(
                checkout.join("package.json"),
                r#"{"name":"fixture","private":true}"#,
            )
            .unwrap();
            fs::write(checkout.join("nx.json"), r#"{"useDaemonProcess":true}"#).unwrap();
            fs::create_dir_all(checkout.join("node_modules")).unwrap();
            std::os::unix::fs::symlink(&package, checkout.join("node_modules/nx")).unwrap();
            Self {
                nx: package.join("dist/bin/nx.js"),
                checkout,
            }
        }

        /// Run `nx daemon <verb>` in the checkout as a host shell does, without the caller's
        /// `NX_*` or `CI`; it must exit 0. Answers what it printed.
        fn daemon(&self, verb: &str) -> String {
            self.run(&["daemon", verb])
        }

        /// Run `nx <args>` in the checkout as [`Self::daemon`] does.
        fn run(&self, args: &[&str]) -> String {
            let mut command = std::process::Command::new("node");
            command.arg(&self.nx).args(args).current_dir(&self.checkout);
            for (key, _) in std::env::vars_os() {
                if key.to_string_lossy().starts_with("NX_") || key == "CI" {
                    command.env_remove(key);
                }
            }
            let output = command
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn_locked()
                .and_then(std::process::Child::wait_with_output)
                .expect("node runs");
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.status.success(), "nx {args:?}: {text}");
            text
        }

        fn record(&self) -> PathBuf {
            self.checkout
                .join(".nx/workspace-data/d/server-process.json")
        }
    }

    /// The checkout's Nx state reached through a build link, as cowshed lays it out
    /// (`.nx/workspace-data` -> `.cowshed/build/nx/workspace-data`), and two volumes the link
    /// can name: `from`, which it names, and `into`.
    #[cfg(target_os = "macos")]
    struct Linked {
        from: PathBuf,
        into: PathBuf,
        state: BuildVolumeState,
    }

    #[cfg(target_os = "macos")]
    impl Linked {
        fn new(root: &crate::scratch_apfs::ScratchRoot, stock: &StockNx) -> Self {
            let [from, into] = ["from", "into"].map(|name| root.path().join("volumes").join(name));
            for volume in [&from, &into] {
                fs::create_dir_all(volume.join("nx/workspace-data")).unwrap();
            }
            // The volumes sit under the checkout only because the scratch root is the checkout.
            fs::write(stock.checkout.join(".nxignore"), "volumes\n").unwrap();
            fs::create_dir_all(stock.checkout.join(".cowshed")).unwrap();
            fs::create_dir_all(stock.checkout.join(".nx")).unwrap();
            std::os::unix::fs::symlink(
                "../.cowshed/build/nx/workspace-data",
                stock.checkout.join(".nx/workspace-data"),
            )
            .unwrap();
            super::super::link::point(&stock.checkout, &from).unwrap();
            Self {
                from,
                into,
                state: BuildVolumeState {
                    paths: vec![
                        BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
                    ],
                    fingerprint: None,
                },
            }
        }
    }

    /// A process that opens the checkout's task database the way every Nx process does (the
    /// native `connectToNxDb` Nx's own clients and daemon call) and keeps it open until its
    /// stdin closes, after printing `open`. The connection stays referenced, as Nx keeps its
    /// own: a collected one closes the database.
    #[cfg(target_os = "macos")]
    fn nx_opener(stock: &StockNx) -> std::process::Child {
        let native = stock
            .nx
            .parent()
            .and_then(Path::parent)
            .expect("nx.js sits in dist/bin")
            .join("src/native");
        std::process::Command::new("node")
            .arg("-e")
            .arg(
                "globalThis.db = require(process.argv[1]).connectToNxDb(process.argv[2]);\
                 process.stdout.write('open\\n'); process.stdin.resume();",
            )
            .arg(native)
            .arg(stock.checkout.join(".nx/workspace-data"))
            .current_dir(&stock.checkout)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .expect("node runs")
    }

    /// Read the opener's `open`: it opened the task database.
    #[cfg(target_os = "macos")]
    fn opened(opener: &mut std::process::Child) {
        use std::io::BufRead as _;
        let mut line = String::new();
        std::io::BufReader::new(opener.stdout.as_mut().expect("piped stdout"))
            .read_line(&mut line)
            .unwrap();
        assert_eq!(line, "open\n", "the opener opened the task database");
    }

    /// One kernel event queue: a file's write, extend or attribute change, and processes' exits.
    #[cfg(target_os = "macos")]
    struct Events {
        queue: std::os::fd::OwnedFd,
        _watched: Vec<std::os::fd::OwnedFd>,
    }

    #[cfg(target_os = "macos")]
    impl Events {
        fn new() -> Self {
            use std::os::fd::FromRawFd as _;
            // SAFETY: `kqueue` takes no arguments; a negative answer is an error.
            let queue = unsafe { libc::kqueue() };
            assert!(queue >= 0, "kqueue: {}", io::Error::last_os_error());
            Self {
                // SAFETY: `queue` is a fresh descriptor only this value owns.
                queue: unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) },
                _watched: Vec::new(),
            }
        }

        fn add(&self, ident: libc::uintptr_t, filter: i16, fflags: u32) {
            use std::os::fd::AsRawFd as _;
            let change = libc::kevent {
                ident,
                filter,
                flags: libc::EV_ADD | libc::EV_CLEAR,
                fflags,
                data: 0,
                udata: std::ptr::null_mut(),
            };
            // SAFETY: one valid change, no events requested back.
            let added = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    &change,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            };
            assert!(added == 0, "kevent: {}", io::Error::last_os_error());
        }

        /// Changes to `path`'s contents or attributes, watched without holding it open.
        fn file(&mut self, path: &Path) {
            use std::os::fd::{AsRawFd as _, FromRawFd as _};
            use std::os::unix::ffi::OsStrExt as _;
            let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
            // SAFETY: `name` is NUL-terminated; a negative answer is an error.
            let file = unsafe { libc::open(name.as_ptr(), libc::O_EVTONLY) };
            assert!(
                file >= 0,
                "open {}: {}",
                path.display(),
                io::Error::last_os_error()
            );
            // SAFETY: `file` is a fresh descriptor only this value owns.
            let file = unsafe { std::os::fd::OwnedFd::from_raw_fd(file) };
            self.add(
                file.as_raw_fd() as libc::uintptr_t,
                libc::EVFILT_VNODE,
                libc::NOTE_WRITE | libc::NOTE_EXTEND | libc::NOTE_ATTRIB,
            );
            self._watched.push(file);
        }

        fn exit(&self, pid: u32) {
            self.add(pid as libc::uintptr_t, libc::EVFILT_PROC, libc::NOTE_EXIT);
        }

        /// The filter of the next event, waiting for it.
        fn next(&self) -> i16 {
            use std::os::fd::AsRawFd as _;
            let mut event = std::mem::MaybeUninit::<libc::kevent>::zeroed();
            loop {
                // SAFETY: room for one event; no timeout.
                let ready = unsafe {
                    libc::kevent(
                        self.queue.as_raw_fd(),
                        std::ptr::null(),
                        0,
                        event.as_mut_ptr(),
                        1,
                        std::ptr::null(),
                    )
                };
                if ready == 1 {
                    // SAFETY: the kernel wrote one event.
                    return unsafe { event.assume_init() }.filter;
                }
                let error = io::Error::last_os_error();
                assert_eq!(error.kind(), io::ErrorKind::Interrupted, "kevent: {error}");
            }
        }
    }

    /// An Nx process that opens the task database while [`hold_opens`] holds its lock waits,
    /// and once the lock is let go opens the database the checkout's link names then: never the
    /// one the link named when it started opening. Without the lock it opened `from`'s and kept
    /// it after the link moved, while every later process (its next daemon included) opened
    /// `into`'s.
    #[cfg(target_os = "macos")]
    #[test]
    fn an_nx_opener_waits_out_held_opens_and_opens_the_database_the_moved_link_names() {
        let root = crate::scratch_apfs::ScratchRoot::new("nx-opens").expect("scratch root");
        let stock = StockNx::new(&root);
        let linked = Linked::new(&root, &stock);
        // Each volume holds the task database a first Nx process made there.
        for volume in [&linked.into, &linked.from] {
            super::super::link::point(&stock.checkout, volume).unwrap();
            let mut first = nx_opener(&stock);
            opened(&mut first);
            drop(first.stdin.take());
            first.wait().unwrap();
        }
        let [database]: [PathBuf; 1] = task_databases_in(&linked.from.join("nx/workspace-data"))
            .unwrap()
            .try_into()
            .expect("one task database");
        let name = database.file_name().unwrap().to_owned();
        let moved = linked.into.join("nx/workspace-data").join(&name);
        let lock = database.with_extension("lock");
        // Nx makes the lock with `File::create`, truncating it: a byte in it tells the opening.
        fs::write(&lock, b"x").unwrap();

        let mut events = Events::new();
        events.file(&lock);
        let opens = hold_opens(&linked.from, &linked.state, &linked.into)
            .unwrap()
            .expect("no Nx process is opening");
        let mut opener = nx_opener(&stock);
        events.exit(opener.id());
        assert_eq!(
            events.next(),
            libc::EVFILT_VNODE,
            "the opener reached `from`'s lock before it could exit"
        );
        super::super::link::point(&stock.checkout, &linked.into).unwrap();
        drop(opens);
        opened(&mut opener);

        let pid = opener.id() as i32;
        let on_moved = holders(&moved).unwrap();
        let on_previous = holders(&database).unwrap();
        assert!(
            on_moved.iter().any(|holder| holder.pid == pid)
                && !on_previous.iter().any(|holder| holder.pid == pid),
            "opener {pid} holds the database the link names now ({}: {on_moved:?}), never the one \
             it named when the opening began ({}: {on_previous:?})",
            moved.display(),
            database.display()
        );
        drop(opener.stdin.take());
        opener.wait().unwrap();
    }

    /// A taken open lock is an Nx process opening its database: [`hold_opens`] answers that
    /// database and the process, and holds nothing.
    #[cfg(target_os = "macos")]
    #[test]
    fn hold_opens_names_an_nx_process_that_is_opening() {
        let root = scratch("opening");
        let data = root.join("nx/workspace-data");
        fs::create_dir_all(&data).unwrap();
        let database = data.join("T-v3.db");
        fs::write(&database, b"").unwrap();
        let lock = database.with_extension("lock");
        fs::write(&lock, b"").unwrap();
        let next = scratch("opening-next");
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap()],
            fingerprint: None,
        };
        // `lockf` takes the lock as Nx does (`flock`), then runs its command.
        let mut opening = std::process::Command::new("/usr/bin/lockf")
            .args([
                "-k",
                lock.to_str().unwrap(),
                "/bin/sh",
                "-c",
                "echo held; read _",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        {
            use std::io::BufRead as _;
            let mut line = String::new();
            std::io::BufReader::new(opening.stdout.as_mut().unwrap())
                .read_line(&mut line)
                .unwrap();
            assert_eq!(line, "held\n");
        }
        let pid = opening.id() as i32;
        match hold_opens(&root, &state, &next).unwrap() {
            Err(Opening {
                database: named,
                holders,
            }) => {
                assert_eq!(named, database);
                assert!(
                    holders
                        .iter()
                        .any(|holder| holder.pid == pid
                            && holder.command.starts_with("/usr/bin/lockf")),
                    "{holders:?}"
                );
            }
            Ok(_) => panic!("a taken open lock is an opening"),
        }
        drop(opening.stdin.take());
        opening.wait().unwrap();
        drop(
            hold_opens(&root, &state, &next)
                .unwrap()
                .expect("nothing is opening"),
        );
        fs::remove_dir_all(&root).unwrap();
        fs::remove_dir_all(&next).unwrap();
    }

    /// A host daemon whose record is handed over keeps serving the checkout after its build
    /// link moves, and a client finds it there. The same move without the record ends the
    /// daemon by its own check that the record names it.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_host_daemon_whose_record_is_handed_over_serves_on_across_the_move() {
        let root = crate::scratch_apfs::ScratchRoot::new("nx-hand-over").expect("scratch root");
        let stock = StockNx::new(&root);
        let linked = Linked::new(&root, &stock);
        stock.daemon("--start");
        let daemon = crate::runtime::nx_daemon::live_pid(&stock.record()).expect("a live daemon");
        assert!(!crate::process::sandboxed(daemon).unwrap());

        hand_over_daemon_records(&linked.from, &linked.state, &linked.into).unwrap();
        super::super::link::point(&stock.checkout, &linked.into).unwrap();
        let status = stock.run(&["daemon"]);
        assert!(status.contains("currently running"), "{status}");
        assert_eq!(
            crate::runtime::nx_daemon::live_pid(&stock.record()),
            Some(daemon),
            "the daemon serves on through the moved link: {status}"
        );

        // Back to `from`, its record gone as a land leaves the landing volume's: the daemon
        // finds no record naming it and exits.
        discard_daemon_records(&linked.from, &linked.state).unwrap();
        let events = Events::new();
        events.exit(daemon as u32);
        super::super::link::point(&stock.checkout, &linked.from).unwrap();
        assert_eq!(events.next(), libc::EVFILT_PROC);
        assert!(!crate::process::running(daemon).unwrap());
    }

    /// A process `sandbox-exec` started is sandboxed. (The host daemon of the hand-over test is
    /// the unsandboxed case.)
    #[cfg(target_os = "macos")]
    #[test]
    fn a_process_under_sandbox_exec_is_sandboxed() {
        let mut sandboxed = std::process::Command::new(crate::exec::SANDBOX_EXEC)
            .args([
                "-p",
                "(version 1)(allow default)",
                "/bin/sh",
                "-c",
                "echo in; read _",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        {
            use std::io::BufRead as _;
            let mut line = String::new();
            std::io::BufReader::new(sandboxed.stdout.as_mut().unwrap())
                .read_line(&mut line)
                .unwrap();
            assert_eq!(line, "in\n");
        }
        assert!(crate::process::sandboxed(sandboxed.id() as i32).unwrap());
        drop(sandboxed.stdin.take());
        sandboxed.wait().unwrap();
    }

    /// Stopping the daemon natively is exactly stock `nx daemon --stop` (`SIGTERM` to the
    /// recorded pid): a real Nx daemon exits on it, and `nx daemon --start` afterwards starts a
    /// new one cleanly. If stock Nx ever needs more than the signal, this fails.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_stock_nx_daemon_stopped_natively_restarts_cleanly() {
        let root = crate::scratch_apfs::ScratchRoot::new("nx-daemon").expect("scratch root");
        let stock = StockNx::new(&root);
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", ".nx/workspace-data").unwrap()],
            fingerprint: None,
        };
        let record = stock.record();
        stock.daemon("--start");
        let first = crate::runtime::nx_daemon::live_pid(&record).expect("a live daemon");
        let socket = PathBuf::from(
            serde_json::from_slice::<serde_json::Value>(&fs::read(&record).unwrap()).unwrap()
                ["socketPath"]
                .as_str()
                .unwrap(),
        );
        assert!(socket.exists());
        assert_eq!(close(&stock.checkout, &state).unwrap(), Ok(()));
        // Exited, though launchd may not have reaped it yet: a zombie, which `kill(pid, 0)`
        // still reaches.
        assert!(
            !crate::process::running(first).unwrap(),
            "the daemon exited"
        );
        // The daemon's own shutdown ran: it removed its record and its socket, as after
        // `nx daemon --stop`. A signal Nx did not handle would leave both behind.
        assert!(!record.exists(), "the daemon removed its record");
        assert!(!socket.exists(), "the daemon removed its socket");
        let restarted = stock.daemon("--start");
        assert!(
            !restarted.to_lowercase().contains("stale") && !restarted.contains("EADDRINUSE"),
            "{restarted}"
        );
        let second = crate::runtime::nx_daemon::live_pid(&record).expect("a new live daemon");
        assert_ne!(first, second);
        assert_eq!(close(&stock.checkout, &state).unwrap(), Ok(()));
    }

    /// A test that fails while its stock Nx daemon runs leaves no daemon behind. The daemon
    /// detaches from the `nx daemon --start` that started it, so nothing but the fixture's own
    /// teardown ever ends it: a failed assertion between `--start` and [`close`] left it running
    /// in a deleted directory, watching files, for good.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_test_that_fails_with_a_live_nx_daemon_leaves_none_under_its_root() {
        let root = crate::scratch_apfs::ScratchRoot::new("nx-daemon-failed").expect("scratch root");
        let path = root.path().to_path_buf();
        let daemon = std::cell::Cell::new(None);
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let root = root;
            let stock = StockNx::new(&root);
            stock.daemon("--start");
            daemon.set(crate::runtime::nx_daemon::live_pid(&stock.record()));
            panic!("the test fails while its Nx daemon runs");
        }));
        assert!(failed.is_err(), "the test failed");
        let daemon = daemon
            .get()
            .expect("the daemon was live when the test failed");
        let left = crate::scratch_apfs::processes_in(|cwd| cwd.starts_with(&path))
            .expect("list the processes working under the root");
        assert_eq!(
            left,
            [],
            "daemon {daemon} or another process outlived the test"
        );
        assert!(!path.exists(), "{} was removed", path.display());
    }
}
