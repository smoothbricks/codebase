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
pub(crate) const DAEMON_EXIT_GRACE: Duration = Duration::from_secs(10);

use cowshed_os::process::{DescriptorName, Holder};

/// Processes, each as `pid <n> (<argv>)`, joined by `; `.
pub struct Holders<'a>(pub &'a [Holder]);

impl std::fmt::Display for Holders<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, holder) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{holder}")?;
        }
        Ok(())
    }
}

/// What a process holds inside a tree: the first hold the census found, cheapest first. A
/// process can hold a tree several ways at once; one is proof enough that it holds it, and
/// naming one keeps the census to one look per process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Hold {
    /// Its working directory is in the tree.
    WorkingDirectory(PathBuf),
    /// Its root directory (`chroot`) is in the tree.
    RootDirectory(PathBuf),
    /// It runs an executable from the tree.
    Executable(PathBuf),
    /// It has a file of the tree open. Event-only opens (`O_EVTONLY`) hold nothing.
    OpenFile(PathBuf),
    /// It maps a file of the tree (procfs `maps`; Darwin's census reads no mappings).
    MappedFile(PathBuf),
}

impl Hold {
    pub fn path(&self) -> &Path {
        match self {
            Self::WorkingDirectory(path)
            | Self::RootDirectory(path)
            | Self::Executable(path)
            | Self::OpenFile(path)
            | Self::MappedFile(path) => path,
        }
    }
}

impl std::fmt::Display for Hold {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkingDirectory(path) => {
                write!(formatter, "working directory {}", path.display())
            }
            Self::RootDirectory(path) => write!(formatter, "root directory {}", path.display()),
            Self::Executable(path) => write!(formatter, "executable {}", path.display()),
            Self::OpenFile(path) => write!(formatter, "open file {}", path.display()),
            Self::MappedFile(path) => write!(formatter, "mapped file {}", path.display()),
        }
    }
}

/// A process holding a tree, with the hold that shows it: `pid <n> (<argv>), <hold>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeHold {
    pub holder: Holder,
    pub hold: Hold,
}

impl std::fmt::Display for TreeHold {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}, {}", self.holder, self.hold)
    }
}

/// Every process holding anything inside the directory tree at `root` ([`tree_holds`]),
/// without what it holds.
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub fn tree_holders(root: &Path) -> io::Result<Vec<Holder>> {
    Ok(tree_holds(root)?
        .into_iter()
        .map(|held| held.holder)
        .collect())
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
        if cowshed_os::process::sandboxed(live.pid)? {
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
        && let Err(busy) = stop_live_daemon(pid)?
    {
        return Ok(Err(busy));
    }
    Ok(first_held(&databases, None)?.map_or(Ok(()), Err))
}

/// Stop the live daemon the `workspace-data` directory `data` records, as stock `nx daemon
/// --stop` does, and wait for its exit; `None` when none is live. It asks nothing of any other
/// process, so it reads no process table: an idle shed's keeper stops its daemon this way once
/// the supervisor's ledger has held no job and no session for the idle interval (04_sandbox.md,
/// "The Nx daemon stays on").
pub fn stop_recorded_daemon(data: &Path) -> io::Result<Result<Option<i32>, Busy>> {
    let Some(pid) = live_daemon(data) else {
        return Ok(Ok(None));
    };
    Ok(stop_live_daemon(pid)?.map(|()| Some(pid)))
}

/// `SIGTERM` the live daemon `pid` and wait for its exit ([`stop_daemon`]); one that outlives
/// its grace is [`Busy::DaemonStayed`].
fn stop_live_daemon(pid: i32) -> io::Result<Result<(), Busy>> {
    if stop_daemon(pid)? {
        return Ok(Ok(()));
    }
    Ok(Err(Busy::DaemonStayed {
        daemon: Holder {
            pid,
            command: command_line(pid),
        },
    }))
}

/// The first of `databases` that a process other than `daemon` holds open, with its holders:
/// one descriptor census for all of them ([`descriptor_holders`]).
fn first_held(databases: &[PathBuf], daemon: Option<i32>) -> io::Result<Option<Busy>> {
    Ok(databases
        .iter()
        .zip(descriptor_holders(databases)?)
        .find_map(|(database, holders)| {
            let holders: Vec<Holder> = holders
                .into_iter()
                .filter(|holder| Some(holder.pid) != daemon)
                .collect();
            (!holders.is_empty()).then(|| Busy::Held {
                database: database.clone(),
                holders,
            })
        }))
}

/// Whether any process holds one `workspace-data` directory's task databases open. This never
/// reads a daemon record, connects to its socket or signals a process.
#[cfg(target_os = "macos")]
pub(crate) fn held_workspace_data(data: &Path) -> io::Result<Result<(), Busy>> {
    Ok(first_held(&task_databases_in(data)?, None)?.map_or(Ok(()), Err))
}

/// Whether any process holds one of the volume's task databases open, without stopping
/// anything: the second look [`close`] takes, for a caller that has to know nothing opened one
/// since.
#[cfg(target_os = "macos")]
pub fn held(volume: &Path, state: &BuildVolumeState) -> io::Result<Result<(), Busy>> {
    for data in state.nx_workspace_data() {
        if let Err(busy) = held_workspace_data(&volume.join(data))? {
            return Ok(Err(busy));
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
/// through whatever the checkout's build link names once they are let go. Fenced (`fork_lock`):
/// Nx gets them back when this is dropped, whatever this process is spawning.
#[cfg(target_os = "macos")]
#[must_use]
pub struct Opens {
    _locks: Vec<cowshed_os::fork_lock::Fenced<fs::File>>,
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
/// the checkout's link would create in `volume` while the link still names it. For mounted
/// capture, `next` is `volume` and the returned guards stay alive through publication.
/// Answers the held locks, or the first database an Nx process is opening right now, after
/// letting go of the rest. While the locks are held, a final [`held`] check distinguishes
/// existing database holders from openers waiting until these guards are dropped.
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
                .open(&lock)
                .map(cowshed_os::fork_lock::Fenced::new)?;
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
pub(crate) fn live_daemon(data: &Path) -> Option<i32> {
    crate::runtime::nx_daemon::live_pid(&data.join(DAEMON_DIRECTORY).join(DAEMON_RECORD))
}

/// `SIGTERM` `pid` and wait for its exit on the kernel's exit event, then end the plugin workers
/// it forked, held from before the signal while they were still its children
/// ([`crate::runtime::daemon_watch`]): `true` once it has exited, `false` when it outlived
/// [`DAEMON_EXIT_GRACE`], which guards only against a hang. The daemon's own termination handler
/// removes its socket and record. A worker that could not be ended is said on stderr, and the mount
/// census names it while it holds the checkout.
#[cfg(target_os = "macos")]
fn stop_daemon(pid: i32) -> io::Result<bool> {
    let Some(daemon) = crate::runtime::job_groups::process_record(pid)? else {
        return Ok(true);
    };
    let Some(watch) = crate::runtime::daemon_watch::DaemonWatch::of(daemon.life())? else {
        return Ok(true);
    };
    // A daemon gone since the watch takes no signal, and its exit already waits on the watch.
    watch.terminate()?;
    match watch.await_exit(DAEMON_EXIT_GRACE)? {
        Ok(orphans) => {
            for said in orphans.end(DAEMON_EXIT_GRACE) {
                cowshed_os::eprintln!("cowshed: {said}");
            }
            Ok(true)
        }
        Err(_running) => Ok(false),
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
    let open = procfs_census(|process| {
        fs::read_dir(process.join("fd"))
            .is_ok_and(|descriptors| {
                descriptors.flatten().any(|descriptor| {
                    fs::read_link(descriptor.path()).is_ok_and(|link| link == file)
                })
            })
            .then_some(())
    })?;
    Ok(open.into_iter().map(|(holder, ())| holder).collect())
}

/// [`holders`] of each of `files`, in order.
#[cfg(target_os = "linux")]
fn descriptor_holders(files: &[PathBuf]) -> io::Result<Vec<Vec<Holder>>> {
    files.iter().map(|file| holders(file)).collect()
}

/// Every process with a descriptor on the inode `(device, inode)`, with those descriptors, from
/// procfs: each `/proc/<pid>/fd` entry whose file (followed, so its device and inode, not its path)
/// is that inode. A process of another user, or one that exited since the listing, shows none.
#[cfg(target_os = "linux")]
pub(crate) fn inode_holders(identity: (u64, u64)) -> io::Result<Vec<(Holder, Vec<i32>)>> {
    use std::os::unix::fs::MetadataExt;
    procfs_census(|process| {
        let descriptors: Vec<i32> = fs::read_dir(process.join("fd"))
            .ok()?
            .flatten()
            .filter(|descriptor| {
                fs::metadata(descriptor.path())
                    .is_ok_and(|file| (file.dev(), file.ino()) == identity)
            })
            .filter_map(|descriptor| descriptor.file_name().to_str()?.parse().ok())
            .collect();
        (!descriptors.is_empty()).then_some(descriptors)
    })
}

/// Every process holding anything on the volume mounted at `mount` -- an open file, a mapped
/// file, its working directory, its root, its executable -- which is what keeps `umount`
/// answering `EBUSY`. From procfs, so a process of another user, or one that exited since the
/// listing, shows none.
#[cfg(target_os = "linux")]
pub fn volume_holders(mount: &Path) -> io::Result<Vec<Holder>> {
    tree_holders(mount)
}

/// Every process holding anything inside the directory tree at `root` -- its working directory,
/// its root, its executable, an open file, a mapped file, checked in that order -- whether or not
/// `root` is a volume's mount point, each with the first hold found. A plain checkout's holders,
/// which `volume_holders` cannot name without naming its whole volume. From procfs, so a process
/// of another user, or one that exited since the listing, shows none. `root` is canonicalized
/// first, so a symlinked path names the tree its target does; a missing `root` has none.
#[cfg(target_os = "linux")]
pub fn tree_holds(root: &Path) -> io::Result<Vec<TreeHold>> {
    let root = match fs::canonicalize(root) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let under = |link: PathBuf| link.starts_with(&root).then_some(link);
    let holds = procfs_census(|process| {
        let link = |name: &str| fs::read_link(process.join(name)).ok().and_then(under);
        link("cwd")
            .map(Hold::WorkingDirectory)
            .or_else(|| link("root").map(Hold::RootDirectory))
            .or_else(|| link("exe").map(Hold::Executable))
            .or_else(|| {
                fs::read_dir(process.join("fd"))
                    .ok()?
                    .flatten()
                    .find_map(|descriptor| fs::read_link(descriptor.path()).ok().and_then(under))
                    .map(Hold::OpenFile)
            })
            // A mapping's path is its line's last field and the line's first `/`: the
            // address range, permissions, offset, device and inode before it hold none.
            .or_else(|| {
                fs::read_to_string(process.join("maps"))
                    .ok()?
                    .lines()
                    .find_map(|mapping| under(PathBuf::from(&mapping[mapping.find('/')?..])))
                    .map(Hold::MappedFile)
            })
    })?;
    Ok(holds
        .into_iter()
        .map(|(holder, hold)| TreeHold { holder, hold })
        .collect())
}

/// Process-table walks this process has begun: every listing of all pids a holder census starts
/// from ([`census_pids`] on macOS, a `/proc` read on Linux). Never on an interval and never in a
/// supervisor's actor (02_workspaces.md, "Holders cowshed does not run"), but for the one exception
/// that section admits: a land-turn head naming who has its outside lock open, once per 90 s report
/// (`land_turn::take`). Tests read the count through [`process_table_walks`].
static PROCESS_TABLE_WALKS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// How many process-table walks this process has begun ([`PROCESS_TABLE_WALKS`]).
pub fn process_table_walks() -> u64 {
    PROCESS_TABLE_WALKS.load(std::sync::atomic::Ordering::Relaxed)
}

fn walk_begins() {
    PROCESS_TABLE_WALKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Every process under `/proc` that `held` names something for, given its `/proc/<pid>`
/// directory, with what it named.
#[cfg(target_os = "linux")]
fn procfs_census<T>(held: impl Fn(&Path) -> Option<T>) -> io::Result<Vec<(Holder, T)>> {
    let mut holders = Vec::new();
    for pid in all_pids()? {
        if let Some(hold) = held(Path::new(&format!("/proc/{pid}"))) {
            holders.push((
                Holder {
                    pid,
                    command: command_line(pid),
                },
                hold,
            ));
        }
    }
    Ok(holders)
}

/// Every pid procfs lists now: the one walker every Linux census starts from, counted once per
/// census ([`walk_begins`]).
#[cfg(target_os = "linux")]
fn all_pids() -> io::Result<Vec<libc::pid_t>> {
    walk_begins();
    let mut pids = Vec::new();
    for process in fs::read_dir("/proc")? {
        if let Some(pid) = process?
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
        {
            pids.push(pid);
        }
    }
    Ok(pids)
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

/// Every process with a descriptor open on the file at `path` ([`descriptor_holders`]). A missing
/// file has none.
#[cfg(target_os = "macos")]
pub fn holders(path: &Path) -> io::Result<Vec<Holder>> {
    Ok(descriptor_holders(&[path.to_owned()])?
        .into_iter()
        .flatten()
        .collect())
}

/// The processes with a descriptor open on each of `files`, in order, from one pass over the
/// processes a census asks about ([`census_pids`]): each vnode descriptor's device and inode
/// (`PROC_PIDFDVNODEINFO`) against each file's. Event-only opens hold nothing. A missing file has
/// no holder. No mapped region is walked: a database or lock is only ever held through a
/// descriptor.
#[cfg(target_os = "macos")]
fn descriptor_holders(files: &[PathBuf]) -> io::Result<Vec<Vec<Holder>>> {
    use std::os::unix::fs::MetadataExt;
    let mut identities = Vec::with_capacity(files.len());
    for file in files {
        identities.push(match fs::metadata(file) {
            Ok(metadata) => Some((metadata.dev(), metadata.ino())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        });
    }
    let mut held = vec![Vec::new(); files.len()];
    if identities.iter().all(Option::is_none) {
        return Ok(held);
    }
    let vinfo_size = c_size::<VnodeFdinfo>()?;
    for pid in census_pids()? {
        let mut holds = vec![false; files.len()];
        for fd in vnode_descriptors(pid)? {
            let Some(identity) = vnode_fd_identity(pid, fd, vinfo_size) else {
                continue;
            };
            for (hold, file) in holds.iter_mut().zip(&identities) {
                *hold |= *file == Some(identity);
            }
        }
        if holds.contains(&true) {
            let command = command_line(pid);
            for (holders, _) in held.iter_mut().zip(holds).filter(|(_, hold)| *hold) {
                holders.push(Holder {
                    pid,
                    command: command.clone(),
                });
            }
        }
    }
    Ok(held)
}

/// Every process with a vnode descriptor on the inode `(device, inode)`, with those descriptors:
/// each process [`census_pids`] asks about, each vnode descriptor's device and inode
/// (`PROC_PIDFDVNODEINFO`). Event-only opens hold nothing. A process a cowshed lifecycle operation
/// runs in is not asked: a fork's first mount of its new image holds that image's vnode for up to
/// minutes, and every per-descriptor query of the fork waits that long (measured 2026-10-25: 245
/// extent-map copies of 50-221 s, the `image-lock wait` holder line and `lsof` stalled behind them).
/// A lock's custody takes such a holder from the lock's own record instead
/// (`held_lock::kernel_view`).
#[cfg(target_os = "macos")]
pub(crate) fn inode_holders(identity: (u64, u64)) -> io::Result<Vec<(Holder, Vec<i32>)>> {
    let vinfo_size = c_size::<VnodeFdinfo>()?;
    let mut holders = Vec::new();
    for pid in census_pids()? {
        let descriptors: Vec<i32> = vnode_descriptors(pid)?
            .into_iter()
            .filter(|&fd| vnode_fd_identity(pid, fd, vinfo_size) == Some(identity))
            .collect();
        if !descriptors.is_empty() {
            holders.push((
                Holder {
                    pid,
                    command: command_line(pid),
                },
                descriptors,
            ));
        }
    }
    Ok(holders)
}

/// One process holding a kernel object a census asked about ([`object_holders`]): the process,
/// the kernel's identity of its life ([`crate::runtime::job_groups::life_of`]), and each of its
/// descriptors on one of the objects, in descriptor order, with the caller's key for that object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectHold<K> {
    pub holder: Holder,
    pub life: u64,
    pub descriptors: Vec<(i32, K)>,
}

/// What a census of descriptor holders found ([`object_holders`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectCensus<K> {
    /// Every process it read that holds one of the objects, in pid listing order.
    pub holds: Vec<ObjectHold<K>>,
    /// The processes whose descriptors were not read: those the kernel withheld from this user
    /// (another user's, unless the census runs as root) and, on Darwin, those a cowshed lifecycle
    /// operation runs in, which the census never asks. Any of them may hold an object, so they are
    /// counted, never reported as holding none.
    pub uninspected: u32,
}

/// Every process with a descriptor on one of `objects`, each named as
/// [`cowshed_os::process::descriptor_name`] names it (lsof's `d` or `i`), with the caller's key
/// for it. One pass over every live pid the kernel lists ([`all_pids`]), reading only the names the
/// objects need ([`NameKinds`]). A process found holding one is confirmed: its life is read, its
/// descriptors read again, then its life again, so the descriptors reported are that life's and
/// never a later process's given the pid. A process that exits during the census holds nothing;
/// one whose descriptors or life the kernel withholds is counted in `uninspected`, and so, on
/// Darwin, is one a cowshed lifecycle operation runs in, whose descriptors are never read: a
/// fork's first mount of its new image holds that vnode for minutes ([`inode_holders`]).
#[cfg(any(target_os = "macos", target_os = "linux"))]
pub fn object_holders<K: Copy>(objects: &[(DescriptorName, K)]) -> io::Result<ObjectCensus<K>> {
    use crate::runtime::job_groups::life_of;
    use cowshed_os::process::{NameKinds, Unread, descriptor_names};

    let kinds = NameKinds {
        devices: objects
            .iter()
            .any(|(name, _)| matches!(name, DescriptorName::Device(_))),
        inodes: objects
            .iter()
            .any(|(name, _)| matches!(name, DescriptorName::Inode(_))),
    };
    let on_objects = |pid| -> io::Result<Result<Vec<(i32, K)>, Unread>> {
        Ok(descriptor_names(pid, kinds)?.map(|named| {
            named
                .into_iter()
                .filter_map(|(descriptor, name)| {
                    objects
                        .iter()
                        .find(|(object, _)| *object == name)
                        .map(|(_, key)| (descriptor, *key))
                })
                .collect()
        }))
    };
    let life = |pid| -> io::Result<Result<Option<u64>, Unread>> {
        match life_of(pid) {
            Ok(life) => Ok(Ok(life)),
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                Ok(Err(Unread::Withheld))
            }
            Err(error) => Err(io::Error::new(
                error.kind(),
                format!("the life of process {pid} cannot be read: {error}"),
            )),
        }
    };
    let mut holds = Vec::new();
    let mut uninspected = 0_usize;
    let mut unread = |unread: Unread| uninspected += usize::from(unread == Unread::Withheld);
    #[cfg(target_os = "macos")]
    let lifecycle = crate::lease_holder::running_lifecycle();
    #[cfg_attr(not(target_os = "macos"), allow(unused_mut))]
    let mut unasked = 0_usize;
    for pid in all_pids()? {
        #[cfg(target_os = "macos")]
        if lifecycle.contains(&pid) {
            unasked += 1;
            continue;
        }
        match on_objects(pid)? {
            Ok(held) if held.is_empty() => continue,
            Ok(_) => {}
            Err(why) => {
                unread(why);
                continue;
            }
        }
        let before = match life(pid)? {
            Ok(Some(before)) => before,
            Ok(None) => continue,
            Err(why) => {
                unread(why);
                continue;
            }
        };
        let descriptors = match on_objects(pid)? {
            Ok(descriptors) => descriptors,
            Err(why) => {
                unread(why);
                continue;
            }
        };
        match life(pid)? {
            Ok(after) if after == Some(before) && !descriptors.is_empty() => {}
            Ok(_) => continue,
            Err(why) => {
                unread(why);
                continue;
            }
        }
        holds.push(ObjectHold {
            holder: Holder {
                pid,
                command: command_line(pid),
            },
            life: before,
            descriptors,
        });
    }
    Ok(ObjectCensus {
        holds,
        uninspected: u32::try_from(uninspected + unasked).map_err(io::Error::other)?,
    })
}

/// Every process holding anything on the volume mounted at `mount` -- its working or root
/// Every process holding anything on the volume mounted at `mount` -- its working or root
/// directory, a thread's working directory, an open file, its executable or another mapped file --
/// which is what keeps `umount` answering `Resource busy`. Event-only opens (FSEvents, Spotlight)
/// never block an unmount and are left out. It is the walk libproc's `proc_listpidspath` makes
/// with `PROC_LISTPIDSPATH_PATH_IS_VOLUME | PROC_LISTPIDSPATH_EXCLUDE_EVTONLY`, by device, over the
/// processes a census asks about ([`census_pids`]) rather than libproc's own listing, which asks
/// every process, a lifecycle operation's included ([`on_volume`]). A missing mount has none.
#[cfg(target_os = "macos")]
pub fn volume_holders(mount: &Path) -> io::Result<Vec<Holder>> {
    use std::os::unix::fs::MetadataExt;
    let device = match fs::metadata(mount) {
        Ok(metadata) => metadata.dev(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut holders = Vec::new();
    for pid in census_pids()? {
        if on_volume(pid, device)? {
            holders.push(Holder {
                pid,
                command: command_line(pid),
            });
        }
    }
    Ok(holders)
}

/// Whether `pid` holds anything on the volume of `device`, asked as `proc_listpidspath` asks it
/// (xnu `libsyscall/wrappers/libproc/proc_listpidspath.c`): its working and root directory
/// (`PROC_PIDVNODEPATHINFO`), its vnode descriptors ([`vnode_fd_identity`]), the working directory
/// of each thread that has its own (only for a process the kernel flags `PROC_FLAG_THCWD`), then
/// the first mapped region on the device, its executable's text included (one call,
/// `PROC_PIDREGIONPATHINFO3`). A process that exited or is withheld from this user holds none.
#[cfg(target_os = "macos")]
fn on_volume(pid: libc::pid_t, device: u64) -> io::Result<bool> {
    let on = |stat: &libc::vinfo_stat| u64::from(stat.vst_dev) == device;
    let size = c_size::<libc::proc_vnodepathinfo>()?;
    // SAFETY: `proc_vnodepathinfo` is plain C data, for which all zeros is a valid value; it is
    // read only when the kernel fills all of it.
    let mut directories: libc::proc_vnodepathinfo = unsafe { std::mem::zeroed() };
    // SAFETY: `directories` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            std::ptr::from_mut(&mut directories).cast(),
            size,
        )
    };
    if filled == size
        && (on(&directories.pvi_cdir.vip_vi.vi_stat) || on(&directories.pvi_rdir.vip_vi.vi_stat))
    {
        return Ok(true);
    }
    let vinfo_size = c_size::<VnodeFdinfo>()?;
    if vnode_descriptors(pid)?
        .into_iter()
        .any(|fd| vnode_fd_identity(pid, fd, vinfo_size).is_some_and(|(dev, _)| dev == device))
    {
        return Ok(true);
    }
    if thread_directories(pid)?.iter().any(on) {
        return Ok(true);
    }
    // The private flavor `proc_listpidspath` asks for the first region whose file is on the device
    // its argument names (xnu `bsd/sys/proc_info_private.h`).
    const PROC_PIDREGIONPATHINFO3: libc::c_int = 23;
    let size = c_size::<RegionWithPath>()?;
    // SAFETY: `RegionWithPath` is plain C data, for which all zeros is a valid value; it is read
    // only when the kernel fills all of it.
    let mut region: RegionWithPath = unsafe { std::mem::zeroed() };
    // SAFETY: `region` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            PROC_PIDREGIONPATHINFO3,
            device,
            std::ptr::from_mut(&mut region).cast(),
            size,
        )
    };
    Ok(filled == size && on(&region.prp_vip.vip_vi.vi_stat))
}

/// `sys/proc_info.h`'s `struct proc_regionwithpathinfo`, which libc 0.2 does not bind: the 96-byte
/// `proc_regioninfo` (four `uint32_t`, a `uint64_t`, fourteen `uint32_t`, two `uint64_t`), then the
/// region's file.
#[cfg(target_os = "macos")]
#[repr(C)]
struct RegionWithPath {
    _prp_prinfo: [u64; 12],
    prp_vip: libc::vnode_info_path,
}

/// The attributes of each thread working directory `pid`'s threads set of their own
/// (`pthread_chdir_np`): none unless the kernel flags the process `PROC_FLAG_THCWD`, then one
/// listing (`PROC_PIDLISTTHREADS`) and one call per thread (`PROC_PIDTHREADPATHINFO`).
#[cfg(target_os = "macos")]
fn thread_directories(pid: libc::pid_t) -> io::Result<Vec<libc::vinfo_stat>> {
    // `sys/proc_info.h`; libc 0.2 binds none of the three.
    const PROC_FLAG_THCWD: u32 = 0x100;
    const PROC_PIDLISTTHREADS: libc::c_int = 6;
    const PROC_PIDTHREADPATHINFO: libc::c_int = 10;
    let size = c_size::<libc::proc_bsdinfo>()?;
    // SAFETY: `proc_bsdinfo` is plain C data, for which all zeros is a valid value; it is read only
    // when the kernel fills all of it.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if filled != size || info.pbi_flags & PROC_FLAG_THCWD == 0 {
        return Ok(Vec::new());
    }
    let mut capacity = 64usize;
    let threads = loop {
        let mut threads = vec![0u64; capacity];
        let bytes = libc::c_int::try_from(capacity * size_of::<u64>()).map_err(io::Error::other)?;
        // SAFETY: `threads` is writable for `bytes` bytes, its whole length.
        let filled = unsafe {
            libc::proc_pidinfo(
                pid,
                PROC_PIDLISTTHREADS,
                0,
                threads.as_mut_ptr().cast(),
                bytes,
            )
        };
        let Ok(filled) = usize::try_from(filled) else {
            return Ok(Vec::new());
        };
        let count = filled / size_of::<u64>();
        // A listing that filled the buffer may have been cut short: grow it and list again.
        if count >= capacity {
            capacity *= 4;
            continue;
        }
        threads.truncate(count);
        break threads;
    };
    let size = c_size::<ThreadWithPath>()?;
    let mut directories = Vec::new();
    for thread in threads {
        // SAFETY: `ThreadWithPath` is plain C data, for which all zeros is a valid value; it is
        // read only when the kernel fills all of it.
        let mut info: ThreadWithPath = unsafe { std::mem::zeroed() };
        // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
        let filled = unsafe {
            libc::proc_pidinfo(
                pid,
                PROC_PIDTHREADPATHINFO,
                thread,
                std::ptr::from_mut(&mut info).cast(),
                size,
            )
        };
        if filled == size {
            directories.push(info.pvip.vip_vi.vi_stat);
        }
    }
    Ok(directories)
}

/// `sys/proc_info.h`'s `struct proc_threadwithpathinfo`, which libc 0.2 does not bind: a thread's
/// `proc_threadinfo`, then its own working directory.
#[cfg(target_os = "macos")]
#[repr(C)]
struct ThreadWithPath {
    _pt: libc::proc_threadinfo,
    pvip: libc::vnode_info_path,
}

/// Every process holding anything inside the directory tree at `root` -- its working directory
/// or root directory, its executable, an open file -- whether or not `root` is a volume's mount
/// point, each with the first hold found ([`hold_in_tree`]). A plain checkout's holders, which
/// `volume_holders` cannot name without naming its whole volume. A process the kernel withholds
/// from this user, or that exited since the listing, shows none. A missing `root` has none.
#[cfg(target_os = "macos")]
pub fn tree_holds(root: &Path) -> io::Result<Vec<TreeHold>> {
    let root = match fs::canonicalize(root) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut holds = Vec::new();
    for pid in census_pids()? {
        if let Some(hold) = hold_in_tree(pid, &root)? {
            holds.push(TreeHold {
                holder: Holder {
                    pid,
                    command: command_line(pid),
                },
                hold,
            });
        }
    }
    Ok(holds)
}

/// What `pid` holds inside `root`, on `device`, if anything. Cheapest first: the directories
/// cost one call, the executable one more, and the descriptors a listing plus one call per vnode.
#[cfg(target_os = "macos")]
fn hold_in_tree(pid: libc::pid_t, root: &Path) -> io::Result<Option<Hold>> {
    if let Some(hold) = directories(pid)?
        .into_iter()
        .find(|hold| hold.path().starts_with(root))
    {
        return Ok(Some(hold));
    }
    if let Some(executable) = executable(pid).filter(|path| path.starts_with(root)) {
        return Ok(Some(Hold::Executable(executable)));
    }
    Ok(open_vnodes(pid)?
        .into_iter()
        .find(|path| path.starts_with(root))
        .map(Hold::OpenFile))
}

/// `pid`'s working directory and root directory (`PROC_PIDVNODEPATHINFO`). A process the kernel
/// withholds from this user, or that exited, names none.
#[cfg(target_os = "macos")]
fn directories(pid: libc::pid_t) -> io::Result<Vec<Hold>> {
    let size = c_size::<libc::proc_vnodepathinfo>()?;
    // SAFETY: `proc_vnodepathinfo` is plain C data (integers and byte arrays), for which all
    // zeros is a valid value; it is read only when the kernel fills all of it, as checked below.
    let mut info: libc::proc_vnodepathinfo = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if filled != size {
        return Ok(Vec::new());
    }
    Ok([
        c_path(info.pvi_cdir.vip_path.as_flattened()).map(Hold::WorkingDirectory),
        c_path(info.pvi_rdir.vip_path.as_flattened()).map(Hold::RootDirectory),
    ]
    .into_iter()
    .flatten()
    .collect())
}

/// The path of each open vnode descriptor of `pid` ([`vnode_descriptors`]), then one
/// `PROC_PIDFDVNODEPATHINFO` call per vnode. A descriptor closed since the listing names none, and
/// an event-only open (`O_EVTONLY`: FSEvents, Spotlight, a file watcher) holds nothing, as
/// `volume_holders` leaves it out.
#[cfg(target_os = "macos")]
fn open_vnodes(pid: libc::pid_t) -> io::Result<Vec<PathBuf>> {
    let vinfo_size = c_size::<VnodeFdinfoWithPath>()?;
    Ok(vnode_descriptors(pid)?
        .into_iter()
        .filter_map(|fd| vnode_fd_path(pid, fd, vinfo_size))
        .collect())
}

/// `pid`'s vnode descriptors: one listing (`PROC_PIDLISTFDS`). A process that exited or is
/// withheld from this user has none.
#[cfg(target_os = "macos")]
fn vnode_descriptors(pid: libc::pid_t) -> io::Result<Vec<libc::c_int>> {
    // Descriptors opened between the sizing call and the listing still fit.
    const HEADROOM: usize = 32;
    let entry = size_of::<libc::proc_fdinfo>();
    // SAFETY: a null buffer of zero bytes asks only how many bytes the listing needs.
    let needed =
        unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDLISTFDS, 0, std::ptr::null_mut(), 0) };
    // Negative: the process exited or is withheld from this user, so it holds no descriptor here.
    let Ok(needed) = usize::try_from(needed) else {
        return Ok(Vec::new());
    };
    let mut capacity = needed / entry + HEADROOM;
    loop {
        let mut fds = vec![
            libc::proc_fdinfo {
                proc_fd: 0,
                proc_fdtype: 0
            };
            capacity
        ];
        let bytes = libc::c_int::try_from(capacity * entry).map_err(io::Error::other)?;
        // SAFETY: `fds` is writable for `bytes` bytes, its whole length.
        let filled = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDLISTFDS,
                0,
                fds.as_mut_ptr().cast(),
                bytes,
            )
        };
        let Ok(filled) = usize::try_from(filled) else {
            return Ok(Vec::new());
        };
        let count = filled / entry;
        // A listing that filled the buffer may have been cut short: grow it and list again.
        if count >= capacity {
            capacity *= 4;
            continue;
        }
        fds.truncate(count);
        return Ok(fds
            .into_iter()
            .filter(|fd| {
                libc::c_int::try_from(fd.proc_fdtype)
                    .is_ok_and(|kind| kind == libc::PROX_FDTYPE_VNODE)
            })
            .map(|fd| fd.proc_fd)
            .collect());
    }
}

/// The device and inode of vnode descriptor `fd` of `pid` (`PROC_PIDFDVNODEINFO`), or `None` if
/// the descriptor closed since its listing or is an event-only open. `size` is `VnodeFdinfo`'s
/// size: the kernel fills exactly that or nothing.
#[cfg(target_os = "macos")]
fn vnode_fd_identity(pid: libc::pid_t, fd: libc::c_int, size: libc::c_int) -> Option<(u64, u64)> {
    // libc 0.2 binds neither this flavor nor `vnode_fdinfo`; `sys/proc_info.h` defines the
    // flavor as 1.
    const PROC_PIDFDVNODEINFO: libc::c_int = 1;
    let mut info = VnodeFdinfo {
        pfi_openflags: 0,
        _pfi_rest: [0; 20],
        // SAFETY: `vnode_info` is plain C data (integers), for which all zeros is a valid value;
        // it is read only when the kernel fills all of it, as checked below.
        pvi: unsafe { std::mem::zeroed() },
    };
    // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidfdinfo(
            pid,
            fd,
            PROC_PIDFDVNODEINFO,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if filled != size || info.pfi_openflags & O_EVTONLY != 0 {
        return None;
    }
    Some((
        u64::from(info.pvi.vi_stat.vst_dev),
        info.pvi.vi_stat.vst_ino,
    ))
}

/// `sys/proc_info.h`'s `struct vnode_fdinfo`, which libc 0.2 does not bind: the 24-byte
/// `proc_fileinfo` head [`VnodeFdinfoWithPath`] also has, then the vnode's attributes.
#[cfg(target_os = "macos")]
#[repr(C)]
struct VnodeFdinfo {
    pfi_openflags: u32,
    _pfi_rest: [u8; 20],
    pvi: libc::vnode_info,
}

/// The path of vnode descriptor `fd` of `pid` (`PROC_PIDFDVNODEPATHINFO`), or `None` if the kernel
/// names none or the descriptor is an event-only open. `size` is `VnodeFdinfoWithPath`'s size:
/// the kernel fills exactly that or nothing.
#[cfg(target_os = "macos")]
fn vnode_fd_path(pid: libc::pid_t, fd: libc::c_int, size: libc::c_int) -> Option<PathBuf> {
    // libc 0.2 binds neither this flavor nor `vnode_fdinfowithpath`; `sys/proc_info.h` defines
    // the flavor as 2.
    const PROC_PIDFDVNODEPATHINFO: libc::c_int = 2;
    let mut info = VnodeFdinfoWithPath {
        pfi_openflags: 0,
        _pfi_rest: [0; 20],
        // SAFETY: `vnode_info_path` is plain C data (integers and byte arrays), for which all
        // zeros is a valid value; it is read only when the kernel fills all of it, as checked.
        pvip: unsafe { std::mem::zeroed() },
    };
    // SAFETY: `info` is writable for exactly `size` bytes, its whole size.
    let filled = unsafe {
        libc::proc_pidfdinfo(
            pid,
            fd,
            PROC_PIDFDVNODEPATHINFO,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    if filled != size || info.pfi_openflags & O_EVTONLY != 0 {
        return None;
    }
    c_path(info.pvip.vip_path.as_flattened())
}

/// `sys/proc_info.h`'s `struct vnode_fdinfowithpath`, which libc 0.2 does not bind: a 24-byte
/// `proc_fileinfo` head whose first field is the descriptor's open flags, then the vnode's path.
#[cfg(target_os = "macos")]
#[repr(C)]
struct VnodeFdinfoWithPath {
    pfi_openflags: u32,
    _pfi_rest: [u8; 20],
    pvip: libc::vnode_info_path,
}

/// `fcntl.h`'s `O_EVTONLY` as `proc_fileinfo.fi_openflags` carries it: an open for event
/// notification only, which holds no data and never blocks an unmount.
#[cfg(target_os = "macos")]
const O_EVTONLY: u32 = 0x0000_8000;

/// The processes a census asks about: every live pid but those a cowshed lifecycle operation runs
/// in, as its lease records them ([`crate::lease_holder::running_lifecycle`]). Such a process can
/// hold one of its descriptors' vnodes in one kernel call for a minute -- a fork's read-write mount
/// of a new clone, which pays the clone's extent-map copy -- and every per-descriptor query of it waits that long; the
/// operation's own leases and image locks already refuse what a census guards. The one walker
/// every macOS census starts from, counted once per census ([`walk_begins`]).
#[cfg(target_os = "macos")]
fn census_pids() -> io::Result<Vec<libc::pid_t>> {
    let running = crate::lease_holder::running_lifecycle();
    Ok(all_pids()?
        .into_iter()
        .filter(|pid| !running.contains(pid))
        .collect())
}

/// Every live pid the kernel lists (`proc_listallpids`, which answers a count of pids, not bytes),
/// except pid 0. The listing grows until it fits.
#[cfg(target_os = "macos")]
fn all_pids() -> io::Result<Vec<libc::pid_t>> {
    walk_begins();
    let mut capacity = 256usize;
    loop {
        let mut pids: Vec<libc::pid_t> = vec![0; capacity];
        let bytes =
            libc::c_int::try_from(capacity * size_of::<libc::pid_t>()).map_err(io::Error::other)?;
        // SAFETY: `pids` is writable for `bytes` bytes, its whole length.
        let filled = unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), bytes) };
        if filled < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = usize::try_from(filled).map_err(io::Error::other)?;
        if count < capacity {
            pids.truncate(count);
            return Ok(pids.into_iter().filter(|&pid| pid > 0).collect());
        }
        capacity *= 4;
    }
}

/// `T`'s byte size as the `c_int` the kernel's buffer arguments take.
#[cfg(target_os = "macos")]
fn c_size<T>() -> io::Result<libc::c_int> {
    libc::c_int::try_from(size_of::<T>()).map_err(io::Error::other)
}

/// The path a NUL-terminated `c_char` buffer holds, or `None` if it holds none. `c_char` is signed
/// on x86-64 Apple and unsigned on arm64 Apple; `to_ne_bytes` reads either as the same byte.
#[cfg(target_os = "macos")]
fn c_path(bytes: &[libc::c_char]) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    let path: Vec<u8> = bytes
        .iter()
        .take_while(|&&byte| byte != 0)
        .map(|&byte| u8::from_ne_bytes(byte.to_ne_bytes()))
        .collect();
    if path.is_empty() {
        return None;
    }
    Some(PathBuf::from(std::ffi::OsString::from_vec(path)))
}

/// `pid`'s executable path (`proc_pidpath`), or `None` if the kernel names none.
#[cfg(target_os = "macos")]
fn executable(pid: libc::pid_t) -> Option<PathBuf> {
    let mut path: Vec<libc::c_char> =
        vec![0; usize::try_from(libc::PROC_PIDPATHINFO_MAXSIZE).ok()?];
    let capacity = u32::try_from(path.len()).ok()?;
    // SAFETY: `path` is writable for `capacity` bytes, its whole length.
    let length = unsafe { libc::proc_pidpath(pid, path.as_mut_ptr().cast(), capacity) };
    // A negative length is the kernel naming nothing: the conversion fails and `None` follows.
    path.truncate(usize::try_from(length).ok()?);
    c_path(&path)
}

/// `pid`'s argument vector joined by spaces (`KERN_PROCARGS2`), or its executable path, or a
/// placeholder naming why neither could be read.
#[cfg(target_os = "macos")]
fn command_line(pid: libc::pid_t) -> String {
    if let Ok(arguments) = cowshed_os::process::process_arguments(pid)
        && !arguments.argv.is_empty()
    {
        return arguments
            .argv
            .iter()
            .map(|argument| String::from_utf8_lossy(argument))
            .collect::<Vec<_>>()
            .join(" ");
    }
    executable(pid).map_or_else(
        || "command unreadable".to_owned(),
        |path| path.display().to_string(),
    )
}

/// Take the opened file's lock once the kernel grants it, with no deadline of its own: a wait
/// that never returns is ended by the test runner's no-progress bound (08_testing.md, "Every
/// test target ... is bounded by progress alone"). First it says on stderr what the wait is for
/// and every process with a descriptor on the inode, by pid, command and descriptors, so the
/// run that bound ends names the process that kept the lock. A census that cannot be read is
/// said as such, and the wait is still taken.
#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
pub(crate) fn hold_announced(
    opened: crate::held_lock::Opened,
    awaited: &str,
) -> io::Result<crate::held_lock::HeldLock> {
    let inode = opened.inode();
    let own = std::process::id();
    let holders = match inode_holders((inode.dev.get(), inode.ino.get())) {
        Ok(holders) => holders
            .iter()
            .map(|(holder, descriptors)| {
                let waiter = if u32::try_from(holder.pid).is_ok_and(|pid| pid == own) {
                    " (this waiter)"
                } else {
                    ""
                };
                format!("{holder} on descriptors {descriptors:?}{waiter}")
            })
            .collect::<Vec<_>>()
            .join("; "),
        Err(error) => format!("unread: {error}"),
    };
    cowshed_os::eprintln!(
        "waiting for {awaited} to let go of {} (inode {inode}); its holders now: [{holders}]",
        opened.path().display()
    );
    opened.hold()
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
/// (16_build_volumes.md, Land step 7 source capture). Stock Nx writes it once, at the end of a run, and
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
pub(crate) fn spelled_by(check: &str, command: &str) -> Option<Vec<String>> {
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

/// A successful check's attributed Nx run, kept only for this land's bounded restoration.
#[derive(serde::Serialize)]
pub struct CheckedRun {
    pub check: String,
    pub cache: PathBuf,
    pub command: String,
    pub tasks: std::collections::BTreeMap<String, String>,
}

/// The cache-only helper never executes a task, including on a missing or uncacheable result.
pub const RESTORE_OUTPUTS_SCRIPT: &str = include_str!("restore_outputs.js");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoredOutputs {
    pub hits: u64,
    pub misses: Vec<OutputMiss>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputMiss {
    pub task: String,
    pub hash: String,
    pub reason: String,
}

/// A land's publish (05_gateway.md "`/nx/v1/cache/{hash}`"): `node - <root> <JSON hashes>` with
/// this on stdin, run as a job of the landing workspace, stores each named task result of that
/// checkout's Nx cache to the project's store (`nx_publish.js` says how).
pub fn publish_script() -> bytes::Bytes {
    static SCRIPT: std::sync::LazyLock<bytes::Bytes> =
        std::sync::LazyLock::new(|| super::cache_adapter::script(include_str!("nx_publish.js")));
    SCRIPT.clone()
}

/// What one publish stored, found already stored, and skipped: a task Nx does not cache, or one
/// whose result failed, has no entry to share.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Published {
    pub published: u64,
    pub existing: u64,
    pub skipped: u64,
}

impl std::fmt::Display for Published {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} stored, {} already stored, {} not cacheable",
            self.published, self.existing, self.skipped
        )
    }
}

/// The count [`publish_script`] printed; anything else is an error naming what was read.
pub fn published(stdout: &[u8]) -> Result<Published, String> {
    serde_json::from_slice(stdout).map_err(|error| {
        format!(
            "the publish printed no count ({error}): {}",
            String::from_utf8_lossy(stdout)
        )
    })
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
    use cowshed_os::fork_lock::Spawn as _;
    use cowshed_os::test_backstop::ChildBackstop as _;
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::UnixStreamBackstop as _;

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

    /// Read a holder child's `ready`: it says so only after the descriptor it holds is
    /// open or the directory it works in is its own, so the census that follows reads a
    /// settled fact once. The child's exit closes the pipe, which the empty line names.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    fn holder_ready(child: &mut std::process::Child) {
        use std::io::BufRead as _;
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.as_mut().expect("piped stdout"))
            .read_line(&mut line)
            .unwrap();
        assert_eq!(line, "ready\n", "the holder child announced its readiness");
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
            .arg("exec 3<\"$0\"; echo ready; read _")
            .arg(&database)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = child.id() as i32;
        holder_ready(&mut child);
        let held = holders(&database).unwrap();
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
        child.wait_bounded().unwrap();
        assert_eq!(close(&root, &state()).unwrap(), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }

    /// One census answers every database at once, each with its own holders: the shell holding
    /// the second database is named for it alone, and a database that vanished has none.
    #[cfg(target_os = "macos")]
    #[test]
    fn one_census_names_each_databases_own_holders() {
        let root = scratch("census");
        let data = root.join("nx/workspace-data");
        fs::create_dir_all(&data).unwrap();
        let [idle, open, gone] = ["A-v3.db", "B-v3.db", "C-v3.db"].map(|name| data.join(name));
        for database in [&idle, &open] {
            fs::write(database, b"").unwrap();
        }
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("exec 3<\"$0\"; echo ready; read _")
            .arg(&open)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = child.id() as i32;
        let databases = [idle.clone(), open.clone(), gone];
        holder_ready(&mut child);
        let held = descriptor_holders(&databases).unwrap();
        let pids = |holders: &[Holder]| holders.iter().map(|holder| holder.pid).collect::<Vec<_>>();
        assert_eq!(
            held.iter().map(|holders| pids(holders)).collect::<Vec<_>>(),
            [vec![], vec![pid], vec![]]
        );
        match held_workspace_data(&data).unwrap() {
            Err(Busy::Held { database, holders }) => {
                assert_eq!((database, pids(&holders)), (open, vec![pid]));
            }
            other => panic!("expected the open database to be named: {other:?}"),
        }
        drop(child.stdin.take());
        child.wait_bounded().unwrap();
        assert_eq!(held_workspace_data(&data).unwrap(), Ok(()));
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
            .arg("echo ready; read _")
            .current_dir(&nested)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = child.id() as i32;
        holder_ready(&mut child);
        let held = volume_holders(&root).unwrap();
        drop(child.stdin.take());
        child.wait_bounded().unwrap();
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

    /// A plain checkout's holders are the processes inside its tree, which `volume_holders`
    /// cannot name: a process working in a subdirectory of the tree, with no file open, is one of
    /// them; a process working in a sibling tree is not.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn a_process_working_inside_a_plain_tree_is_one_of_its_holders() {
        let root = scratch("tree-holders");
        let sub = root.join("checkout/sub");
        fs::create_dir_all(&sub).unwrap();
        let sibling = scratch("tree-holders-sibling");
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("read _")
            .current_dir(&sub)
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let pid = i32::try_from(child.id()).unwrap();
        // Asked while the child blocks reading the stdin this test holds; it is released before
        // anything is asserted.
        let inside = tree_holders(&root).unwrap();
        let outside = tree_holders(&sibling).unwrap();
        let missing = tree_holders(&root.join("missing")).unwrap();
        drop(child.stdin.take());
        child.wait_bounded().unwrap();
        assert!(
            inside.iter().any(|holder| holder.pid == pid),
            "pid {pid} (cwd {}) is not among the {} holders of {}",
            sub.display(),
            inside.len(),
            root.display()
        );
        assert!(
            !outside.iter().any(|holder| holder.pid == pid),
            "pid {pid} is among the holders of the sibling tree {}",
            sibling.display()
        );
        assert!(
            missing.is_empty(),
            "a missing root has holders: {missing:?}"
        );
        fs::remove_dir_all(&root).unwrap();
        fs::remove_dir_all(&sibling).unwrap();
    }

    /// A checkout of the repository's own stock Nx, its daemon enabled, under a test root: on
    /// macOS a scratch root, whose teardown ends every process working in it, the daemon with the
    /// rest, whatever ends the test; elsewhere the test stops the daemon it may start itself
    /// ([`RecordedDaemon`]).
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    struct StockNx {
        nx: PathBuf,
        checkout: PathBuf,
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    impl StockNx {
        fn new(root: &Path) -> Self {
            // Read at run time: `env!` would compile this checkout's path into the test binary,
            // and a test binary built in one checkout must not reach another's files.
            let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
                .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
            let package = PathBuf::from(manifest)
                .join("../../../../node_modules/nx")
                .canonicalize()
                .expect("the repository's own Nx is installed");
            let checkout = root.to_path_buf();
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

        /// `nx <args>` in the checkout as a host shell runs it: without the caller's `NX_*` or
        /// `CI`, so no setting of the shell running the test decides what Nx does.
        fn command(&self, args: &[&str]) -> std::process::Command {
            let mut command = std::process::Command::new("node");
            command.arg(&self.nx).args(args).current_dir(&self.checkout);
            for (key, _) in std::env::vars_os() {
                if key.to_string_lossy().starts_with("NX_") || key == "CI" {
                    command.env_remove(key);
                }
            }
            command
        }

        /// Run `nx daemon <verb>` in the checkout as [`Self::run`] does.
        #[cfg(target_os = "macos")]
        fn daemon(&self, verb: &str) -> String {
            self.run(&["daemon", verb])
        }

        /// Run [`Self::command`]; it must exit 0. Answers what it printed.
        #[cfg(target_os = "macos")]
        fn run(&self, args: &[&str]) -> String {
            let output = self
                .command(args)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn_locked()
                .and_then(std::process::Child::wait_with_output_bounded)
                .expect("node runs");
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.status.success(), "nx {args:?}: {text}");
            text
        }

        fn workspace_data(&self) -> PathBuf {
            self.checkout.join(".nx/workspace-data")
        }

        fn record(&self) -> PathBuf {
            self.workspace_data().join("d/server-process.json")
        }
    }

    /// The daemon a test's own Nx client may start in a checkout's `workspace-data`, stopped as
    /// stock `nx daemon --stop` stops it ([`stop_recorded_daemon`]) however the test ends. The
    /// client detaches the daemon, so where no scratch root's teardown ends it nothing else does.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    struct RecordedDaemon(PathBuf);

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    impl Drop for RecordedDaemon {
        fn drop(&mut self) {
            let failure = match stop_recorded_daemon(&self.0) {
                Ok(Ok(_)) => return,
                Ok(Err(busy)) => format!("{busy:?}"),
                Err(error) => error.to_string(),
            };
            let said = format!(
                "stop the test's Nx daemon in {}: {failure}",
                self.0.display()
            );
            // A second panic while unwinding would abort the run; the failing test already fails.
            if std::thread::panicking() {
                cowshed_os::eprintln!("{said}");
            } else {
                panic!("{said}");
            }
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
        fn new(root: &crate::scratch_directory::ScratchRoot, stock: &StockNx) -> Self {
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
        let root = crate::scratch_directory::ScratchRoot::new("nx-opens").expect("scratch root");
        let stock = StockNx::new(root.path());
        let linked = Linked::new(&root, &stock);
        // Each volume holds the task database a first Nx process made there.
        for volume in [&linked.into, &linked.from] {
            super::super::link::point(&stock.checkout, volume).unwrap();
            let mut first = nx_opener(&stock);
            opened(&mut first);
            drop(first.stdin.take());
            first.wait_bounded().unwrap();
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
        opener.wait_bounded().unwrap();
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
        opening.wait_bounded().unwrap();
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
        let root =
            crate::scratch_directory::ScratchRoot::new("nx-hand-over").expect("scratch root");
        let stock = StockNx::new(root.path());
        let linked = Linked::new(&root, &stock);
        stock.daemon("--start");
        let daemon = crate::runtime::nx_daemon::live_pid(&stock.record()).expect("a live daemon");
        assert!(!cowshed_os::process::sandboxed(daemon).unwrap());

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
        assert!(!cowshed_os::process::running(daemon).unwrap());
    }

    /// Stopping the daemon natively is exactly stock `nx daemon --stop` (`SIGTERM` to the
    /// recorded pid): a real Nx daemon exits on it, and `nx daemon --start` afterwards starts a
    /// new one cleanly. If stock Nx ever needs more than the signal, this fails.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_stock_nx_daemon_stopped_natively_restarts_cleanly() {
        let root = crate::scratch_directory::ScratchRoot::new("nx-daemon").expect("scratch root");
        let stock = StockNx::new(root.path());
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
            !cowshed_os::process::running(first).unwrap(),
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

    /// A real Nx client handed a held lock through the one held-command boundary
    /// ([`crate::held_lock::prepare_command_environment`], then [`crate::held_lock::hand_to`], as
    /// `lock` and `land-turn` spawn their commands) starts no daemon, and once it has ended only
    /// its own descendants hold the lock (02_workspaces.md "Held locks"). The client is `nx show
    /// projects`, which computes the project graph through the daemon and starts one when none
    /// runs. `NX_DAEMON=true` keeps a CI host or a container from switching the daemon off, so the
    /// boundary's policy is all that differs between the two runs. Without it the same client
    /// starts a daemon: the control fails if the environment makes both arms daemonless.
    /// Whether Node passes the descriptor on to its daemon is a runtime implementation detail,
    /// not a Linux/Darwin contract. Direct descriptor custody is proved at `held_lock::hand_to`.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn an_nx_client_holding_a_lock_starts_no_daemon_that_would_keep_it() {
        #[cfg(target_os = "macos")]
        let scratch =
            crate::scratch_directory::ScratchRoot::new("nx-held-lock").expect("scratch root");
        #[cfg(target_os = "macos")]
        let root = scratch.path();
        #[cfg(target_os = "linux")]
        let scratch = crate::temp_root::TempRoot::new("cowshed-nx-held-lock");
        #[cfg(target_os = "linux")]
        let root: &Path = &scratch;
        let stock = StockNx::new(root);
        fs::create_dir_all(stock.checkout.join("p")).unwrap();
        fs::write(
            stock.checkout.join("p/project.json"),
            r#"{"name":"p","targets":{"build":{"executor":"nx:noop","cache":true}}}"#,
        )
        .unwrap();
        // Declared after the root and before any client, so whichever run starts a daemon, it is
        // stopped before the root goes.
        let _daemon = RecordedDaemon(stock.workspace_data());
        let lock = root.join("held.lock");
        let take = || {
            crate::held_lock::Opened::open(&lock)
                .expect("open the lock")
                .try_hold()
                .expect("take the lock")
                .expect("a free lock")
        };
        let client = || {
            let mut command = stock.command(&["show", "projects", "--json"]);
            command.env("NX_DAEMON", "true");
            command
        };
        let show = |mut command: std::process::Command| {
            let output = command
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn_locked()
                .and_then(std::process::Child::wait_with_output_bounded)
                .expect("node runs");
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "nx show projects: {stderr}");
            let projects: Vec<String> =
                serde_json::from_slice(&output.stdout).expect("nx prints the projects as JSON");
            assert!(
                projects.iter().any(|project| project == "p"),
                "{projects:?}: {stderr}"
            );
        };
        let holders = |held: &crate::held_lock::HeldLock| {
            let inode = held.inode();
            inode_holders((inode.dev.get(), inode.ino.get())).expect("the lock's holders")
        };

        // Under the policy, in a checkout no daemon ever served.
        let held = take();
        let mut command = client();
        crate::held_lock::prepare_command_environment(&mut command).expect("the held policy");
        crate::held_lock::hand_to(&mut command, vec![held.descriptor()]);
        show(command);
        assert_eq!(
            crate::runtime::nx_daemon::live_pid(&stock.record()),
            None,
            "the held client started a daemon"
        );
        let after = holders(&held);
        drop(held);
        let own = i32::try_from(std::process::id()).unwrap();
        let others: Vec<_> = after
            .iter()
            .filter(|(holder, _)| holder.pid != own)
            .collect();
        #[cfg(target_os = "macos")]
        assert!(
            others.is_empty(),
            "the held client's processes kept the lock: {others:?}"
        );
        // Linux hands the client's detached plugin workers the descriptor; each ends once it sees
        // its host gone, and holds the lock as the command's descendant until then.
        #[cfg(target_os = "linux")]
        assert!(
            others
                .iter()
                .all(|(holder, _)| holder.command.contains("plugin-worker")),
            "a process other than the client's plugin workers kept the lock: {others:?}"
        );
        drop(
            hold_announced(
                crate::held_lock::Opened::open(&lock).expect("open the lock"),
                "the held client's descendants",
            )
            .expect("the lock is granted once the client's descendants have ended"),
        );

        // The control: the descriptor alone, without the policy.
        let held = take();
        let mut command = client();
        crate::held_lock::hand_to(&mut command, vec![held.descriptor()]);
        show(command);
        let daemon = crate::runtime::nx_daemon::live_pid(&stock.record())
            .expect("the client without the policy started its daemon");
        drop(held);
        assert_eq!(
            stop_recorded_daemon(&stock.workspace_data()).expect("stop the daemon"),
            Ok(Some(daemon))
        );
        drop(
            hold_announced(
                crate::held_lock::Opened::open(&lock).expect("open the lock"),
                "the stopped daemon and its workers",
            )
            .expect("the lock is granted once the daemon and its workers have ended"),
        );
    }

    /// A test that fails while its stock Nx daemon runs leaves no daemon behind. The daemon
    /// detaches from the `nx daemon --start` that started it, so nothing but the fixture's own
    /// teardown ever ends it: a failed assertion between `--start` and [`close`] left it running
    /// in a deleted directory, watching files, for good.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_test_that_fails_with_a_live_nx_daemon_leaves_none_under_its_root() {
        let root =
            crate::scratch_directory::ScratchRoot::new("nx-daemon-failed").expect("scratch root");
        let path = root.path().to_path_buf();
        let daemon = std::cell::Cell::new(None);
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let root = root;
            let stock = StockNx::new(root.path());
            stock.daemon("--start");
            daemon.set(crate::runtime::nx_daemon::live_pid(&stock.record()));
            panic!("the test fails while its Nx daemon runs");
        }));
        assert!(failed.is_err(), "the test failed");
        let daemon = daemon
            .get()
            .expect("the daemon was live when the test failed");
        let left = crate::scratch_directory::processes::processes_in(|cwd| cwd.starts_with(&path))
            .expect("list the processes working under the root");
        assert_eq!(
            left,
            [],
            "daemon {daemon} or another process outlived the test"
        );
        assert!(!path.exists(), "{} was removed", path.display());
    }
    /// `held` answers from the task databases' open locks alone. A daemon record rewritten after a
    /// snapshot to name a foreign live process changes nothing it answers, signals nothing, and
    /// leaves that process running.
    #[cfg(target_os = "macos")]
    #[test]
    fn held_reads_task_databases_only_and_a_rewritten_daemon_record_signals_nothing() {
        let scratch = crate::temp_root::TempRoot::new("nx-held-record");
        let data = scratch.join("nx/workspace-data");
        fs::create_dir_all(data.join(DAEMON_DIRECTORY)).unwrap();
        fs::write(data.join("T-v3.db"), b"").unwrap();
        let state = BuildVolumeState {
            paths: vec![BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap()],
            fingerprint: None,
        };
        let mut foreign = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("read _")
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .unwrap();
        let record = data.join(DAEMON_DIRECTORY).join(DAEMON_RECORD);
        fs::write(
            &record,
            b"{\"processId\":0,\"socketPath\":\"/nonexistent.sock\"}",
        )
        .unwrap();
        assert!(
            held(&scratch, &state).unwrap().is_ok(),
            "no database is held"
        );
        let socket = scratch.join("live.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        fs::write(
            &record,
            serde_json::json!({
                "processId": foreign.id(),
                "socketPath": socket,
            })
            .to_string(),
        )
        .unwrap();
        assert!(
            held(&scratch, &state).unwrap().is_ok(),
            "the record is not read"
        );
        assert!(
            std::os::unix::net::UnixStream::connect_bounded(&socket).is_ok(),
            "held neither closes nor disturbs the live socket"
        );
        assert!(
            foreign.try_wait().unwrap().is_none(),
            "the foreign process is untouched"
        );
        drop(listener);
        drop(foreign.stdin.take());
        foreign.wait_bounded().unwrap();
    }

    /// A census asks nothing of a process a held lifecycle lease names with the unique id it has
    /// now: [`census_pids`], the one walker, passes over it, so the descriptor, tree and volume
    /// censuses never query a descriptor of it (a fork in its new build image's first write, which
    /// its read-write mount makes, held one for 54-90 s). A foreign holder of the same file is
    /// still found by each census, this process is asked whatever its lease says, and a record
    /// whose unique id another life has names nothing, so a reused pid is asked again.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_census_asks_nothing_of_a_process_a_held_lifecycle_lease_names() {
        let root = scratch("lifecycle-lease");
        let sessions = root.join("store/acme/widget/sessions");
        fs::create_dir_all(&sessions).unwrap();
        let database = root.join("T-v3.db");
        fs::write(&database, b"").unwrap();
        let hold = || {
            std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg("exec 3<\"$0\"; echo ready; read _")
                .arg(&database)
                .current_dir(&root)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .spawn_locked()
                .unwrap()
        };
        let (mut lifecycle, mut foreign) = (hold(), hold());
        let (lifecycle_pid, foreign_pid) = (lifecycle.id() as i32, foreign.id() as i32);
        let own_pid = std::process::id() as i32;
        let _own = fs::File::open(&database).unwrap();
        let pids = |holders: &[Holder]| holders.iter().map(|holder| holder.pid).collect::<Vec<_>>();
        holder_ready(&mut lifecycle);
        holder_ready(&mut foreign);
        let life = crate::runtime::job_groups::process_record(lifecycle_pid)
            .unwrap()
            .expect("the lifecycle stand-in runs")
            .unique_id;
        let record = |path: PathBuf, process: crate::lease_holder::Process| {
            fs::write(path, serde_json::to_vec(&process).unwrap()).unwrap();
        };
        let fork = |unique_id| crate::lease_holder::Process {
            pid: lifecycle.id(),
            unique_id: Some(crate::api::held_lock::DecimalU64::new(unique_id)),
            since: "2026-10-21T00:00:00Z".to_owned(),
            command: "cowshed fork main topic".to_owned(),
        };
        record(sessions.join("topic.intent.lock"), fork(life));
        record(
            sessions.join("main.land.lock"),
            crate::lease_holder::Process::this().unwrap(),
        );
        let censuses = || {
            crate::lease_holder::with_stores(vec![root.join("store")], || {
                (
                    census_pids().unwrap(),
                    pids(&holders(&database).unwrap()),
                    pids(&tree_holders(&root).unwrap()),
                    pids(&volume_holders(&root).unwrap()),
                )
            })
        };
        let (asked, held, tree, volume) = censuses();
        assert!(
            !asked.contains(&lifecycle_pid),
            "the lifecycle process was asked"
        );
        assert!(asked.contains(&foreign_pid) && asked.contains(&own_pid));
        for (census, found) in [("descriptor", &held), ("tree", &tree), ("volume", &volume)] {
            assert!(
                found.contains(&foreign_pid) && !found.contains(&lifecycle_pid),
                "{census} census: {found:?}"
            );
        }
        assert!(held.contains(&own_pid), "this process is asked: {held:?}");

        record(sessions.join("topic.intent.lock"), fork(life + 1));
        let (asked, held, _, _) = censuses();
        assert!(asked.contains(&lifecycle_pid) && held.contains(&lifecycle_pid));

        for child in [&mut lifecycle, &mut foreign] {
            drop(child.stdin.take());
            child.wait_bounded().unwrap();
        }
        fs::remove_dir_all(&root).unwrap();
    }
}
