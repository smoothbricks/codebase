//! A held lock: an exclusive `flock(2)` on a caller's lock file, and what the kernel says about who
//! holds it (02_workspaces.md "Held locks"). `cowshed lock`, `land --ref-lock`,
//! `land-turn --outside-lock` and every lifecycle image lock ([`ImageLockTaker`]) take it;
//! `cowshed lock-holders` observes it.
//!
//! The lock is on the inode the path names. No holder unlinks the file: a holder that did would
//! hand a waiter already blocked on it a lock on a file no path names, while the next caller
//! created and locked a new one. A holder records itself on the inode once the kernel grants it
//! the lock, with the lifecycle leases' record ([`crate::lease_holder::Process`]), and empties it
//! before it lets go, so a record outlives its hold only when its process ended without unwinding.
//!
//! A lock passed to a command is passed as its open file description: the command inherits the
//! descriptor ([`keep_across_exec`]), and the kernel releases the lock only once every process
//! holding that description has closed it, a SIGKILL of the one that took it included. So no
//! death of the taker alone lets another holder in while its command runs.
//!
//! Who holds it is read from the kernel, never from the record alone:
//!
//! - Linux shows each descriptor's lock: `/proc/<pid>/fdinfo/<fd>` carries a `lock:` line for the
//!   `flock(2)` its open file description holds ([`Custody::Holds`]); a descriptor without one,
//!   a waiter blocked in `flock(2)` among them, holds nothing ([`Custody::Open`]).
//! - Darwin shows no lock state per descriptor (`proc_pidfdinfo` carries none, and `F_GETLK`
//!   answers a `flock(2)` conflict with no pid), only which processes have a descriptor on the
//!   inode and each one's identity. The life a record names, proven by its kernel unique id and a
//!   descriptor on the inode, is the holder that recorded itself ([`Custody::Recorded`]); every
//!   other descriptor is named [`Custody::Unexposed`]: never counted a holder, never signalled.
//!
//! A holder is signalled only through the kernel's identity of a life its waiter's caller owned and
//! the waiter captured at its start, and only while the kernel proves that very life the owner
//! ([`OwnedLives::signal_owner`]).

use std::borrow::Cow;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read as _};
use std::os::fd::{AsRawFd as _, RawFd};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use crate::api::held_lock::{
    DecimalU64, LockCustody as Custody, LockDescriptor as Descriptor, LockHolding,
    LockInode as Inode, LockObservation as Observation, LockPath, LockRecord,
};
use crate::lease_holder;
use crate::runtime::job_groups;

/// The inode `metadata` describes.
fn inode_of(metadata: &fs::Metadata) -> Inode {
    Inode {
        dev: DecimalU64::new(metadata.dev()),
        ino: DecimalU64::new(metadata.ino()),
    }
}

/// A lock file opened, not yet held.
#[derive(Debug)]
pub struct Opened {
    file: File,
    path: PathBuf,
    inode: Inode,
}

/// A lock this process holds. Dropping it empties its holder record and releases this process's
/// descriptor; a command that inherited the descriptor keeps the lock until it closes it too.
#[derive(Debug)]
pub struct HeldLock {
    file: File,
    path: PathBuf,
    inode: Inode,
}

impl Opened {
    /// Open the lock file at `path`, creating it if no holder has yet, never truncating it. The
    /// descriptor is close-on-exec: only a command handed it ([`keep_across_exec`]) inherits it.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o644)
            .open(path)?;
        let metadata = file.metadata()?;
        Ok(Self {
            file,
            path: path.to_owned(),
            inode: inode_of(&metadata),
        })
    }

    pub fn inode(&self) -> Inode {
        self.inode
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Hold the lock if it is free now; `Ok(Err(self))` while another holds it.
    pub fn try_hold(self) -> io::Result<Result<HeldLock, Self>> {
        match self.file.try_lock() {
            Ok(()) => self.held().map(Ok),
            Err(TryLockError::WouldBlock) => Ok(Err(self)),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }

    /// Wait in `flock(2)` until the kernel grants the lock, then hold it. Nothing bounds the wait:
    /// the holder ahead is a writer this one must follow, and its caller keeps the wait queued, not
    /// silent.
    // WHY unbounded: queued, not silent, at every caller: the ref lock names the holder once per
    // HOLDER_REPORT, the land turn tells its place and the holder at its cadence, `cowshed lock`
    // names the holder once per HOLDER_REPORT and serves its operator's requests to end it, and the
    // test censuses name the holder on stderr before they wait under the test runner's backstop.
    // The holder's release at its work's end, or the kernel's at its exit, ends the wait.
    #[allow(clippy::disallowed_methods)]
    pub fn hold(self) -> io::Result<HeldLock> {
        loop {
            match self.file.lock() {
                Ok(()) => return self.held(),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// The lock just granted, recorded: refused when the path no longer names the inode locked.
    fn held(self) -> io::Result<HeldLock> {
        let named = fs::metadata(&self.path)?;
        if inode_of(&named) != self.inode {
            return Err(io::Error::other(format!(
                "{} names inode {} {}, not the inode {} just locked: a holder removed or replaced the lock file, which every holder keeps",
                self.path.display(),
                named.dev(),
                named.ino(),
                self.inode
            )));
        }
        lease_holder::record(&self.file, &lease_holder::Process::this()?)?;
        Ok(HeldLock {
            file: self.file,
            path: self.path,
            inode: self.inode,
        })
    }
}

impl HeldLock {
    pub fn inode(&self) -> Inode {
        self.inode
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The descriptor that holds the lock, for a command to inherit ([`keep_across_exec`]).
    pub fn descriptor(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

impl Drop for HeldLock {
    fn drop(&mut self) {
        lease_holder::clear(&self.file, &self.path);
    }
}

/// This process as the taker of lifecycle image locks (`main.dmg.lock`, `<ws>.dmg.lock`, the
/// store template's; 02_workspaces.md "Held locks") for `holding`, the request the call that
/// takes them names. It writes the held lock's record with that request beside the process onto
/// each lock once the kernel grants it (the guard empties it before it lets go), so
/// `lock-holders` names the holder and why, and a wait for one another holds names who holds it
/// as `lock-holders` judges it ([`holders`]).
pub struct ImageLockTaker<'a> {
    pub holding: &'a LockHolding,
}

/// An image lock's record: the held lock's process record, and the request beside it.
#[derive(serde::Serialize)]
struct ImageLockRecord<'a> {
    #[serde(flatten)]
    process: lease_holder::Process,
    #[serde(flatten)]
    holding: &'a LockHolding,
}

impl cowshed_apfs::store_lock::Taker for ImageLockTaker<'_> {
    fn record(&self, file: &File) -> io::Result<()> {
        lease_holder::record(
            file,
            &ImageLockRecord {
                process: lease_holder::Process::this()?,
                holding: self.holding,
            },
        )
    }

    fn holders(&self, path: &Path) -> String {
        holders(path)
    }
}

/// Who holds the lock file at `path`, in one line, as the kernel shows it ([`observe`]); why
/// not, when its holders cannot be read.
pub fn holders(path: &Path) -> String {
    match observe(path) {
        Ok(observation) => observation.to_string(),
        Err(error) => format!("its holders cannot be read: {error}"),
    }
}

/// Clear close-on-exec on `descriptor` in a child between `fork` and `exec`, so the exec'd command
/// holds the lock's open file description too: the lock then outlives its taker until the command,
/// and every process it hands the descriptor to, has closed it. One `fcntl`, async-signal-safe.
pub fn keep_across_exec(descriptor: RawFd) -> io::Result<()> {
    // SAFETY: F_SETFD takes a plain integer; the descriptor stays open and owned by its holder.
    if unsafe { libc::fcntl(descriptor, libc::F_SETFD, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Git configuration for a command holding a lock, through Git's own `GIT_CONFIG_COUNT`:
/// disable automatic gc, automatic maintenance and the fsmonitor daemon. This does not prevent
/// other descendants from detaching; an inherited descriptor keeps the lock until it is closed.
const HELD_GIT_CONFIG: [(&str, &str); 3] = [
    ("gc.auto", "0"),
    ("maintenance.auto", "false"),
    ("core.fsmonitor", "false"),
];

/// How many variables [`held_command_environment`] sets: `NX_DAEMON_START`, a key and a value
/// per [`HELD_GIT_CONFIG`] entry, and `GIT_CONFIG_COUNT`.
const HELD_ENVIRONMENT_LEN: usize = 2 + 2 * HELD_GIT_CONFIG.len();

/// Borrowed fixed settings and owned numbered keys for a command holding a lock.
pub type HeldCommandEnvironment = Vec<(Cow<'static, str>, Cow<'static, str>)>;

/// The held command's environment policy: `NX_DAEMON_START=never`, honored by cowshed's managed
/// Nx, and [`HELD_GIT_CONFIG`] appended after the caller's existing `GIT_CONFIG_COUNT` entries.
/// These settings suppress the named background work, not arbitrary detached descendants.
/// Pure: `count` is the caller's `GIT_CONFIG_COUNT`, if any; empty means zero, as in Git.
/// A value that is not a count, or leaves no room for the appended entries below `usize::MAX`,
/// is refused. Fixed names and values are borrowed; only the numbered keys and new count are built.
pub fn held_command_environment(
    count: Option<&std::ffi::OsStr>,
) -> Result<HeldCommandEnvironment, String> {
    let first = match count {
        None => 0,
        Some(count) if count.is_empty() => 0,
        Some(count) => count
            .to_str()
            .and_then(|count| count.parse::<usize>().ok())
            .ok_or_else(|| {
                format!("GIT_CONFIG_COUNT is {count:?}, not a count of git config entries")
            })?,
    };
    // The new count bounds every index below it, so one checked sum covers them all.
    let total = first.checked_add(HELD_GIT_CONFIG.len()).ok_or_else(|| {
        format!(
            "GIT_CONFIG_COUNT is {first}: {} more git config entries do not fit after it",
            HELD_GIT_CONFIG.len()
        )
    })?;
    let mut environment = Vec::with_capacity(HELD_ENVIRONMENT_LEN);
    environment.push((Cow::Borrowed("NX_DAEMON_START"), Cow::Borrowed("never")));
    for (index, (key, value)) in (first..total).zip(HELD_GIT_CONFIG) {
        environment.push((
            Cow::Owned(format!("GIT_CONFIG_KEY_{index}")),
            Cow::Borrowed(key),
        ));
        environment.push((
            Cow::Owned(format!("GIT_CONFIG_VALUE_{index}")),
            Cow::Borrowed(value),
        ));
    }
    environment.push((
        Cow::Borrowed("GIT_CONFIG_COUNT"),
        Cow::Owned(total.to_string()),
    ));
    Ok(environment)
}

/// Prepare a held command's environment before waiting for its lock. The caller's explicit
/// `GIT_CONFIG_COUNT` takes precedence over the inherited value. Only held commands receive
/// this policy; ordinary Git and Nx commands retain their own configuration.
pub fn prepare_command_environment(command: &mut std::process::Command) -> Result<(), String> {
    let count = match command
        .get_envs()
        .find(|(key, _)| *key == std::ffi::OsStr::new("GIT_CONFIG_COUNT"))
    {
        Some((_, set)) => set.map(std::ffi::OsStr::to_owned),
        None => std::env::var_os("GIT_CONFIG_COUNT"),
    };
    let environment = held_command_environment(count.as_deref())?;
    command.envs(environment.iter().map(|(key, value)| (&**key, &**value)));
    Ok(())
}

/// Hand the held descriptors to a command whose environment was already prepared with
/// [`prepare_command_environment`]. Descriptor inheritance is installed without further
/// environment parsing; the actual fcntl failures are returned by the command's spawn.
pub fn hand_to(command: &mut std::process::Command, descriptors: Vec<RawFd>) {
    // SAFETY: the closure runs in the child between fork and exec; it only reads the vector it
    // owns and calls fcntl, which is async-signal-safe.
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(command, move || {
            for &descriptor in &descriptors {
                keep_across_exec(descriptor)?;
            }
            Ok(())
        });
    }
}

/// One line: every descriptor on the inode with what the kernel shows of it, for a waiter to say
/// who holds what it waits for.
impl std::fmt::Display for Observation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.descriptors.is_empty() {
            return formatter.write_str(if self.locked {
                "locked by no process this user can read"
            } else {
                "no process has it open now, so its holder just let go"
            });
        }
        let custody = |custody: Custody| match custody {
            Custody::Holds => "holds it",
            Custody::Recorded => "holds it, as its record says",
            Custody::Open => "has it open without holding it",
            Custody::Unexposed => "has it open, custody unexposed by the kernel",
        };
        for (index, descriptor) in self.descriptors.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(
                formatter,
                "pid {} ({}) {}",
                descriptor.pid,
                descriptor.command,
                custody(descriptor.custody)
            )?;
            let owns = self
                .owner
                .as_ref()
                .is_some_and(|owner| owner.pid == descriptor.pid);
            if let (true, Some(holding)) = (owns, &self.holding) {
                write!(formatter, " for {holding}")?;
            }
        }
        Ok(())
    }
}

/// What the kernel showed of one process's descriptors on the locked inode, as the per-OS shell
/// read it: the facts the pure decision ([`judge`]) works from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KernelView {
    /// Linux: whether the fdinfo of any of its descriptors on the inode shows the granted flock.
    #[cfg(any(target_os = "linux", test))]
    Custody { holds: bool },
    /// Linux: at least one candidate descriptor could not be inspected and none proved custody.
    #[cfg(any(target_os = "linux", test))]
    Unexposed,
    /// Darwin, which shows no lock state: the process's kernel unique id, `None` once it has exited.
    #[cfg(any(target_os = "macos", test))]
    Identity { unique_id: Option<u64> },
}

/// One process with a descriptor on the locked inode, as the shell saw it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Seen {
    pub pid: i32,
    pub command: String,
    pub view: KernelView,
}

/// The custody a kernel view proves: Linux's own answer; on Darwin, `Recorded` only for the life
/// the record names by pid and the unique id it has now, never for a pid a later life was given.
fn custody_of(pid: i32, view: KernelView, recorded: Option<&lease_holder::Process>) -> Custody {
    match (pid, view, recorded) {
        #[cfg(any(target_os = "linux", test))]
        (_, KernelView::Custody { holds: true }, _) => Custody::Holds,
        #[cfg(any(target_os = "linux", test))]
        (_, KernelView::Custody { holds: false }, _) => Custody::Open,
        #[cfg(any(target_os = "linux", test))]
        (_, KernelView::Unexposed, _) => Custody::Unexposed,
        #[cfg(any(target_os = "macos", test))]
        (pid, KernelView::Identity { unique_id }, recorded) => {
            let named = recorded.is_some_and(|record| {
                i64::from(record.pid) == i64::from(pid)
                    && record.unique_id.is_some()
                    && record.unique_id.map(DecimalU64::get) == unique_id
            });
            if named {
                Custody::Recorded
            } else {
                Custody::Unexposed
            }
        }
    }
}

/// The pure decision of an observation: every descriptor but the observer's own (`own`) with its
/// custody, and the owner: the descriptor of the holder the record names, once the kernel proves
/// it. No I/O.
pub(crate) fn judge(
    seen: Vec<Seen>,
    recorded: Option<&lease_holder::Process>,
    own: i32,
) -> (Option<Descriptor>, Vec<Descriptor>) {
    let descriptors: Vec<Descriptor> = seen
        .into_iter()
        .filter(|process| process.pid != own)
        .map(|process| Descriptor {
            pid: process.pid,
            unique_id: match process.view {
                #[cfg(any(target_os = "macos", test))]
                KernelView::Identity { unique_id } => unique_id.map(DecimalU64::new),
                #[cfg(any(target_os = "linux", test))]
                KernelView::Custody { .. } | KernelView::Unexposed => None,
            },
            custody: custody_of(process.pid, process.view, recorded),
            command: process.command,
        })
        .collect();
    let owner = recorded.and_then(|holder| {
        descriptors
            .iter()
            .find(|descriptor| {
                i64::from(descriptor.pid) == i64::from(holder.pid)
                    && matches!(descriptor.custody, Custody::Holds | Custody::Recorded)
            })
            .cloned()
    });
    (owner, descriptors)
}

/// Which retained life a signal may reach: the proven owner of the very inode this waiter is
/// queued on (`queued`; `seen` is the inode the lock path names when it is observed, so a path
/// replaced since the waiter opened it proves nothing about the waiter's lock), only when its pid
/// is one whose life this waiter captured at its start and that very life still runs. `lives`
/// pairs each captured pid with whether its captured life runs now. The same pid owned by a later
/// life (whatever its uid and argv) is refused: the captured life has ended, and authority was
/// given for that life alone. No I/O.
pub(crate) fn owned_target(
    queued: Inode,
    seen: Option<Inode>,
    owner: Option<&Descriptor>,
    lives: &[(i32, bool)],
) -> Result<i32, String> {
    if seen != Some(queued) {
        return Err(match seen {
            Some(seen) => format!(
                "the lock path now names inode {seen}, not the inode {queued} this waiter is queued on"
            ),
            None => format!(
                "the lock path names no file now, not the inode {queued} this waiter is queued on"
            ),
        });
    }
    let Some(owner) = owner else {
        return Err("the kernel proves no owner of the lock".to_owned());
    };
    match lives.iter().find(|(pid, _)| *pid == owner.pid) {
        None => Err(format!(
            "the owner, pid {}, is no life this waiter was given",
            owner.pid
        )),
        Some((pid, false)) => Err(format!(
            "the life captured as pid {pid} has ended; the pid's owner now is another life"
        )),
        Some((pid, true)) => Ok(*pid),
    }
}

/// Read who holds the lock file at `path`: the per-OS shell reads the kernel, [`judge`] decides.
/// A missing file is no lock.
pub fn observe(path: &Path) -> io::Result<Observation> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Observation {
                path: LockPath::new(path.to_owned()),
                inode: None,
                locked: false,
                record: LockRecord::default(),
                owner: None,
                holding: None,
                descriptors: Vec::new(),
            });
        }
        Err(error) => return Err(error),
    };
    let inode = inode_of(&file.metadata()?);
    let mut record = Vec::new();
    file.read_to_end(&mut record)?;
    let recorded = serde_json::from_slice::<lease_holder::Process>(&record).ok();
    let own = i32::try_from(std::process::id()).map_err(io::Error::other)?;
    let (locked, seen) = kernel_view(&file, inode, recorded.as_ref())?;
    let (owner, descriptors) = judge(seen, recorded.as_ref(), own);
    // A request is believed only as far as its owner is proven.
    let holding = owner
        .as_ref()
        .and_then(|_| serde_json::from_slice::<LockHolding>(&record).ok());
    Ok(Observation {
        path: LockPath::new(path.to_owned()),
        inode: Some(inode),
        locked,
        record: LockRecord::new(record),
        owner,
        holding,
        descriptors,
    })
}

/// Linux: each process with a descriptor on `inode` and whether its fdinfo shows the flock, and
/// whether the kernel's lock table shows the inode locked (`/proc/locks`, which also covers a
/// holder of another user whose descriptors this user cannot read).
#[cfg(target_os = "linux")]
fn kernel_view(
    _file: &File,
    inode: Inode,
    _recorded: Option<&lease_holder::Process>,
) -> io::Result<(bool, Vec<Seen>)> {
    let mut seen = Vec::new();
    for (holder, fds) in crate::build_volume::nx::inode_holders((inode.dev.get(), inode.ino.get()))?
    {
        let view = fdinfo_custody(
            fds.iter()
                .map(|fd| fs::read_to_string(format!("/proc/{}/fdinfo/{fd}", holder.pid))),
            u64::from(libc::major(inode.dev.get())),
            u64::from(libc::minor(inode.dev.get())),
            inode.ino.get(),
        );
        if let Some(view) = view {
            seen.push(Seen {
                pid: holder.pid,
                command: holder.command,
                view,
            });
        }
    }
    let locked = seen
        .iter()
        .any(|process| process.view == KernelView::Custody { holds: true })
        || lock_table_holds(&fs::read_to_string("/proc/locks")?, inode);
    Ok((locked, seen))
}

/// Aggregate descriptor evidence without turning an unreadable candidate into negative custody.
/// A proven grant wins; otherwise any unreadable candidate is unexposed. Disappeared candidates
/// contribute nothing, and an entirely disappeared process is omitted from the observation.
#[cfg(any(target_os = "linux", test))]
fn fdinfo_custody<S: AsRef<str>>(
    infos: impl IntoIterator<Item = io::Result<S>>,
    major: u64,
    minor: u64,
    ino: u64,
) -> Option<KernelView> {
    let mut view = None;
    for info in infos {
        match info {
            Ok(info) => {
                if fdinfo_holds_flock(info.as_ref(), major, minor, ino) {
                    return Some(KernelView::Custody { holds: true });
                }
                view.get_or_insert(KernelView::Custody { holds: false });
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => view = Some(KernelView::Unexposed),
        }
    }
    view
}

/// Whether fdinfo names a granted `flock(2)` on the requested inode, not a different lock on a
/// reused descriptor. Waiters are never granted custody; `/proc/locks` marks them `->`.
#[cfg(any(target_os = "linux", test))]
fn fdinfo_holds_flock(info: &str, major: u64, minor: u64, ino: u64) -> bool {
    info.lines().any(|line| {
        line.strip_prefix("lock:")
            .is_some_and(|lock| lock_line_holds(lock, major, minor, ino))
    })
}

/// Whether `/proc/locks` shows a granted `flock(2)` on `inode`: a line `N: FLOCK ... <pid>
/// <maj>:<min>:<ino> ...` (hexadecimal major and minor) that is no `->` waiter.
#[cfg(target_os = "linux")]
fn lock_table_holds(table: &str, inode: Inode) -> bool {
    let (dev, ino) = (inode.dev.get(), inode.ino.get());
    let (major, minor) = (libc::major(dev), libc::minor(dev));
    table
        .lines()
        .any(|line| lock_line_holds(line, u64::from(major), u64::from(minor), ino))
}

#[cfg(any(target_os = "linux", test))]
fn lock_line_holds(line: &str, major: u64, minor: u64, ino: u64) -> bool {
    let mut fields = line.split_whitespace();
    fields.next();
    if fields.next() != Some("FLOCK") {
        return false;
    }
    fields.any(|field| {
        let mut parts = field.split(':');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(maj), Some(min), Some(number), None) => {
                u64::from_str_radix(maj, 16).ok() == Some(major)
                    && u64::from_str_radix(min, 16).ok() == Some(minor)
                    && number.parse::<u64>().ok() == Some(ino)
            }
            _ => false,
        }
    })
}

/// Darwin: each process with a descriptor on `inode` and its kernel unique id, and whether
/// `F_GETLK` shows the inode exclusively locked: a `flock(2)` conflicts with the shared lock it
/// asks about. The descriptor census asks no process a cowshed lifecycle operation runs in
/// ([`crate::build_volume::nx::inode_holders`]): a fork's first mount of its new image holds its vnode
/// for minutes. The lock's `recorded` holder, when it is such a process, is seen by its unique id
/// alone, which the process table answers without reading a descriptor, so a fork holding
/// `main.dmg.lock` is still its proven owner.
#[cfg(target_os = "macos")]
fn kernel_view(
    file: &File,
    inode: Inode,
    recorded: Option<&lease_holder::Process>,
) -> io::Result<(bool, Vec<Seen>)> {
    let mut seen = Vec::new();
    for (holder, _fds) in
        crate::build_volume::nx::inode_holders((inode.dev.get(), inode.ino.get()))?
    {
        let unique_id = job_groups::process_record(holder.pid)?.map(|record| record.unique_id);
        seen.push(Seen {
            pid: holder.pid,
            command: holder.command,
            view: KernelView::Identity { unique_id },
        });
    }
    if let Some(recorded) = recorded
        && let Ok(pid) = i32::try_from(recorded.pid)
        && !seen.iter().any(|process| process.pid == pid)
        && lease_holder::running_lifecycle().contains(&pid)
        && let Some(record) = job_groups::process_record(pid)?
    {
        seen.push(Seen {
            pid,
            command: recorded.command.clone(),
            view: KernelView::Identity {
                unique_id: Some(record.unique_id),
            },
        });
    }
    // SAFETY: an all-zero `flock` is a valid value of the plain C struct.
    let mut probe = unsafe { std::mem::zeroed::<libc::flock>() };
    probe.l_type = libc::c_short::try_from(libc::F_RDLCK).map_err(io::Error::other)?;
    probe.l_whence = libc::c_short::try_from(libc::SEEK_SET).map_err(io::Error::other)?;
    // SAFETY: F_GETLK reads and writes `probe`, a live `flock` this function owns.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETLK, &mut probe) } == -1 {
        return Err(io::Error::last_os_error());
    }
    let locked =
        probe.l_type != libc::c_short::try_from(libc::F_UNLCK).map_err(io::Error::other)?;
    Ok((locked, seen))
}

/// The cancellation authority a waiter captures at its start: the kernel's identity of each life
/// its caller owns (a pidfd on Linux, its pid version on Darwin), held for the waiter's whole run.
/// A pid already gone at the capture is not retained.
pub struct OwnedLives(Vec<job_groups::Process>);

impl OwnedLives {
    pub fn capture(pids: &[i32]) -> io::Result<Self> {
        let mut lives = Vec::with_capacity(pids.len());
        for &pid in pids {
            if let Some(life) = job_groups::Process::observe(pid)? {
                lives.push(life);
            }
        }
        Ok(Self(lives))
    }

    /// Signal the proven owner of the inode `queued` the waiter opened at `path` through the life
    /// captured for it ([`owned_target`]): the signal reaches that life or nothing, never a later
    /// process given its pid nor the holder of a file that replaced the waiter's at `path`.
    /// `Ok(Err(reason))` names why nothing was signalled.
    pub fn signal_owner(
        &self,
        path: &Path,
        queued: Inode,
        signal: libc::c_int,
    ) -> io::Result<Result<i32, String>> {
        let observation = observe(path)?;
        let lives = self
            .0
            .iter()
            .map(|life| Ok((life.pid(), life.running()?)))
            .collect::<io::Result<Vec<_>>>()?;
        let pid = match owned_target(
            queued,
            observation.inode,
            observation.owner.as_ref(),
            &lives,
        ) {
            Ok(pid) => pid,
            Err(reason) => return Ok(Err(reason)),
        };
        let life = self
            .0
            .iter()
            .find(|life| life.pid() == pid)
            .ok_or_else(|| io::Error::other("owned_target chose a pid no captured life has"))?;
        Ok(if life.signal(signal)? {
            Ok(pid)
        } else {
            Err(format!(
                "the life captured as pid {pid} ended before the signal"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_os::test_backstop::ChildBackstop as _;

    /// Every record the properties judge: none, or one naming a pid of the domain or none of it,
    /// with no unique id, the live one, or another life's.
    fn records() -> Vec<Option<lease_holder::Process>> {
        let mut records = vec![None];
        for pid in [1, 2, 3] {
            for unique_id in [None, Some(10), Some(11)] {
                records.push(Some(lease_holder::Process {
                    pid,
                    unique_id: unique_id.map(DecimalU64::new),
                    since: "2026-10-21T00:00:00Z".to_owned(),
                    command: "holder".to_owned(),
                }));
            }
        }
        records
    }

    /// Every view the kernel can give of one process, on either OS.
    const VIEWS: [KernelView; 6] = [
        KernelView::Custody { holds: true },
        KernelView::Custody { holds: false },
        KernelView::Unexposed,
        KernelView::Identity { unique_id: None },
        KernelView::Identity {
            unique_id: Some(10),
        },
        KernelView::Identity {
            unique_id: Some(11),
        },
    ];

    /// Every pair of processes 1 and 2 with every view each, plus the observer itself (pid 3).
    fn sightings() -> Vec<Vec<Seen>> {
        let mut all = Vec::new();
        for first in VIEWS {
            for second in VIEWS {
                for observer in [None, Some(VIEWS[0])] {
                    let mut seen = vec![
                        Seen {
                            pid: 1,
                            command: "one".to_owned(),
                            view: first,
                        },
                        Seen {
                            pid: 2,
                            command: "two".to_owned(),
                            view: second,
                        },
                    ];
                    if let Some(view) = observer {
                        seen.push(Seen {
                            pid: 3,
                            command: "observer".to_owned(),
                            view,
                        });
                    }
                    all.push(seen);
                }
            }
        }
        all
    }

    /// The pure decision over every sighting and record of the domain: the observer is never named,
    /// an owner is only ever the record's pid holding proven custody, an `open` or `unexposed`
    /// descriptor is never the owner and never signalled, Linux custody is the kernel's whatever the
    /// record says, and Darwin proves only the life whose unique id the record carries.
    #[test]
    fn the_decision_proves_only_what_the_kernel_shows_and_signals_only_its_proven_owner() {
        for seen in sightings() {
            for recorded in records() {
                let (owner, descriptors) = judge(seen.clone(), recorded.as_ref(), 3);
                assert!(
                    descriptors.iter().all(|descriptor| descriptor.pid != 3),
                    "{descriptors:?}"
                );
                for (process, descriptor) in seen
                    .iter()
                    .filter(|process| process.pid != 3)
                    .zip(&descriptors)
                {
                    match process.view {
                        KernelView::Custody { holds } => assert_eq!(
                            descriptor.custody,
                            if holds { Custody::Holds } else { Custody::Open },
                            "Linux custody followed the record: {recorded:?}"
                        ),
                        KernelView::Unexposed => assert_eq!(descriptor.custody, Custody::Unexposed),
                        KernelView::Identity { unique_id } => {
                            let recorded_life = recorded.as_ref().is_some_and(|record| {
                                i64::from(record.pid) == i64::from(process.pid)
                                    && record.unique_id.is_some()
                                    && record.unique_id.map(DecimalU64::get) == unique_id
                            });
                            assert_eq!(
                                descriptor.custody,
                                if recorded_life {
                                    Custody::Recorded
                                } else {
                                    Custody::Unexposed
                                },
                                "{process:?} {recorded:?}"
                            );
                        }
                    }
                }
                // Every captured-lives table over pids 1, 2 and 4 (4 holds no descriptor), each life
                // running or ended: an ended life is the counterexample of the same pid, uid and argv
                // owned now by a later life.
                let mut tables: Vec<Vec<(i32, bool)>> = vec![Vec::new()];
                for pid in [1, 2, 4] {
                    tables = tables
                        .into_iter()
                        .flat_map(|table| {
                            [None, Some(true), Some(false)]
                                .into_iter()
                                .map(move |life| {
                                    let mut table = table.clone();
                                    if let Some(running) = life {
                                        table.push((pid, running));
                                    }
                                    table
                                })
                        })
                        .collect();
                }
                match &owner {
                    Some(owner) => {
                        let record = recorded.as_ref().expect("an owner only from a record");
                        assert_eq!(i64::from(owner.pid), i64::from(record.pid));
                        assert_eq!(
                            descriptors
                                .iter()
                                .find(|descriptor| descriptor.pid == owner.pid),
                            Some(owner)
                        );
                        let proven = Some(owner);
                        assert!(
                            proven.is_some_and(|descriptor| matches!(
                                descriptor.custody,
                                Custody::Holds | Custody::Recorded
                            )),
                            "an owner without proven custody: {descriptors:?}"
                        );
                    }
                    None => assert!(
                        !recorded
                            .as_ref()
                            .is_some_and(|record| descriptors.iter().any(|descriptor| {
                                i64::from(descriptor.pid) == i64::from(record.pid)
                                    && matches!(
                                        descriptor.custody,
                                        Custody::Holds | Custody::Recorded
                                    )
                            })),
                        "a proven recorded holder was not named the owner"
                    ),
                }
                // The waiter is queued on inode 1 2; the lock path names it, another inode, or no file.
                let queued = Inode {
                    dev: DecimalU64::new(1),
                    ino: DecimalU64::new(2),
                };
                let replaced = Inode {
                    dev: DecimalU64::new(1),
                    ino: DecimalU64::new(3),
                };
                for path_names in [Some(queued), Some(replaced), None] {
                    for lives in &tables {
                        let target = owned_target(queued, path_names, owner.as_ref(), lives);
                        let expected = owner
                            .as_ref()
                            .filter(|_| path_names == Some(queued))
                            .and_then(|owner| {
                                lives
                                    .iter()
                                    .find(|(pid, running)| *pid == owner.pid && *running)
                                    .map(|(pid, _)| *pid)
                            });
                        assert_eq!(
                            target.clone().ok(),
                            expected,
                            "{path_names:?} {owner:?} {lives:?}"
                        );
                        if let Ok(pid) = target {
                            let signalled = descriptors
                                .iter()
                                .find(|descriptor| descriptor.pid == pid)
                                .expect("the target holds a descriptor");
                            assert!(
                                matches!(signalled.custody, Custody::Holds | Custody::Recorded),
                                "an unproven descriptor may be signalled: {signalled:?}"
                            );
                        }
                        // The same pid whose captured life has ended is never signalled.
                        if let Some(owner) = &owner {
                            let ended: Vec<_> = lives
                                .iter()
                                .map(|(pid, running)| (*pid, *running && *pid != owner.pid))
                                .collect();
                            assert!(owned_target(queued, path_names, Some(owner), &ended).is_err());
                        }
                    }
                }
            }
        }
    }

    /// The held command's entries follow the caller's without overwriting one, the count covers
    /// them all, automatic gc/maintenance/fsmonitor are off, and the managed Nx no-start policy
    /// is set. Invalid counts and counts without room for the appended entries are refused.
    #[test]
    fn a_held_command_appends_git_config_and_sets_the_managed_nx_no_start_policy() {
        use std::ffi::OsStr;
        let largest = usize::MAX - HELD_GIT_CONFIG.len();
        for existing in [None, Some(0), Some(1), Some(2), Some(7), Some(largest)] {
            let count = existing.map(|count: usize| count.to_string());
            let environment =
                held_command_environment(count.as_deref().map(OsStr::new)).expect("a valid count");
            assert_eq!(environment.len(), HELD_ENVIRONMENT_LEN);
            let value = |key: &str| {
                environment
                    .iter()
                    .find(|(name, _)| name == key)
                    .map(|(_, value)| &**value)
            };
            let first = existing.unwrap_or(0);
            assert_eq!(value("NX_DAEMON_START"), Some("never"));
            assert_eq!(
                value("GIT_CONFIG_COUNT"),
                Some((first + HELD_GIT_CONFIG.len()).to_string().as_str())
            );
            // No entry the caller set is overwritten: every numbered entry is at `first` or later.
            for (name, _) in &environment {
                if let Some(index) = name
                    .strip_prefix("GIT_CONFIG_KEY_")
                    .or_else(|| name.strip_prefix("GIT_CONFIG_VALUE_"))
                {
                    assert!(
                        index.parse::<usize>().expect("an index") >= first,
                        "overwrote entry {name}"
                    );
                }
            }
            for (offset, (key, setting)) in HELD_GIT_CONFIG.iter().enumerate() {
                assert_eq!(
                    value(&format!("GIT_CONFIG_KEY_{}", first + offset)),
                    Some(*key)
                );
                assert_eq!(
                    value(&format!("GIT_CONFIG_VALUE_{}", first + offset)),
                    Some(*setting)
                );
            }
        }
        let past = [usize::MAX, usize::MAX - 1, usize::MAX - 2].map(|count| count.to_string());
        for refused in ["-1", "two", "1.5", "18446744073709551616"]
            .into_iter()
            .chain(past.iter().map(String::as_str))
        {
            assert!(
                held_command_environment(Some(OsStr::new(refused))).is_err(),
                "{refused:?}"
            );
        }
        assert_eq!(
            held_command_environment(Some(OsStr::new(""))).expect("empty means zero"),
            held_command_environment(Some(OsStr::new("0"))).expect("zero is a count")
        );
    }

    #[test]
    fn an_fdinfo_lock_line_is_a_granted_flock_and_nothing_else_is() {
        let held = "pos:\t0\nflags:\t02100002\nmnt_id:\t31\nino:\t1311\nlock:\t1: FLOCK  ADVISORY  WRITE 4242 08:02:1311 0 EOF\n";
        assert!(fdinfo_holds_flock(held, 8, 2, 1311));
        assert!(!fdinfo_holds_flock(held, 8, 2, 1312));
        assert!(!fdinfo_holds_flock(held, 9, 2, 1311));
        let posix = "pos:\t0\nlock:\t1: POSIX  ADVISORY  WRITE 4242 08:02:1311 0 EOF\n";
        assert!(!fdinfo_holds_flock(posix, 8, 2, 1311));
        assert!(!fdinfo_holds_flock(
            "pos:\t0\nflags:\t02100002\n",
            8,
            2,
            1311
        ));
    }

    #[test]
    fn unreadable_fdinfo_is_unexposed_and_disappeared_fdinfo_is_not_a_descriptor() {
        let held = "lock:\t1: FLOCK ADVISORY WRITE 4242 08:02:1311 0 EOF\n";
        let open = "pos:\t0\nflags:\t02100002\n";
        let gone = || Err::<&str, _>(io::Error::from(io::ErrorKind::NotFound));
        let unreadable = || Err::<&str, _>(io::Error::from(io::ErrorKind::PermissionDenied));
        assert_eq!(fdinfo_custody([gone()], 8, 2, 1311), None);
        assert_eq!(
            fdinfo_custody([unreadable()], 8, 2, 1311),
            Some(KernelView::Unexposed)
        );
        assert_eq!(
            fdinfo_custody([Ok(open), gone()], 8, 2, 1311),
            Some(KernelView::Custody { holds: false })
        );
        for infos in [[Ok(open), unreadable()], [unreadable(), Ok(open)]] {
            assert_eq!(
                fdinfo_custody(infos, 8, 2, 1311),
                Some(KernelView::Unexposed)
            );
        }
        for infos in [[Ok(held), unreadable()], [unreadable(), Ok(held)]] {
            assert_eq!(
                fdinfo_custody(infos, 8, 2, 1311),
                Some(KernelView::Custody { holds: true })
            );
        }
        assert_eq!(
            fdinfo_custody([Ok(held), unreadable()], 8, 2, 1312),
            Some(KernelView::Unexposed)
        );
    }

    #[test]
    fn a_lock_table_line_names_its_inode_and_never_a_waiter() {
        let granted = "1: FLOCK  ADVISORY  WRITE 4242 fd:01:1311 0 EOF";
        assert!(lock_line_holds(granted, 0xfd, 1, 1311));
        assert!(!lock_line_holds(granted, 0xfd, 1, 1312));
        assert!(!lock_line_holds(granted, 8, 1, 1311));
        let waiter = "1: -> FLOCK  ADVISORY  WRITE 4343 fd:01:1311 0 EOF";
        assert!(!lock_line_holds(waiter, 0xfd, 1, 1311));
    }

    /// The lock at `path`, once the kernel grants it after its holders let go. A probe at a
    /// spawn's return, or at its command's reaping, never proves the lock released: copies of
    /// the parent's descriptors outlive both. Darwin's `posix_spawnp` (Libc `sys/posix_spawn.c`)
    /// and `execvp` try each `PATH` entry in turn, and XNU clones the process for every try
    /// before it looks the program up (`fdt_fork` copies a spawn's close-on-exec descriptors and
    /// an exec's inherited ones); a try that fails is torn down by its own thread after the call
    /// has returned. A forked child reports its `exec` before `exec` has closed every
    /// close-on-exec descriptor. A lock still held when the wait begins is named on stderr first,
    /// so a holder that never lets go, which this wait never outlasts, leaves its name in the
    /// test's output.
    fn granted(path: &Path) -> io::Result<HeldLock> {
        match Opened::open(path)?.try_hold()? {
            Ok(grant) => Ok(grant),
            Err(waiter) => {
                cowshed_os::eprintln!(
                    "{} is still held after its holder let go; waiting for the kernel to grant it: {}",
                    path.display(),
                    holders(path)
                );
                waiter.hold()
            }
        }
    }

    /// Whether `command` still ran when the kernel granted the lock at `path` its holder let go
    /// of ([`granted`]): the proof that the command kept no copy of it.
    fn granted_while_running(path: &Path, command: &mut std::process::Child) -> io::Result<bool> {
        let grant = granted(path)?;
        let running = command.try_wait()?.is_none();
        drop(grant);
        Ok(running)
    }

    /// The boundary that keeps an inherited lock from escaping into a lasting service. A command
    /// handed the lock holds it with its taker; every process cowshed starts to outlive its starter
    /// (a workspace supervisor, a job, the Nx daemon inside one) is spawned through
    /// [`crate::exec::prepare_child_descriptors`], which marks every descriptor above standard
    /// error close-on-exec first, so none of them can pin the lock past the command. This process
    /// stands for that command: its copy of the descriptor is open across exec while the test runs,
    /// which nextest isolates in a process of its own.
    #[test]
    fn a_process_cowshed_starts_never_inherits_a_lock_its_starter_holds_across_exec() {
        use cowshed_os::fork_lock::Spawn as _;

        let root = crate::temp_root::TempRoot::new("held-lock-boundary");
        let path = root.join("exact.lock");
        let held = Opened::open(&path)
            .expect("open the lock")
            .try_hold()
            .expect("take the lock")
            .expect("a free lock");
        keep_across_exec(held.descriptor()).expect("hand the descriptor across exec");
        let mut command = std::process::Command::new("sleep");
        command.arg("600");
        crate::exec::prepare_child_descriptors(&mut command).expect("seal the child's descriptors");
        let mut service = command.spawn_locked().expect("spawn the service");
        drop(held);
        // Collect the observation before ending the service, and assert only once it is reaped.
        let released = granted_while_running(&path, &mut service);
        service.kill().expect("end the service");
        service.wait_bounded().expect("reap the service");
        assert!(
            released.expect("wait for the lock"),
            "a process cowshed started kept the lock its starter let go of: the kernel granted it only after the service ended"
        );
    }

    /// The handoff itself, independent of how Node or another runtime spawns its children.
    /// Both real commands wait on our stdin pipe; only the explicitly handed descriptor keeps
    /// the lock after the parent closes its copy, and EOF releases the child and the lock. A
    /// handed command shares the open file description, so a waiter refused while it reads is
    /// proof; an unhanded one keeps no copy, which only a grant while it reads proves
    /// ([`granted_while_running`]), and the release at its exit is the next waiter's grant.
    #[test]
    fn only_a_command_handed_the_descriptor_keeps_the_lock_until_it_exits() {
        use cowshed_os::fork_lock::Spawn as _;

        let root = crate::temp_root::TempRoot::new("held-lock-handoff");
        let path = root.join("exact.lock");
        for handed in [false, true] {
            let held = Opened::open(&path)
                .expect("open the lock")
                .try_hold()
                .expect("take the lock")
                .expect("a free lock");
            let mut command = std::process::Command::new("sh");
            command
                .args(["-c", "read _ || :"])
                .stdin(std::process::Stdio::piped());
            prepare_command_environment(&mut command).expect("the held policy");
            if handed {
                hand_to(&mut command, vec![held.descriptor()]);
            }
            let mut child = command.spawn_locked().expect("spawn the reader");
            drop(held);
            // Collect the observation without asserting until our child has been released
            // and reaped, so a failing oracle cannot strand it or its inherited lock.
            let failure = if handed {
                Opened::open(&path).and_then(Opened::try_hold).map(|grant| {
                    grant.ok().map(|grant| {
                        drop(grant);
                        format!(
                            "the kernel granted the lock while the command it was handed to still ran; it shows: {}",
                            holders(&path)
                        )
                    })
                })
            } else {
                granted_while_running(&path, &mut child).map(|running| {
                    (!running).then(|| {
                        "the kernel granted the lock only after the command it was not handed to had ended".to_owned()
                    })
                })
            };
            drop(child.stdin.take());
            let status = child.wait_bounded().expect("reap the reader");
            assert!(status.success(), "reader exit: {status}");
            assert_eq!(failure.expect("probe the lock"), None, "handed: {handed}");
            // The reader has exited, so the kernel grants the lock to the next waiter.
            drop(granted(&path).expect("wait for the released lock"));
        }
    }
}
