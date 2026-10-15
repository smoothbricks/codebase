//! The client half of the host disk-lifecycle lease (05_gateway.md, "Disk-lifecycle lease").
//!
//! Every disk tool cowshed runs — production and tests alike — runs inside [`leased`]: the
//! command's class is read off its program, a lease of that class is taken from the gateway's
//! control socket, and the command runs while the connection stays open. Two lifecycle spans
//! say what that cost: `disk-lease <class> wait` from asking to the grant, and
//! `disk-lease <class> phase` around the command the grant admitted.
//!
//! The lease spaces commands out; it never decides whether one runs. With no gateway to ask, a
//! gateway that predates leases, a refusal, or a wait past the admission bound, the command runs
//! unleased. A SAFE command's own total deadline still applies to every admission step and
//! retry; its runner rejects spawn after that budget expires. The wait span ends status=err
//! with an unleased reason; a missing gateway is said once per process.
//!
//! A test's lease waits are the host's queue, not its code. When [`LEASE_WAIT_RECORD_ENV`] names
//! a file, every wait is also appended to it (08_testing.md, "Integration tests (real
//! substrate)"), so the real-APFS lane's wrapper can judge the test's code time without them,
//! and every lease is asked [`PhaseShare::Alone`]: a shared phase's command queues on
//! `storagekitd` behind the other members', which would be the host's queue counted as code.

use std::ffi::OsStr;
use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cowshed_gateway_types::{DiskClass, DiskLeaseAnswer, DiskLeaseRequest, LeaseState, PhaseShare};

use crate::device::DISKUTIL;
use crate::{CommandDeadline, DISK_CHILD_DEADLINE, HDIUTIL, MOUNT_APFS, NEWFS_APFS};

const UMOUNT: &str = "/sbin/umount";

/// Classify disk-changing commands. `diskutil`, `hdiutil` and `newfs_apfs` use storage; mount
/// tools use namespace. No disk tool renames a volume ([`refuse_volume_rename`]).
pub fn class_of(program: &Path) -> Option<DiskClass> {
    let program = program.to_str()?;
    if [DISKUTIL, HDIUTIL, NEWFS_APFS].contains(&program) {
        Some(DiskClass::Storage)
    } else if [MOUNT_APFS, UMOUNT].contains(&program) {
        Some(DiskClass::Namespace)
    } else {
        None
    }
}

/// A Disk Arbitration rename (`diskutil rename` or `renameVolume`), refused before it runs.
///
/// Disk Arbitration names a device through the mount path it recorded, resolved only when its
/// queue reaches the request, and the kernel names whatever volume holds a path: a volume
/// force-unmounted in between hands the name to the volume beneath its bare mountpoint — the
/// host's Data volume (01a_apfs.md, "Ownership, identity, and the volume label"). A volume is
/// named only through its held, proven root ([`crate::OwnedVolumeRoot`]).
#[derive(Debug)]
pub struct VolumeRenameRefused {
    pub program: PathBuf,
    pub args: Vec<std::ffi::OsString>,
}

/// Why a Disk Arbitration rename is refused, as every refusal of one says it.
pub(crate) const VOLUME_RENAME_REFUSAL: &str = "Disk Arbitration renames through the mount path \
     it recorded, which an unmount since hands to the volume beneath; a volume is named only \
     through its held, owned mount root";

impl fmt::Display for VolumeRenameRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "refused {}: {VOLUME_RENAME_REFUSAL}",
            describe(&self.program, &self.args)
        )
    }
}

impl std::error::Error for VolumeRenameRefused {}

impl From<VolumeRenameRefused> for io::Error {
    fn from(refused: VolumeRenameRefused) -> Self {
        io::Error::new(io::ErrorKind::PermissionDenied, refused)
    }
}

/// Refuse `program args` when it is a Disk Arbitration rename: `diskutil rename` or
/// `renameVolume`, verbs diskutil matches without regard to case, whatever path names diskutil.
pub fn refuse_volume_rename(
    program: &Path,
    args: &[impl AsRef<OsStr>],
) -> Result<(), VolumeRenameRefused> {
    let renames = program.file_name() == Some(OsStr::new("diskutil"))
        && args.first().is_some_and(|verb| {
            verb.as_ref().to_str().is_some_and(|verb| {
                verb.eq_ignore_ascii_case("rename") || verb.eq_ignore_ascii_case("renameVolume")
            })
        });
    if renames {
        Err(VolumeRenameRefused {
            program: program.to_owned(),
            args: args.iter().map(|arg| arg.as_ref().to_owned()).collect(),
        })
    } else {
        Ok(())
    }
}

/// Run `run`, the disk tool `program args`, under a lease of its class from the host gateway. A
/// Disk Arbitration rename is refused first, for every caller ([`refuse_volume_rename`]): every
/// disk tool cowshed runs, the runner's, the root setup's and the tests', passes here. A
/// program that is no disk tool runs at once, and so does every disk tool for a minute after the
/// host gateway was found to predate leases; for a minute after it was found to predate `alone`
/// leases, it is asked shared. Every disk tool is first appended to the command record when
/// [`COMMAND_RECORD_ENV`] names one, leased or not.
pub fn leased<T, E: fmt::Display + From<VolumeRenameRefused>>(
    program: &Path,
    args: &[impl AsRef<OsStr>],
    deadline: Option<CommandDeadline>,
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    refuse_volume_rename(program, args)?;
    let Some(class) = class_of(program) else {
        return run();
    };
    leased_class(class, &describe(program, args), deadline, run)
}

/// Admit a descriptor-native namespace operation through the same client as disk children.
/// The operation record is its real native effect, not an invented executable or second runner.
pub(crate) fn namespace<T, E: fmt::Display>(
    operation: &str,
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    leased_class(DiskClass::Namespace, operation, None, run)
}

fn leased_class<T, E: fmt::Display>(
    class: DiskClass,
    command: &str,
    deadline: Option<CommandDeadline>,
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    if let Some(record) = COMMAND_RECORD.as_ref() {
        record.append(class, command);
    }
    let share = match predates_recently() {
        Some(Predates::Leases) => return run(),
        Some(Predates::Alone) => PhaseShare::Shared,
        None => wanted_share(),
    };
    let socket = cowshed_gateway_types::control_socket_path();
    lease_then(
        &socket,
        class,
        share,
        command,
        remember_predates,
        deadline,
        run,
    )
}

/// [`leased`] against the control socket at `socket`, for a command already classed and named.
/// Nothing about that gateway is remembered for the host gateway's sake.
pub fn leased_at<T, E: fmt::Display>(
    socket: &Path,
    class: DiskClass,
    command: &str,
    deadline: Option<CommandDeadline>,
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    lease_then(
        socket,
        class,
        wanted_share(),
        command,
        |_| {},
        deadline,
        run,
    )
}

/// The share this process asks for: the whole phase when its lease waits are recorded.
fn wanted_share() -> PhaseShare {
    match *RECORD {
        Some(_) => PhaseShare::Alone,
        None => PhaseShare::Shared,
    }
}

/// Ask for `share` of a `class` phase and run `run` under the grant, or unleased with the reason
/// said. A gateway that predates `alone` leases is asked again at once, shared, so the command
/// it refused still runs leased.
fn lease_then<T, E: fmt::Display>(
    socket: &Path,
    class: DiskClass,
    share: PhaseShare,
    command: &str,
    unleased_by: impl Fn(&Unleased),
    deadline: Option<CommandDeadline>,
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    if deadline.is_some_and(|deadline| deadline.remaining().is_none()) {
        return run();
    }
    let waiting = RECORD.as_ref().map(|record| record.begin(class));
    let stream = match connect(socket, deadline) {
        Ok(stream) => stream,
        Err(error) => {
            if !ABSENT_SAID.swap(true, Ordering::Relaxed) {
                eprintln!(
                    "cowshed: disk-lease {class} unleased: the gateway's control socket {} does not \
                     answer ({error}); disk commands in this process run unleased while it is down",
                    socket.display()
                );
            }
            drop(waiting);
            return run();
        }
    };
    let lease = cowshed_os::timing::timed("disk-lease", format_args!("{class} wait"), || {
        acquire(
            stream,
            class,
            share,
            command,
            Bounds {
                deadline,
                ..Bounds::PRODUCTION
            },
        )
    });
    drop(waiting);
    match lease {
        Ok(_lease) => cowshed_os::timing::timed("disk-lease", format_args!("{class} phase"), run),
        Err(unleased) => {
            cowshed_os::timing::attribute(
                "disk-lease",
                format_args!("{class} wait"),
                "unleased",
                &unleased,
            );
            unleased_by(&unleased);
            match unleased {
                Unleased::Predates {
                    what: Predates::Alone,
                    ..
                } => lease_then(
                    socket,
                    class,
                    PhaseShare::Shared,
                    command,
                    unleased_by,
                    deadline,
                    run,
                ),
                _ => run(),
            }
        }
    }
}

/// Establish the existing lease connection within the SAFE command's remaining total budget.
/// No thread or connector outlives an error: the one owned descriptor closes on every failure.
fn connect(socket: &Path, deadline: Option<CommandDeadline>) -> io::Result<UnixStream> {
    // SAFETY: sockaddr_un's integer and byte-array fields all admit zero.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    let path = socket.as_os_str().as_bytes();
    if path.is_empty() || path.len() >= address.sun_path.len() || path.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the gateway control socket path is empty, contains NUL, or exceeds sockaddr_un",
        ));
    }
    address.sun_family = libc::sa_family_t::try_from(libc::AF_UNIX).map_err(io::Error::other)?;
    for (target, byte) in address.sun_path.iter_mut().zip(path.iter().copied()) {
        *target = libc::c_char::from_ne_bytes([byte]);
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + path.len() + 1;
    let length = libc::socklen_t::try_from(length).map_err(io::Error::other)?;
    #[cfg(target_os = "macos")]
    {
        address.sun_len = u8::try_from(length).map_err(io::Error::other)?;
    }
    let descriptor = cowshed_os::fork_lock::create(|| {
        // SAFETY: socket creates a fresh descriptor; ownership is transferred exactly once.
        let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if raw == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: raw is the fresh, uniquely owned socket descriptor just returned above.
        let descriptor = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: fcntl only changes flags on this owned descriptor. Both two-step flags
        // are installed under the existing exclusive fork lock, before any fork can copy it.
        if unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) } == -1
            || unsafe { libc::fcntl(raw, libc::F_SETFL, libc::O_NONBLOCK) } == -1
        {
            return Err(io::Error::last_os_error());
        }
        Ok(descriptor)
    })?;
    let stream = UnixStream::from(descriptor);
    if deadline.is_none() {
        // Protected/custom admission retains its blocking connect, outside the fork lock.
        stream.set_nonblocking(false)?;
    }
    if deadline.is_some_and(|deadline| deadline.remaining().is_none()) {
        return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
    }
    // SAFETY: address is a complete pathname sockaddr_un, its checked length fits the object,
    // and the stream owns the descriptor throughout the one connect call and optional poll.
    let connected = unsafe {
        libc::connect(
            stream.as_raw_fd(),
            std::ptr::from_ref(&address).cast(),
            length,
        )
    };
    if connected == -1 {
        let error = io::Error::last_os_error();
        let Some(deadline) = deadline else {
            return Err(error);
        };
        if !matches!(
            error.raw_os_error(),
            Some(libc::EINPROGRESS | libc::EALREADY)
        ) {
            // In particular EAGAIN on a full AF_UNIX backlog is an immediate, explicit
            // refusal/fallback, not a timeout and not permission to retry the connection.
            return Err(error);
        }
        loop {
            let remaining = deadline
                .remaining()
                .ok_or_else(|| io::Error::from_raw_os_error(libc::ETIMEDOUT))?;
            let millis = remaining
                .as_millis()
                .saturating_add(u128::from(remaining.subsec_nanos() % 1_000_000 != 0));
            let timeout = i32::try_from(millis).unwrap_or(i32::MAX);
            let mut pending = libc::pollfd {
                fd: stream.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            };
            // SAFETY: pending is one initialized pollfd, live and exclusively borrowed here.
            let ready = unsafe { libc::poll(std::ptr::from_mut(&mut pending), 1, timeout) };
            if ready == -1 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue; // Recompute the original remaining budget, never restart it.
                }
                return Err(error);
            }
            if ready == 0 {
                continue; // A capped poll interval is not a fresh connection or deadline.
            }
            if let Some(error) = stream.take_error()? {
                return Err(error);
            }
            stream.peer_addr()?;
            break;
        }
    }
    if deadline.is_some_and(|deadline| deadline.remaining().is_none()) {
        return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT));
    }
    stream.set_nonblocking(false)?;
    Ok(stream)
}

/// A held lease: the open connection to the gateway. Dropping it releases the lease.
#[derive(Debug)]
pub struct DiskLease {
    _connection: UnixStream,
}

/// Why a command runs without a lease.
#[derive(Debug)]
pub enum Unleased {
    /// The gateway answered with a refusal.
    Refused { code: String, error: String },
    /// The gateway predates `what`. It is not asked for `what` again for [`PREDATES_RECHECK`].
    Predates { what: Predates, error: String },
    /// The gateway queued nothing within [`Bounds::ack`], yet acknowledged once asked to finish:
    /// it is alive but too slow to schedule this command in time.
    Slow,
    /// The connection broke or said something that is not an answer.
    Broken(io::Error),
    /// The gateway queued the request but granted nothing within [`Bounds::grant`].
    NotGranted(Duration),
    /// The SAFE command's total admission-and-child budget is exhausted.
    BudgetExhausted,
}

/// What a gateway from an older build predates (05_gateway.md, "Disk-lifecycle lease").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Predates {
    /// Disk leases: it said nothing until the request was half-closed, then refused the
    /// operation. Its commands run unleased.
    Leases,
    /// Asking for the whole phase: it refused the request's `share` at once. It is asked shared.
    Alone,
}

/// The end of the refusal a lease-scheduling gateway answers a request field it does not know.
const UNKNOWN_FIELD: &str = "contains an unknown field";

impl fmt::Display for Unleased {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { code, error } => {
                write!(formatter, "the gateway refused the lease ({code}): {error}")
            }
            Self::Predates {
                what: Predates::Leases,
                error,
            } => write!(
                formatter,
                "the gateway predates disk leases ({error}); restart it from this build with \
                 `cowshed setup`"
            ),
            Self::Predates {
                what: Predates::Alone,
                error,
            } => write!(
                formatter,
                "the gateway predates alone disk leases ({error}), so it is asked shared; restart \
                 it from this build with `cowshed setup`"
            ),
            Self::Slow => formatter.write_str(
                "the gateway did not queue the request in time; it is alive but overloaded",
            ),
            Self::Broken(error) => write!(formatter, "the lease connection broke: {error}"),
            Self::BudgetExhausted => formatter
                .write_str("the command's total deadline expired during disk-lease admission"),
            Self::NotGranted(waited) => {
                write!(formatter, "the gateway granted nothing within {waited:?}")
            }
        }
    }
}

/// How long each step of [`acquire`] waits on the gateway.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Bounds {
    /// For the `queued` answer the gateway sends at once. Past it, the request is half-closed: a
    /// gateway that predates leases has been waiting for exactly that EOF and now refuses.
    pub ack: Duration,
    /// For any answer after the half-close.
    pub after_close: Duration,
    /// For the grant, once queued: the bound every disk child answers inside.
    pub grant: Duration,
    /// Present only for SAFE commands with a total monotonic admission-and-child budget.
    pub deadline: Option<CommandDeadline>,
}

impl Bounds {
    fn limit(self, requested: Duration) -> Result<Duration, Unleased> {
        match self.deadline {
            Some(deadline) => deadline
                .remaining()
                .map(|remaining| requested.min(remaining))
                .ok_or(Unleased::BudgetExhausted),
            None => Ok(requested),
        }
    }
    pub(crate) const PRODUCTION: Self = Self {
        ack: Duration::from_secs(2),
        after_close: Duration::from_secs(5),
        grant: DISK_CHILD_DEADLINE,
        deadline: None,
    };
}

/// How long a process stops asking a gateway that predates leases, or `alone` leases, before it
/// asks again: the gateway may be replaced by `cowshed setup` while a long-lived process runs.
const PREDATES_RECHECK: Duration = Duration::from_secs(60);

/// When this process last found the host gateway predating leases or `alone` leases: the
/// milliseconds after [`EPOCH`] plus one, shifted left once, with the low bit set for `alone`;
/// zero for never.
static PREDATES_SEEN: AtomicU64 = AtomicU64::new(0);
static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);
static ABSENT_SAID: AtomicBool = AtomicBool::new(false);

fn since_epoch() -> u64 {
    u64::try_from(EPOCH.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn predates_recently() -> Option<Predates> {
    let seen = PREDATES_SEEN.load(Ordering::Relaxed);
    let at = (seen >> 1).checked_sub(1)?;
    let recheck = u64::try_from(PREDATES_RECHECK.as_millis()).unwrap_or(u64::MAX);
    (since_epoch().saturating_sub(at) < recheck).then_some(if seen & 1 == 1 {
        Predates::Alone
    } else {
        Predates::Leases
    })
}

fn remember_predates(unleased: &Unleased) {
    if let Unleased::Predates { what, .. } = unleased {
        let at = since_epoch().saturating_add(1).min(u64::MAX >> 1);
        PREDATES_SEEN.store(
            (at << 1) | u64::from(*what == Predates::Alone),
            Ordering::Relaxed,
        );
    }
}

/// Names the file a test's lease waits are appended to; unset outside the real-APFS lane.
pub const LEASE_WAIT_RECORD_ENV: &str = "COWSHED_LEASE_WAIT_RECORD";

/// This process's lease-wait record, when its environment names one.
static RECORD: LazyLock<Option<WaitRecord>> =
    LazyLock::new(|| std::env::var_os(LEASE_WAIT_RECORD_ENV).map(WaitRecord::new));

/// The file every lease wait of one test's process tree is appended to, one line per edge:
/// `start <pid> <seq> <unix-µs> <class>` before the client asks, and `end <pid> <seq> <unix-µs>`
/// once the wait is over, granted or not. Each line is one append, so the lines of concurrent
/// processes never interleave. The start goes first so that a process killed while waiting still
/// leaves its wait in the record, open, for the judge to name.
#[derive(Debug)]
struct WaitRecord {
    path: PathBuf,
    next: AtomicU64,
    failed: AtomicBool,
}

/// One wait in progress; dropping it, on any path, unwinding included, appends its end.
struct Waiting<'a> {
    record: &'a WaitRecord,
    seq: u64,
}

impl WaitRecord {
    fn new(path: std::ffi::OsString) -> Self {
        Self {
            path: PathBuf::from(path),
            next: AtomicU64::new(0),
            failed: AtomicBool::new(false),
        }
    }

    fn begin(&self, class: DiskClass) -> Waiting<'_> {
        let seq = self.next.fetch_add(1, Ordering::Relaxed);
        self.append(&format!(
            "start {} {seq} {} {class}\n",
            std::process::id(),
            unix_micros()
        ));
        Waiting { record: self, seq }
    }

    /// Append one line. A record that cannot be written is said once per process: this process's
    /// waits then count as the test's code time, which can only fail the bound, never pass it.
    fn append(&self, line: &str) {
        if let Err(error) = append_line(&self.path, line)
            && !self.failed.swap(true, Ordering::Relaxed)
        {
            eprintln!(
                "cowshed: disk-lease wait record {} is unwritable ({error}); this process's lease \
                 waits count as its test's code time",
                self.path.display()
            );
        }
    }
}

/// Append `line` to `path` in one write, so the lines of concurrent processes never interleave.
fn append_line(path: &Path, line: &str) -> io::Result<()> {
    OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(path)
        .and_then(|mut file| file.write_all(line.as_bytes()))
}

/// Names the file every disk command of a process tree is appended to; unset by default. A test
/// run names one to count the images it attaches and the volumes it mounts (08_testing.md,
/// "Integration tests (real substrate)", Host limit).
pub const COMMAND_RECORD_ENV: &str = "COWSHED_DISK_COMMAND_RECORD";

/// This process's disk-command record, when its environment names one.
static COMMAND_RECORD: LazyLock<Option<CommandRecord>> = LazyLock::new(|| {
    std::env::var_os(COMMAND_RECORD_ENV).map(|path| CommandRecord {
        path: PathBuf::from(path),
        failed: AtomicBool::new(false),
    })
});

/// The file every disk command [`leased`] runs is appended to, one line per command, before it
/// runs: `<class> <program> <args…>`, as the lease names it. A run's attaches are its lines of
/// `diskutil image attach`, its mounts its lines of `mount_apfs`.
#[derive(Debug)]
struct CommandRecord {
    path: PathBuf,
    failed: AtomicBool,
}

impl CommandRecord {
    /// Append one command. A record that cannot be written is said once per process, so a count
    /// read from it is known to be short.
    fn append(&self, class: DiskClass, command: &str) {
        if let Err(error) = append_line(&self.path, &format!("{class} {command}\n"))
            && !self.failed.swap(true, Ordering::Relaxed)
        {
            eprintln!(
                "cowshed: disk command record {} is unwritable ({error}); this process's disk \
                 commands are missing from it",
                self.path.display()
            );
        }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.record.append(&format!(
            "end {} {} {}\n",
            std::process::id(),
            self.seq,
            unix_micros()
        ));
    }
}

/// Wall-clock microseconds since the Unix epoch: the one clock every process of a test shares.
fn unix_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_micros())
}

/// Ask for `share` of a `class` phase on `stream` for `command` and wait for the grant.
pub(crate) fn acquire(
    stream: UnixStream,
    class: DiskClass,
    share: PhaseShare,
    command: &str,
    bounds: Bounds,
) -> Result<DiskLease, Unleased> {
    let mut line = serde_json::to_vec(&DiskLeaseRequest::new(class, share, command))
        .map_err(|error| Unleased::Broken(io::Error::other(error)))?;
    line.push(b'\n');
    stream
        .set_write_timeout(Some(bounds.limit(bounds.ack)?))
        .map_err(Unleased::Broken)?;
    (&stream).write_all(&line).map_err(Unleased::Broken)?;
    let mut answers = BufReader::new(stream.try_clone().map_err(Unleased::Broken)?);
    let ack = bounds.limit(bounds.ack)?;
    match read_answer(&mut answers, ack, bounds.deadline) {
        Ok(answer) => expect(answer, LeaseState::Queued).map_err(|unleased| match unleased {
            Unleased::Refused { code, error }
                if share == PhaseShare::Alone
                    && code == "invalid-request"
                    && error.ends_with(UNKNOWN_FIELD) =>
            {
                Unleased::Predates {
                    what: Predates::Alone,
                    error,
                }
            }
            other => other,
        })?,
        Err(error) if timed_out(&error) => {
            stream
                .shutdown(std::net::Shutdown::Write)
                .map_err(Unleased::Broken)?;
            let after_close = bounds.limit(bounds.after_close)?;
            let answer = read_answer(&mut answers, after_close, bounds.deadline)
                .map_err(Unleased::Broken)?;
            return Err(match answer {
                DiskLeaseAnswer {
                    ok: false, error, ..
                } => Unleased::Predates {
                    what: Predates::Leases,
                    error: error.unwrap_or_default(),
                },
                DiskLeaseAnswer { .. } => Unleased::Slow,
            });
        }
        Err(error) => return Err(Unleased::Broken(error)),
    }
    let grant = bounds.limit(bounds.grant)?;
    match read_answer(&mut answers, grant, bounds.deadline) {
        Ok(answer) => expect(answer, LeaseState::Granted)?,
        Err(error) if timed_out(&error) => return Err(Unleased::NotGranted(grant)),
        Err(error) => return Err(Unleased::Broken(error)),
    }
    Ok(DiskLease {
        _connection: stream,
    })
}

fn expect(answer: DiskLeaseAnswer, state: LeaseState) -> Result<(), Unleased> {
    match answer {
        DiskLeaseAnswer {
            ok: true,
            lease: Some(lease),
            ..
        } if lease == state => Ok(()),
        DiskLeaseAnswer {
            ok: false,
            code,
            error,
            ..
        } => Err(Unleased::Refused {
            code: code.unwrap_or_default(),
            error: error.unwrap_or_default(),
        }),
        DiskLeaseAnswer { lease, .. } => Err(Unleased::Broken(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected a {state:?} answer, got {lease:?}"),
        ))),
    }
}

fn read_answer(
    answers: &mut BufReader<UnixStream>,
    within: Duration,
    deadline: Option<CommandDeadline>,
) -> io::Result<DiskLeaseAnswer> {
    let step = CommandDeadline::new(within);
    let mut line = Vec::new();
    loop {
        let remaining = step
            .remaining()
            .and_then(|remaining| match deadline {
                Some(deadline) => deadline.remaining().map(|total| total.min(remaining)),
                None => Some(remaining),
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the disk-lease answer deadline expired",
                )
            })?;
        answers.get_ref().set_read_timeout(Some(remaining))?;
        let available = answers.fill_buf()?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the gateway closed the lease connection",
            ));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |at| at + 1);
        line.extend_from_slice(&available[..consumed]);
        answers.consume(consumed);
        if newline.is_some() {
            return serde_json::from_slice(&line)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
        }
    }
}

/// A read timeout on a socket reads as `WouldBlock` on macOS and `TimedOut` elsewhere.
fn timed_out(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

/// `program args` as one line, for the gateway to name a holder it evicts.
fn describe(program: &Path, args: &[impl AsRef<OsStr>]) -> String {
    let mut command = program.to_string_lossy().into_owned();
    for argument in args {
        command.push(' ');
        command.push_str(&argument.as_ref().to_string_lossy());
    }
    command
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    /// A wait only the peer ends: by answering, or by closing its socket, which it does when it
    /// is dropped, its thread's panic included. The clock never decides it; a test stuck on it
    /// meets the per-test hang backstop long before it elapses.
    const PEER_ENDS: Duration = Duration::from_secs(3600);

    /// The bound of the one wait a scenario is about, where no answer ever comes: its elapsing is
    /// the scenario's event, and nothing else is in flight to race it.
    const NO_ANSWER: Duration = Duration::from_millis(200);

    /// Every answer the scenario needs is already in the client's receive buffer when it asks, so
    /// no wait can time out, however late the scheduler runs anything.
    const ANSWERED: Bounds = Bounds {
        ack: PEER_ENDS,
        after_close: PEER_ENDS,
        grant: PEER_ENDS,
        deadline: None,
    };

    /// A listening socket the client is already connected to, removed on drop. Directly under
    /// `/tmp`, which macOS links to `/private/tmp` and every Linux host has: a bind path is capped
    /// at `sun_path`'s 104 bytes.
    struct Socket(std::path::PathBuf);

    impl Drop for Socket {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn pair() -> (Socket, UnixListener, UnixStream) {
        let socket = Socket(std::path::PathBuf::from(format!(
            "/tmp/cowshed-dl-{}.sock",
            uuid::Uuid::new_v4().simple()
        )));
        let listener = UnixListener::bind(&socket.0).expect("bind");
        let client = UnixStream::connect(&socket.0).expect("connect");
        (socket, listener, client)
    }

    /// The gateway's end of the client's connection, holding `answers` before the client asks.
    fn answering(listener: &UnixListener, answers: &[u8]) -> UnixStream {
        let (mut server, _) = listener.accept().expect("accept");
        server.write_all(answers).expect("answer");
        server
    }

    #[test]
    fn a_full_control_backlog_is_bounded_and_expired_safe_admission_executes_no_payload() {
        let socket = Socket(std::path::PathBuf::from(format!(
            "/tmp/cowshed-dl-{}.sock",
            uuid::Uuid::new_v4().simple()
        )));
        let marker = socket.0.with_extension("payload");
        let listener = UnixListener::bind(&socket.0).expect("bind exact backlog fixture");
        // SAFETY: adjust only this fixture-owned listener's real kernel backlog.
        assert_eq!(
            unsafe { libc::listen(listener.as_raw_fd(), 1) },
            0,
            "{}",
            io::Error::last_os_error()
        );
        let mut queued = Vec::new();
        let refused = loop {
            match connect(&socket.0, Some(CommandDeadline::new(NO_ANSWER))) {
                Ok(stream) => queued.push(stream),
                Err(error) => break error,
            }
        };
        assert!(
            !queued.is_empty(),
            "the real listener admitted connections before becoming full"
        );
        assert!(
            matches!(
                refused.kind(),
                io::ErrorKind::WouldBlock
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::ConnectionRefused
            ),
            "full backlog refused with {refused}"
        );
        let request = crate::CommandRequest::new(
            "/bin/sh",
            [
                std::ffi::OsString::from("-c"),
                std::ffi::OsString::from("printf executed > \"$1\""),
                std::ffi::OsString::from("owned-backlog-child"),
                marker.clone().into_os_string(),
            ],
        );
        let outcome =
            crate::SystemCommandRunner.run_within(&request, NO_ANSWER, |deadline, run| {
                let deadline = deadline.expect("SAFE admission has the original budget");
                let refused = connect(&socket.0, Some(deadline))
                    .expect_err("the real kernel backlog remains full");
                assert!(
                    matches!(
                        refused.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::ConnectionRefused
                    ),
                    "{refused}"
                );
                // An immediate EAGAIN/refusal is not a timeout. This custom admission fixture
                // deliberately spends the same remaining budget before returning its unleased run.
                while let Some(remaining) = deadline.remaining() {
                    std::thread::park_timeout(remaining);
                }
                run()
            });
        let executed = marker.exists();
        if executed {
            std::fs::remove_file(&marker)
                .expect("remove only an unexpectedly executed fixture marker");
        }
        for _ in &queued {
            let (accepted, _) = listener.accept().expect("drain each actual queued client");
            drop(accepted);
        }
        drop(queued);
        // A failed pending connect owns no background worker or retained descriptor. Once the
        // real queue is drained a new connection succeeds, and only that client is accepted.
        let fresh = connect(&socket.0, Some(CommandDeadline::new(NO_ANSWER)))
            .expect("connect after draining the full backlog");
        let (accepted, _) = listener.accept().expect("accept the fresh owned client");
        drop(accepted);
        drop(fresh);
        listener
            .set_nonblocking(true)
            .expect("observe the drained real queue");
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock),
            "no failed connector was left queued"
        );
        assert!(
            matches!(outcome, Err(crate::CommandRunError { failure: crate::CommandRunFailure::Unspawned(budget), .. }) if budget == NO_ANSWER),
            "{outcome:?}"
        );
        assert!(
            !executed,
            "exhausted admission never executes the native payload"
        );
    }

    #[test]
    fn programs_are_classed_by_what_they_change() {
        for program in [DISKUTIL, HDIUTIL, NEWFS_APFS] {
            assert_eq!(class_of(Path::new(program)), Some(DiskClass::Storage));
        }
        for program in [MOUNT_APFS, UMOUNT] {
            assert_eq!(class_of(Path::new(program)), Some(DiskClass::Namespace));
        }
        for program in ["/sbin/fsck_apfs", "/bin/sh", "diskutil"] {
            assert_eq!(class_of(Path::new(program)), None, "{program}");
        }
    }

    /// Every disk tool cowshed runs passes `leased` — the runner's, root setup's, the tests' — so
    /// a Disk Arbitration rename is refused there, in every spelling diskutil accepts, before its
    /// run is ever called; diskutil's other verbs, and other programs' `rename`, go on to run.
    #[test]
    fn leased_refuses_a_disk_arbitration_rename_without_running_it() {
        for (program, verb) in [
            (DISKUTIL, "renameVolume"),
            (DISKUTIL, "rename"),
            (DISKUTIL, "RENAMEVOLUME"),
            ("diskutil", "ReName"),
        ] {
            let refused = leased(
                Path::new(program),
                &[verb, "/dev/disk3s5", "[cowshed] fixture · dispatch — main"],
                || -> io::Result<()> { panic!("{program} {verb} ran") },
            )
            .expect_err("a Disk Arbitration rename is refused");
            assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied, "{refused}");
            assert!(refused.to_string().contains("/dev/disk3s5"), "{refused}");
        }
        for (program, verb) in [
            (DISKUTIL, "info"),
            (DISKUTIL, "renameVolumes"),
            ("/bin/echo", "rename"),
            ("/sbin/fsck_apfs", "renameVolume"),
        ] {
            assert!(
                refuse_volume_rename(Path::new(program), &[verb]).is_ok(),
                "{program} {verb}"
            );
        }
    }

    #[test]
    fn a_lease_is_asked_by_one_line_queued_then_granted_and_held_until_dropped() {
        let (_socket, listener, client) = pair();
        let server = answering(
            &listener,
            b"{\"ok\":true,\"lease\":\"queued\"}\n{\"ok\":true,\"lease\":\"granted\"}\n",
        );
        let lease = acquire(
            client,
            DiskClass::Namespace,
            PhaseShare::Shared,
            "/sbin/umount /x",
            ANSWERED,
        )
        .expect("granted");
        let mut sent = BufReader::new(&server);
        let mut line = String::new();
        sent.read_line(&mut line).expect("request");
        assert_eq!(
            line,
            "{\"op\":\"disk-lease\",\"class\":\"namespace\",\"command\":\"/sbin/umount /x\"}\n"
        );
        server.set_nonblocking(true).expect("nonblocking");
        let held = io::Read::read(&mut sent, &mut [0; 1]);
        assert!(
            matches!(&held, Err(error) if error.kind() == io::ErrorKind::WouldBlock),
            "a held lease's connection stays open and silent: {held:?}"
        );
        drop(lease);
        server.set_nonblocking(false).expect("blocking");
        let mut rest = Vec::new();
        io::Read::read_to_end(&mut sent, &mut rest).expect("held until the client closes");
        assert!(rest.is_empty(), "the client sends nothing after its line");
    }

    #[test]
    fn a_gateway_that_waits_for_eof_is_recognized_as_predating_leases() {
        let (_socket, listener, client) = pair();
        let server = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            io::Read::read_to_end(&mut server, &mut request).expect("read to EOF");
            server
                .write_all(
                    b"{\"ok\":false,\"code\":\"invalid-request\",\"error\":\"unknown gateway control operation\"}\n",
                )
                .expect("answer");
        });
        // The ack wait is the only one the clock may end; every later one waits for the old
        // gateway's answer, which it writes on the half-close and nothing else.
        let started = Instant::now();
        let unleased = acquire(
            client,
            DiskClass::Storage,
            PhaseShare::Shared,
            "/usr/sbin/diskutil list",
            Bounds {
                ack: NO_ANSWER,
                ..ANSWERED
            },
        )
        .expect_err("an old gateway grants nothing");
        let waited = started.elapsed();
        server.join().expect("server");
        assert!(
            matches!(&unleased, Unleased::Predates { what: Predates::Leases, error } if error.contains("unknown")),
            "{unleased}"
        );
        assert!(
            waited >= NO_ANSWER,
            "the client half-closed after {waited:?}, before its ack bound"
        );
    }

    #[test]
    fn a_refusal_and_a_grant_that_never_comes_both_run_unleased() {
        let (_socket, listener, client) = pair();
        let _server = answering(
            &listener,
            b"{\"ok\":false,\"code\":\"rejected\",\"error\":\"stopped\"}\n",
        );
        let refused = acquire(
            client,
            DiskClass::Storage,
            PhaseShare::Shared,
            "x",
            ANSWERED,
        );
        assert!(
            matches!(&refused, Err(Unleased::Refused { code, .. }) if code == "rejected"),
            "{refused:?}"
        );

        let (_socket, listener, client) = pair();
        let mut server = answering(&listener, b"{\"ok\":true,\"lease\":\"queued\"}\n");
        let ungranted = acquire(
            client,
            DiskClass::Storage,
            PhaseShare::Shared,
            "x",
            Bounds {
                grant: NO_ANSWER,
                ..ANSWERED
            },
        );
        assert!(
            matches!(&ungranted, Err(Unleased::NotGranted(waited)) if *waited == NO_ANSWER),
            "{ungranted:?}"
        );
        let mut rest = Vec::new();
        io::Read::read_to_end(&mut server, &mut rest).expect("client closes");
    }

    /// What a lease-scheduling gateway from before `share` answers a request that names it.
    const UNKNOWN_SHARE: &[u8] = b"{\"ok\":false,\"code\":\"invalid-request\",\"error\":\"gateway control encoding failed: gateway control request contains an unknown field\"}\n";

    #[test]
    fn a_gateway_that_refuses_the_share_field_predates_alone_leases_only_when_asked_alone() {
        let (_socket, listener, client) = pair();
        let server = answering(&listener, UNKNOWN_SHARE);
        let unleased = acquire(client, DiskClass::Storage, PhaseShare::Alone, "x", ANSWERED)
            .expect_err("an older gateway refuses the field");
        assert!(
            matches!(&unleased, Unleased::Predates { what: Predates::Alone, error } if error.ends_with(UNKNOWN_FIELD)),
            "{unleased}"
        );
        let mut line = String::new();
        BufReader::new(&server)
            .read_line(&mut line)
            .expect("request");
        assert_eq!(
            line,
            "{\"op\":\"disk-lease\",\"class\":\"storage\",\"share\":\"alone\",\"command\":\"x\"}\n"
        );

        let (_socket, listener, client) = pair();
        let _server = answering(&listener, UNKNOWN_SHARE);
        let refused = acquire(
            client,
            DiskClass::Storage,
            PhaseShare::Shared,
            "x",
            ANSWERED,
        );
        assert!(
            matches!(&refused, Err(Unleased::Refused { code, .. }) if code == "invalid-request"),
            "a shared request names no share, so its refusal is only a refusal: {refused:?}"
        );
    }

    #[test]
    fn a_command_a_gateway_refuses_alone_is_asked_again_shared_and_runs_leased() {
        let socket = Socket(std::path::PathBuf::from(format!(
            "/tmp/cowshed-dl-{}.sock",
            uuid::Uuid::new_v4().simple()
        )));
        let listener = UnixListener::bind(&socket.0).expect("bind");
        let gateway = std::thread::spawn(move || {
            let granted: &[u8] =
                b"{\"ok\":true,\"lease\":\"queued\"}\n{\"ok\":true,\"lease\":\"granted\"}\n";
            let mut asked = Vec::new();
            for answer in [UNKNOWN_SHARE, granted] {
                let (mut server, _) = listener.accept().expect("accept");
                let mut line = String::new();
                BufReader::new(&server)
                    .read_line(&mut line)
                    .expect("request");
                asked.push(line);
                server.write_all(answer).expect("answer");
                // Until the client closes: at once after a refusal, after its command once granted.
                let mut rest = Vec::new();
                io::Read::read_to_end(&mut server, &mut rest).expect("client closes");
            }
            asked
        });
        let said = std::cell::RefCell::new(Vec::new());
        let ran = lease_then(
            &socket.0,
            DiskClass::Storage,
            PhaseShare::Alone,
            "x",
            |unleased| said.borrow_mut().push(unleased.to_string()),
            None,
            || Ok::<_, String>("ran"),
        );
        assert_eq!(ran, Ok("ran"));
        assert_eq!(
            gateway.join().expect("gateway"),
            [
                "{\"op\":\"disk-lease\",\"class\":\"storage\",\"share\":\"alone\",\"command\":\"x\"}\n",
                "{\"op\":\"disk-lease\",\"class\":\"storage\",\"command\":\"x\"}\n",
            ]
        );
        let said = said.into_inner();
        assert_eq!(
            said.len(),
            1,
            "only the alone ask went unleased; the shared one was granted: {said:?}"
        );
        assert!(said[0].contains("predates alone disk leases"), "{said:?}");
    }

    #[test]
    fn total_safe_budget_covers_queued_legacy_and_alone_admission_without_late_spawn() {
        enum Admission {
            Queued,
            Legacy,
            Alone,
        }
        for admission in [Admission::Queued, Admission::Legacy, Admission::Alone] {
            let socket = Socket(std::path::PathBuf::from(format!(
                "/tmp/cowshed-dl-{}.sock",
                uuid::Uuid::new_v4().simple()
            )));
            let marker = socket.0.with_extension("spawned");
            let listener = UnixListener::bind(&socket.0).expect("bind owned gateway");
            let (finished, finish) = std::sync::mpsc::channel::<()>();
            let legacy = matches!(admission, Admission::Legacy);
            let alone = matches!(admission, Admission::Alone);
            let gateway = std::thread::spawn(move || {
                if alone {
                    let (mut server, _) = listener.accept().expect("alone request");
                    let mut request = String::new();
                    BufReader::new(&server)
                        .read_line(&mut request)
                        .expect("read alone request");
                    assert!(request.contains("\"share\":\"alone\""));
                    server
                        .write_all(UNKNOWN_SHARE)
                        .expect("refuse only the old share field");
                }
                let (mut server, _) = listener.accept().expect("admission request");
                let mut request = String::new();
                BufReader::new(&server)
                    .read_line(&mut request)
                    .expect("read request");
                if !legacy {
                    server
                        .write_all(b"{\"ok\":true,\"lease\":\"queued\"}\n")
                        .expect("queue without grant");
                }
                let mut rest = Vec::new();
                io::Read::read_to_end(&mut server, &mut rest)
                    .expect("client closes or half-closes");
                // The old gateway remains silent after EOF too, so its after-close read must
                // also be clipped. Dropping the sender on any test failure releases this peer.
                let _ = finish.recv();
            });
            let budget = if legacy {
                Duration::from_millis(2100)
            } else {
                NO_ANSWER
            };
            let request = crate::CommandRequest::new(
                "/bin/sh",
                [
                    std::ffi::OsString::from("-c"),
                    std::ffi::OsString::from("printf spawned > \"$1\""),
                    std::ffi::OsString::from("admission-child"),
                    marker.clone().into_os_string(),
                ],
            );
            let error = crate::SystemCommandRunner
                .run_within(&request, budget, |deadline, run| {
                    assert!(
                        deadline.is_some(),
                        "the real SAFE runner supplies its budget"
                    );
                    lease_then(
                        &socket.0,
                        DiskClass::Storage,
                        if alone {
                            PhaseShare::Alone
                        } else {
                            PhaseShare::Shared
                        },
                        "owned-safe-stand-in",
                        |_| {},
                        deadline,
                        run,
                    )
                })
                .expect_err("admission exhausted the same total budget");
            finished.send(()).expect("finish owned gateway");
            gateway.join().expect("owned gateway");
            assert!(
                matches!(error.failure, crate::CommandRunFailure::Unspawned(deadline) if deadline == budget),
                "{error:?}"
            );
            assert!(!marker.exists(), "an over-budget child must never start");
        }
    }

    #[test]
    fn a_wait_is_recorded_as_a_start_before_it_and_an_end_after_it_however_it_ends() {
        let path = std::path::PathBuf::from(format!(
            "/tmp/cowshed-dl-{}.waits",
            uuid::Uuid::new_v4().simple()
        ));
        let record = WaitRecord::new(path.clone().into_os_string());
        let before = unix_micros();
        let first = record.begin(DiskClass::Storage);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _second = record.begin(DiskClass::Namespace);
            panic!("the command's wait unwinds");
        }));
        assert!(unwound.is_err());
        drop(first);
        let after = unix_micros();
        let text = std::fs::read_to_string(&path).expect("record");
        std::fs::remove_file(&path).expect("remove record");
        let pid = std::process::id().to_string();
        let lines: Vec<Vec<&str>> = text.lines().map(|line| line.split(' ').collect()).collect();
        let edges: Vec<(&str, &str, &str)> = lines
            .iter()
            .map(|fields| (fields[0], fields[1], fields[2]))
            .collect();
        assert_eq!(
            edges,
            [
                ("start", pid.as_str(), "0"),
                ("start", pid.as_str(), "1"),
                ("end", pid.as_str(), "1"),
                ("end", pid.as_str(), "0"),
            ]
        );
        assert_eq!(lines[0].get(4), Some(&"storage"));
        assert_eq!(lines[1].get(4), Some(&"namespace"));
        for fields in &lines {
            let at: u128 = fields[3].parse().expect("microseconds");
            assert!((before..=after).contains(&at), "{fields:?}");
        }
    }
}
