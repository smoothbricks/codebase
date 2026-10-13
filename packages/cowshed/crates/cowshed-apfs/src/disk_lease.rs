//! The client half of the host disk-lifecycle lease (05_gateway.md, "Disk-lifecycle lease").
//!
//! Every disk tool cowshed runs — production and tests alike — runs inside [`leased`]: the
//! command's class is read off its program, a lease of that class is taken from the gateway's
//! control socket, and the command runs while the connection stays open. Two lifecycle spans
//! say what that cost: `disk-lease <class> wait` from asking to the grant, and
//! `disk-lease <class> phase` around the command the grant admitted.
//!
//! The lease spaces commands out; it never decides whether one runs. With no gateway to ask, a
//! gateway that predates leases, a refusal, or a wait past [`DISK_CHILD_DEADLINE`], the command
//! runs unleased and the wait span ends `status=err` with the reason beside it — except a gateway
//! that is not running at all, which is said once per process rather than once per command.
//!
//! A test's lease waits are the host's queue, not its code. When [`LEASE_WAIT_RECORD_ENV`] names
//! a file, every wait is also appended to it (08_testing.md, "Integration tests (real
//! substrate)"), so the real-APFS lane's wrapper can judge the test's code time without them.

use std::ffi::OsStr;
use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cowshed_gateway_types::{DiskClass, DiskLeaseAnswer, DiskLeaseRequest, LeaseState};

use crate::device::DISKUTIL;
use crate::{DISK_CHILD_DEADLINE, HDIUTIL, MOUNT_APFS, NEWFS_APFS, UMOUNT};

/// Classify disk-changing commands, not cosmetic labels. `diskutil renameVolume` changes
/// neither the mount table nor st_dev; concurrent renames did not slow five attach or
/// mount/unmount probes (05_gateway.md, "Disk-lifecycle lease"), so it takes no lease.
/// Other `diskutil` verbs, `hdiutil` and `newfs_apfs` use storage; mount tools use namespace.
pub fn class_of(program: &Path, args: &[impl AsRef<OsStr>]) -> Option<DiskClass> {
    let program = program.to_str()?;
    if program == DISKUTIL
        && args
            .first()
            .is_some_and(|argument| argument.as_ref() == "renameVolume")
    {
        return None;
    }
    if [DISKUTIL, HDIUTIL, NEWFS_APFS].contains(&program) {
        Some(DiskClass::Storage)
    } else if [MOUNT_APFS, UMOUNT].contains(&program) {
        Some(DiskClass::Namespace)
    } else {
        None
    }
}

/// Run `run`, the disk tool `program args`, under a lease of its class from the host gateway. A
/// program that is no disk tool runs at once, and so does every disk tool for a minute after the
/// host gateway was found to predate leases.
pub fn leased<T, E: fmt::Display>(
    program: &Path,
    args: &[impl AsRef<OsStr>],
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let Some(class) = class_of(program, args) else {
        return run();
    };
    if predates_recently() {
        return run();
    }
    let command = describe(program, args);
    let socket = cowshed_gateway_types::control_socket_path();
    lease_then(&socket, class, &command, remember_predates, run)
}

/// [`leased`] against the control socket at `socket`, for a command already classed and named.
/// Nothing about that gateway is remembered for the host gateway's sake.
pub fn leased_at<T, E: fmt::Display>(
    socket: &Path,
    class: DiskClass,
    command: &str,
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    lease_then(socket, class, command, |_| {}, run)
}

fn lease_then<T, E: fmt::Display>(
    socket: &Path,
    class: DiskClass,
    command: &str,
    unleased_by: impl FnOnce(&Unleased),
    run: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let waiting = RECORD.as_ref().map(|record| record.begin(class));
    let stream = match UnixStream::connect(socket) {
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
        acquire(stream, class, command, Bounds::PRODUCTION)
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
            run()
        }
    }
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
    /// The gateway predates disk leases: it said nothing until the request was half-closed,
    /// then refused the operation. Leases are not asked of it again for [`PREDATES_RECHECK`].
    Predates(String),
    /// The gateway queued nothing within [`Bounds::ack`], yet acknowledged once asked to finish:
    /// it is alive but too slow to schedule this command in time.
    Slow,
    /// The connection broke or said something that is not an answer.
    Broken(io::Error),
    /// The gateway queued the request but granted nothing within [`Bounds::grant`].
    NotGranted(Duration),
}

impl fmt::Display for Unleased {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { code, error } => {
                write!(formatter, "the gateway refused the lease ({code}): {error}")
            }
            Self::Predates(error) => write!(
                formatter,
                "the gateway predates disk leases ({error}); restart it from this build with \
                 `cowshed setup`"
            ),
            Self::Slow => formatter.write_str(
                "the gateway did not queue the request in time; it is alive but overloaded",
            ),
            Self::Broken(error) => write!(formatter, "the lease connection broke: {error}"),
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
}

impl Bounds {
    pub(crate) const PRODUCTION: Self = Self {
        ack: Duration::from_secs(2),
        after_close: Duration::from_secs(5),
        grant: DISK_CHILD_DEADLINE,
    };
}

/// How long a process stops asking a gateway that predates leases before it asks again: the
/// gateway may be replaced by `cowshed setup` while a long-lived process runs.
const PREDATES_RECHECK: Duration = Duration::from_secs(60);

/// When this process last found the host gateway predating leases, as milliseconds after
/// [`EPOCH`] plus one; zero for never.
static PREDATES_SEEN: AtomicU64 = AtomicU64::new(0);
static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);
static ABSENT_SAID: AtomicBool = AtomicBool::new(false);

fn since_epoch() -> u64 {
    u64::try_from(EPOCH.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn predates_recently() -> bool {
    match PREDATES_SEEN.load(Ordering::Relaxed) {
        0 => false,
        seen => {
            let recheck = u64::try_from(PREDATES_RECHECK.as_millis()).unwrap_or(u64::MAX);
            since_epoch().saturating_sub(seen - 1) < recheck
        }
    }
}

fn remember_predates(unleased: &Unleased) {
    if matches!(unleased, Unleased::Predates(_)) {
        PREDATES_SEEN.store(since_epoch() + 1, Ordering::Relaxed);
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
        let written = OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(&self.path)
            .and_then(|mut file| file.write_all(line.as_bytes()));
        if let Err(error) = written
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

/// Ask for a `class` lease on `stream` for `command` and wait for the grant.
pub(crate) fn acquire(
    stream: UnixStream,
    class: DiskClass,
    command: &str,
    bounds: Bounds,
) -> Result<DiskLease, Unleased> {
    let mut line = serde_json::to_vec(&DiskLeaseRequest::new(class, command))
        .map_err(|error| Unleased::Broken(io::Error::other(error)))?;
    line.push(b'\n');
    (&stream).write_all(&line).map_err(Unleased::Broken)?;
    let mut answers = BufReader::new(stream.try_clone().map_err(Unleased::Broken)?);
    stream
        .set_read_timeout(Some(bounds.ack))
        .map_err(Unleased::Broken)?;
    match read_answer(&mut answers) {
        Ok(answer) => expect(answer, LeaseState::Queued)?,
        Err(error) if timed_out(&error) => {
            stream
                .shutdown(std::net::Shutdown::Write)
                .map_err(Unleased::Broken)?;
            stream
                .set_read_timeout(Some(bounds.after_close))
                .map_err(Unleased::Broken)?;
            let answer = read_answer(&mut answers).map_err(Unleased::Broken)?;
            return Err(match answer {
                DiskLeaseAnswer {
                    ok: false, error, ..
                } => Unleased::Predates(error.unwrap_or_default()),
                DiskLeaseAnswer { .. } => Unleased::Slow,
            });
        }
        Err(error) => return Err(Unleased::Broken(error)),
    }
    stream
        .set_read_timeout(Some(bounds.grant))
        .map_err(Unleased::Broken)?;
    match read_answer(&mut answers) {
        Ok(answer) => expect(answer, LeaseState::Granted)?,
        Err(error) if timed_out(&error) => return Err(Unleased::NotGranted(bounds.grant)),
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

fn read_answer(answers: &mut BufReader<UnixStream>) -> io::Result<DiskLeaseAnswer> {
    let mut line = String::new();
    if answers.read_line(&mut line)? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the gateway closed the lease connection",
        ));
    }
    serde_json::from_str(&line).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
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

    const QUICK: Bounds = Bounds {
        ack: Duration::from_millis(200),
        after_close: Duration::from_secs(5),
        grant: Duration::from_millis(500),
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

    fn request_line(server: &UnixStream) -> String {
        let mut line = String::new();
        BufReader::new(server)
            .read_line(&mut line)
            .expect("request");
        line
    }

    #[test]
    fn programs_are_classed_by_what_they_change() {
        let no_args: [&str; 0] = [];
        for program in [DISKUTIL, HDIUTIL, NEWFS_APFS] {
            assert_eq!(
                class_of(Path::new(program), &no_args),
                Some(DiskClass::Storage)
            );
        }
        for program in [MOUNT_APFS, UMOUNT] {
            assert_eq!(
                class_of(Path::new(program), &no_args),
                Some(DiskClass::Namespace)
            );
        }
        for program in ["/sbin/fsck_apfs", "/bin/sh", "diskutil"] {
            assert_eq!(class_of(Path::new(program), &no_args), None, "{program}");
        }
    }

    #[test]
    fn cosmetic_diskutil_renames_do_not_take_a_storage_or_namespace_lease() {
        assert_eq!(
            class_of(Path::new(DISKUTIL), &["renameVolume", "/volume", "label"]),
            None
        );
        assert_eq!(
            class_of(Path::new(DISKUTIL), &["image", "attach", "/image"]),
            Some(DiskClass::Storage)
        );
        assert_eq!(
            class_of(Path::new(HDIUTIL), &["renameVolume"]),
            Some(DiskClass::Storage)
        );
        assert_eq!(
            class_of(Path::new(DISKUTIL), &["renameVolumeOther"]),
            Some(DiskClass::Storage)
        );
        assert_eq!(
            class_of(Path::new(DISKUTIL), &["rename", "/volume", "label"]),
            Some(DiskClass::Storage)
        );
    }

    #[test]
    fn a_lease_is_asked_by_one_line_queued_then_granted_and_held_until_dropped() {
        let (_socket, listener, client) = pair();
        let server = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().expect("accept");
            let line = request_line(&server);
            server
                .write_all(
                    b"{\"ok\":true,\"lease\":\"queued\"}\n{\"ok\":true,\"lease\":\"granted\"}\n",
                )
                .expect("answer");
            let mut rest = Vec::new();
            io::Read::read_to_end(&mut server, &mut rest).expect("held until the client closes");
            (line, rest)
        });
        let lease =
            acquire(client, DiskClass::Namespace, "/sbin/umount /x", QUICK).expect("granted");
        drop(lease);
        let (line, rest) = server.join().expect("server");
        assert_eq!(
            line,
            "{\"op\":\"disk-lease\",\"class\":\"namespace\",\"command\":\"/sbin/umount /x\"}\n"
        );
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
        let started = Instant::now();
        let unleased = acquire(client, DiskClass::Storage, "/usr/sbin/diskutil list", QUICK)
            .expect_err("an old gateway grants nothing");
        server.join().expect("server");
        assert!(
            matches!(&unleased, Unleased::Predates(error) if error.contains("unknown")),
            "{unleased}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn a_refusal_and_a_grant_that_never_comes_both_run_unleased() {
        let (_socket, listener, client) = pair();
        let server = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().expect("accept");
            request_line(&server);
            server
                .write_all(b"{\"ok\":false,\"code\":\"rejected\",\"error\":\"stopped\"}\n")
                .expect("answer");
        });
        assert!(matches!(
            acquire(client, DiskClass::Storage, "x", QUICK),
            Err(Unleased::Refused { code, .. }) if code == "rejected"
        ));
        server.join().expect("server");

        let (_socket, listener, client) = pair();
        let server = std::thread::spawn(move || {
            let (mut server, _) = listener.accept().expect("accept");
            request_line(&server);
            server
                .write_all(b"{\"ok\":true,\"lease\":\"queued\"}\n")
                .expect("answer");
            let mut rest = Vec::new();
            io::Read::read_to_end(&mut server, &mut rest).expect("client closes");
        });
        assert!(matches!(
            acquire(client, DiskClass::Storage, "x", QUICK),
            Err(Unleased::NotGranted(_))
        ));
        server.join().expect("server");
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
