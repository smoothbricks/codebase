//! The wire protocol between a workspace supervisor and its warm exec hosts.
//!
//! The host itself — a process that holds one activated workspace shell and forks each command
//! with the job's own descriptors — lives in the `cowshed-shell` crate, linked only by binaries
//! that serve as hosts (the `cowshed` CLI). This module is what both sides share: the frame codec,
//! descriptor passing, the request and reply tags, and the host program registration.
//!
//! # Wire protocol
//!
//! One `SOCK_STREAM` socket on descriptor [`CONTROL_DESCRIPTOR`]. Every frame is a little-endian
//! `u32` payload length followed by the payload: a tag byte and its fields, each byte string
//! length-prefixed. Descriptors ride as `SCM_RIGHTS` on the first bytes of a request frame.
//! Requests are served strictly in order; the host is exclusive to one command at a time.
//! Linux command requests require a fourth descriptor: a controller-opened `cgroup.procs`
//! placement, entered only by the forked command and closed before any job program can use it.
//! The host stays outside the job's cgroup; a missing placement refuses the command. macOS
//! instead carries its existing observation-ready and observation-go descriptors after stdio.
//!
//! A command's exchange: `REQUEST_RUN` or `REQUEST_SCRIPT`, then `REPLY_NICE_REFUSED` (a child that
//! placed its own nice was refused it and never ran; the exchange ends) or `REPLY_STARTED`, any number of
//! `REQUEST_SIGNAL`s, `REPLY_EXITED` once the command's leader exited, more `REQUEST_SIGNAL`s while
//! its descendants run on, and `REQUEST_RELEASE` once the job concluded, answered by
//! `REPLY_RELEASED`. Until the release the host holds the leader unreaped, so its group id names
//! the job's group alone and a signal can reach descendants that outlive the leader. A signal the
//! host cannot deliver is answered by `REPLY_SIGNAL_FAILED` in the reply stream. A supervisor that
//! closes the socket while the host holds a command has the host end the command's group --
//! SIGTERM, a grace, SIGKILL -- and reap it. Linux supervisors retain their cgroup capability
//! independently of this exchange and use it to end every member, including changed groups.

use std::ffi::OsString;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use super::job_groups::{Birth, GroupLeader};
use crate::api::dto::ExitStatus;
use crate::error::PriorityFailure;

/// The argument that turns the `cowshed` binary into an exec host.
pub const SHELL_HOST_ARGUMENT: &str = "--cowshed-shell-host";

/// Where the supervisor stages the host executable inside the workspace. Every sandbox
/// profile denies writes beneath it, like the other controller-published files, so no job can
/// replace the program every later command's host runs as.
pub const SHELL_HOST_DIRECTORY: &str = ".cowshed/shell-host";

/// The descriptor the supervisor installs the host's end of the control socket at.
pub const CONTROL_DESCRIPTOR: RawFd = 3;

/// A frame larger than any valid request: argv is bounded at 1 MiB aggregate and the caller
/// environment is bounded by the same request limits.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Approve `.envrc` in the private trust store: `[path]`, descriptors `[stdout, stderr]`.
pub const REQUEST_APPROVE: u8 = 1;
/// Evaluate `.envrc` once: `[directory]`, descriptors `[stderr]`.
pub const REQUEST_ACTIVATE: u8 = 2;
/// Fork an argv into a new process group: `[argv, cwd, overlay, child nice]`, descriptors
/// `[stdin, stdout, stderr]`, followed on Linux by `[cgroup_placement]` and on macOS by
/// `[observation_ready, observation_go]`. Linux enters the cgroup before exec. The child nice
/// is [`FrameWriter::child_nice`].
pub const REQUEST_RUN: u8 = 3;
/// Run a rendered script: `[text, bindings, cwd, overlay, child nice]`, descriptors `[stdin,
/// stdout, stderr]`, followed on Linux by `[cgroup_placement]` and on macOS by
/// `[observation_ready, observation_go]`. The host parses before it forks. The child enters
/// its Linux cgroup immediately after fork, or waits at the macOS observation fence, then
/// builds a brush shell from the activated environment and runs the program. Descendants
/// inherit Linux cgroup membership; the child's own wait status is the job's.
pub const REQUEST_SCRIPT: u8 = 4;
/// Signal the held command's group: `[signal]`. Linux supervisors instead signal the job's
/// cgroup directly, including descendants that changed groups or outlived their host. Sent
/// between `REPLY_STARTED` and `REQUEST_RELEASE`. The host is the command's parent and holds
/// the leader unreaped until the release, so the group id names the job's group alone.
/// Unanswered when delivered; a failure is answered with `REPLY_SIGNAL_FAILED`.
pub const REQUEST_SIGNAL: u8 = 5;
/// The job concluded: reap its command `[]`. Sent once, after `REPLY_EXITED`; answered by
/// `REPLY_RELEASED`, after which the host serves its next command.
pub const REQUEST_RELEASE: u8 = 6;

pub const REPLY_APPROVED: u8 = 1;
pub const REPLY_ACTIVATION_EXITED: u8 = 2;
pub const REPLY_ACTIVATED: u8 = 3;
pub const REPLY_ACTIVATION_UNUSABLE: u8 = 4;
/// The command started: `[birth]`, its group leader as the host observed it before it could
/// reap the command ([`FrameWriter::birth`]).
pub const REPLY_STARTED: u8 = 5;
/// The command's leader exited, or the argv could not be executed: `[exit]`
/// ([`FrameWriter::exit`]). A started command stays held, unreaped, until `REQUEST_RELEASE`.
pub const REPLY_EXITED: u8 = 6;
/// A script request's text did not parse; nothing ran. `[diagnostic]`, also written to the
/// job's stderr.
pub const REPLY_SCRIPT_SYNTAX: u8 = 7;
/// The host could not serve the request it was handling: `[reason]`. The host exits after
/// sending it, so the supervisor learns why instead of seeing only a closed socket.
pub const REPLY_HOST_FAILED: u8 = 8;
/// The held command was reaped: `[]`.
pub const REPLY_RELEASED: u8 = 9;
/// A `REQUEST_SIGNAL` could not be delivered to the command's group: `[signal, reason]`.
pub const REPLY_SIGNAL_FAILED: u8 = 10;
/// The forked command could not run at its nice and never ran: `[priority failure]`
/// ([`FrameWriter::priority_failure`]). Sent instead of `REPLY_STARTED`/`REPLY_EXITED`; the
/// host holds nothing and serves its next command.
pub const REPLY_NICE_REFUSED: u8 = 11;

/// A script binding's value kind on the wire.
pub const BINDING_SCALAR: u8 = 0;
pub const BINDING_ARRAY: u8 = 1;

/// A [`Birth`]'s kind on the wire.
const BIRTH_OBSERVED: u32 = 0;
const BIRTH_UNOBSERVED: u32 = 1;

/// An [`ExitStatus`]'s kind on the wire.
const EXIT_CODE: u32 = 0;
const EXIT_SIGNAL: u32 = 1;

/// A command request's child nice on the wire ([`FrameWriter::child_nice`]).
const CHILD_NICE_SUPERVISED: u32 = 0;
const CHILD_NICE_SELF: u32 = 1;

/// A raw `waitpid` status, decoded only where it is reported.
pub type RawWaitStatus = i32;

// ---------------------------------------------------------------------------------------------
// Frame codec

#[derive(Default)]
pub struct FrameWriter(Vec<u8>);

impl FrameWriter {
    pub fn new(tag: u8) -> Self {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&[0; 4]);
        bytes.push(tag);
        Self(bytes)
    }

    pub fn u32(mut self, value: u32) -> Self {
        self.0.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub fn i32(mut self, value: i32) -> Self {
        self.0.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// A command request's last field: `[0]` when the supervisor sets the command's nice on the
    /// held child through the observation fence (macOS), `[1, nice]` when the forked child sets
    /// it itself before it runs anything ([`report_own_nice`]), because no hold exists to set it
    /// through.
    pub fn child_nice(self, nice: Option<i32>) -> Self {
        match nice {
            None => self.u32(CHILD_NICE_SUPERVISED),
            Some(nice) => self.u32(CHILD_NICE_SELF).i32(nice),
        }
    }

    /// `[pid, requested, observed kind, observed, errno kind, errno]`: kind 1 names an observed
    /// value, kind 0 none (its value then 0). Nothing absent is filled in.
    pub fn priority_failure(self, failure: &PriorityFailure) -> Self {
        let optional = |frame: Self, value: Option<i32>| match value {
            Some(value) => frame.u32(1).i32(value),
            None => frame.u32(0).i32(0),
        };
        let frame = self.u32(failure.pid).i32(failure.requested);
        let frame = optional(frame, failure.observed);
        optional(frame, failure.errno)
    }

    pub fn u64(mut self, value: u64) -> Self {
        self.0.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// `[kind]` then, for an observed leader, `[pgid, birth, start]`; for an unobserved one,
    /// `[pid, reason]`.
    pub fn birth(self, birth: &Birth) -> io::Result<Self> {
        match birth {
            Birth::Observed(leader) => Ok(self
                .u32(BIRTH_OBSERVED)
                .i32(leader.pgid())
                .u64(leader.birth())
                .u64(leader.start())),
            Birth::Unobserved { pid, reason } => self
                .u32(BIRTH_UNOBSERVED)
                .u32(*pid)
                .bytes(reason.as_bytes()),
        }
    }

    /// `[kind, value, core dumped]`: an exit code, or a terminating signal.
    pub fn exit(self, exit: &ExitStatus) -> Self {
        match *exit {
            ExitStatus::Exited { code } => self.u32(EXIT_CODE).i32(code).u32(0),
            ExitStatus::Signaled {
                signal,
                core_dumped,
            } => self
                .u32(EXIT_SIGNAL)
                .i32(signal)
                .u32(u32::from(core_dumped)),
        }
    }

    pub fn bytes(mut self, value: &[u8]) -> io::Result<Self> {
        let length = u32::try_from(value.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "field exceeds u32"))?;
        self = self.u32(length);
        self.0.extend_from_slice(value);
        Ok(self)
    }

    pub fn list<'a>(mut self, values: impl ExactSizeIterator<Item = &'a [u8]>) -> io::Result<Self> {
        let count = u32::try_from(values.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "list exceeds u32"))?;
        self = self.u32(count);
        for value in values {
            self = self.bytes(value)?;
        }
        Ok(self)
    }

    /// The complete frame: length prefix and payload.
    pub fn finish(mut self) -> io::Result<Vec<u8>> {
        let payload = self.0.len() - 4;
        if payload > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "control frame exceeds its bound",
            ));
        }
        let length = u32::try_from(payload).map_err(io::Error::other)?;
        self.0[..4].copy_from_slice(&length.to_le_bytes());
        Ok(self.0)
    }
}

pub struct FrameReader<'a> {
    payload: &'a [u8],
}

fn truncated() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "truncated control frame")
}

impl<'a> FrameReader<'a> {
    pub fn new(payload: &'a [u8]) -> io::Result<(u8, Self)> {
        let (&tag, payload) = payload.split_first().ok_or_else(truncated)?;
        Ok((tag, Self { payload }))
    }

    fn take(&mut self, count: usize) -> io::Result<&'a [u8]> {
        if self.payload.len() < count {
            return Err(truncated());
        }
        let (taken, rest) = self.payload.split_at(count);
        self.payload = rest;
        Ok(taken)
    }

    pub fn u32(&mut self) -> io::Result<u32> {
        let bytes: [u8; 4] = self.take(4)?.try_into().map_err(|_| truncated())?;
        Ok(u32::from_le_bytes(bytes))
    }

    pub fn i32(&mut self) -> io::Result<i32> {
        let bytes: [u8; 4] = self.take(4)?.try_into().map_err(|_| truncated())?;
        Ok(i32::from_le_bytes(bytes))
    }

    pub fn u64(&mut self) -> io::Result<u64> {
        let bytes: [u8; 8] = self.take(8)?.try_into().map_err(|_| truncated())?;
        Ok(u64::from_le_bytes(bytes))
    }

    /// [`FrameWriter::priority_failure`].
    pub fn priority_failure(&mut self) -> io::Result<PriorityFailure> {
        let pid = self.u32()?;
        let requested = self.i32()?;
        let observed = self.optional_i32()?;
        let errno = self.optional_i32()?;
        Ok(PriorityFailure {
            pid,
            requested,
            observed,
            errno,
        })
    }

    fn optional_i32(&mut self) -> io::Result<Option<i32>> {
        match (self.u32()?, self.i32()?) {
            (0, 0) => Ok(None),
            (1, value) => Ok(Some(value)),
            (kind, value) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("malformed optional value (kind {kind}, value {value})"),
            )),
        }
    }

    /// [`FrameWriter::child_nice`]: `None` when the supervisor places the held child itself.
    pub fn child_nice(&mut self) -> io::Result<Option<i32>> {
        match self.u32()? {
            CHILD_NICE_SUPERVISED => Ok(None),
            CHILD_NICE_SELF => self.i32().map(Some),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown child nice kind {other}"),
            )),
        }
    }

    pub fn birth(&mut self) -> io::Result<Birth> {
        match self.u32()? {
            BIRTH_OBSERVED => {
                let pgid = self.i32()?;
                let birth = self.u64()?;
                GroupLeader::from_parts(pgid, birth, self.u64()?).map(Birth::Observed)
            }
            BIRTH_UNOBSERVED => Ok(Birth::Unobserved {
                pid: self.u32()?,
                reason: String::from_utf8_lossy(self.bytes()?).into_owned(),
            }),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown birth kind {other}"),
            )),
        }
    }

    pub fn exit(&mut self) -> io::Result<ExitStatus> {
        let kind = self.u32()?;
        let value = self.i32()?;
        let core_dumped = self.u32()?;
        match (kind, core_dumped) {
            (EXIT_CODE, 0) => Ok(ExitStatus::Exited { code: value }),
            (EXIT_SIGNAL, 0 | 1) => Ok(ExitStatus::Signaled {
                signal: value,
                core_dumped: core_dumped == 1,
            }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("malformed exit status (kind {kind}, core flag {core_dumped})"),
            )),
        }
    }

    pub fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let length = usize::try_from(self.u32()?).map_err(|_| truncated())?;
        self.take(length)
    }

    pub fn list(&mut self) -> io::Result<Vec<&'a [u8]>> {
        let count = usize::try_from(self.u32()?).map_err(|_| truncated())?;
        if count > self.payload.len() / 4 {
            return Err(truncated());
        }
        (0..count).map(|_| self.bytes()).collect()
    }

    pub fn finish(self) -> io::Result<()> {
        if self.payload.is_empty() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing bytes in control frame",
            ))
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Child nice

/// Clear this thread's `errno`: `getpriority` returns -1 for a nice of -1 as well as for an
/// error, so only a cleared `errno` tells them apart. A plain store; async-signal-safe.
pub(crate) fn clear_errno() {
    // SAFETY: the location is this thread's own errno.
    #[cfg(target_os = "macos")]
    unsafe {
        *libc::__error() = 0;
    }
    // SAFETY: as above.
    #[cfg(target_os = "linux")]
    unsafe {
        *libc::__errno_location() = 0;
    }
}

/// A nice report's outcome on its pipe ([`report_own_nice`]).
const NICE_PLACED: u32 = 0;
const NICE_SET_REFUSED: u32 = 1;
const NICE_READ_REFUSED: u32 = 2;
const NICE_MISMATCH: u32 = 3;
/// `[outcome, pid, requested, observed, errno]`, each four little-endian bytes: within
/// `PIPE_BUF`, so one write lands whole.
const NICE_REPORT_BYTES: usize = 20;

/// The pipe a child that places its own nice reports on: both ends close-on-exec, made under
/// the fork lock, so no other spawn carries an end past its `exec` and the parent's read ends
/// once the child has written or ended.
pub fn nice_pipe() -> io::Result<(OwnedFd, OwnedFd)> {
    cowshed_os::fork_lock::create(|| {
        let mut ends = [0; 2];
        // SAFETY: space for both descriptors, protected from a concurrent fork until CLOEXEC.
        if unsafe { libc::pipe(ends.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: these fresh descriptors have no other owners.
        let (read, write) =
            unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
        for end in [&read, &write] {
            // SAFETY: a plain fcntl of this owned descriptor.
            if unsafe { libc::fcntl(end.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok((read, write))
    })
}

/// Called in a forked child before it runs anything of the job's (between fork and exec, or
/// before a script child reports its group) where no observation fence holds it: set this
/// process's nice to `nice`, read it back, and write exactly what happened to `report`
/// ([`read_nice_report`]) -- this pid, the nice asked for, the nice read back, the errno -- so
/// a refusal reaches the supervisor typed, never as a bare spawn errno. Only `getpid`,
/// `setpriority`, `getpriority`, `write` and an `errno` store run: no allocation, no lock.
/// `Err` unless the read-back equals `nice`, so a command that runs is known to run at it.
pub fn report_own_nice(report: RawFd, nice: i32) -> io::Result<()> {
    let errno = || io::Error::last_os_error().raw_os_error().unwrap_or(0);
    // SAFETY: plain system calls on this process's own identity and scheduling priority.
    let pid = unsafe { libc::getpid() };
    // SAFETY: as above.
    let (outcome, observed, cause) =
        if unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, nice) } == -1 {
            (NICE_SET_REFUSED, 0, errno())
        } else {
            clear_errno();
            // SAFETY: as above.
            let read = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
            let cause = errno();
            if read == -1 && cause != 0 {
                (NICE_READ_REFUSED, 0, cause)
            } else if read == nice {
                (NICE_PLACED, read, 0)
            } else {
                (NICE_MISMATCH, read, 0)
            }
        };
    let mut record = [0_u8; NICE_REPORT_BYTES];
    for (slot, value) in record.as_chunks_mut::<4>().0.iter_mut().zip([
        outcome.to_le_bytes(),
        pid.to_le_bytes(),
        nice.to_le_bytes(),
        observed.to_le_bytes(),
        cause.to_le_bytes(),
    ]) {
        slot.copy_from_slice(&value);
    }
    let mut written = 0;
    while written < record.len() {
        // SAFETY: writes from this stack buffer to the report descriptor this child holds.
        let wrote = unsafe {
            libc::write(
                report,
                record[written..].as_ptr().cast(),
                record.len() - written,
            )
        };
        match usize::try_from(wrote) {
            Ok(wrote) => written += wrote,
            Err(_) if errno() == libc::EINTR => {}
            Err(_) => return Err(io::Error::last_os_error()),
        }
    }
    match outcome {
        NICE_PLACED => Ok(()),
        _ if cause != 0 => Err(io::Error::from_raw_os_error(cause)),
        _ => Err(io::Error::from(io::ErrorKind::Other)),
    }
}

/// What a child wrote with [`report_own_nice`], read once the parent dropped its own write end:
/// `None` when the child ended before placing its nice (its spawn failed earlier), the nice it
/// was observed at, or the exact failure. A torn or unknown report is `InvalidData`.
pub fn read_nice_report(
    report: OwnedFd,
) -> io::Result<Option<std::result::Result<i32, PriorityFailure>>> {
    use std::io::Read as _;
    let mut file = std::fs::File::from(report);
    let mut record = [0_u8; NICE_REPORT_BYTES];
    let mut filled = 0;
    while filled < record.len() {
        match file.read(&mut record[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    if filled == 0 {
        return Ok(None);
    }
    if filled < record.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("truncated nice report ({filled} of {NICE_REPORT_BYTES} bytes)"),
        ));
    }
    let field = |index: usize| {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(&record[index * 4..index * 4 + 4]);
        bytes
    };
    let outcome = u32::from_le_bytes(field(0));
    let pid = u32::from_le_bytes(field(1));
    let requested = i32::from_le_bytes(field(2));
    let observed = i32::from_le_bytes(field(3));
    let errno = i32::from_le_bytes(field(4));
    let failure = |observed, errno| PriorityFailure {
        pid,
        requested,
        observed,
        errno,
    };
    match outcome {
        NICE_PLACED => Ok(Some(Ok(observed))),
        NICE_SET_REFUSED | NICE_READ_REFUSED => Ok(Some(Err(failure(None, Some(errno))))),
        NICE_MISMATCH => Ok(Some(Err(failure(Some(observed), None)))),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown nice report outcome {other}"),
        )),
    }
}

// ---------------------------------------------------------------------------------------------
// SCM_RIGHTS

/// Send `frame` with `descriptors` attached to its first byte. Returns how many bytes of the
/// frame the kernel accepted; the caller writes the rest without descriptors.
pub fn send_with_descriptors(
    socket: RawFd,
    frame: &[u8],
    descriptors: &[RawFd],
) -> io::Result<usize> {
    let space = std::mem::size_of_val(descriptors);
    let space = u32::try_from(space).map_err(io::Error::other)?;
    // SAFETY: CMSG_SPACE is a pure size computation.
    let control_len = unsafe { libc::CMSG_SPACE(space) } as usize;
    let mut control = vec![0_u8; control_len];
    let mut iov = libc::iovec {
        iov_base: frame.as_ptr().cast_mut().cast(),
        iov_len: frame.len(),
    };
    // SAFETY: an all-zero msghdr is a valid empty header on every supported platform.
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = &mut iov;
    header.msg_iovlen = 1;
    if !descriptors.is_empty() {
        header.msg_control = control.as_mut_ptr().cast();
        header.msg_controllen = control_len as _;
        // SAFETY: `header` points at `control`, which has CMSG_SPACE(space) bytes, so the first
        // header and its data area are in bounds; the copy writes exactly `space` bytes.
        unsafe {
            let message = libc::CMSG_FIRSTHDR(&header);
            (*message).cmsg_level = libc::SOL_SOCKET;
            (*message).cmsg_type = libc::SCM_RIGHTS;
            (*message).cmsg_len = libc::CMSG_LEN(space) as _;
            std::ptr::copy_nonoverlapping(
                descriptors.as_ptr(),
                libc::CMSG_DATA(message).cast::<RawFd>(),
                descriptors.len(),
            );
        }
    }
    loop {
        // SAFETY: `header` references live buffers for the duration of the call.
        let sent = unsafe { libc::sendmsg(socket, &header, 0) };
        if sent >= 0 {
            return usize::try_from(sent).map_err(io::Error::other);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Receive into `buffer`, collecting any descriptors attached to the bytes read.
fn receive_with_descriptors(
    socket: RawFd,
    buffer: &mut [u8],
    descriptors: &mut Vec<OwnedFd>,
) -> io::Result<usize> {
    const MAX_DESCRIPTORS: usize = 8;
    let space =
        u32::try_from(MAX_DESCRIPTORS * std::mem::size_of::<RawFd>()).map_err(io::Error::other)?;
    // SAFETY: CMSG_SPACE is a pure size computation.
    let control_len = unsafe { libc::CMSG_SPACE(space) } as usize;
    let mut control = vec![0_u8; control_len];
    let mut iov = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    // SAFETY: an all-zero msghdr is a valid empty header.
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = &mut iov;
    header.msg_iovlen = 1;
    header.msg_control = control.as_mut_ptr().cast();
    header.msg_controllen = control_len as _;
    // Linux installs every received descriptor close-on-exec atomically, including the cgroup
    // placement authority, so no command can inherit it through a concurrent exec.
    #[cfg(target_os = "linux")]
    let flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let flags = 0;
    let received = loop {
        // SAFETY: `header` references live, writable buffers for the duration of the call.
        let received = unsafe { libc::recvmsg(socket, &mut header, flags) };
        if received >= 0 {
            break usize::try_from(received).map_err(io::Error::other)?;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    // SAFETY: the kernel filled `header.msg_control` with `msg_controllen` valid bytes; the
    // CMSG macros walk only within that length.
    unsafe {
        let mut message = libc::CMSG_FIRSTHDR(&header);
        while !message.is_null() {
            if (*message).cmsg_level == libc::SOL_SOCKET && (*message).cmsg_type == libc::SCM_RIGHTS
            {
                let data = libc::CMSG_DATA(message).cast::<RawFd>();
                let bytes = (*message).cmsg_len as usize
                    - (data.cast::<u8>().offset_from(message.cast::<u8>()) as usize);
                for index in 0..bytes / std::mem::size_of::<RawFd>() {
                    let raw = std::ptr::read_unaligned(data.add(index));
                    // Owned from here, so a malformed frame still closes what it carried.
                    let owned = OwnedFd::from_raw_fd(raw);
                    #[cfg(not(target_os = "linux"))]
                    libc::fcntl(owned.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC);
                    descriptors.push(owned);
                }
            }
            message = libc::CMSG_NXTHDR(&header, message);
        }
    }
    if header.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control frame carried more descriptors than the protocol allows",
        ));
    }
    Ok(received)
}

/// Read one frame and every descriptor that arrived with it. `None` at a clean end of stream.
pub fn read_frame(socket: &UnixStream) -> io::Result<Option<(Vec<u8>, Vec<OwnedFd>)>> {
    let mut descriptors = Vec::new();
    let mut header = [0_u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        let read =
            receive_with_descriptors(socket.as_raw_fd(), &mut header[filled..], &mut descriptors)?;
        if read == 0 {
            return if filled == 0 {
                Ok(None)
            } else {
                Err(truncated())
            };
        }
        filled += read;
    }
    let length = usize::try_from(u32::from_le_bytes(header)).map_err(io::Error::other)?;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control frame exceeds its bound",
        ));
    }
    let mut payload = vec![0_u8; length];
    let mut filled = 0;
    while filled < length {
        let read =
            receive_with_descriptors(socket.as_raw_fd(), &mut payload[filled..], &mut descriptors)?;
        if read == 0 {
            return Err(truncated());
        }
        filled += read;
    }
    Ok(Some((payload, descriptors)))
}

// ---------------------------------------------------------------------------------------------
// Host

/// The command that starts an exec host: a binary that serves the protocol and the arguments
/// that select its host mode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShellHostProgram {
    executable: PathBuf,
    arguments: Vec<OsString>,
}

impl ShellHostProgram {
    /// A binary whose `main` is exactly `cowshed_shell::serve`.
    pub fn dedicated(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            arguments: Vec::new(),
        }
    }

    /// A binary that serves as a host when started with `arguments`.
    pub fn with_arguments(executable: impl Into<PathBuf>, arguments: Vec<OsString>) -> Self {
        Self {
            executable: executable.into(),
            arguments,
        }
    }

    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
}

static REGISTERED: std::sync::OnceLock<ShellHostProgram> = std::sync::OnceLock::new();

/// Record the program that starts this process's exec hosts. Workspace supervisors this
/// process starts run commands in warm shells only once a host program is registered; a
/// process without one (a Node addon) runs every command through one-shot activation.
pub fn register(program: ShellHostProgram) {
    let _ = REGISTERED.set(program);
}

/// The exec host program registered for this process, if any.
pub fn registered() -> Option<ShellHostProgram> {
    REGISTERED.get().cloned()
}

/// How an exec host its supervisor dropped ended, when that needs saying. No job waits on a
/// dropped host, so the supervisor's log is the only record of its end.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DroppedHostEnd {
    /// SIGBUS: the kernel could not supply a page the host touched. The host runs its program
    /// from the workspace mount ([`SHELL_HOST_DIRECTORY`]) and keeps SIGBUS at its default
    /// disposition, so this is how it ends once that mount is gone from under it: the next code
    /// it runs that is not resident cannot be paged in (11_shell.md, "Exec hosts").
    PageUnavailable { core_dumped: bool },
    /// Any end other than the clean exit an idle host makes at its controller's end of stream.
    Other(ExitStatus),
}

/// The end worth recording for a dropped host that ended with `exit`: `None` for exit 0.
pub fn dropped_host_end(exit: ExitStatus) -> Option<DroppedHostEnd> {
    match exit {
        ExitStatus::Exited { code: 0 } => None,
        ExitStatus::Signaled {
            signal: libc::SIGBUS,
            core_dumped,
        } => Some(DroppedHostEnd::PageUnavailable { core_dumped }),
        other => Some(DroppedHostEnd::Other(other)),
    }
}

impl std::fmt::Display for DroppedHostEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PageUnavailable { core_dumped } => write!(
                f,
                "died by SIGBUS{}: a page it touched could not be supplied, as when the workspace \
                 mount its program runs from is gone",
                if *core_dumped { " (core dumped)" } else { "" }
            ),
            Self::Other(ExitStatus::Exited { code }) => write!(f, "exited {code}"),
            Self::Other(ExitStatus::Signaled {
                signal,
                core_dumped,
            }) => write!(
                f,
                "died by signal {signal}{}",
                if *core_dumped { " (core dumped)" } else { "" }
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};

    use proptest::prelude::*;

    fn any_exit() -> impl Strategy<Value = ExitStatus> {
        prop_oneof![
            (0_i32..=255).prop_map(|code| ExitStatus::Exited { code }),
            (1_i32..=31, any::<bool>()).prop_map(|(signal, core_dumped)| ExitStatus::Signaled {
                signal,
                core_dumped
            }),
        ]
    }

    proptest! {
        /// A dropped host's end is recorded unless it is the clean exit; SIGBUS is named as the
        /// page the host could not be given, with its core flag; every other end is kept exactly.
        #[test]
        fn a_dropped_host_s_end_is_named_unless_it_exited_cleanly(exit in any_exit()) {
            let end = dropped_host_end(exit.clone());
            prop_assert_eq!(end.is_none(), exit == ExitStatus::Exited { code: 0 });
            prop_assert_eq!(
                matches!(end, Some(DroppedHostEnd::PageUnavailable { .. })),
                matches!(exit, ExitStatus::Signaled { signal: libc::SIGBUS, .. })
            );
            match end {
                Some(DroppedHostEnd::PageUnavailable { core_dumped }) => prop_assert_eq!(
                    exit,
                    ExitStatus::Signaled { signal: libc::SIGBUS, core_dumped }
                ),
                Some(DroppedHostEnd::Other(kept)) => prop_assert_eq!(kept, exit),
                None => {}
            }
        }
    }

    #[test]
    fn frames_round_trip_byte_exact_fields() {
        let argv: Vec<&[u8]> = vec![b"printf", b"%s", b"\xff\x00raw"];
        let frame = FrameWriter::new(REQUEST_RUN)
            .list(argv.iter().copied())
            .unwrap()
            .bytes(b"/w")
            .unwrap()
            .list([b"K".as_slice(), b"V"].into_iter())
            .unwrap()
            .finish()
            .unwrap();
        let length = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(length, frame.len() - 4);
        let (tag, mut fields) = FrameReader::new(&frame[4..]).unwrap();
        assert_eq!(tag, REQUEST_RUN);
        assert_eq!(fields.list().unwrap(), argv);
        assert_eq!(fields.bytes().unwrap(), b"/w");
        assert_eq!(fields.list().unwrap(), vec![b"K".as_slice(), b"V"]);
        fields.finish().unwrap();
    }

    /// A request's child nice crosses the wire as the side that places it: the supervisor
    /// through the fence, or the child itself with the exact value; an unknown kind is refused.
    #[test]
    fn a_child_nice_names_who_places_it() {
        for nice in [None, Some(0), Some(10)] {
            let frame = FrameWriter::new(REQUEST_RUN)
                .child_nice(nice)
                .finish()
                .unwrap();
            let (_, mut fields) = FrameReader::new(&frame[4..]).unwrap();
            assert_eq!(fields.child_nice().unwrap(), nice);
            fields.finish().unwrap();
        }
        let frame = FrameWriter::new(REQUEST_RUN).u32(7).finish().unwrap();
        let (_, mut fields) = FrameReader::new(&frame[4..]).unwrap();
        assert!(fields.child_nice().is_err());
    }

    /// A forked child places itself at its class's nice and reports the nice it reads back;
    /// lowering below the nice it inherited needs a privilege it lacks, and the report carries
    /// that refusal typed -- the child's own pid, the nice asked for, the kernel's errno, no
    /// observed value -- instead of a bare errno or a nice nobody saw.
    #[test]
    fn a_child_reports_its_own_nice_and_a_typed_refusal_to_lower_it() {
        let (placed_read, placed_write) = nice_pipe().unwrap();
        let (lowered_read, lowered_write) = nice_pipe().unwrap();
        let (placed_end, lowered_end) = (placed_write.as_raw_fd(), lowered_write.as_raw_fd());
        // SAFETY: the forked child runs only async-signal-safe calls and leaves with _exit.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork: {}", io::Error::last_os_error());
        if pid == 0 {
            let placed = report_own_nice(placed_end, 10).is_ok();
            let lowered = report_own_nice(lowered_end, 0).is_err();
            // SAFETY: ends the forked test child without the harness's destructors.
            unsafe { libc::_exit(if placed && lowered { 0 } else { 1 }) }
        }
        drop((placed_write, lowered_write));
        let mut status = 0;
        // SAFETY: reaps the child forked above.
        assert_eq!(unsafe { libc::waitpid(pid, &raw mut status, 0) }, pid);
        assert!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "the child placed itself at 10 and could not return to 0 (status {status})"
        );
        assert_eq!(read_nice_report(placed_read).unwrap(), Some(Ok(10)));
        let Some(Err(refused)) = read_nice_report(lowered_read).unwrap() else {
            panic!("lowering the nice is refused");
        };
        assert_eq!(
            (refused.pid, refused.requested, refused.observed),
            (pid.cast_unsigned(), 0, None)
        );
        assert!(
            refused
                .errno
                .is_some_and(|errno| errno == libc::EPERM || errno == libc::EACCES),
            "{refused:?}"
        );
    }

    /// A child that ended before it placed anything reports nothing, never a placement.
    #[test]
    fn an_unwritten_nice_report_is_none() {
        let (read, write) = nice_pipe().unwrap();
        drop(write);
        assert_eq!(read_nice_report(read).unwrap(), None);
    }

    /// A refusal crosses the wire exactly: an absent observation stays absent.
    #[test]
    fn a_priority_failure_crosses_the_wire_unchanged() {
        for failure in [
            PriorityFailure {
                pid: 41,
                requested: 10,
                observed: None,
                errno: Some(libc::EPERM),
            },
            PriorityFailure {
                pid: 41,
                requested: 0,
                observed: Some(5),
                errno: None,
            },
        ] {
            let frame = FrameWriter::new(REPLY_NICE_REFUSED)
                .priority_failure(&failure)
                .finish()
                .unwrap();
            let (tag, mut fields) = FrameReader::new(&frame[4..]).unwrap();
            assert_eq!(tag, REPLY_NICE_REFUSED);
            assert_eq!(fields.priority_failure().unwrap(), failure);
            fields.finish().unwrap();
        }
        let frame = FrameWriter::new(REPLY_NICE_REFUSED)
            .u32(41)
            .i32(10)
            .u32(0)
            .i32(7)
            .u32(0)
            .i32(0)
            .finish()
            .unwrap();
        let (_, mut fields) = FrameReader::new(&frame[4..]).unwrap();
        assert!(
            fields.priority_failure().is_err(),
            "an absent value names none"
        );
    }

    /// An observed leader always carries its identity, a non-positive group is refused, and an
    /// unknown kind is refused rather than read as some other shape.
    #[test]
    fn a_birth_crosses_the_wire_unchanged_and_a_malformed_one_is_refused() {
        let births = [
            Birth::Observed(GroupLeader::from_parts(41, 0, 0).unwrap()),
            Birth::Observed(GroupLeader::from_parts(41, u64::MAX, u64::MAX - 1).unwrap()),
            Birth::Unobserved {
                pid: 41,
                reason: "unreadable".into(),
            },
        ];
        for birth in births {
            let frame = FrameWriter::new(REPLY_STARTED)
                .birth(&birth)
                .unwrap()
                .finish()
                .unwrap();
            let (_, mut fields) = FrameReader::new(&frame[4..]).unwrap();
            assert_eq!(fields.birth().unwrap(), birth);
            fields.finish().unwrap();
        }
        for malformed in [
            FrameWriter::new(REPLY_STARTED)
                .u32(BIRTH_OBSERVED)
                .i32(0)
                .u64(7)
                .u64(7),
            FrameWriter::new(REPLY_STARTED).u32(9).i32(41).u64(7),
        ] {
            let frame = malformed.finish().unwrap();
            let (_, mut fields) = FrameReader::new(&frame[4..]).unwrap();
            assert!(fields.birth().is_err());
        }
    }

    /// An exit code and a signal death keep their kind across the wire; a code with a core flag,
    /// a core flag other than 0 or 1, and an unknown kind are refused rather than read as one.
    #[test]
    fn an_exit_crosses_the_wire_unchanged_and_a_malformed_one_is_refused() {
        let exits = [
            ExitStatus::Exited { code: 3 },
            ExitStatus::Exited { code: 255 },
            ExitStatus::Signaled {
                signal: libc::SIGKILL,
                core_dumped: false,
            },
            ExitStatus::Signaled {
                signal: libc::SIGSEGV,
                core_dumped: true,
            },
        ];
        for exit in exits {
            let frame = FrameWriter::new(REPLY_EXITED).exit(&exit).finish().unwrap();
            let (_, mut fields) = FrameReader::new(&frame[4..]).unwrap();
            assert_eq!(fields.exit().unwrap(), exit);
            fields.finish().unwrap();
        }
        for malformed in [
            FrameWriter::new(REPLY_EXITED).u32(EXIT_CODE).i32(3).u32(1),
            FrameWriter::new(REPLY_EXITED)
                .u32(EXIT_SIGNAL)
                .i32(9)
                .u32(2),
            FrameWriter::new(REPLY_EXITED).u32(7).i32(3).u32(0),
        ] {
            let frame = malformed.finish().unwrap();
            let (_, mut fields) = FrameReader::new(&frame[4..]).unwrap();
            assert!(fields.exit().is_err());
        }
    }

    #[test]
    fn a_truncated_or_padded_frame_is_refused() {
        let frame = FrameWriter::new(REPLY_EXITED).i32(7).finish().unwrap();
        let (_, mut fields) = FrameReader::new(&frame[4..frame.len() - 1]).unwrap();
        assert!(fields.i32().is_err());
        let mut padded = frame[4..].to_vec();
        padded.push(0);
        let (_, mut fields) = FrameReader::new(&padded).unwrap();
        fields.i32().unwrap();
        assert!(fields.finish().is_err());
        // A list count larger than the remaining bytes could hold is refused before allocating.
        let hostile = FrameWriter::new(REQUEST_RUN)
            .u32(u32::MAX)
            .finish()
            .unwrap();
        let (_, mut fields) = FrameReader::new(&hostile[4..]).unwrap();
        assert!(fields.list().is_err());
    }

    #[test]
    fn descriptors_ride_the_first_bytes_of_a_frame() {
        let (left, right) = UnixStream::pair().unwrap();
        let (reader, writer) = {
            let mut descriptors = [0; 2];
            // SAFETY: `descriptors` has room for the two ends pipe(2) writes.
            assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
            // SAFETY: both descriptors were just created and are owned here.
            unsafe {
                (
                    OwnedFd::from_raw_fd(descriptors[0]),
                    OwnedFd::from_raw_fd(descriptors[1]),
                )
            }
        };
        let frame = FrameWriter::new(REQUEST_ACTIVATE)
            .bytes(&vec![b'x'; 200_000])
            .unwrap()
            .finish()
            .unwrap();
        let sender = std::thread::spawn(move || {
            let sent =
                send_with_descriptors(left.as_raw_fd(), &frame, &[writer.as_raw_fd()]).unwrap();
            (&left).write_all(&frame[sent..]).unwrap();
            drop(writer);
            left
        });
        let (payload, mut descriptors) = read_frame(&right).unwrap().unwrap();
        let _left = sender.join().unwrap();
        assert_eq!(payload.len(), 1 + 4 + 200_000);
        assert_eq!(descriptors.len(), 1);
        let mut received = std::fs::File::from(descriptors.remove(0));
        received.write_all(b"through").unwrap();
        drop(received);
        let mut read_back = String::new();
        std::fs::File::from(reader)
            .read_to_string(&mut read_back)
            .unwrap();
        assert_eq!(read_back, "through");
    }
}
