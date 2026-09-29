//! The sandboxed exec host: one workspace shell environment, activated once, held by one
//! process, from which each command is forked with the job's own descriptors.
//!
//! # Why a host process and not a live bash
//!
//! A job owns anonymous stdin/stdout/stderr pipes that only the supervisor holds the other end
//! of, a process group the supervisor can signal, and an exact wait status (exit code, or
//! terminating signal and core flag). A long-lived bash can receive none of these per command:
//! it cannot accept descriptors over a socket, named FIFOs are reachable by sibling jobs, and
//! its `wait` folds a signal death into `128+N`. The host therefore is a small program that
//! holds the activated environment and does what bash cannot: it receives the job's descriptors
//! with `SCM_RIGHTS`, forks the argv into its own process group, and reports the raw `waitpid`
//! status. It runs under the executed-child Seatbelt profile like every command, so it adds no
//! authority, and activation still runs repository code only inside that profile.
//!
//! Activation is direnv's own: `direnv export json` evaluated from the host's sandbox
//! environment, its diff applied once. The resulting `DIRENV_WATCHES` is reported to the
//! supervisor, which decides freshness from it (`shell_watch`); the host never re-evaluates.
//!
//! # Wire protocol
//!
//! One `SOCK_STREAM` socket on descriptor [`CONTROL_DESCRIPTOR`]. Every frame is a little-endian
//! `u32` payload length followed by the payload: a tag byte and its fields, each byte string
//! length-prefixed. Descriptors ride as `SCM_RIGHTS` on the first bytes of a request frame.
//! Requests are served strictly in order; the host is exclusive to one command at a time.
//!
//! Tag [`REQUEST_SCRIPT`] is reserved for shell-text jobs interpreted by an embedded brush
//! (bash-compatible) interpreter inside this host. It is refused until that interpreter meets
//! its entry conditions, recorded on the constant.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::net::UnixStream;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// The argument that turns a cowshed-core binary into an exec host.
pub const SHELL_HOST_ARGUMENT: &str = "--cowshed-shell-host";

/// Where the supervisor stages the host executable inside the workspace. Every sandbox
/// profile denies writes beneath it, like the other controller-published files, so no job can
/// replace the program every later command's host runs as.
pub const SHELL_HOST_DIRECTORY: &str = ".cowshed/shell-host";

/// The descriptor the supervisor installs the host's end of the control socket at.
pub const CONTROL_DESCRIPTOR: RawFd = 3;

/// A frame larger than any valid request: argv is bounded at 1 MiB aggregate and the caller
/// environment is bounded by the same request limits.
pub(crate) const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Upper bound on `direnv export json` output kept in memory.
const MAX_EXPORT_BYTES: usize = 64 * 1024 * 1024;

pub(crate) const REQUEST_APPROVE: u8 = 1;
pub(crate) const REQUEST_ACTIVATE: u8 = 2;
pub(crate) const REQUEST_RUN: u8 = 3;
/// Reserved: run shell text through an embedded brush interpreter in this host. Its entry
/// conditions, each a gap in brush as vendored today:
///
/// 1. Process group: every process a script spawns joins ONE job process group that excludes
///    the host (a vendored `ProcessGroupPolicy::Join`), instead of brush's per-child `setsid`,
///    so the supervisor's TERM → grace → KILL reaches grandchildren; plus a host-side cancel so
///    `a; b` does not start `b` after `a` was killed.
/// 2. Exact status: the last foreground child's raw wait status is carried out instead of
///    brush's `128 + signal` fold, which loses the signal/exit distinction and the core flag.
/// 3. Process-wide builtins (`umask`, `ulimit`, `exec`, `trap`) are disabled or restored per
///    command, since they act on the host process and would leak into later commands; each
///    command runs in a clone of the activated `Shell`, so shell state itself never leaks.
/// 4. Byte-exactness: brush's words and environment are `String`. Argv jobs never pass through
///    it; only scripts, which are text by definition, do.
pub(crate) const REQUEST_SCRIPT: u8 = 4;

pub(crate) const REPLY_APPROVED: u8 = 1;
pub(crate) const REPLY_ACTIVATION_EXITED: u8 = 2;
pub(crate) const REPLY_ACTIVATED: u8 = 3;
pub(crate) const REPLY_ACTIVATION_UNUSABLE: u8 = 4;
pub(crate) const REPLY_STARTED: u8 = 5;
pub(crate) const REPLY_EXITED: u8 = 6;

/// A raw `waitpid` status, decoded only where it is reported.
pub(crate) type RawWaitStatus = i32;

/// The wait status a shell reports for a command it could not execute: 127 when the program
/// does not exist, 126 when it exists but cannot run.
fn unexecutable_status(error: &io::Error) -> RawWaitStatus {
    let code = if error.kind() == io::ErrorKind::NotFound {
        127
    } else {
        126
    };
    code << 8
}

// ---------------------------------------------------------------------------------------------
// Frame codec

#[derive(Default)]
pub(crate) struct FrameWriter(Vec<u8>);

impl FrameWriter {
    pub(crate) fn new(tag: u8) -> Self {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&[0; 4]);
        bytes.push(tag);
        Self(bytes)
    }

    pub(crate) fn u32(mut self, value: u32) -> Self {
        self.0.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub(crate) fn i32(mut self, value: i32) -> Self {
        self.0.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub(crate) fn bytes(mut self, value: &[u8]) -> io::Result<Self> {
        let length = u32::try_from(value.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "field exceeds u32"))?;
        self = self.u32(length);
        self.0.extend_from_slice(value);
        Ok(self)
    }

    pub(crate) fn list<'a>(
        mut self,
        values: impl ExactSizeIterator<Item = &'a [u8]>,
    ) -> io::Result<Self> {
        let count = u32::try_from(values.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "list exceeds u32"))?;
        self = self.u32(count);
        for value in values {
            self = self.bytes(value)?;
        }
        Ok(self)
    }

    /// The complete frame: length prefix and payload.
    pub(crate) fn finish(mut self) -> io::Result<Vec<u8>> {
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

pub(crate) struct FrameReader<'a> {
    payload: &'a [u8],
}

fn truncated() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "truncated control frame")
}

impl<'a> FrameReader<'a> {
    pub(crate) fn new(payload: &'a [u8]) -> io::Result<(u8, Self)> {
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

    pub(crate) fn u32(&mut self) -> io::Result<u32> {
        let bytes: [u8; 4] = self.take(4)?.try_into().map_err(|_| truncated())?;
        Ok(u32::from_le_bytes(bytes))
    }

    pub(crate) fn i32(&mut self) -> io::Result<i32> {
        let bytes: [u8; 4] = self.take(4)?.try_into().map_err(|_| truncated())?;
        Ok(i32::from_le_bytes(bytes))
    }

    pub(crate) fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let length = usize::try_from(self.u32()?).map_err(|_| truncated())?;
        self.take(length)
    }

    pub(crate) fn list(&mut self) -> io::Result<Vec<&'a [u8]>> {
        let count = usize::try_from(self.u32()?).map_err(|_| truncated())?;
        if count > self.payload.len() / 4 {
            return Err(truncated());
        }
        (0..count).map(|_| self.bytes()).collect()
    }

    pub(crate) fn finish(self) -> io::Result<()> {
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
// SCM_RIGHTS

/// Send `frame` with `descriptors` attached to its first byte. Returns how many bytes of the
/// frame the kernel accepted; the caller writes the rest without descriptors.
pub(crate) fn send_with_descriptors(
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
    let received = loop {
        // SAFETY: `header` references live, writable buffers for the duration of the call.
        let received = unsafe { libc::recvmsg(socket, &mut header, 0) };
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
fn read_frame(socket: &UnixStream) -> io::Result<Option<(Vec<u8>, Vec<OwnedFd>)>> {
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

/// The command that starts an exec host: a cowshed-core binary and the arguments that select
/// its host mode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShellHostProgram {
    executable: PathBuf,
    arguments: Vec<OsString>,
}

impl ShellHostProgram {
    /// A binary whose `main` is exactly [`serve`].
    pub fn dedicated(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            arguments: Vec::new(),
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

/// Serve as an exec host if this process was started as one, and never return in that case.
/// Otherwise register this same binary as the program that starts exec hosts, so workspace
/// supervisors this process starts run commands in warm shells.
///
/// A binary that hosts workspace supervisors calls this first in `main`, before any runtime or
/// thread exists: the host forks commands, and a single-threaded process is the one in which
/// that is unconditionally sound. A process that never calls it (a Node addon, a test binary)
/// runs every command through one-shot activation.
pub fn dispatch() -> io::Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    if arguments.next().as_deref() == Some(OsStr::new(SHELL_HOST_ARGUMENT)) {
        std::process::exit(serve());
    }
    let program = ShellHostProgram {
        executable: std::env::current_exe()?,
        arguments: vec![SHELL_HOST_ARGUMENT.into()],
    };
    let _ = REGISTERED.set(program);
    Ok(())
}

/// The exec host program [`dispatch`] registered for this process, if any.
pub fn registered() -> Option<ShellHostProgram> {
    REGISTERED.get().cloned()
}

/// Run the exec host on [`CONTROL_DESCRIPTOR`] until the supervisor closes it. Returns the
/// process exit code.
pub fn serve() -> i32 {
    // SAFETY: fcntl(F_GETFD) only queries the descriptor table.
    if unsafe { libc::fcntl(CONTROL_DESCRIPTOR, libc::F_GETFD) } < 0 {
        return 64;
    }
    // SAFETY: descriptor 3 was installed by the supervisor for this process alone and nothing
    // else in this process owns it.
    let socket = unsafe { UnixStream::from_raw_fd(CONTROL_DESCRIPTOR) };
    // SAFETY: plain fcntl on an owned descriptor; commands must not inherit the control socket.
    unsafe { libc::fcntl(CONTROL_DESCRIPTOR, libc::F_SETFD, libc::FD_CLOEXEC) };
    let mut host = Host {
        environment: std::env::vars_os().collect(),
    };
    loop {
        match read_frame(&socket) {
            Ok(Some((payload, descriptors))) => {
                if let Err(error) = host.handle(&socket, &payload, descriptors) {
                    // The host's own stderr is /dev/null; the supervisor observes the closed
                    // socket and the exit status.
                    let _ = error;
                    return 70;
                }
            }
            Ok(None) => return 0,
            Err(_) => return 65,
        }
    }
}

struct Host {
    environment: BTreeMap<OsString, OsString>,
}

fn one_descriptor(descriptors: &mut Vec<OwnedFd>, what: &str) -> io::Result<OwnedFd> {
    if descriptors.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("request carried no {what} descriptor"),
        ));
    }
    Ok(descriptors.remove(0))
}

fn reply(socket: &UnixStream, frame: FrameWriter) -> io::Result<()> {
    let mut socket = socket;
    socket.write_all(&frame.finish()?)
}

impl Host {
    fn handle(
        &mut self,
        socket: &UnixStream,
        payload: &[u8],
        mut descriptors: Vec<OwnedFd>,
    ) -> io::Result<()> {
        let (tag, mut fields) = FrameReader::new(payload)?;
        match tag {
            REQUEST_APPROVE => {
                let envrc = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                fields.finish()?;
                let stdout = one_descriptor(&mut descriptors, "stdout")?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                let status = self.approve(&envrc, stdout, stderr)?;
                reply(socket, FrameWriter::new(REPLY_APPROVED).i32(status))
            }
            REQUEST_ACTIVATE => {
                let directory = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                fields.finish()?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                self.activate(socket, &directory, stderr)
            }
            REQUEST_RUN => {
                let argv = fields.list()?;
                let cwd = PathBuf::from(OsStr::from_bytes(fields.bytes()?));
                let overlay = fields.list()?;
                fields.finish()?;
                let stdin = one_descriptor(&mut descriptors, "stdin")?;
                let stdout = one_descriptor(&mut descriptors, "stdout")?;
                let stderr = one_descriptor(&mut descriptors, "stderr")?;
                self.run(socket, &argv, &cwd, &overlay, [stdin, stdout, stderr])
            }
            REQUEST_SCRIPT => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "script requests need the embedded interpreter, which this host does not carry",
            )),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown control request {other}"),
            )),
        }
    }

    fn tool(&self, directory: &Path) -> Command {
        let mut command = Command::new("direnv");
        command
            .env_clear()
            .envs(&self.environment)
            .env("PWD", directory)
            .current_dir(directory);
        command
    }

    /// Approve the `.envrc` in the workspace's private trust store, unless it already is.
    ///
    /// `direnv allow` rewrites its allow file on every call, and that file is one of the inputs
    /// direnv records: approving an approved file would mark every generation stale.
    fn approve(&self, envrc: &Path, stdout: OwnedFd, stderr: OwnedFd) -> io::Result<RawWaitStatus> {
        let directory = envrc.parent().unwrap_or(Path::new("/"));
        let status = self
            .tool(directory)
            .args(["status", "--json"])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output();
        if let Ok(output) = status
            && output.status.success()
            && approved(&output.stdout, envrc)
        {
            return Ok(0);
        }
        let status = self
            .tool(directory)
            .arg("allow")
            .arg(envrc)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()?;
        Ok(status.into_raw())
    }

    /// Evaluate the `.envrc` once and apply direnv's exported diff to this host's environment.
    fn activate(
        &mut self,
        socket: &UnixStream,
        directory: &Path,
        stderr: OwnedFd,
    ) -> io::Result<()> {
        // An activation can run for minutes (a Nix evaluation, an install). If the supervisor
        // that asked for it goes away — its process exited, or it dropped this host — nothing
        // will ever use the result, so the evaluation must not keep running unobserved.
        let _orphan_guard = OrphanGuard::watch(socket)?;
        let mut child = self
            .tool(directory)
            .args(["export", "json"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .spawn()?;
        let mut exported = Vec::new();
        let read = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("direnv export has no stdout pipe"))?
            .take(u64::try_from(MAX_EXPORT_BYTES + 1).unwrap_or(u64::MAX))
            .read_to_end(&mut exported);
        let status = child.wait()?;
        read?;
        if !status.success() {
            return reply(
                socket,
                FrameWriter::new(REPLY_ACTIVATION_EXITED).i32(status.into_raw()),
            );
        }
        if exported.len() > MAX_EXPORT_BYTES {
            return reply(
                socket,
                FrameWriter::new(REPLY_ACTIVATION_UNUSABLE)
                    .bytes(b"direnv export json output exceeds its bound")?,
            );
        }
        let diff: BTreeMap<String, Option<String>> = match serde_json::from_slice(&exported) {
            Ok(diff) => diff,
            Err(error) => {
                return reply(
                    socket,
                    FrameWriter::new(REPLY_ACTIVATION_UNUSABLE).bytes(
                        format!("direnv export json is not an environment diff: {error}")
                            .as_bytes(),
                    )?,
                );
            }
        };
        for (name, value) in diff {
            match value {
                Some(value) => {
                    self.environment.insert(name.into(), value.into());
                }
                None => {
                    self.environment.remove(OsStr::new(&name));
                }
            }
        }
        let watches = self
            .environment
            .get(OsStr::new("DIRENV_WATCHES"))
            .map(|value| value.as_bytes().to_vec());
        let frame = FrameWriter::new(REPLY_ACTIVATED).u32(u32::from(watches.is_some()));
        let frame = match &watches {
            Some(watches) => frame.bytes(watches)?,
            None => frame,
        };
        reply(socket, frame)
    }

    fn run(
        &self,
        socket: &UnixStream,
        argv: &[&[u8]],
        cwd: &Path,
        overlay: &[&[u8]],
        [stdin, stdout, stderr]: [OwnedFd; 3],
    ) -> io::Result<()> {
        let Some((program, arguments)) = argv.split_first() else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty argv"));
        };
        let (pairs, rest) = overlay.as_chunks::<2>();
        if !rest.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "environment overlay is not name/value pairs",
            ));
        }
        let mut environment = self.environment.clone();
        for [name, value] in pairs {
            environment.insert(
                OsString::from_vec(name.to_vec()),
                OsString::from_vec(value.to_vec()),
            );
        }
        environment.insert("PWD".into(), cwd.as_os_str().to_owned());
        let diagnostics = stderr.try_clone()?;
        let spawned = Command::new(OsStr::from_bytes(program))
            .args(arguments.iter().map(|argument| OsStr::from_bytes(argument)))
            .env_clear()
            .envs(&environment)
            .current_dir(cwd)
            .process_group(0)
            .stdin(Stdio::from(stdin))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn();
        let status = match spawned {
            Ok(mut child) => {
                reply(socket, FrameWriter::new(REPLY_STARTED).u32(child.id()))?;
                child.wait()?.into_raw()
            }
            Err(error) => {
                let mut diagnostics = std::fs::File::from(diagnostics);
                let _ = writeln!(
                    diagnostics,
                    "cowshed: cannot run {}: {error}",
                    String::from_utf8_lossy(program)
                );
                unexecutable_status(&error)
            }
        };
        reply(socket, FrameWriter::new(REPLY_EXITED).i32(status))
    }
}

/// Ends this host's whole process group — the host and everything its activation started —
/// when the control socket reaches end of stream while the guard lives.
///
/// A thread polls the socket and a cancel pipe. The supervisor never writes during an
/// activation, so readable data is not expected; end of stream is recognized by a zero-byte
/// `MSG_PEEK`, which consumes nothing. Dropping the guard cancels the watch.
struct OrphanGuard {
    cancel: Option<OwnedFd>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl OrphanGuard {
    fn watch(socket: &UnixStream) -> io::Result<Self> {
        let mut ends = [0; 2];
        // SAFETY: `ends` has room for the two descriptors pipe(2) writes.
        if unsafe { libc::pipe(ends.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: both descriptors were just created and are owned here alone.
        let (cancelled, cancel) =
            unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
        for end in [&cancelled, &cancel] {
            // SAFETY: plain fcntl on an owned descriptor; activation children must not inherit it.
            unsafe { libc::fcntl(end.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
        }
        let control = socket.as_raw_fd();
        let thread = std::thread::Builder::new()
            .name("cowshed-shell-host-orphan-guard".into())
            .spawn(move || {
                let mut watched = [
                    libc::pollfd {
                        fd: control,
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: cancelled.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                loop {
                    // SAFETY: `watched` holds two initialized pollfd entries.
                    let ready = unsafe { libc::poll(watched.as_mut_ptr(), 2, -1) };
                    if ready < 0 {
                        if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                            continue;
                        }
                        return;
                    }
                    if watched[1].revents != 0 {
                        return;
                    }
                    if watched[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) == 0 {
                        continue;
                    }
                    let mut byte = 0_u8;
                    // SAFETY: one writable byte; MSG_PEEK leaves the stream untouched.
                    let peeked = unsafe {
                        libc::recv(
                            control,
                            (&raw mut byte).cast(),
                            1,
                            libc::MSG_PEEK | libc::MSG_DONTWAIT,
                        )
                    };
                    if peeked == 0 || watched[0].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
                        // SAFETY: signals this host's own process group, which it leads; the
                        // commands it forked lead groups of their own and are untouched.
                        unsafe { libc::kill(0, libc::SIGKILL) };
                    }
                    return;
                }
            })?;
        Ok(Self {
            cancel: Some(cancel),
            thread: Some(thread),
        })
    }
}

impl Drop for OrphanGuard {
    fn drop(&mut self) {
        drop(self.cancel.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Whether `direnv status --json` reports `envrc` itself as the found, allowed rc.
fn approved(status: &[u8], envrc: &Path) -> bool {
    #[derive(serde::Deserialize)]
    struct Status {
        state: State,
    }
    #[derive(serde::Deserialize)]
    struct State {
        #[serde(rename = "foundRC")]
        found_rc: Option<FoundRc>,
    }
    #[derive(serde::Deserialize)]
    struct FoundRc {
        /// direnv's `AllowStatus`: 0 allowed, 1 not allowed, 2 denied.
        allowed: i64,
        path: PathBuf,
    }
    serde_json::from_slice::<Status>(status).is_ok_and(|status| {
        status
            .state
            .found_rc
            .is_some_and(|found| found.allowed == 0 && found.path == envrc)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn only_the_exact_allowed_rc_counts_as_approved() {
        let status = br#"{"config":{},"state":{"foundRC":{"allowed":0,"path":"/w/.envrc"}}}"#;
        assert!(approved(status, Path::new("/w/.envrc")));
        assert!(!approved(status, Path::new("/other/.envrc")));
        let denied = br#"{"state":{"foundRC":{"allowed":1,"path":"/w/.envrc"}}}"#;
        assert!(!approved(denied, Path::new("/w/.envrc")));
        assert!(!approved(br#"{"state":{}}"#, Path::new("/w/.envrc")));
    }
}
