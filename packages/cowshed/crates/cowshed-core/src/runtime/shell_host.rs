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

use std::ffi::OsString;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

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
/// Fork an argv into a new process group: `[argv, cwd, overlay]`, descriptors
/// `[stdin, stdout, stderr]`.
pub const REQUEST_RUN: u8 = 3;
/// Run a rendered script: `[text, bindings, cwd, overlay]`, descriptors `[stdin, stdout,
/// stderr]`. The host parses the text before anything runs, then forks: the child leads a new
/// process group, builds a brush shell from the already-activated environment and runs the
/// program there, so every process the script starts is in the job's group and the child's own
/// wait status is the job's.
pub const REQUEST_SCRIPT: u8 = 4;

pub const REPLY_APPROVED: u8 = 1;
pub const REPLY_ACTIVATION_EXITED: u8 = 2;
pub const REPLY_ACTIVATED: u8 = 3;
pub const REPLY_ACTIVATION_UNUSABLE: u8 = 4;
pub const REPLY_STARTED: u8 = 5;
pub const REPLY_EXITED: u8 = 6;
/// A script request's text did not parse; nothing ran. `[diagnostic]`, also written to the
/// job's stderr.
pub const REPLY_SCRIPT_SYNTAX: u8 = 7;
/// The host could not serve the request it was handling: `[reason]`. The host exits after
/// sending it, so the supervisor learns why instead of seeing only a closed socket.
pub const REPLY_HOST_FAILED: u8 = 8;

/// A script binding's value kind on the wire.
pub const BINDING_SCALAR: u8 = 0;
pub const BINDING_ARRAY: u8 = 1;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};

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
}
