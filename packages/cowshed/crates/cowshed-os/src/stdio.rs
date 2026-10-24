//! Writing an inherited stdout or stderr as a blocking descriptor, whatever its flags say.
//!
//! `O_NONBLOCK` belongs to an open file description, not to a descriptor, so every process that
//! shares an inherited stdout or stderr shares the flag. A parent that drives the same pipe from an
//! event loop, such as Node, may set it while cowshed runs, and a write to a full pipe then fails
//! with `EAGAIN` (`WouldBlock`) where a blocking write would have waited for the reader. Observed
//! on a release gate: a foreground `cowshed exec` relaying a 22-minute gate exited 5 with
//! `could not write child output: Resource temporarily unavailable (os error 35)` while its
//! consumer was still reading (06_cli.md, "Every `cowshed exec` submission"). Cowshed's own
//! `cowshed:` lines go to that same stderr ([`crate::stderr`]), so they and the CLI's output, the
//! relayed child bytes included, all write through [`Blocking`].
//!
//! [`Blocking`] keeps a blocking write's semantics: a write the kernel refuses as `WouldBlock`
//! waits in `poll(2)` for `POLLOUT` on that same descriptor, with no deadline, as the blocking
//! write would have, and is then made again. The flag is never cleared, because the description is
//! the parent's too. A reader that goes away wakes the wait (`POLLHUP`, `POLLERR`) and the write
//! made again reports the real error (`EPIPE`); a descriptor that is not open is `EBADF`.

use std::io::{self, Write};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};

/// A writer whose `write` and `flush` wait out `WouldBlock` as a blocking descriptor would (see
/// the module).
pub struct Blocking<W> {
    inner: W,
}

impl<W: Write + AsFd> Blocking<W> {
    pub const fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W: Write + AsFd> Write for Blocking<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            match self.inner.write(bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    writable(self.inner.as_fd())?;
                }
                written => return written,
            }
        }
    }

    /// A buffered writer keeps what the kernel did not take, so the flush made again after the
    /// wait writes only the rest.
    fn flush(&mut self) -> io::Result<()> {
        loop {
            match self.inner.flush() {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    writable(self.inner.as_fd())?;
                }
                flushed => return flushed,
            }
        }
    }
}

/// Wait, with no deadline, until `descriptor` takes a write or can say why it never will.
fn writable(descriptor: BorrowedFd<'_>) -> io::Result<()> {
    let mut entry = libc::pollfd {
        fd: descriptor.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    loop {
        // SAFETY: `entry` is one initialized `pollfd` that outlives the call, and the count is one.
        if unsafe { libc::poll(&mut entry, 1, -1) } >= 0 {
            break;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
    if entry.revents & libc::POLLNVAL != 0 {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Blocking;
    use std::io::{self, PipeReader, PipeWriter, Read, Write};
    use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::thread::{self, JoinHandle};

    /// More than any pipe holds: macOS grows a pipe to 64 KiB, and Linux's default is 64 KiB.
    const PAYLOAD: usize = 1 << 20;

    /// `PAYLOAD` bytes cycling through every value from 0 to 250, NUL included.
    fn payload() -> Vec<u8> {
        (0..=250).cycle().take(PAYLOAD).collect()
    }

    /// A pipe whose write end is nonblocking, as a parent sharing its description can leave it.
    fn nonblocking_pipe() -> (PipeReader, PipeWriter) {
        let (reader, writer) = io::pipe().expect("a pipe");
        let descriptor = writer.as_raw_fd();
        // SAFETY: F_GETFL reads the open description's flags and touches no memory of ours.
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        assert!(flags >= 0, "{}", io::Error::last_os_error());
        // SAFETY: F_SETFL sets the flags of a description only this test holds.
        let set = unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        assert_eq!(set, 0, "{}", io::Error::last_os_error());
        (reader, writer)
    }

    /// The pipe's write end, saying once that the kernel refused a write as `WouldBlock`. Only a
    /// full pipe sends it, so a reader that waits for it starts after the writer met one.
    struct Watched {
        pipe: PipeWriter,
        blocked: Option<Sender<()>>,
    }

    impl Write for Watched {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let written = self.pipe.write(bytes);
            if let Err(error) = &written
                && error.kind() == io::ErrorKind::WouldBlock
                && let Some(blocked) = self.blocked.take()
            {
                blocked.send(()).expect("the test waits for a full pipe");
            }
            written
        }

        fn flush(&mut self) -> io::Result<()> {
            self.pipe.flush()
        }
    }

    impl AsFd for Watched {
        fn as_fd(&self) -> BorrowedFd<'_> {
            self.pipe.as_fd()
        }
    }

    /// Write the payload to `pipe` through [`Blocking`] on a thread of its own, with the channel
    /// that hears when it first met a full pipe.
    fn write_payload(pipe: PipeWriter) -> (JoinHandle<io::Result<()>>, Receiver<()>) {
        let (blocked, full) = mpsc::channel();
        let writer = thread::spawn(move || {
            let mut out = Blocking::new(Watched {
                pipe,
                blocked: Some(blocked),
            });
            out.write_all(&payload())?;
            out.flush()
        });
        (writer, full)
    }

    #[test]
    fn a_full_nonblocking_pipe_takes_every_byte_once_its_reader_drains_it() {
        let (mut reader, pipe) = nonblocking_pipe();
        let (writer, full) = write_payload(pipe);
        full.recv()
            .expect("the writer met a full pipe before anything read it");
        let mut read = Vec::new();
        reader
            .read_to_end(&mut read)
            .expect("the pipe reads to its end");
        writer
            .join()
            .expect("the writer thread")
            .expect("every byte is written");
        let expected = payload();
        assert_eq!(read.len(), expected.len());
        assert!(
            read == expected,
            "first differing byte: {:?}",
            read.iter()
                .zip(&expected)
                .position(|(got, want)| got != want)
        );
    }

    #[test]
    fn a_reader_that_leaves_a_full_pipe_ends_the_wait_as_a_broken_pipe() {
        let (reader, pipe) = nonblocking_pipe();
        let (writer, full) = write_payload(pipe);
        full.recv()
            .expect("the writer met a full pipe before its reader left");
        drop(reader);
        let error = writer
            .join()
            .expect("the writer thread")
            .expect_err("a pipe nobody reads takes no more bytes");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe, "{error}");
    }
}
