//! The gateway's jobserver fifos (05_gateway.md, "Host CPU budget": one fifo per
//! client process).
//!
//! On a grant the gateway creates one fifo here, keeps its own `O_RDWR` descriptor
//! until the drain so the token bytes survive while no client has it open, and
//! writes `g − 1` token bytes into it: the client's implicit make token is the
//! first. When the client's connection closes the gateway drains the bytes left,
//! unlinks the fifo and the caller returns all `g` tokens to the ledger, so
//! tokens a killed process read and never wrote back are credited too. A restart
//! forgets every grant, so startup removes every fifo left here.

use std::fs::{File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::FileTypeExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The byte a GNU make jobserver hands out as one token.
const TOKEN_BYTE: u8 = b'+';

/// The fifos of one gateway, in its private runtime directory.
#[derive(Clone, Debug)]
pub(crate) struct JobserverFifos {
    directory: std::sync::Arc<PathBuf>,
    counter: std::sync::Arc<AtomicU64>,
}

impl JobserverFifos {
    /// Name the fifo directory, sweeping a previous gateway's leftover fifos.
    pub(crate) fn start(directory: PathBuf) -> Self {
        // Sweep the fifos a previous gateway left in the directory when it
        // exists: a restart forgot those grants, and their bytes belong to
        // nobody. The directory itself is created at the first grant, so
        // gateway start neither creates the control socket's parent (masking
        // its absence) nor fails on it.
        if let Ok(entries) = std::fs::read_dir(&directory) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_fifo()) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        Self {
            directory: std::sync::Arc::new(directory),
            counter: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create one fifo holding `tokens - 1` token bytes, kept open by the
    /// gateway's own descriptor until [`JobserverFifo::drain_and_unlink`].
    pub(crate) fn create(&self, tokens: usize) -> io::Result<JobserverFifo> {
        std::fs::create_dir_all(self.directory.as_ref() as &Path)?;
        let sequence = self.counter.fetch_add(1, Ordering::Relaxed);
        let path = self
            .directory
            .join(format!("jobserver-{}-{sequence}.fifo", std::process::id()));
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "jobserver fifo path has a NUL")
        })?;
        // SAFETY: c_path is a valid NUL-terminated path for the duration of the call.
        if unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let opened = OpenOptions::new().read(true).write(true).open(&path);
        let mut file = match opened {
            Ok(file) => file,
            Err(error) => {
                let _ = std::fs::remove_file(&path);
                return Err(error);
            }
        };
        if tokens > 1
            && let Err(error) = file.write_all(&vec![TOKEN_BYTE; tokens - 1])
        {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        Ok(JobserverFifo { path, file })
    }
}

/// One granted client's fifo, held open by the gateway until the drain.
#[derive(Debug)]
pub(crate) struct JobserverFifo {
    path: PathBuf,
    file: File,
}

impl JobserverFifo {
    /// The path the granted answer names for `CARGO_MAKEFLAGS`.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Drain the token bytes left in the fifo and unlink it. A cargo that
    /// outlives its launcher keeps its own descriptor and runs over budget
    /// until it finishes; what it still holds is not the gateway's to reclaim.
    pub(crate) fn drain_and_unlink(mut self) {
        // SAFETY: the descriptor is this fifo's own, opened above.
        let flags = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_GETFL) };
        if flags >= 0 {
            // SAFETY: same descriptor; read what is already there, never wait.
            unsafe {
                libc::fcntl(
                    self.file.as_raw_fd(),
                    libc::F_SETFL,
                    flags | libc::O_NONBLOCK,
                );
            }
            let mut discard = [0_u8; 4096];
            while self.file.read(&mut discard).is_ok_and(|read| read > 0) {}
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

use std::os::unix::io::AsRawFd as _;

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;
    fn directory() -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("cowshed-jobserver-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let directory = root.join("jobserver");
        (root, directory)
    }

    #[test]
    fn a_grant_fifo_holds_one_fewer_token_than_the_grant_and_drains_clean() {
        let (_root, directory) = directory();
        let fifos = JobserverFifos::start(directory.clone());
        let fifo = fifos.create(4).unwrap();
        let path = fifo.path().to_path_buf();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // A client takes two of the three token bytes, then dies without
        // writing them back: the drain removes what is left and the path.
        let mut client = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut taken = [0_u8; 2];
        client.read_exact(&mut taken).unwrap();
        assert_eq!(taken, [TOKEN_BYTE; 2]);
        drop(client);
        fifo.drain_and_unlink();
        assert!(!path.exists());
    }

    #[test]
    fn startup_removes_leftover_fifos_and_nothing_else() {
        let (_root, directory) = directory();
        std::fs::create_dir_all(&directory).unwrap();
        let leftover = directory.join("jobserver-1-0.fifo");
        let c_path = std::ffi::CString::new(leftover.as_os_str().as_bytes()).unwrap();
        // SAFETY: c_path is a valid NUL-terminated path for the call.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let other = directory.join("control.sock-note");
        std::fs::write(&other, b"keep").unwrap();
        let _fifos = JobserverFifos::start(directory);
        assert!(!leftover.exists());
        assert!(other.exists());
    }
}
