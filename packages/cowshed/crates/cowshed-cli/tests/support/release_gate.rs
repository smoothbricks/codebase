//! A release gate for a command a test holds: a named pipe the test keeps open read-write from its
//! creation. The command's `read x < gate` never waits for a writer to appear, and a release
//! written before the command opens the gate waits in the pipe for it, so no open is retried and
//! none blocks the test (08_testing.md, "Fixtures"). A command that never reads is the runner's
//! no-progress bound to name.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

pub struct Gate {
    path: PathBuf,
    held: File,
}

impl Gate {
    /// Make the gate at `path`, which must not exist, and hold it open for the gate's life.
    pub fn new(path: PathBuf) -> Self {
        let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
            .expect("a gate path has no NUL");
        // SAFETY: mkfifo(3) reads the NUL-terminated path, which outlives the call.
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(
            rc,
            0,
            "mkfifo {}: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
        let held = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap_or_else(|error| panic!("hold the gate {} open: {error}", path.display()));
        Self { path, held }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Lets one command reading the gate run on: its `read` takes this line.
    pub fn release(&self) {
        (&self.held)
            .write_all(b"\n")
            .expect("write the release line");
    }
}
