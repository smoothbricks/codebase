//! A test's private directory under `/tmp`, removed when its owner drops: on return, on an
//! assertion failure, and on any other panic. Every leaked fixture directory is one more entry
//! each process started under `/tmp` has to read, so no exit path may keep one.
//!
//! The parent is fixed here rather than read from TMPDIR: a cowshed workspace exports a TMPDIR
//! inside the protected store (`/private/cowshed`), and the sandbox refuses to grant any path
//! there, so a fixture rooted in that TMPDIR fails every test that sandboxes it. `/tmp` is the
//! same directory on macOS (`/private/tmp`) and exists on every Linux host.

use std::path::{Path, PathBuf};

const SCRATCH_PARENT: &str = "/tmp";

pub(crate) struct TempRoot(PathBuf);

impl TempRoot {
    /// Creates `/tmp/<prefix>-<uuid>`, canonical: macOS's `/tmp` resolves into `/private/tmp`,
    /// and capability detection refuses a path that resolves elsewhere.
    pub(crate) fn new(prefix: &str) -> Self {
        let parent = std::fs::canonicalize(SCRATCH_PARENT)
            .unwrap_or_else(|error| panic!("canonical {SCRATCH_PARENT}: {error}"));
        let path = parent.join(format!("{prefix}-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("create test root {}: {error}", path.display()));
        Self(path)
    }
}

impl std::ops::Deref for TempRoot {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        match std::fs::remove_dir_all(&self.0) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            // The unwinding test already reports its own failure; a second panic would abort it.
            Err(error) if std::thread::panicking() => {
                eprintln!("remove test root {}: {error}", self.0.display());
            }
            Err(error) => panic!("remove test root {}: {error}", self.0.display()),
        }
    }
}
