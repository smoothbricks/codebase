//! A test's private directory under TMPDIR, removed when its owner drops: on return, on an
//! assertion failure, and on any other panic. Every leaked fixture directory is one more entry
//! each process started under TMPDIR has to read, so no exit path may keep one.

use std::path::{Path, PathBuf};

pub(crate) struct TempRoot(PathBuf);

impl TempRoot {
    /// Creates `$TMPDIR/<prefix>-<uuid>`, canonical: `/var/folders` resolves into
    /// `/private/var`, and capability detection refuses a path that resolves elsewhere.
    pub(crate) fn new(prefix: &str) -> Self {
        let temp = std::fs::canonicalize(std::env::temp_dir()).expect("canonical TMPDIR");
        let path = temp.join(format!("{prefix}-{}", uuid::Uuid::new_v4().simple()));
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
