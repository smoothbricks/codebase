use std::path::{Path, PathBuf};

/// A private temporary directory the test owns, removed when the guard drops.
///
/// Removal lives in `Drop` so it runs on every way out of a test: the closing assertion, an early
/// `return`, an `expect` that panics, or a refused `Gateway::start`. A test that only removes its
/// directory as its last statement leaves one behind for every failure, and one per test process
/// adds up to tens of thousands of entries in the shared temp directory.
///
/// Bind the guard before anything that writes into the directory — a `Gateway` built from a
/// config naming it, a spawned fixture — so locals drop in reverse order and the writer is gone
/// before the directory is removed.
pub struct FixtureDir(PathBuf);

impl FixtureDir {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for FixtureDir {
    fn drop(&mut self) {
        // Drop cannot return an error, and a removal failure must not turn a passing test into a
        // panic or a failing one into an abort; name the leaked path instead of hiding it.
        if let Err(error) = std::fs::remove_dir_all(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("remove fixture directory {}: {error}", self.0.display());
        }
    }
}

/// Where every gateway test fixture lives: canonical `/tmp` (`/private/tmp` on macOS), never
/// TMPDIR. A cowshed workspace exports a TMPDIR deep inside its store, which puts a control
/// socket under it past `sockaddr_un.sun_path`'s 104 bytes, so `Gateway::start` refused it
/// ("path must be shorter than SUN_LEN") whenever the tests ran in a workspace.
pub fn scratch_parent() -> PathBuf {
    std::fs::canonicalize("/tmp").unwrap_or_else(|error| panic!("canonical /tmp: {error}"))
}

/// A temporary directory every cowshed private-root check accepts.
///
/// `create_dir` masks 0o777 with the process umask, so a permissive umask yields a group- and
/// world-writable directory. Every gateway root check refuses one — correctly, because a directory
/// a stranger can write is a directory a stranger can swap a socket or a cache entry into — so the
/// mode is set explicitly here rather than inherited from the environment.
///
/// The mode is then read back and asserted. That is what keeps a permissive umask, or a temp
/// strategy that hands back a directory this fixture did not create, from resurfacing as a bare
/// `InvalidInput` raised deep inside `Gateway::start`, which names neither the path nor the mode.
pub fn secure_fixture_dir(name: &str) -> FixtureDir {
    let path = scratch_parent().join(name);
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir(&path)
        .unwrap_or_else(|error| panic!("create fixture directory {}: {error}", path.display()));
    // Owned from here: a failed check below removes the directory as the panic unwinds.
    let dir = FixtureDir(path);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let path = dir.path();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap_or_else(
            |error| panic!("restrict fixture directory {}: {error}", path.display()),
        );
        let metadata = std::fs::symlink_metadata(path)
            .unwrap_or_else(|error| panic!("lstat fixture directory {}: {error}", path.display()));
        assert!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "fixture directory {} must be a real directory, got {:?}",
            path.display(),
            metadata.file_type()
        );
        assert_eq!(
            metadata.uid(),
            unsafe { libc::geteuid() },
            "fixture directory {} must be owned by the test process",
            path.display()
        );
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(
            mode,
            0o700,
            "fixture directory {} is mode {mode:04o}, not 0700: it is group- or other-writable and \
             every gateway private-root check will refuse it",
            path.display()
        );
    }
    dir
}
