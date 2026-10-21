//! A test's private directory under its selected granted parent, removed when its owner drops:
//! on return, on an assertion failure, and on any other panic. Every leaked fixture directory is
//! one more entry each process started under that parent has to read, so no exit path may keep one.
//!
//! `new_in` creates a private child without changing the selected parent's runtime alias.
//! Independent sandbox fixtures explicitly select the canonical `/tmp` parent because their
//! profiles cannot grant protected store paths inherited through TMPDIR. This guard adds no grant.

use std::path::{Path, PathBuf};

pub(crate) struct TempRoot(PathBuf);

impl TempRoot {
    /// Creates one private child under the canonical `/tmp` parent.
    #[allow(dead_code)]
    pub(crate) fn new(prefix: &str) -> Self {
        let parent =
            std::fs::canonicalize("/tmp").unwrap_or_else(|error| panic!("canonical /tmp: {error}"));
        Self::new_in(&parent, prefix)
    }

    /// Creates one private child within the caller's granted parent, preserving any runtime alias.
    pub(crate) fn new_in(parent: &Path, prefix: &str) -> Self {
        assert!(
            parent.is_absolute(),
            "test parent {} must be absolute",
            parent.display()
        );
        assert!(
            parent.is_dir(),
            "test parent {} must be a directory",
            parent.display()
        );
        // 16 hex digits: unique enough for one parent, and short enough that a fixture under the
        // runtime link keeps its sockets inside `sun_path`.
        let unique = uuid::Uuid::new_v4().simple().to_string();
        let path = parent.join(format!("{prefix}-{}", &unique[..16]));
        cowshed_os::scratch::create(&path)
            .unwrap_or_else(|error| panic!("create owned test root {}: {error}", path.display()));
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
                cowshed_os::eprintln!("remove test root {}: {error}", self.0.display());
            }
            Err(error) => panic!("remove test root {}: {error}", self.0.display()),
        }
    }
}

/// A test's own cowshed store. Controller Git a fixture runs writes its sandbox profiles into this
/// store's `run/profiles`, never into the host's (04_sandbox.md, "Exec pipeline" step 4;
/// 08_testing.md, "Fixtures"). It lives under the canonical `/tmp` parent and goes with its owner.
#[allow(dead_code)]
pub(crate) struct FixtureStore {
    _root: TempRoot,
    shared: std::sync::Arc<Path>,
}

#[allow(dead_code)]
impl FixtureStore {
    pub(crate) fn new() -> Self {
        let root = TempRoot::new("cowshed-fixture-store");
        let shared = std::sync::Arc::from(&*root);
        Self {
            _root: root,
            shared,
        }
    }

    /// The store root, shared as controller Git takes it.
    pub(crate) fn shared(&self) -> &std::sync::Arc<Path> {
        &self.shared
    }
}
