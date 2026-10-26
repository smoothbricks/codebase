//! A test's private directory under its selected granted parent, removed when its owner drops:
//! on return, on an assertion failure, and on any other panic. Every leaked fixture directory is
//! one more entry each process started under that parent has to read, so no exit path may keep one.
//!
//! `new_in` creates a private child without changing the selected parent's runtime alias.
//! Independent sandbox fixtures explicitly select the canonical `/tmp` parent because their
//! profiles cannot grant protected store paths inherited through TMPDIR. This guard adds no grant.
//!
//! Drop handles every return and panic, but not a killed process: that run's root is reclaimed by
//! the shared-temp sweep, which claims a directory only when its pid-bearing name opens with
//! `cowshed-` or `cs-` (cowshed-os `scratch::named_owner`). The constructor therefore names every
//! root claimably itself, whatever prefix the caller spelled, rather than trusting each caller.

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
        let path =
            cowshed_os::scratch::create_named(parent, &claimable(prefix)).unwrap_or_else(|error| {
                panic!("create owned test root under {}: {error}", parent.display())
            });
        Self(path)
    }
}

impl std::ops::Deref for TempRoot {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

/// The claimable spelling of a caller's prefix: the shared-temp sweep recovers a
/// killed run's root from its pid-bearing name only when that name opens with
/// `cowshed-` or `cs-`, so a prefix opening with neither gains `cs-`.
fn claimable(prefix: &str) -> std::borrow::Cow<'_, str> {
    if prefix.starts_with("cowshed-") || prefix.starts_with("cs-") {
        std::borrow::Cow::Borrowed(prefix)
    } else {
        std::borrow::Cow::Owned(format!("cs-{prefix}"))
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

#[cfg(test)]
mod tests {
    use super::{TempRoot, claimable};

    #[test]
    fn claimable_normalizes_a_prefix() {
        assert_eq!(claimable("cowshed-store"), "cowshed-store");
        assert_eq!(claimable("cs-seal"), "cs-seal");
        assert_eq!(claimable("plain-name"), "cs-plain-name");
        assert_eq!(claimable("pr"), "cs-pr");
    }

    #[test]
    fn every_name_it_creates_is_claimable() {
        // The parent fixtures are granted (temp_parent::scratch_parent's rule, spelled
        // here because this file also stands alone in test binaries without it).
        let granted = std::env::var_os("COWSHED_RUNTIME_LINK")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
        let parent = TempRoot::new_in(&granted, "cs-claim-parent");
        for prefix in ["plain-name", "cs-seal", "cowshed-store"] {
            let root = TempRoot::new_in(&parent, prefix);
            let name = root
                .file_name()
                .expect("a created root has a name")
                .to_string_lossy();
            assert!(
                name.starts_with("cowshed-") || name.starts_with("cs-"),
                "prefix {prefix:?} created the unclaimable name {name:?}"
            );
            assert!(
                name.starts_with(&*claimable(prefix)),
                "prefix {prefix:?} created {name:?}, not its claimable spelling"
            );
        }
    }
}
