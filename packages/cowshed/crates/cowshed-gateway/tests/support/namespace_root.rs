//! A fixture's test namespace and its root (specs/cowshed/01_storage.md, "Host namespace").

use std::path::Path;

use cowshed_os::namespace::{HostNamespace, TestNamespace};

use crate::fixture_dir::{FixtureDir, secure_fixture_dir_in};

/// A test namespace of a fixture's own and its root: the namespace's directory below the per-user
/// root `cowshed setup` makes, where the fixture's gateway binds `gateway.sock` and from which every
/// other name the fixture mints derives. The root is made 0700, refused when the per-user root is
/// missing, and removed when the guard drops, on every way out of the test.
pub struct NamespaceRoot {
    namespace: TestNamespace,
    dir: FixtureDir,
}

impl NamespaceRoot {
    /// Mint a namespace and make its root.
    pub fn mint() -> Self {
        let namespace = TestNamespace::mint().expect("a test namespace");
        // SAFETY: geteuid has no preconditions.
        let euid = unsafe { libc::geteuid() };
        let root = HostNamespace::Test(namespace).macos_user_root(euid);
        let user_root = root
            .parent()
            .expect("a test namespace's root lies below the per-user root");
        let dir = secure_fixture_dir_in(user_root, namespace.as_str());
        Self { namespace, dir }
    }

    pub fn namespace(&self) -> TestNamespace {
        self.namespace
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }
}
