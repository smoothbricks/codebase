//! A fixture's test namespace and its root (specs/cowshed/01_storage.md, "Host namespace").

use std::path::Path;

use cowshed_os::namespace::TestNamespace;

use cowshed_apfs::{
    CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend, SystemCommandRunner,
    store_lock::verify_no_symlinks,
};

#[path = "../../../cowshed-apfs/tests/support/scratch_apfs.rs"]
pub(crate) mod scratch_apfs;

/// A fixture namespace owned by the shared scratch-root protocol: owner birth identity,
/// dead-run reclamation and exact attachment cleanup all precede removal of its root.
pub struct NamespaceRoot(scratch_apfs::ScratchRoot);

impl NamespaceRoot {
    /// Mint a namespace and make its root.
    pub fn mint() -> Self {
        Self(scratch_apfs::ScratchRoot::new("gateway namespace").expect("a swept namespace root"))
    }

    pub fn namespace(&self) -> TestNamespace {
        self.0.namespace()
    }

    pub fn path(&self) -> &Path {
        self.0.path()
    }
}
