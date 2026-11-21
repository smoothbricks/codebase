//! Linux's native project storage: the directory host, which keeps each payload as a ZFS dataset
//! when the store root is a cowshed ZFS store dataset and as a real directory otherwise
//! (`DirectoryExecutionHost::new` decides which from the store root itself). Its
//! [`super::RuntimeStorageHost`] is the directory store's own (`storage/directory/runtime_host.rs`).

use crate::error::{CowshedError, Result};
use crate::storage::apfs::{ApfsStorageError, ApfsSubstrateConfig};
use crate::storage::bootstrap::SelectedSubstrate;
use crate::storage::directory::DirectoryExecutionHost;

/// The execution host a native opening runs its lifecycle on.
pub type NativeHost = DirectoryExecutionHost;

/// The directory host over the store `config` names.
pub fn native_host(
    config: ApfsSubstrateConfig,
) -> std::result::Result<NativeHost, ApfsStorageError> {
    DirectoryExecutionHost::new(config)
}

/// The native runtime serves the ZFS and directory substrates. Linux attaches no APFS image, so a
/// store selected onto that substrate is refused before a host is built over it.
pub(crate) fn require_native_substrate(substrate: &SelectedSubstrate) -> Result<()> {
    match substrate {
        SelectedSubstrate::Zfs { .. } | SelectedSubstrate::Directory { .. } => Ok(()),
        SelectedSubstrate::Apfs { .. } => Err(CowshedError::environment_missing(
            "the Linux runtime serves a ZFS or directory store, not the APFS image substrate",
            "remove the unsupported substrate override and retry",
        )),
    }
}
