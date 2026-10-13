//! The project controller's storage capabilities over a directory store.
//!
//! Build volumes are real payload directories (`build_volumes.rs`). Volume labels, capacities
//! and re-adoption are physical-image operations: re-adoption moves a replaced main's image and
//! its build image aside, attaches them read-only and proves the replacement sparse, and none of
//! that has a directory counterpart, so each is refused with `ApfsStorageError::Unsupported`.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{DirectoryExecutionHost, unsupported};
use crate::build_volume::{BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, ReleaseClaim};
use crate::runtime::build_volumes::Quiet;
use crate::runtime::storage_host::RuntimeStorageHost;
use crate::storage::apfs::native::{BuildVolumeClaim, BuildVolumeRefusal, BuildVolumeRelease};
use crate::storage::apfs::{ApfsStorageError, Labelled};
use crate::storage::lifecycle::ResizeOutcome;
use crate::storage::recovery::ReAdoptSource;
use cowshed_apfs::image::ImageCapacity;

impl RuntimeStorageHost for DirectoryExecutionHost {
    fn label_image_volume(
        &self,
        _image: &Path,
        _label: &str,
    ) -> Result<Labelled, ApfsStorageError> {
        Err(unsupported("volume labels"))
    }

    fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<PathBuf, ApfsStorageError> {
        DirectoryExecutionHost::create_build_volume(self, layout, id, capacity)
    }

    fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        DirectoryExecutionHost::clone_build_volume(self, layout, source, destination, record)
    }

    fn clone_quiet_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        quiet: &Quiet,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        self.clone_held_build_volume(layout, quiet.id(), destination, record)
    }

    fn clone_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        self.clone_held_build_volume(layout, claim.id(), destination, record)
    }

    fn build_volume_written(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<SystemTime, ApfsStorageError> {
        DirectoryExecutionHost::build_volume_written(self, layout, id)
    }

    fn mark_seed_written(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        written: SystemTime,
    ) -> Result<(), ApfsStorageError> {
        DirectoryExecutionHost::mark_seed_written(self, layout, seed, written)
    }

    fn fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(BuildVolumeId, PathBuf), ApfsStorageError> {
        DirectoryExecutionHost::fork_build_volume(self, layout, seed, record)
    }

    fn mount_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<PathBuf, ApfsStorageError> {
        DirectoryExecutionHost::mount_build_volume(self, layout, id)
    }

    fn release_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeRelease, ApfsStorageError> {
        DirectoryExecutionHost::release_build_volume(self, layout, id)
    }

    fn claim_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeClaim, ApfsStorageError> {
        DirectoryExecutionHost::claim_build_volume(self, layout, id)
    }

    fn release_claimed(
        &self,
        layout: &BuildVolumeLayout,
        claim: ReleaseClaim,
    ) -> Result<Option<BuildVolumeRefusal>, ApfsStorageError> {
        DirectoryExecutionHost::release_claimed(self, layout, claim)
    }

    fn build_volume_capacity(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<ImageCapacity, ApfsStorageError> {
        DirectoryExecutionHost::build_volume_capacity(self, layout, id)
    }

    fn resize_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError> {
        DirectoryExecutionHost::resize_build_volume(self, layout, id, capacity)
    }

    fn re_adopt_volume_holders(
        &self,
        _image: &Path,
        _mount: &Path,
    ) -> Result<Vec<cowshed_os::process::Holder>, ApfsStorageError> {
        Err(unsupported("re-adoption volume holders"))
    }

    fn retain_re_adopt_source(&self, _source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        Err(unsupported("re-adoption"))
    }

    fn attached_capacity_bytes(&self, _image: &Path) -> Result<u64, ApfsStorageError> {
        Err(unsupported("attached image capacities"))
    }

    fn retained_re_adopt_main_is_mounted(
        &self,
        _source: &ReAdoptSource,
    ) -> Result<bool, ApfsStorageError> {
        Err(unsupported("re-adoption"))
    }

    fn mount_re_adopt_sources(&self, _source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        Err(unsupported("re-adoption"))
    }

    fn release_re_adopt_sources(&self, _source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        Err(unsupported("re-adoption"))
    }

    fn copy_re_adopt_build(&self, _source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        Err(unsupported("re-adoption"))
    }

    fn require_sparse_replacement(
        &self,
        _replacement: &Path,
        _retained: &Path,
        _capacity_bytes: u64,
    ) -> Result<(u64, u64), ApfsStorageError> {
        Err(unsupported("sparse image replacement"))
    }

    fn publish_re_adopt_build(
        &self,
        _layout: &BuildVolumeLayout,
        _source: &ReAdoptSource,
        _build_claim: Option<&ReleaseClaim>,
    ) -> Result<(), ApfsStorageError> {
        Err(unsupported("re-adoption"))
    }
}
