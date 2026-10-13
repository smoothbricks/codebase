//! Storage capabilities used by the shared project controller beyond lifecycle execution.
//!
//! Handles and locks remain the execution host's associated types. A directory store does not
//! own a kernel volume: its controller never configures physical volume labels or counters.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::build_volume::{BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, ReleaseClaim};
use crate::storage::apfs::native::{BuildVolumeClaim, BuildVolumeRefusal, BuildVolumeRelease};
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError, Labelled};
use crate::storage::lifecycle::ResizeOutcome;
use crate::storage::recovery::ReAdoptSource;
use cowshed_apfs::image::ImageCapacity;

mod native;

/// The controller's storage boundary, shared by native images and real directory stores.
///
/// These operations preserve the lifecycle host's lock and publication authority. Physical
/// operations on a backend without kernel volumes must return an explicit unsupported error,
/// never a successful disk result. The capability flag keeps optional cosmetic labels and job
/// volume sampling out of those backends; their resource samples report unconfigured volumes.
pub(crate) trait RuntimeStorageHost: ApfsExecutionHost<LockGuard: Sync> {
    fn label_image_volume(&self, image: &Path, label: &str) -> Result<Labelled, ApfsStorageError>;

    fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<PathBuf, ApfsStorageError>;
    fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError>;
    fn clone_quiet_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        quiet: &super::build_volumes::Quiet,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError>;
    /// Capture the source `claim` claimed into seed `destination`, under that claim: a reseed
    /// takes it before it stops the source's Nx daemon, so no job (the daemon keeper's start job
    /// included) is admitted on the source until the capture is done (16_build_volumes.md,
    /// "Targets and seeds").
    fn clone_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError>;
    fn build_volume_written(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<SystemTime, ApfsStorageError>;
    fn mark_seed_written(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        written: SystemTime,
    ) -> Result<(), ApfsStorageError>;
    fn fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(BuildVolumeId, PathBuf), ApfsStorageError>;
    fn mount_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<PathBuf, ApfsStorageError>;
    fn release_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeRelease, ApfsStorageError>;
    fn claim_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeClaim, ApfsStorageError>;
    fn release_claimed(
        &self,
        layout: &BuildVolumeLayout,
        claim: ReleaseClaim,
    ) -> Result<Option<BuildVolumeRefusal>, ApfsStorageError>;
    fn build_volume_capacity(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<ImageCapacity, ApfsStorageError>;
    fn resize_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError>;

    fn retain_re_adopt_source(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError>;
    fn attached_capacity_bytes(&self, image: &Path) -> Result<u64, ApfsStorageError>;
    fn retained_re_adopt_main_is_mounted(
        &self,
        source: &ReAdoptSource,
    ) -> Result<bool, ApfsStorageError>;
    fn mount_re_adopt_sources(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError>;
    fn release_re_adopt_sources(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError>;
    fn copy_re_adopt_build(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError>;
    fn require_sparse_replacement(
        &self,
        replacement: &Path,
        retained: &Path,
        capacity_bytes: u64,
    ) -> Result<(u64, u64), ApfsStorageError>;
    fn publish_re_adopt_build(
        &self,
        layout: &BuildVolumeLayout,
        source: &ReAdoptSource,
        build_claim: Option<&ReleaseClaim>,
    ) -> Result<(), ApfsStorageError>;
}
