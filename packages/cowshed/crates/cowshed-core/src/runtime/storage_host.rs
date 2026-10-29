//! Storage capabilities used by the shared project controller beyond lifecycle execution.
//!
//! Handles and locks remain the execution host's associated types. A directory store does not
//! own a kernel volume: its controller never configures physical volume labels or counters.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::build_volume::{
    BuildVolumeHold, BuildVolumeId, BuildVolumeLayout, CapturedBuildVolume, Registration,
    ReleaseClaim,
};
use crate::metadata::{DetachedWorkspaceMetadata, WorkspaceName};
use crate::repository::RepoId;
use crate::storage::apfs::native::{
    BuildVolumeClaim, BuildVolumeRefusal, BuildVolumeRelease, CompactedSeed, CompactionSource,
    SeedCatchUp, SeedCompaction,
};
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError, Labelled};
use crate::storage::lifecycle::DerivedWorkspace;
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
    fn canonical_metadata(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<DetachedWorkspaceMetadata>, ApfsStorageError>;
    fn workspace_snapshot(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata)>, ApfsStorageError>;

    fn label_image_volume(&self, image: &Path, label: &str) -> Result<Labelled, ApfsStorageError>;

    /// Every operation here that writes a build image registers `registration`'s owner record
    /// before the image's first byte exists (16_build_volumes.md, "Garbage collection"). A
    /// created volume stays registered, held by the answered hold, until the caller publishes
    /// it; a clone is published before it answers.
    fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<(PathBuf, BuildVolumeHold), ApfsStorageError>;
    /// Mint a blank image while the caller retains its exclusive claim through publication.
    fn create_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<PathBuf, ApfsStorageError>;
    fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError>;
    /// Capture retained state while the caller keeps the source's existing shared volume hold
    /// and lifecycle/image lock. Ordinary jobs may keep running; release and adoption cannot.
    fn clone_shared_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &super::build_volumes::HeldVolume,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError>;
    fn clone_quiet_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        quiet: &super::build_volumes::Quiet,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError>;
    /// Capture the source `claim` claimed into seed `destination`, under that claim: a reseed
    /// takes it before it stops the source's Nx daemon, so no job (the daemon keeper's start job
    /// included) is admitted on the source until the capture is done (16_build_volumes.md,
    /// "Targets and seeds").
    fn clone_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError>;
    fn build_volume_written(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<SystemTime, ApfsStorageError>;
    /// Fork step 3's capture of `seed` into a new live volume registered as `registration`
    /// (16_build_volumes.md, "Fork"): the only step of it that reads `seed`, so the only one the
    /// fork takes under its source's image lock. Not yet proven or published.
    fn capture_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<CapturedBuildVolume, ApfsStorageError>;
    /// The captured live volume proven and published, then `own_seed` cloned from it when one is
    /// asked for, then the live volume mounted without the seed's Nx daemon record, once the
    /// source's image lock is let go: its id and mountpoint. A failure releases what it made.
    fn ready_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
        own_seed: Option<(&BuildVolumeId, &Registration)>,
    ) -> Result<(BuildVolumeId, PathBuf, Option<std::time::Duration>), ApfsStorageError>;
    /// Release a capture its failed fork never readies.
    fn abandon_captured_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
    ) -> Result<BuildVolumeRelease, ApfsStorageError>;
    fn mount_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<(PathBuf, Option<std::time::Duration>), ApfsStorageError>;
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
    /// Seal the still-linked volume without force, borrowing its exclusive admission claim.
    /// This never deletes storage. A native detach failure can follow an accepted unmount, so
    /// the caller restores canonical before dropping the retained claim on a prepublication
    /// failure. The claim remains usable for release (16_build_volumes.md, "Targets and seeds").
    fn detach_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<(), ApfsStorageError>;
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

    /// Census a mounted re-adoption input under its image lease, excluding in-flight cosmetic
    /// rename descriptors without exempting any actual filesystem holder.
    fn re_adopt_volume_holders(
        &self,
        image: &Path,
        mount: &Path,
    ) -> Result<Vec<cowshed_os::process::Holder>, ApfsStorageError>;

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
        mbx: Option<&Path>,
        retention: &[crate::capabilities::BuildStatePath],
    ) -> Result<(), ApfsStorageError>;

    /// Rewrite a build image under its retained exclusive admission claim.
    fn defragment_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<crate::storage::lifecycle::DefragmentOutcome, ApfsStorageError>;

    fn release_seed_compaction_leftovers(
        &self,
        layout: &BuildVolumeLayout,
    ) -> Result<(), ApfsStorageError>;
    fn stage_seed_compaction(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
    ) -> Result<CompactionSource, ApfsStorageError>;
    fn copy_seed_compaction(
        &self,
        source: CompactionSource,
        minimum_extents: u64,
        keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
    ) -> Result<SeedCompaction, ApfsStorageError>;
    fn publish_seed_compaction(
        &self,
        layout: &BuildVolumeLayout,
        compacted: CompactedSeed,
        destination: &BuildVolumeId,
        registration: Registration,
    ) -> Result<(), ApfsStorageError>;
    fn catch_up_seed_compaction(
        &self,
        compacted: CompactedSeed,
        next: CompactionSource,
        keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
    ) -> Result<SeedCatchUp, ApfsStorageError>;
}
