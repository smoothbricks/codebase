//! The project controller's storage capabilities over a directory store.
//!
//! Build volumes are real payload directories (`build_volumes.rs`). Volume labels, capacities
//! and re-adoption are physical-image operations: re-adoption moves a replaced main's image and
//! its build image aside, attaches them read-only and proves the replacement sparse, and none of
//! that has a directory counterpart, so each is refused with `ApfsStorageError::Unsupported`.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{DirectoryExecutionHost, unsupported};
use crate::build_volume::{
    BuildVolumeHold, BuildVolumeId, BuildVolumeLayout, CapturedBuildVolume, Registration,
    ReleaseClaim,
};
use crate::metadata::{DetachedWorkspaceMetadata, WorkspaceName};
use crate::repository::RepoId;
use crate::runtime::build_volumes::Quiet;
use crate::runtime::storage_host::RuntimeStorageHost;
use crate::storage::apfs::native::{BuildVolumeClaim, BuildVolumeRefusal, BuildVolumeRelease};
use crate::storage::apfs::{ApfsStorageError, Labelled};
use crate::storage::lifecycle::DerivedWorkspace;
use crate::storage::lifecycle::ResizeOutcome;
use crate::storage::recovery::ReAdoptSource;
use cowshed_apfs::image::ImageCapacity;

impl RuntimeStorageHost for DirectoryExecutionHost {
    fn canonical_metadata(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<DetachedWorkspaceMetadata>, ApfsStorageError> {
        DirectoryExecutionHost::canonical_metadata(self, repo, workspace)
    }

    fn pending_publications_for(
        &self,
        repo: &RepoId,
        workspaces: Option<&std::collections::BTreeSet<WorkspaceName>>,
    ) -> Result<Vec<crate::storage::apfs::PendingPublicationFact>, ApfsStorageError> {
        DirectoryExecutionHost::pending_publications_for(self, repo, workspaces)
    }

    fn workspace_snapshot(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata)>, ApfsStorageError> {
        DirectoryExecutionHost::workspace_snapshot(self, repo, workspace)
    }

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
        registration: &Registration,
    ) -> Result<(PathBuf, BuildVolumeHold), ApfsStorageError> {
        DirectoryExecutionHost::create_build_volume(self, layout, id, capacity, registration)
    }

    fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        DirectoryExecutionHost::clone_build_volume(self, layout, source, destination, registration)
            .map(|()| None)
    }

    fn clone_quiet_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        quiet: &Quiet,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        self.clone_held_build_volume(layout, quiet.id(), destination, registration)
            .map(|()| None)
    }

    fn clone_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        self.clone_held_build_volume(layout, claim.id(), destination, registration)
            .map(|()| None)
    }

    fn build_volume_written(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<SystemTime, ApfsStorageError> {
        DirectoryExecutionHost::build_volume_written(self, layout, id)
    }

    fn capture_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<CapturedBuildVolume, ApfsStorageError> {
        DirectoryExecutionHost::capture_fork_build_volume(self, layout, seed, registration)
    }

    fn ready_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
        own_seed: Option<(&BuildVolumeId, &Registration)>,
    ) -> Result<(BuildVolumeId, PathBuf, Option<std::time::Duration>), ApfsStorageError> {
        DirectoryExecutionHost::ready_fork_build_volume(self, layout, captured, own_seed)
            .map(|(id, mount)| (id, mount, None))
    }

    fn abandon_captured_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
    ) -> Result<BuildVolumeRelease, ApfsStorageError> {
        DirectoryExecutionHost::abandon_captured_build_volume(self, layout, captured)
    }

    fn mount_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<(PathBuf, Option<std::time::Duration>), ApfsStorageError> {
        DirectoryExecutionHost::mount_build_volume(self, layout, id).map(|mount| (mount, None))
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

    fn detach_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<(), ApfsStorageError> {
        DirectoryExecutionHost::detach_claimed_build_volume(self, layout, claim)
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

    fn defragment_build_volume(
        &self,
        _layout: &BuildVolumeLayout,
        _claim: &ReleaseClaim,
    ) -> Result<crate::storage::lifecycle::DefragmentOutcome, ApfsStorageError> {
        Err(unsupported("defragmenting a build volume"))
    }

    fn release_seed_compaction_leftovers(
        &self,
        _layout: &BuildVolumeLayout,
    ) -> Result<(), ApfsStorageError> {
        Err(unsupported("compacting a seed"))
    }
    fn stage_seed_compaction(
        &self,
        _layout: &BuildVolumeLayout,
        _seed: &BuildVolumeId,
    ) -> Result<crate::storage::apfs::native::CompactionSource, ApfsStorageError> {
        Err(unsupported("compacting a seed"))
    }

    fn copy_seed_compaction(
        &self,
        _source: crate::storage::apfs::native::CompactionSource,
        _minimum_extents: u64,
        _keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
    ) -> Result<crate::storage::apfs::native::SeedCompaction, ApfsStorageError> {
        Err(unsupported("compacting a seed"))
    }

    fn publish_seed_compaction(
        &self,
        _layout: &BuildVolumeLayout,
        _compacted: crate::storage::apfs::native::CompactedSeed,
        _destination: &BuildVolumeId,
        _registration: crate::build_volume::Registration,
    ) -> Result<(), ApfsStorageError> {
        Err(unsupported("compacting a seed"))
    }

    fn catch_up_seed_compaction(
        &self,
        _compacted: crate::storage::apfs::native::CompactedSeed,
        _next: crate::storage::apfs::native::CompactionSource,
        _keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
    ) -> Result<crate::storage::apfs::native::SeedCatchUp, ApfsStorageError> {
        Err(unsupported("compacting a seed"))
    }
}
