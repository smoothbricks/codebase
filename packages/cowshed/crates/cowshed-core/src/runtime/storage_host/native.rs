//! Native delegations keep the shared runtime on the existing APFS implementation.

use super::*;
use crate::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_apfs::CommandRunner;

impl<R> RuntimeStorageHost for MacOsApfsExecutionHost<R>
where
    R: CommandRunner + Send + Sync + 'static,
{
    fn canonical_metadata(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<DetachedWorkspaceMetadata>, ApfsStorageError> {
        MacOsApfsExecutionHost::canonical_metadata(self, repo, workspace)
    }

    fn pending_publications_for(
        &self,
        repo: &RepoId,
        workspaces: Option<&std::collections::BTreeSet<WorkspaceName>>,
    ) -> Result<Vec<crate::storage::apfs::PendingPublicationFact>, ApfsStorageError> {
        MacOsApfsExecutionHost::pending_publications_for(self, repo, workspaces)
    }

    fn workspace_snapshot(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata)>, ApfsStorageError> {
        MacOsApfsExecutionHost::workspace_snapshot(self, repo, workspace)
    }

    fn label_image_volume(&self, image: &Path, label: &str) -> Result<Labelled, ApfsStorageError> {
        MacOsApfsExecutionHost::label_image_volume(self, image, label)
    }

    fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<(PathBuf, BuildVolumeHold), ApfsStorageError> {
        MacOsApfsExecutionHost::create_build_volume(self, layout, id, capacity, registration)
    }

    fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        MacOsApfsExecutionHost::clone_build_volume(self, layout, source, destination, registration)
    }

    fn clone_quiet_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        quiet: &crate::runtime::build_volumes::Quiet,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        MacOsApfsExecutionHost::clone_quiet_build_volume(
            self,
            layout,
            quiet,
            destination,
            registration,
        )
    }

    fn clone_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        MacOsApfsExecutionHost::clone_claimed_build_volume(
            self,
            layout,
            claim,
            destination,
            registration,
        )
    }

    fn build_volume_written(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<SystemTime, ApfsStorageError> {
        MacOsApfsExecutionHost::build_volume_written(self, layout, id)
    }

    fn capture_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<CapturedBuildVolume, ApfsStorageError> {
        MacOsApfsExecutionHost::capture_fork_build_volume(self, layout, seed, registration)
    }

    fn ready_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
        own_seed: Option<(&BuildVolumeId, &Registration)>,
    ) -> Result<(BuildVolumeId, PathBuf, Option<std::time::Duration>), ApfsStorageError> {
        MacOsApfsExecutionHost::ready_fork_build_volume(self, layout, captured, own_seed)
    }

    fn abandon_captured_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
    ) -> Result<BuildVolumeRelease, ApfsStorageError> {
        MacOsApfsExecutionHost::abandon_captured_build_volume(self, layout, captured)
    }

    fn mount_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<(PathBuf, Option<std::time::Duration>), ApfsStorageError> {
        MacOsApfsExecutionHost::mount_build_volume(self, layout, id)
    }

    fn release_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeRelease, ApfsStorageError> {
        MacOsApfsExecutionHost::release_build_volume(self, layout, id)
    }

    fn claim_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeClaim, ApfsStorageError> {
        MacOsApfsExecutionHost::claim_build_volume(self, layout, id)
    }

    fn detach_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::detach_claimed_build_volume(self, layout, claim)
    }

    fn release_claimed(
        &self,
        layout: &BuildVolumeLayout,
        claim: ReleaseClaim,
    ) -> Result<Option<BuildVolumeRefusal>, ApfsStorageError> {
        MacOsApfsExecutionHost::release_claimed(self, layout, claim)
    }

    fn build_volume_capacity(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<ImageCapacity, ApfsStorageError> {
        MacOsApfsExecutionHost::build_volume_capacity(self, layout, id)
    }

    fn resize_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError> {
        MacOsApfsExecutionHost::resize_build_volume(self, layout, id, capacity)
    }

    fn re_adopt_volume_holders(
        &self,
        image: &Path,
        mount: &Path,
    ) -> Result<Vec<cowshed_os::process::Holder>, ApfsStorageError> {
        MacOsApfsExecutionHost::re_adopt_volume_holders(self, image, mount)
    }

    fn retain_re_adopt_source(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::retain_re_adopt_source(self, source)
    }

    fn attached_capacity_bytes(&self, image: &Path) -> Result<u64, ApfsStorageError> {
        MacOsApfsExecutionHost::attached_capacity_bytes(self, image)
    }

    fn retained_re_adopt_main_is_mounted(
        &self,
        source: &ReAdoptSource,
    ) -> Result<bool, ApfsStorageError> {
        MacOsApfsExecutionHost::retained_re_adopt_main_is_mounted(self, source)
    }

    fn mount_re_adopt_sources(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::mount_re_adopt_sources(self, source)
    }

    fn release_re_adopt_sources(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::release_re_adopt_sources(self, source)
    }

    fn copy_re_adopt_build(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::copy_re_adopt_build(self, source)
    }

    fn require_sparse_replacement(
        &self,
        replacement: &Path,
        retained: &Path,
        capacity_bytes: u64,
    ) -> Result<(u64, u64), ApfsStorageError> {
        MacOsApfsExecutionHost::require_sparse_replacement(
            self,
            replacement,
            retained,
            capacity_bytes,
        )
    }

    fn publish_re_adopt_build(
        &self,
        layout: &BuildVolumeLayout,
        source: &ReAdoptSource,
        build_claim: Option<&ReleaseClaim>,
    ) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::publish_re_adopt_build(self, layout, source, build_claim)
    }

    fn defragment_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<crate::storage::lifecycle::DefragmentOutcome, ApfsStorageError> {
        MacOsApfsExecutionHost::defragment_build_volume(self, layout, claim)
    }

    fn release_seed_compaction_leftovers(
        &self,
        layout: &BuildVolumeLayout,
    ) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::release_seed_compaction_leftovers(self, layout)
    }
    fn stage_seed_compaction(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
    ) -> Result<CompactionSource, ApfsStorageError> {
        MacOsApfsExecutionHost::stage_seed_compaction(self, layout, seed)
    }

    fn copy_seed_compaction(
        &self,
        source: CompactionSource,
        minimum_extents: u64,
        keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
    ) -> Result<SeedCompaction, ApfsStorageError> {
        MacOsApfsExecutionHost::copy_seed_compaction(self, source, minimum_extents, keep_going)
    }

    fn publish_seed_compaction(
        &self,
        layout: &BuildVolumeLayout,
        compacted: CompactedSeed,
        destination: &BuildVolumeId,
        registration: Registration,
    ) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::publish_seed_compaction(
            self,
            layout,
            compacted,
            destination,
            registration,
        )
    }

    fn catch_up_seed_compaction(
        &self,
        compacted: CompactedSeed,
        next: CompactionSource,
        keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
    ) -> Result<SeedCatchUp, ApfsStorageError> {
        MacOsApfsExecutionHost::catch_up_seed_compaction(self, compacted, next, keep_going)
    }
}
