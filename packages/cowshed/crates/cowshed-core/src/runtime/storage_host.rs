//! Storage capabilities used by the shared project controller beyond lifecycle execution.
//!
//! Handles and locks remain the execution host's associated types. A directory store does not
//! own a kernel volume: its controller never configures physical volume labels or counters.
//!
//! Which execution host is this host's native one is the platform's (`macos.rs`: the APFS image
//! host or the dataset host, whichever a project is kept on; `linux.rs`: the directory host,
//! over ZFS datasets or plain directories), selected here and nowhere else. What an adoption may
//! keep a project on is [`AdoptFilesystems`], which each host answers for itself.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::api::dto::StoreSubstrate;
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, CapturedBuildVolume, Registration, ReleaseClaim,
};
use crate::metadata::{DetachedWorkspaceMetadata, Platform, WorkspaceName};
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

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

pub(crate) use platform::require_native_substrate;
/// The platform's native execution host and its constructor, public (`runtime::{NativeHost,
/// native_host}`) so a composition root that works from store records alone builds the same
/// host a project controller runs on.
pub use platform::{NativeHost, native_host};

/// Refuse a filesystem no store of this platform keeps a project on (`directory` on macOS,
/// `apfs` on Linux), as the usage error the adoption itself would answer. It reads nothing, so
/// an adoption asks it before the host's storage is set up or opened; what this host's own
/// store serves, and what an adopted project has, the adoption decides once the store stands
/// ([`AdoptFilesystems::adopt`]).
pub fn require_platform_filesystem(requested: Option<StoreSubstrate>) -> crate::Result<()> {
    platform::PLATFORM_FILESYSTEMS
        .adopt(None, requested)
        .map(drop)
        .map_err(AdoptFilesystemRefusal::into_error)
}

/// What a host keeps a project it adopts on (09_substrates.md, where selection is described):
/// the filesystem an adoption naming none gets, and what else an adoption may name there.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdoptFilesystems {
    /// The platform's default, or the one filesystem the store is.
    pub(crate) default: StoreSubstrate,
    pub(crate) others: &'static [StoreSubstrate],
}

/// Why an adoption that names a filesystem is refused, decided before the adoption changes
/// anything.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdoptFilesystemRefusal {
    /// The host keeps no project on `requested`.
    Unserved {
        requested: StoreSubstrate,
        serves: AdoptFilesystems,
    },
    /// The project is adopted already, on `has`: an adoption never moves it.
    Kept {
        requested: StoreSubstrate,
        has: StoreSubstrate,
    },
}

impl AdoptFilesystems {
    /// A host whose store is one filesystem and keeps every project on it.
    pub(crate) const fn only(substrate: StoreSubstrate) -> Self {
        Self {
            default: substrate,
            others: &[],
        }
    }

    fn serves(self, substrate: StoreSubstrate) -> bool {
        self.default == substrate || self.others.contains(&substrate)
    }

    /// What an adoption naming `requested` keeps the project on, where `existing` is what the
    /// project's main is kept on already and `None` for a project with no main yet. Naming none
    /// keeps what the project has, or the default; naming what the project has changes nothing.
    pub(crate) fn adopt(
        self,
        existing: Option<StoreSubstrate>,
        requested: Option<StoreSubstrate>,
    ) -> Result<StoreSubstrate, AdoptFilesystemRefusal> {
        let Some(requested) = requested else {
            return Ok(existing.unwrap_or(self.default));
        };
        if !self.serves(requested) {
            return Err(AdoptFilesystemRefusal::Unserved {
                requested,
                serves: self,
            });
        }
        match existing {
            Some(has) if has != requested => Err(AdoptFilesystemRefusal::Kept { requested, has }),
            Some(_) | None => Ok(requested),
        }
    }
}

impl AdoptFilesystemRefusal {
    /// The usage error an adoption answers this refusal as: the value it named and what stands
    /// against it. A named conversion, not `From`: the crate's error keeps `?` inferable by
    /// converting from itself alone.
    pub(crate) fn into_error(self) -> crate::CowshedError {
        match self {
            Self::Unserved { requested, serves } => {
                let mut named = serves.default.to_string();
                for other in serves.others {
                    named.push_str(" or ");
                    named.push_str(other.as_str());
                }
                crate::CowshedError::usage(
                    format!(
                        "this host does not keep a project on {requested}: it adopts one onto \
                         {named}"
                    ),
                    "omit --filesystem, or name one this host adopts onto",
                )
            }
            Self::Kept { requested, has } => crate::CowshedError::usage(
                format!(
                    "this project is already adopted on {has}; adopting it again does not move \
                     it to {requested}"
                ),
                format!(
                    "omit --filesystem; to keep the project on {requested}, cowshed unadopt it \
                     and adopt it with --filesystem {requested}"
                ),
            ),
        }
    }
}

/// The controller's storage boundary, shared by native images and real directory stores.
///
/// These operations preserve the lifecycle host's lock and publication authority. Physical
/// operations on a backend without kernel volumes must return an explicit unsupported error,
/// never a successful disk result. The capability flag keeps optional cosmetic labels and job
/// volume sampling out of those backends; their resource samples report unconfigured volumes.
pub(crate) trait RuntimeStorageHost: ApfsExecutionHost<LockGuard: Sync> {
    /// What an adoption through this host may keep its project on. A host over one kind of
    /// store keeps every project on that kind.
    fn adopt_filesystems(&self) -> AdoptFilesystems {
        AdoptFilesystems::only(self.substrate())
    }
    /// The platform this host records in the sidecar of every workspace it publishes
    /// ([`DetachedWorkspaceMetadata::platform`]): what decides, before a workspace exists,
    /// whether its grants start with a port block (macOS) or with none (Linux).
    fn sidecar_platform(&self) -> Platform;
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
    /// before the image's first byte exists (16_build_volumes.md, "Garbage collection"); a clone
    /// is published before it answers. A created volume stays registered, held by the answered
    /// exclusive claim, until the caller publishes it and lets the claim go: the creator of a
    /// checkout's first volume captures its seed under that claim ("Targets and seeds").
    fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<(PathBuf, ReleaseClaim), ApfsStorageError>;
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
    /// asked for, then the live volume mounted without the seed's Nx daemon record and, for an mbx
    /// fork, with the rebuildable state `nx` says it does not keep moved aside, once the source's
    /// image lock is let go: its id and mountpoint. A failure releases what it made.
    fn ready_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
        own_seed: Option<(&BuildVolumeId, &Registration)>,
        nx: crate::build_volume::ForkNx,
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
    /// [`Self::claim_build_volume`] without the census of what a held volume has open: the claim,
    /// or `None` while a job is admitted on the volume or another operation claims it. For a
    /// caller the hold alone decides (16_build_volumes.md, "A non-mbx reseed excludes the named
    /// non-transactional writers", step 1).
    fn try_claim_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<Option<ReleaseClaim>, ApfsStorageError>;
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

#[cfg(test)]
mod tests {
    use super::{AdoptFilesystemRefusal, AdoptFilesystems};
    use crate::api::dto::StoreSubstrate::{self, Apfs, Directory, Zfs};

    /// macOS: an APFS store whose projects are images unless adopted onto ZFS.
    const MACOS: AdoptFilesystems = AdoptFilesystems {
        default: Apfs,
        others: &[Zfs],
    };
    /// Linux: the store is one filesystem.
    const ZFS_STORE: AdoptFilesystems = AdoptFilesystems::only(Zfs);
    const DIRECTORY_STORE: AdoptFilesystems = AdoptFilesystems::only(Directory);
    /// Linux before its store is read: either of the two a store can be.
    const LINUX: AdoptFilesystems = AdoptFilesystems {
        default: Zfs,
        others: &[Directory],
    };

    fn unserved(
        requested: StoreSubstrate,
        serves: AdoptFilesystems,
    ) -> Result<StoreSubstrate, AdoptFilesystemRefusal> {
        Err(AdoptFilesystemRefusal::Unserved { requested, serves })
    }

    fn kept(
        requested: StoreSubstrate,
        has: StoreSubstrate,
    ) -> Result<StoreSubstrate, AdoptFilesystemRefusal> {
        Err(AdoptFilesystemRefusal::Kept { requested, has })
    }

    #[test]
    fn an_adoption_keeps_a_project_on_what_the_host_serves_and_never_moves_it() {
        let table = [
            // An omitted filesystem is the default, or what the project already has.
            (MACOS, None, None, Ok(Apfs)),
            (MACOS, Some(Apfs), None, Ok(Apfs)),
            (MACOS, Some(Zfs), None, Ok(Zfs)),
            (ZFS_STORE, None, None, Ok(Zfs)),
            (ZFS_STORE, Some(Zfs), None, Ok(Zfs)),
            (DIRECTORY_STORE, None, None, Ok(Directory)),
            (DIRECTORY_STORE, Some(Directory), None, Ok(Directory)),
            // A project with no main yet is kept on any filesystem the host serves.
            (MACOS, None, Some(Apfs), Ok(Apfs)),
            (MACOS, None, Some(Zfs), Ok(Zfs)),
            (ZFS_STORE, None, Some(Zfs), Ok(Zfs)),
            (DIRECTORY_STORE, None, Some(Directory), Ok(Directory)),
            // Naming what an adopted project has changes nothing.
            (MACOS, Some(Apfs), Some(Apfs), Ok(Apfs)),
            (MACOS, Some(Zfs), Some(Zfs), Ok(Zfs)),
            (ZFS_STORE, Some(Zfs), Some(Zfs), Ok(Zfs)),
            (
                DIRECTORY_STORE,
                Some(Directory),
                Some(Directory),
                Ok(Directory),
            ),
            // An adopted project is never moved to another filesystem the host serves.
            (MACOS, Some(Apfs), Some(Zfs), kept(Zfs, Apfs)),
            (MACOS, Some(Zfs), Some(Apfs), kept(Apfs, Zfs)),
            // A filesystem the host does not serve is refused, adopted or not.
            (MACOS, None, Some(Directory), unserved(Directory, MACOS)),
            (LINUX, None, Some(Apfs), unserved(Apfs, LINUX)),
            (LINUX, None, Some(Zfs), Ok(Zfs)),
            (LINUX, None, Some(Directory), Ok(Directory)),
            (
                MACOS,
                Some(Apfs),
                Some(Directory),
                unserved(Directory, MACOS),
            ),
            (
                MACOS,
                Some(Zfs),
                Some(Directory),
                unserved(Directory, MACOS),
            ),
            (ZFS_STORE, None, Some(Apfs), unserved(Apfs, ZFS_STORE)),
            (
                ZFS_STORE,
                None,
                Some(Directory),
                unserved(Directory, ZFS_STORE),
            ),
            (
                ZFS_STORE,
                Some(Zfs),
                Some(Directory),
                unserved(Directory, ZFS_STORE),
            ),
            (
                DIRECTORY_STORE,
                None,
                Some(Apfs),
                unserved(Apfs, DIRECTORY_STORE),
            ),
            (
                DIRECTORY_STORE,
                None,
                Some(Zfs),
                unserved(Zfs, DIRECTORY_STORE),
            ),
            (
                DIRECTORY_STORE,
                Some(Directory),
                Some(Zfs),
                unserved(Zfs, DIRECTORY_STORE),
            ),
        ];
        for (serves, existing, requested, expected) in table {
            assert_eq!(
                serves.adopt(existing, requested),
                expected,
                "{serves:?} adopting a project on {existing:?} as {requested:?}"
            );
        }
    }

    #[test]
    fn a_refusal_is_a_usage_error_naming_the_value_and_what_stands() {
        let unserved = AdoptFilesystemRefusal::Unserved {
            requested: Directory,
            serves: MACOS,
        }
        .into_error();
        assert_eq!(unserved.code, crate::ErrorCode::Usage);
        assert!(unserved.message.contains("on directory"), "{unserved}");
        assert!(unserved.message.contains("apfs or zfs"), "{unserved}");
        let kept = AdoptFilesystemRefusal::Kept {
            requested: Apfs,
            has: Zfs,
        }
        .into_error();
        assert_eq!(kept.code, crate::ErrorCode::Usage);
        assert!(kept.message.contains("already adopted on zfs"), "{kept}");
        assert!(kept.message.contains("to apfs"), "{kept}");
    }
}
