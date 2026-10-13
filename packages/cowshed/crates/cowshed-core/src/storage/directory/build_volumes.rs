//! Build volumes in a directory store (16_build_volumes.md, "Substrate"): each one a payload
//! directory at the layout's `<id>.dmg`, bound at its mountpoint, with the same record, hold
//! file and job-admission claims as an image volume. A directory has no capacity, so nothing
//! here reads or grows one.

use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use super::{
    DirectoryExecutionHost, MountPointState, bind, host_error, mount_point_state, observed_mount,
    observed_payload_root, remove_payload_tree, unbind, unsupported,
};
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, LinkResolution,
    ReleaseClaim, link,
};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::storage::apfs::ApfsStorageError;
use crate::storage::apfs::native::{
    BuildVolumeClaim, BuildVolumeRefusal, BuildVolumeRelease, io_error, path_exists,
    sync_parent_path,
};
use crate::storage::lifecycle::ResizeOutcome;
use cowshed_apfs::image::ImageCapacity;
use cowshed_os::process::Holder;

/// The record beside a build volume of the write instant it holds everything up to
/// (`mark_seed_written`). It speaks only while the tree is unchanged since it was written.
const WRITTEN_SUFFIX: &str = ".written";
const WRITTEN_VERSION: u32 = 1;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WrittenMark {
    version: u32,
    seconds: u64,
    nanos: u32,
}

fn written_mark_path(image: &Path) -> PathBuf {
    crate::metadata::append_suffix(image, WRITTEN_SUFFIX)
}

fn change_time(metadata: &fs::Metadata) -> SystemTime {
    let seconds = u64::try_from(metadata.ctime()).unwrap_or_default();
    let nanos = u32::try_from(metadata.ctime_nsec()).unwrap_or_default();
    SystemTime::UNIX_EPOCH + Duration::new(seconds, nanos)
}

/// The last change anywhere in the tree at `root`: the newest inode change time of every entry
/// below the root, and the root's own modification time. A change time cannot be set back, so a
/// write that preserves modification times (`cp -p`, an unpacked archive) still moves it, and so
/// does a removal, through the directory that held the removed name. The root's change time is
/// left out because mounting and unmounting rename the root itself, which is no write to what
/// the volume holds; an entry added to or removed from the root still moves its modification
/// time.
fn tree_change_clock(root: &Path) -> io::Result<SystemTime> {
    let mut newest = fs::symlink_metadata(root)?.modified()?;
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path)? {
            let child = entry?.path();
            let metadata = fs::symlink_metadata(&child)?;
            newest = newest.max(change_time(&metadata));
            if metadata.is_dir() {
                pending.push(child);
            }
        }
    }
    Ok(newest)
}

fn remove_if_present(path: &Path) -> Result<(), ApfsStorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("remove build volume record", path, error)),
    }
}

/// Every process holding anything inside the payload tree at `image`, confirmed by a second
/// census (see `confirmed_tree_holders`).
fn holders_of(image: &Path) -> Result<Vec<Holder>, ApfsStorageError> {
    super::confirmed_tree_holders(image)
}

fn journal_release(layout: &BuildVolumeLayout, image: &Path, recorded: Option<&BuildVolumeRole>) {
    use crate::storage::deletion_log::{DeletionKind, DeletionOp, log_deletion};
    let (kind, workspace) = match recorded {
        Some(BuildVolumeRole::Seed { target, .. }) => (DeletionKind::BuildSeed, target.as_str()),
        Some(BuildVolumeRole::Linked { checkout }) => {
            (DeletionKind::BuildVolume, checkout.as_str())
        }
        Some(BuildVolumeRole::Unlinked) | None => (DeletionKind::BuildVolume, ""),
    };
    log_deletion(
        layout.project_root(),
        DeletionOp::ReleaseBuildVolume,
        kind,
        workspace,
        Some(image),
        image,
    );
}

impl DirectoryExecutionHost {
    /// Whether build volume `id` is mounted at its mountpoint. Mounted anywhere else refuses:
    /// the mountpoint is the volume's alone, and the volume mounts nowhere else.
    fn build_volume_bound(
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<bool, ApfsStorageError> {
        let image = layout.image(id);
        let mount = layout.mount(id);
        match observed_mount(&image)? {
            None => Ok(false),
            Some(current) if current == mount => Ok(true),
            Some(current) => Err(ApfsStorageError::Host(format!(
                "build volume {id} is mounted at {}, not at {}",
                current.display(),
                mount.display()
            ))),
        }
    }

    /// Mint the empty build volume `id` and mount it, without a record. A directory has no
    /// capacity; `capacity` bounds nothing here, the store's filesystem does.
    pub fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        _capacity: ImageCapacity,
    ) -> Result<PathBuf, ApfsStorageError> {
        let image = layout.image(id);
        self.require_store_image(&image)?;
        if path_exists(&image)? {
            return Err(ApfsStorageError::Host(format!(
                "build volume payload already exists: {}",
                image.display()
            )));
        }
        super::ensure_parent(&image)?;
        fs::create_dir(&image)
            .map_err(|error| io_error("create build volume payload", &image, error))?;
        sync_parent_path(&image)?;
        let mount = layout.mount(id);
        if let Err(primary) = bind(&image, &mount) {
            return crate::storage::apfs::combine_cleanup(
                "mount build volume",
                primary,
                remove_payload_tree(&image),
            );
        }
        Ok(mount)
    }

    /// Clone `source` to the new volume `destination` with `record`. A bound source is claimed
    /// against job admission for the whole capture, so nothing writes it while it is cloned.
    pub fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        let _restoring = if Self::build_volume_bound(layout, source)? {
            match self.claim_build_volume(layout, source)? {
                BuildVolumeClaim::Claimed(claim) => Some(claim),
                BuildVolumeClaim::Held { open } => {
                    return Err(ApfsStorageError::BuildVolumeHeld {
                        image: layout.image(source),
                        hold: layout.hold_path(source),
                        open,
                    });
                }
            }
        } else {
            self.require_store_image(&layout.image(source))?;
            None
        };
        self.clone_build_volume_under_hold(layout, source, destination, record)
    }

    /// Clone a source whose caller already keeps job admission off it: the quiet landing volume,
    /// under the hold its stopped socket lease keeps (16_build_volumes.md, Land step 4), where a
    /// second exclusive claim would conflict, or a reseed's source under the claim the reseed
    /// took before it stopped the source's Nx daemon ("Targets and seeds").
    pub fn clone_held_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        self.require_store_image(&layout.image(source))?;
        self.clone_build_volume_under_hold(layout, source, destination, record)
    }

    fn clone_build_volume_under_hold(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        let from = layout.image(source);
        let to = layout.image(destination);
        self.require_store_image(&to)?;
        // A tree copy of a held volume would capture a half-written state: refuse, naming
        // the holders, as a non-forced detach of the mounted volume would.
        let open = holders_of(&from)?;
        if !open.is_empty() {
            return Err(ApfsStorageError::BuildVolumeHeld {
                image: from,
                hold: layout.hold_path(source),
                open,
            });
        }
        let hold = layout.hold_path(destination);
        let _creating = layout
            .hold_new(destination)
            .map_err(|error| io_error("hold a build volume while it is cloned", &hold, error))?;
        let written = self.build_volume_written(layout, source)?;
        if let Err(error) = self.clone_tree(&from, &to) {
            // No payload, so nothing will ever release this hold file.
            if !path_exists(&to)? {
                remove_if_present(&hold)?;
            }
            return Err(error);
        }
        self.mark_seed_written(layout, destination, written)?;
        layout.write_record(destination, record).map_err(host_error)
    }

    /// The instant of the last write build volume `id` holds: its mark, while the tree is
    /// unchanged since the mark was written, and otherwise the tree's own last change.
    pub fn build_volume_written(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<SystemTime, ApfsStorageError> {
        let image = layout.image(id);
        self.require_store_image(&image)?;
        let root = observed_payload_root(&image)?;
        let clock = tree_change_clock(&root)
            .map_err(|error| io_error("read a build volume's last change", &root, error))?;
        let mark = written_mark_path(&image);
        let mark_metadata = match fs::symlink_metadata(&mark) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(clock),
            Err(error) => {
                return Err(io_error(
                    "inspect a build volume's written mark",
                    &mark,
                    error,
                ));
            }
        };
        if clock > change_time(&mark_metadata) {
            return Ok(clock);
        }
        let recorded: WrittenMark = crate::metadata::read_json(&mark).map_err(host_error)?;
        if recorded.version != WRITTEN_VERSION || recorded.nanos >= 1_000_000_000 {
            return Err(ApfsStorageError::Host(format!(
                "build volume written mark {} is not version {WRITTEN_VERSION}",
                mark.display()
            )));
        }
        Ok(SystemTime::UNIX_EPOCH + Duration::new(recorded.seconds, recorded.nanos))
    }

    /// Record that seed `seed` holds every write its target's live volume had at `written`.
    pub fn mark_seed_written(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        written: SystemTime,
    ) -> Result<(), ApfsStorageError> {
        let image = layout.image(seed);
        self.require_store_image(&image)?;
        let since_epoch = written
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| {
                ApfsStorageError::InvalidPlan("build volume write instant precedes the epoch")
            })?;
        let mark = written_mark_path(&image);
        crate::metadata::write_json(
            &mark,
            &WrittenMark {
                version: WRITTEN_VERSION,
                seconds: since_epoch.as_secs(),
                nanos: since_epoch.subsec_nanos(),
            },
        )
        .map_err(host_error)?;
        sync_parent_path(&mark)
    }

    /// A new live volume cloned from `seed` with `record`, bound, without the seed's Nx daemon
    /// record (16_build_volumes.md, "Fork" steps 2 and 3).
    pub fn fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(BuildVolumeId, PathBuf), ApfsStorageError> {
        let live = BuildVolumeId::mint();
        self.clone_build_volume(layout, seed, &live, record)?;
        let mount = self.mount_build_volume(layout, &live)?;
        let state = crate::build_volume::BuildVolumeState::read(&mount).map_err(host_error)?;
        crate::build_volume::nx::discard_daemon_records(&mount, &state)
            .map_err(|error| io_error("discard the seed's Nx daemon record", &mount, error))?;
        Ok((live, mount))
    }

    /// Bind build volume `id` at its mountpoint. Idempotent.
    pub fn mount_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<PathBuf, ApfsStorageError> {
        let image = layout.image(id);
        self.require_store_image(&image)?;
        let mount = layout.mount(id);
        bind(&image, &mount)?;
        Ok(mount)
    }

    /// Release build volume `id` unless something holds it: a job admitted on it, or any
    /// process with a file, directory or mapping open inside its tree. A directory cannot be
    /// unmounted out from under a holder, so a held volume is kept whole and answered as held.
    pub fn release_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeRelease, ApfsStorageError> {
        match self.claim_build_volume(layout, id)? {
            BuildVolumeClaim::Claimed(claim) => {
                let open = holders_of(&layout.image(id))?;
                if !open.is_empty() {
                    drop(claim);
                    return Ok(BuildVolumeRelease::Held { open });
                }
                Ok(BuildVolumeRelease::Deleted {
                    refused: self.release_claimed(layout, claim)?,
                })
            }
            BuildVolumeClaim::Held { open } => Ok(BuildVolumeRelease::Held { open }),
        }
    }

    /// Claim build volume `id` for its release, or answer what holds it.
    pub fn claim_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeClaim, ApfsStorageError> {
        let image = layout.image(id);
        self.require_store_image(&image)?;
        let hold = layout.hold_path(id);
        match layout
            .claim_release(id)
            .map_err(|error| io_error("claim a build volume for release", &hold, error))?
        {
            Some(claim) => Ok(BuildVolumeClaim::Claimed(claim)),
            None => Ok(BuildVolumeClaim::Held {
                open: holders_of(&image)?,
            }),
        }
    }

    /// Unbind and delete the volume `claim` claimed, with its record, mark, hold and
    /// mountpoint, journaled in the project's deletion log. A process that still has anything
    /// open inside the tree refuses the release before anything changes: there is no forced
    /// fallback, so a held seed is never deleted from under its holder. A release that goes
    /// through has no refusal to report.
    pub fn release_claimed(
        &self,
        layout: &BuildVolumeLayout,
        claim: ReleaseClaim,
    ) -> Result<Option<BuildVolumeRefusal>, ApfsStorageError> {
        let id = claim.id();
        let image = layout.image(id);
        self.require_store_image(&image)?;
        let mount = layout.mount(id);
        let present = path_exists(&image)?;
        let recorded = layout
            .read_record_present(id)
            .ok()
            .flatten()
            .map(|record| record.role);
        if present {
            let open = holders_of(&image)?;
            if !open.is_empty() {
                return Err(ApfsStorageError::BuildVolumeHeld {
                    hold: layout.hold_path(id),
                    image,
                    open,
                });
            }
        }
        // Nothing holds the tree (just checked), so an unforced unmount is the whole release.
        unbind(&image, &mount, cowshed_apfs::DetachIntent::Release)?;
        if present {
            remove_payload_tree(&image)?;
            journal_release(layout, &image, recorded.as_ref());
        }
        remove_if_present(&written_mark_path(&image))?;
        remove_if_present(&layout.record(id))?;
        remove_if_present(&layout.hold_path(id))?;
        match mount_point_state(&mount)? {
            MountPointState::Vacant => fs::remove_dir(&mount)
                .map_err(|error| io_error("remove build volume mountpoint", &mount, error))?,
            MountPointState::Absent => {}
            other => {
                return Err(ApfsStorageError::Host(format!(
                    "build volume {id}'s mountpoint {} is not vacant after release: {other:?}",
                    mount.display()
                )));
            }
        }
        drop(claim);
        Ok(None)
    }

    /// Bind what `workspace`'s checkout links once the link is settled
    /// (16_build_volumes.md, "One link per checkout").
    pub fn settle_build_link(
        &self,
        layout: &BuildVolumeLayout,
        workspace: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
        checkout: &Path,
        linked: &BuildVolumeId,
    ) -> Result<(), ApfsStorageError> {
        let refork = |seed: &BuildVolumeId| {
            let tree = layout.read_record(seed).map_err(host_error)?.tree;
            let (_, mount) = self.fork_build_volume(
                layout,
                seed,
                &BuildVolumeRecord::new(
                    tree,
                    BuildVolumeRole::Linked {
                        checkout: workspace.clone(),
                    },
                ),
            )?;
            link::point(checkout, &mount).map_err(host_error)
        };
        match layout.resolve_link(workspace, linked).map_err(host_error)? {
            LinkResolution::Keep => self.mount_build_volume(layout, linked).map(|_| ()),
            LinkResolution::Repoint(own) => {
                let mount = self.mount_build_volume(layout, &own)?;
                link::point(checkout, &mount).map_err(host_error)
            }
            LinkResolution::Refork(seed) => refork(&seed),
            LinkResolution::Vanished => {
                let lost = |error: ApfsStorageError| {
                    ApfsStorageError::Host(format!(
                        "{workspace}'s build link names build volume {linked}, which no longer exists, and settling the link failed: {error}"
                    ))
                };
                match layout.seed_of(workspace, incarnation).map_err(host_error)? {
                    Some((seed, _)) => {
                        refork(&seed).map_err(lost)?;
                        eprintln!(
                            "cowshed: {workspace}'s build link named build volume {linked}, which no longer exists; {workspace} now links a fresh clone of its seed {seed}, and what {linked} held beyond that seed is lost"
                        );
                    }
                    None => {
                        link::unlink(checkout).map_err(host_error).map_err(lost)?;
                        eprintln!(
                            "cowshed: {workspace}'s build link named build volume {linked}, which no longer exists, and {workspace} has no seed to clone; the link is removed, and {workspace}'s next build-state refresh makes its first volume"
                        );
                    }
                }
                Ok(())
            }
        }
    }

    pub fn build_volume_capacity(
        &self,
        _layout: &BuildVolumeLayout,
        _id: &BuildVolumeId,
    ) -> Result<ImageCapacity, ApfsStorageError> {
        Err(unsupported("build volume capacities"))
    }

    pub fn resize_build_volume(
        &self,
        _layout: &BuildVolumeLayout,
        _id: &BuildVolumeId,
        _capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError> {
        Err(unsupported("resizing a build volume"))
    }
}
