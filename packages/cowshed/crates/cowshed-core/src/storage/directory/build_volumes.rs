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

fn tree_change_clock(root: &Path) -> io::Result<SystemTime> {
    let mut newest = fs::symlink_metadata(root)
        .and_then(|metadata| metadata.modified())
        .map_err(|error| clock_error(root, error))?;
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(error) if path != root && disappeared(&error) => {
                newest = newest.max(removal_clock(root, &path)?);
                continue;
            }
            Err(error) => return Err(clock_error(&path, error)),
        };
        scan_clock_entries(root, &path, entries, &mut newest, &mut pending)?;
    }
    // A removed child is a write, but a lost volume is not an empty or fresh tree.
    fs::read_dir(root).map_err(|error| clock_error(root, error))?;
    Ok(newest)
}

fn disappeared(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
    )
}

fn clock_error(path: &Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("inspect tree clock {}: {error}", path.display()),
    )
}

/// A name just observed in a directory vanished or changed kind. Its nearest
/// surviving ancestor records that mutation; never manufacture a wall-clock mark.
fn removal_clock(root: &Path, removed: &Path) -> io::Result<SystemTime> {
    let mut parent = removed
        .parent()
        .filter(|parent| parent.starts_with(root))
        .ok_or_else(|| {
            clock_error(
                removed,
                io::Error::new(io::ErrorKind::NotFound, "volume root is unavailable"),
            )
        })?;
    loop {
        match fs::symlink_metadata(parent) {
            Ok(metadata) => {
                return if parent == root {
                    metadata
                        .modified()
                        .map_err(|error| clock_error(parent, error))
                } else {
                    Ok(change_time(&metadata))
                };
            }
            Err(error) if parent != root && disappeared(&error) => {
                parent = parent
                    .parent()
                    .expect("a descendant of the root has a parent");
            }
            Err(error) => return Err(clock_error(parent, error)),
        }
    }
}

/// Consume the actual listing separately from metadata reads: a live Nx daemon
/// may unlink a temporary file or directory between those filesystem operations.
fn scan_clock_entries(
    root: &Path,
    directory: &Path,
    entries: impl Iterator<Item = io::Result<fs::DirEntry>>,
    newest: &mut SystemTime,
    pending: &mut Vec<PathBuf>,
) -> io::Result<()> {
    for entry in entries {
        let child = entry.map_err(|error| clock_error(directory, error))?.path();
        match fs::symlink_metadata(&child) {
            Ok(metadata) => {
                *newest = (*newest).max(change_time(&metadata));
                if metadata.is_dir() {
                    pending.push(child);
                }
            }
            Err(error) if disappeared(&error) => {
                *newest = (*newest).max(removal_clock(root, &child)?);
            }
            Err(error) => return Err(clock_error(&child, error)),
        }
    }
    Ok(())
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
                        holds: Vec::new(),
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
                holds: Vec::new(),
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

    /// Seal the still-linked payload under its caller's exclusive admission claim
    /// (16_build_volumes.md, "Targets and seeds"). This is an idle tree census and an unbind,
    /// not a physical detach: the payload, record, mark, hold and empty mountpoint survive. A
    /// failed unbind restores the previous binding, reporting both errors if restoration fails.
    pub fn detach_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<(), ApfsStorageError> {
        let id = claim.id();
        let image = layout.image(id);
        self.require_store_image(&image)?;
        let mount = layout.mount(id);
        let was_bound = Self::build_volume_bound(layout, id)?;
        let open = holders_of(&image)?;
        if !open.is_empty() {
            return Err(ApfsStorageError::BuildVolumeHeld {
                hold: layout.hold_path(id),
                image,
                holds: Vec::new(),
                open,
            });
        }
        match unbind(&image, &mount, cowshed_apfs::DetachIntent::WhenIdle) {
            Ok(()) => Ok(()),
            Err(primary) if was_bound => crate::storage::apfs::combine_cleanup(
                "detach and restore claimed build volume",
                primary,
                bind(&image, &mount),
            ),
            Err(primary) => Err(primary),
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
                    holds: Vec::new(),
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

#[cfg(test)]
mod clock_tests {
    use super::*;

    #[test]
    fn listed_child_removal_uses_surviving_parent_clock_and_missing_root_refuses() {
        let root =
            std::env::temp_dir().join(format!("cowshed-clock-{}", uuid::Uuid::new_v4().simple()));
        let parent = root.join("nx/workspace-data");
        fs::create_dir_all(&parent).unwrap();
        let temporary = parent.join("watch-start");
        fs::write(&temporary, b"probe").unwrap();
        let listed: Vec<_> = fs::read_dir(&parent).unwrap().collect();
        fs::remove_file(&temporary).unwrap();
        let actual = change_time(&fs::symlink_metadata(&parent).unwrap());
        let mut newest = SystemTime::UNIX_EPOCH;
        let mut pending = Vec::new();
        scan_clock_entries(
            &root,
            &parent,
            listed.into_iter(),
            &mut newest,
            &mut pending,
        )
        .unwrap();
        assert_eq!(
            newest, actual,
            "the observed deletion clock, not wall-clock now"
        );
        assert!(pending.is_empty());

        let nested = parent.join("transient/nested");
        fs::create_dir_all(&nested).unwrap();
        let child = nested.join("data");
        fs::write(&child, b"data").unwrap();
        let listed: Vec<_> = fs::read_dir(&nested).unwrap().collect();
        fs::remove_dir_all(parent.join("transient")).unwrap();
        let actual = change_time(&fs::symlink_metadata(&parent).unwrap());
        newest = SystemTime::UNIX_EPOCH;
        scan_clock_entries(
            &root,
            &nested,
            listed.into_iter(),
            &mut newest,
            &mut pending,
        )
        .unwrap();
        assert_eq!(
            newest, actual,
            "a removed parent uses its surviving ancestor"
        );
        assert!(tree_change_clock(&root).unwrap() >= actual);

        fs::remove_dir_all(&root).unwrap();
        let error = removal_clock(&root, &child).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(
            error.to_string().contains(&root.display().to_string()),
            "{error}"
        );
        assert_eq!(
            tree_change_clock(&root).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }
}

#[cfg(test)]
mod detach_tests {
    use super::*;
    use crate::repository::{ProjectPaths, RepoId};
    use crate::storage::apfs::ApfsSubstrateConfig;

    #[test]
    fn claimed_build_detach_refuses_holders_and_preserves_payload_metadata_and_claim() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-directory-build-detach-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        let root = fs::canonicalize(root).unwrap();
        let store = root.join("store");
        let host =
            DirectoryExecutionHost::new(ApfsSubstrateConfig::new(&store, root.join("checkout")))
                .unwrap();
        let project = ProjectPaths::with_mount_root(
            &store,
            root.join("mnt"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let layout = BuildVolumeLayout::new(&project).unwrap();
        let id = BuildVolumeId::mint();
        let image = layout.image(&id);
        fs::create_dir_all(&image).unwrap();
        fs::write(image.join("warm"), b"old build state\n").unwrap();
        layout
            .write_record(
                &id,
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Linked {
                        checkout: WorkspaceName::main(),
                    },
                ),
            )
            .unwrap();
        host.mark_seed_written(&layout, &id, SystemTime::now())
            .unwrap();
        let mark = written_mark_path(&image);
        let mark_bytes = fs::read(&mark).unwrap();
        let record = fs::read(layout.record(&id)).unwrap();
        let payload_inode = fs::metadata(&image).unwrap().ino();
        let mount = host.mount_build_volume(&layout, &id).unwrap();
        let claim = layout.claim_release(&id).unwrap().unwrap();
        let hold_inode = fs::metadata(layout.hold_path(&id)).unwrap().ino();
        let open = fs::File::open(mount.join("warm")).unwrap();
        let me = i32::try_from(std::process::id()).unwrap();

        let error = host
            .detach_claimed_build_volume(&layout, &claim)
            .expect_err("an unmanaged tree holder must refuse the unbind");
        let ApfsStorageError::BuildVolumeHeld { open: holders, .. } = &error else {
            panic!("the refusal must name tree holders: {error:?}");
        };
        assert!(holders.iter().any(|holder| holder.pid == me), "{holders:?}");
        assert!(error.to_string().contains(&format!("pid {me} (")));
        assert_eq!(observed_mount(&image).unwrap(), Some(mount.clone()));
        assert_eq!(fs::read(mount.join("warm")).unwrap(), b"old build state\n");
        assert!(layout.hold(&id).is_err());
        // Exercise the unbind's own census too: a holder arriving after the first census
        // must have the same typed refusal, without parsing a host-error string.
        let late = unbind(&image, &mount, cowshed_apfs::DetachIntent::WhenIdle).unwrap_err();
        let ApfsStorageError::BuildVolumeHeld {
            image: refused_image,
            hold,
            open: holders,
            ..
        } = &late
        else {
            panic!("the unbind census must preserve holder taxonomy: {late:?}");
        };
        assert_eq!(refused_image, &image);
        assert_eq!(hold, &layout.hold_path(&id));
        assert!(holders.iter().any(|holder| holder.pid == me), "{holders:?}");
        assert!(late.to_string().contains(&format!("pid {me} (")));
        assert_eq!(observed_mount(&image).unwrap(), Some(mount.clone()));
        drop(open);

        for _ in 0..2 {
            host.detach_claimed_build_volume(&layout, &claim).unwrap();
            assert_eq!(observed_mount(&image).unwrap(), None);
            assert_eq!(fs::metadata(&image).unwrap().ino(), payload_inode);
            assert_eq!(fs::read(image.join("warm")).unwrap(), b"old build state\n");
            assert_eq!(fs::read(layout.record(&id)).unwrap(), record);
            assert_eq!(fs::read(&mark).unwrap(), mark_bytes);
            assert_eq!(
                fs::metadata(layout.hold_path(&id)).unwrap().ino(),
                hold_inode
            );
            assert!(matches!(
                mount_point_state(&mount).unwrap(),
                MountPointState::Vacant
            ));
            assert!(
                layout.hold(&id).is_err(),
                "the borrowed claim still fences admission"
            );
        }
        host.mount_build_volume(&layout, &id).unwrap();
        assert_eq!(fs::read(mount.join("warm")).unwrap(), b"old build state\n");
        host.detach_claimed_build_volume(&layout, &claim).unwrap();
        assert_eq!(host.release_claimed(&layout, claim).unwrap(), None);
        for path in [
            image,
            layout.record(&id),
            mark,
            layout.hold_path(&id),
            mount,
        ] {
            assert!(
                !path.exists(),
                "ordinary release removes {}",
                path.display()
            );
        }
        fs::remove_dir_all(root).unwrap();
    }
}
