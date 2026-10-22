//! Build volumes in a directory store (16_build_volumes.md, "Substrate"): each one a payload
//! directory at the layout's `<id>.dmg`, bound at its mountpoint, with the same record, hold
//! file and job-admission claims as an image volume. A directory has no capacity, so nothing
//! here reads or grows one.

use std::ffi::{CStr, CString, OsStr};
use std::fs::{self, File};
use std::io;
use std::ops::ControlFlow;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use super::{
    DirectoryExecutionHost, MountPointState, bind, host_error, mount_point_state, observed_mount,
    observed_payload_root, remove_payload_tree, unbind, unsupported,
};
use crate::build_volume::{
    BuildVolumeHold, BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole,
    LinkResolution, Registrant, Registration, ReleaseClaim, link,
};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::storage::apfs::ApfsStorageError;
use crate::storage::apfs::native::{
    BuildVolumeClaim, BuildVolumeRefusal, BuildVolumeRelease, io_error, path_exists,
    sync_parent_path,
};
use crate::storage::apfs::release::{Drained, Reclaim};
use crate::storage::lifecycle::ResizeOutcome;
use cowshed_apfs::image::ImageCapacity;
use cowshed_os::fsio::AnchoredDirectory;
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
/// The last component of `path` as a directory-relative name.
fn leaf(path: &Path) -> Result<CString, ApfsStorageError> {
    path.file_name()
        .and_then(|name| CString::new(name.as_bytes()).ok())
        .ok_or_else(|| ApfsStorageError::Host(format!("{} names no entry", path.display())))
}

/// The project's images directory, held by descriptor, refusing a link or a non-directory there.
fn held_images(layout: &BuildVolumeLayout) -> Result<File, ApfsStorageError> {
    cowshed_os::fsio::open_directory_nofollow(layout.images())
        .map_err(|error| io_error("open the build images directory", layout.images(), error))
}

/// The project's build journal held by descriptor, absent when missing. Every component
/// is opened without following links; the pathname is never used again for its entries.
fn held_journal(layout: &BuildVolumeLayout) -> Result<Option<AnchoredDirectory>, ApfsStorageError> {
    let journal = layout.releasing();
    match AnchoredDirectory::open(&journal) {
        Ok(held) => Ok(Some(held)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("open the build release journal", &journal, error)),
    }
}

/// A held release prepared for deletion. Its preparation owns the rename's durability barrier;
/// finishing consumes it and never resolves the journal's path again.
struct DirectoryRelease<'a> {
    journal: &'a AnchoredDirectory,
    name: &'a CStr,
    tree: AnchoredDirectory,
}

impl<'a> DirectoryRelease<'a> {
    fn prepare(
        images: &File,
        journal: &'a AnchoredDirectory,
        name: &'a CStr,
        tree: AnchoredDirectory,
    ) -> io::Result<Self> {
        // This entry was observed before these syncs. A committer may have stopped just after
        // its rename; do not delete any byte until both directory changes are durable.
        journal.sync()?;
        cowshed_os::fsio::sync_held_directory(images)?;
        Ok(Self {
            journal,
            name,
            tree,
        })
    }

    fn finish(self) -> io::Result<u64> {
        self.journal.remove_tree(self.name, &self.tree)
    }
}

/// Whether `path` was there to remove.
fn remove_if_present(path: &Path) -> Result<bool, ApfsStorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("remove build volume record", path, error)),
    }
}

/// Inspect holders without opening the payload ourselves: that descriptor would itself be a
/// holder. The path is derived from the held journal's current kernel name, and its entry must
/// keep the fd-relative identity observed before the census. Deletion never uses this path.
fn held_tree_holders(
    journal: &AnchoredDirectory,
    name: &CStr,
    expected: &libc::stat,
) -> io::Result<Vec<Holder>> {
    let directory = journal.current_path()?;
    let path = directory.join(OsStr::from_bytes(name.to_bytes()));
    let require_identity = || {
        let at = journal.stat_child(name)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} vanished during holder inspection", path.display()),
            )
        })?;
        if at.st_dev == expected.st_dev
            && at.st_ino == expected.st_ino
            && at.st_mode & libc::S_IFMT == libc::S_IFDIR
            && same_directory(&fs::symlink_metadata(&path)?, &at)
        {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                format!(
                    "{} no longer names the observed released tree",
                    path.display()
                ),
            ))
        }
    };
    require_identity()?;
    let open = holders_of(&path).map_err(io::Error::other)?;
    require_identity()?;
    if journal.current_path()? != directory {
        return Err(io::Error::new(
            io::ErrorKind::ResourceBusy,
            format!(
                "{} moved while its holders were inspected",
                directory.display()
            ),
        ));
    }
    Ok(open)
}

fn same_directory(observed: &fs::Metadata, expected: &libc::stat) -> bool {
    observed.is_dir()
        && i128::from(observed.dev()) == i128::from(expected.st_dev)
        && u128::from(observed.ino()) == u128::from(expected.st_ino)
}

/// Every process holding anything inside the payload tree at `image`, confirmed by a second
/// census (see `confirmed_tree_holders`).
fn holders_of(image: &Path) -> Result<Vec<Holder>, ApfsStorageError> {
    super::confirmed_tree_holders(image)
}

fn hold_owners(
    layout: &BuildVolumeLayout,
    id: &BuildVolumeId,
) -> Result<Vec<Holder>, ApfsStorageError> {
    let hold = layout.hold_path(id);
    crate::build_volume::nx::holders(&hold).map_err(|error| {
        io_error(
            "list the processes holding a build volume's hold",
            &hold,
            error,
        )
    })
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

    /// Mint the empty build volume `id` and mount it, its owner record registered first and left
    /// registered, held by the answered hold, until the caller publishes it. A directory has no
    /// capacity; `capacity` bounds nothing here, the store's filesystem does.
    pub fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        _capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<(PathBuf, BuildVolumeHold), ApfsStorageError> {
        let image = layout.image(id);
        self.require_store_image(&image)?;
        super::ensure_parent(&image)?;
        let creating = layout.register(id, registration).map_err(host_error)?;
        let made = (|| {
            fs::create_dir(&image)
                .map_err(|error| io_error("create build volume payload", &image, error))?;
            sync_parent_path(&image)?;
            let mount = layout.mount(id);
            bind(&image, &mount)?;
            Ok(mount)
        })();
        match made {
            Ok(mount) => Ok((mount, creating)),
            Err(error) => Err(self.unregister(layout, id, creating, error)),
        }
    }

    /// Release `id`, whose registered creation failed with `primary`, joining a release that
    /// cannot run to `primary`; the registered record then stays for collection.
    fn unregister(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        creating: BuildVolumeHold,
        primary: ApfsStorageError,
    ) -> ApfsStorageError {
        drop(creating);
        let cleanup = match self.release_build_volume(layout, id) {
            Ok(BuildVolumeRelease::Deleted { .. }) => Ok(()),
            Ok(BuildVolumeRelease::Held { open }) => Err(ApfsStorageError::Host(format!(
                "build volume {id} stays registered while another hold on it lives ({} open); its holder releases it, or the next collection does",
                open.len()
            ))),
            Err(error) => Err(error),
        };
        match crate::storage::apfs::combine_cleanup::<std::convert::Infallible>(
            "release a build volume whose creation failed",
            primary,
            cleanup,
        ) {
            Err(error) => error,
            Ok(never) => match never {},
        }
    }

    /// Clone `source` to the new volume `destination`, registered and then published as
    /// `registration`. A bound source is claimed against job admission for the whole capture;
    /// the runtime caller retains Nx/Cargo guards.
    pub fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<(), ApfsStorageError> {
        let _restoring = if Self::build_volume_bound(layout, source)? {
            match self.claim_build_volume(layout, source)? {
                BuildVolumeClaim::Claimed(claim) => Some(claim),
                BuildVolumeClaim::Held { open } => {
                    return Err(ApfsStorageError::BuildVolumeHeld {
                        image: layout.image(source),
                        hold: layout.hold_path(source),
                        holds: hold_owners(layout, source)?,
                        open,
                    });
                }
            }
        } else {
            self.require_store_image(&layout.image(source))?;
            None
        };
        self.clone_build_volume_under_hold(layout, source, destination, registration)
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
        registration: &Registration,
    ) -> Result<(), ApfsStorageError> {
        self.require_store_image(&layout.image(source))?;
        self.clone_build_volume_under_hold(layout, source, destination, registration)
    }

    fn clone_build_volume_under_hold(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<(), ApfsStorageError> {
        let from = layout.image(source);
        let to = layout.image(destination);
        self.require_store_image(&to)?;
        // The source is someone's: a published volume, or a live one its owner still writes.
        let source_record = layout.read_record(source).map_err(host_error)?;
        if let BuildVolumeRole::Seed { target, .. } = &source_record.role
            && !source_record.is_published()
        {
            return Err(ApfsStorageError::Host(format!(
                "seed {source} of {target} is registered and unfinished, so nothing may be cloned from it; the next collection releases it"
            )));
        }
        // Runtime owns the named Nx/Cargo exclusions. Other contributed paths and retained
        // guard descriptors must not refuse capture. This controlled test substrate copies a
        // tree; it does not prove the point-in-time atomicity of an APFS image clone.
        let written = self.build_volume_written(layout, source)?;
        let creating = layout
            .register(destination, registration)
            .map_err(host_error)?;
        let cloned = (|| {
            self.clone_tree(&from, &to)?;
            self.mark_seed_written(layout, destination, written)?;
            layout
                .publish(destination, registration)
                .map_err(host_error)
        })();
        match cloned {
            Ok(()) => {
                drop(creating);
                Ok(())
            }
            Err(error) => Err(self.unregister(layout, destination, creating, error)),
        }
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

    /// A new live volume cloned from `seed` as `registration`, bound, without the seed's Nx
    /// daemon record (16_build_volumes.md, "Fork" steps 2 and 3).
    pub fn fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<(BuildVolumeId, PathBuf), ApfsStorageError> {
        let live = BuildVolumeId::mint();
        self.clone_build_volume(layout, seed, &live, registration)?;
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
                    refused: self.release_claimed(layout, claim, Reclaim::Now)?,
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

    /// Unbind and release the volume `claim` claimed, deleting its record, mark, hold and
    /// mountpoint, journaled in the project's deletion log. With [`Reclaim::Now`] the tree is
    /// deleted before this answers, and a process that still has anything open inside it
    /// refuses the release before anything changes: there is no forced fallback, so a held seed
    /// is never deleted from under its holder. With [`Reclaim::Journaled`] the tree is only
    /// renamed into the project's build journal, which deletes nothing, so a holder keeps what
    /// it has open and [`Self::finish_build_releases`] waits it out. A release that goes
    /// through has no refusal to report.
    pub fn release_claimed(
        &self,
        layout: &BuildVolumeLayout,
        claim: ReleaseClaim,
        reclaim: Reclaim,
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
        if present && reclaim == Reclaim::Now {
            let open = holders_of(&image)?;
            if !open.is_empty() {
                return Err(ApfsStorageError::BuildVolumeHeld {
                    hold: layout.hold_path(id),
                    image,
                    holds: hold_owners(layout, id)?,
                    open,
                });
            }
        }
        if reclaim == Reclaim::Journaled {
            // The journal is the store's own: reached through a link, nothing is unbound or moved.
            self.require_store_image(&layout.released(id))?;
        }
        // An unforced unmount is the whole release: a deletion finds no holder (just checked),
        // and a journal rename deletes nothing. An image already gone has nothing to unbind: a
        // journaled retry confirms the commit an earlier attempt made, and otherwise its record
        // alone is retired, said as such.
        let committed = if present {
            unbind(&image, &mount, cowshed_apfs::DetachIntent::Release)?;
            match reclaim {
                Reclaim::Now => remove_payload_tree(&image)?,
                Reclaim::Journaled => self.journal_payload_tree(layout, id)?,
            }
            true
        } else {
            reclaim == Reclaim::Journaled && self.confirm_journaled_tree(layout, id)?
        };
        if committed {
            layout.journal_release(id, recorded.as_ref(), true);
        }
        remove_if_present(&written_mark_path(&image))?;
        if remove_if_present(&layout.record(id))? && !committed {
            layout.journal_release(id, recorded.as_ref(), false);
        }
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

    /// Commit the release of `id`'s unbound payload tree: one rename, refusing to replace
    /// anything, from the images directory into the project's build journal, both held by
    /// descriptor and synced. The journal is created when missing; a link or a non-directory at
    /// either refuses, untouched; its caller has refused a journal reached through a link. Nothing
    /// is deleted until [`Self::finish_build_releases`].
    fn journal_payload_tree(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<(), ApfsStorageError> {
        let entry = layout.released(id);
        let images = held_images(layout)?;
        let (journal, _) =
            cowshed_os::fsio::open_or_create_child_directory(&images, &leaf(&layout.releasing())?)
                .map_err(|error| {
                    io_error("open the build release journal", &layout.releasing(), error)
                })?;
        let name = leaf(&entry)?;
        cowshed_os::fsio::rename_noreplace(images.as_raw_fd(), &name, journal.as_raw_fd(), &name)
            .map_err(|error| {
            io_error(
                "commit a build volume release to the journal",
                &entry,
                error,
            )
        })?;
        let synced = |directory: &File, path: &Path| {
            cowshed_os::fsio::sync_held_directory(directory)
                .map_err(|error| io_error("sync the build release journal", path, error))
        };
        synced(&journal, &layout.releasing())?;
        synced(&images, layout.images())
    }

    /// Whether an earlier attempt already committed `id`'s tree to the build journal, made
    /// durable before this answers: that attempt may have died, or failed a sync, between its
    /// rename and the rename's syncs, and the caller removes the owner record once this answers.
    /// A link or a non-directory at the journal or the entry refuses, untouched.
    fn confirm_journaled_tree(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<bool, ApfsStorageError> {
        let images = held_images(layout)?;
        let Some(journal) = held_journal(layout)? else {
            return Ok(false);
        };
        self.confirm_held_journaled_tree(layout, id, &images, &journal)
    }

    fn confirm_held_journaled_tree(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        images: &File,
        journal: &AnchoredDirectory,
    ) -> Result<bool, ApfsStorageError> {
        let entry = layout.released(id);
        match journal.existing_child(&leaf(&entry)?) {
            Ok(_tree) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(io_error("open a released build volume", &entry, error)),
        }
        journal.sync().map_err(|error| {
            io_error("sync the build release journal", &layout.releasing(), error)
        })?;
        cowshed_os::fsio::sync_held_directory(images)
            .map_err(|error| io_error("sync the build images directory", layout.images(), error))?;
        Ok(true)
    }

    /// Finish every release committed to the project's build journal: delete each `<id>.dmg`
    /// payload tree a [`Reclaim::Journaled`] release moved there. The journal is the store's
    /// own: one reached through a link refuses, untouched. A tree a process still holds anything
    /// inside is said and left for a later pass, never deleted from under its holder; any other
    /// name, or anything that is not a directory, is said and left, untouched.
    pub fn finish_build_releases(
        &self,
        layout: &BuildVolumeLayout,
    ) -> Result<Drained, ApfsStorageError> {
        let journal = layout.releasing();
        self.verify_controller_path(&journal)?;
        let Some(held) = held_journal(layout)? else {
            return Ok(Drained::default());
        };
        let images = held_images(layout)?;
        self.finish_held_build_releases(layout, &images, &held)
    }

    fn finish_held_build_releases(
        &self,
        layout: &BuildVolumeLayout,
        images: &File,
        held: &AnchoredDirectory,
    ) -> Result<Drained, ApfsStorageError> {
        let journal = layout.releasing();
        let mut drained = Drained::default();
        held.visit_entries(|name| {
            let entry = journal.join(OsStr::from_bytes(name.to_bytes()));
            let named = name.to_str().ok().is_some_and(crate::build_volume::is_image_name);
            let metadata = if named { held.stat_child(name)? } else { None };
            let Some(metadata) = metadata.filter(|at| at.st_mode & libc::S_IFMT == libc::S_IFDIR) else {
                cowshed_os::eprintln!(
                    "cowshed: {} is not a released build volume; it is left", entry.display()
                );
                return Ok(ControlFlow::Continue(()));
            };
            let open = held_tree_holders(held, name, &metadata)?;
            if !open.is_empty() {
                cowshed_os::eprintln!(
                    "cowshed: released build volume {} stays while {} process(es) hold it; a later release or cowshed gc deletes it",
                    entry.display(), open.len()
                );
                return Ok(ControlFlow::Continue(()));
            }
            let tree = held.existing_child(name)?;
            if !same_directory(&tree.metadata()?, &metadata) {
                return Err(io::Error::new(io::ErrorKind::ResourceBusy,
                    format!("{} changed after its holders were inspected", entry.display())));
            }
            let bytes = DirectoryRelease::prepare(images, held, name, tree)?.finish()?;
            drained.add(Drained { images: 1, bytes });
            Ok(ControlFlow::Continue(()))
        }).map_err(|error| io_error("drain the held build release journal", &journal, error))?;
        if drained.images > 0 {
            held.sync()
                .map_err(|error| io_error("sync the build release journal", &journal, error))?;
        }
        Ok(drained)
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
                &Registration::new(
                    BuildVolumeRecord::new(
                        tree,
                        BuildVolumeRole::Linked {
                            checkout: workspace.clone(),
                        },
                    ),
                    Registrant::Workspace {
                        incarnation: incarnation.clone(),
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
                        cowshed_os::eprintln!(
                            "cowshed: {workspace}'s build link named build volume {linked}, which no longer exists; {workspace} now links a fresh clone of its seed {seed}, and what {linked} held beyond that seed is lost"
                        );
                    }
                    None => {
                        link::unlink(checkout).map_err(host_error).map_err(lost)?;
                        cowshed_os::eprintln!(
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
mod journal_tests {
    use super::*;
    use crate::repository::{ProjectPaths, RepoId};
    use crate::storage::apfs::ApfsSubstrateConfig;

    fn swap_fixture() -> (PathBuf, DirectoryExecutionHost, BuildVolumeLayout) {
        let root = std::env::temp_dir().join(format!(
            "cowshed-directory-journal-swap-{}",
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
        fs::create_dir_all(layout.releasing()).unwrap();
        (root, host, layout)
    }

    #[test]
    fn a_retry_never_confirms_a_foreign_tree_after_the_journal_path_is_swapped() {
        let (root, host, layout) = swap_fixture();
        let ours = BuildVolumeId::mint();
        let theirs = BuildVolumeId::mint();
        fs::create_dir(layout.released(&ours)).unwrap();
        let foreign = root.join("foreign");
        fs::create_dir_all(foreign.join(format!("{theirs}.dmg"))).unwrap();
        let images = held_images(&layout).unwrap();
        let journal = held_journal(&layout).unwrap().unwrap();
        fs::rename(layout.releasing(), root.join("held-journal")).unwrap();
        std::os::unix::fs::symlink(&foreign, layout.releasing()).unwrap();

        assert!(
            !host
                .confirm_held_journaled_tree(&layout, &theirs, &images, &journal)
                .unwrap(),
            "the foreign journal's tree is not our durable release"
        );
        assert!(
            host.confirm_held_journaled_tree(&layout, &ours, &images, &journal)
                .unwrap(),
            "confirmation follows the held journal, not its replaced name"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_drain_never_deletes_foreign_data_after_the_journal_path_is_swapped() {
        use std::os::unix::fs::PermissionsExt;

        let (root, host, layout) = swap_fixture();
        let ours = BuildVolumeId::mint();
        let theirs = BuildVolumeId::mint();
        let image = layout.released(&ours);
        fs::create_dir_all(image.join("sealed/nested")).unwrap();
        fs::write(image.join("sealed/nested/warm"), b"superseded seed").unwrap();
        let foreign = root.join("foreign");
        let foreign_tree = foreign.join(format!("{theirs}.dmg"));
        fs::create_dir_all(&foreign_tree).unwrap();
        fs::write(foreign_tree.join("kept"), b"not the store's").unwrap();
        std::os::unix::fs::symlink(&foreign_tree, image.join("foreign-link")).unwrap();
        let bytes = [image.join("sealed/nested/warm"), image.join("foreign-link")]
            .iter()
            .map(|path| fs::symlink_metadata(path).unwrap().blocks() * 512)
            .sum();
        fs::set_permissions(image.join("sealed"), fs::Permissions::from_mode(0o555)).unwrap();
        fs::set_permissions(
            image.join("sealed/nested"),
            fs::Permissions::from_mode(0o555),
        )
        .unwrap();
        let journal = held_journal(&layout).unwrap().unwrap();
        let retained = root.join("held-journal");
        fs::rename(layout.releasing(), &retained).unwrap();
        std::os::unix::fs::symlink(&foreign, layout.releasing()).unwrap();

        let images = held_images(&layout).unwrap();
        let drained = host
            .finish_held_build_releases(&layout, &images, &journal)
            .unwrap();
        assert_eq!(
            fs::read(foreign_tree.join("kept")).unwrap(),
            b"not the store's"
        );
        assert_eq!(drained, Drained { images: 1, bytes });
        assert!(!retained.join(format!("{ours}.dmg")).exists());
        assert!(
            fs::symlink_metadata(layout.releasing())
                .unwrap()
                .is_symlink()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_unsynced_directory_rename_is_durable_before_the_drain_deletes_any_bytes() {
        let (root, _host, layout) = swap_fixture();
        let images = held_images(&layout).unwrap();
        let journal = held_journal(&layout).unwrap().unwrap();
        let id = BuildVolumeId::mint();
        let image = layout.image(&id);
        fs::create_dir(&image).unwrap();
        fs::write(image.join("warm"), b"unsynced seed").unwrap();
        let entry = layout.released(&id);
        fs::rename(&image, &entry).unwrap();
        let name = leaf(&entry).unwrap();
        let tree = journal.existing_child(&name).unwrap();
        cowshed_os::fsio::take_syncs();

        let release = DirectoryRelease::prepare(&images, &journal, &name, tree).unwrap();
        assert_eq!(
            cowshed_os::fsio::take_syncs().full,
            2,
            "the journal and source directory must sync before deletion is admitted"
        );
        assert_eq!(
            fs::read(entry.join("warm")).unwrap(),
            b"unsynced seed",
            "neither sync is deferred until after the first deletion"
        );
        release.finish().unwrap();
        assert!(!entry.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn a_drain_counts_real_self_holds_but_not_its_own_observation() {
        let (root, host, layout) = swap_fixture();
        let id = BuildVolumeId::mint();
        let entry = layout.released(&id);
        fs::create_dir(&entry).unwrap();
        fs::write(entry.join("warm"), b"held seed").unwrap();
        let held = File::open(entry.join("warm")).unwrap();
        assert_eq!(host.finish_build_releases(&layout).unwrap().images, 0);
        assert_eq!(fs::read(entry.join("warm")).unwrap(), b"held seed");
        drop(held);
        assert_eq!(host.finish_build_releases(&layout).unwrap().images, 1);
        assert!(!entry.exists());
        fs::remove_dir_all(root).unwrap();
    }

    /// The build journal is the store's own. A link planted at `build/.releasing` is never
    /// followed: a journaled release refuses with the seed, its record and the foreign tree
    /// intact, and a finisher refuses without deleting the foreign tree's children. In a real
    /// journal a name that is not a released volume's is left. A journaled retry that finds the
    /// tree its earlier attempt renamed but never synced makes the rename durable (the journal
    /// and the images directory) before the record goes, and frees none of it.
    #[test]
    fn the_build_journal_is_never_followed_through_a_link_and_a_retry_syncs_its_commit() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-directory-build-journal-{}",
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
        fs::write(image.join("warm"), b"superseded seed\n").unwrap();
        layout
            .write_record(
                &id,
                &BuildVolumeRecord::new(
                    None,
                    BuildVolumeRole::Seed {
                        target: WorkspaceName::main(),
                        incarnation: WorkspaceIncarnation::new("1".repeat(32)).unwrap(),
                    },
                ),
            )
            .unwrap();
        let foreign = root.join("foreign");
        let foreign_child = foreign.join(format!("{}.dmg", BuildVolumeId::mint()));
        fs::create_dir_all(&foreign_child).unwrap();
        fs::write(foreign_child.join("kept"), b"not the store's").unwrap();
        std::os::unix::fs::symlink(&foreign, layout.releasing()).unwrap();

        let claim = layout.claim_release(&id).unwrap().unwrap();
        assert!(
            host.release_claimed(&layout, claim, Reclaim::Journaled)
                .is_err()
        );
        assert_eq!(fs::read(image.join("warm")).unwrap(), b"superseded seed\n");
        assert!(
            layout.record(&id).exists(),
            "the record stays for the retry"
        );
        assert!(host.finish_build_releases(&layout).is_err());
        assert_eq!(
            fs::read(foreign_child.join("kept")).unwrap(),
            b"not the store's"
        );
        assert_eq!(fs::read_dir(&foreign).unwrap().count(), 1);

        fs::remove_file(layout.releasing()).unwrap();
        fs::create_dir(layout.releasing()).unwrap();
        let stray = layout.releasing().join("stray");
        fs::create_dir(&stray).unwrap();
        fs::rename(&image, layout.released(&id)).unwrap();
        cowshed_os::fsio::take_syncs();
        assert!(host.confirm_journaled_tree(&layout, &id).unwrap());
        assert_eq!(
            cowshed_os::fsio::take_syncs().full,
            2,
            "the journal and the images directory are synced before the commit is answered"
        );
        let claim = layout.claim_release(&id).unwrap().unwrap();
        assert_eq!(
            host.release_claimed(&layout, claim, Reclaim::Journaled)
                .unwrap(),
            None
        );
        assert!(!layout.record(&id).exists());
        assert_eq!(
            fs::read(layout.released(&id).join("warm")).unwrap(),
            b"superseded seed\n",
            "the retry frees none of it"
        );

        let drained = host.finish_build_releases(&layout).unwrap();
        assert_eq!(drained.images, 1);
        assert!(!layout.released(&id).exists());
        assert!(
            stray.is_dir(),
            "a name that is not a released volume's is left"
        );
        fs::remove_dir_all(&root).unwrap();
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
        assert!(layout.admit(&id).is_err());
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
                layout.admit(&id).is_err(),
                "the borrowed claim still fences admission"
            );
        }
        host.mount_build_volume(&layout, &id).unwrap();
        assert_eq!(fs::read(mount.join("warm")).unwrap(), b"old build state\n");
        host.detach_claimed_build_volume(&layout, &claim).unwrap();
        assert_eq!(
            host.release_claimed(&layout, claim, Reclaim::Now).unwrap(),
            None
        );
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
