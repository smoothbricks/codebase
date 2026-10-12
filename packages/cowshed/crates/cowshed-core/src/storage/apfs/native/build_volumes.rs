//! Build volumes on APFS (16_build_volumes.md, "Substrate"): one RAW image each, created,
//! cloned, attached and released through the same backend as workspace images, but with no
//! workspace sidecar, no marker and no lifecycle intent. A build volume nothing links is
//! garbage by construction, so an interrupted operation leaves only what garbage collection
//! removes.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{MacOsApfsExecutionHost, io_error};
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, LinkResolution,
    ReleaseClaim, link,
};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError};
use crate::storage::lifecycle::ResizeOutcome;
use cowshed_apfs::image::ImageCapacity;
use cowshed_apfs::{ApfsBackend, CommandRunner, DetachIntent, MountAccess};
use cowshed_os::process::Holder;

/// What releasing a build volume did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Release {
    /// Detached, its image, record, hold and mountpoint deleted; `refused` is the unforced
    /// unmount's refusal, when there was one.
    Deleted { refused: Option<Refusal> },
    /// A job admitted on the volume still runs, so its hold keeps the volume; nothing was
    /// changed. `open` names the processes that have the volume open now.
    Held { open: Vec<Holder> },
}

/// Whether a release may take a build volume now ([`MacOsApfsExecutionHost::claim_build_volume`]).
#[derive(Debug)]
pub enum Claim {
    /// No job holds the volume, and none can until the claim is dropped.
    Claimed(ReleaseClaim),
    /// A job admitted on the volume still runs; `open` names what has the volume open now.
    Held { open: Vec<Holder> },
}

/// The kernel refused a build volume's unforced unmount: something nobody in cowshed owns had
/// it open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Refusal {
    /// Each process that had the volume open when the unmount was refused, as the kernel
    /// answers for this user's processes: empty when no holder is visible in that listing.
    /// The kernel or another user's process may still hold the volume.
    pub open: Vec<Holder>,
    /// The grace ran out and the unmount was forced past them; otherwise they let go in time.
    pub forced: bool,
}

impl<R> MacOsApfsExecutionHost<R>
where
    R: CommandRunner + Send + Sync + 'static,
{
    /// Mint the empty build volume `id` at `capacity` and mount it, without a record: one
    /// `clonefile` of the store's blank template and one attach (01_storage.md, "Images"). The
    /// caller writes the record last, once the volume holds what it should (a migration's copied
    /// build state); until then the volume is an interrupted creation, which collection deletes
    /// unless a live build link names it. The clone carries the template's label until the
    /// checkout's supervisor names it after the checkout (16_build_volumes.md, "Substrate"):
    /// relabelling queues on Disk Arbitration, so it never runs on a provisioning path.
    pub fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<PathBuf, ApfsStorageError> {
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        Self::ensure_parent(&image)?;
        let attachment = self.mint(capacity, &image)?;
        let mount = layout.mount(id);
        if let Err(primary) = self
            .backend
            .mount(&attachment, &mount, MountAccess::ReadWrite, false)
        {
            return super::super::combine_cleanup(
                "mount build volume",
                primary.into(),
                self.backend
                    .detach(&attachment, DetachIntent::Release)
                    .and_then(|()| self.backend.delete_image(&image))
                    .map_err(Into::into),
            );
        }
        Ok(mount)
    }

    /// Clone `source`'s image to the new volume `destination`, with `record`. The clone is not
    /// attached. A mounted source's volume is flushed first; the caller guarantees nothing
    /// writes it while it is cloned (a seed has no writer, a landing volume is quiesced). The
    /// clone is held from before its image exists until its record is written, so a collection
    /// that lists it in between never releases it as an interrupted creation.
    ///
    /// The clone keeps the source's flushed post-restore clock. A mounted source is held
    /// exclusively against job admission until the seed is marked; an admitted job after that
    /// hold is released makes it behind (16_build_volumes.md, "Targets and seeds").
    pub fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        let mount = layout.mount(source);
        let mounted = self.mounted_at(&mount)?.is_some();
        let _restoring = if mounted {
            match self.claim_build_volume(layout, source)? {
                Claim::Claimed(claim) => Some(claim),
                Claim::Held { open } => {
                    return Err(ApfsStorageError::BuildVolumeHeld {
                        image: layout.image(source),
                        hold: layout.hold_path(source),
                        open,
                    });
                }
            }
        } else {
            self.verify_controller_path(&layout.image(source))?;
            None
        };
        self.clone_build_volume_under_hold(
            layout,
            source,
            destination,
            record,
            mounted.then_some(mount.as_path()),
        )
    }

    /// Quiet retains the stopped supervisor's socket lease against replacement admission and
    /// the landing's shared GC hold. Taking a second exclusive claim would conflict with that
    /// hold; upgrading it would open an admission gap (16_build_volumes.md, Land step 4).
    #[cfg(target_os = "macos")]
    pub(crate) fn clone_quiet_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        quiet: &crate::runtime::build_volumes::Quiet,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        let source = quiet.id();
        self.verify_controller_path(&layout.image(source))?;
        let mount = layout.mount(source);
        let mounted = self.mounted_at(&mount)?.is_some();
        self.clone_build_volume_under_hold(
            layout,
            source,
            destination,
            record,
            mounted.then_some(mount.as_path()),
        )
    }

    /// Clone the build volume `claim` holds to `destination`, borrowing that claim for the whole
    /// capture: the seal, the canonical restore, the freshness mark and the destination's record
    /// all run under it. A second exclusive claim on the same hold file would conflict with it,
    /// and an upgrade would reopen admission (16_build_volumes.md, "Targets and seeds"). The source
    /// is the claim's own volume, so no caller can pair a claim with another volume's id.
    #[cfg(target_os = "macos")]
    pub(crate) fn clone_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        let source = claim.id();
        self.verify_controller_path(&layout.image(source))?;
        let mount = layout.mount(source);
        let mounted = self.mounted_at(&mount)?.is_some();
        self.clone_build_volume_under_hold(
            layout,
            source,
            destination,
            record,
            mounted.then_some(mount.as_path()),
        )
    }

    fn clone_build_volume_under_hold(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
        mount: Option<&Path>,
    ) -> Result<(), ApfsStorageError> {
        let from = layout.image(source);
        let to = layout.image(destination);
        self.verify_controller_path(&to)?;
        let hold = layout.hold_path(destination);
        let _creating = layout
            .hold_new(destination)
            .map_err(|error| io_error("hold a build volume while it is cloned", &hold, error))?;
        if let Err(error) = self.capture_build_seed(&from, mount, &to) {
            // No image, so nothing will ever release this hold file.
            if !to.exists() {
                remove_if_present(&hold)?;
            }
            return Err(error);
        }
        let written = if mount.is_some() {
            self.build_volume_written(layout, source)?
        } else {
            written_at(&to)?
        };
        // The clone's own extent map, paid here rather than by its first write inside a mount.
        self.write_first(&to)?;
        self.mark_seed_written(layout, destination, written)?;
        layout
            .write_record(destination, record)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }

    /// The instant of the last write build volume `id`'s image holds: its volume flushed first
    /// when it is mounted, so a write still in the kernel's cache counts.
    pub fn build_volume_written(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<SystemTime, ApfsStorageError> {
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        let mount = layout.mount(id);
        let mounted = self.mounted_at(&mount)?.is_some();
        self.backend
            .sync_for_freshness(&image, mounted.then_some(mount.as_path()))?;
        written_at(&image)
    }

    /// Record that seed `seed` holds every relevant write its target's live volume had at
    /// `written`. The canonical restore is flushed under the admission hold before that mark;
    /// a fork's mount and dropped daemon record likewise do not add reusable build state.
    /// Admitted jobs can write only after the claim or Quiet's stopped socket lease is released.
    pub fn mark_seed_written(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        written: SystemTime,
    ) -> Result<(), ApfsStorageError> {
        let image = layout.image(seed);
        self.verify_controller_path(&image)?;
        set_written(&image, written)
    }

    /// A new live volume cloned from `seed` with `record`, mounted, without the seed's Nx daemon
    /// record (16_build_volumes.md, "Fork" steps 2 and 3): a daemon record names another
    /// checkout's process. The caller points a checkout's link at the answered mountpoint.
    pub fn fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(BuildVolumeId, PathBuf), ApfsStorageError> {
        let live = BuildVolumeId::mint();
        self.clone_build_volume(layout, seed, &live, record)?;
        let mount = self.mount_build_volume(layout, &live)?;
        let state = crate::build_volume::BuildVolumeState::read(&mount)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        crate::build_volume::nx::discard_daemon_records(&mount, &state)
            .map_err(|error| io_error("discard the seed's Nx daemon record", &mount, error))?;
        Ok((live, mount))
    }

    /// Mount build volume `id` at its mountpoint, attaching it first when the kernel holds no
    /// attachment of it. Idempotent: a volume already mounted there is left as it is.
    pub fn mount_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<PathBuf, ApfsStorageError> {
        let image = layout.image(id);
        let mount = layout.mount(id);
        self.verify_controller_path(&image)?;
        let attachment = match self.backend.existing_attachment_for_mount(&image)? {
            Some(attachment) => attachment,
            None => self.backend.attach_verified(&image)?,
        };
        for mounted in self.mount_source.mounts()? {
            if mounted.source_device != attachment.volume_device() {
                continue;
            }
            if MountPoint::new(&mount).names(&mounted.mount_point) {
                return Ok(mount);
            }
            return Err(ApfsStorageError::Host(format!(
                "build volume {id} is mounted at {}, not at {}",
                mounted.mount_point.display(),
                mount.display()
            )));
        }
        self.backend
            .mount(&attachment, &mount, MountAccess::ReadWrite, false)?;
        Ok(mount)
    }

    /// The kernel mount at `mount`, if a volume is mounted exactly there.
    pub(super) fn mounted_at(&self, mount: &Path) -> Result<Option<String>, ApfsStorageError> {
        let mount = MountPoint::new(mount);
        Ok(self
            .mount_source
            .mounts()?
            .into_iter()
            .find(|mounted| mount.names(&mounted.mount_point))
            .map(|mounted| mounted.source_device))
    }

    /// Unmount and detach build volume `id`, then delete its image, record, hold and mountpoint
    /// (16_build_volumes.md, "Garbage collection"). Only cowshed's own claims keep a volume: a job
    /// admitted on it holds it ([`BuildVolumeLayout::hold`]), which answers [`Release::Held`] and
    /// touches nothing. Anything else that has it open does not own it: when the kernel refuses
    /// the unforced unmount, the holders are named, given the detach grace to let go, and then
    /// forced, so a volume no checkout links never outlives the operation that unlinked it.
    pub fn release_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<Release, ApfsStorageError> {
        match self.claim_build_volume(layout, id)? {
            Claim::Claimed(claim) => Ok(Release::Deleted {
                refused: self.release_claimed(layout, claim)?,
            }),
            Claim::Held { open } => Ok(Release::Held { open }),
        }
    }

    /// Claim build volume `id` for its release, or answer what holds it. A caller that decided
    /// the volume is garbage from what it read before the claim reads again under it: nothing
    /// that holds the volume (a job admitted on it, a land moving a link onto it) can still be
    /// changing it once the claim is taken.
    pub fn claim_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<Claim, ApfsStorageError> {
        self.verify_controller_path(&layout.image(id))?;
        let hold = layout.hold_path(id);
        match layout
            .claim_release(id)
            .map_err(|error| io_error("claim a build volume for release", &hold, error))?
        {
            Some(claim) => Ok(Claim::Claimed(claim)),
            None => {
                let mount = layout.mount(id);
                let open = match self.mounted_at(&mount)? {
                    Some(_) => holders_of(&mount)?,
                    None => Vec::new(),
                };
                Ok(Claim::Held { open })
            }
        }
    }

    /// [`Self::release_build_volume`] of the volume `claim` claimed. Answers the unforced
    /// unmount's refusal, when there was one. The claim is dropped once the image and its hold
    /// file are gone, so a hold taken after that finds no image. The image's deletion is
    /// journaled in the project's deletion log, with whose its sidecar said the volume was.
    pub fn release_claimed(
        &self,
        layout: &BuildVolumeLayout,
        claim: ReleaseClaim,
    ) -> Result<Option<Refusal>, ApfsStorageError> {
        let id = claim.id();
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        let mount = layout.mount(id);
        let present = image.exists();
        // Read before anything is deleted: once the volume is gone, nothing else says whose it
        // was. Evidence only, like the log it goes to, so an unreadable sidecar names nobody.
        let recorded = layout
            .read_record_present(id)
            .ok()
            .flatten()
            .map(|record| record.role);
        let mut refused = None;
        // A minted volume is formatted before anything attaches it, so an attachment of a build
        // image is an APFS one; anything else is refused, not released.
        if present && let Some(attachment) = self.backend.existing_attachment(&image)? {
            let mounted = self
                .mount_source
                .mounts()?
                .into_iter()
                .any(|mounted| mounted.source_device == attachment.volume_device());
            if mounted {
                match self
                    .backend
                    .unmount_verified(&attachment, DetachIntent::WhenIdle)
                {
                    Ok(()) => {}
                    Err(error) if cowshed_apfs::detach_was_dissented(&error) => {
                        let open = holders_of(&mount)?;
                        let unmounted = self.backend.unmount_within_grace(&attachment)?;
                        refused = Some(Refusal {
                            open,
                            forced: unmounted == cowshed_apfs::Unmounted::Forced,
                        });
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            self.backend.detach(&attachment, DetachIntent::Release)?;
        }
        self.backend.delete_image(&image)?;
        if present {
            journal_release(layout, &image, recorded.as_ref());
        }
        remove_if_present(&layout.record(id))?;
        remove_if_present(&layout.hold_path(id))?;
        match fs::remove_dir(&mount) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(io_error("remove build volume mountpoint", &mount, error));
            }
        }
        drop(claim);
        Ok(refused)
    }

    /// Mount what `workspace`'s checkout links, whose build link names `linked`, once the link
    /// is settled by [`BuildVolumeLayout::resolve_link`] (16_build_volumes.md, "One link per
    /// checkout"). A link to a volume that no longer exists is said on stderr and replaced: by a
    /// fresh clone of `workspace`'s latest seed at `incarnation`, or, when it has none, by no
    /// link, so the next build-state refresh makes its first volume.
    pub fn settle_build_link(
        &self,
        layout: &BuildVolumeLayout,
        workspace: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
        checkout: &Path,
        linked: &BuildVolumeId,
    ) -> Result<(), ApfsStorageError> {
        let host = |error: crate::CowshedError| ApfsStorageError::Host(error.to_string());
        let refork = |seed: &BuildVolumeId| {
            let tree = layout.read_record(seed).map_err(host)?.tree;
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
            link::point(checkout, &mount).map_err(host)
        };
        match layout.resolve_link(workspace, linked).map_err(host)? {
            LinkResolution::Keep => self.mount_build_volume(layout, linked).map(|_| ()),
            LinkResolution::Repoint(own) => {
                let mount = self.mount_build_volume(layout, &own)?;
                link::point(checkout, &mount).map_err(host)
            }
            LinkResolution::Refork(seed) => refork(&seed),
            LinkResolution::Vanished => {
                let lost = |error: ApfsStorageError| {
                    ApfsStorageError::Host(format!(
                        "{workspace}'s build link names build volume {linked}, which no longer exists, and settling the link failed: {error}"
                    ))
                };
                match layout.seed_of(workspace, incarnation).map_err(host)? {
                    Some((seed, _)) => {
                        refork(&seed).map_err(lost)?;
                        eprintln!(
                            "cowshed: {workspace}'s build link named build volume {linked}, which no longer exists; {workspace} now links a fresh clone of its seed {seed}, and what {linked} held beyond that seed is lost"
                        );
                    }
                    None => {
                        link::unlink(checkout).map_err(host).map_err(lost)?;
                        eprintln!(
                            "cowshed: {workspace}'s build link named build volume {linked}, which no longer exists, and {workspace} has no seed to clone; the link is removed, and {workspace}'s next build-state refresh makes its first volume"
                        );
                    }
                }
                Ok(())
            }
        }
    }

    /// The capacity build volume `id`'s image holds, attached or not.
    pub fn build_volume_capacity(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<ImageCapacity, ApfsStorageError> {
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        Ok(self.backend.image_capacity(&image)?)
    }

    /// Grow build volume `id`'s image to `capacity`: detached without force, grown, attached,
    /// its container grown, and mounted again. A volume in use refuses before anything changes.
    /// Answers the capacity it had and the one the kernel now reports.
    pub fn resize_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError> {
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        let previous = self.backend.image_capacity(&image)?;
        if capacity <= previous {
            return Err(ApfsStorageError::CapacityNotGrowing {
                requested: capacity,
                current: previous,
            });
        }
        let was_mounted = self.mounted_at(&layout.mount(id))?.is_some();
        if let Some(attachment) = self.backend.existing_attachment(&image)? {
            if was_mounted {
                self.backend
                    .unmount_verified(&attachment, DetachIntent::WhenIdle)?;
            }
            self.backend.detach(&attachment, DetachIntent::WhenIdle)?;
        }
        self.backend.resize_image(&image, capacity)?;
        let attachment = self.backend.attach_verified(&image)?;
        self.backend.grow_container(&attachment)?;
        let observed = self.backend.attached_capacity(&image)?;
        if was_mounted {
            self.backend.mount(
                &attachment,
                &layout.mount(id),
                MountAccess::ReadWrite,
                false,
            )?;
        } else {
            self.backend.detach(&attachment, DetachIntent::Release)?;
        }
        if observed < capacity {
            return Err(ApfsStorageError::ResizeNotObserved {
                requested: capacity,
                observed,
            });
        }
        Ok(ResizeOutcome {
            previous,
            capacity: observed,
        })
    }
}

/// Every process that has the volume mounted at `mount` open, as the kernel answers for this
/// user's processes; nothing when nothing is mounted there.
fn holders_of(mount: &Path) -> Result<Vec<Holder>, ApfsStorageError> {
    match crate::build_volume::nx::volume_holders(mount) {
        Ok(holders) => Ok(holders),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(io_error(
            "list the processes holding a build volume",
            mount,
            error,
        )),
    }
}

pub(super) fn written_at(image: &Path) -> Result<SystemTime, ApfsStorageError> {
    fs::metadata(image)
        .and_then(|metadata| metadata.modified())
        .map_err(|error| {
            io_error(
                "read a build volume image's modification time",
                image,
                error,
            )
        })
}

/// Set `image`'s modification time without writing it: opening for write changes nothing.
fn set_written(image: &Path, written: SystemTime) -> Result<(), ApfsStorageError> {
    fs::OpenOptions::new()
        .write(true)
        .open(image)
        .and_then(|file| file.set_modified(written))
        .map_err(|error| io_error("set a build volume image's modification time", image, error))
}

fn remove_if_present(path: &Path) -> Result<(), ApfsStorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("remove build volume record", path, error)),
    }
}

/// The deletion log's line for a released build volume's image: a seed's or another volume's,
/// naming the workspace its sidecar `recorded` (the checkout that linked it, the target whose
/// seed it was), or nobody for one recorded unlinked or with no readable sidecar.
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

/// A path asked about, against the kernel's mount table. The kernel records a mount point as
/// the path it resolved at mount time, so only the asked-for spelling needs resolving, once.
/// Resolving each mounted filesystem's path instead `statfs`es every volume on the host, and
/// one volume whose I/O is stalled -- a fresh clone whose extent map the store is still
/// committing -- held a fork's `new build-volume` for 14-17 s.
struct MountPoint<'a> {
    given: &'a Path,
    /// `None` when the path does not resolve: no directory, so nothing is mounted there.
    canonical: Option<PathBuf>,
}

impl<'a> MountPoint<'a> {
    fn new(given: &'a Path) -> Self {
        Self {
            given,
            canonical: fs::canonicalize(given).ok(),
        }
    }

    fn names(&self, mounted: &Path) -> bool {
        mounted == self.given || self.canonical.as_deref() == Some(mounted)
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use crate::api::resources::JobAccounting;
    use crate::build_volume::BuildVolumeRole;
    use crate::metadata::WorkspaceName;
    use crate::repository::{ProjectPaths, RepoId};
    use crate::storage::apfs::ApfsSubstrateConfig;
    use cowshed_apfs::{
        AttachedDiskImage, CommandOutput, CommandRequest, CommandRunError, SystemCommandRunner,
    };
    use cowshed_os::fork_lock::Fenced;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant, SystemTime};

    fn fixture(
        root: &Path,
    ) -> (
        MacOsApfsExecutionHost<SystemCommandRunner>,
        BuildVolumeLayout,
    ) {
        fixture_with(root, SystemCommandRunner)
    }

    fn fixture_with<R: CommandRunner>(
        root: &Path,
        runner: R,
    ) -> (MacOsApfsExecutionHost<R>, BuildVolumeLayout) {
        let store = root.join("store");
        fs::create_dir_all(&store).unwrap();
        let project = ProjectPaths::with_mount_root(
            &store,
            root.join("mnt"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let host = MacOsApfsExecutionHost::new(
            runner,
            ApfsSubstrateConfig::new(&store, root.join("checkout")),
        )
        .unwrap();
        (host, BuildVolumeLayout::new(&project).unwrap())
    }

    struct RestoreAdmissionRunner {
        layout: BuildVolumeLayout,
        source: BuildVolumeId,
        socket: Option<(tokio::runtime::Handle, PathBuf)>,
        armed: AtomicBool,
        denied: AtomicBool,
    }

    impl CommandRunner for RestoreAdmissionRunner {
        fn run_guarded<E>(
            &self,
            request: &CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<CommandOutput>, E>
        where
            E: From<CommandRunError> + std::fmt::Display,
        {
            SystemCommandRunner.run_guarded(request, check)
        }
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            let output = SystemCommandRunner.run(request)?;
            if request.program == Path::new("/sbin/mount_apfs")
                && output.succeeded()
                && request
                    .args
                    .last()
                    .is_some_and(|mount| Path::new(mount) == self.layout.mount(&self.source))
                && self.armed.swap(false, Ordering::SeqCst)
            {
                if let Some((runtime, socket)) = &self.socket {
                    let refused = runtime
                        .block_on(crate::runtime::supervisor_socket::bind(socket))
                        .expect_err("replacement admission must stay closed through the seed mark");
                    assert_eq!(refused.code, crate::error::ErrorCode::Conflict);
                } else {
                    let refused = self.layout.hold(&self.source).expect_err(
                        "no job may be admitted after canonical restore and before the seed mark",
                    );
                    assert_eq!(refused.kind(), io::ErrorKind::ResourceBusy);
                }
                self.denied.store(true, Ordering::SeqCst);
            }
            Ok(output)
        }

        fn image_lease(&self, identity: &Path) -> io::Result<Option<Fenced<fs::File>>> {
            SystemCommandRunner.image_lease(identity)
        }

        fn pin_raw_device(&self, device: &Path) -> io::Result<Option<Fenced<fs::File>>> {
            SystemCommandRunner.pin_raw_device(device)
        }

        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            SystemCommandRunner.attached_disk_images()
        }

        fn grow_image(&self, image: &Path, capacity: ImageCapacity) -> io::Result<()> {
            SystemCommandRunner.grow_image(image, capacity)
        }
    }

    #[test]
    fn real_apfs_build_seed_restore_is_fresh_until_an_admitted_job_writes() {
        let root = crate::scratch_apfs::ScratchRoot::new("seed-admission-clock").unwrap();
        let source = BuildVolumeId::mint();
        let (_, layout) = fixture(root.path());
        let runner = RestoreAdmissionRunner {
            layout: layout.clone(),
            source: source.clone(),
            socket: None,
            armed: AtomicBool::new(false),
            denied: AtomicBool::new(false),
        };
        let (host, layout) = fixture_with(root.path(), runner);
        crate::blank_image::blank_image(&layout.image(&source));
        let mount = host.mount_build_volume(&layout, &source).unwrap();
        layout.write_record(&source, &linked("main")).unwrap();
        fs::write(mount.join("warm"), b"captured build state\n").unwrap();
        let before = snapshot(&mount);
        let owner = fs::read(layout.record(&source)).unwrap();
        let record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Seed {
                target: WorkspaceName::main(),
                incarnation: WorkspaceIncarnation::new("a".repeat(32)).unwrap(),
            },
        );
        host.backend.runner().armed.store(true, Ordering::SeqCst);
        let seed = BuildVolumeId::mint();
        host.clone_build_volume(&layout, &source, &seed, &record)
            .unwrap();
        assert!(host.backend.runner().denied.load(Ordering::SeqCst));
        let live = host.build_volume_written(&layout, &source).unwrap();
        let seeded = host.build_volume_written(&layout, &seed).unwrap();
        eprintln!("canonical restore seed clock={seeded:?}, flushed live={live:?}");
        assert!(
            live <= seeded,
            "own restore must leave a Fresh seed without tolerance"
        );
        assert_eq!(snapshot(&mount), before);
        assert_eq!(fs::read(layout.record(&source)).unwrap(), owner);

        let admitted = layout
            .hold(&source)
            .expect("job admission after restore hold release");
        let refused_seed = BuildVolumeId::mint();
        assert!(matches!(
            host.clone_build_volume(&layout, &source, &refused_seed, &record),
            Err(ApfsStorageError::BuildVolumeHeld { .. })
        ));
        assert!(!layout.image(&refused_seed).exists());
        assert!(!layout.record(&refused_seed).exists());
        assert!(!layout.hold_path(&refused_seed).exists());
        assert!(
            layout.image(&seed).exists(),
            "busy capture retains the original seed"
        );
        assert_eq!(fs::read(layout.record(&source)).unwrap(), owner);
        job(
            &mount,
            "printf 'written by admitted job\\n' > after-admission",
        );
        fs::File::open(mount.join("after-admission"))
            .unwrap()
            .sync_all()
            .unwrap();
        drop(admitted);
        assert_eq!(
            fs::read(mount.join("after-admission")).unwrap(),
            b"written by admitted job\n"
        );
        let live = host.build_volume_written(&layout, &source).unwrap();
        let seeded = host.build_volume_written(&layout, &seed).unwrap();
        eprintln!("admitted job seed clock={seeded:?}, flushed live={live:?}");
        assert!(
            live > seeded,
            "the admitted job's write must make the seed behind"
        );
        let inspection = BuildVolumeId::mint();
        host.clone_build_volume(&layout, &seed, &inspection, &linked("inspection"))
            .unwrap();
        let captured = host.mount_build_volume(&layout, &inspection).unwrap();
        assert!(!captured.join("after-admission").exists());
        assert_eq!(snapshot(&captured), before);
        assert_eq!(fs::read(layout.record(&source)).unwrap(), owner);
        for id in [&inspection, &seed, &source] {
            assert_eq!(
                host.release_build_volume(&layout, id).unwrap(),
                Release::Deleted { refused: None }
            );
        }
    }

    #[tokio::test]
    async fn real_apfs_quiet_seed_restore_refuses_admission_until_the_mark() {
        use crate::api::dto::{ExecCommand, ExecRequest, ExitStatus, RunSandboxMode, StdinSource};
        use crate::runtime::build_volumes::BuildVolumes;
        use crate::runtime::supervisor::{
            CommitmentPublisher, WorkspaceSupervisor, WorkspaceSupervisorConfig,
        };
        use crate::runtime::supervisor_socket;
        use std::sync::Arc;

        let root = crate::scratch_apfs::ScratchRoot::new("seed-quiet-admission").unwrap();
        let source = BuildVolumeId::mint();
        let (system, layout) = fixture(root.path());
        let checkout = root.path().join("checkout");
        fs::create_dir_all(checkout.join(".cowshed")).unwrap();
        crate::blank_image::blank_image(&layout.image(&source));
        let mount = system.mount_build_volume(&layout, &source).unwrap();
        crate::build_volume::BuildVolumeState::default()
            .write(&mount)
            .unwrap();
        layout.write_record(&source, &linked("main")).unwrap();
        crate::build_volume::link::point(&checkout, &mount).unwrap();
        fs::write(mount.join("warm"), b"captured build state\n").unwrap();
        let before = snapshot(&mount);
        let owner = fs::read(layout.record(&source)).unwrap();
        let volumes = BuildVolumes::new(Arc::new(system), layout.clone());
        let socket = supervisor_socket::socket_path(
            &root.path().join("store"),
            &RepoId::parse("acme/widget").unwrap(),
            &WorkspaceName::main(),
        );
        let stopped = supervisor_socket::bind(&socket).await.unwrap();
        let quiet = volumes
            .quiesce(
                volumes.hold_landing(&checkout).unwrap().unwrap(),
                stopped,
                checkout.clone(),
            )
            .await
            .unwrap()
            .expect("the stopped workspace has no writers");
        let runner = RestoreAdmissionRunner {
            layout: layout.clone(),
            source: source.clone(),
            socket: Some((tokio::runtime::Handle::current(), socket.clone())),
            armed: AtomicBool::new(true),
            denied: AtomicBool::new(false),
        };
        let (host, _) = fixture_with(root.path(), runner);
        let host = Arc::new(host);
        let seed = BuildVolumeId::mint();
        let record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Seed {
                target: WorkspaceName::main(),
                incarnation: WorkspaceIncarnation::new("a".repeat(32)).unwrap(),
            },
        );
        let capture = {
            let host = Arc::clone(&host);
            let layout = layout.clone();
            let quiet = quiet.clone();
            let seed = seed.clone();
            tokio::task::spawn_blocking(move || {
                host.clone_quiet_build_volume(&layout, &quiet, &seed, &record)
            })
        };
        capture.await.unwrap().unwrap();
        assert!(host.backend.runner().denied.load(Ordering::SeqCst));
        let live = host.build_volume_written(&layout, &source).unwrap();
        let seeded = host.build_volume_written(&layout, &seed).unwrap();
        eprintln!("quiet restore seed clock={seeded:?}, flushed live={live:?}");
        assert!(
            live <= seeded,
            "Quiet's restore must leave Fresh without tolerance"
        );
        assert_eq!(snapshot(&mount), before);
        assert_eq!(fs::read(layout.record(&source)).unwrap(), owner);
        assert_eq!(
            supervisor_socket::bind(&socket).await.unwrap_err().code,
            crate::error::ErrorCode::Conflict
        );
        let landing_hold = quiet.release_admission();
        let replacement = supervisor_socket::bind(&socket)
            .await
            .expect("admission reopens after the mark");
        let defaults = WorkspaceSupervisorConfig::default();
        let workspace = crate::storage::lifecycle::LifecycleWorkspace::new(
            defaults.authority.repo_id.clone(),
            defaults.authority.workspace.clone(),
            defaults.authority.workspace_incarnation.clone(),
            crate::storage::lifecycle::Revision::new(defaults.authority.lifecycle_revision),
            crate::storage::lifecycle::Revision::new(defaults.authority.grant_revision),
            crate::metadata::WorkspaceRole::Main,
        )
        .unwrap();
        crate::workspace_credentials::mint_workspace_credentials(
            &workspace,
            &checkout,
            crate::metadata::Platform::Macos,
            Some(defaults.sandbox.port_block),
            &root.path().join("ca.key"),
        )
        .expect("mint the replacement supervisor's real workspace credentials");
        let publisher = CommitmentPublisher::open(
            root.path().join("telemetry"),
            crate::storage::audit::ContinuityAudit::Arrow,
            16,
        )
        .unwrap();
        let handle = WorkspaceSupervisor::start(
            WorkspaceSupervisorConfig {
                workspace_root: checkout.clone(),
                default_cwd: None,
                sandbox: crate::sandbox::SandboxConfig {
                    home: root.path().join("home"),
                    mount_root: root.path().join("mnt"),
                    workspace_mount: checkout,
                    exec_temp_dir: root.path().join("exec"),
                    build_volume_mount: Some(mount.clone()),
                    ..defaults.sandbox
                },
                build_volume_layout: Some(layout.clone()),
                ..defaults
            },
            publisher,
        )
        .unwrap();
        let job = handle
            .exec(
                None,
                Some(mount.clone()),
                ExecRequest {
                    command: ExecCommand::Argv(vec![
                        "/bin/sh".into(),
                        "-c".into(),
                        format!(
                            "printf 'written by admitted job\\n' > {}/after-admission",
                            mount.display()
                        )
                        .into(),
                    ]),
                    cwd: None,
                    mode: RunSandboxMode::ReadWrite,
                    env: std::collections::HashMap::new(),
                    trace: None,
                    stdin: StdinSource::Empty,
                    stdout_copy: None,
                    stderr_copy: None,
                    admission_key: None,
                },
            )
            .await
            .expect("the replacement supervisor admits the shell after the mark");
        let terminal = handle.wait(job).await.unwrap();
        assert_eq!(terminal.exit, Some(ExitStatus::Exited { code: 0 }));
        assert_eq!(
            fs::read(mount.join("after-admission")).unwrap(),
            b"written by admitted job\n"
        );
        let live = host.build_volume_written(&layout, &source).unwrap();
        let seeded = host.build_volume_written(&layout, &seed).unwrap();
        eprintln!("replacement admitted job seed clock={seeded:?}, flushed live={live:?}");
        assert!(
            live > seeded,
            "an admitted job after Quiet releases must make the seed behind"
        );
        handle.retire().await.unwrap();
        drop(handle);
        drop(replacement);
        drop(landing_hold);
        let inspection = BuildVolumeId::mint();
        host.clone_build_volume(&layout, &seed, &inspection, &linked("inspection"))
            .unwrap();
        let captured = host.mount_build_volume(&layout, &inspection).unwrap();
        assert!(!captured.join("after-admission").exists());
        assert_eq!(snapshot(&captured), before);
        assert_eq!(fs::read(layout.record(&source)).unwrap(), owner);
        for id in [&inspection, &seed, &source] {
            assert_eq!(
                host.release_build_volume(&layout, id).unwrap(),
                Release::Deleted { refused: None }
            );
        }
    }

    fn linked(name: &str) -> BuildVolumeRecord {
        BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Linked {
                checkout: WorkspaceName::new(name).unwrap(),
            },
        )
    }

    /// A checkout mounted from an image of its own, with its own build volume recorded, linked and
    /// reached through a `target` build-state link: what a job of the checkout runs on.
    struct Lane {
        attachment: cowshed_apfs::AttachedImage,
        checkout: PathBuf,
        build_id: BuildVolumeId,
        /// The volume `WorkspaceRef::build_volume` names for the checkout.
        build_mount: PathBuf,
    }

    impl Lane {
        fn mount(
            root: &Path,
            host: &MacOsApfsExecutionHost<SystemCommandRunner>,
            layout: &BuildVolumeLayout,
        ) -> Self {
            use crate::build_volume::link;
            use crate::capabilities::BuildStatePath;

            let image = root.join("store/lane.dmg");
            crate::blank_image::blank_image(&image);
            let attachment = host
                .backend
                .attach_verified(&image)
                .expect("attach the workspace");
            let checkout = root.join("lane");
            fs::create_dir_all(&checkout).unwrap();
            host.backend
                .mount(&attachment, &checkout, MountAccess::ReadWrite, false)
                .expect("mount the workspace");
            fs::create_dir_all(checkout.join(".cowshed")).unwrap();

            let build_id = BuildVolumeId::mint();
            crate::blank_image::blank_image(&layout.image(&build_id));
            let build_mount = host
                .mount_build_volume(layout, &build_id)
                .expect("mount the build volume");
            layout.write_record(&build_id, &linked("lane")).unwrap();
            link::point(&checkout, &build_mount).unwrap();
            link::link_paths(
                &checkout,
                &build_mount,
                &[BuildStatePath::new("target", "target").unwrap()],
            )
            .unwrap();
            let named = layout
                .grant(&WorkspaceName::new("lane").unwrap(), &checkout)
                .unwrap()
                .expect("the workspace links a build volume");
            assert_eq!(named, build_mount);
            Self {
                attachment,
                checkout,
                build_id,
                build_mount,
            }
        }
    }

    /// Every regular file and directory under `root` with its bytes and modification time.
    fn snapshot(root: &Path) -> Vec<(PathBuf, Option<Vec<u8>>, SystemTime)> {
        let mut entries = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory).unwrap() {
                let path = entry.unwrap().path();
                let metadata = fs::symlink_metadata(&path).unwrap();
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                if relative.starts_with(".fseventsd")
                    || relative.starts_with(".Spotlight-V100")
                    || relative == Path::new(cowshed_apfs::SPOTLIGHT_OPT_OUT)
                {
                    continue;
                }
                if metadata.is_dir() {
                    pending.push(path.clone());
                    entries.push((relative, None, metadata.modified().unwrap()));
                } else {
                    entries.push((
                        relative,
                        Some(fs::read(&path).unwrap()),
                        metadata.modified().unwrap(),
                    ));
                }
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }

    /// A build volume's clone holds every byte and every modification time of its source (rule
    /// "Clones preserve mtimes"), in milliseconds; a volume a job holds refuses release and keeps
    /// everything; one a process merely has a file open in is released past it, the process
    /// named; an idle one is deleted with its record, hold and mountpoint.
    #[test]
    fn real_apfs_build_volumes_clone_with_mtimes_and_release_unless_a_job_holds_them() {
        let root = crate::scratch_apfs::ScratchRoot::new("build-volume").expect("scratch root");
        let (host, layout) = fixture(root.path());
        let source = BuildVolumeId::mint();
        // Creation is proven by first touch (build_volume::migrate) and adoption; this test's
        // source is a clone of the run's blank image, mounted the way a fork's volume is.
        crate::blank_image::blank_image(&layout.image(&source));
        let mount = host
            .mount_build_volume(&layout, &source)
            .expect("mount a build volume");
        layout.write_record(&source, &linked("main")).unwrap();
        assert_eq!(mount, layout.mount(&source));
        assert_eq!(
            host.mount_build_volume(&layout, &source).unwrap(),
            mount,
            "idempotent"
        );
        fs::create_dir_all(mount.join("target/debug/deps")).unwrap();
        let old = SystemTime::now() - Duration::from_secs(86_400);
        for (index, name) in ["a.rlib", "b.rlib", "c.d"].iter().enumerate() {
            let path = mount.join("target/debug/deps").join(name);
            fs::write(&path, name.repeat(1000)).unwrap();
            fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(old + Duration::from_secs(index as u64))
                .unwrap();
        }
        let before = snapshot(&mount);

        let clone = BuildVolumeId::mint();
        let started = Instant::now();
        host.clone_build_volume(&layout, &source, &clone, &linked("topic"))
            .expect("clone the build volume");
        let cloned = started.elapsed();
        let started = Instant::now();
        let clone_mount = host
            .mount_build_volume(&layout, &clone)
            .expect("mount the clone");
        let mounted = started.elapsed();
        eprintln!("build volume clone {cloned:?}, attach+mount {mounted:?}");
        assert_eq!(
            snapshot(&clone_mount),
            before,
            "bytes and mtimes survive the clone"
        );
        assert_eq!(
            layout.read_record(&clone).unwrap().role,
            linked("topic").role
        );

        let hold = layout.hold(&clone).expect("a job's hold");
        let open = fs::File::open(clone_mount.join("target/debug/deps/a.rlib")).unwrap();
        let me = i32::try_from(std::process::id()).unwrap();
        match host.release_build_volume(&layout, &clone).unwrap() {
            Release::Held { open } => assert!(
                open.iter().any(|holder| holder.pid == me),
                "the refusal names what has the volume open: {open:?}"
            ),
            released @ Release::Deleted { .. } => {
                panic!("a volume a job holds was released: {released:?}")
            }
        }
        assert!(layout.image(&clone).exists() && layout.record(&clone).exists());
        assert_eq!(
            snapshot(&clone_mount),
            before,
            "a refused release changes nothing"
        );
        assert_eq!(
            layout.claim_release(&clone).unwrap().map(|_| ()),
            None,
            "the hold keeps every release out"
        );
        drop(hold);
        match host.release_build_volume(&layout, &clone).unwrap() {
            Release::Deleted {
                refused: Some(Refusal { open, forced }),
            } => {
                assert!(forced, "the file stayed open through the grace");
                assert!(
                    open.iter().any(|holder| holder.pid == me),
                    "the refusal names the process the force cut off: {open:?}"
                );
            }
            other => panic!("an open file refuses the unforced unmount: {other:?}"),
        }
        drop(open);
        assert!(!layout.image(&clone).exists());
        assert!(!layout.record(&clone).exists());
        assert!(!layout.hold_path(&clone).exists());
        assert!(!layout.mount(&clone).exists());
        assert_eq!(
            host.release_build_volume(&layout, &source).unwrap(),
            Release::Deleted { refused: None }
        );
        assert_eq!(layout.list().unwrap(), []);
    }

    const MIB: i64 = 1 << 20;
    /// What a volume may move by beside a job's file data: the file's inode and extent records,
    /// and what fseventsd journals on the volume meanwhile. Measured at 4 KiB for a 64 MiB write;
    /// a container-wide reading moves by the sibling volume's whole 32 MiB, far outside it.
    const METADATA: i64 = 256 << 10;

    /// Run `script` under `/bin/sh` in `cwd`, as a job's command runs, and require it to succeed.
    fn job(cwd: &Path, script: &str) {
        use cowshed_os::fork_lock::Run as _;
        let status = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(script)
            .current_dir(cwd)
            .status_locked()
            .expect("spawn the job");
        assert!(status.success(), "{script}: {status:?}");
    }

    /// A command writing `mebibytes` of incompressible bytes to `path`, fsynced before it exits.
    fn write(path: &str, mebibytes: u32) -> String {
        format!("/bin/dd if=/dev/urandom of={path} bs=1048576 count={mebibytes} conv=fsync")
    }

    #[track_caller]
    fn near(observed: i64, expected: i64, what: &str) {
        assert!(
            (observed - expected).abs() <= METADATA,
            "{what}: {observed} bytes, expected {expected} within {METADATA}"
        );
    }

    fn diskutil(args: &[&str]) -> String {
        let output = SystemCommandRunner
            .run(&CommandRequest::new(cowshed_apfs::device::DISKUTIL, args))
            .expect("run diskutil");
        assert!(
            output.succeeded(),
            "diskutil {args:?}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("diskutil answers UTF-8")
    }

    /// Add an unmounted APFS volume named `name` to `container`, answering its device. A scratch
    /// root's release takes it with the container's other volumes, on a panic too.
    fn add_volume(container: &str, name: &str) -> String {
        diskutil(&["apfs", "addVolume", container, "APFS", name, "-nomount"])
            .lines()
            .find_map(|line| line.strip_prefix("Created new APFS Volume "))
            .expect("diskutil names the volume it added")
            .trim()
            .to_owned()
    }

    /// The APFS volume stat a job's samples read: a job writes 64 MiB into its workspace and 32
    /// MiB through a build-state link into the volume the workspace's build link names, then
    /// deletes a 16 MiB file that predates it, then its own 64 MiB. Each sample is an owned
    /// volume's own used bytes against the spawn baseline: the deletion drops the workspace
    /// delta by 16 MiB and the last one takes it below zero. A write to a sibling volume in the
    /// workspace's own container moves neither delta, which a container-wide reading would by the
    /// sibling's whole 32 MiB. A mountpoint its volume has left is a typed failure, never the
    /// parent volume's usage.
    fn prove_owned_volume_usage(root: &Path, lane: &Lane) {
        use crate::api::resources::VolumeUsage;
        use crate::runtime::volume_usage::{
            VolumeAtSpawn, VolumeBaseline, VolumeStat as _, VolumeStatError,
        };
        use crate::storage::apfs::native::{ApfsVolume, ApfsVolumeStat};

        let (attachment, checkout, named) = (&lane.attachment, &lane.checkout, &lane.build_mount);

        // A sibling volume in the workspace's own container, nothing to do with the job.
        let container = attachment
            .volume_device()
            .rsplit_once('s')
            .map(|(container, _)| container)
            .expect("an APFS volume device is <container>s<n>");
        let added = add_volume(container, "sibling");
        let sibling = root.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        diskutil(&["mount", "-mountPoint", sibling.to_str().unwrap(), &added]);

        job(checkout, &write("predates.bin", 16));
        let captured = |mount: &Path, what: &str| {
            VolumeAtSpawn::observed(Ok(ApfsVolume::capture(mount).expect(what)))
        };
        let baseline = VolumeBaseline {
            workspace: captured(checkout, "the workspace volume"),
            build: Some(captured(named, "the build volume")),
        };
        let delta = |usage: Option<VolumeUsage>| match usage {
            Some(VolumeUsage::Read { delta_bytes }) => delta_bytes.get(),
            other => panic!("each owned volume is read: {other:?}"),
        };
        let sample = || {
            let sample = baseline.sample(&ApfsVolumeStat);
            (delta(Some(sample.workspace)), delta(sample.build))
        };

        job(
            checkout,
            &format!(
                "{} && {}",
                write("job.bin", 64),
                write("target/job.bin", 32)
            ),
        );
        let (workspace, build) = sample();
        near(workspace, 64 * MIB, "workspace after the job's writes");
        near(build, 32 * MIB, "build volume after the job's writes");

        job(&sibling, &write("unrelated.bin", 32));
        let (unmoved, unmoved_build) = sample();
        near(
            unmoved,
            workspace,
            "workspace after a sibling volume's write",
        );
        near(
            unmoved_build,
            build,
            "build volume after a sibling volume's write",
        );

        job(checkout, "rm predates.bin");
        let (pruned, pruned_build) = sample();
        near(
            workspace - pruned,
            16 * MIB,
            "workspace drop for the file that predates the job",
        );
        near(
            pruned_build,
            build,
            "build volume after a workspace deletion",
        );

        job(checkout, "rm job.bin");
        let (emptied, _) = sample();
        assert!(
            emptied < 0,
            "the workspace is below its spawn usage: {emptied}"
        );
        near(
            emptied,
            -16 * MIB,
            "workspace after the job removes its own file",
        );

        // A mountpoint whose volume has gone answers for its parent volume: refused, not read.
        // Deleting the added volume unmounts it first; a separate unmount storagekitd may dissent.
        let (left, _) = ApfsVolume::capture(&sibling).expect("the sibling volume");
        diskutil(&["apfs", "deleteVolume", &added]);
        assert!(matches!(
            ApfsVolumeStat.used_bytes(&left),
            Err(VolumeStatError::Read(_))
        ));
        assert!(
            matches!(ApfsVolume::capture(&sibling), Err(VolumeStatError::Read(_))),
            "a directory on its parent's volume is no volume"
        );
    }

    /// A release takes an image whose container holds a volume beside the image's own, as a test
    /// that adds one leaves it when it dies: each mounted volume, the added one too, is unmounted
    /// through its own pinned identity, and the whole disk is detached once, leaving no
    /// attachment and no mount. (A 1 GiB test container holds at most two volumes.) What attaches,
    /// verifies or looks up one attachment still refuses the image, attached before the release
    /// and attached afresh after.
    fn prove_multi_volume_release(
        root: &Path,
        host: &MacOsApfsExecutionHost<SystemCommandRunner>,
        attachment: &cowshed_apfs::AttachedImage,
    ) {
        use cowshed_apfs::ApfsError;
        let one_volume_refusal = |error: &ApfsError| {
            matches!(error,
                ApfsError::InvalidAttachmentInventory(message)
                | ApfsError::InvalidAttachmentPlist(message)
                    if message.starts_with("expected one APFS volume, reported [")
                        && message.matches("/dev/disk").count() == 2)
        };

        let image = root.join("store/lane.dmg");
        let container = cowshed_apfs::device::container_of(
            attachment
                .volume_device()
                .strip_prefix("/dev/")
                .expect("canonical volume device"),
        )
        .expect("APFS volume names its synthesized container");
        let sibling = add_volume(container, "sibling");
        let sibling_mount = root.join("sibling");
        fs::create_dir_all(&sibling_mount).unwrap();
        diskutil(&[
            "mount",
            "-mountPoint",
            sibling_mount.to_str().unwrap(),
            &sibling,
        ]);

        let looked_up = host.backend.recovered_image_attachment(&image).err();
        assert!(
            looked_up.as_ref().is_some_and(one_volume_refusal),
            "{looked_up:?}"
        );
        let unmounted = host
            .backend
            .unmount_verified(attachment, DetachIntent::WhenIdle);
        assert!(
            unmounted.as_ref().is_err_and(one_volume_refusal),
            "{unmounted:?}"
        );

        host.detach_existing_image(&image, DetachIntent::Release)
            .expect("release every volume of the image");
        let held = host.backend.attached_whole_devices(&image).unwrap();
        assert!(held.is_empty(), "the image is still attached as {held:?}");
        let scratch = fs::canonicalize(root).unwrap();
        let left: Vec<_> = host
            .mount_source
            .mounts()
            .unwrap()
            .into_iter()
            .filter(|mount| mount.mount_point.starts_with(&scratch))
            .collect();
        assert_eq!(left, [], "the released volumes are still mounted");

        let attached = host.backend.attach_verified(&image);
        assert!(
            attached.as_ref().is_err_and(one_volume_refusal),
            "{attached:?}"
        );
        let held = host.backend.attached_whole_devices(&image).unwrap();
        assert!(held.is_empty(), "the refused attach left {held:?}");
    }

    /// A process group of its own the test leads, standing in for a job's activation or command:
    /// the supervisor samples it like any job group.
    fn lone_group() -> std::process::Child {
        use cowshed_os::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;
        std::process::Command::new("/bin/sleep")
            .arg("300")
            .stdin(std::process::Stdio::null())
            .process_group(0)
            .spawn_locked()
            .expect("a test-owned process group")
    }

    /// End the group `leader` leads and wait for the leader's exit without reaping it: like a
    /// job's parent, the test holds it until nothing reads its rusage any more ([`reap`]).
    fn end_group(leader: &std::process::Child) {
        let pgid = i32::try_from(leader.id()).unwrap();
        // SAFETY: the unreaped test child leads this group.
        assert_eq!(unsafe { libc::killpg(pgid, libc::SIGKILL) }, 0);
        // SAFETY: an all-zero siginfo is a valid value of the plain C struct.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: waiting for our own child without reaping it.
        let waited = unsafe {
            libc::waitid(
                libc::P_PID,
                leader.id(),
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        assert_eq!(waited, 0, "waitid: {}", std::io::Error::last_os_error());
    }

    /// Reap a leader once nothing reads it any more: its interval closed, or its job concluded.
    fn reap(mut leader: std::process::Child) {
        leader.wait().unwrap();
    }

    /// The job's volumes, each read: `(workspace, build)` deltas.
    fn read_volumes(sample: &crate::api::resources::JobResourceSample) -> (i64, i64) {
        use crate::api::resources::VolumeUsage;
        let delta = |usage: Option<&VolumeUsage>| match usage {
            Some(VolumeUsage::Read { delta_bytes }) => delta_bytes.get(),
            other => panic!("each of the job's volumes is read: {other:?}"),
        };
        (
            delta(Some(&sample.volumes.workspace)),
            delta(sample.volumes.build.as_ref()),
        )
    }

    /// One pair of real volumes proves owned-volume stats, admission-through-terminal job
    /// accounting, and whole-image release after an extra sibling volume is added. The phases
    /// share their workspace/build attachments; each sample takes its own baseline.
    #[tokio::test]
    async fn real_apfs_owned_volume_usage_and_job_accounting_through_multi_volume_release() {
        use crate::api::dto::{ExecCommand, ExecRequest, ExitStatus, JobId, StdinSource};
        use crate::runtime::job_groups::Birth;
        use crate::runtime::supervisor::{
            ArtifactSink as _, ArtifactStoreSink, CommitmentDraft, CommitmentSink, OwnedProcess,
            ProcessEvent, ProcessSignal, ProcessSpawnRequest, RunningProcess, SpawnSink,
            WorkspaceSupervisor, WorkspaceSupervisorConfig, WorkspaceSupervisorHandle,
        };
        use crate::runtime::volume_usage::VolumeMountpoint;
        use crate::storage::job_artifact::StreamKind;
        use tokio::sync::mpsc;

        /// The job's first process is an activation that writes into the workspace before its
        /// spawn returns, as a warm host may start work before the supervisor hears of it; its
        /// processes are reported through the events the test is handed.
        struct Activation {
            checkout: PathBuf,
            dispatched: mpsc::UnboundedSender<mpsc::Sender<ProcessEvent>>,
        }

        #[async_trait::async_trait]
        impl SpawnSink for Activation {
            async fn spawn(
                &mut self,
                _request: ProcessSpawnRequest,
                events: mpsc::Sender<ProcessEvent>,
            ) -> crate::error::Result<Box<dyn RunningProcess>> {
                job(&self.checkout, &write("activation.bin", 8));
                self.dispatched
                    .send(events)
                    .expect("the test awaits the job");
                Ok(Box::new(Reported))
            }
        }

        /// A job whose processes arrive as events.
        struct Reported;

        impl RunningProcess for Reported {
            fn process(&self) -> Option<&OwnedProcess> {
                None
            }

            fn observations(&self) -> Option<&crate::runtime::process_monitor::ProcessMonitor> {
                None
            }

            fn try_write_stdin(&mut self, _bytes: bytes::Bytes) -> crate::error::Result<bool> {
                Ok(true)
            }

            fn close_stdin(&mut self) -> bool {
                true
            }

            fn end_stdin(&mut self) {}

            fn signal_process_tree(&mut self, _signal: ProcessSignal) -> crate::error::Result<()> {
                Ok(())
            }
        }

        struct Commitments;

        #[async_trait::async_trait]
        impl CommitmentSink for Commitments {
            async fn record(&mut self, _draft: CommitmentDraft) -> crate::error::Result<()> {
                Ok(())
            }
        }

        /// Deliver `event`, then one stdout byte, and return once the supervisor served that byte
        /// at `offset`: it handles a job's events in order, so `event` has been handled too.
        async fn deliver(
            handle: &WorkspaceSupervisorHandle,
            events: &mpsc::Sender<ProcessEvent>,
            job_id: JobId,
            event: ProcessEvent,
            offset: u64,
        ) {
            events.send(event).await.unwrap();
            events
                .send(ProcessEvent::Output {
                    job_id,
                    stream: StreamKind::Stdout,
                    bytes: bytes::Bytes::from_static(b"."),
                })
                .await
                .unwrap();
            let chunk = handle
                .log_read(job_id, StreamKind::Stdout, offset, true)
                .await
                .unwrap();
            assert_eq!(chunk.bytes.as_ref(), b".");
        }

        let owned = |leader: &std::process::Child| OwnedProcess {
            birth: Birth::of(leader.id()),
            spawned: Instant::now(),
            host: crate::host_load::read_host_load(),
        };

        let root = crate::scratch_apfs::ScratchRoot::new("job-volumes").expect("scratch root");
        let (host, layout) = fixture(root.path());
        let lane = Lane::mount(root.path(), &host, &layout);
        prove_owned_volume_usage(root.path(), &lane);
        let defaults = WorkspaceSupervisorConfig::default();
        let config = WorkspaceSupervisorConfig {
            workspace_root: lane.checkout.clone(),
            default_cwd: None,
            sandbox: crate::sandbox::SandboxConfig {
                // The host mount root the build volume is mounted under.
                mount_root: root.path().join("mnt"),
                workspace_mount: lane.checkout.clone(),
                build_volume_mount: Some(lane.build_mount.clone()),
                ..defaults.sandbox
            },
            build_volume_layout: Some(layout.clone()),
            workspace_volume: Some(
                VolumeMountpoint::new(lane.checkout.clone()).expect("the checkout is a volume"),
            ),
            ..defaults
        };
        let store = || {
            ArtifactStoreSink::open(
                config.workspace_root.clone(),
                &config.owned_repo_ids,
                &config.authority,
                config.artifacts.clone(),
            )
            .expect("open the artifact store")
        };
        let (dispatched, mut dispatches) = mpsc::unbounded_channel();
        let handle = WorkspaceSupervisor::start_with_sinks(
            config.clone(),
            Box::new(Activation {
                checkout: lane.checkout.clone(),
                dispatched,
            }),
            Box::new(store()),
            Box::new(Commitments),
        )
        .expect("start the supervisor");

        let job_id = handle
            .exec(
                None,
                Some(lane.build_mount.clone()),
                ExecRequest {
                    command: ExecCommand::Argv(vec!["true".into()]),
                    cwd: None,
                    mode: crate::api::dto::RunSandboxMode::ReadWrite,
                    env: std::collections::HashMap::new(),
                    trace: None,
                    stdin: StdinSource::Empty,
                    stdout_copy: None,
                    stderr_copy: None,
                    admission_key: None,
                },
            )
            .await
            .expect("admit the job");
        let events = dispatches.recv().await.expect("the job was dispatched");

        let activation = lone_group();
        deliver(
            &handle,
            &events,
            job_id,
            ProcessEvent::Activating {
                job_id,
                process: owned(&activation),
            },
            0,
        )
        .await;
        let (workspace, build) = read_volumes(&handle.resources(job_id).await.unwrap());
        near(
            workspace,
            8 * MIB,
            "the activation's write, made before its spawn returned",
        );
        near(build, 0, "the build volume before anything wrote to it");

        job(
            &lane.checkout,
            &format!(
                "{} && {}",
                write("command.bin", 16),
                write("target/command.bin", 4)
            ),
        );
        // The activation ended: its parent read what it cost while it still held it, and only
        // then does the command take the lead (runtime::job_accounting).
        events
            .send(ProcessEvent::ActivationEnded {
                job_id,
                usage: crate::runtime::job_accounting::read_leader(&Birth::of(activation.id())),
            })
            .await
            .unwrap();
        let command = lone_group();
        deliver(
            &handle,
            &events,
            job_id,
            ProcessEvent::Started {
                job_id,
                process: owned(&command),
            },
            1,
        )
        .await;
        end_group(&activation);
        reap(activation);
        let running = handle.resources(job_id).await.unwrap();
        assert_eq!(
            running.leader_pid,
            command.id(),
            "the command leads the job"
        );
        let (workspace, build) = read_volumes(&running);
        near(
            workspace,
            24 * MIB,
            "the workspace since admission, across both processes",
        );
        near(build, 4 * MIB, "the build volume since admission");

        end_group(&command);
        events
            .send(ProcessEvent::Exited {
                job_id,
                exit: ExitStatus::Exited { code: 0 },
            })
            .await
            .unwrap();
        for stream in [StreamKind::Stdout, StreamKind::Stderr] {
            events
                .send(ProcessEvent::OutputEof { job_id, stream })
                .await
                .unwrap();
        }
        let terminal = handle
            .wait(job_id)
            .await
            .unwrap()
            .resources
            .expect("a terminal sample");
        let (workspace, build) = read_volumes(&terminal);
        near(workspace, 24 * MIB, "the terminal sample's workspace");
        near(build, 4 * MIB, "the terminal sample's build volume");
        assert!(
            matches!(
                terminal.accounting,
                JobAccounting::MacOsRusageChildren { .. }
            ),
            "the job's CPU totals ride the same terminal sample as its volumes"
        );
        assert!(
            terminal.consistent(),
            "the terminal sample agrees with its own CPU totals"
        );
        assert_eq!(
            handle.sealed(job_id).await.unwrap().resources.as_ref(),
            Some(&terminal),
            "the sealed sample is the terminal one"
        );
        handle.quiesce().await.unwrap();
        handle.retire().await.unwrap();
        drop(handle);
        assert_eq!(
            store().sealed(job_id).and_then(|sealed| sealed.resources),
            Some(terminal),
            "the stored record decodes to the terminal sample, its volumes and accounting with it"
        );

        reap(command);
        assert_eq!(
            host.release_build_volume(&layout, &lane.build_id).unwrap(),
            Release::Deleted { refused: None }
        );
        prove_multi_volume_release(root.path(), &host, &lane.attachment);
    }
}
