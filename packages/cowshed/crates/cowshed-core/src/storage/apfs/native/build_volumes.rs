//! Build volumes on APFS (16_build_volumes.md, "Substrate"): one ASIF image each, created,
//! cloned, attached and released through the same backend as workspace images, but with no
//! workspace sidecar, no marker and no lifecycle intent. A build volume nothing links is
//! garbage by construction, so an interrupted operation leaves only what garbage collection
//! removes.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{MacOsApfsExecutionHost, io_error};
use crate::apfs::{ApfsBackend, CommandRunner, DetachIntent, MountAccess};
use crate::build_volume::nx::Holder;
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, LinkResolution,
    ReleaseClaim, link,
};
use crate::metadata::{ImageCapacity, WorkspaceIncarnation, WorkspaceName};
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError};
use crate::storage::lifecycle::ResizeOutcome;

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
    /// The clone keeps the modification time the source's image had when it was cloned, across
    /// the first write below: an image's mtime is the instant of the last write it holds, so a
    /// seed is behind its live volume exactly when the live image's mtime is the later one
    /// (16_build_volumes.md, "Targets and seeds").
    pub fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        record: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        let from = layout.image(source);
        let to = layout.image(destination);
        self.verify_controller_path(&from)?;
        self.verify_controller_path(&to)?;
        let mount = layout.mount(source);
        let mounted = self.mounted_at(&mount)?.is_some();
        let hold = layout.hold_path(destination);
        let _creating = layout
            .hold_new(destination)
            .map_err(|error| io_error("hold a build volume while it is cloned", &hold, error))?;
        if let Err(error) = self.capture_image(&from, mounted.then_some(mount.as_path()), &to, None)
        {
            // No image, so nothing will ever release this hold file.
            if !to.exists() {
                remove_if_present(&hold)?;
            }
            return Err(error);
        }
        let written = written_at(&to)?;
        // The clone's own extent map, paid here rather than by its first write inside a mount.
        self.write_first(&to)?;
        set_written(&to, written)?;
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

    /// Record that seed `seed` holds every write its target's live volume had at `written`: a
    /// fork's own seed is a second clone of the seed its live volume came from, and holds
    /// everything that volume does but the fork's mount and its dropped daemon record.
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
        let attachment = match self.backend.existing_attachment(&image)? {
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
                    Err(error) if crate::apfs::detach_was_dissented(&error) => {
                        let open = holders_of(&mount)?;
                        let unmounted = self.backend.unmount_within_grace(&attachment)?;
                        refused = Some(Refusal {
                            open,
                            forced: unmounted == crate::apfs::Unmounted::Forced,
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

fn written_at(image: &Path) -> Result<SystemTime, ApfsStorageError> {
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
    use crate::apfs::SystemCommandRunner;
    use crate::build_volume::BuildVolumeRole;
    use crate::metadata::WorkspaceName;
    use crate::repository::{ProjectPaths, RepoId};
    use crate::storage::apfs::ApfsSubstrateConfig;
    use std::time::{Duration, Instant, SystemTime};

    fn fixture(
        root: &Path,
    ) -> (
        MacOsApfsExecutionHost<SystemCommandRunner>,
        BuildVolumeLayout,
    ) {
        let store = root.join("store");
        fs::create_dir_all(&store).unwrap();
        let project = ProjectPaths::with_mount_root(
            &store,
            root.join("mnt"),
            &RepoId::parse("acme/widget").unwrap(),
        )
        .unwrap();
        let host = MacOsApfsExecutionHost::new(
            SystemCommandRunner,
            ApfsSubstrateConfig::new(&store, root.join("checkout")),
        )
        .unwrap();
        (host, BuildVolumeLayout::new(&project).unwrap())
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
        attachment: crate::apfs::AttachedImage,
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

            let image = root.join("store/lane.asif");
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

        fn release(
            self,
            host: &MacOsApfsExecutionHost<SystemCommandRunner>,
            layout: &BuildVolumeLayout,
        ) {
            host.backend
                .unmount_verified(&self.attachment, DetachIntent::WhenIdle)
                .expect("unmount the workspace");
            host.backend
                .detach(&self.attachment, DetachIntent::Release)
                .expect("detach the workspace");
            assert_eq!(
                host.release_build_volume(layout, &self.build_id).unwrap(),
                Release::Deleted { refused: None }
            );
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
                if relative.starts_with(".fseventsd") || relative.starts_with(".Spotlight-V100") {
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
        use crate::fork_lock::Run as _;
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
        use crate::fork_lock::Run as _;
        let output = std::process::Command::new("/usr/sbin/diskutil")
            .args(args)
            .output_locked()
            .expect("run diskutil");
        assert!(
            output.status.success(),
            "diskutil {args:?}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("diskutil answers UTF-8")
    }

    /// A volume the test added to a scratch image's container. The scratch sweep releases only
    /// one-volume images, so it is deleted before the scratch root drops, on a panic too.
    struct AddedVolume(Option<String>);

    impl AddedVolume {
        fn device(&self) -> &str {
            self.0.as_deref().expect("the volume is not deleted yet")
        }

        fn delete(mut self) {
            let device = self.0.take().expect("the volume is not deleted yet");
            diskutil(&["apfs", "deleteVolume", &device]);
        }
    }

    impl Drop for AddedVolume {
        fn drop(&mut self) {
            if let Some(device) = self.0.take() {
                use crate::fork_lock::Run as _;
                let deleted = std::process::Command::new("/usr/sbin/diskutil")
                    .args(["apfs", "deleteVolume", &device])
                    .output_locked();
                eprintln!("deleting the added volume {device} after a failure: {deleted:?}");
            }
        }
    }

    /// The APFS volume stat a job's samples read: a job writes 64 MiB into its workspace and 32
    /// MiB through a build-state link into the volume the workspace's build link names, then
    /// deletes a 16 MiB file that predates it, then its own 64 MiB. Each sample is an owned
    /// volume's own used bytes against the spawn baseline: the deletion drops the workspace
    /// delta by 16 MiB and the last one takes it below zero. A write to a sibling volume in the
    /// workspace's own container moves neither delta, which a container-wide reading would by the
    /// sibling's whole 32 MiB. A mountpoint its volume has left is a typed failure, never the
    /// parent volume's usage.
    #[test]
    fn real_apfs_volume_usage_reads_the_owned_volumes_and_nothing_else() {
        use crate::api::resources::VolumeUsage;
        use crate::runtime::volume_usage::{
            VolumeAtSpawn, VolumeBaseline, VolumeStat as _, VolumeStatError,
        };
        use crate::storage::apfs::native::{ApfsVolume, ApfsVolumeStat};

        let root = crate::scratch_apfs::ScratchRoot::new("volume-usage").expect("scratch root");
        let (host, layout) = fixture(root.path());
        let lane = Lane::mount(root.path(), &host, &layout);
        let (attachment, checkout, named) = (&lane.attachment, &lane.checkout, &lane.build_mount);

        // A sibling volume in the workspace's own container, nothing to do with the job.
        let container = attachment
            .volume_device()
            .rsplit_once('s')
            .map(|(container, _)| container)
            .expect("an APFS volume device is <container>s<n>");
        let added = diskutil(&[
            "apfs",
            "addVolume",
            container,
            "APFS",
            "sibling",
            "-nomount",
        ]);
        let added = AddedVolume(Some(
            added
                .lines()
                .find_map(|line| line.strip_prefix("Created new APFS Volume "))
                .expect("diskutil names the volume it added")
                .trim()
                .to_owned(),
        ));
        let sibling = root.path().join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        diskutil(&[
            "mount",
            "-mountPoint",
            sibling.to_str().unwrap(),
            added.device(),
        ]);

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
        added.delete();
        assert!(matches!(
            ApfsVolumeStat.used_bytes(&left),
            Err(VolumeStatError::Read(_))
        ));
        assert!(
            matches!(ApfsVolume::capture(&sibling), Err(VolumeStatError::Read(_))),
            "a directory on its parent's volume is no volume"
        );

        lane.release(&host, &layout);
    }

    /// A process group of its own the test leads, standing in for a job's activation or command:
    /// the supervisor samples it like any job group.
    fn lone_group() -> std::process::Child {
        use crate::fork_lock::Spawn as _;
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

    /// A job's volumes through the supervisor that runs it, on real APFS volumes. The baseline is
    /// read at admission, before the job's first process is dispatched: the activation writes 8
    /// MiB into the workspace before its spawn even returns, and the first sample counts it. The
    /// command that takes the lead keeps that baseline, so its 16 MiB in the workspace and 4 MiB
    /// through the build-state link add to it. The terminal sample is sealed with each volume's
    /// usage, and a store opened afresh reads the same sample back.
    #[tokio::test]
    async fn a_jobs_volumes_count_from_admission_through_its_command_to_its_sealed_record() {
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
            terminal.accounting.is_some(),
            "the job's CPU totals ride the same terminal sample as its volumes"
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
        lane.release(&host, &layout);
    }
}
