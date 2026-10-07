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
        if let Err(error) =
            self.backend
                .sync_and_clone(&from, mounted.then_some(mount.as_path()), &to)
        {
            // No image, so nothing will ever release this hold file.
            if !to.exists() {
                remove_if_present(&hold)?;
            }
            return Err(error.into());
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
    fn mounted_at(&self, mount: &Path) -> Result<Option<String>, ApfsStorageError> {
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
}
