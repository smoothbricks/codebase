//! Build volumes on APFS (16_build_volumes.md, "Substrate"): one RAW image each, created,
//! cloned, attached and released through the same backend as workspace images, but with no
//! workspace sidecar and no marker. Every operation that writes a build image takes a
//! [`Registration`] and registers the volume's owner record before the first byte of the image
//! exists, so an interrupted operation leaves a record naming its owner and registrant, which
//! collection finds by its exact id, never an image nobody owns.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{Image, MacOsApfsExecutionHost, io_error};
use crate::build_volume::{
    BuildVolumeHold, BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole,
    CapturedBuildVolume, LinkResolution, Registrant, Registration, ReleaseClaim, link,
};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::storage::apfs::ApfsStorageError;
use crate::storage::lifecycle::ResizeOutcome;
use cowshed_apfs::image::ImageCapacity;
use cowshed_apfs::{ApfsBackend, CommandRunner, DetachIntent, MountAccess, mount_is_in_progress};
use cowshed_os::process::Holder;
use std::ops::ControlFlow;

use crate::storage::apfs::extents::{self, CaughtUp, Copied};
use crate::storage::apfs::release::{Released, STEP_EXTENTS, release_file};
use crate::storage::lifecycle::ExtentCount;

/// The compaction's contiguous copy, until it is published or released. Each seed it copies or
/// catches up to is cloned beside it as `<seed>.dmg` ([`CompactionSource`]).
const COMPACTION_COPY: &str = "copy.dmg";

/// A seed cloned aside for its compaction (16_build_volumes.md, "Seed compaction"), as
/// `build/.compacting/<seed>.dmg`. The clone shares the seed's blocks and is never written, so a
/// land or reseed that retires the seed meanwhile changes nothing the copy reads, and a catch-up
/// reads from its map which blocks a newer seed still shares with it.
#[derive(Debug)]
pub struct CompactionSource {
    seed: BuildVolumeId,
    clone: PathBuf,
    written: SystemTime,
}

impl CompactionSource {
    /// The seed this is a clone of.
    pub fn seed(&self) -> &BuildVolumeId {
        &self.seed
    }
}

/// A seed's data copied into fresh, contiguous space, not yet published, with the clone of the
/// seed whose bytes it holds.
#[derive(Debug)]
pub struct CompactedSeed {
    source: CompactionSource,
    copy: PathBuf,
    /// The first copied seed's extents.
    pub before: ExtentCount,
    /// The copy's extents.
    pub after: ExtentCount,
    /// The data bytes written into the copy, its catch-ups' included.
    pub bytes: u64,
}

impl CompactedSeed {
    /// The seed whose bytes this holds.
    pub fn seed(&self) -> &BuildVolumeId {
        &self.source.seed
    }
}

/// What a seed compaction's copy did.
#[derive(Debug)]
pub enum Compaction {
    Copied(CompactedSeed),
    /// Stopped once `bytes` were copied, because `why`; the copy is removed.
    Stopped {
        bytes: u64,
        why: String,
    },
}

/// What catching a compaction's copy up to the seed that replaced its own did.
#[derive(Debug)]
pub enum CatchUp {
    /// The copy holds the newer seed's bytes: `written` data bytes were written where that seed
    /// does not share the older one's blocks, and `punched` bytes punched where it has a hole.
    Caught {
        compacted: CompactedSeed,
        written: u64,
        punched: u64,
    },
    /// Stopped once `bytes` were written into or punched from the copy in all, because `why`.
    Stopped { bytes: u64, why: String },
}

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

/// What a build volume capture's telemetry reads of its source and destination images
/// (13_telemetry.md, "Native image observations").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Observed {
    /// Both extent maps and allocations: every capture outside a fork's source lock.
    Walked,
    /// Their allocation alone: a fork's capture runs under its source's image lock, and a walk
    /// of a seed's map, 880,000 extents on a long-used main, would hold that lock seconds more.
    /// The clone's map equals the seed's, which its own capture walked.
    UnderSourceLock,
}
impl<R> MacOsApfsExecutionHost<R>
where
    R: CommandRunner + Send + Sync + 'static,
{
    /// Mint the empty build volume `id` at `capacity` and mount it: `registration` is written
    /// first, then one `clonefile` of the store's blank template and one attach (01a_apfs.md,
    /// "Images"). The record stays registered, and the answered hold keeps every release off the
    /// volume, until the caller publishes it once it holds what it should (a migration's linked
    /// build state, main's first touch). A mint that fails releases what it registered. The
    /// clone carries the template's label until the checkout's supervisor names it after the
    /// checkout (16_build_volumes.md, "Substrate"): the label is cosmetic, so naming never runs
    /// on a provisioning path.
    pub fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<(PathBuf, BuildVolumeHold), ApfsStorageError> {
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        Self::ensure_parent(&image)?;
        let creating = layout.register(id, registration).map_err(host_error)?;
        let mount = layout.mount(id);
        if let Err(error) = self.mint(capacity, &image, &mount) {
            return Err(self.unregister(layout, id, creating, error));
        }
        Ok((mount, creating))
    }

    /// [`Self::create_build_volume`] under the caller's exclusive claim on `claim.id()`
    /// ([`BuildVolumeLayout::register_claimed`]): a re-adoption mints its lost build volume under
    /// the claim it holds on that id. A mint that fails leaves the registered record for the
    /// replay that owns it, which converges on it by its exact id.
    pub fn create_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<PathBuf, ApfsStorageError> {
        let id = claim.id();
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        Self::ensure_parent(&image)?;
        layout
            .register_claimed(claim, registration)
            .map_err(host_error)?;
        let mount = layout.mount(id);
        self.mint(capacity, &image, &mount)?;
        Ok(mount)
    }

    /// Clone `source`'s image to the new volume `destination`, registered and then published as
    /// `registration`. The clone is not attached. A mounted source is flushed and captured live
    /// under the runtime caller's retained Nx/Cargo guards; other contributed paths are exempt.
    /// Immutable sources are captured detached. The destination's owner record is registered
    /// before its image exists and held until it is published, so collection never releases it
    /// in between, and a crash leaves the registered record naming its owner.
    ///
    /// The seed's write mark comes from the captured clone, before anything writes it.
    /// A mounted source is claimed ([`Self::claim_capture_source`]) and held exclusively against
    /// job admission until publication; a later source write makes the seed behind
    /// (16_build_volumes.md, "Targets and seeds").
    pub fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<(), ApfsStorageError> {
        if self.mounted_at(&layout.mount(source))?.is_some() {
            let claim = self.claim_capture_source(layout, source)?;
            return self.clone_claimed_build_volume(layout, &claim, destination, registration);
        }
        self.verify_controller_path(&layout.image(source))?;
        self.clone_build_volume_under_hold(layout, source, destination, registration, None)
    }

    /// Claim build volume `source` exclusively for a seed capture (16_build_volumes.md, "Targets
    /// and seeds", reseed step 1): from here no job is admitted on it until the claim is dropped.
    /// A job already admitted is [`ApfsStorageError::BuildVolumeHeld`], naming each process
    /// that holds the hold file (an admitted job's supervisor) and each that has the volume open.
    pub fn claim_capture_source(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
    ) -> Result<ReleaseClaim, ApfsStorageError> {
        match self.claim_build_volume(layout, source)? {
            Claim::Claimed(claim) => Ok(claim),
            Claim::Held { open } => {
                // The failed claim's own descriptor is closed, so this process is named only
                // for a hold of its own.
                let hold = layout.hold_path(source);
                let holds = crate::build_volume::nx::holders(&hold).map_err(|error| {
                    io_error(
                        "list the processes holding a build volume's hold",
                        &hold,
                        error,
                    )
                })?;
                Err(ApfsStorageError::BuildVolumeHeld {
                    image: layout.image(source),
                    hold,
                    holds,
                    open,
                })
            }
        }
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
        registration: &Registration,
    ) -> Result<(), ApfsStorageError> {
        let source = quiet.id();
        self.verify_controller_path(&layout.image(source))?;
        let mount = layout.mount(source);
        let mounted = self.mounted_at(&mount)?.is_some();
        self.clone_build_volume_under_hold(
            layout,
            source,
            destination,
            registration,
            mounted.then_some(mount.as_path()),
        )
    }

    /// Clone the build volume `claim` holds to `destination`, borrowing that claim through
    /// capture, the immutable freshness mark and the destination's record. A second exclusive
    /// claim on the same hold file would conflict with it, and an upgrade would reopen admission
    /// (16_build_volumes.md, "Targets and seeds"). The runtime caller retains Nx/Cargo guards;
    /// the source is the claim's own volume, so a claim cannot be paired with another volume.
    pub fn clone_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<(), ApfsStorageError> {
        let source = claim.id();
        self.verify_controller_path(&layout.image(source))?;
        let mount = layout.mount(source);
        let mounted = self.mounted_at(&mount)?.is_some();
        self.clone_build_volume_under_hold(
            layout,
            source,
            destination,
            registration,
            mounted.then_some(mount.as_path()),
        )
    }

    /// Register `destination`, capture `source` into it, fix its freshness mark and publish it:
    /// [`Self::capture_build_volume_under_hold`], then [`Self::finish_captured_build_volume`].
    fn clone_build_volume_under_hold(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
        mount: Option<&Path>,
    ) -> Result<(), ApfsStorageError> {
        let captured = self.capture_build_volume_under_hold(
            layout,
            source,
            destination,
            registration,
            mount,
            Observed::Walked,
        )?;
        self.finish_captured_build_volume(layout, captured)
    }

    /// Register `destination` and capture `source` into it, nothing more: the step a fork takes
    /// under its source's image lock. A seed source must be published. `observed` says whether
    /// the capture's telemetry walks both images' extent maps or reads their allocation alone.
    /// A failure releases what was registered.
    fn capture_build_volume_under_hold(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
        mount: Option<&Path>,
        observed: Observed,
    ) -> Result<CapturedBuildVolume, ApfsStorageError> {
        let from = layout.image(source);
        let to = layout.image(destination);
        self.verify_controller_path(&to)?;
        let source_record = layout.read_record(source).map_err(host_error)?;
        if let BuildVolumeRole::Seed { target, .. } = &source_record.role
            && !source_record.is_published()
        {
            return Err(ApfsStorageError::Host(format!(
                "seed {source} of {target} is registered and unfinished, so nothing may be cloned from it; the next collection releases it"
            )));
        }
        let source_owner = source_record.role.owner().clone();
        let operation = match registration.role() {
            BuildVolumeRole::Seed { .. } => "seed_capture",
            BuildVolumeRole::Linked { .. } | BuildVolumeRole::Superseded { .. } => "clone",
        };
        let creating = layout
            .register(destination, registration)
            .map_err(host_error)?;
        let images = [
            Image {
                role: "source",
                before: &from,
                after: &from,
            },
            Image {
                role: "destination",
                before: &to,
                after: &to,
            },
        ];
        let capture = || self.capture_build_seed(&from, mount, &to);
        let captured = match observed {
            Observed::Walked => self.image_telemetry.measure(operation, &images, capture),
            Observed::UnderSourceLock => self
                .image_telemetry
                .measure_under_source_lock(operation, &images, capture),
        }
        .and_then(|()| written_at(&to));
        match captured {
            Ok(written) => Ok(CapturedBuildVolume {
                id: destination.clone(),
                registration: registration.clone(),
                written,
                source_owner,
                creating,
            }),
            Err(error) => Err(self.unregister(layout, destination, creating, error)),
        }
    }

    /// Fix `captured`'s freshness mark and publish it. The captured generation is proven
    /// forkable, whatever the source is, and a seed is proven again over its final generation
    /// before it is published (01a_apfs.md, "Clone cost follows extents, not size"); a refusal
    /// names the defragmentation of the workspace whose volume the capture came from. Nothing
    /// writes the clone here: its first mutation pays for its extent map wherever it lands. Every
    /// failure releases what was registered, so a refused clone leaves neither an image nor a
    /// record behind it. Nothing here reads the source, so it runs without the source's image
    /// lock.
    pub fn finish_captured_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
    ) -> Result<(), ApfsStorageError> {
        let CapturedBuildVolume {
            id: destination,
            registration,
            written,
            source_owner,
            creating,
        } = captured;
        let to = layout.image(&destination);
        let finished = (|| {
            crate::storage::apfs::extents::prove_forkable(&to)
                .map_err(|error| defragment_first(error, &source_owner))?;
            self.mark_seed_written(layout, &destination, written)?;
            match registration.role() {
                BuildVolumeRole::Seed { target, .. } => {
                    crate::storage::apfs::extents::prove_forkable(&to)
                        .map(Some)
                        .map_err(|error| defragment_first(error, target))
                }
                BuildVolumeRole::Linked { .. } | BuildVolumeRole::Superseded { .. } => Ok(None),
            }
        })();
        let proof = match finished {
            Ok(proof) => proof,
            Err(error) => return Err(self.unregister(layout, &destination, creating, error)),
        };
        // A seed's record keeps its proof's count, for the cleanup that frees it once released.
        let registration = match &proof {
            Some(proof) => registration.counted(proof.extents().get()),
            None => registration,
        };
        if let Err(error) = layout.publish(&destination, &registration) {
            return Err(self.unregister(layout, &destination, creating, host_error(error)));
        }
        if let (Some(proof), BuildVolumeRole::Seed { target, .. }) = (proof, registration.role())
            && let Err(error) = proof.revalidate(&to)
        {
            // Published over a generation that changed under the proof: back to registered, so
            // no fork takes it, and released with the rest of the failed capture.
            let error = defragment_first(error, target);
            let error = match layout.write_record(&destination, registration.registered()) {
                Ok(()) => error,
                Err(cleanup) => ApfsStorageError::Cleanup {
                    operation: "unpublish a seed whose extent proof went stale",
                    primary: Box::new(error),
                    cleanup: Box::new(host_error(cleanup)),
                },
            };
            return Err(self.unregister(layout, &destination, creating, error));
        }
        drop(creating);
        Ok(())
    }

    /// Release `captured`, which a failed fork never finishes: the creation's hold goes first,
    /// so the release can claim it.
    pub fn abandon_captured_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
    ) -> Result<Release, ApfsStorageError> {
        let CapturedBuildVolume { id, creating, .. } = captured;
        drop(creating);
        self.release_build_volume(layout, &id)
    }
    /// Release `id`, whose registered creation failed with `primary`: the creation's hold goes
    /// first, so the release can claim it, and the record goes with the image or alone when no
    /// image was made. A release that cannot run is joined to `primary`, never hidden: the
    /// registered record then stays, naming its owner, for collection to release by its id.
    fn unregister(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        creating: BuildVolumeHold,
        primary: ApfsStorageError,
    ) -> ApfsStorageError {
        drop(creating);
        let cleanup = match self.release_build_volume(layout, id) {
            Ok(Release::Deleted { .. }) => Ok(()),
            Ok(Release::Held { open }) => Err(ApfsStorageError::Host(format!(
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

    /// Record the write instant held by seed `seed`, taken from the clone before its own
    /// extent-map write, never from the live source after capture. A fork's mount and dropped
    /// daemon record likewise do not add reusable build state. Admitted jobs can write only
    /// after the claim or Quiet's stopped socket lease is released.
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

    /// Fork step 3's capture (16_build_volumes.md, "Fork"), the one step of it that reads `seed`
    /// and so the one the fork takes under its source's image lock: a new live volume
    /// registered as `registration` and holding `seed`'s bytes. A seed is never mounted, so it
    /// is captured detached. [`Self::ready_fork_build_volume`] makes it usable once that lock
    /// is let go.
    pub fn capture_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<CapturedBuildVolume, ApfsStorageError> {
        self.verify_controller_path(&layout.image(seed))?;
        self.capture_build_volume_under_hold(
            layout,
            seed,
            &BuildVolumeId::mint(),
            registration,
            None,
            Observed::UnderSourceLock,
        )
    }

    /// The captured live volume finished and published, then `own_seed`, when one is asked for,
    /// cloned from it before anything mounts it, so the seed holds exactly the bytes the source's
    /// seed held, then the live volume mounted without the seed's Nx daemon record: a daemon
    /// record names another checkout's process. A failure after publication releases the live
    /// volume and a seed already made. The caller points a checkout's link at the answered
    /// mountpoint.
    pub fn ready_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
        own_seed: Option<(&BuildVolumeId, &Registration)>,
    ) -> Result<(BuildVolumeId, PathBuf), ApfsStorageError> {
        let live = captured.id.clone();
        self.finish_captured_build_volume(layout, captured)?;
        let made = match own_seed {
            Some((seed, registration)) => {
                match self.clone_build_volume_under_hold(layout, &live, seed, registration, None) {
                    Ok(()) => Some(seed),
                    Err(primary) => return Err(self.release_unready(layout, &[&live], primary)),
                }
            }
            None => None,
        };
        let mounted = self.mount_build_volume(layout, &live).and_then(|mount| {
            let state = crate::build_volume::BuildVolumeState::read(&mount)
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
            crate::build_volume::nx::discard_daemon_records(&mount, &state)
                .map_err(|error| io_error("discard the seed's Nx daemon record", &mount, error))?;
            Ok(mount)
        });
        match mounted {
            Ok(mount) => Ok((live, mount)),
            Err(primary) => {
                let made: Vec<&BuildVolumeId> = std::iter::once(&live).chain(made).collect();
                Err(self.release_unready(layout, &made, primary))
            }
        }
    }

    /// Release `volumes`, which a fork's ready that failed with `primary` published: a release
    /// that cannot run is joined to `primary`, never hidden, and leaves the volume for collection.
    fn release_unready(
        &self,
        layout: &BuildVolumeLayout,
        volumes: &[&BuildVolumeId],
        primary: ApfsStorageError,
    ) -> ApfsStorageError {
        volumes.iter().fold(primary, |primary, id| {
            let cleanup = match self.release_build_volume(layout, id) {
                Ok(Release::Deleted { .. }) => Ok(()),
                Ok(Release::Held { open }) => Err(ApfsStorageError::Host(format!(
                    "build volume {id} stays while another hold on it lives ({} open); its holder releases it, or the next collection does",
                    open.len()
                ))),
                Err(error) => Err(error),
            };
            match crate::storage::apfs::combine_cleanup::<std::convert::Infallible>(
                "release a build volume whose fork never readied",
                primary,
                cleanup,
            ) {
                Err(error) => error,
                Ok(never) => match never {},
            }
        })
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
            None => {
                self.backend
                    .attach_mounted(&image, &mount, MountAccess::ReadWrite, false)?;
                return Ok(mount);
            }
        };
        let lease = self.backend.prepare_mount(&attachment)?;
        let observed = || -> Result<bool, ApfsStorageError> {
            let expected = MountPoint::new(&mount);
            let mut found = false;
            for mounted in self.mount_source.mounts()? {
                match (
                    mounted.source_device == attachment.volume_device(),
                    expected.names(&mounted.mount_point),
                ) {
                    (true, true) => found = true,
                    (false, false) => {}
                    _ => {
                        return Err(ApfsStorageError::Host(format!(
                            "build volume {id} image {} expects {} at {}, but the kernel mounts {} at {}",
                            image.display(),
                            attachment.volume_device(),
                            mount.display(),
                            mounted.source_device,
                            mounted.mount_point.display(),
                        )));
                    }
                }
            }
            Ok(found)
        };
        // Attachment recovery released its image lease, though its raw pin survived. A second
        // mounter may have won that gap: decide only after reacquiring the lease, and retain it
        // through both the command and any re-observation of the kernel's refusal.
        if observed()? {
            return Ok(mount);
        }
        if let Err(error) = lease.mount(&mount, MountAccess::ReadWrite, false) {
            if !mount_is_in_progress(&error) {
                return Err(error.into());
            }
            let finding = match observed() {
                Ok(true) => return Ok(mount),
                Ok(false) => "the expected mount is absent".to_owned(),
                Err(observation) => observation.to_string(),
            };
            return Err(ApfsStorageError::Host(format!(
                "cannot adopt build volume {id} image {} device {} at {}: {error}; {finding}",
                image.display(),
                attachment.volume_device(),
                mount.display(),
            )));
        }
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

    /// Unmount and detach build volume `id`, then commit its image to `build/.deleting`
    /// and delete its record, hold and mountpoint (16_build_volumes.md, "Garbage collection"); the gateway's cleanup frees the image. Only cowshed's own claims keep a volume: a job Only cowshed's own claims keep a volume: a job
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

    /// Detach build volume `id` of `owner`, kept whole for its next mount, once `owner` is
    /// detached: claimed, so no job is admitted on it and no land moves a link onto it meanwhile,
    /// then unmounted unforced. Whose it is is read again under the claim, from its record: a
    /// volume a land gave another checkout since is left as it is. A refused detach mounts it
    /// again before the claim is let go, as sealing does (16_build_volumes.md, "Targets and
    /// seeds"), and answers the refusal. A volume a job holds answers `BuildVolumeHeld`.
    pub fn detach_owned_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        owner: &WorkspaceName,
    ) -> Result<(), ApfsStorageError> {
        let claim = match self.claim_build_volume(layout, id)? {
            Claim::Claimed(claim) => claim,
            Claim::Held { open } => {
                return Err(ApfsStorageError::BuildVolumeHeld {
                    image: layout.image(id),
                    hold: layout.hold_path(id),
                    holds: Vec::new(),
                    open,
                });
            }
        };
        let role = layout
            .read_record_present(id)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?
            .map(|record| record.role);
        if !matches!(role, Some(BuildVolumeRole::Linked { checkout }) if checkout == *owner) {
            return Ok(());
        }
        let Err(refused) = self.detach_claimed_build_volume(layout, &claim) else {
            return Ok(());
        };
        let remounted = self.mount_build_volume(layout, id);
        drop(claim);
        Err(match remounted {
            Ok(_) => refused,
            Err(cleanup) => ApfsStorageError::Cleanup {
                operation: "mount again the build volume whose detach was refused",
                primary: Box::new(refused),
                cleanup: Box::new(cleanup),
            },
        })
    }

    /// Seal the live volume before a replacement build link is published, borrowing the existing
    /// exclusive claim rather than reacquiring or upgrading it (16_build_volumes.md, "Targets
    /// and seeds"). Unlike release, this never forces or deletes the image, record, hold or
    /// mountpoint. A refusal after an accepted unmount may leave the volume unmounted: the
    /// caller restores `layout.mount(claim.id())` before dropping the retained claim.
    pub fn detach_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<(), ApfsStorageError> {
        let id = claim.id();
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        let mount = layout.mount(id);
        let held = |open| ApfsStorageError::BuildVolumeHeld {
            image: image.clone(),
            hold: layout.hold_path(id),
            holds: Vec::new(),
            open,
        };
        let attachment = {
            // Wait for cosmetic operations before census, without exempting this process. The
            // backend re-proves attachment identity under its own lease and raw pin at unmount.
            let _lease = self.backend.image_lease(&image)?;
            let Some(attachment) = self.backend.existing_attachment(&image)? else {
                return Ok(());
            };
            let canonical = MountPoint::new(&mount);
            let mounted = self.mount_source.mounts()?.into_iter().find(|mounted| {
                mounted.source_device == attachment.volume_device()
                    || canonical.names(&mounted.mount_point)
            });
            if let Some(mounted) = &mounted {
                if !canonical.names(&mounted.mount_point)
                    || mounted.source_device != attachment.volume_device()
                {
                    return Err(ApfsStorageError::Host(format!(
                        "cannot seal build volume {id} at {}: {} is mounted at {}, expected {}",
                        mount.display(),
                        mounted.source_device,
                        mounted.mount_point.display(),
                        attachment.volume_device()
                    )));
                }
                let open = holders_of(&mount)?;
                if !open.is_empty() {
                    return Err(held(open));
                }
            }
            attachment
        };
        self.backend
            .unmount_verified(&attachment, DetachIntent::WhenIdle)
            .map_err(|refusal| {
                if !cowshed_apfs::detach_was_dissented(&refusal) {
                    return ApfsStorageError::from(refusal);
                }
                let open = self.mounted_at(&mount).and_then(|mounted| match mounted {
                    Some(device) if device == attachment.volume_device() => holders_of(&mount),
                    Some(_) | None => Ok(Vec::new()),
                });
                match open {
                    Ok(open) if !open.is_empty() => held(open),
                    Ok(_) => refusal.into(),
                    Err(cleanup) => ApfsStorageError::Cleanup {
                        operation: "name claimed build volume holders",
                        primary: Box::new(refusal.into()),
                        cleanup: Box::new(cleanup),
                    },
                }
            })?;
        // An accepted unforced unmount excludes filesystem holders. Wait out only a transient
        // device refusal, never escalating to force (16_build_volumes.md, seal step 4).
        self.backend
            .detach(&attachment, DetachIntent::WithinGrace)?;
        Ok(())
    }

    /// Release the volume while retaining its claim and ownership record until its detached
    /// image's release is committed to `build/.deleting`, durably; then the record, hold and
    /// mountpoint go and this answers, the bytes left to the gateway's cleanup. A failure refuses
    /// the retirement with its record intact. The deletion log names the owner read before the
    /// image and record are removed.
    pub fn release_claimed(
        &self,
        layout: &BuildVolumeLayout,
        claim: ReleaseClaim,
    ) -> Result<Option<Refusal>, ApfsStorageError> {
        let id = claim.id();
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        let mount = layout.mount(id);
        let present = image
            .try_exists()
            .map_err(|error| io_error("inspect a build volume before release", &image, error))?;
        // Resolve ownership before deleting anything; unreadable evidence refuses release.
        let record = layout
            .read_record_present(id)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let extents = record.as_ref().and_then(|record| record.extents);
        let recorded = record.map(|record| record.role);
        if present && recorded.is_none() {
            return Err(ApfsStorageError::Host(format!(
                "build volume {id} at {} has no ownership record; refusing to release an unregistered image",
                image.display()
            )));
        }
        let mut refused = None;
        // A minted volume is formatted before anything attaches it, so an attachment of a build
        // image is an APFS one; anything else is refused, not released.
        if present && let Some(attachment) = self.backend.existing_attachment(&image)? {
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
            self.backend.detach(&attachment, DetachIntent::Release)?;
        }
        let removed = self.image_telemetry.measure_release(&image, || {
            layout
                .release_image(id)
                .map_err(|error| io_error("release a build volume image", &image, error))
        })?;
        if removed {
            layout.journal_release(id, recorded.as_ref(), true);
            self.hand_over(
                &layout.released(id),
                cowshed_gateway_types::ReleaseRecord {
                    unique_bytes: None,
                    extents: extents.and_then(std::num::NonZeroU64::new),
                },
            );
        }
        // A volume whose image was already gone is retired by its record alone, said as such.
        if remove_if_present(&layout.record(id))? && !removed {
            layout.journal_release(id, recorded.as_ref(), false);
        }
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
            let captured = self.capture_fork_build_volume(
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
            let (_, mount) = self.ready_fork_build_volume(layout, captured, None)?;
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
                        cowshed_os::eprintln!(
                            "cowshed: {workspace}'s build link named build volume {linked}, which no longer exists; {workspace} now links a fresh clone of its seed {seed}, and what {linked} held beyond that seed is lost"
                        );
                    }
                    None => {
                        link::unlink(checkout).map_err(host).map_err(lost)?;
                        cowshed_os::eprintln!(
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
            self.backend
                .unmount_verified(&attachment, DetachIntent::WhenIdle)?;
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

    /// The existing contiguous rewrite, extended to the exact build source or seed that doctor
    /// names. The caller retains the exclusive build claim and has closed its Nx daemon.
    pub fn defragment_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<crate::storage::lifecycle::DefragmentOutcome, ApfsStorageError> {
        use crate::storage::apfs::extents;
        let id = claim.id();
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        let was_mounted = self.mounted_at(&layout.mount(id))?.is_some();
        let written = written_at(&image)?;
        // Extents are read once the image is detached: while it is mounted, a write its volume
        // has not flushed has no device offset yet (`F_LOG2PHYS_EXT` answers -1 for it).
        let rewritten = self
            .detach_claimed_build_volume(layout, claim)
            .and_then(|()| {
                let previous = extents::count_extents(&image)?;
                extents::require_room_for_rewrite(&image)?;
                extents::rewrite_contiguously(&image).map(|bytes| (previous, bytes))
            });
        let (previous, bytes) = match rewritten {
            Ok(rewritten) => rewritten,
            Err(primary) if was_mounted => {
                return match self.mount_build_volume(layout, id) {
                    Ok(_) => Err(primary),
                    Err(cleanup) => Err(ApfsStorageError::Cleanup {
                        operation: "restore the build mount after a refused defrag",
                        primary: Box::new(primary),
                        cleanup: Box::new(cleanup),
                    }),
                };
            }
            Err(primary) => return Err(primary),
        };
        if was_mounted {
            self.mount_build_volume(layout, id)?;
        } else {
            let attachment = self.backend.attach_verified(&image)?;
            self.backend.detach(&attachment, DetachIntent::Release)?;
            // A physical rewrite of an immutable seed does not make its build contents newer.
            // Its ctime/inode still invalidate any earlier physical-generation extent proof.
            set_written(&image, written)?;
        }
        Ok(crate::storage::lifecycle::DefragmentOutcome {
            previous,
            extents: extents::count_extents(&image)?,
            bytes,
        })
    }

    /// How many extents seed `seed`'s image holds, counted no further than `stop_at`.
    pub fn seed_extents(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        stop_at: u64,
    ) -> Result<ExtentCount, ApfsStorageError> {
        let image = layout.image(seed);
        self.verify_controller_path(&image)?;
        extents::count_extents_to(&image, stop_at)
    }

    /// Release what a compaction left in [`BuildVolumeLayout::compacting`], every seed's clone and
    /// the copy, in bounded steps (16_build_volumes.md, "Seed compaction", "Leftovers"). The
    /// caller holds the project's compaction lock, so they are garbage, and no image lock:
    /// freeing a whole seed's extent map takes minutes.
    pub fn release_seed_compaction_leftovers(
        &self,
        layout: &BuildVolumeLayout,
    ) -> Result<(), ApfsStorageError> {
        let directory = layout.compacting();
        let entries = fs::read_dir(&directory)
            .map_err(|error| io_error("list the seed compaction directory", &directory, error))?;
        for entry in entries {
            let path = entry
                .map_err(|error| io_error("list the seed compaction directory", &directory, error))?
                .path();
            if path.extension().is_some_and(|extension| extension == "dmg") {
                release_compaction_file(&path)?;
            }
        }
        Ok(())
    }
    /// Clone published seed `seed` aside for its compaction (16_build_volumes.md, "Seed
    /// compaction"), with the instant of the last write it holds. The caller holds the target's
    /// image lock, so nothing retires the seed during the clone, and the project's compaction
    /// lock.
    pub fn stage_seed_compaction(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
    ) -> Result<CompactionSource, ApfsStorageError> {
        let image = layout.image(seed);
        self.verify_controller_path(&image)?;
        let directory = layout.compacting();
        fs::create_dir_all(&directory)
            .map_err(|error| io_error("create the seed compaction directory", &directory, error))?;
        let clone = directory.join(format!("{seed}.dmg"));
        let written = written_at(&image)?;
        self.capture_build_seed(&image, None, &clone)?;
        Ok(CompactionSource {
            seed: seed.clone(),
            clone,
            written,
        })
    }

    /// Copy `source`'s data into fresh, contiguous space at normal disk priority and past the cache,
    /// asking `keep_going` after every chunk with the bytes copied so far. No lock is held. The
    /// clone stays, for a catch-up to read which blocks a newer seed shares with it; the caller
    /// releases it with the copy ([`Self::release_seed_compaction_leftovers`]). A stop or a
    /// failure removes the copy.
    pub fn copy_seed_compaction(
        &self,
        source: CompactionSource,
        keep_going: &mut dyn FnMut(u64) -> ControlFlow<String>,
    ) -> Result<Compaction, ApfsStorageError> {
        let copy = source.clone.with_file_name(COMPACTION_COPY);
        let before = extents::count_extents(&source.clone)?;
        Ok(
            match extents::copy_contiguously(&source.clone, &copy, keep_going)? {
                Copied::Stopped { bytes, why } => Compaction::Stopped { bytes, why },
                Copied::Complete { bytes } => Compaction::Copied(CompactedSeed {
                    after: extents::count_extents(&copy)?,
                    source,
                    copy,
                    before,
                    bytes,
                }),
            },
        )
    }

    /// Catch `compacted` up to `next`, the clone of the seed that replaced its seed
    /// (16_build_volumes.md, "Seed compaction"): the copy is written in place where `next`'s data
    /// does not share the older clone's blocks and punched where `next` has a hole
    /// ([`extents::catch_up`]), asking `keep_going` as the copy does. Then the older clone is
    /// released in bounded steps, and the copy holds `next`'s bytes. No lock is held. A stop
    /// leaves both clones and the half-caught copy for the caller to release.
    pub fn catch_up_seed_compaction(
        &self,
        compacted: CompactedSeed,
        next: CompactionSource,
        keep_going: &mut dyn FnMut(u64) -> ControlFlow<String>,
    ) -> Result<CatchUp, ApfsStorageError> {
        let CompactedSeed {
            source,
            copy,
            before,
            bytes,
            ..
        } = compacted;
        match extents::catch_up(&source.clone, &next.clone, &copy, keep_going)? {
            CaughtUp::Stopped {
                bytes: written,
                why,
            } => Ok(CatchUp::Stopped {
                bytes: bytes + written,
                why,
            }),
            CaughtUp::Complete { written, punched } => {
                release_compaction_file(&source.clone)?;
                Ok(CatchUp::Caught {
                    compacted: CompactedSeed {
                        after: extents::count_extents(&copy)?,
                        source: next,
                        copy,
                        before,
                        bytes: bytes + written,
                    },
                    written,
                    punched,
                })
            }
        }
    }

    /// Publish `compacted` as `destination`, the seed `registration` records, which is its
    /// target's latest from then on: registered before its image exists, moved into place, given
    /// the copied seed's freshness mark (the bytes are that seed's, so they are no newer), proven
    /// forkable, published and proven again across publication, as a captured seed is. The
    /// caller holds the target's image lock and has checked that `compacted`'s seed is still the
    /// latest. A failure releases what was registered. The seed's clone stays for the caller to
    /// release with no lock held ([`Self::release_seed_compaction_leftovers`]).
    pub fn publish_seed_compaction(
        &self,
        layout: &BuildVolumeLayout,
        compacted: CompactedSeed,
        destination: &BuildVolumeId,
        registration: Registration,
    ) -> Result<(), ApfsStorageError> {
        let to = layout.image(destination);
        self.verify_controller_path(&to)?;
        let creating = match layout.register(destination, &registration) {
            Ok(creating) => creating,
            Err(error) => {
                return crate::storage::apfs::combine_cleanup(
                    "release a compacted copy whose seed could not be registered",
                    host_error(error),
                    release_compaction_file(&compacted.copy),
                );
            }
        };
        let placed = (|| {
            fs::rename(&compacted.copy, &to)
                .map_err(|error| io_error("place the compacted seed", &to, error))?;
            let images = layout.images();
            fs::File::open(images)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| io_error("sync the build images directory", images, error))?;
            set_written(&to, compacted.source.written)?;
            extents::prove_forkable(&to)
        })();
        let proof = match placed {
            Ok(proof) => proof,
            Err(error) => return Err(self.unregister(layout, destination, creating, error)),
        };
        // A seed's record keeps its proof's count, for the cleanup that frees it once released.
        let registration = registration.counted(proof.extents().get());
        if let Err(error) = layout.publish(destination, &registration) {
            return Err(self.unregister(layout, destination, creating, host_error(error)));
        }
        if let Err(error) = proof.revalidate(&to) {
            let error = match layout.write_record(destination, registration.registered()) {
                Ok(()) => error,
                Err(cleanup) => ApfsStorageError::Cleanup {
                    operation: "unpublish a compacted seed whose extent proof went stale",
                    primary: Box::new(error),
                    cleanup: Box::new(host_error(cleanup)),
                },
            };
            return Err(self.unregister(layout, destination, creating, error));
        }
        drop(creating);
        Ok(())
    }
}

/// Release one of a seed compaction's files in bounded steps (16_build_volumes.md, "Garbage
/// collection"): a clone whose seed was retired meanwhile owns that seed's whole extent map.
fn release_compaction_file(path: &Path) -> Result<(), ApfsStorageError> {
    match release_file(path, STEP_EXTENTS) {
        Ok(Released::Freed { .. } | Released::Absent) => Ok(()),
        Ok(Released::Busy) => Err(ApfsStorageError::Host(format!(
            "{} is being released by another process, but only the compaction holding its project's compaction lock touches it",
            path.display()
        ))),
        Err(error) => Err(io_error("release a seed compaction's file", path, error)),
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

/// Whether `path` was there to remove.
fn remove_if_present(path: &Path) -> Result<bool, ApfsStorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("remove build volume record", path, error)),
    }
}

fn host_error(error: crate::CowshedError) -> ApfsStorageError {
    ApfsStorageError::Host(error.to_string())
}

/// An extent refusal of `target`'s seed, with the command that makes it forkable again.
fn defragment_first(error: ApfsStorageError, target: &WorkspaceName) -> ApfsStorageError {
    ApfsStorageError::Host(format!("{error}; next: cowshed defrag {target}"))
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
    use super::super::{KernelMountSource, SystemKernelMountSource};
    use super::*;
    use crate::api::resources::JobAccounting;
    use crate::build_volume::BuildVolumeRole;
    use crate::metadata::WorkspaceName;
    use crate::repository::{ProjectPaths, RepoId};
    use crate::storage::apfs::ApfsSubstrateConfig;
    use cowshed_apfs::mounts::KernelMountSnapshot;
    use cowshed_apfs::{CommandRequest, SystemCommandRunner};
    use cowshed_os::namespace::{HostNamespace, TestNamespace};
    use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};
    use std::io::Read;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant, SystemTime};

    fn fixture(
        namespace: TestNamespace,
        root: &Path,
    ) -> (
        MacOsApfsExecutionHost<SystemCommandRunner>,
        BuildVolumeLayout,
    ) {
        fixture_with(namespace, root, SystemKernelMountSource)
    }

    fn fixture_with(
        namespace: TestNamespace,
        root: &Path,
        mount_source: impl KernelMountSource,
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
        let namespace = HostNamespace::Test(namespace);
        let host = MacOsApfsExecutionHost::with_mount_source(
            SystemCommandRunner::new(namespace),
            ApfsSubstrateConfig::new(namespace, &store, root.join("checkout")),
            mount_source,
        )
        .unwrap();
        (host, BuildVolumeLayout::new(&project).unwrap())
    }

    /// Observe admission at the mounted-source selection that leads to the live clone, while
    /// forwarding the real kernel mount table. No remount or command is part of that capture.
    struct CaptureAdmissionMounts {
        layout: BuildVolumeLayout,
        source: BuildVolumeId,
        socket: Option<(tokio::runtime::Handle, PathBuf)>,
        denied: Arc<AtomicBool>,
    }

    impl KernelMountSource for CaptureAdmissionMounts {
        fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
            let mounts = SystemKernelMountSource.mounts()?;
            if !self.denied.load(Ordering::SeqCst) {
                if let Some((runtime, socket)) = &self.socket {
                    let refused = runtime
                        .block_on(crate::runtime::supervisor_socket::bind(socket))
                        .expect_err("replacement admission must stay closed during capture");
                    assert_eq!(refused.code, crate::error::ErrorCode::Conflict);
                    self.denied.store(true, Ordering::SeqCst);
                } else if let Err(refused) = self.layout.admit(&self.source) {
                    // clone_build_volume first checks for a mount before taking its claim.
                    // Its second census, in clone_claimed_build_volume, must refuse a job.
                    assert_eq!(refused.kind(), io::ErrorKind::ResourceBusy);
                    self.denied.store(true, Ordering::SeqCst);
                }
            }
            Ok(mounts)
        }
    }

    #[test]
    fn real_apfs_build_seed_live_capture_is_fresh_until_an_admitted_job_writes() {
        let root = crate::scratch_apfs::ScratchRoot::new("seed-admission-clock").unwrap();
        let source = BuildVolumeId::mint();
        let (system, layout) = fixture(root.namespace(), root.path());
        crate::blank_image::blank_image(root.namespace(), &layout.image(&source));
        let mount = system.mount_build_volume(&layout, &source).unwrap();
        layout.write_record(&source, &linked("main")).unwrap();
        fs::write(mount.join("warm"), b"captured build state\n").unwrap();
        fs::create_dir(mount.join("cache")).unwrap();
        fs::write(mount.join("cache/index"), b"captured index state\n").unwrap();
        let mut reader = fs::File::open(mount.join("cache/index")).unwrap();
        let before = snapshot(&mount);
        let owner = fs::read(layout.record(&source)).unwrap();
        let record = BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Seed {
                target: WorkspaceName::main(),
                incarnation: WorkspaceIncarnation::new("a".repeat(32)).unwrap(),
            },
        );
        let denied = Arc::new(AtomicBool::new(false));
        let (host, _) = fixture_with(
            root.namespace(),
            root.path(),
            CaptureAdmissionMounts {
                layout: layout.clone(),
                source: source.clone(),
                socket: None,
                denied: Arc::clone(&denied),
            },
        );
        let seed = BuildVolumeId::mint();
        host.clone_build_volume(&layout, &source, &seed, &registered(record.clone()))
            .unwrap();
        assert!(denied.load(Ordering::SeqCst));
        assert!(
            host.backend
                .existing_attachment(&layout.image(&seed))
                .unwrap()
                .is_none(),
            "the live source capture leaves its seed detached"
        );
        let mut index = String::new();
        reader.read_to_string(&mut index).unwrap();
        assert_eq!(index, "captured index state\n");
        drop(reader);
        let live = host.build_volume_written(&layout, &source).unwrap();
        let seeded = host.build_volume_written(&layout, &seed).unwrap();
        let captured_written = seeded;
        cowshed_os::eprintln!("live capture seed clock={seeded:?}, flushed live={live:?}");
        assert!(
            live <= seeded,
            "capture without a later write leaves a Fresh seed without tolerance"
        );
        assert_eq!(snapshot(&mount), before);
        assert_eq!(fs::read(layout.record(&source)).unwrap(), owner);

        let admitted = layout
            .admit(&source)
            .expect("job admission after capture hold release");
        let refused_seed = BuildVolumeId::mint();
        let me = i32::try_from(std::process::id()).expect("pid");
        let refused =
            host.clone_build_volume(&layout, &source, &refused_seed, &registered(record.clone()));
        let Err(ApfsStorageError::BuildVolumeHeld { holds, .. }) = &refused else {
            panic!("an admitted job refuses the capture: {refused:?}");
        };
        assert!(
            holds.iter().any(|holder| holder.pid == me),
            "the skip names the admitted job's holder: {holds:?}"
        );
        let said = refused.unwrap_err().to_string();
        assert!(said.contains(&format!("is held by pid {me} (")), "{said}");
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
        assert_eq!(seeded, captured_written, "the capture mark stays immutable");
        cowshed_os::eprintln!("admitted job seed clock={seeded:?}, flushed live={live:?}");
        assert!(
            live > seeded,
            "the admitted job's write must make the seed behind"
        );
        let inspection = BuildVolumeId::mint();
        host.clone_build_volume(
            &layout,
            &seed,
            &inspection,
            &registered(linked("inspection")),
        )
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
    async fn real_apfs_quiet_seed_live_capture_refuses_admission_until_publication() {
        use crate::api::dto::{ExecCommand, ExecRequest, ExitStatus, RunSandboxMode, StdinSource};
        use crate::runtime::build_volumes::BuildVolumes;
        use crate::runtime::supervisor::{
            CommitmentPublisher, WorkspaceSupervisor, WorkspaceSupervisorConfig,
        };
        use crate::runtime::supervisor_socket;

        let root = crate::scratch_apfs::ScratchRoot::new("seed-quiet-admission").unwrap();
        let source = BuildVolumeId::mint();
        let (system, layout) = fixture(root.namespace(), root.path());
        let checkout = root.path().join("checkout");
        fs::create_dir_all(checkout.join(".cowshed")).unwrap();
        crate::blank_image::blank_image(root.namespace(), &layout.image(&source));
        let mount = system.mount_build_volume(&layout, &source).unwrap();
        crate::build_volume::BuildVolumeState::default()
            .write(&mount)
            .unwrap();
        layout.write_record(&source, &linked("main")).unwrap();
        crate::build_volume::link::point(&checkout, &mount).unwrap();
        fs::write(mount.join("warm"), b"captured build state\n").unwrap();
        fs::create_dir(mount.join("cache")).unwrap();
        fs::write(mount.join("cache/index"), b"captured index state\n").unwrap();
        let mut reader = fs::File::open(mount.join("cache/index")).unwrap();
        let before = snapshot(&mount);
        let owner = fs::read(layout.record(&source)).unwrap();
        let volumes = BuildVolumes::new(
            Arc::new(system),
            layout.clone(),
            crate::project_retirement::acquire_project_reader(
                &root.path().join("store"),
                &RepoId::parse("acme/widget").unwrap(),
            )
            .unwrap(),
        );
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
        let denied = Arc::new(AtomicBool::new(false));
        let (host, _) = fixture_with(
            root.namespace(),
            root.path(),
            CaptureAdmissionMounts {
                layout: layout.clone(),
                source: source.clone(),
                socket: Some((tokio::runtime::Handle::current(), socket.clone())),
                denied: Arc::clone(&denied),
            },
        );
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
                host.clone_quiet_build_volume(&layout, &quiet, &seed, &registered(record))
            })
        };
        capture.await.unwrap().unwrap();
        assert!(denied.load(Ordering::SeqCst));
        assert!(
            host.backend
                .existing_attachment(&layout.image(&seed))
                .unwrap()
                .is_none(),
            "the quiet source capture leaves its seed detached"
        );
        let mut index = String::new();
        reader.read_to_string(&mut index).unwrap();
        assert_eq!(index, "captured index state\n");
        drop(reader);
        let live = host.build_volume_written(&layout, &source).unwrap();
        let seeded = host.build_volume_written(&layout, &seed).unwrap();
        let captured_written = seeded;
        cowshed_os::eprintln!("quiet live capture seed clock={seeded:?}, flushed live={live:?}");
        assert!(
            live <= seeded,
            "Quiet's live capture leaves Fresh without a later write or tolerance"
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
                    class: crate::api::dto::ExecClass::Background,
                    want: None,
                    scope: crate::api::dto::ExecScope::Workspace,
                },
                crate::runtime::supervisor_socket::AdmissionSource::Worker,
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
        assert_eq!(seeded, captured_written, "the capture mark stays immutable");
        cowshed_os::eprintln!(
            "replacement admitted job seed clock={seeded:?}, flushed live={live:?}"
        );
        assert!(
            live > seeded,
            "an admitted job after Quiet releases must make the seed behind"
        );
        handle
            .retire(crate::runtime::supervisor::NxDaemonRetirement::Recorded)
            .await
            .unwrap();
        drop(handle);
        drop(replacement);
        drop(landing_hold);
        let inspection = BuildVolumeId::mint();
        host.clone_build_volume(
            &layout,
            &seed,
            &inspection,
            &registered(linked("inspection")),
        )
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

    /// What a test's clone registers: the forming verb's, as a fork's would be.
    fn registered(record: BuildVolumeRecord) -> Registration {
        Registration::new(record, Registrant::Intent)
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
            namespace: TestNamespace,
            root: &Path,
            host: &MacOsApfsExecutionHost<SystemCommandRunner>,
            layout: &BuildVolumeLayout,
        ) -> Self {
            use crate::build_volume::link;
            use crate::capabilities::BuildStatePath;

            let image = root.join("store/lane.dmg");
            crate::blank_image::blank_image(namespace, &image);
            let checkout = root.join("lane");
            fs::create_dir_all(&checkout).unwrap();
            let attachment = host
                .backend
                .attach_mounted(&image, &checkout, MountAccess::ReadWrite, false)
                .expect("attach and mount the workspace");
            fs::create_dir_all(checkout.join(".cowshed")).unwrap();

            let build_id = BuildVolumeId::mint();
            crate::blank_image::blank_image(namespace, &layout.image(&build_id));
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
    fn real_apfs_claimed_build_detach_refuses_holders_and_preserves_storage_and_claim() {
        use std::os::unix::fs::MetadataExt;

        let root = crate::scratch_apfs::ScratchRoot::new("claimed-build-detach").unwrap();
        let (host, layout) = fixture(root.namespace(), root.path());
        let id = BuildVolumeId::mint();
        let image = layout.image(&id);
        crate::blank_image::blank_image(root.namespace(), &image);
        let mount = host.mount_build_volume(&layout, &id).unwrap();
        layout.write_record(&id, &linked("main")).unwrap();
        fs::write(mount.join("warm"), b"old build state\n").unwrap();
        let before = snapshot(&mount);
        let record = fs::read(layout.record(&id)).unwrap();
        let claim = host.claim_capture_source(&layout, &id).unwrap();
        let image_inode = fs::metadata(&image).unwrap().ino();
        let hold_inode = fs::metadata(layout.hold_path(&id)).unwrap().ino();
        let device = host.mounted_at(&mount).unwrap().unwrap();
        let open = fs::File::open(mount.join("warm")).unwrap();
        let me = i32::try_from(std::process::id()).unwrap();

        let error = host
            .detach_claimed_build_volume(&layout, &claim)
            .expect_err("an unmanaged filesystem holder must refuse the seal");
        let ApfsStorageError::BuildVolumeHeld { open: holders, .. } = &error else {
            panic!("the refusal must name filesystem holders: {error:?}");
        };
        assert!(holders.iter().any(|holder| holder.pid == me), "{holders:?}");
        assert!(error.to_string().contains(&format!("pid {me} (")));
        assert_eq!(host.mounted_at(&mount).unwrap(), Some(device));
        assert_eq!(snapshot(&mount), before);
        assert_eq!(fs::read(layout.record(&id)).unwrap(), record);
        assert!(
            layout.admit(&id).is_err(),
            "the borrowed claim still fences admission"
        );
        drop(open);

        for _ in 0..2 {
            host.detach_claimed_build_volume(&layout, &claim).unwrap();
            assert!(host.backend.existing_attachment(&image).unwrap().is_none());
            assert!(host.mounted_at(&mount).unwrap().is_none());
            assert!(mount.is_dir(), "detach retains the empty mountpoint");
            assert_eq!(fs::metadata(&image).unwrap().ino(), image_inode);
            assert_eq!(
                fs::metadata(layout.hold_path(&id)).unwrap().ino(),
                hold_inode
            );
            assert_eq!(fs::read(layout.record(&id)).unwrap(), record);
            assert!(layout.admit(&id).is_err());
        }
        host.mount_build_volume(&layout, &id).unwrap();
        assert_eq!(
            snapshot(&mount),
            before,
            "publication rollback can restore the old volume"
        );
        host.detach_claimed_build_volume(&layout, &claim).unwrap();
        assert_eq!(host.release_claimed(&layout, claim).unwrap(), None);
        for path in [&image, &layout.record(&id), &layout.hold_path(&id), &mount] {
            assert!(
                !path.exists(),
                "ordinary release removes {}",
                path.display()
            );
        }
        assert!(
            layout.released(&id).is_file(),
            "the image waits in build/.deleting; a fixture store names no gateway cleanup"
        );
    }

    #[test]
    fn real_apfs_build_volumes_clone_with_mtimes_and_release_unless_a_job_holds_them() {
        let root = crate::scratch_apfs::ScratchRoot::new("build-volume").expect("scratch root");
        let (host, layout) = fixture(root.namespace(), root.path());
        let source = BuildVolumeId::mint();
        // Creation is proven by first touch (build_volume::migrate) and adoption; this test's
        // source is a clone of the run's blank image, mounted the way a fork's volume is.
        crate::blank_image::blank_image(root.namespace(), &layout.image(&source));
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
        let claim = layout
            .claim_release(&source)
            .unwrap()
            .expect("claim idle build source");
        host.defragment_build_volume(&layout, &claim)
            .expect("rewrite mounted build source");
        assert_eq!(
            snapshot(&mount),
            before,
            "source rewrite restores its mount, bytes and mtimes"
        );
        drop(claim);

        let clone = BuildVolumeId::mint();
        let started = Instant::now();
        host.clone_build_volume(&layout, &source, &clone, &registered(linked("topic")))
            .expect("clone the build volume");
        let cloned = started.elapsed();
        let written = written_at(&layout.image(&clone)).unwrap();
        let claim = layout
            .claim_release(&clone)
            .unwrap()
            .expect("claim detached build image");
        host.defragment_build_volume(&layout, &claim)
            .expect("rewrite detached seed-shaped image");
        assert_eq!(
            written_at(&layout.image(&clone)).unwrap(),
            written,
            "physical seed rewrite cannot make its build contents newer"
        );
        assert!(
            host.mounted_at(&layout.mount(&clone)).unwrap().is_none(),
            "a detached seed stays detached"
        );
        drop(claim);
        let started = Instant::now();
        let clone_mount = host
            .mount_build_volume(&layout, &clone)
            .expect("mount the clone");
        let mounted = started.elapsed();
        cowshed_os::eprintln!("build volume clone {cloned:?}, attach+mount {mounted:?}");
        assert_eq!(
            snapshot(&clone_mount),
            before,
            "bytes and mtimes survive the clone"
        );
        assert_eq!(
            layout.read_record(&clone).unwrap().role,
            linked("topic").role
        );

        let hold = layout.admit(&clone).expect("a job's hold");
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
        let claim = layout
            .claim_release(&clone)
            .unwrap()
            .expect("no admitted job");
        assert!(
            host.defragment_build_volume(&layout, &claim).is_err(),
            "an open build file refuses defrag without force"
        );
        assert_eq!(
            snapshot(&clone_mount),
            before,
            "refused rewrite preserves all data"
        );
        drop(claim);
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
        let status = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(script)
            .current_dir(cwd)
            .status_bounded()
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

    fn diskutil(namespace: HostNamespace, args: &[&str]) -> String {
        let output = SystemCommandRunner::new(namespace)
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
    fn add_volume(namespace: HostNamespace, container: &str, name: &str) -> String {
        diskutil(
            namespace,
            &["apfs", "addVolume", container, "APFS", name, "-nomount"],
        )
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
    fn prove_owned_volume_usage(namespace: HostNamespace, root: &Path, lane: &Lane) {
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
        let added = add_volume(namespace, container, "sibling");
        let sibling = root.join("sibling");
        fs::create_dir_all(&sibling).unwrap();
        diskutil(
            namespace,
            &["mount", "-mountPoint", sibling.to_str().unwrap(), &added],
        );

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
        diskutil(namespace, &["apfs", "deleteVolume", &added]);
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
        let namespace = host.backend.runner().namespace();
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
        let sibling = add_volume(namespace, container, "sibling");
        let sibling_mount = root.join("sibling");
        fs::create_dir_all(&sibling_mount).unwrap();
        diskutil(
            namespace,
            &[
                "mount",
                "-mountPoint",
                sibling_mount.to_str().unwrap(),
                &sibling,
            ],
        );

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
    /// the supervisor samples it like any job group. It reads its stdin, a pipe the returned
    /// child holds, so it runs until [`end_group`] or the child's drop.
    fn lone_group() -> std::process::Child {
        use cowshed_os::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;
        std::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
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
        cowshed_os::test_backstop::waitid(libc::P_PID, leader.id(), libc::WEXITED | libc::WNOWAIT)
            .expect("waitid");
    }

    /// Reap a leader once nothing reads it any more: its interval closed, or its job concluded.
    fn reap(mut leader: std::process::Child) {
        leader.wait_bounded().unwrap();
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

            fn nice(&self) -> Option<i32> {
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
        let (host, layout) = fixture(root.namespace(), root.path());
        let lane = Lane::mount(root.namespace(), root.path(), &host, &layout);
        prove_owned_volume_usage(host.backend.runner().namespace(), root.path(), &lane);
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
                    class: crate::api::dto::ExecClass::Background,
                    want: None,
                    scope: crate::api::dto::ExecScope::Workspace,
                },
                crate::runtime::supervisor_socket::AdmissionSource::Worker,
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
                nice: 10,
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
        handle
            .retire(crate::runtime::supervisor::NxDaemonRetirement::Recorded)
            .await
            .unwrap();
        drop(handle);
        assert_eq!(
            store()
                .sealed(job_id)
                .unwrap()
                .and_then(|sealed| sealed.resources),
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
