//! Re-adoption's retained source (02_workspaces.md, "Re-adoption" step 2): the replaced main's
//! image, its build volume and its old-incarnation seeds leave the names cowshed enumerates for
//! the exact retained paths the re-adoption intent recorded, each detached without force first,
//! then the retained main and build images are attached read-only at their recorded staging
//! mounts for the replacement to be copied from. Every step converges from what is on disk and
//! in the kernel, so a replay after a crash finishes what the last attempt began, and nothing is
//! ever overwritten: a retained path that already exists while its original does too is refused.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{MacOsApfsExecutionHost, allocated_file_bytes, io_error, sync_parent_path};
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, RECORD_SUFFIX,
    ReleaseClaim,
};
use crate::metadata::append_suffix;
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError, COMPANION_SUFFIX};
use crate::storage::recovery::{
    MissingMainBuild, ReAdoptBuild, ReAdoptEmptyBuild, ReAdoptMain, ReAdoptSource,
};
use cowshed_apfs::image::ImageCapacity;
use cowshed_apfs::{ApfsBackend, CommandRunner, DetachIntent, MountAccess, RetainedInput};

/// The files beside a workspace image: its grants sidecar and its CA companion.
const MAIN_COMPANION_SUFFIXES: [&str; 2] =
    [crate::metadata::GRANTS_SIDECAR_SUFFIX, COMPANION_SUFFIX];

/// The claim a seed of main's published build volume is captured under (16_build_volumes.md,
/// "Targets and seeds"). A volume the caller's re-adoption claim already holds is captured under
/// that claim, borrowed: a second exclusive claim on the same hold file would conflict with it.
/// Any other volume takes its own ordinary claim.
enum SeedSource<'a> {
    /// The published volume is the one this claim holds.
    Claimed(&'a ReleaseClaim),
    /// No claim of the caller's holds this volume.
    Unclaimed(&'a BuildVolumeId),
}

impl<'a> SeedSource<'a> {
    /// The published volume of `build`, which `claim` claimed at `build.image.original`. A RAW
    /// original is the published volume, so `claim` holds the very hold file a fresh claim would
    /// lock, and the seed borrows it. A legacy original holds `<original>.hold`, a different file:
    /// the caller's claim keeps that resource, and the published volume takes its own claim.
    fn of_published(
        layout: &BuildVolumeLayout,
        build: &'a ReAdoptBuild,
        claim: &'a ReleaseClaim,
    ) -> Self {
        if build.image.original == layout.image(&build.id) {
            Self::Claimed(claim)
        } else {
            Self::Unclaimed(&build.id)
        }
    }
}

impl<R> MacOsApfsExecutionHost<R>
where
    R: CommandRunner + Send + Sync + 'static,
{
    /// Source census follows recovery's image-lease ordering (01_storage.md, "Mounts" and
    /// "Ownership, identity, and the volume label"). A supervisor's started blocking rename
    /// outlives retirement and holds a normal mount descriptor until its command returns. The
    /// image lease lets that descriptor close before census, without waiting in the supervisor
    /// or excluding the caller's PID. Retention still performs its own non-forced kernel seal.
    pub(crate) fn re_adopt_volume_holders(
        &self,
        image: &Path,
        mount: &Path,
    ) -> Result<Vec<cowshed_os::process::Holder>, ApfsStorageError> {
        self.verify_controller_path(image)?;
        let _lease = self.backend.image_lease(image)?;
        // A bare mountpoint is on the volume beneath it, not the input being recovered.
        if self.mounted_at(mount)?.is_none() {
            return Ok(Vec::new());
        }
        crate::build_volume::nx::volume_holders(mount)
            .map_err(|error| io_error("list a re-adoption source's holders", mount, error))
    }

    /// Move every input `source` replaces to its recorded retained path: main's image with its
    /// sidecar and companion, main's build volume with its record, and each old-incarnation
    /// seed with its record. A mounted image is unmounted and detached without force first; a
    /// holder refuses, named, and nothing of that image moves. The caller keeps the recorded
    /// build image's job-admission claim from before this step through canonical build
    /// publication; a claim scoped only to this rename would reopen admission during the copy.
    /// The kernel's refusal to unmount any held volume stays the final filesystem fence.
    pub(crate) fn retain_re_adopt_source(
        &self,
        source: &ReAdoptSource,
    ) -> Result<(), ApfsStorageError> {
        // A missing main retains no image: nothing of it is attached or registered.
        if let Some(image) = source.main.retained() {
            self.retain_image(
                source,
                &image.original,
                &image.retained,
                &MAIN_COMPANION_SUFFIXES,
            )?;
            // The kernel source has been released and moved. Its old cached main registration
            // must leave with it, otherwise staged publication sees a duplicate mounted main.
            // Do this only after successful retention: a busy-source refusal keeps its registration.
            let key = (
                source.metadata.repo_id.clone(),
                source.metadata.workspace.clone(),
            );
            if let Some(entry) = self.mounted.remove(key.clone())?
                && entry.attachment.image() != image.original.as_path()
            {
                let restored = self.mounted.restore(key, entry)?;
                if restored.is_err() {
                    return Err(ApfsStorageError::Host(
                        "main's mount registration changed during retained-source handoff"
                            .to_owned(),
                    ));
                }
                return Err(ApfsStorageError::Host(
                    "main's cached attachment does not name the recorded source image".to_owned(),
                ));
            }
        }
        // A build volume or seed whose image is at neither recorded path is lost: its cache
        // content is gone, and only its record, if any, moves to the retained name.
        let built = source
            .build
            .iter()
            .map(|build| (&build.image.original, &build.image.retained))
            .chain(
                source
                    .old_seeds
                    .iter()
                    .map(|seed| (&seed.original, &seed.retained)),
            );
        for (original, retained) in built {
            if exists(original)? || exists(retained)? {
                self.retain_image(source, original, retained, &[RECORD_SUFFIX])?;
            } else {
                self.retain_lost_record(original, retained)?;
            }
        }
        Ok(())
    }

    /// Move a lost build input's record beside its original to its retained name. A record
    /// already retained is a replay's finished step; one at both names refuses.
    fn retain_lost_record(&self, original: &Path, retained: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(original)?;
        self.verify_controller_path(retained)?;
        let from = append_suffix(original, RECORD_SUFFIX);
        if !regular_file_exists(&from)? {
            return Ok(());
        }
        let directory = retained
            .parent()
            .ok_or(ApfsStorageError::InvalidPlan("retained path has no parent"))?;
        fs::create_dir_all(directory)
            .map_err(|error| io_error("create the retained directory", directory, error))?;
        self.move_companions(&[(from, append_suffix(retained, RECORD_SUFFIX))])
    }

    /// The capacity the kernel's IOMedia mapping reports for attached `image`
    /// (01_storage.md, "Images": an old image's file length is not its virtual capacity).
    pub(crate) fn attached_capacity_bytes(&self, image: &Path) -> Result<u64, ApfsStorageError> {
        self.verify_controller_path(image)?;
        Ok(self.backend.attached_capacity(image)?.bytes())
    }

    /// A recorded retained main restored at the checkout is a recovery input, not an active
    /// workspace. Its actual backing-image/device join and read-only mount must both match.
    pub(crate) fn retained_re_adopt_main_is_mounted(
        &self,
        source: &ReAdoptSource,
    ) -> Result<bool, ApfsStorageError> {
        let Some(image) = source.main.retained() else {
            return Ok(false);
        };
        self.verify_controller_path(&image.retained)?;
        let input = retained_input(source, &image.retained)?;
        let Some(attachment) = self.backend.existing_retained_attachment(&input)? else {
            return Ok(false);
        };
        Ok(self.mount_source.mounts()?.into_iter().any(|mount| {
            mount.mount_point == source.checkout
                && mount.source_device == attachment.volume_device()
                && mount.access() == MountAccess::ReadOnly
        }))
    }

    /// Attach the retained main and build images read-only at their recorded staging mounts,
    /// each verified by the attach's fsck. An image already mounted there by its own volume is
    /// left as it is.
    pub(crate) fn mount_re_adopt_sources(
        &self,
        source: &ReAdoptSource,
    ) -> Result<(), ApfsStorageError> {
        if let Some(image) = source.main.retained() {
            self.mount_retained(
                source,
                &image.retained,
                &image.source_mount,
                &source.checkout,
            )?;
        }
        if let Some(build) = retained_build(source)? {
            let layout = BuildVolumeLayout::new(
                super::layout(&self.config, &source.metadata.repo_id)?.project(),
            )?;
            self.mount_retained(
                source,
                &build.image.retained,
                &build.image.source_mount,
                &layout.mount(&build.id),
            )?;
        }
        Ok(())
    }

    /// Detach the retained main and build images from their staging mounts once the
    /// replacement no longer reads them, without force, and remove the emptied mountpoints.
    pub(crate) fn release_re_adopt_sources(
        &self,
        source: &ReAdoptSource,
    ) -> Result<(), ApfsStorageError> {
        let mut images: Vec<_> = source
            .main
            .retained()
            .map(|image| (&image.retained, &image.source_mount))
            .into_iter()
            .collect();
        if let Some(build) = retained_build(source)? {
            images.push((&build.image.retained, &build.image.source_mount));
        }
        for (retained, mount) in images {
            self.release_retained(source, retained)?;
            remove_empty_directory(mount)?;
        }
        Ok(())
    }

    /// Main's build volume's replacement (02_workspaces.md, "Re-adoption" step 3): a fresh
    /// sparse RAW image at the recorded capacity, minted at its staging image path outside the
    /// build images collection reads, mounted at its staging mount, and filled from the retained
    /// volume's read-only mount by the same convergent copy and verification adopt uses. A
    /// replay resumes the copy on the image an earlier attempt left. Nothing for a re-adoption
    /// with no build volume. A lost build volume has nothing to copy from: whatever staged copy
    /// an earlier attempt left can no longer be verified against its source and is discarded,
    /// and the volume is published empty ([`Self::publish_re_adopt_build`]).
    pub(crate) fn copy_re_adopt_build(
        &self,
        source: &ReAdoptSource,
    ) -> Result<(), ApfsStorageError> {
        let Some(build) = &source.build else {
            return Ok(());
        };
        if retained_build(source)?.is_none() {
            return self.discard_staged_build(build);
        }
        let (new, old) = self.copy_replacement(
            &build.replacement_image,
            &build.replacement_mount,
            build.image.capacity_bytes,
            &build.image.source_mount,
            &build.image.retained,
        )?;
        eprintln!(
            "cowshed: re-adoption build replacement allocates {new} bytes; the retained image {old}"
        );
        Ok(())
    }

    /// Detach without force and delete a lost build volume's staged copy, then its emptied
    /// staging mountpoint. A copy already gone is a replay's finished step.
    fn discard_staged_build(&self, build: &ReAdoptBuild) -> Result<(), ApfsStorageError> {
        let staged = &build.replacement_image;
        self.verify_controller_path(staged)?;
        if exists(staged)? {
            self.detach_without_force(staged)?;
            fs::remove_file(staged)
                .map_err(|error| io_error("discard a lost build's staged copy", staged, error))?;
            sync_parent_path(staged)?;
            eprintln!(
                "cowshed: re-adoption discarded {}, a staged copy of a build volume whose retained \
                 image is gone",
                staged.display()
            );
        }
        remove_empty_directory(&build.replacement_mount)
    }

    /// Mint (or resume) the RAW replacement at `image`, mount it read-write at `mount`, copy the
    /// tree the read-only `source_mount` holds into it, and check its allocation against
    /// `capacity_bytes` (02_workspaces.md, "Re-adoption" steps 3-4). Answers the replacement's and
    /// the retained original's allocated bytes.
    fn copy_replacement(
        &self,
        image: &Path,
        mount: &Path,
        capacity_bytes: u64,
        source_mount: &Path,
        retained: &Path,
    ) -> Result<(u64, u64), ApfsStorageError> {
        self.verify_controller_path(image)?;
        if exists(image)? {
            self.mount_staged(image, mount, MountAccess::ReadWrite)?;
        } else {
            let directory = image.parent().ok_or(ApfsStorageError::InvalidPlan(
                "staged image path has no parent",
            ))?;
            fs::create_dir_all(directory)
                .map_err(|error| io_error("create the staged image directory", directory, error))?;
            self.mint(ImageCapacity::from_bytes(capacity_bytes), image, mount)?;
        }
        // Per-file hole preservation is the copy verifier's to prove, in the walk it already
        // makes; here only the image as a whole is checked against its nominal capacity.
        self.copy_tree(source_mount, mount)?;
        self.require_sparse_replacement(image, retained, capacity_bytes)
    }

    /// Re-adoption's allocation check (02_workspaces.md, "Re-adoption" step 4), by `st_blocks *
    /// 512`, never apparent length: a replacement that holds its nominal capacity in host blocks
    /// materialized the image instead of copying entries, and refuses publication. Answers the
    /// replacement's and the retained image's allocated bytes, for the report.
    pub(crate) fn require_sparse_replacement(
        &self,
        replacement: &Path,
        retained: &Path,
        capacity_bytes: u64,
    ) -> Result<(u64, u64), ApfsStorageError> {
        let allocated = |path: &Path| {
            fs::metadata(path)
                .map(|metadata| allocated_file_bytes(&metadata))
                .map_err(|error| io_error("read a re-adoption image's allocation", path, error))
        };
        let new = allocated(replacement)?;
        let old = allocated(retained)?;
        if new >= capacity_bytes {
            return Err(ApfsStorageError::Host(format!(
                "re-adoption replacement {} allocates {new} bytes, its whole {capacity_bytes}-byte \
                 capacity: the copy materialized the image instead of preserving its holes; nothing \
                 was published and {} is retained",
                replacement.display(),
                retained.display()
            )));
        }
        Ok((new, old))
    }

    /// Publish main's build volume for the replacement: the verified build replacement under
    /// main's build volume identity (detached without force, renamed to the build image path the
    /// retained volume left, its recorded owner record written verbatim, mounted where main's
    /// build link points; 16_build_volumes.md, "Targets and seeds"); for a missing main, its
    /// surviving build volume kept as it is, or else the recorded build volume empty; for a lost
    /// build volume (its retained image gone), the recorded build volume empty
    /// ([`Self::publish_empty_build`]). Then freeze one seed of it for the replacement
    /// incarnation; the old incarnation's seeds stay retained. Converges from whichever step a
    /// crash interrupted. `build_claim` is the exclusive claim on the recorded source build
    /// volume's hold, held by the caller across this whole publication: present exactly when a
    /// retained main records a build volume, and naming that volume. A missing main records no
    /// source build, so it has none.
    pub(crate) fn publish_re_adopt_build(
        &self,
        layout: &BuildVolumeLayout,
        source: &ReAdoptSource,
        build_claim: Option<&ReleaseClaim>,
    ) -> Result<(), ApfsStorageError> {
        match (&source.main, &source.build, build_claim) {
            (ReAdoptMain::Missing(missing), None, None) => match &missing.build {
                Some(MissingMainBuild::Empty(build)) => self.publish_empty_build(
                    layout,
                    source,
                    build,
                    SeedSource::Unclaimed(&build.id),
                ),
                // Main's own volume outlived main's image: seed it before mounting it where
                // main's build link points, under its own claim.
                Some(MissingMainBuild::Kept(build)) => self.seed_replacement(
                    layout,
                    source,
                    SeedSource::Unclaimed(&build.kept),
                    &build.record,
                ),
                None => Ok(()),
            },
            (ReAdoptMain::Retained(_), None, None) => Ok(()),
            (ReAdoptMain::Retained(_), Some(build), Some(claim)) if claim.id() == &build.id => {
                let seed = SeedSource::of_published(layout, build, claim);
                // Once published, the replacement no longer reads its retained input: losing
                // that image afterwards changes nothing, and the replay converges on what it
                // published.
                if retained_build(source)?.is_some() || replacement_published(layout, build)? {
                    self.publish_build_replacement(layout, build)?;
                    return self.seed_replacement(layout, source, seed, &build.owner_record);
                }
                // Lost before publication. Copy is not run again once main is published, so its
                // staged copy, which nothing can verify any more, is discarded here too.
                self.discard_staged_build(build)?;
                let lost = ReAdoptEmptyBuild {
                    id: build.id.clone(),
                    record: BuildVolumeRecord {
                        tree: None,
                        ..build.owner_record.clone()
                    },
                    capacity_bytes: build.image.capacity_bytes,
                };
                self.publish_empty_build(layout, source, &lost, seed)
            }
            _ => Err(ApfsStorageError::InvalidPlan(
                "the build claim does not hold the recorded build volume",
            )),
        }
    }

    /// A build volume published empty (02_workspaces.md, "Re-adoption" step 3 and "Missing main
    /// image"): an empty RAW volume minted under the recorded id at the recorded capacity and
    /// mounted where main's build link points, its owner record written last, then one seed of
    /// it for the replacement incarnation, captured under `seed`'s claim. Converges from an
    /// attempt interrupted after the image was minted and before its record was written. An
    /// image holding another record, or a record that names no image, refuses: nothing is
    /// published over either.
    fn publish_empty_build(
        &self,
        layout: &BuildVolumeLayout,
        source: &ReAdoptSource,
        build: &ReAdoptEmptyBuild,
        seed: SeedSource<'_>,
    ) -> Result<(), ApfsStorageError> {
        let image = layout.image(&build.id);
        self.verify_controller_path(&image)?;
        let recorded = layout
            .read_record_present(&build.id)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        match (exists(&image)?, recorded) {
            (false, None) => {
                self.create_build_volume(
                    layout,
                    &build.id,
                    ImageCapacity::from_bytes(build.capacity_bytes),
                )?;
            }
            (true, None) => {
                self.mount_build_volume(layout, &build.id)?;
            }
            (true, Some(found)) if found == build.record => {
                self.mount_build_volume(layout, &build.id)?;
            }
            (true, Some(_)) => {
                return Err(ApfsStorageError::InvalidPlan(
                    "main's build volume image holds another owner record; refusing to publish over it",
                ));
            }
            (false, Some(_)) => {
                return Err(ApfsStorageError::InvalidPlan(
                    "main's build volume record names a missing image; refusing to publish over it",
                ));
            }
        }
        layout
            .write_record(&build.id, &build.record)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        self.seed_replacement(layout, source, seed, &build.record)
    }

    /// Freeze one seed of main's published build volume, then mount the live volume where main's
    /// link points. An already-detached source is captured before its first mount, not mounted
    /// merely to detach and restore it for capture. Admission stays closed through the new seed's
    /// post-mount freshness mark. A seed an earlier attempt froze keeps its original mark.
    fn seed_replacement(
        &self,
        layout: &BuildVolumeLayout,
        source: &ReAdoptSource,
        seed: SeedSource<'_>,
        owner: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        let acquired;
        let claim = match seed {
            SeedSource::Claimed(claim) => claim,
            SeedSource::Unclaimed(id) => {
                acquired = self.claim_capture_source(layout, id)?;
                &acquired
            }
        };
        let seeded = layout
            .seed_of(&crate::metadata::WorkspaceName::main(), &source.replacement)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        if seeded.is_some() {
            self.mount_build_volume(layout, claim.id())?;
            return Ok(());
        }
        let destination = BuildVolumeId::mint();
        let record = BuildVolumeRecord::new(
            owner.tree.clone(),
            BuildVolumeRole::Seed {
                target: crate::metadata::WorkspaceName::main(),
                incarnation: source.replacement.clone(),
            },
        );
        self.clone_claimed_build_volume(layout, claim, &destination, &record)?;
        self.mount_build_volume(layout, claim.id())?;
        let written = self.build_volume_written(layout, claim.id())?;
        self.mark_seed_written(layout, &destination, written)
    }

    /// Publish a verified build replacement under its build volume identity: detach it without
    /// force (an attached image is never renamed), rename it to the build image path the retained
    /// volume left, and write the recorded owner record verbatim. Leave it detached so
    /// `seed_replacement` can capture it before mounting (16_build_volumes.md, "Targets and seeds").
    /// Converges from whichever step a crash interrupted.
    fn publish_build_replacement(
        &self,
        layout: &BuildVolumeLayout,
        build: &ReAdoptBuild,
    ) -> Result<(), ApfsStorageError> {
        let published = layout.image(&build.id);
        // The replacement is RAW at the build image path of its build volume identity, whatever
        // format the retained input was.
        if build
            .image
            .original
            .with_extension(cowshed_apfs::image::IMAGE_EXTENSION)
            != published
        {
            return Err(ApfsStorageError::InvalidPlan(
                "the recorded build image is not its build volume path in this layout",
            ));
        }
        match (exists(&build.replacement_image)?, exists(&published)?) {
            (true, true) => {
                return Err(ApfsStorageError::InvalidPlan(
                    "the build image path is occupied while its replacement is unpublished",
                ));
            }
            (true, false) => {
                self.detach_without_force(&build.replacement_image)?;
                remove_empty_directory(&build.replacement_mount)?;
                rename_exclusive(&build.replacement_image, &published).map_err(|error| {
                    io_error(
                        "publish the build replacement",
                        &build.replacement_image,
                        error,
                    )
                })?;
                sync_parent_path(&published)?;
                sync_parent_path(&build.replacement_image)?;
            }
            // Published by an earlier attempt.
            (false, true) => {}
            // No payload anywhere: never write a record that names nothing.
            (false, false) => {
                return Err(ApfsStorageError::InvalidPlan(
                    "the build replacement is neither staged nor published; nothing was recorded",
                ));
            }
        }
        layout
            .write_record(&build.id, &build.owner_record)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }

    fn retain_image<const N: usize>(
        &self,
        source: &ReAdoptSource,
        original: &Path,
        retained: &Path,
        companions: &[&str; N],
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(original)?;
        self.verify_controller_path(retained)?;
        let moves: [(PathBuf, PathBuf); N] = std::array::from_fn(|index| {
            (
                append_suffix(original, companions[index]),
                append_suffix(retained, companions[index]),
            )
        });
        if !exists(original)? {
            if !exists(retained)? {
                return Err(ApfsStorageError::InvalidPlan(
                    "a re-adopted image is neither at its original nor at its retained path",
                ));
            }
            // The image moved; finish any companion a crash left behind.
            return self.move_companions(&moves);
        }
        if exists(retained)? {
            return Err(ApfsStorageError::InvalidPlan(
                "a retained re-adoption path already exists beside its original; refusing to overwrite it",
            ));
        }
        self.release_retained(source, original)?;
        // Companions first: an image whose sidecar moved is replayed by its intent, while a
        // sidecar whose image moved would describe a missing payload.
        let directory = retained
            .parent()
            .ok_or(ApfsStorageError::InvalidPlan("retained path has no parent"))?;
        fs::create_dir_all(directory)
            .map_err(|error| io_error("create the retained directory", directory, error))?;
        self.move_companions(&moves)?;
        rename_exclusive(original, retained)
            .map_err(|error| io_error("retain the replaced image", original, error))?;
        sync_parent_path(retained)?;
        sync_parent_path(original)
    }

    /// Move each required companion to its retained name. Each must be at exactly one of the two:
    /// absent from both, the replaced input was incomplete, and nothing stands in for it.
    fn move_companions<const N: usize>(
        &self,
        moves: &[(PathBuf, PathBuf); N],
    ) -> Result<(), ApfsStorageError> {
        // Every name is read before the first rename, so a companion absent from both names, present
        // at both, or not a regular file moves nothing.
        let mut beside_original = [false; N];
        for ((from, to), present) in moves.iter().zip(beside_original.iter_mut()) {
            *present = match (regular_file_exists(from)?, regular_file_exists(to)?) {
                (true, true) => {
                    return Err(ApfsStorageError::InvalidPlan(
                        "a retained companion already exists beside its original; refusing to overwrite it",
                    ));
                }
                (false, false) => {
                    return Err(ApfsStorageError::Host(format!(
                        "the replaced image's required companion {} is missing, and nothing was \
                         retained in its place",
                        from.display()
                    )));
                }
                (beside_original_only, _) => beside_original_only,
            };
        }
        for ((from, to), present) in moves.iter().zip(beside_original) {
            // Only a companion still beside its original moves; one already retained is a replay's
            // finished step.
            if present {
                rename_exclusive(from, to).map_err(|error| {
                    io_error("retain the replaced image's companion", from, error)
                })?;
                sync_parent_path(to)?;
                sync_parent_path(from)?;
            }
        }
        Ok(())
    }

    /// Unmount and detach `image` without force. A refusal of a mounted image names the
    /// processes that hold it.
    fn detach_without_force(&self, image: &Path) -> Result<(), ApfsStorageError> {
        let Some(attachment) = self.backend.existing_attachment(image)? else {
            return Ok(());
        };
        let mount = self
            .mount_source
            .mounts()?
            .into_iter()
            .find(|mounted| mounted.source_device == attachment.volume_device())
            .map(|mounted| mounted.mount_point);
        let released = self
            .backend
            .unmount_verified(&attachment, DetachIntent::WhenIdle)
            .and_then(|()| self.backend.detach(&attachment, DetachIntent::WhenIdle));
        match (released, mount) {
            (Ok(()), _) => Ok(()),
            (Err(refusal), Some(mount)) => Err(self.capture_refusal(image, &mount, refusal)),
            (Err(refusal), None) => Err(refusal.into()),
        }
    }

    /// Unmount and detach the re-adoption input at `path` (one `source` records, in whatever
    /// format it was written) without force, through the retained-input attachment API. A
    /// refusal of a mounted input names the processes that hold it.
    fn release_retained(
        &self,
        source: &ReAdoptSource,
        path: &Path,
    ) -> Result<(), ApfsStorageError> {
        let input = retained_input(source, path)?;
        let Some(attachment) = self.backend.existing_retained_attachment(&input)? else {
            return Ok(());
        };
        let mount = self
            .mount_source
            .mounts()?
            .into_iter()
            .find(|mounted| mounted.source_device == attachment.volume_device())
            .map(|mounted| mounted.mount_point);
        let released = self
            .backend
            .unmount_retained_when_idle(&attachment)
            .and_then(|()| self.backend.detach_retained_when_idle(&attachment));
        match (released, mount) {
            (Ok(()), _) => Ok(()),
            (Err(refusal), Some(mount)) => Err(self.capture_refusal(path, &mount, refusal)),
            (Err(refusal), None) => Err(refusal.into()),
        }
    }

    /// Attach the retained input at `path` read-only and mount it at `mount`. An exact read-only
    /// staging mount is kept. An input restored read-only at `canonical`, where it was mounted
    /// before it was retained, is moved back without force; every foreign or read-write mount
    /// still refuses.
    fn mount_retained(
        &self,
        source: &ReAdoptSource,
        path: &Path,
        mount: &Path,
        canonical: &Path,
    ) -> Result<(), ApfsStorageError> {
        let input = retained_input(source, path)?;
        if let Some(attachment) = self.backend.existing_retained_attachment(&input)? {
            let mounted = self
                .mount_source
                .mounts()?
                .into_iter()
                .find(|mounted| mounted.source_device == attachment.volume_device());
            match mounted {
                Some(mounted)
                    if mounted.mount_point == mount
                        && mounted.access() == MountAccess::ReadOnly =>
                {
                    return Ok(());
                }
                Some(mounted) if mounted.access() == MountAccess::ReadOnly => {
                    if mounted.mount_point != canonical {
                        return Err(ApfsStorageError::InvalidPlan(
                            "a retained input is mounted outside its recorded canonical or staging path",
                        ));
                    }
                    self.backend.unmount_retained_when_idle(&attachment)?;
                }
                Some(_) => {
                    return Err(ApfsStorageError::InvalidPlan(
                        "a retained source is mounted somewhere other than its recorded read-only staging mount",
                    ));
                }
                None => {}
            }
            return self
                .backend
                .mount_retained_read_only(&attachment, mount)
                .map_err(Into::into);
        }
        self.backend
            .attach_retained_mounted(&input, mount)
            .map(|_| ())
            .map_err(Into::into)
    }

    /// Mount `image` at `mount` with `access`, attaching and verifying it first. An image
    /// already mounted exactly there with that access is left as it is; one attached without a
    /// mount is checked and mounted by Disk Arbitration without detach/reattach churn.
    fn mount_staged(
        &self,
        image: &Path,
        mount: &Path,
        access: MountAccess,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(image)?;
        if let Some(attachment) = self.backend.existing_attachment(image)? {
            let mounted = self
                .mount_source
                .mounts()?
                .into_iter()
                .find(|mounted| mounted.source_device == attachment.volume_device());
            match mounted {
                Some(mounted) if mounted.mount_point == mount && mounted.access() == access => {
                    return Ok(());
                }
                Some(_) => {
                    return Err(ApfsStorageError::InvalidPlan(
                        "a re-adoption staged image is mounted somewhere other than its recorded staging mount",
                    ));
                }
                None => {}
            }
            return self
                .backend
                .mount(&attachment, mount, access, false)
                .map_err(Into::into);
        }
        self.backend
            .attach_mounted(image, mount, access, false)
            .map(|_| ())
            .map_err(Into::into)
    }
}

/// `path` as the retained-input type the backend's re-adoption-only attachment API takes: one
/// of exactly the images `source` records.
fn retained_input<'a>(
    source: &'a ReAdoptSource,
    path: &'a Path,
) -> Result<RetainedInput<'a>, ApfsStorageError> {
    source
        .retained_input(path)
        .map_err(|error| ApfsStorageError::Host(error.to_string()))
}

/// Rename `from` to `to`, refusing with `EEXIST` if `to` exists at the instant of the rename:
/// retained evidence and a published build image are never overwritten (02_workspaces.md,
/// "Re-adoption"). Both paths are absolute, so the directory descriptor the kernel would resolve
/// relative names against is never consulted.
fn rename_exclusive(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    if !from.is_absolute() || !to.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "re-adoption renames name absolute paths only",
        ));
    }
    let name = |path: &Path| {
        std::ffi::CString::new(path.as_os_str().as_bytes())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))
    };
    cowshed_os::fsio::rename_noreplace(libc::AT_FDCWD, &name(from)?, &name(to)?)
}

fn exists(path: &Path) -> Result<bool, ApfsStorageError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect a re-adoption path", path, error)),
    }
}

/// Whether a companion is present at `path`. Only a regular file counts: a link, a directory or any
/// other entry at a companion name is refused, never moved or read through.
fn regular_file_exists(path: &Path) -> Result<bool, ApfsStorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(ApfsStorageError::Host(format!(
            "a re-adoption companion is not a regular file: {}",
            path.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect a re-adoption path", path, error)),
    }
}

fn remove_empty_directory(path: &Path) -> Result<(), ApfsStorageError> {
    match fs::remove_dir(path) {
        Ok(()) => sync_parent_path(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(
            "remove a re-adoption staging mountpoint",
            path,
            error,
        )),
    }
}

/// Main's recorded build volume while its retained image is there to copy from. Retention moves
/// a build volume's image to its retained name before anything reads it, so past retention a
/// recorded build volume with no retained image is lost (02_workspaces.md, "Re-adoption" step 3):
/// `None`, and, unless its replacement was already published ([`replacement_published`]), it is
/// published empty.
fn retained_build(source: &ReAdoptSource) -> Result<Option<&ReAdoptBuild>, ApfsStorageError> {
    match &source.build {
        Some(build) if exists(&build.image.retained)? => Ok(Some(build)),
        Some(_) | None => Ok(None),
    }
}

/// Whether an earlier attempt published `build`'s verified replacement: no staged copy is left,
/// and the build image path holds an image whose record is the recorded owner record verbatim.
/// An empty volume published for a lost build carries a record with its built-at tree cleared,
/// which is the owner record only when that had no tree either, and then the two are the same.
fn replacement_published(
    layout: &BuildVolumeLayout,
    build: &ReAdoptBuild,
) -> Result<bool, ApfsStorageError> {
    if exists(&build.replacement_image)? || !exists(&layout.image(&build.id))? {
        return Ok(false);
    }
    let recorded = layout
        .read_record_present(&build.id)
        .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
    Ok(recorded.as_ref() == Some(&build.owner_record))
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
    use crate::storage::StorageLayout;
    use crate::storage::apfs::ApfsSubstrateConfig;
    use crate::storage::recovery::{
        ReAdoptKeptBuild, ReAdoptMissingMain, preserved_checkout, re_adopt_tests::Fixture,
    };
    use cowshed_apfs::{
        AttachedDiskImage, CommandOutput, CommandRequest, CommandRunError, CommandRunFailure,
    };
    use cowshed_os::fork_lock::Fenced;

    struct RefuseFirstMount {
        layout: BuildVolumeLayout,
        id: BuildVolumeId,
        incarnation: WorkspaceIncarnation,
    }

    impl CommandRunner for RefuseFirstMount {
        fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
            assert_eq!(request.program, Path::new("/usr/sbin/diskutil"));
            assert_eq!(request.args[0], "image");
            assert_eq!(request.args[1], "attach");
            assert!(
                self.layout
                    .seed_of(&WorkspaceName::main(), &self.incarnation)
                    .unwrap()
                    .is_some(),
                "seed the detached build before its first attach, not after a mount/detach cycle"
            );
            assert_eq!(
                self.layout.hold(&self.id).unwrap_err().kind(),
                io::ErrorKind::ResourceBusy,
                "job admission stays closed through the mount and freshness mark"
            );
            Err(CommandRunError {
                request: request.clone(),
                failure: CommandRunFailure::Spawn(io::Error::other("injected attach refusal")),
            })
        }

        fn run_guarded<E>(
            &self,
            request: &CommandRequest,
            check: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Option<CommandOutput>, E>
        where
            E: From<CommandRunError> + std::fmt::Display,
        {
            if check()? {
                self.run(request).map(Some).map_err(E::from)
            } else {
                Ok(None)
            }
        }

        fn image_lease(&self, _: &Path) -> io::Result<Option<Fenced<fs::File>>> {
            Ok(None)
        }

        fn pin_raw_device(&self, _: &Path) -> io::Result<Option<Fenced<fs::File>>> {
            panic!("the refused attach never yields a device")
        }

        fn attached_disk_images(&self) -> io::Result<Vec<AttachedDiskImage>> {
            Ok(Vec::new())
        }

        fn grow_image(&self, _: &Path, _: ImageCapacity) -> io::Result<()> {
            panic!("seed publication never grows its source")
        }
    }

    fn seed_before_mount(kept: bool) {
        let fixture = Fixture::new();
        let build = fixture.build();
        let mut source = fixture.source(Some(build.clone()));
        let storage = StorageLayout::new(fixture.store(), &source.metadata.repo_id).unwrap();
        let layout = BuildVolumeLayout::new(storage.project()).unwrap();
        fs::create_dir_all(layout.images()).unwrap();
        let bytes = [0x5a; 4096];
        fs::write(layout.image(&build.id), bytes).unwrap();
        layout.write_record(&build.id, &build.owner_record).unwrap();
        let owner = fs::read(layout.record(&build.id)).unwrap();
        let claim = if kept {
            source.main = ReAdoptMain::Missing(ReAdoptMissingMain {
                canonical: fixture.main_image().original,
                preserved: preserved_checkout(&source.checkout, &source.replacement).unwrap(),
                capacity_bytes: fixture.main_image().capacity_bytes,
                build: Some(MissingMainBuild::Kept(ReAdoptKeptBuild {
                    kept: build.id.clone(),
                    record: build.owner_record.clone(),
                })),
            });
            source.build = None;
            None
        } else {
            Some(layout.claim_release(&build.id).unwrap().unwrap())
        };
        let host = MacOsApfsExecutionHost::new(
            RefuseFirstMount {
                layout: layout.clone(),
                id: build.id.clone(),
                incarnation: source.replacement.clone(),
            },
            ApfsSubstrateConfig::new(fixture.store(), &fixture.checkout),
        )
        .unwrap();

        let publish = || {
            let error = host
                .publish_re_adopt_build(&layout, &source, claim.as_ref())
                .expect_err("the injected mount failure remains an error");
            assert!(
                error.to_string().contains("injected attach refusal"),
                "{error}"
            );
            layout
                .seed_of(&WorkspaceName::main(), &source.replacement)
                .unwrap()
                .expect("capture happened before the refused mount")
        };
        let (seed, record) = publish();
        let seed_time = fs::metadata(layout.image(&seed))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(fs::read(layout.image(&seed)).unwrap(), bytes);
        assert_eq!(record.tree, build.owner_record.tree);
        assert_eq!(publish(), (seed.clone(), record));
        assert_eq!(
            fs::metadata(layout.image(&seed))
                .unwrap()
                .modified()
                .unwrap(),
            seed_time,
            "a replay never relabels an existing seed as freshly captured"
        );
        assert_eq!(fs::read(layout.image(&build.id)).unwrap(), bytes);
        assert_eq!(fs::read(layout.record(&build.id)).unwrap(), owner);
        drop(claim);
        layout.hold(&build.id).expect("refusal releases admission");
    }

    #[test]
    fn a_kept_detached_build_is_seeded_before_mount_under_one_claim() {
        seed_before_mount(true);
    }

    #[test]
    fn a_published_detached_build_is_seeded_before_mount_under_the_borrowed_claim() {
        seed_before_mount(false);
    }
}
