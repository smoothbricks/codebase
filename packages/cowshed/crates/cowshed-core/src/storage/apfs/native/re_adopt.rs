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
use crate::apfs::{ApfsBackend, CommandRunner, DetachIntent, MountAccess, RetainedInput};
use crate::build_volume::{
    BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord, BuildVolumeRole, RECORD_SUFFIX,
    ReleaseClaim,
};
use crate::metadata::{ImageCapacity, append_suffix};
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError, COMPANION_SUFFIX};
use crate::storage::recovery::{
    MissingMainBuild, ReAdoptBuild, ReAdoptEmptyBuild, ReAdoptMain, ReAdoptSource,
};

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
        if let Some(build) = &source.build {
            self.retain_image(
                source,
                &build.image.original,
                &build.image.retained,
                &[RECORD_SUFFIX],
            )?;
        }
        for seed in &source.old_seeds {
            self.retain_image(source, &seed.original, &seed.retained, &[RECORD_SUFFIX])?;
        }
        Ok(())
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
        if let Some(build) = &source.build {
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
        if let Some(build) = &source.build {
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
    /// with no build volume.
    pub(crate) fn copy_re_adopt_build(
        &self,
        source: &ReAdoptSource,
    ) -> Result<(), ApfsStorageError> {
        let Some(build) = &source.build else {
            return Ok(());
        };
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
            let attachment = self.mint(ImageCapacity::from_bytes(capacity_bytes), image)?;
            if let Err(primary) =
                self.backend
                    .mount(&attachment, mount, MountAccess::ReadWrite, false)
            {
                return Err(
                    match self.backend.detach(&attachment, DetachIntent::WhenIdle) {
                        Ok(()) => primary.into(),
                        Err(cleanup) => ApfsStorageError::Cleanup {
                            operation: "mount a replacement image",
                            primary: Box::new(primary.into()),
                            cleanup: Box::new(cleanup.into()),
                        },
                    },
                );
            }
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
    /// surviving build volume kept as it is, or else the recorded build volume empty
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
                Some(MissingMainBuild::Empty(build)) => self.publish_empty_build(layout, source, build),
                // Main's own volume outlived main's image: it is mounted where main's build link
                // points and seeded as it is, under its own claim.
                Some(MissingMainBuild::Kept(build)) => {
                    self.mount_build_volume(layout, &build.kept)?;
                    self.seed_replacement(
                        layout,
                        source,
                        SeedSource::Unclaimed(&build.kept),
                        &build.record,
                    )
                }
                None => Ok(()),
            },
            (ReAdoptMain::Retained(_), None, None) => Ok(()),
            (ReAdoptMain::Retained(_), Some(build), Some(claim)) if claim.id() == &build.id => {
                self.publish_build_replacement(layout, build)?;
                let seed = SeedSource::of_published(layout, build, claim);
                self.seed_replacement(layout, source, seed, &build.owner_record)
            }
            _ => Err(ApfsStorageError::InvalidPlan(
                "the build claim does not hold the recorded build volume",
            )),
        }
    }

    /// A missing main's build volume (02_workspaces.md, "Missing main image"): an empty RAW
    /// volume minted under the recorded id at the recorded capacity and mounted where main's
    /// build link points, its owner record written last, then one seed of it for the
    /// replacement incarnation. Converges from an attempt interrupted after the image was minted
    /// and before its record was written. An image holding another record, or a record that
    /// names no image, refuses: nothing is published over either. Nothing else claims a volume
    /// minted here, so its seed takes the volume's own ordinary claim.
    fn publish_empty_build(
        &self,
        layout: &BuildVolumeLayout,
        source: &ReAdoptSource,
        build: &ReAdoptEmptyBuild,
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
        self.seed_replacement(
            layout,
            source,
            SeedSource::Unclaimed(&build.id),
            &build.record,
        )
    }

    /// Freeze one seed of main's published build volume for the replacement incarnation, under
    /// the claim `seed` says it is captured with; the old incarnation's seeds stay retained. A
    /// seed an earlier attempt froze is kept.
    fn seed_replacement(
        &self,
        layout: &BuildVolumeLayout,
        source: &ReAdoptSource,
        seed: SeedSource<'_>,
        owner: &BuildVolumeRecord,
    ) -> Result<(), ApfsStorageError> {
        let seeded = layout
            .seed_of(&crate::metadata::WorkspaceName::main(), &source.replacement)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        if seeded.is_none() {
            let destination = BuildVolumeId::mint();
            let record = BuildVolumeRecord::new(
                owner.tree.clone(),
                BuildVolumeRole::Seed {
                    target: crate::metadata::WorkspaceName::main(),
                    incarnation: source.replacement.clone(),
                },
            );
            match seed {
                SeedSource::Claimed(claim) => {
                    self.clone_claimed_build_volume(layout, claim, &destination, &record)?;
                }
                SeedSource::Unclaimed(id) => {
                    self.clone_build_volume(layout, id, &destination, &record)?;
                }
            }
        }
        Ok(())
    }

    /// Publish a verified build replacement under its build volume identity: detach it without
    /// force (an attached image is never renamed), rename it to the build image path the retained
    /// volume left, write the recorded owner record verbatim, and mount it where the build link
    /// points (16_build_volumes.md, "Targets and seeds"). Converges from whichever step a crash
    /// interrupted.
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
            .with_extension(crate::metadata::IMAGE_EXTENSION)
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
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        self.mount_build_volume(layout, &build.id)?;

        Ok(())
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
                    self.backend.detach_retained_when_idle(&attachment)?;
                }
                Some(_) => {
                    return Err(ApfsStorageError::InvalidPlan(
                        "a retained source is mounted somewhere other than its recorded read-only staging mount",
                    ));
                }
                None => self.backend.detach_retained_when_idle(&attachment)?,
            }
        }
        let attachment = self.backend.attach_retained_verified(&input)?;
        if let Err(primary) = self.backend.mount_retained_read_only(&attachment, mount) {
            return Err(match self.backend.detach_retained_when_idle(&attachment) {
                Ok(()) => primary.into(),
                Err(cleanup) => ApfsStorageError::Cleanup {
                    operation: "mount a retained source read-only",
                    primary: Box::new(primary.into()),
                    cleanup: Box::new(cleanup.into()),
                },
            });
        }
        Ok(())
    }

    /// Mount `image` at `mount` with `access`, attaching and verifying it first. An image
    /// already mounted exactly there with that access is left as it is; one attached without a
    /// mount (a crash between attach and mount) is detached and verified afresh.
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
                // Attached without a mount: a replay finds the attach a crash left, verified
                // afresh rather than trusted.
                None => self.backend.detach(&attachment, DetachIntent::WhenIdle)?,
            }
        }
        let attachment = self.backend.attach_verified(image)?;
        if let Err(primary) = self.backend.mount(&attachment, mount, access, false) {
            return match self.backend.detach(&attachment, DetachIntent::WhenIdle) {
                Ok(()) => Err(primary.into()),
                Err(cleanup) => Err(ApfsStorageError::Cleanup {
                    operation: "mount a re-adoption staged image",
                    primary: Box::new(primary.into()),
                    cleanup: Box::new(cleanup.into()),
                }),
            };
        }
        Ok(())
    }
}

/// `path` as the retained-input type the backend's re-adoption-only attachment API takes: one
/// of exactly the images `source` records.
fn retained_input<'a>(
    source: &'a ReAdoptSource,
    path: &'a Path,
) -> Result<RetainedInput<'a>, ApfsStorageError> {
    RetainedInput::from_record(source, path)
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
    crate::fsio::rename_noreplace(libc::AT_FDCWD, &name(from)?, &name(to)?)
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
