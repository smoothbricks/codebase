//! The store's blank templates (01_storage.md, "Images"): one formatted, verified, detached ASIF
//! image per capacity and owner, from which every new image is cloned. A mint is then one
//! `clonefile` and one attach. The `diskutil image create`, the formatting attach, the
//! `newfs_apfs` and the detach are paid once per store and capacity, here, instead of once per
//! image — and the create and the attach queue on `storagekitd` (01_storage.md, "How the APFS
//! host degrades").

use std::fs;
use std::path::{Path, PathBuf};

use super::{MacOsApfsExecutionHost, acquire_image_locks, path_exists, sync_parent_path};
use crate::apfs::{
    ApfsBackend, ApfsError, AttachedImage, CommandRunner, CreateImageRequest, DetachIntent,
    MacOsApfsBackend, RecoveredImageAttachment, apfs_step_leg, timed_apfs_step,
};
use crate::metadata::{IMAGE_EXTENSION, ImageCapacity};
use crate::storage::apfs::{ApfsStorageError, LockMode};
use crate::storage::verify_no_symlinks;

/// The store-root namespace the templates live in. Dot-prefixed, so no store walker takes it for
/// a repository owner (`is_reserved_store_namespace`).
const BLANK_TEMPLATE_DIRECTORY: &str = ".blank";

/// The label every template's volume carries, and so every image minted from one until its
/// workspace's supervisor relabels it (`relabel_off_the_path`): labels are human-facing only, and
/// a relabel queues on Disk Arbitration (01_storage.md, "Ownership, identity, and the volume
/// label").
pub const BLANK_TEMPLATE_LABEL: &str = "[cowshed]";

/// `<store>/.blank/<bytes>-<uid>-<gid>.asif`: the template a mint at `capacity` clones for the
/// invoking user. The owner is part of the name because `newfs_apfs -U/-G` writes it into the
/// volume root, and every clone inherits it.
pub fn blank_template_path(store_root: &Path, capacity: ImageCapacity) -> PathBuf {
    template_file(store_root, capacity, "").with_extension(IMAGE_EXTENSION)
}

/// `<store>/.blank/<bytes>-<uid>-<gid>-minting`: where the template is written, formatted and
/// detached before it takes its name, as `<stem>.asif`.
pub fn blank_template_staged_stem(store_root: &Path, capacity: ImageCapacity) -> PathBuf {
    template_file(store_root, capacity, "-minting")
}

fn template_file(store_root: &Path, capacity: ImageCapacity, suffix: &str) -> PathBuf {
    let (uid, gid) = owner();
    store_root
        .join(BLANK_TEMPLATE_DIRECTORY)
        .join(format!("{}-{uid}-{gid}{suffix}", capacity.bytes()))
}

fn owner() -> (u32, u32) {
    // SAFETY: `getuid`/`getgid` read this process's credentials; they take no pointers and
    // cannot fail.
    unsafe { (libc::getuid(), libc::getgid()) }
}

impl<R: CommandRunner> MacOsApfsExecutionHost<R> {
    /// Mint `image`, which must not exist: one `clonefile` of the store's template at `capacity`
    /// and one attach, handed back verified and pinned but not mounted. The clone shares the
    /// template's volume and container UUIDs, as every clone shares its source's, and carries its
    /// label. A failed attach removes the clone.
    pub(super) fn mint(
        &self,
        capacity: ImageCapacity,
        image: &Path,
    ) -> Result<AttachedImage, ApfsStorageError> {
        let leg = apfs_step_leg(image);
        timed_apfs_step(leg, "mint", || {
            let template = blank_template(&self.backend, &self.config.store_root, capacity)?;
            timed_apfs_step(leg, "clonefile", || {
                self.backend
                    .clone_image(&template, image)
                    .map_err(ApfsError::from)
            })?;
            sync_parent_path(image)?;
            self.backend.attach_verified(image).or_else(|primary| {
                super::super::combine_cleanup(
                    "attach minted image",
                    primary.into(),
                    self.backend.delete_image(image).map_err(Into::into),
                )
            })
        })
    }
}

/// The store's template at `capacity`, minted first if the store has none yet.
///
/// A template only ever appears under its name complete, formatted, verified (`fsck_apfs -q`) and
/// detached: it is minted under a staging name of its own and renamed into place after the
/// detach. So an existing template needs no lock and no check — a mint clones it on sight. Only
/// minting takes the template's lock, and a minter that finds the template once it holds the lock
/// returns it: concurrent first mints make one template. A minter killed partway leaves its
/// staging image, perhaps still attached; the next minter releases and removes it first.
pub fn blank_template<R: CommandRunner>(
    backend: &MacOsApfsBackend<R>,
    store_root: &Path,
    capacity: ImageCapacity,
) -> Result<PathBuf, ApfsStorageError> {
    let template = blank_template_path(store_root, capacity);
    verify_no_symlinks(store_root, &template)?;
    if path_exists(&template)? {
        return Ok(template);
    }
    let _minting = acquire_image_locks(
        store_root,
        &[template.with_extension("lock")],
        LockMode::Wait,
    )?
    .ok_or(ApfsStorageError::InvalidPlan(
        "a waiting lock acquisition answered busy",
    ))?;
    if path_exists(&template)? {
        return Ok(template);
    }
    timed_apfs_step("template", "mint", || {
        mint(
            backend,
            &blank_template_staged_stem(store_root, capacity),
            capacity,
        )
        .and_then(|minted| rename_into_place(backend, &minted, &template))
    })?;
    Ok(template)
}

/// Create, format, verify and detach a fresh template at `staged_stem.asif`, replacing whatever
/// a killed minter left there.
fn mint<R: CommandRunner>(
    backend: &MacOsApfsBackend<R>,
    staged_stem: &Path,
    capacity: ImageCapacity,
) -> Result<PathBuf, ApfsStorageError> {
    let (owner_uid, owner_gid) = owner();
    let request = CreateImageRequest {
        staged_stem: staged_stem.to_owned(),
        capacity,
        volume_name: BLANK_TEMPLATE_LABEL.to_owned(),
        owner_uid,
        owner_gid,
    };
    let image = staged_stem.with_extension(IMAGE_EXTENSION);
    release_leftover(backend, &image)?;
    if let Err(primary) = backend.create_blank_image(&request) {
        return super::super::combine_cleanup(
            "create blank template",
            primary.into(),
            backend.delete_image(&image).map_err(Into::into),
        );
    }
    // Formatting verifies the volume and, on failure, releases its attachment and removes the
    // image itself.
    let attachment = backend.format_attached(&image, &request)?;
    if let Err(primary) = backend.detach(&attachment, DetachIntent::Release) {
        return super::super::combine_cleanup(
            "detach blank template",
            primary.into(),
            backend.delete_image(&image).map_err(Into::into),
        );
    }
    Ok(image)
}

/// Release the attachment a killed minter may have left of `image` and remove the file. Nothing
/// is asked of the kernel when no file is there.
fn release_leftover<R: CommandRunner>(
    backend: &MacOsApfsBackend<R>,
    image: &Path,
) -> Result<(), ApfsStorageError> {
    if !path_exists(image)? {
        return Ok(());
    }
    match backend.recovered_image_attachment(image)? {
        Some(RecoveredImageAttachment::Apfs(attachment)) => {
            backend.detach(&attachment, DetachIntent::Release)?;
        }
        Some(RecoveredImageAttachment::Unformatted {
            image,
            whole_device,
        }) => backend.detach_unformatted_image(&image, &whole_device, DetachIntent::Release)?,
        None => {}
    }
    backend.delete_image(image).map_err(Into::into)
}

fn rename_into_place<R: CommandRunner>(
    backend: &MacOsApfsBackend<R>,
    minted: &Path,
    template: &Path,
) -> Result<(), ApfsStorageError> {
    if let Err(error) = fs::rename(minted, template) {
        return super::super::combine_cleanup(
            "publish blank template",
            super::io_error("rename blank template into place", template, error),
            backend.delete_image(minted).map_err(Into::into),
        );
    }
    sync_parent_path(template)
}
