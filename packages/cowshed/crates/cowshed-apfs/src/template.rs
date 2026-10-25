//! The store's blank templates (01a_apfs.md, "Images"): one formatted, verified, detached RAW
//! image per capacity and owner, from which every new image is cloned. A mint is then one
//! `clonefile` and one attach. Creating the sparse file, the formatting attach, the `newfs_apfs`
//! and the detach are paid once per store and capacity, here, instead of once per image.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::image::{IMAGE_EXTENSION, ImageCapacity};
use crate::store_lock::{LockMode, StoreLockError, Taker, acquire_image_locks, verify_no_symlinks};
use crate::{
    ApfsBackend, ApfsError, CommandRunner, CreateImageRequest, DetachIntent, MacOsApfsBackend,
    timed_apfs_step,
};

/// Why the store's template could not be found or minted.
#[derive(Debug)]
pub enum TemplateError {
    Apfs(ApfsError),
    Lock(StoreLockError),
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    InvalidPlan(&'static str),
    /// A failed step whose cleanup failed too: both are kept.
    Cleanup {
        operation: &'static str,
        primary: Box<TemplateError>,
        cleanup: Box<TemplateError>,
    },
}

impl fmt::Display for TemplateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Apfs(error) => error.fmt(formatter),
            Self::Lock(error) => error.fmt(formatter),
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
            Self::InvalidPlan(reason) => formatter.write_str(reason),
            Self::Cleanup {
                operation,
                primary,
                cleanup,
            } => write!(
                formatter,
                "{operation} failed: {primary}; cleanup failed: {cleanup}"
            ),
        }
    }
}

impl std::error::Error for TemplateError {}

impl From<ApfsError> for TemplateError {
    fn from(error: ApfsError) -> Self {
        Self::Apfs(error)
    }
}

impl From<StoreLockError> for TemplateError {
    fn from(error: StoreLockError) -> Self {
        Self::Lock(error)
    }
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> TemplateError {
    TemplateError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

fn combine_cleanup<T>(
    operation: &'static str,
    primary: TemplateError,
    cleanup: Result<(), TemplateError>,
) -> Result<T, TemplateError> {
    match cleanup {
        Ok(()) => Err(primary),
        Err(cleanup) => Err(TemplateError::Cleanup {
            operation,
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn path_exists(path: &Path) -> Result<bool, TemplateError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect blank template", path, error)),
    }
}

fn sync_parent_path(path: &Path) -> Result<(), TemplateError> {
    let parent = path
        .parent()
        .ok_or(TemplateError::InvalidPlan("image path has no parent"))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync image directory", parent, error))
}

/// The store-root namespace the templates live in. Dot-prefixed, so no store walker takes it for
/// a repository owner (`is_reserved_store_namespace`).
const BLANK_TEMPLATE_DIRECTORY: &str = ".blank";

/// `<store>/.blank/<bytes>-<uid>-<gid>.dmg`: the template a mint at `capacity` clones for the
/// invoking user. The owner is part of the name because `newfs_apfs -U/-G` writes it into the
/// volume root, and every clone inherits it.
pub fn blank_template_path(store_root: &Path, capacity: ImageCapacity) -> PathBuf {
    template_file(store_root, capacity, "").with_extension(IMAGE_EXTENSION)
}

/// The suffix every staging name of a template carries after the template's own stem.
const STAGING_SUFFIX: &str = "-minting";

/// `<store>/.blank/<bytes>-<uid>-<gid>-minting-<nonce>`: where one minter writes, formats and
/// detaches the template before it takes its name, as `<stem>.dmg`. Each call answers a name
/// no minter used before, so a path is attached at most once in its life: a minter killed with
/// its attach still queued on `storagekitd` can have that attach land later, after the next
/// minter removed its file, and on a reused name it landed beside the next minter's own attach
/// and gave the path two attachments.
pub fn blank_template_staged_stem(store_root: &Path, capacity: ImageCapacity) -> PathBuf {
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    template_file(store_root, capacity, &format!("{STAGING_SUFFIX}-{nonce}"))
}

/// Whether `path` is a staging image of the template at `capacity`, under any nonce or none
/// (the single staging name older minters reused).
fn is_staging_image(store_root: &Path, capacity: ImageCapacity, path: &Path) -> bool {
    let prefix = template_file(store_root, capacity, STAGING_SUFFIX);
    let (Some(directory), Some(stem)) = (prefix.parent(), prefix.file_name()) else {
        return false;
    };
    path.parent() == Some(directory)
        && path.extension() == Some(std::ffi::OsStr::new(IMAGE_EXTENSION))
        && path
            .file_stem()
            .is_some_and(|name| name.as_encoded_bytes().starts_with(stem.as_encoded_bytes()))
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

/// The store's template at `capacity`, minted first if the store has none yet.
///
/// A template only ever appears under its name complete, formatted, verified (`fsck_apfs -q`) and
/// detached: it is minted under a staging name of its own and renamed into place after the
/// detach. So an existing template needs no lock and no check — a mint clones it on sight. Only
/// minting takes the template's lock, and a minter that finds the template once it holds the lock
/// returns it: concurrent first mints make one template. Every staging image a minter killed
/// partway left — attached or not, its file present or not — is released and removed first. The
/// minter takes the template's lock as `taker`, recorded on it and naming who holds it when it
/// waits.
///
/// The template's volume carries `label`, its store's namespace's prefix
/// (`HostNamespace::label_prefix`, `[cowshed]` for production), and so does every image minted
/// from it until the supervisor of the checkout it serves names it (`VolumeLabels` in the
/// runtime's supervisor): after its workspace, a build volume after the checkout that links it.
/// Relabelling queues on Disk Arbitration, so it runs off every provisioning path, but it always
/// runs: Disk Utility shows every attached volume, and one named only this says nothing about what
/// it is (01a_apfs.md, "Ownership, identity, and the volume label"; 16_build_volumes.md,
/// "Substrate").
pub fn blank_template<R: CommandRunner>(
    backend: &MacOsApfsBackend<R>,
    store_root: &Path,
    label: &str,
    capacity: ImageCapacity,
    taker: &dyn Taker,
) -> Result<PathBuf, TemplateError> {
    let template = blank_template_path(store_root, capacity);
    verify_no_symlinks(store_root, &template)?;
    if path_exists(&template)? {
        return Ok(template);
    }
    let _minting = acquire_image_locks(
        store_root,
        &[template.with_extension("lock")],
        LockMode::Wait,
        taker,
    )?
    .ok_or(TemplateError::InvalidPlan(
        "a waiting lock acquisition answered busy",
    ))?;
    if path_exists(&template)? {
        return Ok(template);
    }
    timed_apfs_step("template", "mint", || {
        release_leftovers(backend, store_root, capacity)?;
        mint(
            backend,
            &blank_template_staged_stem(store_root, capacity),
            label,
            capacity,
        )
        .and_then(|minted| rename_into_place(backend, &minted, &template))
    })?;
    Ok(template)
}

/// Create, format, verify and detach a fresh template at `staged_stem.dmg`, a name nothing has
/// held before.
fn mint<R: CommandRunner>(
    backend: &MacOsApfsBackend<R>,
    staged_stem: &Path,
    label: &str,
    capacity: ImageCapacity,
) -> Result<PathBuf, TemplateError> {
    let (owner_uid, owner_gid) = owner();
    let request = CreateImageRequest {
        staged_stem: staged_stem.to_owned(),
        capacity,
        volume_name: label.to_owned(),
        owner_uid,
        owner_gid,
    };
    let image = staged_stem.with_extension(IMAGE_EXTENSION);
    if let Err(primary) = backend.create_blank_image(&request) {
        return combine_cleanup(
            "create blank template",
            primary.into(),
            backend.delete_image(&image).map_err(Into::into),
        );
    }
    // Formatting verifies the volume and, on failure, releases its attachment and removes the
    // image itself.
    let attachment = backend.format_attached(&image, &request)?;
    if let Err(primary) = backend.detach(&attachment, DetachIntent::Release) {
        return combine_cleanup(
            "detach blank template",
            primary.into(),
            backend.delete_image(&image).map_err(Into::into),
        );
    }
    Ok(image)
}

/// Release every attachment of every staging image of this template that killed minters left,
/// then remove their files. The kernel's inventory decides what is attached, not the directory:
/// an attachment outlives its file (a cleanup that removed the image while a queued attach was
/// still to land), and keeps the path it was handed. The caller holds the template's lock, so no
/// live minter owns any of them.
fn release_leftovers<R: CommandRunner>(
    backend: &MacOsApfsBackend<R>,
    store_root: &Path,
    capacity: ImageCapacity,
) -> Result<(), TemplateError> {
    for image in backend.attached_image_files()? {
        if is_staging_image(store_root, capacity, &image) {
            backend.release_every_attachment(&image, DetachIntent::Release)?;
        }
    }
    let directory = blank_template_path(store_root, capacity)
        .parent()
        .map(Path::to_owned)
        .ok_or(TemplateError::InvalidPlan("a template has a directory"))?;
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("list blank templates", &directory, error)),
    };
    for entry in entries {
        let image = entry
            .map_err(|error| io_error("list blank templates", &directory, error))?
            .path();
        if is_staging_image(store_root, capacity, &image) {
            backend.delete_image(&image)?;
        }
    }
    Ok(())
}

fn rename_into_place<R: CommandRunner>(
    backend: &MacOsApfsBackend<R>,
    minted: &Path,
    template: &Path,
) -> Result<(), TemplateError> {
    if let Err(error) = fs::rename(minted, template) {
        return combine_cleanup(
            "publish blank template",
            io_error("rename blank template into place", template, error),
            backend.delete_image(minted).map_err(Into::into),
        );
    }
    sync_parent_path(template)
}
