//! Build volumes on APFS (16_build_volumes.md, "Substrate"): one ASIF image each, created,
//! cloned, attached and released through the same backend as workspace images, but with no
//! workspace sidecar, no marker and no lifecycle intent. A build volume nothing links is
//! garbage by construction, so an interrupted operation leaves only what garbage collection
//! removes.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{MacOsApfsExecutionHost, io_error, sync_parent_path};
use crate::apfs::{
    ApfsBackend, CommandRunner, CreateImageRequest, DetachIntent, MountAccess,
    RecoveredImageAttachment,
};
use crate::build_volume::{BuildVolumeId, BuildVolumeLayout, BuildVolumeRecord};
use crate::metadata::{IMAGE_EXTENSION, ImageCapacity};
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError};

/// What releasing a build volume did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Release {
    /// Detached, its image, record and mountpoint deleted.
    Deleted,
    /// The image driver refused a non-forced detach: a process has a file or its working
    /// directory in the volume. Nothing was changed; the next collection tries again.
    Busy(String),
}

impl<R> MacOsApfsExecutionHost<R>
where
    R: CommandRunner + Send + Sync + 'static,
{
    /// Create the empty build volume `id` at `capacity` and mount it. Its record is written last:
    /// an image without one is an interrupted creation, which nothing links.
    pub fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        record: &BuildVolumeRecord,
        capacity: ImageCapacity,
    ) -> Result<PathBuf, ApfsStorageError> {
        let image = layout.image(id);
        let stem = layout.staged_stem(id);
        self.verify_controller_path(&image)?;
        self.verify_controller_path(&stem)?;
        Self::ensure_parent(&image)?;
        Self::ensure_parent(&stem)?;
        let request = CreateImageRequest {
            staged_stem: stem.clone(),
            capacity,
            volume_name: volume_name(id),
            // SAFETY: `getuid`/`getgid` read this process's credentials; they take no pointers
            // and cannot fail.
            owner_uid: unsafe { libc::getuid() },
            // SAFETY: as above.
            owner_gid: unsafe { libc::getgid() },
        };
        let blank = stem.with_extension(IMAGE_EXTENSION);
        if let Err(primary) = self.backend.create_blank_image(&request) {
            return super::super::combine_cleanup(
                "create blank build volume",
                primary.into(),
                self.backend.delete_image(&blank).map_err(Into::into),
            );
        }
        if let Err(error) = fs::rename(&blank, &image) {
            return super::super::combine_cleanup(
                "publish blank build volume",
                io_error("rename blank build volume into place", &image, error),
                self.backend.delete_image(&blank).map_err(Into::into),
            );
        }
        sync_parent_path(&image)?;
        let attachment = match self.backend.format_attached(&image, &request) {
            Ok(attachment) => attachment,
            Err(primary) => {
                return super::super::combine_cleanup(
                    "format build volume",
                    primary.into(),
                    self.backend.delete_image(&image).map_err(Into::into),
                );
            }
        };
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
        layout
            .write_record(id, record)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        Ok(mount)
    }

    /// Clone `source`'s image to the new volume `destination`, with `record`. The clone is not
    /// attached. A mounted source's volume is flushed first; the caller guarantees nothing
    /// writes it while it is cloned (a seed has no writer, a landing volume is quiesced).
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
        self.backend
            .sync_and_clone(&from, mounted.then_some(mount.as_path()), &to)?;
        // The clone's own extent map, paid here rather than by its first write inside a mount.
        self.write_first(&to)?;
        layout
            .write_record(destination, record)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
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
            if same_path(&mounted.mount_point, &mount) {
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
        Ok(self
            .mount_source
            .mounts()?
            .into_iter()
            .find(|mounted| same_path(&mounted.mount_point, mount))
            .map(|mounted| mounted.source_device))
    }

    /// Unmount and detach build volume `id` without force, then delete its image, record and
    /// mountpoint. The kernel's refusal is the only judge of whether the volume is in use
    /// (16_build_volumes.md, "Garbage collection"): it is reported as [`Release::Busy`] and
    /// nothing is touched.
    pub fn release_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<Release, ApfsStorageError> {
        let image = layout.image(id);
        self.verify_controller_path(&image)?;
        if image.exists() {
            match self.backend.recovered_image_attachment(&image)? {
                Some(RecoveredImageAttachment::Apfs(attachment)) => {
                    let mounted = self
                        .mount_source
                        .mounts()?
                        .into_iter()
                        .any(|mount| mount.source_device == attachment.volume_device());
                    if mounted
                        && let Err(error) = self
                            .backend
                            .unmount_verified(&attachment, DetachIntent::WhenIdle)
                    {
                        return busy_or(error);
                    }
                    if let Err(error) = self.backend.detach(&attachment, DetachIntent::WhenIdle) {
                        return busy_or(error);
                    }
                }
                Some(RecoveredImageAttachment::Unformatted {
                    image,
                    whole_device,
                }) => {
                    if let Err(error) = self.backend.detach_unformatted_image(
                        &image,
                        &whole_device,
                        DetachIntent::WhenIdle,
                    ) {
                        return busy_or(error);
                    }
                }
                None => {}
            }
        }
        self.backend.delete_image(&image)?;
        remove_if_present(&layout.record(id))?;
        let mount = layout.mount(id);
        match fs::remove_dir(&mount) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(io_error("remove build volume mountpoint", &mount, error));
            }
        }
        Ok(Release::Deleted)
    }

    /// Remove what an interrupted build-volume creation left in the staging directory.
    pub fn sweep_build_volume_staging(
        &self,
        layout: &BuildVolumeLayout,
    ) -> Result<(), ApfsStorageError> {
        let staging = layout.staging();
        let entries = match fs::read_dir(&staging) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io_error("list build volume staging", &staging, error)),
        };
        for entry in entries {
            let path = entry
                .map_err(|error| io_error("list build volume staging", &staging, error))?
                .path();
            if path
                .extension()
                .is_some_and(|extension| extension == IMAGE_EXTENSION)
            {
                // A blank never attached: deleting it frees nothing anyone holds.
                if self.backend.existing_attachment(&path)?.is_none() {
                    self.backend.delete_image(&path)?;
                }
            }
        }
        Ok(())
    }

    /// Grow build volume `id`'s image to `capacity`: detached without force, grown, attached,
    /// its container grown, and mounted again. A volume in use refuses before anything changes.
    pub fn resize_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<ImageCapacity, ApfsStorageError> {
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
        if let Some(RecoveredImageAttachment::Apfs(attachment)) =
            self.backend.recovered_image_attachment(&image)?
        {
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
        Ok(observed)
    }
}

/// The volume label: a label and nothing else (01_storage.md "Volume name").
fn volume_name(id: &BuildVolumeId) -> String {
    format!("cowshed build {}", &id.as_str()[..8])
}

/// A detach the image driver dissented from is the kernel saying "in use"; any other failure is
/// an error.
fn busy_or(error: crate::apfs::ApfsError) -> Result<Release, ApfsStorageError> {
    if crate::apfs::detach_was_dissented(&error) {
        Ok(Release::Busy(error.to_string()))
    } else {
        Err(error.into())
    }
}

fn remove_if_present(path: &Path) -> Result<(), ApfsStorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("remove build volume record", path, error)),
    }
}

/// Whether two spellings name one directory: the kernel reports canonical mount points.
fn same_path(left: &Path, right: &Path) -> bool {
    left == right
        || match (fs::canonicalize(left), fs::canonicalize(right)) {
            (Ok(left), Ok(right)) => left == right,
            _ => false,
        }
}
