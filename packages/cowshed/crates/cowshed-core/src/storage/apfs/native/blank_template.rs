//! Mint a workspace image from the disk-image layer's store template.

use std::path::Path;

use super::{MacOsApfsExecutionHost, sync_parent_path};
use crate::storage::apfs::ApfsStorageError;
use cowshed_apfs::image::ImageCapacity;
use cowshed_apfs::template::blank_template;
use cowshed_apfs::{
    ApfsBackend, ApfsError, AttachedImage, CommandRunner, MountAccess, apfs_step_leg,
    timed_apfs_step,
};

impl<R: CommandRunner> MacOsApfsExecutionHost<R> {
    /// Mint `image`, which must not exist: one `clonefile` of the store's template at `capacity`
    /// and one exact-image `--noMount` attach, pinned `fsck_apfs` check and `mount_apfs` mount.
    /// The clone shares the
    /// template's volume and container UUIDs, as every clone shares its source's, and carries its
    /// label. A failed attach removes the clone only after its attachment was released.
    pub(super) fn mint(
        &self,
        capacity: ImageCapacity,
        image: &Path,
        mount_point: &Path,
    ) -> Result<AttachedImage, ApfsStorageError> {
        let leg = apfs_step_leg(image);
        timed_apfs_step(leg, "mint", || {
            let template = blank_template(
                &self.backend,
                &self.config.store_root,
                capacity,
                &crate::held_lock::ImageLockTaker {
                    holding: &crate::api::held_lock::LockHolding::store(
                        crate::api::held_lock::LockOperation::MintTemplate,
                    ),
                },
            )?;
            timed_apfs_step(leg, "clonefile", || {
                self.backend
                    .clone_image(&template, image, cowshed_apfs::ImageCapture::Detached)
                    .map_err(ApfsError::from)
            })?;
            sync_parent_path(image)?;
            match self
                .backend
                .attach_mounted(image, mount_point, MountAccess::ReadWrite, false)
            {
                Ok(attachment) => Ok(attachment),
                Err(
                    primary @ (ApfsError::AttachmentCleanupFailed { .. }
                    | ApfsError::VerificationAndDetachFailed { .. }),
                ) => Err(primary.into()),
                Err(primary) => super::super::combine_cleanup(
                    "attach minted image",
                    primary.into(),
                    self.backend.delete_image(image).map_err(Into::into),
                ),
            }
        })
    }
}
