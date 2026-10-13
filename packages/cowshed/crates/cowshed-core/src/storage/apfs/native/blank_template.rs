//! Mint a workspace image from the disk-image layer's store template.

use std::path::Path;

use super::{MacOsApfsExecutionHost, sync_parent_path};
use cowshed_apfs::{
    ApfsBackend, ApfsError, AttachedImage, CommandRunner, apfs_step_leg, timed_apfs_step,
};
use cowshed_apfs::image::ImageCapacity;
use cowshed_apfs::template::blank_template;
use crate::storage::apfs::ApfsStorageError;

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
                    .clone_image(&template, image, cowshed_apfs::ImageCapture::Detached)
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

