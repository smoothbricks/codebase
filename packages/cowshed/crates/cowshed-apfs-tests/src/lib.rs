#![cfg(all(test, target_os = "macos"))]

use cowshed_apfs::image::{IMAGE_EXTENSION, ImageCapacity};
use cowshed_apfs::store_lock::verify_no_symlinks;
use cowshed_apfs::template::blank_template;
use cowshed_apfs::{
    ApfsBackend, ApfsError, AttachedImage, CommandRequest, CommandRunner, CreateImageRequest,
    DISK_CHILD_DEADLINE, DetachIntent, DiskImageSource, ImageCapture, MacOsApfsBackend,
    MountAccess, RecoveredImageAttachment, RetainedInput, RetainedInputs, SystemCommandRunner,
    disk_image_helpers,
};

#[path = "../../cowshed-apfs/tests/support/blank_image.rs"]
mod blank_image;
#[path = "../../cowshed-apfs/tests/support/scratch_apfs.rs"]
mod scratch_apfs;

mod tests;
