//! What a workspace image is called on disk: its one file extension.

use std::ffi::OsStr;
use std::path::Path;

/// Every workspace image is a sparse RAW file holding one case-sensitive APFS volume
/// (specs/cowshed/01a_apfs.md, "Images"): one extension, no alias, nothing inferred from
/// anything else.
pub const IMAGE_EXTENSION: &str = "dmg";

/// Whether `path` names `<stem>.dmg`.
pub fn is_image_path(path: &Path) -> bool {
    path.extension() == Some(OsStr::new(IMAGE_EXTENSION))
}
