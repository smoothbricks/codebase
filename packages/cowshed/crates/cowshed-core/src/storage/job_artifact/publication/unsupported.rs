use std::ffi::CStr;
use std::fs::File;
use std::path::Path;

use super::{ArtifactError, Parent, PublicationStage, publication_error};

pub(super) fn try_fast_clone(
    _parent: &mut Parent,
    _source: &File,
) -> Result<Option<File>, ArtifactError> {
    Ok(None)
}

pub(super) fn rename_noreplace(
    _directory_fd: libc::c_int,
    _temporary: &CStr,
    destination: &CStr,
) -> Result<libc::c_int, ArtifactError> {
    Err(publication_error(
        Path::new(destination.to_string_lossy().as_ref()),
        PublicationStage::Publish,
        "atomic create-new publication is unsupported on this platform",
    ))
}
