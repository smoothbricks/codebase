use std::fs::File;

use super::{ArtifactError, Parent};

pub(super) fn try_fast_clone(
    _parent: &mut Parent,
    _source: &File,
) -> Result<Option<File>, ArtifactError> {
    Ok(None)
}
