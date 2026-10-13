//! Root-local reclamation after every selected image release has finished.

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

pub(super) fn reclaim_dead_roots(
    roots: impl IntoIterator<Item = PathBuf>,
    unreleased: &BTreeSet<PathBuf>,
    mut remove: impl FnMut(&Path) -> io::Result<()>,
    mut refusal: impl FnMut(&Path, &io::Error),
) {
    for root in roots {
        if let Some(image) = unreleased.iter().find(|image| image.starts_with(&root)) {
            refusal(
                &root,
                &io::Error::other(format!("image {} could not be released", image.display())),
            );
            continue;
        }
        if let Err(error) = remove(&root) {
            refusal(&root, &error);
        }
    }
}
