//! Root-local reclamation after every selected image release has finished.

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Reclaimed {
    pub(super) removed: usize,
    pub(super) retained: usize,
}

/// Called under the sweep lock. An interrupted or failed pass never publishes a run record.
pub(super) fn sweep_once<T>(
    run: &str,
    recorded: Option<&[u8]>,
    sweep: impl FnOnce() -> io::Result<T>,
    record: impl FnOnce(&[u8]) -> io::Result<()>,
) -> io::Result<Option<T>> {
    if recorded == Some(run.as_bytes()) {
        return Ok(None);
    }
    let result = sweep()?;
    record(run.as_bytes())?;
    Ok(Some(result))
}

pub(super) fn reclaim_dead_roots(
    roots: impl IntoIterator<Item = PathBuf>,
    unreleased: &BTreeSet<PathBuf>,
    mut remove: impl FnMut(&Path) -> io::Result<()>,
    mut refusal: impl FnMut(&Path, &io::Error),
) -> Reclaimed {
    let mut counts = Reclaimed::default();
    for root in roots {
        if let Some(image) = unreleased.iter().find(|image| image.starts_with(&root)) {
            refusal(
                &root,
                &io::Error::other(format!("image {} could not be released", image.display())),
            );
            counts.retained += 1;
            continue;
        }
        match remove(&root) {
            Ok(()) => counts.removed += 1,
            Err(error) => {
                counts.retained += 1;
                refusal(&root, &error);
            }
        }
    }
    counts
}
