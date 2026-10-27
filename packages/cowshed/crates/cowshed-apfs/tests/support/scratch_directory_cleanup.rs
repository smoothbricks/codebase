//! The directory fixture's existing exact-tree reclamation, shared with lane setup.

use super::super::scratch_namespace::processes;
use std::fs;
use std::path::Path;

pub(crate) const ROOT_PREFIX: &str = "cowshed-dtest-";
pub(super) fn reclaim(path: &Path) -> std::io::Result<()> {
    processes::end_processes_in(|cwd| cwd.starts_with(path))?;
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
