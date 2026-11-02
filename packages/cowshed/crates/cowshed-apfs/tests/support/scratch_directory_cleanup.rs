//! The directory fixture's exact-tree reclamation: its own Drop's, and the lane setup's for what a
//! killed run left.

use super::super::scratch_namespace::{processes, tree};
use std::path::Path;

pub(crate) const ROOT_PREFIX: &str = "cowshed-dtest-";
/// End the processes working in `path`, then remove it ([`tree::remove`]).
pub(super) fn reclaim(path: &Path) -> std::io::Result<()> {
    processes::end_processes_in(|cwd| cwd.starts_with(path))?;
    tree::remove(path)
}
