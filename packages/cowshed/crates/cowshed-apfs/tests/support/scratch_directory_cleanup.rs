//! The directory fixture's exact-tree reclamation: its own Drop's, and the lane setup's for what a
//! killed run left.

use super::super::scratch_namespace::{processes, tree};
use std::ffi::OsStr;
use std::path::Path;

pub(crate) const ROOT_PREFIX: &str = "cowshed-dtest-";
/// The pid a short directory's name records, `cowshed-dtest-<pid>-…` (a directory fixture's root,
/// or a run's template directory), once no process holds it: the lane setup's template and
/// short-directory sweep takes such a directory. A live pid keeps it, a reused one included.
pub(crate) fn dead_recorded_pid(name: &OsStr) -> Option<libc::pid_t> {
    let pid = name
        .to_str()?
        .strip_prefix(ROOT_PREFIX)?
        .split('-')
        .next()?
        .parse::<libc::pid_t>()
        .ok()
        .filter(|pid| *pid > 0)?;
    // SAFETY: signal zero checks one positive pid without delivering a signal.
    let probed = unsafe { libc::kill(pid, 0) };
    (probed == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH))
        .then_some(pid)
}
/// End the processes working in `path`, then remove it ([`tree::remove`]).
pub(super) fn reclaim(path: &Path) -> std::io::Result<()> {
    processes::end_processes_in(|cwd| cwd.starts_with(path))?;
    tree::remove(path)
}
