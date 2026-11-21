//! Where a job's Nx daemon runs on Darwin: inside the job's sandbox profile.

use std::io;

/// Why the live process `pid` is no job's daemon, or `None` when it may be one: a job's daemon
/// runs under a sandbox profile, so a process with none is not one. `sandbox_check` answers that
/// for a live process alone; an exited one may have been.
pub(super) fn not_a_jobs_daemon(pid: libc::pid_t) -> io::Result<Option<String>> {
    if !cowshed_os::process::sandboxed(pid).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot inspect sandbox of process {pid}: {error}"),
        )
    })? {
        return Ok(Some(format!(
            "process {pid} is not sandboxed, so it is not a job's daemon"
        )));
    }
    Ok(None)
}
