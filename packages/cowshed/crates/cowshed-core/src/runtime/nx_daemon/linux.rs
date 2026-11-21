//! Where a job's Nx daemon runs on Linux: in the cgroup its supervisor made for the job
//! (`job_cgroup.rs`), which every process the job starts is born in.

use std::io;

use crate::runtime::job_cgroup::job_cgroup_of;

/// Why the live process `pid` is no job's daemon, or `None` when it may be one: a job's daemon
/// runs in its job's cgroup, so a process whose `/proc/<pid>/cgroup` lies in no job cgroup is not
/// one. An exited process has no membership left to read; it may have been.
pub(super) fn not_a_jobs_daemon(pid: libc::pid_t) -> io::Result<Option<String>> {
    match job_cgroup_of(pid) {
        Ok(Some(_)) => Ok(None),
        Ok(None) => Ok(Some(format!(
            "process {pid} runs in no job's cgroup, so it is not a job's daemon"
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!("cannot inspect the cgroup of process {pid}: {error}"),
        )),
    }
}
