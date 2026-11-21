//! launchd ends a job's whole process group, and the daemon stays in the host's, so the
//! manager's own teardown reaches it even when the host itself is killed.

use std::io;

/// Run in the child between fork and exec: nothing to arrange, launchd's teardown of the job's
/// process group already ends the daemon with its host.
pub(super) fn end_with_host(_host: libc::pid_t) -> io::Result<()> {
    Ok(())
}
