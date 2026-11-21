//! A build user's processes as XNU lists them: `proc_listpids(PROC_UID_ONLY)`, one uid at a
//! time.

use std::io;

use super::Builds;

/// Why the daemon's builds cannot be seen.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Unwatched {
    /// The `nixbld` group does not exist or lists no user with an account.
    NoBuildUsers,
}

impl std::fmt::Display for Unwatched {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoBuildUsers => formatter.write_str("the host has no nixbld build users"),
        }
    }
}

/// `PROC_UID_ONLY` of `<sys/proc_info.h>`, which the libc crate does not declare: the processes
/// whose effective uid is the one named.
const PROC_UID_ONLY: u32 = 4;

/// Whether any process runs as one of `users`. libproc's `proc_listpids` answers the kernel's
/// -1 with 0 and leaves errno set, so a 0 is no process only when errno stayed clear across the
/// call (as `process_events::list_child_pids` reads it).
fn any_process_of(users: &[libc::uid_t]) -> io::Result<bool> {
    let mut pids: [libc::pid_t; 16] = [0; 16];
    let bytes = libc::c_int::try_from(size_of_val(&pids)).map_err(io::Error::other)?;
    for &uid in users {
        // SAFETY: `__error` names this thread's errno.
        unsafe { *libc::__error() = 0 };
        // SAFETY: `pids` is writable for `bytes` bytes, its whole size.
        let filled =
            unsafe { libc::proc_listpids(PROC_UID_ONLY, uid, pids.as_mut_ptr().cast(), bytes) };
        let error = io::Error::last_os_error();
        match usize::try_from(filled) {
            Ok(filled) if filled > 0 => {
                let listed = filled / size_of::<libc::pid_t>();
                if pids[..listed.min(pids.len())].iter().any(|&pid| pid > 0) {
                    return Ok(true);
                }
            }
            _ if error.raw_os_error() == Some(0) => {}
            _ => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("proc_listpids(PROC_UID_ONLY, {uid}): {error}"),
                ));
            }
        }
    }
    Ok(false)
}

/// Whether a build user runs a process, as the kernel lists each one's.
pub(super) fn builds_of(users: &[libc::uid_t]) -> io::Result<Builds> {
    Ok(if any_process_of(users)? {
        Builds::Running
    } else {
        Builds::Idle
    })
}

#[cfg(test)]
mod tests {
    /// This test's own uid runs a process (this one); a uid no account uses runs none.
    #[test]
    fn a_users_live_process_is_seen_and_an_unused_uid_has_none() {
        // SAFETY: geteuid reads this process's credentials.
        let own = unsafe { libc::geteuid() };
        assert!(super::any_process_of(&[own]).unwrap());
        assert!(!super::any_process_of(&[0x7fff_fff0]).unwrap());
    }
}
