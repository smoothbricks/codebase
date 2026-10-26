//! macOS class placement: a job's class is one `setpriority(PRIO_PROCESS)` nice on its held
//! leader, read back before the leader runs anything of the job's (05_gateway.md "Classes";
//! 11_shell.md "Job admission and class placement"). Nothing else is set: no QoS clamp, no
//! `PRIO_DARWIN_BG` and no disk I/O policy below IMPORTANT, because each throttles the I/O of a
//! process that holds the store's locks and every waiter on those locks sleeps with it (05_gateway.md
//! "On macOS nothing cowshed starts is throttled for I/O"; commit 57b1c81bc0: an unlink 53.1 s throttled
//! against 7.6 s at normal policy).
//!
//! The class's offset is the shared table [`super::class_nice`]; this file adds the supervisor's
//! own nice to it, bounds it at the kernel's lowest priority, and places and reads it back.

use std::io;

use crate::api::dto::ExecClass;
use crate::error::PriorityFailure;

/// The lowest priority setpriority places and reads back: Darwin places PRIO_MAX, 20, itself.
pub const LOWEST_NICE: i32 = libc::PRIO_MAX;

/// The nice a class's command runs at from `own`, the nice this supervisor runs at: its class's
/// offset below it, never past the kernel's lowest priority. Only lowering: raising a priority
/// above the supervisor's own takes a privilege no supervisor holds.
pub const fn nice_from(own: i32, class: ExecClass) -> i32 {
    let nice = own.saturating_add(super::class_nice(class));
    if nice > LOWEST_NICE {
        LOWEST_NICE
    } else {
        nice
    }
}

/// The nice this supervisor runs at, which its classes are placed from.
pub fn own_nice() -> i32 {
    crate::runtime::shell_host::clear_errno();
    // SAFETY: a plain read of this process's own priority.
    let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) };
    // -1 is a nice as well as getpriority's error; a process reading its own priority has no
    // error to report, so a set errno here is an impossible state, not a placement.
    let read = io::Error::last_os_error();
    assert!(
        nice != -1 || read.raw_os_error() == Some(0),
        "this process reads its own nice: {read}"
    );
    nice
}

/// Set the held command `pid` -- forked, leading its own group, not yet running anything of the
/// job's -- to `nice`, and read back what its kernel entry holds: the nice the command is
/// observed at. Only a child held at its observation fence is placed here, never a host or this
/// supervisor.
pub fn place_nice(pid: u32, nice: i32) -> Result<i32, PriorityFailure> {
    let failure = |observed, errno| PriorityFailure {
        pid,
        requested: nice,
        observed,
        errno,
    };
    // SAFETY: plain system calls naming a child this supervisor's spawn holds unreleased.
    if unsafe { libc::setpriority(libc::PRIO_PROCESS, pid, nice) } == -1 {
        return Err(failure(None, io::Error::last_os_error().raw_os_error()));
    }
    crate::runtime::shell_host::clear_errno();
    // SAFETY: as above.
    let observed = unsafe { libc::getpriority(libc::PRIO_PROCESS, pid) };
    if observed == -1
        && let Some(errno) = io::Error::last_os_error().raw_os_error()
        && errno != 0
    {
        return Err(failure(None, Some(errno)));
    }
    if observed == nice {
        Ok(observed)
    } else {
        Err(failure(Some(observed), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A class lowers from whatever nice the supervisor itself runs at, so a supervisor started
    /// nice -- under `cowshed exec --host`, a gate's runner -- still places every class, and the
    /// lowest priority bounds the classes rather than refusing them (05_gateway.md "Classes").
    #[test]
    fn a_class_lowers_from_the_supervisors_own_nice() {
        let classes = [
            ExecClass::Interactive,
            ExecClass::InteractiveResident,
            ExecClass::Gate,
            ExecClass::Background,
            ExecClass::Resident,
        ];
        for (own, placed) in [
            (0, [0, 0, 5, 10, 10]),
            (10, [10, 10, 15, LOWEST_NICE, LOWEST_NICE]),
            (17, [17, 17, LOWEST_NICE, LOWEST_NICE, LOWEST_NICE]),
            (LOWEST_NICE, [LOWEST_NICE; 5]),
        ] {
            assert_eq!(
                classes.map(|class| nice_from(own, class)),
                placed,
                "supervisor at nice {own}"
            );
        }
    }
}
