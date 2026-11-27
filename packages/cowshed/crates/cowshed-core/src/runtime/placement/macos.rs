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

/// The nice `pid` runs at (`getpriority(PRIO_PROCESS)`), clearing `errno` first so a real `-1`
/// nice is never mistaken for an error.
pub fn nice_of(pid: u32) -> io::Result<i32> {
    crate::runtime::shell_host::clear_errno();
    // SAFETY: a plain read of `pid`'s priority.
    let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, pid) };
    if nice == -1
        && let Some(errno) = io::Error::last_os_error().raw_os_error()
        && errno != 0
    {
        return Err(io::Error::from_raw_os_error(errno));
    }
    Ok(nice)
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
    let observed = nice_of(pid).map_err(|error| failure(None, error.raw_os_error()))?;
    if observed == nice {
        Ok(observed)
    } else {
        Err(failure(Some(observed), None))
    }
}

/// Set every process of the group `pgid` leads to `nice` -- interactive work decaying
/// ([`super::DecayStep`]) -- and read back the group's nice, its highest-priority
/// member's. Only lowering: a member a command already placed below `nice` is refused, never
/// raised. The caller holds the leader unreaped, so `pgid` names the job's group alone.
pub fn place_group_nice(pgid: i32, nice: i32) -> io::Result<i32> {
    let who = libc::id_t::try_from(pgid).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("process group {pgid} is not a group id"),
        )
    })?;
    // SAFETY: plain system calls naming a group whose leader the caller holds unreaped.
    if unsafe { libc::setpriority(libc::PRIO_PGRP, who, nice) } == -1 {
        return Err(io::Error::last_os_error());
    }
    crate::runtime::shell_host::clear_errno();
    // SAFETY: as above.
    let observed = unsafe { libc::getpriority(libc::PRIO_PGRP, who) };
    if observed == -1
        && let Some(errno) = io::Error::last_os_error().raw_os_error()
        && errno != 0
    {
        return Err(io::Error::from_raw_os_error(errno));
    }
    if observed == nice {
        Ok(observed)
    } else {
        Err(io::Error::other(format!(
            "process group {pgid} reads nice {observed} once set to {nice}"
        )))
    }
}

/// The nice a job of `class` is placed at: its command's own, its class's offset below the nice
/// this supervisor runs at now.
pub fn placed_nice(class: ExecClass) -> i32 {
    nice_from(own_nice(), class)
}

/// A step of interactive work's decay: every process of the group `pgid` leads runs on at `nice`
/// ([`place_group_nice`]).
pub fn decay_group(pgid: i32, nice: i32) -> io::Result<()> {
    place_group_nice(pgid, nice).map(drop)
}

/// Thread priority ceiling reported by `PROC_PIDTHREADINFO` (`pth_maxpriority`) while
/// `PRIO_DARWIN_BG` is active on a process (05_gateway.md "A land's gate takes tokens back").
pub const DARWIN_BG_MAX_PRIORITY: i32 = 4;

/// Thread priority ceiling reported by `PROC_PIDTHREADINFO` (`pth_maxpriority`) when unclamped
/// and `PRIO_DARWIN_BG` is not active on a process.
pub const DARWIN_NORMAL_MAX_PRIORITY: i32 = 63;

/// Result of setting or clearing `PRIO_DARWIN_BG` on one process, checked via `PROC_PIDTHREADINFO`
/// (`procinfo::thread_max_priorities`) — never `getpriority` or `getiopolicy_np`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DarwinBgOutcome {
    /// Every thread read back the expected `pth_maxpriority` (`4` on set, the recorded pre-set
    /// ceiling — or `> 4` for a legacy ledger life without one — on clear).
    Confirmed,
    /// `setpriority` succeeded, but `PROC_PIDTHREADINFO` readback disagreed (emit warn row).
    Unconfirmed,
    /// The process exited before or during the call (`ESRCH` / absent from `proc_pidinfo`).
    Exited,
}

/// Current thread priority ceiling (`pth_maxpriority` from `PROC_PIDTHREADINFO`) of `pid`, or
/// `Ok(None)` when `pid` has exited or has no live threads.
pub fn thread_ceiling(pid: u32) -> io::Result<Option<i32>> {
    let signed = i32::try_from(pid).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("pid {pid} exceeds i32"),
        )
    })?;
    Ok(
        procinfo::thread_max_priorities(signed)?
            .and_then(|priorities| priorities.into_iter().max()),
    )
}

/// Set `PRIO_DARWIN_BG` on `pid` and confirm via `PROC_PIDTHREADINFO` (`pth_maxpriority == 4`).
pub fn set_darwinbg(pid: u32) -> io::Result<DarwinBgOutcome> {
    apply_darwinbg(pid, libc::PRIO_DARWIN_BG, |prio| {
        prio == DARWIN_BG_MAX_PRIORITY
    })
}

/// Clear `PRIO_DARWIN_BG` on `pid` and confirm via `PROC_PIDTHREADINFO`: against
/// `expected_ceiling` when recorded, or strictly above `DARWIN_BG_MAX_PRIORITY` (`> 4`) for a
/// legacy ledger entry written without a ceiling.
pub fn clear_darwinbg(pid: u32, expected_ceiling: Option<i32>) -> io::Result<DarwinBgOutcome> {
    match expected_ceiling {
        Some(ceiling) => apply_darwinbg(pid, 0, |prio| prio == ceiling),
        None => apply_darwinbg(pid, 0, |prio| prio > DARWIN_BG_MAX_PRIORITY),
    }
}

fn apply_darwinbg(
    pid: u32,
    policy: i32,
    matches_expected: impl Fn(i32) -> bool,
) -> io::Result<DarwinBgOutcome> {
    let signed = i32::try_from(pid).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("pid {pid} exceeds i32"),
        )
    })?;
    // SAFETY: `setpriority(PRIO_DARWIN_PROCESS, pid, policy)` takes plain integer arguments.
    if unsafe { libc::setpriority(libc::PRIO_DARWIN_PROCESS, pid, policy) } == -1 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(DarwinBgOutcome::Exited);
        }
        return Err(error);
    }
    match procinfo::thread_max_priorities(signed)? {
        None => Ok(DarwinBgOutcome::Exited),
        Some(priorities)
            if !priorities.is_empty() && priorities.iter().all(|&prio| matches_expected(prio)) =>
        {
            Ok(DarwinBgOutcome::Confirmed)
        }
        Some(_) => Ok(DarwinBgOutcome::Unconfirmed),
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

    fn spawn_held_shell(program: &str, args: &[&str]) -> std::process::Child {
        use cowshed_os::fork_lock::Spawn as _;
        use std::io::Read as _;
        use std::process::{Command, Stdio};

        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn_locked()
            .expect("spawn held shell");
        let mut ready = [0_u8; 1];
        child
            .stdout
            .as_mut()
            .expect("child stdout")
            .read_exact(&mut ready)
            .expect("child ready byte");
        assert_eq!(&ready, b"r");
        child
    }

    /// Red: a child spawned under a UTILITY QoS clamp starts at ceiling 20, drops to 4 when
    /// `PRIO_DARWIN_BG` is set, and returns to 20 when cleared; clearing must confirm at 20.
    #[test]
    fn clear_darwinbg_confirms_a_utility_clamped_child_at_its_ceiling() {
        use cowshed_os::test_backstop::ChildBackstop as _;

        let mut child = spawn_held_shell(
            "/usr/sbin/taskpolicy",
            &["-c", "utility", "/bin/sh", "-c", "printf r; read _; exit 0"],
        );
        let pid = child.id();
        let before = procinfo::thread_max_priorities(pid.cast_signed())
            .expect("read pre-set ceiling")
            .expect("child is live");
        assert!(
            !before.is_empty() && before.iter().all(|&p| p == 20),
            "expected UTILITY clamp ceiling 20, got {before:?}"
        );
        assert_eq!(
            set_darwinbg(pid).expect("set darwinbg"),
            DarwinBgOutcome::Confirmed
        );
        assert_eq!(
            clear_darwinbg(pid, Some(20)).expect("clear darwinbg"),
            DarwinBgOutcome::Confirmed
        );
        let after = procinfo::thread_max_priorities(pid.cast_signed())
            .expect("read post-clear ceiling")
            .expect("child is live");
        assert!(
            !after.is_empty() && after.iter().all(|&p| p == 20),
            "expected restored ceiling 20, got {after:?}"
        );
        drop(child.stdin.take());
        assert!(child.wait_bounded().expect("reap child").success());
    }

    /// Control 1: an unclamped child still confirms at 63 when cleared.
    #[test]
    fn clear_darwinbg_confirms_an_unclamped_child_at_63() {
        use cowshed_os::test_backstop::ChildBackstop as _;

        let mut child = spawn_held_shell("/bin/sh", &["-c", "printf r; read _; exit 0"]);
        let pid = child.id();
        let before = procinfo::thread_max_priorities(pid.cast_signed())
            .expect("read pre-set ceiling")
            .expect("child is live");
        assert!(
            !before.is_empty() && before.iter().all(|&p| p == DARWIN_NORMAL_MAX_PRIORITY),
            "expected unclamped ceiling 63, got {before:?}"
        );
        assert_eq!(
            set_darwinbg(pid).expect("set darwinbg"),
            DarwinBgOutcome::Confirmed
        );
        assert_eq!(
            clear_darwinbg(pid, Some(DARWIN_NORMAL_MAX_PRIORITY)).expect("clear darwinbg"),
            DarwinBgOutcome::Confirmed
        );
        let after = procinfo::thread_max_priorities(pid.cast_signed())
            .expect("read post-clear ceiling")
            .expect("child is live");
        assert!(
            !after.is_empty() && after.iter().all(|&p| p == DARWIN_NORMAL_MAX_PRIORITY),
            "expected restored ceiling 63, got {after:?}"
        );
        drop(child.stdin.take());
        assert!(child.wait_bounded().expect("reap child").success());
    }

    /// Control 2: a child whose `PRIO_DARWIN_BG` remains held (self-set via `taskpolicy -b`) is
    /// not confirmed by clearing external `PRIO_DARWIN_BG`.
    #[test]
    fn clear_darwinbg_is_unconfirmed_while_bg_remains_held() {
        use cowshed_os::test_backstop::ChildBackstop as _;

        let mut child = spawn_held_shell(
            "/usr/sbin/taskpolicy",
            &["-b", "/bin/sh", "-c", "printf r; read _; exit 0"],
        );
        let pid = child.id();
        let before = procinfo::thread_max_priorities(pid.cast_signed())
            .expect("read ceiling")
            .expect("child is live");
        assert!(
            !before.is_empty() && before.iter().all(|&p| p == DARWIN_BG_MAX_PRIORITY),
            "expected self-BG ceiling 4, got {before:?}"
        );
        assert_eq!(
            set_darwinbg(pid).expect("set darwinbg"),
            DarwinBgOutcome::Confirmed
        );
        assert_eq!(
            clear_darwinbg(pid, Some(DARWIN_NORMAL_MAX_PRIORITY)).expect("clear darwinbg"),
            DarwinBgOutcome::Unconfirmed
        );
        assert_eq!(
            clear_darwinbg(pid, None).expect("clear darwinbg without recorded ceiling"),
            DarwinBgOutcome::Unconfirmed
        );
        drop(child.stdin.take());
        assert!(child.wait_bounded().expect("reap child").success());
    }
}
