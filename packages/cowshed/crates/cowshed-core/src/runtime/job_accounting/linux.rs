//! Linux's read of a job leader's own and reaped-children CPU: `/proc/<pid>/stat`.
//!
//! `utime` and `stime` (fields 14 and 15) are the leader's own CPU, every thread's; `cutime` and
//! `cstime` (16 and 17) are that of the children it waited for, with what they waited for in
//! turn (`wait_task_zombie` adds a reaped child's own and children times to its parent's
//! signal struct). The leader's parent holds it unreaped until the job's terminal sample, and
//! procfs keeps an exited process's stat until it is reaped, fixed at its exit. The one read
//! carries `starttime` (field 22) beside the counters, which fences it to the leader's life.
//!
//! Two things differ from the macOS source, and neither is hidden:
//!
//! - The times are clock ticks (`USER_HZ`, 10 ms on every supported kernel), so a leader or a
//!   child that ran for less than a tick counts nothing yet.
//! - A parent that ignores `SIGCHLD` (or sets `SA_NOCLDWAIT`) waits for no child: the kernel
//!   releases its children itself and adds their CPU to no one, so such a parent's descendants
//!   never reach the leader's children total.
//!
//! The job's complete totals on Linux are its cgroup's (`job_cgroup.rs`: `cpu.stat`, `io.stat`,
//! `memory.current`), which count every process that ever ran in it. This reading is the
//! leader-interval source the fold in `job_accounting.rs` takes, and no more than that.

use std::num::NonZeroU32;

use super::{LeaderRusage, LeaderRusageError};
use crate::api::resources::{CpuMicros, CpuTotals, ResourceUnitError};
use crate::runtime::job_groups::GroupLeader;
use crate::runtime::job_groups::linux::{Stat, clock_ticks_per_second, stat};

/// The kernel read a [`LeaderRusageError::Read`] names.
pub(super) const READ: &str = "reading /proc/<pid>/stat";

/// Why the length of a clock tick could not be read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TimebaseError {
    #[error("sysconf(_SC_CLK_TCK) answered {0}, which is no tick rate")]
    NoRate(libc::c_long),
}

/// The four CPU counters of `stat` as a reading of `pid`, each converted once from clock ticks
/// of which the kernel counts `hertz` a second.
fn convert(pid: u32, stat: &Stat, hertz: NonZeroU32) -> Result<LeaderRusage, ResourceUnitError> {
    // One tick is `1_000_000_000 / hertz` nanoseconds.
    let micros = |ticks| CpuMicros::of_ticks(ticks, 1_000_000_000, hertz);
    Ok(LeaderRusage {
        pid,
        started: stat.starttime,
        own: CpuTotals {
            user_us: micros(stat.utime)?,
            sys_us: micros(stat.stime)?,
        },
        children: CpuTotals {
            user_us: micros(stat.cutime)?,
            sys_us: micros(stat.cstime)?,
        },
    })
}

/// Read `observed`'s own and reaped-children CPU now, as `pid`'s reading. The start the read
/// carries proves it that leader's.
pub(super) fn read(pid: u32, observed: GroupLeader) -> Result<LeaderRusage, LeaderRusageError> {
    let stat = stat(observed.pgid())
        .map_err(|error| match error.raw_os_error() {
            Some(errno) => LeaderRusageError::Read { pid, errno },
            None => LeaderRusageError::Malformed {
                pid,
                reason: error.to_string(),
            },
        })?
        .ok_or(LeaderRusageError::Reaped { pid })?;
    if stat.starttime != observed.birth() {
        return Err(LeaderRusageError::OtherLife {
            pid,
            born: observed.birth(),
            read: stat.starttime,
        });
    }
    let hertz = clock_ticks_per_second().map_err(|rate| LeaderRusageError::Timebase {
        pid,
        source: TimebaseError::NoRate(rate),
    })?;
    convert(pid, &stat, hertz).map_err(|source| LeaderRusageError::Inexact { pid, source })
}

#[cfg(test)]
mod tests {
    use super::super::read_leader;
    use super::super::tests::cpu;
    use super::*;
    use crate::runtime::job_groups::Birth;

    /// Every counter is converted from clock ticks by the kernel's rate, once: at 100 ticks a
    /// second a tick is 10 ms, never a microsecond or a nanosecond.
    #[test]
    fn clock_ticks_convert_by_the_tick_rate() {
        let me = i32::try_from(std::process::id()).expect("a pid");
        let mut stat = stat(me).expect("read this process").expect("it runs");
        (stat.utime, stat.stime, stat.cutime, stat.cstime) = (250, 3, 1_000, 0);
        let hertz = |rate| NonZeroU32::new(rate).expect("a rate");
        let reading = convert(9, &stat, hertz(100)).expect("exact");
        assert_eq!(
            (reading.pid, reading.started, reading.own, reading.children),
            (
                9,
                stat.starttime,
                cpu(2_500_000, 30_000),
                cpu(10_000_000, 0)
            )
        );
        let finer = convert(9, &stat, hertz(1_000)).expect("exact");
        assert_eq!(
            (finer.own, finer.children),
            (cpu(250_000, 3_000), cpu(1_000_000, 0))
        );
        let mut beyond = stat;
        beyond.utime = u64::MAX;
        assert!(
            convert(9, &beyond, hertz(1)).is_err(),
            "a count no projection holds exactly is refused, never truncated"
        );
    }

    /// A leader held by its own life is read whole from one stat: its start is the one its
    /// observation carries, and a pid nothing holds is a leader reaped, never a zero reading.
    #[test]
    fn a_running_leader_reads_under_its_start_and_a_pid_nothing_holds_is_reaped() {
        let me = std::process::id();
        let mine = GroupLeader::observe(me).expect("observe this process");
        let reading = read_leader(&Birth::Observed(mine)).expect("this process is read");
        assert_eq!((reading.pid, reading.started), (me, mine.birth()));

        // A group id is at most the kernel's highest pid, which no host raises to `i32::MAX`.
        let absent = GroupLeader::from_parts(i32::MAX, 1, 1).expect("a leader");
        assert_eq!(
            read_leader(&Birth::Observed(absent)),
            Err(LeaderRusageError::Reaped {
                pid: i32::MAX.unsigned_abs()
            })
        );
    }
}
