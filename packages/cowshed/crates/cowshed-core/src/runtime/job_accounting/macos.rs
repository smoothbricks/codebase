//! macOS's read of a job leader's own and reaped-children CPU: `proc_pid_rusage`
//! (`RUSAGE_INFO_V4`).
//!
//! When a parent reaps a child, xnu adds the child's own CPU and its reaped children's to the
//! parent's `ri_child_*` (`update_rusage_info_child` in `reap_child_locked`; a parent that
//! ignores SIGCHLD gets the same when the kernel reaps for it). The leader's parent holds it
//! unreaped until the job's terminal sample, so `proc_pid_rusage` answers for it after its exit
//! too, and every read carries the leader's start (`ri_proc_start_abstime`), which fences it to
//! that one life. A leader that has exited is read from its held zombie, whose rusage xnu fixed
//! at the exit (`proc_prepareexit`). `ri_child_*` holds no disk I/O.
//!
//! xnu does not keep each counter of a reading from falling below the last: one job leader's
//! own user CPU read 239 us, then 222 us. The fold holds each counter at the highest value read
//! of it (`super::Charged::checked`).

use super::*;
use crate::runtime::job_groups::GroupLeader;
use crate::runtime::process_usage::MachTick;
pub use crate::runtime::process_usage::TimebaseError;

/// The kernel read a [`LeaderRusageError::Read`] names.
pub(super) const READ: &str = "proc_pid_rusage";

/// The CPU fields of `rusage_info_v4` as the kernel keeps them: Mach ticks, not nanoseconds
/// (xnu `task_power_info_locked` stores `rm_time_mach` unconverted; the children accumulators sum
/// those same ticks).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RusageTicks {
    pub started: u64,
    pub user: u64,
    pub system: u64,
    pub child_user: u64,
    pub child_system: u64,
}

impl RusageTicks {
    /// Each part through the timebase, once.
    pub fn convert(self, pid: u32, tick: MachTick) -> Result<LeaderRusage, ResourceUnitError> {
        Ok(LeaderRusage {
            pid,
            started: self.started,
            own: CpuTotals {
                user_us: tick.micros(self.user)?,
                sys_us: tick.micros(self.system)?,
            },
            children: CpuTotals {
                user_us: tick.micros(self.child_user)?,
                sys_us: tick.micros(self.child_system)?,
            },
        })
    }
}

/// `proc_pid_rusage(pid, RUSAGE_INFO_V4)`'s CPU fields, named by pid alone; `None` once the pid
/// names no process, reaped or never. A failure is the call's errno.
pub fn rusage_ticks(pid: libc::pid_t) -> Result<Option<RusageTicks>, i32> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::zeroed();
    // SAFETY: `info` is writable storage of the size the V4 flavor writes.
    let result =
        unsafe { libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V4, info.as_mut_ptr().cast()) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH) => Ok(None),
            Some(errno) => Err(errno),
            // `proc_pid_rusage` reports its failure through errno alone.
            None => unreachable!("last_os_error always carries an errno"),
        };
    }
    // SAFETY: a successful call filled the V4 record.
    let info = unsafe { info.assume_init() };
    Ok(Some(RusageTicks {
        started: info.ri_proc_start_abstime,
        user: info.ri_user_time,
        system: info.ri_system_time,
        child_user: info.ri_child_user_time,
        child_system: info.ri_child_system_time,
    }))
}

/// Read `observed`'s own and reaped-children CPU now, as `pid`'s reading. The start the read
/// carries proves it that leader's.
pub(super) fn read(pid: u32, observed: GroupLeader) -> Result<LeaderRusage, LeaderRusageError> {
    let ticks = rusage_ticks(observed.pgid())
        .map_err(|errno| LeaderRusageError::Read { pid, errno })?
        .ok_or(LeaderRusageError::Reaped { pid })?;
    if ticks.started != observed.birth() {
        return Err(LeaderRusageError::OtherLife {
            pid,
            born: observed.birth(),
            read: ticks.started,
        });
    }
    let tick = MachTick::read().map_err(|source| LeaderRusageError::Timebase { pid, source })?;
    ticks
        .convert(pid, tick)
        .map_err(|source| LeaderRusageError::Inexact { pid, source })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::super::tests::{cpu, leader, totals};
    use super::*;

    /// Every part of a reading is converted from Mach ticks through the timebase's rational, once.
    /// On Apple silicon's 125/3 ns tick, reading the ticks as nanoseconds understates the CPU
    /// they count about 41.67-fold, and the conversion is refused unless it uses the rational.
    /// On Intel's 1/1 tick the two agree, so a native raw-unit control proves nothing there.
    #[test]
    fn mach_ticks_convert_through_the_injected_timebase_never_as_nanoseconds() {
        let ticks = RusageTicks {
            started: 1,
            user: 6_000_569,
            system: 1_200_000,
            child_user: 24_000_000,
            child_system: 2_400_003,
        };
        let raw_us = (ticks.user + ticks.system + ticks.child_user + ticks.child_system) / 1_000;
        let tick = |numerator, denominator| MachTick {
            numerator,
            denominator: std::num::NonZeroU32::new(denominator).expect("a denominator"),
        };

        let apple = ticks.convert(9, tick(125, 3)).expect("exact");
        // 6_000_569 ticks are 250_023_708 ns, 2_400_003 ticks 100_000_125 ns.
        assert_eq!(
            (apple.own, apple.children),
            (cpu(250_023, 50_000), cpu(1_000_000, 100_000))
        );
        let converted = apple.total().expect("exact");
        let converted = converted.user_us.get() + converted.sys_us.get();
        assert_eq!(converted, 1_400_023);
        assert_ne!(
            raw_us, converted,
            "ticks read as nanoseconds ({raw_us} us) are not the CPU they count"
        );

        let intel = ticks.convert(9, tick(1, 1)).expect("exact");
        let intel = intel.total().expect("exact");
        assert_eq!(
            intel.user_us.get() + intel.sys_us.get(),
            raw_us,
            "a 1/1 tick is a nanosecond"
        );

        assert_eq!(
            RusageTicks {
                user: u64::MAX,
                ..ticks
            }
            .convert(9, tick(u32::MAX, 1)),
            Err(ResourceUnitError::Inexact {
                unit: "cpuUs",
                value: u128::from(u64::MAX) * u128::from(u32::MAX) / 1_000,
            }),
            "a count no projection holds exactly is refused, never truncated"
        );
    }

    proptest! {
        /// However long a loaded host made each leader and its reaped children run, the job's CPU
        /// is each leader's interval counted once, at its last reading, and never more than the
        /// kernel's own total of that tree: per interval, the leader's and its children's user
        /// ticks summed and converted once, and likewise their system ticks, as `wait4` reports on
        /// reaping it. The job converts the four counters one by one, so it may fall short of that
        /// by under a microsecond per sum, never exceed it. Timebase and counters are generated:
        /// no host, clock or load decides the verdict.
        #[test]
        fn the_job_s_cpu_is_each_interval_once_and_never_above_the_kernel_s_reaped_total(
            numerator in 1_u32..=1_000,
            denominator in 1_u32..=1_000,
            intervals in prop::collection::vec(
                prop::collection::vec(
                    (0..1_u64 << 36, 0..1_u64 << 36, 0..1_u64 << 36, 0..1_u64 << 36),
                    1..6,
                ),
                1..4,
            ),
        ) {
            let tick = MachTick {
                numerator,
                denominator: std::num::NonZeroU32::new(denominator).expect("a denominator"),
            };
            // The kernel's figure, from the ticks themselves, not through the conversion under test.
            let kernel_us = |ticks: u64| {
                u128::from(ticks) * u128::from(numerator) / (u128::from(denominator) * 1_000)
            };
            let mut fold = RusageChildren::charging(leader(100));
            let (mut counted, mut kernel) = (0_u128, 0_u128);
            for (index, increments) in intervals.iter().enumerate() {
                let pid = 100 + u32::try_from(index).expect("a pid");
                if index > 0 {
                    fold.lead(leader(pid));
                }
                let mut ticks = RusageTicks {
                    started: u64::from(pid) * 10,
                    user: 0,
                    system: 0,
                    child_user: 0,
                    child_system: 0,
                };
                for (step, &(user, system, child_user, child_system)) in increments.iter().enumerate() {
                    ticks.user += user;
                    ticks.system += system;
                    ticks.child_user += child_user;
                    ticks.child_system += child_system;
                    let reading = ticks.convert(pid, tick).expect("exact");
                    if step + 1 == increments.len() {
                        fold.end(Ok(reading));
                    } else {
                        totals(fold.account(|_| Ok(reading)));
                    }
                }
                let last = ticks.convert(pid, tick).expect("exact").total().expect("exact");
                counted += u128::from(last.user_us.get() + last.sys_us.get());
                kernel += kernel_us(ticks.user + ticks.child_user)
                    + kernel_us(ticks.system + ticks.child_system);
            }
            let job = totals(fold.account(|_| panic!("every interval closed")));
            let job = u128::from(job.user_us.get() + job.sys_us.get());
            prop_assert_eq!(job, counted, "each interval counts once, at its last reading");
            prop_assert!(
                job <= kernel,
                "the job's {} us exceed the kernel's reaped {} us",
                job,
                kernel
            );
            let sums = 2 * u128::try_from(intervals.len()).expect("a count");
            prop_assert!(
                kernel - job <= sums,
                "the job's {} us fall {} us short of the kernel's {} us, more than its {} sums truncate",
                job,
                kernel - job,
                kernel,
                sums
            );
        }
    }
}
