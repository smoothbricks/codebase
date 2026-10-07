//! A job's cumulative leader-own plus reaped-children CPU on macOS (07_api.md, "Complete job
//! accounting and observation reconciliation"). This is what each process that led the job cost
//! itself, plus every child it reaped. It is not a complete job total; its limits are below.
//!
//! When a parent reaps a child, xnu adds the child's own CPU and its reaped children's to the
//! parent's `ri_child_*` (`update_rusage_info_child` in `reap_child_locked`; a parent that
//! ignores SIGCHLD gets the same when the kernel reaps for it). A descendant that forked,
//! ran and was reaped between two samples therefore still reaches the leader's children total
//! once each parent up to the leader reaped it. The leader's parent holds it unreaped until the
//! job's terminal sample, so `proc_pid_rusage` answers for it after its exit too, and every read
//! carries the leader's start, which fences it to that one life.
//!
//! A cold host's activation leads a job first. Its interval closes at the activation's end, read
//! by the host's parent while it still holds the host ([`ActivationEnded`]); the host's later
//! work -- serving the command, then other jobs -- is never charged. The command then leads,
//! charged from its own start. Each interval counts once.
//!
//! The source misses three kinds of descendant: one still running (it reaches the total only
//! once each parent up to the leader has reaped it), one that exited and was not yet reaped,
//! and an orphan reparented to another reaper, which never reaches it. A leader that has exited is
//! read from its held zombie, whose rusage xnu fixed at the exit (`proc_prepareexit`), so it
//! gains no CPU from orphans that run on after it. The process tree's observed rows state that
//! difference; this source never guesses it. Nor does it count bytes: `ri_child_*` holds no disk
//! I/O, so the job's storage I/O is unavailable, never zero.
//!
//! [`ActivationEnded`]: super::supervisor::ProcessEvent::ActivationEnded

use crate::api::resources::{CpuTotals, JobAccounting, ResourceUnitError};
use crate::runtime::job_groups::Birth;
use crate::runtime::process_usage::{MachTick, TimebaseError};

/// One read of a leader's `proc_pid_rusage`, each part converted from Mach ticks once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaderRusage {
    pub pid: u32,
    /// `ri_proc_start_abstime`: the life that was read.
    pub started: u64,
    /// `ri_user_time`/`ri_system_time`: the leader's own CPU.
    pub own: CpuTotals,
    /// `ri_child_user_time`/`ri_child_system_time`: every child it reaped, with theirs.
    pub children: CpuTotals,
}

impl LeaderRusage {
    /// What the leader's interval cost the job: its own CPU and its reaped children's.
    pub fn total(&self) -> Result<CpuTotals, ResourceUnitError> {
        self.own.checked_add(self.children)
    }
}

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

/// Why a leader's rusage could not be read as its own.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum LeaderRusageError {
    #[error(
        "job leader {pid} was not identified at its start ({reason}), so no rusage read can be \
         proven its own"
    )]
    Unidentified { pid: u32, reason: String },
    #[error("proc_pid_rusage of job leader {pid} failed: {}", errno_text(.errno))]
    Read { pid: u32, errno: i32 },
    #[error(
        "job leader {pid} was reaped before its rusage was read: its CPU and its reaped \
         children's are lost to this job"
    )]
    Reaped { pid: u32 },
    #[error("pid {pid} names a process started at {read}, not the job leader started at {born}")]
    OtherLife { pid: u32, born: u64, read: u64 },
    #[error("job leader {pid}'s rusage could not be converted: {source}")]
    Timebase { pid: u32, source: TimebaseError },
    #[error(
        "job leader {pid}'s rusage holds more CPU than every projection holds exactly: {source}"
    )]
    Inexact { pid: u32, source: ResourceUnitError },
}

fn errno_text(errno: &i32) -> std::io::Error {
    std::io::Error::from_raw_os_error(*errno)
}

/// Read `leader`'s own and reaped-children CPU now. Its parent holds it unreaped, exited or not,
/// so its pid names it; the start the read carries proves that it did.
pub fn read_leader(leader: &Birth) -> Result<LeaderRusage, LeaderRusageError> {
    let observed = match leader {
        Birth::Observed(observed) => *observed,
        Birth::Unobserved { pid, reason } => {
            return Err(LeaderRusageError::Unidentified {
                pid: *pid,
                reason: reason.clone(),
            });
        }
    };
    let pid = leader.pid();
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

/// Why the job's totals cannot be stated. Kept once it happened: a lost interval is lost for the
/// rest of the job, never replaced by a smaller total.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum AccountingError {
    #[error(transparent)]
    Leader(#[from] LeaderRusageError),
    #[error("job leader {pid}'s {counter} fell from {before} us to {after} us")]
    Regressed {
        pid: u32,
        counter: &'static str,
        before: u64,
        after: u64,
    },
    #[error("process {read} was read as the job's leader, but {charged} is charged")]
    OtherLeader { read: u32, charged: u32 },
    #[error("process {pid}'s interval ended while no leader of the job was charged")]
    NoneCharged { pid: u32 },
    #[error(
        "job leader {pid} handed the lead to {next} before its interval was closed: its CPU \
         would be lost or counted twice"
    )]
    Unclosed { pid: u32, next: u32 },
    #[error("the job's CPU totals exceed what every projection holds exactly: {0}")]
    Inexact(#[from] ResourceUnitError),
}

/// The leader charged now, and its latest reading.
#[derive(Debug)]
struct Charged {
    leader: Birth,
    latest: Option<LeaderRusage>,
}

/// The fold of a job's leader readings into its totals.
#[derive(Debug)]
pub struct RusageChildren {
    /// What leaders that no longer lead cost, each counted once at its interval's end: a cold
    /// host's activation. An end that could not be read stays its error.
    closed: Result<CpuTotals, AccountingError>,
    /// The leader charged now; `None` between the activation's end and the command's start, and
    /// after an activation that started no command.
    charged: Option<Charged>,
}

impl RusageChildren {
    /// The job's first owned process leads it, charged from its spawn.
    pub fn charging(leader: Birth) -> Self {
        Self {
            closed: Ok(CpuTotals::ZERO),
            charged: Some(Charged {
                leader,
                latest: None,
            }),
        }
    }

    /// `next` takes the lead, charged from its own start. The previous leader's interval must
    /// have been closed by its end: one still open would be lost, or counted again by a read.
    pub fn lead(&mut self, next: Birth) {
        if let Some(previous) = self.charged.take() {
            self.fail(AccountingError::Unclosed {
                pid: previous.leader.pid(),
                next: next.pid(),
            });
        }
        self.charged = Some(Charged {
            leader: next,
            latest: None,
        });
    }

    /// The charged leader's interval ended with `last`, read by its parent while it still held
    /// it: that interval counts once, and nothing it does later.
    pub fn end(&mut self, last: Result<LeaderRusage, LeaderRusageError>) {
        let pid = last
            .as_ref()
            .map_or_else(Self::failed_pid, |reading| reading.pid);
        let Some(charged) = self.charged.take() else {
            self.fail(AccountingError::NoneCharged { pid });
            return;
        };
        let interval = last
            .map_err(AccountingError::from)
            .and_then(|last| Ok(charged.checked(last)?.total()?));
        self.closed = match (&self.closed, interval) {
            // The first loss stands.
            (Err(_), _) => return,
            (Ok(_), Err(error)) => Err(error),
            (Ok(closed), Ok(interval)) => closed.checked_add(interval).map_err(Into::into),
        };
    }

    /// The job's totals now: every closed interval, and the charged leader as `read` finds it.
    pub fn account(
        &mut self,
        read: impl FnOnce(&Birth) -> Result<LeaderRusage, LeaderRusageError>,
    ) -> Result<JobAccounting, AccountingError> {
        let closed = self.closed.clone()?;
        let cpu = match &mut self.charged {
            None => closed,
            Some(charged) => {
                let reading = charged.checked(read(&charged.leader)?)?;
                charged.latest = Some(reading);
                closed.checked_add(reading.total()?)?
            }
        };
        Ok(JobAccounting::MacOsRusageChildren { cpu, io: None })
    }

    fn fail(&mut self, error: AccountingError) {
        if self.closed.is_ok() {
            self.closed = Err(error);
        }
    }

    fn failed_pid(error: &LeaderRusageError) -> u32 {
        match error {
            LeaderRusageError::Unidentified { pid, .. }
            | LeaderRusageError::Read { pid, .. }
            | LeaderRusageError::Reaped { pid }
            | LeaderRusageError::OtherLife { pid, .. }
            | LeaderRusageError::Timebase { pid, .. }
            | LeaderRusageError::Inexact { pid, .. } => *pid,
        }
    }
}

impl Charged {
    /// `reading` if it is of this leader and no counter of it fell since the last.
    fn checked(&self, reading: LeaderRusage) -> Result<LeaderRusage, AccountingError> {
        let charged = self.leader.pid();
        if reading.pid != charged {
            return Err(AccountingError::OtherLeader {
                read: reading.pid,
                charged,
            });
        }
        let Some(latest) = self.latest else {
            return Ok(reading);
        };
        for (counter, before, after) in [
            ("own user CPU", latest.own.user_us, reading.own.user_us),
            ("own system CPU", latest.own.sys_us, reading.own.sys_us),
            (
                "children user CPU",
                latest.children.user_us,
                reading.children.user_us,
            ),
            (
                "children system CPU",
                latest.children.sys_us,
                reading.children.sys_us,
            ),
        ] {
            if after < before {
                return Err(AccountingError::Regressed {
                    pid: charged,
                    counter,
                    before: before.get(),
                    after: after.get(),
                });
            }
        }
        Ok(reading)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::resources::CpuMicros;

    fn leader(pid: u32) -> Birth {
        Birth::Observed(
            crate::runtime::job_groups::GroupLeader::from_parts(
                i32::try_from(pid).expect("pid"),
                u64::from(pid) * 10,
            )
            .expect("a leader"),
        )
    }

    fn cpu(user: u64, sys: u64) -> CpuTotals {
        CpuTotals {
            user_us: CpuMicros::new(user).expect("exact"),
            sys_us: CpuMicros::new(sys).expect("exact"),
        }
    }

    /// A reading of `pid` with `own` and `children` (user, sys) microseconds.
    fn reading(pid: u32, own: (u64, u64), children: (u64, u64)) -> LeaderRusage {
        LeaderRusage {
            pid,
            started: u64::from(pid) * 10,
            own: cpu(own.0, own.1),
            children: cpu(children.0, children.1),
        }
    }

    fn totals(accounting: Result<JobAccounting, AccountingError>) -> CpuTotals {
        match accounting.expect("the job's totals") {
            JobAccounting::MacOsRusageChildren { cpu, io } => {
                assert_eq!(io, None, "the source has no byte totals, never zero ones");
                cpu
            }
        }
    }

    /// The activation counts up to its end and the command from its start, each once: the
    /// host's later work never enters, and no read of the command repeats the activation.
    #[test]
    fn an_activation_and_its_command_each_count_once() {
        let (host, command) = (leader(100), leader(200));
        let mut fold = RusageChildren::charging(host.clone());
        assert_eq!(
            totals(fold.account(|_| Ok(reading(100, (10, 5), (40, 20))))),
            cpu(50, 25)
        );
        fold.end(Ok(reading(100, (12, 6), (300, 100))));
        // Between the activation's end and the command's start nothing is charged or read.
        assert_eq!(
            totals(fold.account(|_| panic!("no leader is charged"))),
            cpu(312, 106)
        );
        fold.lead(command);
        let mut read = Vec::new();
        assert_eq!(
            totals(fold.account(|leader| {
                read.push(leader.pid());
                Ok(reading(200, (1_000, 50), (2_000, 70)))
            })),
            cpu(312 + 3_000, 106 + 120)
        );
        assert_eq!(read, [200], "only the command is read once it leads");
        assert_eq!(
            totals(fold.account(|_| Ok(reading(200, (1_500, 60), (2_000, 70))))),
            cpu(312 + 3_500, 106 + 130)
        );
    }

    /// An activation that failed keeps its cost: nothing leads after it, and its last reading
    /// is the job's total.
    #[test]
    fn a_failed_activation_keeps_its_cost() {
        let mut fold = RusageChildren::charging(leader(100));
        fold.end(Ok(reading(100, (7, 3), (900, 90))));
        assert_eq!(
            totals(fold.account(|_| panic!("nothing leads"))),
            cpu(907, 93)
        );
    }

    /// A leader that takes over from one whose interval never closed, an end with nothing
    /// charged, and an end that could not be read each leave the job without totals, saying why:
    /// never a smaller total that drops an interval.
    #[test]
    fn a_lost_interval_is_an_error_for_the_rest_of_the_job() {
        let mut unclosed = RusageChildren::charging(leader(100));
        unclosed.lead(leader(200));
        assert_eq!(
            unclosed.account(|_| Ok(reading(200, (1, 1), (1, 1)))),
            Err(AccountingError::Unclosed {
                pid: 100,
                next: 200
            })
        );

        let mut lost = RusageChildren::charging(leader(100));
        lost.end(Err(LeaderRusageError::Reaped { pid: 100 }));
        lost.lead(leader(200));
        assert_eq!(
            lost.account(|_| Ok(reading(200, (1, 1), (1, 1)))),
            Err(AccountingError::Leader(LeaderRusageError::Reaped {
                pid: 100
            }))
        );

        let mut twice = RusageChildren::charging(leader(100));
        twice.end(Ok(reading(100, (1, 1), (1, 1))));
        twice.end(Ok(reading(100, (2, 2), (2, 2))));
        assert_eq!(
            twice.account(|_| panic!("nothing leads")),
            Err(AccountingError::NoneCharged { pid: 100 })
        );
    }

    /// A reading of another process, or one in which a counter fell, is refused and changes
    /// nothing: the next sound reading still counts from the last accepted one.
    #[test]
    fn a_reading_of_another_leader_or_a_falling_counter_is_refused() {
        let mut fold = RusageChildren::charging(leader(100));
        totals(fold.account(|_| Ok(reading(100, (10, 10), (500, 50)))));
        assert_eq!(
            fold.account(|_| Ok(reading(101, (10, 10), (500, 50)))),
            Err(AccountingError::OtherLeader {
                read: 101,
                charged: 100
            })
        );
        assert_eq!(
            fold.account(|_| Ok(reading(100, (10, 10), (499, 50)))),
            Err(AccountingError::Regressed {
                pid: 100,
                counter: "children user CPU",
                before: 500,
                after: 499,
            })
        );
        assert_eq!(
            fold.account(|_| Err(LeaderRusageError::Read {
                pid: 100,
                errno: libc::EPERM
            })),
            Err(AccountingError::Leader(LeaderRusageError::Read {
                pid: 100,
                errno: libc::EPERM
            }))
        );
        assert_eq!(
            totals(fold.account(|_| Ok(reading(100, (11, 10), (500, 50))))),
            cpu(511, 60)
        );
    }

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

    /// A leader no one identified cannot be read as itself; a pid that names another life, or
    /// none, is never read as the leader's.
    #[test]
    fn only_the_identified_life_of_a_held_leader_is_read() {
        assert_eq!(
            read_leader(&Birth::Unobserved {
                pid: 7,
                reason: "a test names no process".into()
            }),
            Err(LeaderRusageError::Unidentified {
                pid: 7,
                reason: "a test names no process".into()
            })
        );
        let me = std::process::id();
        let mine = rusage_ticks(libc::pid_t::try_from(me).expect("pid"))
            .expect("read")
            .expect("this process runs");
        let impostor = Birth::Observed(
            crate::runtime::job_groups::GroupLeader::from_parts(
                i32::try_from(me).expect("pid"),
                mine.started + 1,
            )
            .expect("a leader"),
        );
        assert_eq!(
            read_leader(&impostor),
            Err(LeaderRusageError::OtherLife {
                pid: me,
                born: mine.started + 1,
                read: mine.started,
            })
        );
    }
}
