//! A job's cumulative leader-own plus reaped-children CPU (07_api.md, "Complete job accounting
//! and observation reconciliation"). This is what each process that led the job cost itself,
//! plus every child it reaped. It is not a complete job total; its limits are below.
//!
//! When a parent reaps a child, the kernel adds the child's own CPU and its reaped children's to
//! the parent's children total. A descendant that forked, ran and was reaped between two samples
//! therefore still reaches the leader's children total once each parent up to the leader reaped
//! it. The leader's parent holds it unreaped until the job's terminal sample, so the kernel
//! answers for it after its exit too, and every read carries the leader's start, which fences it
//! to that one life.
//!
//! A cold host's activation leads a job first. Its interval closes at the activation's end, read
//! by the host's parent while it still holds the host ([`ActivationEnded`]); the host's later
//! work -- serving the command, then other jobs -- is never charged. The command then leads,
//! charged from its own start. Each interval counts once.
//!
//! The source misses three kinds of descendant: one still running (it reaches the total only
//! once each parent up to the leader has reaped it), one that exited and was not yet reaped,
//! and an orphan reparented to another reaper, which never reaches it. A leader that has exited is
//! read from its held zombie, whose counters the kernel fixed at the exit, so it gains no CPU
//! from orphans that run on after it. The process tree's observed rows state that difference;
//! this source never guesses it. Nor does it count bytes: the children totals hold no disk I/O,
//! so the job's storage I/O is unavailable from it, never zero.
//!
//! A kernel may read a counter lower than it read it before (macOS does: `macos.rs`). CPU spent
//! is never unspent, so a lower reading is the kernel's, not the job's: each counter holds the
//! highest value read of it, and the job's total never falls. A dip is therefore never an error,
//! and the total may exceed the leader's final reading by at most the deepest dip.
//!
//! The read is the platform's (`macos.rs`: `proc_pid_rusage`, in Mach ticks; `linux.rs`:
//! `/proc/<pid>/stat`, in clock ticks), selected here and nowhere else. Each says there what its
//! kernel adds to a parent and when. The fold below reads nothing, so it is tested on every host.
//!
//! [`ActivationEnded`]: super::supervisor::ProcessEvent::ActivationEnded

use crate::api::resources::{CpuTotals, ResourceUnitError};
use crate::runtime::job_groups::Birth;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

pub use platform::TimebaseError;

/// One read of a leader's own and reaped-children CPU, each part converted from the kernel's
/// ticks once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaderRusage {
    pub pid: u32,
    /// The life that was read: the leader's birth as the read itself carried it
    /// ([`crate::runtime::job_groups::GroupLeader::birth`]).
    pub started: u64,
    /// The leader's own CPU.
    pub own: CpuTotals,
    /// Every child it reaped, with theirs.
    pub children: CpuTotals,
}

impl LeaderRusage {
    /// What the leader's interval cost the job: its own CPU and its reaped children's.
    pub fn total(&self) -> Result<CpuTotals, ResourceUnitError> {
        self.own.checked_add(self.children)
    }
}

/// Why a leader's counters could not be read as its own.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum LeaderRusageError {
    #[error(
        "job leader {pid} was not identified at its start ({reason}), so no rusage read can be \
         proven its own"
    )]
    Unidentified { pid: u32, reason: String },
    #[error("{} of job leader {pid} failed: {}", platform::READ, errno_text(.errno))]
    Read { pid: u32, errno: i32 },
    /// The read failed with no errno: what the kernel answered is not the record it documents.
    #[error(
        "{} of job leader {pid} answered no usable record: {reason}",
        platform::READ
    )]
    Malformed { pid: u32, reason: String },
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
    platform::read(leader.pid(), observed)
}

/// Why the job's totals cannot be stated. Kept once it happened: a lost interval is lost for the
/// rest of the job, never replaced by a smaller total.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum AccountingError {
    #[error(transparent)]
    Leader(#[from] LeaderRusageError),
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

    /// The job's CPU totals now: every closed interval, and the charged leader as `read` finds it.
    /// The source has no storage byte totals.
    pub fn account(
        &mut self,
        read: impl FnOnce(&Birth) -> Result<LeaderRusage, LeaderRusageError>,
    ) -> Result<CpuTotals, AccountingError> {
        let closed = self.closed.clone()?;
        match &mut self.charged {
            None => Ok(closed),
            Some(charged) => {
                let reading = charged.checked(read(&charged.leader)?)?;
                charged.latest = Some(reading);
                Ok(closed.checked_add(reading.total()?)?)
            }
        }
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
            | LeaderRusageError::Malformed { pid, .. }
            | LeaderRusageError::Reaped { pid }
            | LeaderRusageError::OtherLife { pid, .. }
            | LeaderRusageError::Timebase { pid, .. }
            | LeaderRusageError::Inexact { pid, .. } => *pid,
        }
    }
}

impl Charged {
    /// `reading` if it is of this leader, each counter at the highest value read of it in this
    /// interval: a counter read lower than before holds where it was (module docs).
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
        let highest = |before: CpuTotals, after: CpuTotals| CpuTotals {
            user_us: before.user_us.max(after.user_us),
            sys_us: before.sys_us.max(after.sys_us),
        };
        Ok(LeaderRusage {
            own: highest(latest.own, reading.own),
            children: highest(latest.children, reading.children),
            ..reading
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::resources::CpuMicros;

    pub(super) fn leader(pid: u32) -> Birth {
        Birth::Observed(
            crate::runtime::job_groups::GroupLeader::from_parts(
                i32::try_from(pid).expect("pid"),
                u64::from(pid) * 10,
                u64::from(pid) * 10,
            )
            .expect("a leader"),
        )
    }

    pub(super) fn cpu(user: u64, sys: u64) -> CpuTotals {
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

    pub(super) fn totals(accounting: Result<CpuTotals, AccountingError>) -> CpuTotals {
        accounting.expect("the job's totals")
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

    /// A reading of another process is refused and changes nothing: the next sound reading still
    /// counts from the last accepted one.
    #[test]
    fn a_reading_of_another_leader_is_refused() {
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

    /// xnu does not keep each CPU counter of a live process from falling between two
    /// `proc_pid_rusage` reads: a deployed job leader's own user CPU read 239 us, then 222 us.
    /// The dip is the platform's, not the job's: it fails nothing, each counter holds the highest
    /// value read, and the job's total never falls, through its interval's end.
    #[test]
    fn a_counter_read_lower_than_before_fails_nothing_and_the_total_never_falls() {
        let mut fold = RusageChildren::charging(leader(100));
        assert_eq!(
            totals(fold.account(|_| Ok(reading(100, (239, 40), (500, 50))))),
            cpu(739, 90)
        );
        assert_eq!(
            totals(fold.account(|_| Ok(reading(100, (222, 41), (500, 50))))),
            cpu(739, 91),
            "the fallen own user CPU holds at 239 us; the risen system CPU counts"
        );
        assert_eq!(
            totals(fold.account(|_| Ok(reading(100, (250, 41), (500, 50))))),
            cpu(750, 91),
            "growth past the high mark counts from it, once"
        );
        fold.end(Ok(reading(100, (245, 41), (499, 50))));
        assert_eq!(
            totals(fold.account(|_| panic!("nothing leads"))),
            cpu(750, 91),
            "an end read lower closes the interval at its high mark"
        );
    }

    /// A leader no one identified cannot be read as itself; a pid that names another life is
    /// never read as the leader's.
    #[test]
    fn only_the_identified_life_of_a_held_leader_is_read() {
        use crate::runtime::job_groups::GroupLeader;

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
        let mine = GroupLeader::observe(me).expect("observe this process");
        let impostor = Birth::Observed(
            GroupLeader::from_parts(i32::try_from(me).expect("pid"), mine.birth() + 1, 0)
                .expect("a leader"),
        );
        assert_eq!(
            read_leader(&impostor),
            Err(LeaderRusageError::OtherLife {
                pid: me,
                born: mine.birth() + 1,
                read: mine.birth(),
            })
        );
    }
}
