//! A workspace supervisor's job admission, as one state machine (05_gateway.md "Host scheduling",
//! "Admission", "Held jobs"). Pure: it reads no clock, no file and no kernel. The supervisor feeds
//! it events -- a start request, a memory-pressure report from the kernel's notification, a hold's
//! grant or refusal by mbx's pool, a spawn, a cancel, a job's end -- each with the instant it
//! observed, and carries out the effects it answers, in order.
//!
//! A job is `queued` until the host's pressure is known, `waiting` while memory pressure holds it,
//! `pending` while a held job of its checkout and pool priority waits in mbx's pool ahead of it,
//! `holding` while its own hold waits there, `admitted` once it may start, `running` from its
//! spawn, and concluded when it ends, which takes it out of the ledger. Only a `background` start
//! without mbx ever waits for memory: a gate, interactive work and residents are admitted at once,
//! and an mbx project's unheld jobs are admitted at once in every class, since mbx's pool admits
//! their compiles, tests and reservations itself.
//!
//! A held job, one that declared its count in an mbx project, is admitted whole: it holds that
//! count of mbx's pool from its grant to its end. A checkout's held jobs keep request order, one
//! lane per pool priority, and one hold per lane waits in the pool at a time, so a checkout takes
//! one place in the pool's queue per lane however much it submits, and a gate never queues behind
//! its own checkout's background work. Every job that reserved is released exactly once, as it
//! leaves the ledger.
//!
//! An unheld job waits on no other job: its jobserver holds its own constant count, never a share
//! of a host ledger, so a grant made on an idle host cannot hold a later start, however long its
//! holder runs. A drop in memory pressure releases every waiter, oldest first.
//!
//! Without mbx, macOS Critical pressure ends the youngest running `background` or `resident` job,
//! one at a time: the next only once the last one ended for memory has concluded, while the level
//! is still Critical. An mbx project's jobs are never ended for memory: mbx holds further
//! admissions instead.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use cowshed_gateway_types::host_status::{DarwinPressureLevel, MemoryPressure};

use crate::api::dto::{ExecClass, JobId};
pub use crate::host_pressure::Reading;
use crate::host_pressure::{Decision, job_to_end, may_start, waits_for_memory};

/// Which authority admits a job's CPU and memory (05_gateway.md "Host scheduling").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ledger {
    /// The project has the mbx capability: mbx's one pool admits every compile, test and
    /// reservation, under its own memory accounting. `hold` is the count a held job holds of that
    /// pool for its life (05_gateway.md "Held jobs"); an unheld job's leaves enter it one by one.
    Mbx { hold: Option<NonZeroUsize> },
    /// No mbx: the job's own jobserver bounds its tree, the kernel weighs the classes, and memory
    /// pressure holds background starts.
    Jobservers,
}

/// A job's priority in mbx's pool: a land gate's `normal`, every other class's `low`
/// (05_gateway.md "Classes"). Each is a lane of a checkout's held jobs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolPriority {
    Normal,
    Low,
}

impl PoolPriority {
    pub const fn of(class: ExecClass) -> Self {
        match class {
            ExecClass::Gate => Self::Normal,
            ExecClass::Background
            | ExecClass::Interactive
            | ExecClass::Resident
            | ExecClass::InteractiveResident => Self::Low,
        }
    }

    /// mbx's own word for it, as `mbx reserve --priority` and `MBX_SCHEDULER_PRIORITY` take it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Low => "low",
        }
    }

    const fn lane(self) -> usize {
        match self {
            Self::Normal => 0,
            Self::Low => 1,
        }
    }
}

/// What the supervisor observed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    /// A job asks to start.
    Requested {
        job: JobId,
        class: ExecClass,
        ledger: Ledger,
    },
    /// The kernel reported the host's memory pressure: the first report, or a change.
    Pressure(Reading),
    /// mbx's pool granted the job's hold.
    Granted { job: JobId },
    /// The job's hold ended without a grant: mbx refused it, or could not run.
    HoldRefused { job: JobId, error: String },
    /// An admitted job's first process was dispatched.
    Spawned { job: JobId },
    /// A job was withdrawn or killed. A queued, waiting, pending or holding one leaves at once;
    /// one that was admitted leaves when it concludes.
    Cancelled { job: JobId },
    /// A job ended: its command exited, it failed, or it was killed. Its jobserver goes with it.
    Concluded { job: JobId },
}

/// What the supervisor does about an event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    /// Make the job's jobserver and spawn its command.
    Admit { job: JobId },
    /// Say on the job's stderr that it waits for memory pressure to drop.
    WaitStarted {
        job: JobId,
        pressure: MemoryPressure,
    },
    /// Say on the job's stderr that its wait ended, and how.
    WaitEnded {
        job: JobId,
        waited: Duration,
        ok: bool,
    },
    /// Fail the job before its command, `cpuAdmission`: its start needs a pressure reading the
    /// host cannot give.
    Refuse { job: JobId, error: String },
    /// End the job for memory through the ordinary kill.
    EndForMemory {
        job: JobId,
        level: DarwinPressureLevel,
    },
    /// Ask mbx's pool to hold `cpus` for the job at `priority`.
    Reserve {
        job: JobId,
        cpus: NonZeroUsize,
        priority: PoolPriority,
    },
    /// End the job's hold, granted or still waiting.
    Release { job: JobId },
    /// Say on the job's stderr that it waits for the host's pool, `ahead` being its own
    /// checkout's held jobs before it in its lane.
    HoldWaitStarted {
        job: JobId,
        cpus: NonZeroUsize,
        priority: PoolPriority,
        ahead: usize,
    },
    /// Say on the job's stderr that its wait for the pool ended, and how.
    HoldWaitEnded {
        job: JobId,
        waited: Duration,
        ok: bool,
    },
    /// Fail the job before its command, `cpuAdmission`: its hold ended without a grant.
    RefuseHold { job: JobId, error: String },
}

/// Where a job stands. A concluded job is no longer in the ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Queued,
    Waiting {
        since: Instant,
    },
    /// A held job behind an earlier held job of its checkout and lane, which waits in the pool.
    Pending {
        since: Instant,
    },
    /// A held job whose hold waits in mbx's pool.
    Holding {
        since: Instant,
    },
    Admitted,
    Running {
        spawned: Instant,
    },
    EndingForMemory,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    class: ExecClass,
    ledger: Ledger,
    phase: Phase,
}

impl Entry {
    /// The count the job holds of mbx's pool, when it is a held job.
    fn hold(&self) -> Option<NonZeroUsize> {
        match self.ledger {
            Ledger::Mbx { hold } => hold,
            Ledger::Jobservers => None,
        }
    }

    fn lane(&self) -> usize {
        PoolPriority::of(self.class).lane()
    }
}

/// One pool priority's held jobs of a checkout: the one whose hold waits in the pool, and the
/// rest in request order behind it.
#[derive(Debug, Default)]
struct Lane {
    holding: Option<JobId>,
    pending: VecDeque<JobId>,
}

/// One supervisor's admission ledger.
#[derive(Debug, Default)]
pub struct Admission {
    pressure: Option<Reading>,
    jobs: BTreeMap<JobId, Entry>,
    /// Indexed by [`PoolPriority::lane`].
    lanes: [Lane; 2],
}

impl Admission {
    /// Where `job` stands, or `None` once it concluded.
    pub fn phase(&self, job: JobId) -> Option<Phase> {
        self.jobs.get(&job).map(|entry| entry.phase)
    }

    /// Apply one event observed at `now`; the answer is what the supervisor does, in order.
    pub fn step(&mut self, now: Instant, event: Event) -> Vec<Effect> {
        let mut effects = Vec::new();
        match event {
            Event::Requested { job, class, ledger } => {
                let entry = Entry {
                    class,
                    ledger,
                    phase: Phase::Queued,
                };
                let entry = match ledger {
                    Ledger::Mbx { hold: Some(cpus) } => Some(Entry {
                        phase: self.enter(job, class, cpus, now, &mut effects),
                        ..entry
                    }),
                    Ledger::Jobservers if waits_for_memory(class) => {
                        under_pressure(job, entry, self.pressure.as_ref(), now, &mut effects)
                    }
                    Ledger::Mbx { hold: None } | Ledger::Jobservers => {
                        effects.push(Effect::Admit { job });
                        Some(Entry {
                            phase: Phase::Admitted,
                            ..entry
                        })
                    }
                };
                if let Some(entry) = entry {
                    self.jobs.insert(job, entry);
                }
            }
            Event::Pressure(reading) => {
                self.pressure = Some(reading);
                let pressure = self.pressure.as_ref();
                self.jobs.retain(|&job, entry| {
                    match under_pressure(job, *entry, pressure, now, &mut effects) {
                        Some(next) => {
                            *entry = next;
                            true
                        }
                        None => false,
                    }
                });
                self.end_for_memory(&mut effects);
            }
            Event::Granted { job } => {
                if let Some(entry) = self.jobs.get_mut(&job)
                    && let Phase::Holding { since } = entry.phase
                {
                    entry.phase = Phase::Admitted;
                    let lane = entry.lane();
                    effects.push(Effect::HoldWaitEnded {
                        job,
                        waited: now.saturating_duration_since(since),
                        ok: true,
                    });
                    effects.push(Effect::Admit { job });
                    self.lanes[lane].holding = None;
                    self.promote(lane, &mut effects);
                }
            }
            Event::HoldRefused { job, error } => {
                if let Some(entry) = self.jobs.get(&job).copied()
                    && let Phase::Holding { since } = entry.phase
                {
                    self.jobs.remove(&job);
                    effects.push(Effect::HoldWaitEnded {
                        job,
                        waited: now.saturating_duration_since(since),
                        ok: false,
                    });
                    effects.push(Effect::Release { job });
                    effects.push(Effect::RefuseHold { job, error });
                    let lane = entry.lane();
                    self.lanes[lane].holding = None;
                    self.promote(lane, &mut effects);
                }
            }
            Event::Spawned { job } => {
                if let Some(entry) = self.jobs.get_mut(&job)
                    && entry.phase == Phase::Admitted
                {
                    entry.phase = Phase::Running { spawned: now };
                }
            }
            Event::Cancelled { job } => {
                if let Some(entry) = self.jobs.get(&job).copied()
                    && matches!(
                        entry.phase,
                        Phase::Queued
                            | Phase::Waiting { .. }
                            | Phase::Pending { .. }
                            | Phase::Holding { .. }
                    )
                {
                    self.jobs.remove(&job);
                    self.leave(job, entry, &mut effects);
                }
            }
            Event::Concluded { job } => {
                if let Some(entry) = self.jobs.remove(&job) {
                    self.leave(job, entry, &mut effects);
                }
                self.end_for_memory(&mut effects);
            }
        }
        effects
    }

    /// Where a held job of `class` asking for `cpus` starts: in the pool when nothing of its
    /// checkout's lane waits there, else behind the lane's last job.
    fn enter(
        &mut self,
        job: JobId,
        class: ExecClass,
        cpus: NonZeroUsize,
        now: Instant,
        effects: &mut Vec<Effect>,
    ) -> Phase {
        let priority = PoolPriority::of(class);
        let lane = &mut self.lanes[priority.lane()];
        let ahead = usize::from(lane.holding.is_some()) + lane.pending.len();
        effects.push(Effect::HoldWaitStarted {
            job,
            cpus,
            priority,
            ahead,
        });
        if lane.holding.is_some() {
            lane.pending.push_back(job);
            Phase::Pending { since: now }
        } else {
            lane.holding = Some(job);
            effects.push(Effect::Reserve {
                job,
                cpus,
                priority,
            });
            Phase::Holding { since: now }
        }
    }

    /// The oldest pending held job of `lane` enters the pool, now that none of the lane waits
    /// there.
    fn promote(&mut self, lane: usize, effects: &mut Vec<Effect>) {
        let Some(job) = self.lanes[lane].pending.pop_front() else {
            return;
        };
        let entry = self
            .jobs
            .get_mut(&job)
            .expect("a lane's pending job is in the ledger");
        let (Phase::Pending { since }, Some(cpus)) = (entry.phase, entry.hold()) else {
            unreachable!("a lane's pending job is a held job in phase pending");
        };
        entry.phase = Phase::Holding { since };
        self.lanes[lane].holding = Some(job);
        effects.push(Effect::Reserve {
            job,
            cpus,
            priority: PoolPriority::of(entry.class),
        });
    }

    /// `job`, just taken out of the ledger, leaves its lane and the pool: a held job that
    /// reserved is released, and when its hold was the lane's waiter, the lane's next enters.
    fn leave(&mut self, job: JobId, entry: Entry, effects: &mut Vec<Effect>) {
        if entry.hold().is_none() {
            return;
        }
        let lane = entry.lane();
        match entry.phase {
            Phase::Pending { .. } => self.lanes[lane].pending.retain(|&pending| pending != job),
            Phase::Holding { .. } => {
                effects.push(Effect::Release { job });
                self.lanes[lane].holding = None;
                self.promote(lane, effects);
            }
            Phase::Admitted | Phase::Running { .. } | Phase::EndingForMemory => {
                effects.push(Effect::Release { job });
            }
            Phase::Queued | Phase::Waiting { .. } => {}
        }
    }

    /// At macOS Critical, end the youngest running job of the jobserver ledger nobody waits on,
    /// unless one ended for memory has not concluded yet.
    fn end_for_memory(&mut self, effects: &mut Vec<Effect>) {
        let Some(Reading::Known(MemoryPressure::Darwin { level })) = self.pressure else {
            return;
        };
        if self
            .jobs
            .values()
            .any(|entry| entry.phase == Phase::EndingForMemory)
        {
            return;
        }
        let running = self
            .jobs
            .iter()
            .filter_map(|(&job, entry)| match entry.phase {
                Phase::Running { spawned } if entry.ledger == Ledger::Jobservers => {
                    Some((job, entry.class, spawned))
                }
                _ => None,
            });
        if let Some(job) = job_to_end(level, running)
            && let Some(entry) = self.jobs.get_mut(&job)
        {
            entry.phase = Phase::EndingForMemory;
            effects.push(Effect::EndForMemory { job, level });
        }
    }
}

/// Where `entry`, a start that waits for memory, stands under `pressure`: a queued or waiting
/// start is decided by a known reading, and one the host cannot give refuses it (`None`, out of
/// the ledger). Any other phase is left as it is.
fn under_pressure(
    job: JobId,
    entry: Entry,
    pressure: Option<&Reading>,
    now: Instant,
    effects: &mut Vec<Effect>,
) -> Option<Entry> {
    let since = match entry.phase {
        Phase::Queued => None,
        Phase::Waiting { since } => Some(since),
        Phase::Pending { .. }
        | Phase::Holding { .. }
        | Phase::Admitted
        | Phase::Running { .. }
        | Phase::EndingForMemory => return Some(entry),
    };
    let phase = match pressure {
        None => Phase::Queued,
        Some(Reading::Unreadable(error)) => {
            if let Some(since) = since {
                effects.push(Effect::WaitEnded {
                    job,
                    waited: now.saturating_duration_since(since),
                    ok: false,
                });
            }
            effects.push(Effect::Refuse {
                job,
                error: error.clone(),
            });
            return None;
        }
        Some(Reading::Known(pressure)) => match (may_start(*pressure), since) {
            (Decision::Start, since) => {
                if let Some(since) = since {
                    effects.push(Effect::WaitEnded {
                        job,
                        waited: now.saturating_duration_since(since),
                        ok: true,
                    });
                }
                effects.push(Effect::Admit { job });
                Phase::Admitted
            }
            (Decision::Wait { pressure }, None) => {
                effects.push(Effect::WaitStarted { job, pressure });
                Phase::Waiting { since: now }
            }
            (Decision::Wait { .. }, Some(since)) => Phase::Waiting { since },
        },
    };
    Some(Entry { phase, ..entry })
}

#[cfg(test)]
mod tests {
    use cowshed_gateway_types::host_status::CentiPercent;

    use super::*;

    fn id(value: u64) -> JobId {
        JobId::new(value).expect("a job id within range")
    }

    fn darwin(level: DarwinPressureLevel) -> Event {
        Event::Pressure(Reading::Known(MemoryPressure::Darwin { level }))
    }

    fn psi(centi: u16) -> Event {
        Event::Pressure(Reading::Known(MemoryPressure::Psi {
            some_avg10: CentiPercent::new(centi).expect("a centi-percent"),
        }))
    }

    fn ask(job: u64, class: ExecClass, ledger: Ledger) -> Event {
        Event::Requested {
            job: id(job),
            class,
            ledger,
        }
    }

    const NORMAL: DarwinPressureLevel = DarwinPressureLevel::Normal;
    const WARNING: DarwinPressureLevel = DarwinPressureLevel::Warning;
    const CRITICAL: DarwinPressureLevel = DarwinPressureLevel::Critical;
    use ExecClass::{Background, Gate, Interactive, InteractiveResident, Resident};
    use Ledger::Jobservers;
    /// An mbx project's job that declared no hold.
    const MBX: Ledger = Ledger::Mbx { hold: None };

    fn n(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).expect("a positive count")
    }

    /// An mbx project's job that holds `cpus` of the pool.
    fn held(cpus: usize) -> Ledger {
        Ledger::Mbx {
            hold: Some(n(cpus)),
        }
    }

    fn warning() -> MemoryPressure {
        MemoryPressure::Darwin { level: WARNING }
    }

    /// Run `events`, one second apart from `at`, and answer each one's effects.
    fn run(admission: &mut Admission, at: Instant, events: Vec<Event>) -> Vec<Vec<Effect>> {
        events
            .into_iter()
            .enumerate()
            .map(|(second, event)| admission.step(at + Duration::from_secs(second as u64), event))
            .collect()
    }

    /// Only a background start without mbx waits for memory: every other class, and every class
    /// of an mbx project, is admitted at once whatever the host reports, even when it cannot be
    /// read. A gate is never held behind background work.
    #[test]
    fn only_a_background_start_without_mbx_waits_for_memory() {
        let unreadable = || Event::Pressure(Reading::Unreadable("sysctl failed".to_owned()));
        for (pressure, class, ledger, admitted) in [
            (darwin(NORMAL), Background, Jobservers, true),
            (darwin(WARNING), Background, Jobservers, false),
            (darwin(CRITICAL), Background, Jobservers, false),
            (psi(1_000), Background, Jobservers, true),
            (psi(1_001), Background, Jobservers, false),
            (darwin(CRITICAL), Gate, Jobservers, true),
            (darwin(CRITICAL), Interactive, Jobservers, true),
            (darwin(CRITICAL), Resident, Jobservers, true),
            (darwin(CRITICAL), InteractiveResident, Jobservers, true),
            (unreadable(), Gate, Jobservers, true),
            (darwin(CRITICAL), Background, MBX, true),
            (unreadable(), Background, MBX, true),
        ] {
            let mut admission = Admission::default();
            let at = Instant::now();
            let answered = run(
                &mut admission,
                at,
                vec![pressure.clone(), ask(1, class, ledger)],
            );
            let expected = if admitted {
                vec![Effect::Admit { job: id(1) }]
            } else {
                let Event::Pressure(Reading::Known(reading)) = pressure else {
                    unreachable!("a held start names a known reading")
                };
                vec![Effect::WaitStarted {
                    job: id(1),
                    pressure: reading,
                }]
            };
            assert_eq!(answered[1], expected, "{pressure:?} {class:?} {ledger:?}");
        }
    }

    /// A start that waits for memory on a host whose pressure cannot be read fails rather than
    /// starting: an unreadable host is not shown to have room. A waiter whose next report is
    /// unreadable ends its wait `err` and fails the same way.
    #[test]
    fn an_unreadable_host_refuses_a_waiting_start() {
        let mut admission = Admission::default();
        let at = Instant::now();
        let answered = run(
            &mut admission,
            at,
            vec![
                Event::Pressure(Reading::Unreadable("sysctl failed".to_owned())),
                ask(1, Background, Jobservers),
                darwin(WARNING),
                ask(2, Background, Jobservers),
                Event::Pressure(Reading::Unreadable("sysctl failed".to_owned())),
            ],
        );
        assert_eq!(
            answered[1],
            [Effect::Refuse {
                job: id(1),
                error: "sysctl failed".to_owned()
            }]
        );
        assert_eq!(admission.phase(id(1)), None);
        assert_eq!(
            answered[4],
            [
                Effect::WaitEnded {
                    job: id(2),
                    waited: Duration::from_secs(1),
                    ok: false
                },
                Effect::Refuse {
                    job: id(2),
                    error: "sysctl failed".to_owned()
                },
            ]
        );
        assert_eq!(admission.phase(id(2)), None);
    }

    /// Before the kernel's first report a background start is queued, and that report decides it.
    #[test]
    fn the_first_report_decides_a_queued_start() {
        for (first, expected) in [
            (darwin(NORMAL), vec![Effect::Admit { job: id(1) }]),
            (
                darwin(WARNING),
                vec![Effect::WaitStarted {
                    job: id(1),
                    pressure: warning(),
                }],
            ),
        ] {
            let mut admission = Admission::default();
            let at = Instant::now();
            let answered = run(&mut admission, at, vec![ask(1, Background, Jobservers)]);
            assert_eq!(answered[0], []);
            assert_eq!(admission.phase(id(1)), Some(Phase::Queued));
            assert_eq!(admission.step(at, first), expected);
        }
    }

    /// A drop in pressure releases every waiter, oldest first, each saying how long it waited; a
    /// withdrawn waiter is gone and is not started. A report that still holds them changes nothing.
    #[test]
    fn a_drop_releases_every_waiter_oldest_first() {
        let mut admission = Admission::default();
        let at = Instant::now();
        let answered = run(
            &mut admission,
            at,
            vec![
                darwin(WARNING),
                ask(1, Background, Jobservers),
                ask(2, Background, Jobservers),
                ask(3, Background, Jobservers),
                Event::Cancelled { job: id(2) },
                darwin(CRITICAL),
                darwin(NORMAL),
            ],
        );
        assert_eq!(admission.phase(id(2)), None);
        assert_eq!(answered[4], []);
        assert_eq!(answered[5], []);
        assert_eq!(
            answered[6],
            [
                Effect::WaitEnded {
                    job: id(1),
                    waited: Duration::from_secs(5),
                    ok: true
                },
                Effect::Admit { job: id(1) },
                Effect::WaitEnded {
                    job: id(3),
                    waited: Duration::from_secs(3),
                    ok: true
                },
                Effect::Admit { job: id(3) },
            ]
        );
        assert_eq!(admission.phase(id(1)), Some(Phase::Admitted));
    }

    /// The 2026-10-19 hole: a grant made on an idle host that could never shrink held every later
    /// start behind it. Here no job holds another's start: a long holder admitted while the host
    /// was idle runs on, and a later start is admitted at once, in either ledger.
    #[test]
    fn a_long_holder_admitted_on_an_idle_host_never_holds_a_later_start() {
        for ledger in [Jobservers, MBX] {
            let mut admission = Admission::default();
            let at = Instant::now();
            let answered = run(
                &mut admission,
                at,
                vec![
                    darwin(NORMAL),
                    ask(1, Background, ledger),
                    Event::Spawned { job: id(1) },
                    ask(2, Background, ledger),
                    ask(3, Gate, ledger),
                ],
            );
            assert_eq!(answered[1], [Effect::Admit { job: id(1) }], "{ledger:?}");
            assert_eq!(answered[3], [Effect::Admit { job: id(2) }], "{ledger:?}");
            assert_eq!(answered[4], [Effect::Admit { job: id(3) }], "{ledger:?}");
            assert_eq!(
                admission.phase(id(1)),
                Some(Phase::Running {
                    spawned: at + Duration::from_secs(2)
                })
            );
        }
    }

    /// Critical pressure without mbx ends the youngest running job nobody waits on, and only one
    /// at a time: the next goes once the last has concluded while the level is still Critical,
    /// and none once it drops. A gate, interactive work and an mbx project's jobs are never ended.
    #[test]
    fn critical_ends_the_youngest_job_nobody_waits_on_one_at_a_time() {
        let mut admission = Admission::default();
        let at = Instant::now();
        let mut events = vec![darwin(NORMAL)];
        for (job, class, ledger) in [
            (1, Background, Jobservers),
            (2, Resident, Jobservers),
            (3, Gate, Jobservers),
            (4, Interactive, Jobservers),
            (5, Background, MBX),
            (6, Background, Jobservers),
        ] {
            events.push(ask(job, class, ledger));
            events.push(Event::Spawned { job: id(job) });
        }
        // Job 7 is admitted and not yet spawned: it has no command to end.
        events.push(ask(7, Background, Jobservers));
        let base = events.len();
        events.extend([
            darwin(CRITICAL),
            darwin(CRITICAL),
            Event::Concluded { job: id(6) },
            darwin(WARNING),
            Event::Concluded { job: id(2) },
            darwin(CRITICAL),
        ]);
        let answered = run(&mut admission, at, events);
        let ended = |job| {
            vec![Effect::EndForMemory {
                job: id(job),
                level: CRITICAL,
            }]
        };
        assert_eq!(answered[base], ended(6));
        assert_eq!(answered[base + 1], [], "one at a time");
        assert_eq!(
            answered[base + 2],
            ended(2),
            "the next youngest, once 6 concluded"
        );
        assert_eq!(answered[base + 3], []);
        assert_eq!(
            answered[base + 4],
            [],
            "below Critical nothing more is ended"
        );
        assert_eq!(answered[base + 5], ended(1));
        assert_eq!(admission.phase(id(1)), Some(Phase::EndingForMemory));
        assert_eq!(admission.phase(id(6)), None);
        for job in [3, 4, 5] {
            assert!(matches!(
                admission.phase(id(job)),
                Some(Phase::Running { .. })
            ));
        }
    }

    /// Linux reports PSI, which has no Critical level: nothing is ended for memory there, since the
    /// kernel throttles and kills inside the background cgroup itself.
    #[test]
    fn psi_pressure_ends_nothing() {
        let mut admission = Admission::default();
        let at = Instant::now();
        let answered = run(
            &mut admission,
            at,
            vec![
                psi(0),
                ask(1, Background, Jobservers),
                Event::Spawned { job: id(1) },
                psi(10_000),
            ],
        );
        assert_eq!(answered[3], []);
    }

    /// A job's end takes it out of the ledger in every phase, and an event naming a job the ledger
    /// does not hold changes nothing.
    #[test]
    fn a_concluded_job_leaves_the_ledger() {
        let mut admission = Admission::default();
        let at = Instant::now();
        let answered = run(
            &mut admission,
            at,
            vec![
                darwin(NORMAL),
                ask(1, Gate, Jobservers),
                Event::Spawned { job: id(1) },
                Event::Cancelled { job: id(1) },
                Event::Concluded { job: id(1) },
                Event::Spawned { job: id(9) },
                Event::Concluded { job: id(9) },
            ],
        );
        assert_eq!(answered[3], []);
        assert_eq!(admission.phase(id(1)), None);
        assert!(answered[4..].iter().all(Vec::is_empty));
    }

    /// A held job reserves its count at its class's pool priority and starts only on its grant,
    /// whatever the host's memory pressure; an unheld mbx job of the same checkout starts at once
    /// beside it and reserves nothing.
    #[test]
    fn a_held_job_reserves_and_starts_only_on_its_grant() {
        let mut admission = Admission::default();
        let at = Instant::now();
        let answered = run(
            &mut admission,
            at,
            vec![
                darwin(CRITICAL),
                ask(1, Background, held(4)),
                ask(2, Background, MBX),
                Event::Granted { job: id(1) },
            ],
        );
        assert_eq!(
            answered[1],
            [
                Effect::HoldWaitStarted {
                    job: id(1),
                    cpus: n(4),
                    priority: PoolPriority::Low,
                    ahead: 0
                },
                Effect::Reserve {
                    job: id(1),
                    cpus: n(4),
                    priority: PoolPriority::Low
                },
            ]
        );
        assert_eq!(answered[2], [Effect::Admit { job: id(2) }]);
        assert_eq!(
            answered[3],
            [
                Effect::HoldWaitEnded {
                    job: id(1),
                    waited: Duration::from_secs(2),
                    ok: true
                },
                Effect::Admit { job: id(1) },
            ]
        );
        assert_eq!(admission.phase(id(1)), Some(Phase::Admitted));
    }

    /// One hold per lane waits in the pool: a checkout's later held jobs wait in request order and
    /// enter one by one as the one ahead is granted or withdrawn, while its gate has a lane of its
    /// own and enters at once at `normal`.
    #[test]
    fn a_checkout_waits_in_the_pool_once_per_lane_in_request_order() {
        let mut admission = Admission::default();
        let at = Instant::now();
        let answered = run(
            &mut admission,
            at,
            vec![
                ask(1, Background, held(2)),
                ask(2, Interactive, held(3)),
                ask(3, Background, held(1)),
                ask(4, Gate, held(8)),
                Event::Granted { job: id(1) },
                Event::Cancelled { job: id(2) },
            ],
        );
        let started = |job, cpus, priority, ahead| Effect::HoldWaitStarted {
            job: id(job),
            cpus: n(cpus),
            priority,
            ahead,
        };
        let reserve = |job, cpus, priority| Effect::Reserve {
            job: id(job),
            cpus: n(cpus),
            priority,
        };
        assert_eq!(answered[1], [started(2, 3, PoolPriority::Low, 1)]);
        assert_eq!(answered[2], [started(3, 1, PoolPriority::Low, 2)]);
        assert_eq!(
            answered[3],
            [
                started(4, 8, PoolPriority::Normal, 0),
                reserve(4, 8, PoolPriority::Normal)
            ]
        );
        assert_eq!(
            answered[4],
            [
                Effect::HoldWaitEnded {
                    job: id(1),
                    waited: Duration::from_secs(4),
                    ok: true
                },
                Effect::Admit { job: id(1) },
                reserve(2, 3, PoolPriority::Low),
            ]
        );
        assert_eq!(
            answered[5],
            [
                Effect::Release { job: id(2) },
                reserve(3, 1, PoolPriority::Low)
            ]
        );
        assert_eq!(
            admission.phase(id(3)),
            Some(Phase::Holding {
                since: at + Duration::from_secs(2)
            })
        );
    }

    /// A hold mbx ends without a grant fails its job and lets the lane's next enter; a held job is
    /// released exactly once however it leaves, and a grant for a job already gone changes nothing.
    #[test]
    fn a_held_job_is_released_once_however_it_leaves() {
        let mut admission = Admission::default();
        let at = Instant::now();
        let refusal = "reserve needs scheduler.enabled".to_owned();
        let answered = run(
            &mut admission,
            at,
            vec![
                ask(1, Background, held(2)),
                ask(2, Background, held(2)),
                Event::HoldRefused {
                    job: id(1),
                    error: refusal.clone(),
                },
                Event::Granted { job: id(2) },
                Event::Spawned { job: id(2) },
                Event::Cancelled { job: id(2) },
                Event::Concluded { job: id(2) },
                Event::Granted { job: id(1) },
                Event::Concluded { job: id(2) },
            ],
        );
        assert_eq!(
            answered[2],
            [
                Effect::HoldWaitEnded {
                    job: id(1),
                    waited: Duration::from_secs(2),
                    ok: false
                },
                Effect::Release { job: id(1) },
                Effect::RefuseHold {
                    job: id(1),
                    error: refusal
                },
                Effect::Reserve {
                    job: id(2),
                    cpus: n(2),
                    priority: PoolPriority::Low
                },
            ]
        );
        assert_eq!(
            answered[3],
            [
                Effect::HoldWaitEnded {
                    job: id(2),
                    waited: Duration::from_secs(2),
                    ok: true
                },
                Effect::Admit { job: id(2) },
            ]
        );
        assert_eq!(answered[5], [], "a running job leaves at its end");
        assert_eq!(answered[6], [Effect::Release { job: id(2) }]);
        assert!(answered[7..].iter().all(Vec::is_empty));
        assert_eq!(admission.phase(id(1)), None);
        assert_eq!(admission.phase(id(2)), None);
    }
}
