//! A workspace supervisor's job admission, as one state machine (05_gateway.md "Host scheduling",
//! "Admission"). Pure: it reads no clock, no file and no kernel. The supervisor feeds it events --
//! a start request, a memory-pressure report from the kernel's notification, a spawn, a cancel, a
//! job's end -- each with the instant it observed, and carries out the effects it answers.
//!
//! A job is `queued` until the host's pressure is known, `waiting` while memory pressure holds it,
//! `admitted` once it may start, `running` from its spawn, and concluded when it ends, which takes
//! it out of the ledger. Only a `background` start without mbx ever waits: a gate, interactive work
//! and residents are admitted at once, and an mbx project's jobs are admitted at once in every class,
//! since mbx's pool admits their compiles, tests and reservations itself.
//!
//! No job waits on another job: a job's jobserver holds its own constant count, never a share of a
//! host ledger, so a grant made on an idle host cannot hold a later start, however long its holder
//! runs. The one queue is memory, and a drop in pressure releases every waiter, oldest first.
//!
//! Without mbx, macOS Critical pressure ends the youngest running `background` or `resident` job,
//! one at a time: the next only once the last one ended for memory has concluded, while the level
//! is still Critical. An mbx project's jobs are never ended for memory: mbx holds further
//! admissions instead.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use cowshed_gateway_types::host_status::{DarwinPressureLevel, MemoryPressure};

use crate::api::dto::{ExecClass, JobId};
use crate::host_pressure::{Decision, job_to_end, may_start, waits_for_memory};

/// Which authority admits a job's CPU and memory (05_gateway.md "Host scheduling").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ledger {
    /// The project has the mbx capability: mbx's one pool admits every compile, test and
    /// reservation, under its own memory accounting.
    Mbx,
    /// No mbx: the job's own jobserver bounds its tree, the kernel weighs the classes, and memory
    /// pressure holds background starts.
    Jobservers,
}

/// The host's memory pressure as the kernel last reported it, or why it cannot be read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reading {
    Known(MemoryPressure),
    Unreadable(String),
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
    /// An admitted job's first process was dispatched.
    Spawned { job: JobId },
    /// A job was withdrawn or killed. A queued or waiting one leaves at once; one that was
    /// admitted leaves when it concludes.
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
}

/// Where a job stands. A concluded job is no longer in the ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Queued,
    Waiting { since: Instant },
    Admitted,
    Running { spawned: Instant },
    EndingForMemory,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    class: ExecClass,
    ledger: Ledger,
    phase: Phase,
}

/// One supervisor's admission ledger.
#[derive(Debug, Default)]
pub struct Admission {
    pressure: Option<Reading>,
    jobs: BTreeMap<JobId, Entry>,
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
                let entry = if ledger == Ledger::Mbx || !waits_for_memory(class) {
                    effects.push(Effect::Admit { job });
                    Some(Entry {
                        phase: Phase::Admitted,
                        ..entry
                    })
                } else {
                    held(job, entry, self.pressure.as_ref(), now, &mut effects)
                };
                if let Some(entry) = entry {
                    self.jobs.insert(job, entry);
                }
            }
            Event::Pressure(reading) => {
                self.pressure = Some(reading);
                let pressure = self.pressure.as_ref();
                self.jobs.retain(|&job, entry| {
                    match held(job, *entry, pressure, now, &mut effects) {
                        Some(next) => {
                            *entry = next;
                            true
                        }
                        None => false,
                    }
                });
                self.end_for_memory(&mut effects);
            }
            Event::Spawned { job } => {
                if let Some(entry) = self.jobs.get_mut(&job)
                    && entry.phase == Phase::Admitted
                {
                    entry.phase = Phase::Running { spawned: now };
                }
            }
            Event::Cancelled { job } => {
                if let Some(entry) = self.jobs.get(&job)
                    && matches!(entry.phase, Phase::Queued | Phase::Waiting { .. })
                {
                    self.jobs.remove(&job);
                }
            }
            Event::Concluded { job } => {
                self.jobs.remove(&job);
                self.end_for_memory(&mut effects);
            }
        }
        effects
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
fn held(
    job: JobId,
    entry: Entry,
    pressure: Option<&Reading>,
    now: Instant,
    effects: &mut Vec<Effect>,
) -> Option<Entry> {
    let since = match entry.phase {
        Phase::Queued => None,
        Phase::Waiting { since } => Some(since),
        Phase::Admitted | Phase::Running { .. } | Phase::EndingForMemory => return Some(entry),
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
    use Ledger::{Jobservers, Mbx};

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
            (darwin(CRITICAL), Background, Mbx, true),
            (unreadable(), Background, Mbx, true),
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
        for ledger in [Jobservers, Mbx] {
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
            (5, Background, Mbx),
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
}
