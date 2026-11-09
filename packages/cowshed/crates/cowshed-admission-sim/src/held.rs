//! Held jobs against mbx's pool as it admits (05_gateway.md "Held jobs", "Admission"): several
//! checkouts' real `job_admission` state machines reserve from one pool that grants by
//! [`POOL_RULE`], the rule the pinned mbx implements. A grant is moldable: a hold may start on
//! fewer cores than it asked for, and its job then runs on what it holds. Within a priority the
//! pool grants the shortest predicted hold first, and a hold that has waited the aging bound
//! before every fresher one, oldest first.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use cowshed_core::api::dto::{ExecClass, JobId};
use cowshed_core::runtime::job_admission::{Admission, Effect, Event, Ledger, Phase, PoolPriority};
use proptest::prelude::*;

use crate::admit::{self, Charge, Kind, Lease, Priority, Request};
use crate::model::{Class, Secs};
use crate::policy::{AGING, POOL_RULE};

const CHECKOUTS: usize = 4;
const CORES: u32 = 8;
/// A hold names no memory, mbx's zero-byte charge, so only the cores bound it.
const CAP: Charge = Charge {
    cpus: CORES,
    memory: 1 << 40,
};

/// A checkout's job.
type Holder = (usize, JobId);

fn id(value: u64) -> JobId {
    JobId::new(value).expect("a job id within range")
}

/// What a job declared when it asked.
#[derive(Clone, Copy, Debug)]
struct Job {
    class: ExecClass,
    hold: Option<NonZeroUsize>,
    /// What mbx predicts its hold lasts: none for a job it has not seen.
    predicted: Option<Secs>,
}

/// mbx's pool: the holds waiting in ticket order, and what each granted one holds.
#[derive(Debug, Default)]
struct Pool {
    tickets: u64,
    queue: Vec<(Holder, Request)>,
    leases: BTreeMap<Holder, Charge>,
}

impl Pool {
    /// A hold enters at its job's pool priority and prediction: mbx knows nothing else of a job.
    fn enter(
        &mut self,
        holder: Holder,
        cpus: NonZeroUsize,
        priority: PoolPriority,
        predicted: Option<Secs>,
        now: Secs,
    ) {
        self.tickets += 1;
        let (class, priority) = match priority {
            PoolPriority::Normal => (Class::Gate, Priority::Normal),
            PoolPriority::Low => (Class::Background, Priority::Low),
        };
        let cpus = u32::try_from(cpus.get()).expect("a hold's cores fit u32");
        self.queue.push((
            holder,
            Request {
                ticket: self.tickets,
                class,
                priority,
                since: now,
                rank: 0.0,
                kind: Kind::Reservation,
                predicted,
                charge: Charge { cpus, memory: 0 },
            },
        ));
    }

    fn leave(&mut self, holder: Holder) {
        self.queue.retain(|&(waiting, _)| waiting != holder);
        self.leases.remove(&holder);
    }

    fn used(&self) -> u32 {
        self.leases.values().map(|lease| lease.cpus).sum()
    }

    /// One pass of the pool rule: the waiting holds it starts, each with what it holds now.
    fn admit(&mut self, now: Secs) -> Vec<(Holder, Charge)> {
        let requests: Vec<Request> = self.queue.iter().map(|&(_, request)| request).collect();
        let live: Vec<Lease> = self
            .leases
            .values()
            .map(|&charge| Lease { charge, ends: now })
            .collect();
        let mut grants = admit::admit(POOL_RULE, &requests, &live, CAP, now);
        grants.sort_by_key(|grant| std::cmp::Reverse(grant.index));
        let started: Vec<(Holder, Charge)> = grants
            .into_iter()
            .map(|grant| {
                let (holder, _) = self.queue.remove(grant.index);
                self.leases.insert(holder, grant.charge);
                (holder, grant.charge)
            })
            .collect();
        started
    }
}

#[derive(Clone, Debug)]
enum Op {
    Request {
        checkout: usize,
        class: ExecClass,
        cpus: u32,
        held: bool,
        /// What mbx predicts the job's hold lasts.
        predicted: Option<Secs>,
    },
    /// One pass of the pool rule.
    Grant,
    /// mbx ends a waiting hold without a grant.
    Refuse(usize),
    Spawn(usize),
    Cancel(usize),
    Conclude(usize),
}

struct World {
    origin: Instant,
    now: Secs,
    checkouts: Vec<Admission>,
    jobs: BTreeMap<Holder, Job>,
    pool: Pool,
    /// Every job that reserved and is not released yet.
    reserved: BTreeSet<Holder>,
    minted: u64,
}

impl World {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            now: 0.0,
            checkouts: (0..CHECKOUTS).map(|_| Admission::default()).collect(),
            jobs: BTreeMap::new(),
            pool: Pool::default(),
            reserved: BTreeSet::new(),
            minted: 0,
        }
    }

    /// Tell `checkout`'s admission of `event` a second after the last one, and carry out what it
    /// asks of the pool.
    fn step(&mut self, checkout: usize, event: Event) {
        self.now += 1.0;
        let at = self.origin + Duration::from_secs_f64(self.now);
        for effect in self.checkouts[checkout].step(at, event) {
            match effect {
                Effect::Reserve {
                    job,
                    cpus,
                    priority,
                } => {
                    let holder = (checkout, job);
                    assert!(self.reserved.insert(holder), "{holder:?} reserved twice");
                    assert_eq!(
                        self.jobs.get(&holder).and_then(|declared| declared.hold),
                        Some(cpus),
                        "{holder:?} reserved what it did not declare"
                    );
                    let predicted = self.jobs[&holder].predicted;
                    self.pool.enter(holder, cpus, priority, predicted, self.now);
                }
                Effect::Release { job } => {
                    let holder = (checkout, job);
                    assert!(
                        self.reserved.remove(&holder),
                        "{holder:?} released without a reservation"
                    );
                    self.pool.leave(holder);
                }
                Effect::Admit { .. }
                | Effect::WaitStarted { .. }
                | Effect::WaitEnded { .. }
                | Effect::Refuse { .. }
                | Effect::EndForMemory { .. }
                | Effect::HoldWaitStarted { .. }
                | Effect::HoldWaitEnded { .. }
                | Effect::RefuseHold { .. } => {}
            }
        }
        let admissions = &self.checkouts;
        self.jobs
            .retain(|&(checkout, job), _| admissions[checkout].phase(job).is_some());
    }

    fn phase(&self, (checkout, job): Holder) -> Option<Phase> {
        self.checkouts[checkout].phase(job)
    }

    /// The live jobs `keep` selects by phase.
    fn jobs(&self, keep: impl Fn(Phase) -> bool) -> Vec<Holder> {
        self.jobs
            .keys()
            .copied()
            .filter(|&holder| self.phase(holder).is_some_and(&keep))
            .collect()
    }

    fn pick(holders: &[Holder], pick: usize) -> Option<Holder> {
        (!holders.is_empty()).then(|| holders[pick % holders.len()])
    }

    fn apply(&mut self, op: Op) {
        match op {
            Op::Request {
                checkout,
                class,
                cpus,
                held,
                predicted,
            } => {
                self.minted += 1;
                let job = id(self.minted);
                let hold = held.then(|| {
                    NonZeroUsize::new(usize::try_from(cpus).expect("a count fits usize"))
                        .expect("a positive count")
                });
                self.jobs.insert(
                    (checkout, job),
                    Job {
                        class,
                        hold,
                        predicted,
                    },
                );
                let ledger = Ledger::Mbx { hold };
                self.step(checkout, Event::Requested { job, class, ledger });
            }
            Op::Grant => {
                let seen = self.pool.queue.clone();
                let requests: Vec<Request> = seen.iter().map(|&(_, request)| request).collect();
                let predicted = admit::predictions(&requests);
                let started = self.pool.admit(self.now);
                self.check_order(&seen, &predicted, &started);
                let low_started = started.iter().any(|&(holder, _)| {
                    self.jobs
                        .get(&holder)
                        .is_some_and(|job| PoolPriority::of(job.class) == PoolPriority::Low)
                });
                let normal_waits = self
                    .pool
                    .queue
                    .iter()
                    .any(|(_, request)| request.priority == Priority::Normal);
                assert!(
                    !(low_started && normal_waits),
                    "a background hold started while a gate's hold waited"
                );
                for ((checkout, job), _) in started {
                    self.step(checkout, Event::Granted { job });
                }
            }
            Op::Refuse(pick) => {
                let waiting: Vec<Holder> =
                    self.pool.queue.iter().map(|&(holder, _)| holder).collect();
                if let Some((checkout, job)) = Self::pick(&waiting, pick) {
                    self.pool.leave((checkout, job));
                    let error = "mbx exited 1".to_owned();
                    self.step(checkout, Event::HoldRefused { job, error });
                }
            }
            Op::Spawn(pick) => {
                let admitted = self.jobs(|phase| phase == Phase::Admitted);
                if let Some((checkout, job)) = Self::pick(&admitted, pick) {
                    self.step(checkout, Event::Spawned { job });
                }
            }
            Op::Cancel(pick) => {
                let live = self.jobs(|_| true);
                if let Some((checkout, job)) = Self::pick(&live, pick) {
                    self.step(checkout, Event::Cancelled { job });
                }
            }
            Op::Conclude(pick) => {
                let live = self.jobs(|_| true);
                if let Some((checkout, job)) = Self::pick(&live, pick) {
                    self.step(checkout, Event::Concluded { job });
                }
            }
        }
        self.check();
    }

    /// One pass's order within each priority, against the queue it saw and the predictions it
    /// ordered by: no fresh hold is granted while a fresh hold of its priority predicted shorter,
    /// or as short and asked before it, still waits; and once a hold has waited the aging bound,
    /// nothing that asked after it in its priority is granted while it waits.
    fn check_order(
        &self,
        seen: &[(Holder, Request)],
        predicted: &[Secs],
        started: &[(Holder, Charge)],
    ) {
        let aged = |request: &Request| admit::aged(Some(AGING), request, self.now);
        let granted = |holder: Holder| started.iter().any(|&(started, _)| started == holder);
        for (g, &(holder, request)) in seen.iter().enumerate() {
            if !granted(holder) {
                continue;
            }
            for (w, &(waiter, waits)) in seen.iter().enumerate() {
                if granted(waiter) || waits.priority != request.priority {
                    continue;
                }
                if aged(&waits) {
                    assert!(
                        waits.ticket > request.ticket,
                        "{holder:?} was granted while {waiter:?}, aged and asked before it, waited"
                    );
                } else if !aged(&request) {
                    assert!(
                        predicted[w]
                            .total_cmp(&predicted[g])
                            .then(waits.ticket.cmp(&request.ticket))
                            .is_gt(),
                        "{holder:?}, predicted {} s, was granted while {waiter:?}, predicted {} s, waited",
                        predicted[g],
                        predicted[w]
                    );
                }
            }
        }
    }

    /// What holds between happenings: the grants fit the pool; each admitted held job holds a
    /// grant of at least one and at most the cores it asked for; a holding job waits in the pool
    /// and a pending one holds no reservation; a checkout waits in the pool at most once per
    /// priority; and no grant outlives its job's admission.
    fn check(&self) {
        assert!(
            self.pool.used() <= CORES,
            "grants of {} cores exceed the pool's {CORES}",
            self.pool.used()
        );
        let mut lanes = BTreeSet::new();
        for (holder, request) in &self.pool.queue {
            assert!(
                lanes.insert((holder.0, request.priority)),
                "checkout {} waits in the pool twice at {:?}",
                holder.0,
                request.priority
            );
        }
        for (&holder, job) in &self.jobs {
            let phase = self.phase(holder).expect("a live job is in its admission");
            let waits = self
                .pool
                .queue
                .iter()
                .any(|&(waiting, _)| waiting == holder);
            match (job.hold, phase) {
                (Some(asked), Phase::Admitted | Phase::Running { .. }) => {
                    let lease = self
                        .pool
                        .leases
                        .get(&holder)
                        .unwrap_or_else(|| panic!("{holder:?} was admitted without its grant"));
                    assert!(
                        lease.cpus >= 1
                            && usize::try_from(lease.cpus).is_ok_and(|cpus| cpus <= asked.get()),
                        "{holder:?} holds {} of the {asked} cores it asked for",
                        lease.cpus
                    );
                }
                (Some(_), Phase::Holding { .. }) => {
                    assert!(waits, "{holder:?} holds no place in the pool");
                }
                (Some(_), Phase::Pending { .. }) => {
                    assert!(
                        !self.reserved.contains(&holder),
                        "{holder:?} reserved behind its lane"
                    );
                    // Its lane's waiter is of its own checkout and priority: a gate never waits
                    // behind its checkout's background hold, nor background behind a gate.
                    let priority = PoolPriority::of(job.class);
                    assert!(
                        self.jobs.iter().any(|(&(checkout, other), ahead)| {
                            checkout == holder.0
                                && ahead.hold.is_some()
                                && PoolPriority::of(ahead.class) == priority
                                && matches!(
                                    self.checkouts[checkout].phase(other),
                                    Some(Phase::Holding { .. })
                                )
                        }),
                        "{holder:?} waits behind no hold of its own lane"
                    );
                }
                (Some(_), other) => panic!("{holder:?}, a held job, is {other:?}"),
                (None, _) => {
                    assert!(
                        !self.reserved.contains(&holder),
                        "{holder:?} reserved without declaring a hold"
                    );
                }
            }
        }
        for &holder in self.pool.leases.keys() {
            let phase = self.phase(holder);
            assert!(
                matches!(phase, Some(Phase::Admitted | Phase::Running { .. })),
                "{holder:?}'s grant outlives its admission: {phase:?}"
            );
        }
    }

    /// Every admitted job ends and the pool grants what waits, until nothing does: every hold is
    /// granted once the holds ahead of it end, and the pool is left empty.
    fn drain(&mut self) {
        for _ in 0..=self.minted {
            for (checkout, job) in
                self.jobs(|phase| matches!(phase, Phase::Admitted | Phase::Running { .. }))
            {
                self.step(checkout, Event::Concluded { job });
            }
            self.apply(Op::Grant);
        }
        let starved =
            self.jobs(|phase| matches!(phase, Phase::Pending { .. } | Phase::Holding { .. }));
        assert!(starved.is_empty(), "{starved:?} were never granted");
        for (checkout, job) in self.jobs(|_| true) {
            self.step(checkout, Event::Concluded { job });
        }
        self.check();
        assert!(self.pool.queue.is_empty() && self.pool.leases.is_empty());
        assert!(
            self.reserved.is_empty(),
            "{:?} were never released",
            self.reserved
        );
    }

    /// A released grant is the pool's again: a hold of every core starts whole.
    fn reuse(&mut self) {
        self.apply(Op::Request {
            checkout: 0,
            class: ExecClass::Background,
            cpus: CORES,
            held: true,
            predicted: None,
        });
        self.apply(Op::Grant);
        let holder = (0, id(self.minted));
        assert_eq!(self.phase(holder), Some(Phase::Admitted));
        assert_eq!(
            self.pool.leases.get(&holder).map(|lease| lease.cpus),
            Some(CORES)
        );
    }
}

fn class() -> impl Strategy<Value = ExecClass> {
    prop_oneof![
        Just(ExecClass::Gate),
        Just(ExecClass::Background),
        Just(ExecClass::Interactive),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (
            0..CHECKOUTS,
            class(),
            1..=CORES,
            prop::bool::weighted(0.8),
            prop::option::weighted(0.8, 1..=600_u32),
        )
            .prop_map(|(checkout, class, cpus, held, predicted)| Op::Request {
                checkout,
                class,
                cpus,
                held,
                predicted: predicted.map(f64::from),
            }),
        4 => Just(Op::Grant),
        1 => any::<usize>().prop_map(Op::Refuse),
        2 => any::<usize>().prop_map(Op::Spawn),
        1 => any::<usize>().prop_map(Op::Cancel),
        3 => any::<usize>().prop_map(Op::Conclude),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// Random requests, pool passes, refusals, spawns, cancels and ends from four checkouts keep
    /// every grant within the pool and within what its job asked for; start no background hold
    /// while a gate's hold waits; within a priority, grant fresh holds shortest predicted first
    /// and nothing younger while an aged hold waits; grant every hold once the holds ahead of it
    /// end; and leave every grant released and the whole pool grantable again.
    #[test]
    fn held_jobs_across_checkouts_keep_the_pool_contract(ops in prop::collection::vec(op(), 1..96)) {
        let mut world = World::new();
        for op in ops {
            world.apply(op);
        }
        world.drain();
        world.reuse();
    }
}

/// One core is free and two `low` holds wait, the older predicted 300 s and the younger 5 s: the
/// younger goes first. Once the older has waited the aging bound, it goes before a fresh 5 s hold.
#[test]
fn a_shorter_younger_hold_goes_first_until_the_older_one_ages() {
    let one = NonZeroUsize::MIN;
    let low = PoolPriority::Low;
    let (filler, older, younger, fresh) = ((0, id(1)), (1, id(2)), (2, id(3)), (3, id(4)));
    let mut pool = Pool::default();
    let seven = NonZeroUsize::new(7).expect("a positive count");
    pool.enter(filler, seven, low, Some(1000.0), 0.0);
    pool.admit(0.0);
    pool.enter(older, one, low, Some(300.0), 1.0);
    pool.enter(younger, one, low, Some(5.0), 2.0);
    let started = |pool: &mut Pool, now: Secs| -> Vec<Holder> {
        pool.admit(now)
            .into_iter()
            .map(|(holder, _)| holder)
            .collect()
    };
    assert_eq!(
        started(&mut pool, 2.0),
        [younger],
        "the shorter hold waited"
    );
    pool.leave(younger);
    pool.enter(fresh, one, low, Some(5.0), 1.0 + AGING);
    assert_eq!(
        started(&mut pool, 1.0 + AGING),
        [older],
        "the aged hold was passed"
    );
}
