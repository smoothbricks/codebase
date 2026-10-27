//! The gateway's task server: a deterministic scheduler over dependency, ownership and lease
//! facts and the host's memory pressure (specs/cowshed/05_gateway.md, "The task server schedules
//! work units").
//!
//! Work units admit with the facts of their plan — the units they wait on, the owner that runs
//! them and the serialization windows they hold — and this scheduler alone decides which ready
//! unit runs next. The order is a pure function of the logged facts: the same facts always select
//! the same units in the same order, and no model, operator or arrival order is consulted. Strong
//! models propose task boundaries; they do not choose which ready unit acquires a lease.
//!
//! Four rules do the work the operator keeps by hand today:
//!
//! - **Order.** Aged waiters first — the starvation bound, so no unit is outranked forever —
//!   then the class (gate-class work first, interactive and background work behind it, the same
//!   on any host), the coordinator's urgent override, leverage (the transitive dependents), the
//!   cache-domain deadline, and finally the stable unit id.
//! - **Serialization.** A unit's run holds its lease classes from its selection until its
//!   terminal fact: one `cowshed new` at a time, image windows serialized, and one checked land
//!   publication at a time. The land lease is a publication fence — the checked land-authority
//!   transition — and nothing more; gate groups run concurrently, and an urgent land waits behind
//!   the turn in progress in publication order.
//! - **Memory.** A background unit is not selected while the host is under memory pressure
//!   (05_gateway.md "Host scheduling", "Memory pressure"). A selection pass takes at most one
//!   reading, and only when a background unit would otherwise be selected; while one is held, the
//!   pass runs again every [`RECHECK_EVERY`]. A reading that cannot be taken holds the unit too and
//!   the queue names why: an unreadable pressure is not an unpressured host. Gate and interactive
//!   units never take a reading.
//! - **Caps.** A unit may declare its own parallelism ceiling — the caps its run kind applies to
//!   itself — and the grant reports it as declared. The grant allocates nothing: the kernel
//!   schedules CPU among every runnable process, and this scheduler holds no CPU counter.
//!
//! Every decision is a logged fact on stderr, without an opt-in environment variable, and the
//! whole fold is inspectable through the `task-queue` control operation, so a lander reads the queue instead
//! of asking for an assignment. A unit's terminal fact is the only lifecycle this scheduler
//! speaks: it records the outcome — with the owner and workspace incarnation the receipts bind
//! to — and releases what the run held.

use std::cell::LazyCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use cowshed_core::api::ExecClass;
use cowshed_core::host_pressure::{self, Decision, HostPressureError, RECHECK_EVERY};
use cowshed_gateway_types::host_status::{MemoryPressure, MemoryPressureReading};
use cowshed_gateway_types::{
    ConfigError, LeaseClass, TaskBand, TaskClass, TaskDepWait, TaskGrant, TaskLeaseStatus,
    TaskLeaseWait, TaskOutcome, TaskQueueStatus, TaskReadyStatus, TaskRunCaps, TaskRunningStatus,
    TaskWaitingStatus,
};
use tokio::sync::{mpsc, oneshot};

/// How many runs at once may hold each serialization class: the turns the operator keeps by
/// hand today become places on a class. One `cowshed new` at a time, image windows serialized,
/// one checked land publication at a time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeasePlaces {
    pub cowshed_new: usize,
    pub image: usize,
    pub land_publication: usize,
}

impl Default for LeasePlaces {
    fn default() -> Self {
        Self {
            cowshed_new: 1,
            image: 1,
            land_publication: 1,
        }
    }
}

impl LeasePlaces {
    fn places(self, class: LeaseClass) -> usize {
        match class {
            LeaseClass::CowshedNew => self.cowshed_new,
            LeaseClass::Image => self.image,
            LeaseClass::LandPublication => self.land_publication,
        }
    }
}

/// The task server's policy: the serialization classes' places and the age bound after which a
/// waiter outranks fresh priority. The scheduler holds no sizing policy: a run's parallelism is
/// its own declared ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskServerLimits {
    pub lease_places: LeasePlaces,
    /// How long a queued unit waits before it becomes an aged waiter and runs before fresh
    /// priority. This is the starvation bound: without it a stream of urgent work could hold a
    /// unit back forever.
    pub age_bound: Duration,
}

impl Default for TaskServerLimits {
    fn default() -> Self {
        Self {
            lease_places: LeasePlaces::default(),
            age_bound: Duration::from_secs(30),
        }
    }
}

impl TaskServerLimits {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let places = [
            self.lease_places.cowshed_new,
            self.lease_places.image,
            self.lease_places.land_publication,
        ];
        if places.into_iter().any(|value| value == 0) {
            return Err(ConfigError::ZeroLimit);
        }
        Ok(())
    }
}

/// Why the task server refused an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskRefusal {
    /// The unit already has a live attempt; only its terminal fact or its closed connection
    /// frees it, so a delayed old holder can never advance a new attempt.
    LiveAttempt,
    /// The unit has no live attempt to report on.
    NoLiveAttempt,
    /// The task scheduler has stopped.
    Stopped,
}

impl fmt::Display for TaskRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LiveAttempt => formatter.write_str("the unit already has a live attempt"),
            Self::NoLiveAttempt => formatter.write_str("the unit has no live attempt"),
            Self::Stopped => formatter.write_str("the gateway's task scheduler has stopped"),
        }
    }
}

/// The facts a unit admits with. The scheduler selects from these and the host's memory pressure:
/// dependency, ownership and lease facts, and nothing else.
#[derive(Clone, Debug)]
pub struct TaskFacts {
    pub unit: Arc<str>,
    pub owner: Arc<str>,
    /// The workspace incarnation the run works in, as the lifecycle receipts name it.
    pub incarnation: Option<Arc<str>>,
    /// The units that must land before this one is ready.
    pub deps: Vec<Arc<str>>,
    /// The serialization classes the run holds from selection to terminal.
    pub leases: Vec<LeaseClass>,
    /// The coordinator's priority, higher first.
    pub priority: u32,
    /// The coordinator's "land now" override; it outranks every leverage.
    pub urgent: bool,
    pub class: TaskClass,
    /// The cache-domain deadline in epoch milliseconds, for the cache-deadline ordering key.
    pub deadline: Option<u64>,
    /// The run's own parallelism ceiling, reported on its grant as declared.
    pub caps: Option<TaskRunCaps>,
    pub command: Arc<str>,
    /// The admitting client's pid, named on its selection's decision log.
    pub pid: Option<i32>,
}

#[derive(Debug)]
enum State {
    Queued,
    Selected { leases: Vec<LeaseClass> },
    Terminal { outcome: TaskOutcome },
}

#[derive(Debug)]
struct Unit {
    facts: TaskFacts,
    state: State,
    /// When the unit was admitted: its wait, and the fact the age bound is measured from.
    admitted: Instant,
    /// The unit's authoritative queue-entry order, assigned at the admit fold.
    entry: u64,
    /// The attempt number this admission owns.
    attempt: u32,
    /// The live attempt's ticket.
    ticket: u64,
    /// What held the unit back for memory at the last selection pass that reached it: the
    /// pressure, or why the reading could not be taken. `None` when that pass did not.
    memory_wait: Option<MemoryPressureReading>,
}

/// What a [`Ledger`] transition did, for the actor to carry out. Selection effects name the
/// retained ticket; the ledger owns its facts, so an effect never copies their payload.
#[derive(Debug)]
enum Effect {
    /// The unit's turn came: its lease places are held and its run may start. The actor answers
    /// the client with the grant: the run's band and its declared ceiling.
    Select {
        ticket: u64,
        attempt: u32,
        band: TaskBand,
        waited: Duration,
    },
    /// The attempt is over: whatever it held goes back.
    Release { ticket: u64 },
}

/// Actual leverage computations witnessed on the current thread. A pass on another
/// thread never changes this thread's count.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct LeverageWork {
    passes: u64,
}

#[cfg(test)]
thread_local! {
    static LEVERAGE_WORK: std::cell::Cell<LeverageWork> = const {
        std::cell::Cell::new(LeverageWork { passes: 0 })
    };
}

#[cfg(test)]
fn record_leverage_pass() {
    LEVERAGE_WORK.with(|work| {
        let mut current = work.get();
        current.passes += 1;
        work.set(current);
    });
}

#[cfg(test)]
fn reset_leverage_work() {
    LEVERAGE_WORK.with(|work| work.set(LeverageWork::default()));
}

#[cfg(test)]
fn leverage_work() -> LeverageWork {
    LEVERAGE_WORK.with(|work| work.get())
}

/// One operation's leverage counts over an unchanged dependency graph. It is created for one
/// actor turn (or one pure operation), shared by selection, ready order and queue status, and
/// dropped with the operation: the ledger keeps no leverage cache. Selection does not
/// invalidate it, because a selected unit stays live and neither deps nor terminal facts
/// change there. An abandonment made through leave below is the one invalidation: only an
/// actual abandoned attempt drops the counts, and the next use takes one fresh pass.
#[derive(Debug)]
struct LeverageObservation {
    counts: Option<HashMap<Arc<str>, usize>>,
}

impl LeverageObservation {
    fn new() -> Self {
        Self { counts: None }
    }

    fn counts(&mut self, ledger: &Ledger) -> &HashMap<Arc<str>, usize> {
        self.counts.get_or_insert_with(|| ledger.leverage())
    }

    /// Abandon the attempt behind ticket while this observation is live. A stale ticket
    /// changes no graph fact and keeps the counts; an actual abandonment makes the unit
    /// terminal, so the counts go and the next reader recomputes them once.
    fn leave(
        &mut self,
        ledger: &mut Ledger,
        ticket: u64,
        effects: &mut Vec<Effect>,
    ) -> Option<TaskFacts> {
        let facts = ledger.leave(ticket, effects);
        if facts.is_some() {
            self.counts = None;
        }
        facts
    }
}

/// The schedule's whole state: one live attempt per unit, the lease classes' holders, and the
/// terminal facts every dependent reads. Invariants: a ticket appears in exactly one live unit;
/// a holder list names exactly the selected tickets that declared the class; a terminal unit
/// holds nothing.
#[derive(Debug)]
struct Ledger {
    limits: TaskServerLimits,
    units: HashMap<Arc<str>, Unit>,
    /// Every live ticket's (unit, attempt): a leave names its ticket, and only the live attempt
    /// acts on it.
    tickets: HashMap<u64, (Arc<str>, u32)>,
    /// The selected tickets holding each lease class, in grant order.
    holders: HashMap<LeaseClass, Vec<u64>>,
    /// The attempt number each unit id has reached.
    attempts: HashMap<Arc<str>, u32>,
    /// The next queue-entry order to assign: the admit fold linearizes entry.
    next_entry: u64,
}

impl Ledger {
    fn new(limits: TaskServerLimits) -> Self {
        Self {
            limits,
            units: HashMap::new(),
            tickets: HashMap::new(),
            holders: HashMap::new(),
            attempts: HashMap::new(),
            next_entry: 0,
        }
    }

    /// Admit `facts` behind `ticket`. A unit keeps exactly one live attempt: a second admission
    /// while one waits or runs is refused, and only that attempt's terminal fact or closed
    /// connection frees the unit.
    fn admit(&mut self, now: Instant, ticket: u64, facts: TaskFacts) -> Result<(), TaskRefusal> {
        if let Some(unit) = self.units.get(&facts.unit)
            && !matches!(unit.state, State::Terminal { .. })
        {
            return Err(TaskRefusal::LiveAttempt);
        }
        let unit = facts.unit.clone();
        let attempt = self.attempts.get(&unit).copied().unwrap_or(0) + 1;
        self.attempts.insert(Arc::clone(&unit), attempt);
        // The admit fold is the queue-entry linearization point: the entry order is assigned
        // here, never from a caller-minted ticket or a caller timestamp.
        let entry = self.next_entry;
        self.next_entry += 1;
        self.units.insert(
            Arc::clone(&unit),
            Unit {
                facts,
                state: State::Queued,
                admitted: now,
                entry,
                attempt,
                ticket,
                memory_wait: None,
            },
        );
        self.tickets.insert(ticket, (unit, attempt));
        Ok(())
    }

    /// Record `outcome` for the unit's live attempt: the fact its dependents read, and the
    /// release of its lease classes. The settled attempt's facts come back for the decision log.
    fn terminal(
        &mut self,
        unit: &str,
        outcome: TaskOutcome,
        effects: &mut Vec<Effect>,
    ) -> Result<TaskFacts, TaskRefusal> {
        let key: Arc<str> = Arc::from(unit);
        let Some(entry) = self.units.get_mut(&key) else {
            return Err(TaskRefusal::NoLiveAttempt);
        };
        if matches!(entry.state, State::Terminal { .. }) {
            return Err(TaskRefusal::NoLiveAttempt);
        }
        let ticket = entry.ticket;
        if let State::Selected { leases } = &entry.state {
            for class in leases {
                if let Some(held) = self.holders.get_mut(class) {
                    held.retain(|held| *held != ticket);
                }
            }
        }
        entry.state = State::Terminal { outcome };
        self.tickets.remove(&ticket);
        effects.push(Effect::Release { ticket });
        Ok(entry.facts.clone())
    }

    /// The attempt behind `ticket` ended without a report: a crash or a hang-up abandons it, and
    /// dependents see `abandoned` rather than wait on a ghost. A stale ticket — a delayed holder
    /// from an attempt that already settled — changes nothing.
    fn leave(&mut self, ticket: u64, effects: &mut Vec<Effect>) -> Option<TaskFacts> {
        let (unit, attempt) = self.tickets.remove(&ticket)?;
        let entry = self.units.get_mut(&unit)?;
        if entry.attempt != attempt || matches!(entry.state, State::Terminal { .. }) {
            return None;
        }
        if let State::Selected { leases } = &entry.state {
            for class in leases {
                if let Some(held) = self.holders.get_mut(class) {
                    held.retain(|held| *held != ticket);
                }
            }
        }
        entry.state = State::Terminal {
            outcome: TaskOutcome::Abandoned,
        };
        effects.push(Effect::Release { ticket });
        Some(entry.facts.clone())
    }

    /// Recompute the schedule at this instant and select every unit whose turn came, whose lease
    /// classes have room and that memory does not hold, in order. The order is a pure function of
    /// the facts, and the memory decision of the facts and the one reading `read_memory` returns:
    /// the pass calls it at most once, and only when a unit that waits for memory would otherwise
    /// be selected. A lease- or memory-held unit holds up nothing beside it.
    fn select(
        &mut self,
        now: Instant,
        observation: &mut LeverageObservation,
        read_memory: impl FnOnce() -> Result<MemoryPressure, HostPressureError>,
        effects: &mut Vec<Effect>,
    ) {
        let reading = LazyCell::new(|| read_memory().map_err(|error| error.to_string()));
        for ticket in self.ready_order(now, observation) {
            let Some((unit, attempt)) = self.tickets.get(&ticket).cloned() else {
                continue;
            };
            let Some(entry) = self.units.get(&unit) else {
                continue;
            };
            let fits = entry.facts.leases.iter().all(|class| {
                self.holders.get(class).map_or(0, |held| held.len())
                    < self.limits.lease_places.places(*class)
            });
            let entry = self.units.get_mut(&unit).expect("checked above");
            entry.memory_wait = (fits && waits_for_memory(entry.facts.class))
                .then(|| memory_hold(LazyCell::force(&reading)))
                .flatten();
            if !fits || entry.memory_wait.is_some() {
                continue;
            }
            for class in &entry.facts.leases {
                self.holders.entry(*class).or_default().push(ticket);
            }
            let waited = now.saturating_duration_since(entry.admitted);
            entry.state = State::Selected {
                leases: entry.facts.leases.clone(),
            };
            effects.push(Effect::Select {
                ticket,
                band: entry.facts.class.band(),
                attempt,
                waited,
            });
        }
    }

    /// The ready units in the exact order the scheduler would grant them.
    fn ready_order(&self, now: Instant, observation: &mut LeverageObservation) -> Vec<u64> {
        let leverage = observation.counts(self);
        let mut ready: Vec<&Unit> = self
            .units
            .values()
            .filter(|unit| matches!(unit.state, State::Queued))
            .filter(|unit| unit.facts.deps.iter().all(|dep| self.landed(dep)))
            .collect();
        ready.sort_by(|a, b| self.order_cmp(a, b, now, leverage));
        ready.into_iter().map(|unit| unit.ticket).collect()
    }

    fn landed(&self, unit: &Arc<str>) -> bool {
        matches!(
            self.units.get(unit).map(|entry| &entry.state),
            Some(State::Terminal {
                outcome: TaskOutcome::Landed,
                ..
            })
        )
    }

    /// The distinct live units transitively depending on each unit: the leverage ordering key.
    fn leverage(&self) -> HashMap<Arc<str>, usize> {
        #[cfg(test)]
        record_leverage_pass();
        let mut counts: HashMap<Arc<str>, usize> = HashMap::new();
        let mut seen: HashSet<&str> = HashSet::new();
        let mut stack: Vec<&Arc<str>> = Vec::new();
        for unit in self
            .units
            .values()
            .filter(|unit| !matches!(unit.state, State::Terminal { .. }))
        {
            let source = &*unit.facts.unit;
            seen.clear();
            stack.clear();
            stack.extend(unit.facts.deps.iter());
            while let Some(dep) = stack.pop() {
                let name = &**dep;
                if name == source {
                    continue;
                }
                if !seen.insert(name) {
                    continue;
                }
                if let Some(entry) = self.units.get(name) {
                    stack.extend(entry.facts.deps.iter());
                }
                match counts.get_mut(name) {
                    Some(count) => *count += 1,
                    None => {
                        counts.insert(Arc::clone(dep), 1);
                    }
                }
            }
        }
        counts
    }

    /// The schedule's total order. Aged waiters run before fresh priority in the order they
    /// waited — the starvation bound — then the class, the urgent override, leverage, the
    /// cache-domain deadline, and the stable unit id.
    fn order_cmp(
        &self,
        a: &Unit,
        b: &Unit,
        now: Instant,
        leverage: &HashMap<Arc<str>, usize>,
    ) -> Ordering {
        let aged =
            |unit: &Unit| now.saturating_duration_since(unit.admitted) >= self.limits.age_bound;
        // Lands are their own queue: the publication fence goes to the first claimant in
        // admission order — the land lock's one order — behind older aged waiters, and the land
        // class ranks as the foreground class beside bulk work. The urgent override orders bulk
        // and never overtakes an older claimant.
        let land = |unit: &Unit| unit.facts.leases.contains(&LeaseClass::LandPublication);
        match (land(a), land(b)) {
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (true, true) => {
                return match (aged(a), aged(b)) {
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    _ => a.entry.cmp(&b.entry),
                };
            }
            (false, false) => {}
        }
        match (aged(a), aged(b)) {
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (true, true) => {
                return a
                    .admitted
                    .cmp(&b.admitted)
                    .then_with(|| a.facts.unit.cmp(&b.facts.unit));
            }
            (false, false) => {}
        }
        let leverage_of = |unit: &Unit| leverage.get(&unit.facts.unit).copied().unwrap_or(0);
        class_rank(a.facts.class)
            .cmp(&class_rank(b.facts.class))
            .then_with(|| b.facts.urgent.cmp(&a.facts.urgent))
            .then_with(|| leverage_of(b).cmp(&leverage_of(a)))
            .then_with(|| match (a.facts.deadline, b.facts.deadline) {
                (Some(x), Some(y)) => x.cmp(&y),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            })
            .then_with(|| a.facts.unit.cmp(&b.facts.unit))
    }

    /// When the actor must next recompute without an arrival: the oldest unaged waiter's age
    /// bound, or the next memory reading while a unit is held for memory, whichever comes first.
    fn wake(&self, now: Instant) -> Option<Instant> {
        let queued = || {
            self.units
                .values()
                .filter(|unit| matches!(unit.state, State::Queued))
        };
        let aging = queued()
            .map(|unit| unit.admitted + self.limits.age_bound)
            .filter(|deadline| *deadline > now)
            .min();
        let recheck = queued()
            .any(|unit| unit.memory_wait.is_some())
            .then(|| now + RECHECK_EVERY);
        aging.into_iter().chain(recheck).min()
    }

    fn unit_of(&self, ticket: u64) -> Option<&Unit> {
        let (unit, _) = self.tickets.get(&ticket)?;
        self.units.get(unit)
    }

    fn holder_names(&self, class: LeaseClass) -> Vec<String> {
        self.holders
            .get(&class)
            .map(|held| {
                held.iter()
                    .filter_map(|ticket| {
                        self.unit_of(*ticket)
                            .map(|unit| unit.facts.unit.to_string())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The whole fold at this instant: the ready order with what holds each ready unit back, what
    /// waits on what, and who holds each lease.
    fn status(&self, now: Instant, observation: &mut LeverageObservation) -> TaskQueueStatus {
        let ready_tickets = self.ready_order(now, observation);
        let leverage = observation.counts(self);
        let ready = ready_tickets
            .into_iter()
            .filter_map(|ticket| {
                let entry = self.unit_of(ticket)?;
                let blocked_leases = entry
                    .facts
                    .leases
                    .iter()
                    .copied()
                    .filter(|class| {
                        self.holders.get(class).map_or(0, |held| held.len())
                            >= self.limits.lease_places.places(*class)
                    })
                    .map(|class| TaskLeaseWait {
                        class,
                        holders: self.holder_names(class),
                    })
                    .collect();
                Some(TaskReadyStatus {
                    unit: entry.facts.unit.to_string(),
                    owner: entry.facts.owner.to_string(),
                    class: entry.facts.class,
                    band: entry.facts.class.band(),
                    priority: entry.facts.priority,
                    urgent: entry.facts.urgent,
                    leverage: leverage.get(&entry.facts.unit).copied().unwrap_or(0),
                    waited_ms: milliseconds(now.saturating_duration_since(entry.admitted)),
                    blocked_leases,
                    memory_wait: entry.memory_wait.clone(),
                })
            })
            .collect();
        let mut waiting: Vec<TaskWaitingStatus> = self
            .units
            .values()
            .filter(|unit| matches!(unit.state, State::Queued))
            .filter(|unit| !unit.facts.deps.iter().all(|dep| self.landed(dep)))
            .map(|unit| TaskWaitingStatus {
                unit: unit.facts.unit.to_string(),
                owner: unit.facts.owner.to_string(),
                waited_ms: milliseconds(now.saturating_duration_since(unit.admitted)),
                deps: unit
                    .facts
                    .deps
                    .iter()
                    .map(|dep| TaskDepWait {
                        unit: dep.to_string(),
                        outcome: self.units.get(dep).and_then(|entry| match entry.state {
                            State::Terminal { outcome } => Some(outcome),
                            _ => None,
                        }),
                    })
                    .collect(),
            })
            .collect();
        waiting.sort_by(|a, b| a.unit.cmp(&b.unit));
        let mut running: Vec<TaskRunningStatus> = self
            .units
            .values()
            .filter_map(|unit| match &unit.state {
                State::Selected { leases } => Some(TaskRunningStatus {
                    unit: unit.facts.unit.to_string(),
                    owner: unit.facts.owner.to_string(),
                    leases: leases.clone(),
                    caps: unit.facts.caps,
                    granted_after_ms: milliseconds(now.saturating_duration_since(unit.admitted)),
                }),
                _ => None,
            })
            .collect();
        running.sort_by(|a, b| a.unit.cmp(&b.unit));
        let leases = LeaseClass::ALL
            .into_iter()
            .map(|class| TaskLeaseStatus {
                class,
                places: self.limits.lease_places.places(class),
                held: self.holders.get(&class).map_or(0, |held| held.len()),
                holders: self.holder_names(class),
            })
            .collect();
        TaskQueueStatus {
            ready,
            waiting,
            running,
            leases,
        }
    }
}

/// The order's class rank: gate-class work first, interactive and background work behind it, the
/// same on any host.
const fn class_rank(class: TaskClass) -> u8 {
    match class {
        TaskClass::Gate => 0,
        TaskClass::Interactive | TaskClass::Background => 1,
    }
}

/// Whether a unit of `class` waits for memory pressure to drop: the host's rule for the exec
/// class the unit runs as, background alone.
const fn waits_for_memory(class: TaskClass) -> bool {
    host_pressure::waits_for_memory(match class {
        TaskClass::Gate => ExecClass::Gate,
        TaskClass::Interactive => ExecClass::Interactive,
        TaskClass::Background => ExecClass::Background,
    })
}

/// What holds a unit that waits for memory under one pass's `reading`: the pressure when it says
/// wait, the read failure when no reading could be taken — an unreadable pressure is not an
/// unpressured host — and nothing when the host is not under pressure.
fn memory_hold(reading: &Result<MemoryPressure, String>) -> Option<MemoryPressureReading> {
    match reading {
        Ok(pressure) => match host_pressure::may_start(*pressure) {
            Decision::Start => None,
            Decision::Wait { pressure } => Some(MemoryPressureReading::Read { pressure }),
        },
        Err(error) => Some(MemoryPressureReading::Unread {
            error: error.clone(),
        }),
    }
}

fn milliseconds(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

enum Message {
    Admit {
        ticket: u64,
        facts: TaskFacts,
        reply: oneshot::Sender<Result<TaskGrant, TaskRefusal>>,
    },
    Terminal {
        unit: Arc<str>,
        outcome: TaskOutcome,
        reply: oneshot::Sender<Result<(), TaskRefusal>>,
    },
    Leave {
        ticket: u64,
    },
    Queue {
        reply: oneshot::Sender<TaskQueueStatus>,
    },
}

/// The task server's handle, held by the control plane like the disk lease.
#[derive(Clone, Debug)]
pub struct TaskServer {
    messages: mpsc::UnboundedSender<Message>,
    tickets: Arc<AtomicU64>,
    /// Serializes ticket minting with the admission's enqueue: the client-visible ticket order
    /// is the queue-entry order, so no two concurrent asks can invert it.
    mint: Arc<Mutex<()>>,
}

/// One live admission, held open by its connection like a disk lease: dropping it leaves
/// the queue or abandons the run, and a killed client releases everything with its socket.
#[derive(Debug)]
pub struct Tenure {
    ticket: u64,
    messages: mpsc::UnboundedSender<Message>,
}

impl Drop for Tenure {
    fn drop(&mut self) {
        let _ = self.messages.send(Message::Leave {
            ticket: self.ticket,
        });
    }
}

impl TaskServer {
    pub fn start(limits: TaskServerLimits) -> Self {
        let (messages, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(limits, receiver));
        Self {
            messages,
            tickets: Arc::new(AtomicU64::new(1)),
            mint: Arc::new(Mutex::new(())),
        }
    }

    /// Admit one unit and hold its attempt: the answer is its grant once the scheduler selects
    /// it, or a refusal.
    pub fn ask(
        &self,
        facts: TaskFacts,
    ) -> (Tenure, oneshot::Receiver<Result<TaskGrant, TaskRefusal>>) {
        // Minting the ticket and enqueueing the admission is one critical section, so the ticket
        // order is the queue-entry order even under concurrent asks.
        let minted = self.mint.lock().expect("the mint lock is never poisoned");
        let ticket = self.tickets.fetch_add(1, AtomicOrdering::Relaxed);
        let (reply, granted) = oneshot::channel();
        let tenure = Tenure {
            ticket,
            messages: self.messages.clone(),
        };
        let _ = self.messages.send(Message::Admit {
            ticket,
            facts,
            reply,
        });
        drop(minted);
        (tenure, granted)
    }

    /// Record the unit's terminal outcome; its lease classes return at once.
    pub async fn terminal(&self, unit: &str, outcome: TaskOutcome) -> Result<(), TaskRefusal> {
        let (reply, settled) = oneshot::channel();
        self.messages
            .send(Message::Terminal {
                unit: Arc::from(unit),
                outcome,
                reply,
            })
            .map_err(|_| TaskRefusal::Stopped)?;
        settled.await.map_err(|_| TaskRefusal::Stopped)?
    }

    /// The whole fold at this instant: what is ready in order, what waits on what, and who holds
    /// each lease.
    pub async fn queue(&self) -> Option<TaskQueueStatus> {
        let (reply, status) = oneshot::channel();
        self.messages.send(Message::Queue { reply }).ok()?;
        status.await.ok()
    }
}

async fn run(limits: TaskServerLimits, mut receiver: mpsc::UnboundedReceiver<Message>) {
    let mut ledger = Ledger::new(limits);
    let mut replies: HashMap<u64, oneshot::Sender<Result<TaskGrant, TaskRefusal>>> = HashMap::new();
    let mut pending_queue: Vec<oneshot::Sender<TaskQueueStatus>> = Vec::new();
    loop {
        // Every message, the oldest waiter's age bound and, while a unit is held for memory, the
        // next reading recompute the schedule.
        let mut follow: Vec<Effect> = Vec::new();
        tokio::select! {
            message = receiver.recv() => match message {
                Some(Message::Admit { ticket, facts, reply }) => {
                    match ledger.admit(Instant::now(), ticket, facts) {
                        Ok(()) => {
                            replies.insert(ticket, reply);
                        }
                        Err(refusal) => {
                            let _ = reply.send(Err(refusal));
                        }
                    }
                }
                Some(Message::Terminal { unit, outcome, reply }) => {
                    let settled = ledger.terminal(&unit, outcome, &mut follow);
                    if let Ok(facts) = &settled {
                        cowshed_os::eprintln!(
                            "cowshed: timing task-server terminal unit={:?} owner={:?} incarnation={:?} outcome={outcome}",
                            facts.unit,
                            facts.owner,
                            facts.incarnation,
                        );
                    }
                    let _ = reply.send(settled.map(|_| ()));
                }
                Some(Message::Leave { ticket }) => {
                    if let Some(facts) = ledger.leave(ticket, &mut follow) {
                        cowshed_os::eprintln!(
                            "cowshed: timing task-server terminal ticket={ticket} unit={:?} owner={:?} incarnation={:?} outcome=abandoned",
                            facts.unit,
                            facts.owner,
                            facts.incarnation,
                        );
                    }
                }
                Some(Message::Queue { reply }) => pending_queue.push(reply),
                None => return,
            },
            _ = wake_at(ledger.wake(Instant::now())) => {}
        }

        let mut observation = LeverageObservation::new();
        ledger.select(
            Instant::now(),
            &mut observation,
            host_pressure::read_memory_pressure,
            &mut follow,
        );

        for effect in follow {
            match effect {
                Effect::Release { ticket } => {
                    replies.remove(&ticket);
                }
                Effect::Select {
                    ticket,
                    attempt,
                    band,
                    waited,
                } => {
                    let facts = &ledger
                        .unit_of(ticket)
                        .expect("selected ticket remains live")
                        .facts;
                    cowshed_os::eprintln!(
                        "cowshed: timing task-server select ticket={ticket} unit={:?} attempt={attempt} owner={:?} incarnation={:?} waited_ns={} band={band:?} leases={:?} command={:?} pid={:?}",
                        facts.unit,
                        facts.owner,
                        facts.incarnation,
                        waited.as_nanos(),
                        facts.leases,
                        facts.command,
                        facts.pid,
                    );
                    let Some(reply) = replies.remove(&ticket) else {
                        // The client left before its turn: abandon the attempt it freed.
                        observation.leave(&mut ledger, ticket, &mut Vec::new());
                        continue;
                    };
                    let _ = reply.send(Ok(TaskGrant {
                        unit: facts.unit.to_string(),
                        band,
                        caps: facts.caps,
                    }));
                }
            }
        }
        for reply in pending_queue.drain(..) {
            let _ = reply.send(ledger.status(Instant::now(), &mut observation));
        }
    }
}

/// Sleeps until `deadline`, or forever when nothing is due: the age bound and the memory
/// re-check fire on this wake alone, needing no arrival.
async fn wake_at(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_gateway_types::host_status::DarwinPressureLevel;

    /// A ledger at a controllable clock, fed explicit tickets.
    struct Clock {
        ledger: Ledger,
        origin: Instant,
    }

    impl Clock {
        fn new(age_ms: u64) -> Self {
            let limits = TaskServerLimits {
                age_bound: Duration::from_millis(age_ms),
                ..TaskServerLimits::default()
            };
            Clock {
                ledger: Ledger::new(limits),
                origin: Instant::now(),
            }
        }

        fn at(&self, ms: u64) -> Instant {
            self.origin + Duration::from_millis(ms)
        }

        fn admit(&mut self, ms: u64, ticket: u64, facts: TaskFacts) -> Result<(), TaskRefusal> {
            self.ledger.admit(self.at(ms), ticket, facts)
        }

        fn select(&mut self, ms: u64) -> Vec<String> {
            self.select_under(ms, NORMAL)
        }

        fn select_under(&mut self, ms: u64, pressure: MemoryPressure) -> Vec<String> {
            let mut effects = Vec::new();
            let mut observation = LeverageObservation::new();
            self.ledger
                .select(self.at(ms), &mut observation, || Ok(pressure), &mut effects);

            effects
                .iter()
                .filter_map(|effect| match effect {
                    Effect::Select { ticket, .. } => self
                        .ledger
                        .unit_of(*ticket)
                        .map(|unit| unit.facts.unit.to_string()),
                    _ => None,
                })
                .collect()
        }

        fn terminal(&mut self, unit: &str, outcome: TaskOutcome) {
            let mut effects = Vec::new();
            self.ledger
                .terminal(unit, outcome, &mut effects)
                .expect("live attempt");
        }

        fn leave(&mut self, ticket: u64) -> Vec<Effect> {
            let mut effects = Vec::new();
            self.ledger.leave(ticket, &mut effects);
            effects
        }

        fn status(&self, ms: u64) -> TaskQueueStatus {
            let mut observation = LeverageObservation::new();
            self.ledger.status(self.at(ms), &mut observation)
        }

        fn ready(&self, ms: u64) -> Vec<String> {
            let mut observation = LeverageObservation::new();
            self.ledger
                .ready_order(self.at(ms), &mut observation)
                .into_iter()
                .filter_map(|ticket| {
                    self.ledger
                        .unit_of(ticket)
                        .map(|unit| unit.facts.unit.to_string())
                })
                .collect()
        }
    }

    fn facts(unit: &str) -> TaskFacts {
        TaskFacts {
            unit: Arc::from(unit),
            owner: Arc::from("owner"),
            incarnation: None,
            deps: Vec::new(),
            leases: Vec::new(),
            priority: 0,
            urgent: false,
            class: TaskClass::Gate,
            deadline: None,
            caps: None,
            command: Arc::from("nx run gate"),
            pid: None,
        }
    }

    const NORMAL: MemoryPressure = MemoryPressure::Darwin {
        level: DarwinPressureLevel::Normal,
    };

    /// The wrong-order regression: the policy order over leverage, urgency, deadlines and
    /// dependencies is exact, and a unit runs only once its dependencies have landed.
    #[test]
    fn ready_units_run_in_the_policy_order_and_dependents_wait_for_their_deps() {
        let mut clock = Clock::new(30_000);
        // api carries two transitive dependents, impl one, tests none.
        let mut dependent = facts("impl");
        dependent.deps = vec![Arc::from("api")];
        let mut leaf = facts("tests");
        leaf.deps = vec![Arc::from("impl")];
        let mut fix = facts("fix");
        fix.urgent = true;
        let mut background = facts("bg");
        background.class = TaskClass::Background;
        let mut far = facts("far");
        far.deadline = Some(1_000);
        let mut soon = facts("soon");
        soon.deadline = Some(100);
        // Admitted in an order the policy must not honor.
        clock.admit(0, 7, leaf).unwrap();
        clock.admit(0, 6, background).unwrap();
        clock.admit(0, 5, far).unwrap();
        clock.admit(0, 4, soon).unwrap();
        clock.admit(0, 3, fix).unwrap();
        clock.admit(0, 2, dependent).unwrap();
        clock.admit(0, 1, facts("api")).unwrap();
        // Urgent first; leverage (api) next; then the known deadlines soon-before-far; the
        // background unit last, behind gate-class work; dependents never appear
        // before the units they wait on have landed.
        assert_eq!(
            clock.select(0),
            ["fix", "api", "soon", "far", "bg"],
            "the policy order over the ready set"
        );
        clock.terminal("api", TaskOutcome::Landed);
        assert_eq!(clock.select(1), ["impl"]);
        clock.terminal("impl", TaskOutcome::Landed);
        assert_eq!(clock.select(2), ["tests"]);
    }

    /// The band and class-order regression. The wrong results it rules out: a background unit
    /// reported in the gate's band, the land turn's holder reported foreground above work a person
    /// or model is waiting on, and an interactive or background unit ordered ahead of gate work.
    #[test]
    fn every_unit_runs_in_its_class_band_and_gate_work_is_ordered_first() {
        let mut clock = Clock::new(30_000);
        let mut interactive = facts("a-interactive");
        interactive.class = TaskClass::Interactive;
        let mut background = facts("b-background");
        background.class = TaskClass::Background;
        let mut land = facts("z-land");
        land.leases = vec![LeaseClass::LandPublication];
        clock.admit(0, 1, interactive).unwrap();
        clock.admit(0, 2, background).unwrap();
        clock.admit(0, 3, facts("y-gate")).unwrap();
        clock.admit(0, 4, land).unwrap();

        let bands: Vec<(String, TaskBand)> = clock
            .status(0)
            .ready
            .into_iter()
            .map(|ready| (ready.unit, ready.band))
            .collect();
        let band = |unit: &str| {
            bands
                .iter()
                .find(|(name, _)| name == unit)
                .map(|(_, band)| *band)
                .expect("ready unit")
        };
        assert_eq!(band("a-interactive"), TaskBand::Foreground);
        assert_eq!(band("y-gate"), TaskBand::Normal);
        assert_eq!(band("z-land"), TaskBand::Normal);
        assert_eq!(band("b-background"), TaskBand::Background);
        // The land's own queue first, then gate work ahead of interactive and background work.
        assert_eq!(
            clock.ready(0),
            ["z-land", "y-gate", "a-interactive", "b-background"]
        );
    }

    /// The memory rule at its threshold (05_gateway.md "Host scheduling", "Memory pressure"): at
    /// Warning a background unit is held and its queue entry names the reading, while the gate
    /// unit beside it is selected; at Normal the background unit goes.
    #[test]
    fn a_background_unit_waits_under_memory_pressure_and_a_gate_unit_does_not() {
        let mut clock = Clock::new(30_000);
        let mut background = facts("bg");
        background.class = TaskClass::Background;
        clock.admit(0, 1, background).unwrap();
        clock.admit(0, 2, facts("gate")).unwrap();
        let warning = MemoryPressure::Darwin {
            level: DarwinPressureLevel::Warning,
        };
        assert_eq!(clock.select_under(0, warning), ["gate"]);

        let status = clock.status(0);

        let held = status
            .ready
            .iter()
            .find(|ready| ready.unit == "bg")
            .expect("the held unit stays ready");
        assert_eq!(
            held.memory_wait,
            Some(MemoryPressureReading::Read { pressure: warning })
        );
        assert_eq!(clock.select_under(1, NORMAL), ["bg"]);
    }

    /// The determinism regression: the same facts produce the same order, however they arrive.
    #[test]
    fn the_same_facts_produce_the_same_order_however_they_arrive() {
        let build = |reverse: bool| {
            let mut clock = Clock::new(30_000);
            let mut pieces: Vec<(u64, TaskFacts)> =
                vec![(1, facts("alpha")), (2, facts("beta")), (3, facts("gamma"))];
            let mut depending = facts("delta");
            depending.deps = vec![Arc::from("alpha")];
            let mut urgent = facts("epsilon");
            urgent.urgent = true;
            pieces.push((4, depending));
            pieces.push((5, urgent));
            if reverse {
                pieces.reverse();
            }
            for (ticket, facts) in pieces {
                clock.admit(0, ticket, facts).unwrap();
            }
            clock.ready(0)
        };
        assert_eq!(build(false), build(true));
        assert_eq!(build(false), ["epsilon", "alpha", "beta", "gamma"]);
    }

    /// The starvation regression: one-place class, a stream of fresher urgent arrivals, and a
    /// unit that only the age bound ever puts first. Without aged waiters outranking fresh
    /// priority the stream starves it forever.
    #[test]
    fn a_starved_unit_runs_once_aged_however_often_fresher_work_arrives() {
        let mut clock = Clock::new(200);
        let mut lonely = facts("lonely");
        lonely.leases = vec![LeaseClass::Image];
        clock.admit(0, 1, lonely).unwrap();
        let mut ran: Vec<(u64, String)> = Vec::new();
        for step in 1..=8_u64 {
            let ms = step * 50;
            let mut arrival = facts(&format!("urgent-{step}"));
            arrival.urgent = true;
            arrival.leases = vec![LeaseClass::Image];
            clock.admit(ms, step + 1, arrival).unwrap();
            let selected = clock.select(ms);
            for unit in &selected {
                ran.push((ms, unit.to_string()));
                clock.terminal(unit, TaskOutcome::Landed);
            }
        }
        // The aged waiter runs the moment its bound passes, before every fresher urgent arrival;
        // the one-place class makes each pass run exactly one unit, so an arrival stream that
        // never ages it would keep it queued forever.
        assert_eq!(
            ran.iter()
                .map(|(ms, unit)| (*ms, unit.as_str()))
                .collect::<Vec<_>>(),
            [
                (50, "urgent-1"),
                (100, "urgent-2"),
                (150, "urgent-3"),
                (200, "lonely"),
                (250, "urgent-4"),
                (300, "urgent-5"),
                (350, "urgent-6"),
                (400, "urgent-7"),
            ],
            "aged waiters outrank the fresh stream at the bound"
        );
        // The wake that ages it is itself a scheduled fact.
        let mut clock = Clock::new(200);
        let mut lonely = facts("lonely");
        lonely.leases = vec![LeaseClass::Image];
        clock.admit(0, 1, lonely).unwrap();
        assert_eq!(
            clock.ledger.wake(clock.at(0)),
            Some(clock.at(200)),
            "the age bound is a wake, not a poll"
        );
    }

    /// The serialization rules the operator keeps by hand: one `cowshed new` at a time, and an
    /// urgent land waits behind the publication turn in progress.
    #[test]
    fn serial_classes_hold_one_unit_at_a_time_and_urgent_lands_wait_behind_the_turn() {
        let mut clock = Clock::new(30_000);
        let mut fork_a = facts("fork-a");
        fork_a.leases = vec![LeaseClass::CowshedNew, LeaseClass::Image];
        let mut fork_b = facts("fork-b");
        fork_b.leases = vec![LeaseClass::CowshedNew, LeaseClass::Image];
        clock.admit(0, 1, fork_a).unwrap();
        clock.admit(0, 2, fork_b).unwrap();
        assert_eq!(clock.select(0), ["fork-a"], "one cowshed new at a time");

        let status = clock.status(0);

        let fork_b_status = status
            .ready
            .iter()
            .find(|entry| entry.unit == "fork-b")
            .expect("fork-b is ready behind its turn");
        assert!(
            fork_b_status
                .blocked_leases
                .iter()
                .any(|wait| wait.class == LeaseClass::CowshedNew && wait.holders == ["fork-a"]),
            "the queue names what holds the class"
        );
        // A land turn in progress holds the publication fence; an urgent land waits behind it,
        // and both fork-b and the urgent land hold up neither the turn nor each other.
        let mut turn = facts("turn");
        turn.leases = vec![LeaseClass::LandPublication];
        let mut urgent_land = facts("urgent-land");
        urgent_land.leases = vec![LeaseClass::LandPublication];
        urgent_land.urgent = true;
        clock.admit(1, 3, turn).unwrap();
        assert_eq!(
            clock.select(1),
            ["turn"],
            "the turn takes the fence at its own event"
        );
        clock.admit(2, 4, urgent_land).unwrap();
        assert!(
            clock.select(2).is_empty(),
            "the urgent land waits behind the turn in progress; fork-b waits its class"
        );
        clock.terminal("turn", TaskOutcome::Landed);
        assert_eq!(
            clock.select(3),
            ["urgent-land"],
            "the urgent land takes the freed fence; fork-b still waits its class"
        );
        clock.terminal("fork-a", TaskOutcome::Landed);
        assert_eq!(clock.select(4), ["fork-b"]);
    }

    /// The land queue's own order: the publication fence goes to the first claimant in
    /// admission order — the land lock's one order — never to a later urgent claimant.
    #[test]
    fn lands_queue_in_admission_order_however_urgent_a_later_claimant_is() {
        let mut clock = Clock::new(30_000);
        let mut holder = facts("turn");
        holder.leases = vec![LeaseClass::LandPublication];
        let mut first = facts("first-land");
        first.leases = vec![LeaseClass::LandPublication];
        let mut urgent = facts("urgent-land");
        urgent.leases = vec![LeaseClass::LandPublication];
        urgent.urgent = true;
        clock.admit(0, 1, holder).unwrap();
        assert_eq!(clock.select(0), ["turn"]);
        clock.admit(1, 2, first).unwrap();
        clock.admit(2, 3, urgent).unwrap();
        assert!(
            clock.select(2).is_empty(),
            "the turn in progress holds the fence"
        );
        clock.terminal("turn", TaskOutcome::Landed);
        assert_eq!(
            clock.select(3),
            ["first-land"],
            "the first claimant takes the fence; a later urgent land waits its turn"
        );
        clock.terminal("first-land", TaskOutcome::Landed);
        assert_eq!(clock.select(4), ["urgent-land"]);
    }

    /// The queue-entry linearization: the admit fold assigns the entry order, so lands whose
    /// client tickets minted out of order can never select a later-accepted claimant ahead of
    /// an older accepted one.
    #[test]
    fn lands_never_overtake_an_older_accepted_claimant() {
        let mut clock = Clock::new(30_000);
        let mut accepted_first = facts("accepted-first");
        accepted_first.leases = vec![LeaseClass::LandPublication];
        let mut minted_older = facts("minted-older");
        minted_older.leases = vec![LeaseClass::LandPublication];
        // The client tickets minted in the opposite order of their acceptance.
        clock.admit(0, 2, accepted_first).unwrap();
        clock.admit(0, 1, minted_older).unwrap();
        assert_eq!(
            clock.select(0),
            ["accepted-first"],
            "the fold's entry order, not the caller's ticket mint, is the land order"
        );
    }

    fn clock_with_deps(pieces: &[(&str, &[&str])]) -> Clock {
        let mut clock = Clock::new(30_000);
        for (index, (unit, deps)) in pieces.iter().enumerate() {
            let mut admitted = facts(unit);
            admitted.deps = deps.iter().map(|dep| Arc::from(*dep)).collect();
            clock
                .admit(0, u64::try_from(index + 1).expect("ticket"), admitted)
                .unwrap();
        }
        clock
    }

    fn sorted_leverage(clock: &Clock) -> Vec<(String, usize)> {
        let mut counts: Vec<(String, usize)> = clock
            .ledger
            .leverage()
            .into_iter()
            .map(|(unit, count)| (unit.to_string(), count))
            .collect();
        counts.sort();
        counts
    }

    fn named_counts(rows: &[(&str, usize)]) -> Vec<(String, usize)> {
        rows.iter()
            .map(|(unit, count)| (unit.to_string(), *count))
            .collect()
    }

    /// The leverage table: distinct transitive dependents alone count, across chains,
    /// diamonds with repeated deps, cycles, self-deps, missing deps and terminal units. A
    /// terminal unit is no longer a counting source, but a walk still passes through its
    /// deps and still counts it when a live unit depends on it.
    #[test]
    fn leverage_counts_distinct_transitive_dependents_across_graph_shapes() {
        let chain = clock_with_deps(&[("a", &[]), ("b", &["a"]), ("c", &["b"]), ("d", &["c"])]);
        assert_eq!(
            sorted_leverage(&chain),
            named_counts(&[("a", 3), ("b", 2), ("c", 1)]),
            "a chain counts every unit behind each link once"
        );

        let diamond = clock_with_deps(&[
            ("base", &[]),
            ("left", &["base"]),
            ("right", &["base"]),
            ("top", &["left", "right", "left"]),
        ]);
        assert_eq!(
            sorted_leverage(&diamond),
            named_counts(&[("base", 3), ("left", 1), ("right", 1)]),
            "a diamond and a repeated dep name still count each dependent once"
        );

        let cycle = clock_with_deps(&[("x", &["y"]), ("y", &["x", "z"]), ("z", &["y"])]);
        assert_eq!(
            sorted_leverage(&cycle),
            named_counts(&[("x", 2), ("y", 2), ("z", 2)]),
            "a cycle counts the other live units and never the source itself"
        );

        let self_dep = clock_with_deps(&[("s", &["s", "a"]), ("a", &[])]);
        assert_eq!(
            sorted_leverage(&self_dep),
            named_counts(&[("a", 1)]),
            "a self dep counts nothing for its own unit"
        );

        let missing = clock_with_deps(&[("m", &["ghost", "a"]), ("a", &["ghost"])]);
        assert_eq!(
            sorted_leverage(&missing),
            named_counts(&[("a", 1), ("ghost", 2)]),
            "a missing dep is counted where it is named and expands to nothing"
        );

        let mut terminal = clock_with_deps(&[
            ("base", &[]),
            ("mid", &["base"]),
            ("leaf", &["mid"]),
            ("other", &["base"]),
        ]);
        terminal.terminal("mid", TaskOutcome::Landed);
        assert_eq!(
            sorted_leverage(&terminal),
            named_counts(&[("base", 2), ("mid", 1)]),
            "the terminal mid no longer counts as a source, but leaf still reaches base through it"
        );
    }

    /// Larger graph counts retain one actual computation per explicit pass without
    /// asserting anything about the standard hash set's random comparison layout.
    #[test]
    fn leverage_counts_large_chains_and_shared_diamonds_in_one_pass() {
        for n in [1_usize, 8, 64, 128, 256] {
            let mut clock = Clock::new(30_000);
            for index in 0..n {
                let mut unit = facts(&format!("u{:03}", index));
                if index > 0 {
                    unit.deps = vec![Arc::from(format!("u{:03}", index - 1).as_str())];
                }
                clock
                    .admit(0, u64::try_from(index + 1).expect("ticket"), unit)
                    .unwrap();
            }
            reset_leverage_work();
            let counts = clock.ledger.leverage();
            let work = leverage_work();
            assert_eq!(work.passes, 1, "one leverage call is one pass at n={}", n);
            if n > 1 {
                assert_eq!(counts.get("u000"), Some(&(n - 1)));
                assert_eq!(counts.get(format!("u{:03}", n - 1).as_str()), None);
            }
        }

        // A fan of diamonds: every mid reaches base, and top reaches base through all of
        // them, so duplicate arrivals at the visited set are the common case.
        let mut clock = Clock::new(30_000);
        clock.admit(0, 1, facts("base")).unwrap();
        let mut top = facts("top");
        for index in 0..64_usize {
            let mut mid = facts(&format!("m{:02}", index));
            mid.deps = vec![Arc::from("base")];
            top.deps.push(Arc::from(format!("m{:02}", index).as_str()));
            clock
                .admit(0, u64::try_from(index + 2).expect("ticket"), mid)
                .unwrap();
        }
        top.deps.push(Arc::from("m00"));
        clock.admit(0, 66, top).unwrap();
        reset_leverage_work();
        let counts = clock.ledger.leverage();
        let work = leverage_work();
        assert_eq!(counts.get("base"), Some(&65));
        assert_eq!(counts.get("m37"), Some(&1));
        assert_eq!(work.passes, 1);
    }

    /// The epoch rule at the observation the actor itself uses: selection and the queue
    /// status after it share one leverage pass while the graph is unchanged. A stale leave
    /// changes nothing and keeps that pass; actual post-selection abandonments drop it, and
    /// however many land before the status, the status takes exactly one fresh pass and
    /// reports the abandoned graph.
    #[test]
    fn an_observation_survives_a_stale_leave_and_refreshes_once_after_actual_abandonments() {
        // x reaches r only through the landed t, so x is ready beside r and still counts
        // toward r's leverage until x itself abandons. The one image place selects the
        // urgent x and leaves r and z ready behind it.
        let build = || {
            let mut clock = Clock::new(30_000);
            let mut r = facts("r");
            r.leases = vec![LeaseClass::Image];
            let mut t = facts("t");
            t.deps = vec![Arc::from("r")];
            let mut x = facts("x");
            x.deps = vec![Arc::from("t")];
            x.leases = vec![LeaseClass::Image];
            x.urgent = true;
            let mut z = facts("z");
            z.leases = vec![LeaseClass::Image];
            clock.admit(0, 1, r).unwrap();
            clock.admit(0, 2, t).unwrap();
            clock.admit(0, 3, x).unwrap();
            clock.admit(0, 4, z).unwrap();
            clock.terminal("t", TaskOutcome::Landed);
            clock
        };

        let mut clock = build();
        reset_leverage_work();
        let mut observation = LeverageObservation::new();
        let mut effects = Vec::new();
        clock
            .ledger
            .select(clock.at(0), &mut observation, || Ok(NORMAL), &mut effects);
        assert!(
            matches!(effects.as_slice(), [Effect::Select { ticket: 3, .. }]),
            "the urgent x takes the one image place"
        );
        assert!(
            observation
                .leave(&mut clock.ledger, 999, &mut Vec::new())
                .is_none(),
            "an unknown ticket abandons nothing"
        );
        let status = clock.ledger.status(clock.at(0), &mut observation);
        assert_eq!(
            leverage_work().passes,
            1,
            "select and status share one pass, and a leave that returns None does not invalidate it"
        );
        assert_eq!(
            status
                .ready
                .into_iter()
                .map(|ready| (ready.unit, ready.leverage))
                .collect::<Vec<_>>(),
            named_counts(&[("r", 1), ("z", 0)]),
            "x still counts toward r through the landed t"
        );

        let mut clock = build();
        reset_leverage_work();
        let mut observation = LeverageObservation::new();
        let mut effects = Vec::new();
        clock
            .ledger
            .select(clock.at(0), &mut observation, || Ok(NORMAL), &mut effects);
        assert_eq!(
            observation
                .leave(&mut clock.ledger, 3, &mut Vec::new())
                .map(|facts| facts.unit.to_string()),
            Some("x".to_string()),
            "the selected x actually abandons after selection"
        );
        assert!(
            observation
                .leave(&mut clock.ledger, 4, &mut Vec::new())
                .is_some(),
            "the queued z actually abandons too, before any status reads the graph"
        );
        let status = clock.ledger.status(clock.at(0), &mut observation);
        assert_eq!(
            leverage_work().passes,
            2,
            "two actual abandonments before the status still cost exactly one fresh pass"
        );
        assert_eq!(
            status
                .ready
                .into_iter()
                .map(|ready| (ready.unit, ready.leverage))
                .collect::<Vec<_>>(),
            named_counts(&[("r", 0)]),
            "with x abandoned, nothing live counts toward r any more"
        );
    }

    /// The epoch rule on the real actor: one queue turn runs selection and then answers
    /// the queue from the same unchanged graph, so the whole turn takes one leverage pass.
    /// The current-thread runtime pins the actor to this test thread, which is where the
    /// thread-local pass counter lives: were the actor on another thread, this thread
    /// would read zero passes and fail rather than count someone else's work.
    #[tokio::test(flavor = "current_thread")]
    async fn one_queue_turn_shares_one_leverage_pass_between_select_and_status() {
        let server = TaskServer::start(TaskServerLimits::default());
        let mut holder = facts("holder");
        holder.leases = vec![LeaseClass::Image];
        let mut api = facts("api");
        api.leases = vec![LeaseClass::Image];
        let mut other = facts("other");
        other.leases = vec![LeaseClass::Image];
        let mut dependent = facts("impl");
        dependent.deps = vec![Arc::from("api")];
        let mut leaf = facts("leaf");
        leaf.deps = vec![Arc::from("impl")];
        let mut tenures = Vec::new();
        let mut grants = Vec::new();
        for unit_facts in [holder, api, other, dependent, leaf] {
            let (tenure, grant) = server.ask(unit_facts);
            tenures.push(tenure);
            grants.push(grant);
        }
        // The holder runs with the one image place; api and other stay ready behind it,
        // and impl and leaf wait behind api. One queue reply settles every admission.
        let settled = server.queue().await.expect("the queue settles the setup");
        assert_eq!(
            settled
                .ready
                .iter()
                .map(|ready| ready.unit.as_str())
                .collect::<Vec<_>>(),
            ["api", "other"]
        );
        reset_leverage_work();
        let status = server.queue().await.expect("the queue answers");
        let work = leverage_work();
        assert_eq!(
            work.passes, 1,
            "one queue turn selects and reports status from one leverage observation"
        );
        assert_eq!(
            status
                .ready
                .into_iter()
                .map(|ready| (ready.unit, ready.leverage))
                .collect::<Vec<_>>(),
            named_counts(&[("api", 2), ("other", 0)]),
            "the shared observation still reports the exact leverage"
        );
        drop(grants);
        drop(tenures);
    }

    /// Ownership: one live attempt per unit, and a delayed holder from a settled attempt can
    /// never disturb the next one.
    #[test]
    fn one_unit_keeps_one_live_attempt_and_a_stale_ticket_cannot_disturb_the_next() {
        let mut clock = Clock::new(30_000);
        clock.admit(0, 1, facts("unit")).unwrap();
        assert_eq!(
            clock.admit(0, 2, facts("unit")),
            Err(TaskRefusal::LiveAttempt)
        );
        // The old holder hangs up: the attempt abandons and a new one may start.
        clock.leave(1);
        clock.admit(2, 3, facts("unit")).unwrap();
        // A delayed leave for the dead ticket changes nothing: the new attempt still runs.
        assert!(clock.leave(1).is_empty());
        assert_eq!(clock.select(3), ["unit"]);
    }
}
