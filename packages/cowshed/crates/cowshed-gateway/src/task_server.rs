//! The gateway's task server: a deterministic scheduler over dependency, ownership, lease and
//! capacity facts (specs/cowshed/05_gateway.md, "The task server schedules work units").
//!
//! Work units admit with the facts of their plan — the units they wait on, the owner that runs
//! them, the CPU they would use and the serialization windows they hold — and this scheduler
//! alone decides which ready unit runs next. The order is a pure function of the logged facts and
//! the host observations: the same facts always select the same units in the same order, and no
//! model, operator or arrival order is consulted. Strong models propose task boundaries; they do
//! not choose which ready unit acquires a lease.
//!
//! Three rules do the work the operator keeps by hand today:
//!
//! - **Order.** Aged waiters first — the starvation bound, so no unit is outranked forever —
//!   then the class order (gate-class work first; interactive and background work behind it
//!   while the host is busy),
//!   the coordinator's urgent override, leverage (the transitive dependents), the cache-domain
//!   deadline, resource fit against the free capacity, and finally the stable unit id.
//! - **Serialization.** A unit's run holds its lease classes from its selection until its
//!   terminal fact: one `cowshed new` at a time, image windows serialized, and one checked land
//!   publication at a time. The land lease is a publication fence — the checked land-authority
//!   transition — never a whole-host CPU grant; gate groups run concurrently, and an urgent land
//!   waits behind the turn in progress in publication order.
//! - **Caps.** A unit may declare its own parallelism ceiling — the caps its run kind applies to
//!   itself — and the grant reports that ceiling clamped to the CPU tokens it holds. The ceiling
//!   is a clamp on the run's own parallelism, never a grant and never a scheduler default. The
//!   host budget stays the only allocator: this scheduler holds no token counter and a capacity
//!   observation never mints a grant. Its asks reach the budget in selection order, and what
//!   cannot be observed stays unknown.
//!
//! Every decision is a logged fact (`COWSHED_TIMING` shows the stream) and the whole fold is
//! inspectable through the `task-queue` control operation, so a lander reads the queue instead
//! of asking for an assignment. A unit's terminal fact is the only lifecycle this scheduler
//! speaks: it records the outcome — with the owner and workspace incarnation the receipts bind
//! to — and releases what the run held.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use cowshed_gateway_types::{
    ConfigError, CpuBudgetStatus, LeaseClass, TaskBand, TaskClass, TaskDepWait, TaskGrant,
    TaskLeaseStatus, TaskLeaseWait, TaskOutcome, TaskQueueStatus, TaskReadyStatus, TaskRunCaps,
    TaskRunningStatus, TaskWaitingStatus,
};
use tokio::sync::{mpsc, oneshot};

use crate::cpu_budget::{CpuBudget, Stake};
use crate::disk_lease::Claim;

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
/// its own declared ceiling clamped to its grant.
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
    /// The task scheduler — or the host CPU budget it serves — has stopped.
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

/// The facts a unit admits with. The scheduler selects from these and the host observations:
/// dependency, ownership, lease and capacity facts, and nothing else.
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
    /// The CPU tokens the run would use; 0 asks for none and takes no budget stake.
    pub want: usize,
    /// The coordinator's priority, higher first.
    pub priority: u32,
    /// The coordinator's "land now" override; it outranks every leverage.
    pub urgent: bool,
    pub class: TaskClass,
    /// The predicted resource cost, for the resource-fit ordering key.
    pub cost: Option<u64>,
    /// The cache-domain deadline in epoch milliseconds, for the cache-deadline ordering key.
    pub deadline: Option<u64>,
    /// The run's own parallelism ceiling, clamped to its grant; never a grant of its own.
    pub caps: Option<TaskRunCaps>,
    pub command: Arc<str>,
    /// The admitting client's pid, named on its CPU claim.
    pub pid: Option<i32>,
}

/// The host observations the schedule is computed against, folded at every event. What cannot
/// be read stays unknown and never becomes headroom.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HostFacts {
    /// The host is busy: gate-class work is live or the CPU ledger reports no free token.
    busy: bool,
    /// Free CPU tokens as the ledger reports them; `None` when it cannot be read.
    cpu_free: Option<usize>,
}

/// How a unit's predicted cost fits the free capacity: the resource-fit ordering key. Unknown
/// observations sort behind known ones rather than inventing a fit.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Fit {
    Fits,
    Unfits,
    Unknown,
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
}

/// What a [`Ledger`] transition did, for the actor to carry out. Selection effects name the
/// retained ticket; the ledger owns its facts, so an effect never copies their payload.
#[derive(Debug)]
enum Effect {
    /// The unit's turn came: its lease places are held and its run may start. The actor takes
    /// its CPU from the host budget — the only allocator — in selection order, and answers the
    /// client with the grant and the caps its run is sized to.
    Select {
        ticket: u64,
        attempt: u32,
        band: TaskBand,
        waited: Duration,
    },
    /// The attempt is over: whatever it held goes back.
    Release { ticket: u64 },
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
            },
        );
        self.tickets.insert(ticket, (unit, attempt));
        Ok(())
    }

    /// Record `outcome` for the unit's live attempt: the fact its dependents read, and the
    /// release of its lease classes and CPU grant. The settled attempt's facts come back for the
    /// decision log.
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

    /// Recompute the schedule at this instant and select every unit whose turn came and whose
    /// lease classes have room, in order. The order is a pure function of the facts: the same
    /// facts select the same units in the same order, and a lease-blocked unit holds up nothing
    /// beside it.
    fn select(&mut self, now: Instant, host: HostFacts, effects: &mut Vec<Effect>) {
        for ticket in self.ready_order(now, host) {
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
            if !fits {
                continue;
            }
            let entry = self.units.get_mut(&unit).expect("checked above");
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
    fn ready_order(&self, now: Instant, host: HostFacts) -> Vec<u64> {
        let leverage = self.leverage();
        let mut ready: Vec<&Unit> = self
            .units
            .values()
            .filter(|unit| matches!(unit.state, State::Queued))
            .filter(|unit| unit.facts.deps.iter().all(|dep| self.landed(dep)))
            .collect();
        ready.sort_by(|a, b| self.order_cmp(a, b, now, host, &leverage));
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
        let mut counts: HashMap<Arc<str>, usize> = HashMap::new();
        for unit in self
            .units
            .values()
            .filter(|unit| !matches!(unit.state, State::Terminal { .. }))
        {
            let mut seen: Vec<Arc<str>> = Vec::new();
            let mut stack = unit.facts.deps.clone();
            while let Some(dep) = stack.pop() {
                if dep == unit.facts.unit || seen.contains(&dep) {
                    continue;
                }
                seen.push(dep.clone());
                if let Some(entry) = self.units.get(&dep) {
                    stack.extend(entry.facts.deps.iter().cloned());
                }
                *counts.entry(dep).or_default() += 1;
            }
        }
        counts
    }

    /// The schedule's total order. Aged waiters run before fresh priority in the order they
    /// waited — the starvation bound — then the class band, the urgent override, leverage, the
    /// cache-domain deadline, resource fit, and the stable unit id.
    fn order_cmp(
        &self,
        a: &Unit,
        b: &Unit,
        now: Instant,
        host: HostFacts,
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
        let fit_of = |unit: &Unit| match (unit.facts.cost, host.cpu_free) {
            (Some(cost), Some(free)) if cost <= free as u64 => Fit::Fits,
            (Some(_), Some(_)) => Fit::Unfits,
            _ => Fit::Unknown,
        };
        order_band(&a.facts, host.busy)
            .cmp(&order_band(&b.facts, host.busy))
            .then_with(|| b.facts.urgent.cmp(&a.facts.urgent))
            .then_with(|| leverage_of(b).cmp(&leverage_of(a)))
            .then_with(|| match (a.facts.deadline, b.facts.deadline) {
                (Some(x), Some(y)) => x.cmp(&y),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            })
            .then_with(|| fit_of(a).cmp(&fit_of(b)))
            .then_with(|| a.facts.unit.cmp(&b.facts.unit))
    }

    /// When the oldest unaged waiter becomes an aged one: the actor wakes exactly then, so the
    /// starvation bound needs no arrival to fire.
    fn aging_deadline(&self, now: Instant) -> Option<Instant> {
        self.units
            .values()
            .filter(|unit| matches!(unit.state, State::Queued))
            .map(|unit| unit.admitted + self.limits.age_bound)
            .filter(|deadline| *deadline > now)
            .min()
    }

    /// The host observations at this instant: gate-class work is live or no CPU token is free.
    fn host_facts(&self, cpu: Option<&CpuBudgetStatus>) -> HostFacts {
        let cpu_free = cpu.map(|status| status.total.saturating_sub(status.held));
        let gate_live = self.units.values().any(|unit| {
            unit.facts.class == TaskClass::Gate && !matches!(unit.state, State::Terminal { .. })
        });
        HostFacts {
            busy: gate_live || cpu_free == Some(0),
            cpu_free,
        }
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

    /// The whole fold at this instant: the ready order, what waits on what, and who holds each
    /// lease. `granted` names the CPU tokens the budget actually granted each selected run.
    fn status(
        &self,
        now: Instant,
        host: HostFacts,
        granted: &HashMap<u64, usize>,
        cpu: Option<CpuBudgetStatus>,
    ) -> TaskQueueStatus {
        let leverage = self.leverage();
        let ready = self
            .ready_order(now, host)
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
                    want: unit.facts.want,
                    cpu_tokens: granted.get(&unit.ticket).copied(),
                    caps: unit
                        .facts
                        .caps
                        .map(|ceiling| clamp_caps(ceiling, granted.get(&unit.ticket).copied())),
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
            cpu,
        }
    }
}

/// The order's class rank (05_gateway.md "Bands", Danny tg#6693: "the gate class is served FIFO
/// at highest priority over all other execs"): gate-class work first, interactive and background
/// work behind it while the host is busy and at parity when it is not.
fn order_band(facts: &TaskFacts, busy: bool) -> u8 {
    match facts.class {
        TaskClass::Gate => 0,
        TaskClass::Interactive | TaskClass::Background => u8::from(busy),
    }
}

/// A run's declared ceiling clamped to the tokens its grant holds: a clamp on the run's own
/// parallelism, never a grant and never a scheduler default. The grant is the authority.
fn clamp_caps(ceiling: TaskRunCaps, tokens: Option<usize>) -> TaskRunCaps {
    let clamp = tokens.unwrap_or(usize::MAX);
    TaskRunCaps {
        nx_parallel: ceiling.nx_parallel.min(clamp),
        cargo_jobs: ceiling.cargo_jobs.min(clamp),
        test_threads: ceiling.test_threads.min(clamp),
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
    Granted {
        ticket: u64,
        tokens: usize,
    },
    Queue {
        reply: oneshot::Sender<TaskQueueStatus>,
    },
}

/// The task server's handle, held by the control plane like the CPU budget and the disk lease.
#[derive(Clone, Debug)]
pub struct TaskServer {
    messages: mpsc::UnboundedSender<Message>,
    tickets: Arc<AtomicU64>,
    /// Serializes ticket minting with the admission's enqueue: the client-visible ticket order
    /// is the queue-entry order, so no two concurrent asks can invert it.
    mint: Arc<Mutex<()>>,
}

/// One live admission, held open by its connection like a CPU-token stake: dropping it leaves
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
    pub fn start(limits: TaskServerLimits, cpu: CpuBudget) -> Self {
        let (messages, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(limits, cpu, messages.clone(), receiver));
        Self {
            messages,
            tickets: Arc::new(AtomicU64::new(1)),
            mint: Arc::new(Mutex::new(())),
        }
    }

    /// Admit one unit and hold its attempt: the answer is its grant once the scheduler selects
    /// it — after any CPU ask the budget holds — or a refusal.
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

    /// Record the unit's terminal outcome; its lease classes and CPU grant return at once.
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

async fn run(
    limits: TaskServerLimits,
    cpu: CpuBudget,
    records: mpsc::UnboundedSender<Message>,
    mut receiver: mpsc::UnboundedReceiver<Message>,
) {
    let mut ledger = Ledger::new(limits);
    let mut replies: HashMap<u64, oneshot::Sender<Result<TaskGrant, TaskRefusal>>> = HashMap::new();
    let mut cpu_stakes: HashMap<u64, Stake> = HashMap::new();
    let mut granted: HashMap<u64, usize> = HashMap::new();
    let mut pending_queue: Vec<oneshot::Sender<TaskQueueStatus>> = Vec::new();
    loop {
        // Every message — and the oldest waiter's age bound — recomputes the schedule.
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
                        cowshed_os::timing::event("task-server", || {
                            format!(
                                "terminal {} owner={} incarnation={} outcome={}",
                                facts.unit,
                                facts.owner,
                                facts.incarnation.as_deref().unwrap_or("none"),
                                outcome,
                            )
                        });
                    }
                    let _ = reply.send(settled.map(|_| ()));
                }
                Some(Message::Leave { ticket }) => {
                    if let Some(facts) = ledger.leave(ticket, &mut follow) {
                        cowshed_os::timing::event("task-server", || {
                            format!(
                                "terminal {} owner={} incarnation={} outcome=abandoned",
                                facts.unit,
                                facts.owner,
                                facts.incarnation.as_deref().unwrap_or("none"),
                            )
                        });
                    }
                }
                Some(Message::Granted { ticket, tokens }) => {
                    granted.insert(ticket, tokens);
                }
                Some(Message::Queue { reply }) => pending_queue.push(reply),
                None => return,
            },
            _ = aging_wake(ledger.aging_deadline(Instant::now())) => {}
        }
        let cpu_status = cpu.status().await;
        let host = ledger.host_facts(cpu_status.as_ref());
        ledger.select(Instant::now(), host, &mut follow);
        for effect in follow {
            match effect {
                Effect::Release { ticket } => {
                    cpu_stakes.remove(&ticket);
                    granted.remove(&ticket);
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
                    cowshed_os::timing::event("task-server", || {
                        format!(
                            "select {} attempt={attempt} owner={} incarnation={} waited={waited:?} band={band:?} want={} leases={:?}",
                            facts.unit,
                            facts.owner,
                            facts.incarnation.as_deref().unwrap_or("none"),
                            facts.want,
                            facts.leases,
                        )
                    });
                    let Some(reply) = replies.remove(&ticket) else {
                        // The client left before its turn: abandon the attempt it freed.
                        ledger.leave(ticket, &mut Vec::new());
                        continue;
                    };
                    if facts.want == 0 {
                        let _ = reply.send(Ok(TaskGrant {
                            unit: facts.unit.to_string(),
                            band,
                            caps: facts.caps.map(|ceiling| clamp_caps(ceiling, None)),
                            cpu_tokens: None,
                        }));
                        continue;
                    }
                    // The host budget stays the only allocator: selected work submits its ask to
                    // it, in selection order, and no capacity observation mints a grant here.
                    let want = NonZeroUsize::new(facts.want).expect("want is positive here");
                    let (stake, tokens) = cpu.ask(
                        Arc::clone(&facts.owner),
                        want,
                        Claim::new(&facts.command, facts.pid),
                        crate::cpu_budget::Rank::Background,
                    );
                    cpu_stakes.insert(ticket, stake);
                    let ceiling = facts.caps;
                    let unit = facts.unit.to_string();
                    let records = records.clone();
                    tokio::spawn(async move {
                        let answer = match tokens.await {
                            Ok(tokens) => {
                                let tokens = NonZeroUsize::new(tokens)
                                    .expect("the budget grants at least one token");
                                let _ = records.send(Message::Granted {
                                    ticket,
                                    tokens: tokens.get(),
                                });
                                Ok(TaskGrant {
                                    unit,
                                    band,
                                    caps: ceiling
                                        .map(|ceiling| clamp_caps(ceiling, Some(tokens.get()))),
                                    cpu_tokens: Some(tokens),
                                })
                            }
                            Err(_) => Err(TaskRefusal::Stopped),
                        };
                        let _ = reply.send(answer);
                    });
                }
            }
        }
        for reply in pending_queue.drain(..) {
            let _ = reply.send(ledger.status(Instant::now(), host, &granted, cpu_status.clone()));
        }
    }
}

/// Sleeps until the oldest waiter's age bound, or forever when there is no waiter to age: the
/// starvation bound fires on this wake alone, needing no arrival.
async fn aging_wake(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ledger at a controllable clock, fed explicit tickets like the CPU budget's test clock.
    struct Clock {
        ledger: Ledger,
        origin: Instant,
        granted: HashMap<u64, usize>,
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
                granted: HashMap::new(),
            }
        }

        fn at(&self, ms: u64) -> Instant {
            self.origin + Duration::from_millis(ms)
        }

        fn admit(&mut self, ms: u64, ticket: u64, facts: TaskFacts) -> Result<(), TaskRefusal> {
            self.ledger.admit(self.at(ms), ticket, facts)
        }

        fn select(&mut self, ms: u64, host: HostFacts) -> Vec<String> {
            let mut effects = Vec::new();
            self.ledger.select(self.at(ms), host, &mut effects);
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

        fn ready(&self, ms: u64, host: HostFacts) -> Vec<String> {
            self.ledger
                .ready_order(self.at(ms), host)
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
            want: 0,
            priority: 0,
            urgent: false,
            class: TaskClass::Gate,
            cost: None,
            deadline: None,
            caps: None,
            command: Arc::from("nx run gate"),
            pid: None,
        }
    }

    const IDLE: HostFacts = HostFacts {
        busy: false,
        cpu_free: Some(16),
    };
    const BUSY: HostFacts = HostFacts {
        busy: true,
        cpu_free: Some(0),
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
        // unknown-deadline background unit last even on an idle host; dependents never appear
        // before the units they wait on have landed.
        assert_eq!(
            clock.select(0, IDLE),
            ["fix", "api", "soon", "far", "bg"],
            "the policy order over the ready set"
        );
        clock.terminal("api", TaskOutcome::Landed);
        assert_eq!(clock.select(1, IDLE), ["impl"]);
        clock.terminal("impl", TaskOutcome::Landed);
        assert_eq!(clock.select(2, IDLE), ["tests"]);
    }

    /// The priority policy: background work is demoted behind gate work while the host is busy
    /// and runs at parity when it is not — facts, not a manual priority flag.
    #[test]
    fn background_work_is_demoted_while_the_host_is_busy() {
        let mut idle = Clock::new(30_000);
        let mut background = facts("a-bg");
        background.class = TaskClass::Background;
        idle.admit(0, 1, background).unwrap();
        idle.admit(0, 2, facts("z-gate")).unwrap();
        // On an idle host only the stable id orders them.
        assert_eq!(idle.ready(0, IDLE), ["a-bg", "z-gate"]);
        let mut busy = Clock::new(30_000);
        let mut background = facts("a-bg");
        background.class = TaskClass::Background;
        busy.admit(0, 1, background).unwrap();
        busy.admit(0, 2, facts("z-gate")).unwrap();
        assert_eq!(busy.ready(0, BUSY), ["z-gate", "a-bg"]);
    }

    /// The band regression (05_gateway.md "Bands", Danny tg#6693: interactive above gate above
    /// regular exec, the gate first for CPU). The wrong results it rules out: a background unit
    /// reported in the gate's band on a quiet host, the land turn's holder reported foreground
    /// above work a person or model is waiting on, and an interactive unit ordered for CPU ahead
    /// of gate work on a busy host.
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
        for host in [IDLE, BUSY] {
            let bands: Vec<(String, TaskBand)> = clock
                .ledger
                .status(clock.at(0), host, &clock.granted, None)
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
            assert_eq!(band("a-interactive"), TaskBand::Foreground, "{host:?}");
            assert_eq!(band("y-gate"), TaskBand::Normal, "{host:?}");
            assert_eq!(band("z-land"), TaskBand::Normal, "{host:?}");
            assert_eq!(band("b-background"), TaskBand::Background, "{host:?}");
        }
        // The land's own queue first, then gate work ahead of interactive and background work on
        // a busy host; on a quiet host only the stable id orders the rest.
        assert_eq!(
            clock.ready(0, BUSY),
            ["z-land", "y-gate", "a-interactive", "b-background"]
        );
        assert_eq!(
            clock.ready(0, IDLE),
            ["z-land", "a-interactive", "b-background", "y-gate"]
        );
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
            let mut priced = facts("zeta");
            priced.cost = Some(4);
            pieces.push((4, depending));
            pieces.push((5, urgent));
            pieces.push((6, priced));
            if reverse {
                pieces.reverse();
            }
            for (ticket, facts) in pieces {
                clock.admit(0, ticket, facts).unwrap();
            }
            clock.ready(0, IDLE)
        };
        assert_eq!(build(false), build(true));
        assert_eq!(build(false), ["epsilon", "alpha", "zeta", "beta", "gamma"]);
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
            let selected = clock.select(ms, IDLE);
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
            clock.ledger.aging_deadline(clock.at(0)),
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
        assert_eq!(
            clock.select(0, IDLE),
            ["fork-a"],
            "one cowshed new at a time"
        );
        let status = clock.ledger.status(clock.at(0), IDLE, &clock.granted, None);
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
            clock.select(1, IDLE),
            ["turn"],
            "the turn takes the fence at its own event"
        );
        clock.admit(2, 4, urgent_land).unwrap();
        assert!(
            clock.select(2, IDLE).is_empty(),
            "the urgent land waits behind the turn in progress; fork-b waits its class"
        );
        clock.terminal("turn", TaskOutcome::Landed);
        assert_eq!(
            clock.select(3, IDLE),
            ["urgent-land"],
            "the urgent land takes the freed fence; fork-b still waits its class"
        );
        clock.terminal("fork-a", TaskOutcome::Landed);
        assert_eq!(clock.select(4, IDLE), ["fork-b"]);
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
        assert_eq!(clock.select(0, IDLE), ["turn"]);
        clock.admit(1, 2, first).unwrap();
        clock.admit(2, 3, urgent).unwrap();
        assert!(
            clock.select(2, IDLE).is_empty(),
            "the turn in progress holds the fence"
        );
        clock.terminal("turn", TaskOutcome::Landed);
        assert_eq!(
            clock.select(3, IDLE),
            ["first-land"],
            "the first claimant takes the fence; a later urgent land waits its turn"
        );
        clock.terminal("first-land", TaskOutcome::Landed);
        assert_eq!(clock.select(4, IDLE), ["urgent-land"]);
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
            clock.select(0, IDLE),
            ["accepted-first"],
            "the fold's entry order, not the caller's ticket mint, is the land order"
        );
    }

    /// The caps regression: a run's declared ceiling is applied and clamped to the tokens its
    /// grant holds — the ceiling bounds the run's own parallelism and is never a grant.
    #[test]
    fn a_declared_ceiling_clamps_the_runs_parallelism_to_its_grant() {
        let ceiling = TaskRunCaps {
            nx_parallel: 2,
            cargo_jobs: 4,
            test_threads: 6,
        };
        assert_eq!(clamp_caps(ceiling, Some(12)), ceiling);
        assert_eq!(
            clamp_caps(ceiling, Some(3)),
            TaskRunCaps {
                nx_parallel: 2,
                cargo_jobs: 3,
                test_threads: 3,
            }
        );
        assert_eq!(
            clamp_caps(ceiling, Some(1)),
            TaskRunCaps {
                nx_parallel: 1,
                cargo_jobs: 1,
                test_threads: 1,
            }
        );
        // No grant clamps nothing: a want-0 unit keeps its own ceiling as declared.
        assert_eq!(clamp_caps(ceiling, None), ceiling);
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
        assert_eq!(clock.select(3, IDLE), ["unit"]);
    }
}
