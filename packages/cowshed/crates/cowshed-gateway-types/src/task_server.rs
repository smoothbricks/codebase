//! The task server's wire shapes: the facts a work unit admits with, the grant its selection
//! runs under, and the queue view the scheduler reports (specs/cowshed/05_gateway.md, "The task
//! server schedules work units").
//!
//! A client writes one `TaskAdmitRequest` line and keeps the connection open, exactly like a
//! `cpu-tokens` request: the gateway answers `queued` at once and a granted `TaskGrant` when the
//! deterministic scheduler selects the unit and, where it asked for CPU, the host budget has
//! granted it. `TaskTerminalRequest` records the unit's outcome as a fact, and `TaskQueueStatus`
//! reports the whole fold: the ready order, what each waiting unit waits on, what holds each
//! lease, and the host CPU ledger as an observation — never as an allocation.

use std::fmt;
use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::cpu_budget::CpuBudgetStatus;

/// The control operation that admits a work unit and holds its attempt.
pub const TASK_ADMIT_OP: &str = "task-admit";
/// The control operation that records a unit's terminal outcome.
pub const TASK_TERMINAL_OP: &str = "task-terminal";
/// The control operation that reports the scheduler's queue.
pub const TASK_QUEUE_OP: &str = "task-queue";

/// What a work unit runs as: one row of specs/cowshed/05_gateway.md "Bands" (the owner's ruling,
/// 2026-10-17). Interactive is work a person or model is waiting on; gate is the land's gate job and
/// the work it relays, the land turn's holder included; background is every other unit. Gate-class
/// work is ordered first for CPU; interactive and background work wait behind it while the host is
/// busy and run at parity when it is not.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskClass {
    #[default]
    Gate,
    Interactive,
    Background,
}

impl TaskClass {
    /// The band this class runs in, its row of "Bands": interactive foreground, gate normal,
    /// background background. The band follows the class alone, whatever the host's state.
    pub const fn band(self) -> TaskBand {
        match self {
            Self::Interactive => TaskBand::Foreground,
            Self::Gate => TaskBand::Normal,
            Self::Background => TaskBand::Background,
        }
    }
}

/// The band a granted run is placed in, fixed by its class (`TaskClass::band`): foreground above
/// normal above background, as the nice each band runs at orders them (05_gateway.md "Bands").
/// The server owns placement, and no caller field but the class chooses it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskBand {
    Foreground,
    Normal,
    Background,
}

/// A serialization class a unit's run holds from its selection until its terminal fact: the
/// turns the operator keeps by hand today. One `cowshed new` at a time, image windows serialized,
/// and one checked land publication at a time. They bound serialized windows, never CPU: parallel
/// preparation and gates are bounded by the host CPU budget alone.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LeaseClass {
    /// The source-fork window: one `cowshed new` on the host at a time.
    CowshedNew,
    /// A workspace-image mutation window (fork, capture, adoption).
    Image,
    /// The checked land-authority transition: a publication fence, never a whole-host CPU grant.
    /// An urgent land waits behind the turn in progress, in publication order, not for CPU.
    LandPublication,
}

impl LeaseClass {
    /// Every class, in a stable order for reports.
    pub const ALL: [LeaseClass; 3] = [Self::CowshedNew, Self::Image, Self::LandPublication];

    /// The class's wire and log name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CowshedNew => "cowshed-new",
            Self::Image => "image",
            Self::LandPublication => "land-publication",
        }
    }
}

impl fmt::Display for LeaseClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A unit's terminal outcome. A live attempt whose connection closes without one is
/// `abandoned`: dependents see the truth and wait for a heal, never a false land.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskOutcome {
    Landed,
    Failed,
    Abandoned,
}

impl TaskOutcome {
    /// The outcome's wire and log name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Landed => "landed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

impl fmt::Display for TaskOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The one line a client writes to admit a unit and hold its attempt:
/// `{"op":"task-admit","unit":"…","owner":"…","want":8,"command":"…"}`.
///
/// `deps` names the units that must land before this one is ready; `leases` the serialization
/// classes the run holds while it runs; `want` the CPU tokens it would use, where 0 asks for
/// none. `priority` is the coordinator's priority (higher first, default 0) and `urgent` its
/// "land now" override, which outranks every leverage. `cost` is the predicted resource cost and
/// `deadline` the cache-domain deadline in epoch milliseconds; observations a client does not
/// have stay absent and sort behind known ones, never invented.
#[derive(Clone, Debug, Serialize)]
pub struct TaskAdmitRequest<'a> {
    op: &'static str,
    pub unit: &'a str,
    pub owner: &'a str,
    /// The workspace incarnation the run works in, as the lifecycle receipts name it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<&'a str>,
    #[serde(skip_serializing_if = "deps_are_empty")]
    pub deps: Vec<&'a str>,
    #[serde(skip_serializing_if = "leases_are_empty")]
    pub leases: Vec<LeaseClass>,
    pub want: usize,
    #[serde(skip_serializing_if = "priority_is_zero")]
    pub priority: u32,
    #[serde(skip_serializing_if = "is_false")]
    pub urgent: bool,
    #[serde(skip_serializing_if = "is_gate")]
    pub class: TaskClass,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline: Option<u64>,
    /// The run's own parallelism ceiling, clamped to its grant; never a grant of its own.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caps: Option<TaskRunCaps>,
    pub command: &'a str,
}

impl<'a> TaskAdmitRequest<'a> {
    /// A gate-class unit with no leases, priority or observations.
    pub fn new(unit: &'a str, owner: &'a str, want: usize, command: &'a str) -> Self {
        Self {
            op: TASK_ADMIT_OP,
            unit,
            owner,
            incarnation: None,
            deps: Vec::new(),
            leases: Vec::new(),
            want,
            priority: 0,
            urgent: false,
            class: TaskClass::default(),
            cost: None,
            deadline: None,
            caps: None,
            command,
        }
    }
}

/// The one line a client writes to record a unit's terminal outcome: the fact every dependent
/// reads, and the release of the unit's lease classes and CPU grant.
#[derive(Clone, Debug, Serialize)]
pub struct TaskTerminalRequest<'a> {
    op: &'static str,
    pub unit: &'a str,
    pub outcome: TaskOutcome,
}

impl<'a> TaskTerminalRequest<'a> {
    pub fn new(unit: &'a str, outcome: TaskOutcome) -> Self {
        Self {
            op: TASK_TERMINAL_OP,
            unit,
            outcome,
        }
    }
}

/// A run's own parallelism ceiling, declared with its facts and reported on its grant clamped to
/// the CPU tokens the grant holds: a clamp on the run's own parallelism, never a grant and never
/// a scheduler default. Leaf controls derive from the grant; the ceiling only bounds them.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskRunCaps {
    /// Nx task parallelism (`nx run --parallel`).
    pub nx_parallel: usize,
    /// Cargo build jobs (`CARGO_BUILD_JOBS`).
    pub cargo_jobs: usize,
    /// Test threads (`--test-threads`, `NEXTEST_TEST_THREADS`).
    pub test_threads: usize,
}

/// What a granted unit runs under: its band, its ceiling, and the CPU tokens the host budget
/// granted it — the grant being the only allocation authority. A unit that asked for no CPU
/// carries no tokens.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskGrant {
    pub unit: String,
    pub band: TaskBand,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caps: Option<TaskRunCaps>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_tokens: Option<NonZeroUsize>,
}

/// One ready unit in the order the scheduler would grant it next.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskReadyStatus {
    pub unit: String,
    pub owner: String,
    pub class: TaskClass,
    pub band: TaskBand,
    pub priority: u32,
    pub urgent: bool,
    /// The distinct units transitively depending on this one.
    pub leverage: usize,
    /// How long it has waited, in milliseconds.
    pub waited_ms: u64,
    /// Lease classes it needs that are currently full; empty when nothing but its turn blocks it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_leases: Vec<TaskLeaseWait>,
}

/// A full lease class a ready unit is waiting on.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskLeaseWait {
    pub class: LeaseClass,
    pub holders: Vec<String>,
}

/// One waiting unit and exactly what it waits on: its dependencies' outcomes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskWaitingStatus {
    pub unit: String,
    pub owner: String,
    pub waited_ms: u64,
    pub deps: Vec<TaskDepWait>,
}

/// A dependency's state as the waiting unit sees it: the outcome it reported, or nothing yet.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDepWait {
    pub unit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<TaskOutcome>,
}

/// One selected unit: what it holds while it runs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskRunningStatus {
    pub unit: String,
    pub owner: String,
    pub leases: Vec<LeaseClass>,
    pub want: usize,
    /// The CPU tokens the host budget granted it; absent while its ask is still queued and for a
    /// unit that asked for none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_tokens: Option<usize>,
    /// The run's declared ceiling clamped to its granted tokens; absent when it declares none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caps: Option<TaskRunCaps>,
    /// How long it waited from admission to grant, in milliseconds.
    pub granted_after_ms: u64,
}

/// One serialization class's ledger: its places and who holds them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskLeaseStatus {
    pub class: LeaseClass,
    pub places: usize,
    pub held: usize,
    pub holders: Vec<String>,
}

/// The scheduler's whole fold at one instant: the ready order, what waits on what, what holds
/// each lease, and the host CPU ledger as an observation. The CPU figures are folded from the
/// budget's own ledger; when it cannot be read they are absent, never fabricated headroom.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskQueueStatus {
    /// Ready units in the exact order the scheduler would grant them.
    pub ready: Vec<TaskReadyStatus>,
    pub waiting: Vec<TaskWaitingStatus>,
    pub running: Vec<TaskRunningStatus>,
    pub leases: Vec<TaskLeaseStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuBudgetStatus>,
}

fn deps_are_empty(deps: &[&str]) -> bool {
    deps.is_empty()
}

fn leases_are_empty(leases: &[LeaseClass]) -> bool {
    leases.is_empty()
}

fn priority_is_zero(priority: &u32) -> bool {
    *priority == 0
}

fn is_false(urgent: &bool) -> bool {
    !*urgent
}

fn is_gate(class: &TaskClass) -> bool {
    *class == TaskClass::Gate
}
