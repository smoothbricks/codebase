//! The candidate policies, each a value over the one engine: what enters which pool, how many
//! threads a runner is told, and the pool's rule (05_gateway.md "Host scheduling", "Parallelism
//! inside a job"). The engine reads nothing else, so a candidate is exactly its fields.

use cowshed_core::runtime::job_admission::Ledger as JobLedger;

use crate::admit::{Easy, List, Moldable, Order, Priority, Rule};
use crate::model::Secs;

/// Every knob a candidate sets.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Policy {
    pub(crate) name: &'static str,
    /// A one-process leaf: `bun test`, lint, typecheck.
    pub(crate) process: Leaf,
    /// A leaf that starts its own workers: `bun test --parallel`, Vitest, Stryker.
    pub(crate) workers: Leaf,
    /// A nextest run of prebuilt test binaries.
    pub(crate) nextest: Leaf,
    pub(crate) rule: Rule,
    /// Whether the pool counts cpus, or admits memory alone.
    pub(crate) cpus: CpuLedger,
    pub(crate) memory: MemoryCharge,
    /// cowshed's job ledger: mbx admits every job at once, the jobserver ledger holds a
    /// background start under memory pressure.
    pub(crate) jobs: JobLedger,
    /// The mbx priority a gate's work carries; every other class is `low`.
    pub(crate) gate: Priority,
    /// From a grant to its process running.
    pub(crate) grant_latency: Secs,
    /// How far the history estimates a rule sees are off: each task's estimate is its uncontended
    /// time times `e^(spread * z)`, `z` a fixed standard-normal draw per task; 0 is exact.
    pub(crate) estimate_spread: f64,
}

/// How a leaf enters admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Leaf {
    /// One request for its whole thread count, held until it ends: `mbx reserve --cpus n`.
    Reserve(Threads),
    /// No leaf request: each of its processes asks for one cpu as it starts.
    PerProcess(Threads),
    /// No admission: it runs when Nx starts it.
    Direct(Threads),
}

/// How many threads a leaf's runner is told to run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Threads {
    /// What it reserved in the recorded run; one thread and no reservation when it reserved
    /// nothing.
    Recorded,
    /// One per core for a runner that fans out, one for a process: the runner defaults before
    /// any share.
    PerCore,
    /// `ceil(cores / parallel)`: the static share.
    Share,
    /// As many as it can use, at most the cores.
    Width,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CpuLedger {
    Counted,
    Unbounded,
}

/// What a request charges in memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MemoryCharge {
    /// mbx today: a `--cpus`-only reservation charges nothing, an unmeasured nextest test two
    /// shares of the budget, a compile its measured peak.
    Mbx,
    /// Every request its runner's measured peak per thread.
    Measured,
}

/// The starvation bound of the list-scheduling candidates: the task server's age bound
/// (`TaskServerLimits::age_bound`, 05_gateway.md "The task server schedules work units").
pub(crate) const AGING: Secs = 30.0;

/// mbx's measured cost from a grant to the reservation's command running: a parent and a worker
/// process and a lease handoff.
pub(crate) const MBX_GRANT: Secs = 0.02;

/// The pool rule this simulation recommends for mbx: priority order (a gate's work first), first
/// come first served within a priority with the aging bound, and a moldable grant of whatever is
/// free up to the request, so a wide request never waits for the whole pool and no request waits
/// while a cpu it could use is free.
/// moldable grant of whatever is free up to the request, so a wide request never waits for the
/// whole pool and no request waits while a cpu it could use is free.
pub(crate) const POOL_RULE: Rule = Rule::Moldable(Moldable {
    order: Order::Class,
    aging: AGING,
    greedy: true,
});

const LIST: Rule = Rule::List(List {
    order: Order::ClassCriticalPath,
    backfill: true,
    aging: Some(AGING),
});

/// A, as recorded: every non-Cargo leaf `mbx reserve --cpus n` once for its runner count, the
/// gate's work at mbx's `low` (every admission of the run was), Cargo admitted per rustc.
pub(crate) const TODAY: Policy = Policy {
    name: "A today",
    process: Leaf::Reserve(Threads::Recorded),
    workers: Leaf::Reserve(Threads::Recorded),
    nextest: Leaf::Reserve(Threads::Recorded),
    rule: Rule::Mbx,
    cpus: CpuLedger::Counted,
    memory: MemoryCharge::Mbx,
    jobs: JobLedger::Mbx,
    gate: Priority::Low,
    grant_latency: MBX_GRANT,
    estimate_spread: 0.0,
};

/// A with the gate at `normal`, as 05_gateway.md "Classes" specifies.
pub(crate) const TODAY_SPECIFIED: Policy = Policy {
    name: "A gate normal",
    gate: Priority::Normal,
    ..TODAY
};

/// A before the share fix: a prebuilt-nextest leaf reserves one cpu per core.
pub(crate) const TODAY_PER_CORE: Policy = Policy {
    name: "A nextest --cpus cores",
    nextest: Leaf::Reserve(Threads::PerCore),
    ..TODAY
};

/// A with mbx's per-test admission: each nextest test process asks for its own cpu, the run
/// reserving nothing.
pub(crate) const TODAY_PER_TEST: Policy = Policy {
    name: "A per-test tickets",
    nextest: Leaf::PerProcess(Threads::PerCore),
    ..TODAY
};

/// Today's leaves, with mbx's pool backfilling any request that fits and holding everything
/// fresher for one that has waited the aging bound.
pub(crate) const TODAY_DRAIN: Policy = Policy {
    name: "A + pool aged backfill",
    rule: Rule::List(List {
        order: Order::Class,
        backfill: true,
        aging: Some(AGING),
    }),
    gate: Priority::Normal,
    ..TODAY
};

/// Today's leaves, with mbx's pool backfilling by EASY on history estimates.
pub(crate) const TODAY_EASY: Policy = Policy {
    name: "A + pool EASY",
    rule: Rule::Easy(Easy {
        order: Order::Class,
        aging: None,
    }),
    gate: Priority::Normal,
    ..TODAY
};

/// Today's leaves, with mbx's pool granting a reservation whatever is free up to its request.
pub(crate) const TODAY_MOLDABLE: Policy = Policy {
    name: "A + pool moldable",
    rule: POOL_RULE,
    gate: Priority::Normal,
    ..TODAY
};
/// B: static arithmetic. A job's runner gets `ceil(cores / parallel)` threads and no leaf asks
/// anything; mbx admits only compiles, the gate's at `normal`.
pub(crate) const STATIC_SHARE: Policy = Policy {
    name: "B static share",
    process: Leaf::Direct(Threads::Share),
    workers: Leaf::Direct(Threads::Share),
    nextest: Leaf::Direct(Threads::Share),
    gate: Priority::Normal,
    ..TODAY
};

/// C: one host pool, critical-path list scheduling with cpu and memory packing, backfill and the
/// aging bound; a leaf asks for what its runner can use.
pub(crate) const LIST_PACKING: Policy = Policy {
    name: "C list + packing",
    process: Leaf::Reserve(Threads::Width),
    workers: Leaf::Reserve(Threads::Width),
    nextest: Leaf::Reserve(Threads::Width),
    rule: LIST,
    memory: MemoryCharge::Measured,
    gate: Priority::Normal,
    ..TODAY
};

/// D: no cpu admission. Runners take their width, the kernel shares cores by class weight, a
/// leaf and each rustc wait only for their memory, and cowshed's jobserver ledger holds
/// background starts under pressure.
pub(crate) const WEIGHTS: Policy = Policy {
    name: "D weights + memory",
    rule: Rule::List(List {
        order: Order::Class,
        backfill: true,
        aging: Some(AGING),
    }),
    cpus: CpuLedger::Unbounded,
    jobs: JobLedger::Jobservers,
    ..LIST_PACKING
};

/// E: one pool of one-cpu process tokens under the recommended pool rule. Every process a runner
/// starts asks for one cpu as it starts and returns it as it exits, so a grant is never wider than
/// what runs; only a runner whose workers cannot ask reserves its width, moldably.
pub(crate) const TOKENS: Policy = Policy {
    name: "E process tokens",
    process: Leaf::PerProcess(Threads::Width),
    nextest: Leaf::PerProcess(Threads::Width),
    rule: POOL_RULE,
    grant_latency: 0.002,
    ..LIST_PACKING
};

/// E on mbx's rule and memory charge as they are: the token entry alone.
pub(crate) const TOKENS_ON_MBX: Policy = Policy {
    name: "E tokens on mbx as is",
    rule: Rule::Mbx,
    memory: MemoryCharge::Mbx,
    grant_latency: MBX_GRANT,
    ..TOKENS
};

/// F: every leaf asks the pool for the threads its runner can use, with no share or default of
/// its own, and the recommended pool grants what is free up to that; the runner runs on its grant.
pub(crate) const WIDTH_MOLDABLE: Policy = Policy {
    name: "F width + moldable pool",
    process: Leaf::Reserve(Threads::Width),
    workers: Leaf::Reserve(Threads::Width),
    nextest: Leaf::Reserve(Threads::Width),
    ..TODAY_MOLDABLE
};
/// The recommendation: the recommended pool rule in mbx, every leaf asking for the width its
/// runner can use and running on its grant, and a gate's work at `normal`.
pub(crate) const RECOMMENDED: Policy = WIDTH_MOLDABLE;
/// The candidates the score table compares, A first.
pub(crate) const CANDIDATES: [Policy; 13] = [
    TODAY,
    TODAY_SPECIFIED,
    TODAY_PER_CORE,
    TODAY_PER_TEST,
    TODAY_DRAIN,
    TODAY_EASY,
    TODAY_MOLDABLE,
    STATIC_SHARE,
    LIST_PACKING,
    WEIGHTS,
    TOKENS,
    TOKENS_ON_MBX,
    WIDTH_MOLDABLE,
];
