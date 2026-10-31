//! The engine: one discrete-event simulation of the admission stack, whatever the policy.
//!
//! A job asks cowshed's job admission to start: the real [`Admission`] state machine, fed the
//! events a supervisor feeds it. Once admitted, its Nx run starts ready tasks up to its
//! `--parallel`, in its own order. A task's leaf enters the policy's pool once, per process, or
//! not at all; a granted leaf runs its units on as many threads as its runner was told. Running
//! units share the cores as the kernel shares them, by weighted processor sharing over the class
//! weights, so time is fluid and the next event is the earliest of a unit's end, a grant's
//! wake-up, a job's arrival and a waiter's aging bound. Nothing sleeps: the clock jumps.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use cowshed_core::api::dto::JobId;
use cowshed_core::runtime::job_admission::{Admission, Effect, Event, Reading};
use cowshed_gateway_types::host_status::{DarwinPressureLevel, MemoryPressure};

use crate::admit::{Charge, Grant, Lease, Priority, Request, Rule, admit};
use crate::model::{Class, JobIx, Runner, Secs, TaskIx, Workload, list_span};
use crate::policy::{CpuLedger, Leaf, MemoryCharge, Policy, Threads};
use crate::random::Rng;

/// Work below this is done.
const DONE: Secs = 1e-9;

/// What a run of one policy over one workload did.
#[derive(Clone, Debug)]
pub(crate) struct Outcome {
    pub(crate) jobs: Vec<JobOutcome>,
    pub(crate) tasks: Vec<TaskOutcome>,
    pub(crate) log: Log,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct JobOutcome {
    pub(crate) arrival: Secs,
    pub(crate) start: Option<Secs>,
    pub(crate) end: Option<Secs>,
}

impl JobOutcome {
    /// From its request to its last task's end.
    pub(crate) fn latency(&self) -> Option<Secs> {
        self.end.map(|end| end - self.arrival)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct TaskOutcome {
    pub(crate) ready: Secs,
    pub(crate) slotted: Secs,
    pub(crate) started: Option<Secs>,
    pub(crate) end: Option<Secs>,
    pub(crate) threads: u32,
    /// How long its leaf request waited in the pool.
    pub(crate) waited: Secs,
}

/// Integrals and extremes over the whole run.
#[derive(Clone, Debug, Default)]
pub(crate) struct Log {
    /// Core-seconds of work done.
    pub(crate) busy: Secs,
    /// Core-seconds idle while work waited: in a pool, for an Nx slot, or for job admission.
    pub(crate) idle_queued: Secs,
    /// Core-seconds idle while a pool request that fit the free capacity waited.
    pub(crate) idle_fitting: Secs,
    /// Thread-seconds runnable beyond the cores.
    pub(crate) excess: Secs,
    /// Core-seconds granted but not running.
    pub(crate) held_idle: Secs,
    /// Byte-seconds of resident memory over the budget.
    pub(crate) memory_over: f64,
    pub(crate) peak_runnable: u32,
    pub(crate) peak_memory: u64,
    pub(crate) peak_held: Charge,
    /// The longest a request waited while it fit what was free.
    pub(crate) longest_fitting_wait: Secs,
    /// The longest a request waited in the pool, by class index.
    pub(crate) longest_wait: [Secs; 3],
    /// Grants of lower-class work while a gate request that fit waited.
    pub(crate) gate_behind: u32,
    /// Instants at which several grants together held more than the pool's capacity.
    pub(crate) over_budget: u32,
    pub(crate) ended_for_memory: u32,
    pub(crate) job_waits: u32,
    /// Tasks that never ended: a stuck pool or a failed job.
    pub(crate) unfinished: u32,
    pub(crate) events: u64,
    pub(crate) end: Secs,
}

/// Runs `policy` over `workload` to its end.
pub(crate) fn run(workload: &Workload, policy: &Policy) -> Outcome {
    let mut sim = Sim::new(workload, policy);
    sim.pressure();
    loop {
        sim.arrive();
        sim.settle();
        let Some(next) = sim.next_event() else {
            break;
        };
        sim.advance(next);
        sim.complete();
        sim.pressure();
    }
    sim.finish()
}

/// The cores each runnable thread of a class gets: weighted processor sharing, the fluid model of
/// a kernel that splits contended cores by class weight. A class whose fair share per thread
/// reaches a whole core runs at full speed and the rest share what is left.
pub(crate) fn share(cores: u32, threads: [u32; 3]) -> [f64; 3] {
    let mut rate = [1.0; 3];
    if threads.iter().sum::<u32>() <= cores {
        return rate;
    }
    let mut left = f64::from(cores);
    for (k, class) in Class::ALL.iter().enumerate() {
        let weights: f64 = threads[k..]
            .iter()
            .zip(&Class::ALL[k..])
            .map(|(&count, class)| f64::from(count) * class.weight())
            .sum();
        if weights == 0.0 {
            break;
        }
        let per_weight = left / weights;
        if class.weight() * per_weight >= 1.0 {
            left -= f64::from(threads[k]);
            continue;
        }
        for (rate, class) in rate[k..].iter_mut().zip(&Class::ALL[k..]) {
            *rate = class.weight() * per_weight;
        }
        break;
    }
    rate
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    Blocked,
    Ready,
    Slotted,
    Running,
    Done,
    Failed,
}

#[derive(Clone, Copy, Debug)]
struct TaskRun {
    stage: Stage,
    missing: usize,
    out: TaskOutcome,
    threads: u32,
    /// Whether each unit past the setup asks the pool for its own cpu.
    per_process: bool,
    /// What its leaf grant holds.
    held: Charge,
    /// When its leaf grant is expected to end.
    ends: Secs,
    /// No unit starts before this: its grant's wake-up.
    wake: Secs,
    next: usize,
    inflight: u32,
    finished: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JobStage {
    Unborn,
    Asked,
    Running,
    Done,
    Failed,
}

#[derive(Debug)]
struct JobRun {
    stage: JobStage,
    out: JobOutcome,
    slotted: u32,
    left: usize,
    /// Its ready tasks by their place in the job's start order.
    ready: BTreeSet<(usize, TaskIx)>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    Asking,
    Waking {
        until: Secs,
    },
    /// On the cores, ending when its class's virtual clock reaches `finish`.
    Running {
        finish: Secs,
    },
}

#[derive(Clone, Copy, Debug)]
struct Unit {
    task: TaskIx,
    /// Its one-thread work.
    work: Secs,
    phase: Phase,
    held: Charge,
    /// When its grant is expected to end.
    ends: Secs,
}

/// The cores as the kernel shares them, in virtual time. Every runnable thread of a class runs
/// at its class's one rate, so each class keeps a clock of the service one of its threads has
/// had, and a unit ends when its class's clock reaches the clock at its start plus its work
/// (generalized processor sharing's virtual time). An event costs a logarithm, not a pass over
/// every unit.
#[derive(Debug, Default)]
struct Cores {
    clock: [Secs; 3],
    /// Each class's running units by the virtual time they end at; the times are non-negative,
    /// so their bits order as their values do.
    ending: [BTreeSet<(u64, u64)>; 3],
}

impl Cores {
    fn threads(&self) -> [u32; 3] {
        self.ending
            .each_ref()
            .map(|units| u32::try_from(units.len()).unwrap_or(u32::MAX))
    }

    fn start(&mut self, class: Class, id: u64, work: Secs) -> Secs {
        let finish = self.clock[class.index()] + work;
        self.ending[class.index()].insert((finish.to_bits(), id));
        finish
    }

    fn stop(&mut self, class: Class, id: u64, finish: Secs) {
        self.ending[class.index()].remove(&(finish.to_bits(), id));
    }

    /// How long until the next unit ends at these rates.
    fn next_end(&self, rates: [f64; 3]) -> Option<Secs> {
        (0..Class::ALL.len())
            .filter_map(|c| {
                let &(finish, _) = self.ending[c].first()?;
                Some(((f64::from_bits(finish) - self.clock[c]) / rates[c]).max(0.0))
            })
            .min_by(f64::total_cmp)
    }

    fn advance(&mut self, rates: [f64; 3], dt: Secs) {
        for (clock, rate) in self.clock.iter_mut().zip(rates) {
            *clock += rate * dt;
        }
    }

    /// The units whose work is done, taken off the cores.
    fn ended(&mut self) -> Vec<u64> {
        let mut ended = Vec::new();
        for (clock, units) in self.clock.iter().zip(&mut self.ending) {
            while let Some(&(finish, id)) = units.first()
                && f64::from_bits(finish) <= clock + DONE
            {
                units.pop_first();
                ended.push(id);
            }
        }
        ended
    }
}

#[derive(Clone, Copy, Debug)]
enum Owner {
    Leaf(TaskIx),
    Process(u64),
}

#[derive(Clone, Copy, Debug)]
struct Pending {
    owner: Owner,
    threads: u32,
    request: Request,
    /// Since when it has fit what is free while it waits.
    fit_since: Option<Secs>,
}

/// How a task enters admission under the policy.
enum Path {
    Direct { threads: u32 },
    Reserve { threads: u32 },
    PerProcess { threads: u32 },
}

struct Sim<'a> {
    w: &'a Workload,
    p: &'a Policy,
    now: Secs,
    origin: Instant,
    admission: Admission,
    level: Option<DarwinPressureLevel>,
    jobs: Vec<JobRun>,
    tasks: Vec<TaskRun>,
    place: Vec<usize>,
    running: BTreeSet<usize>,
    units: BTreeMap<u64, Unit>,
    unit_ids: u64,
    cores: Cores,
    /// Granted units waiting out the grant's latency, by when they wake.
    waking: BTreeSet<(u64, u64)>,
    /// The units that hold a grant of their own.
    holding: BTreeSet<u64>,
    /// Resident memory of the units that hold a process.
    in_use: u64,
    queue: Vec<Pending>,
    tickets: u64,
    held: Charge,
    fitting: bool,
    log: Log,
}

fn job_id(j: usize) -> JobId {
    JobId::new(u64::try_from(j).expect("a job index fits u64") + 1)
        .expect("a workload holds fewer jobs than the job id range")
}

fn job_index(job: JobId) -> usize {
    usize::try_from(job.get() - 1).expect("a job id came from a job index")
}

impl<'a> Sim<'a> {
    fn new(w: &'a Workload, p: &'a Policy) -> Self {
        let mut place = vec![0; w.tasks.len()];
        for job in &w.jobs {
            for (i, t) in job.tasks.iter().enumerate() {
                place[t.0] = i;
            }
        }
        Self {
            w,
            p,
            now: 0.0,
            origin: Instant::now(),
            admission: Admission::default(),
            level: None,
            jobs: w
                .jobs
                .iter()
                .map(|job| JobRun {
                    stage: JobStage::Unborn,
                    out: JobOutcome {
                        arrival: job.arrival,
                        ..JobOutcome::default()
                    },
                    slotted: 0,
                    left: job.tasks.len(),
                    ready: BTreeSet::new(),
                })
                .collect(),
            tasks: w
                .tasks
                .iter()
                .map(|task| TaskRun {
                    stage: Stage::Blocked,
                    missing: task.deps.len(),
                    out: TaskOutcome::default(),
                    threads: 0,
                    per_process: false,
                    held: Charge::default(),
                    ends: 0.0,
                    wake: 0.0,
                    next: 0,
                    inflight: 0,
                    finished: 0,
                })
                .collect(),
            place,
            running: BTreeSet::new(),
            units: BTreeMap::new(),
            unit_ids: 0,
            cores: Cores::default(),
            waking: BTreeSet::new(),
            holding: BTreeSet::new(),
            in_use: 0,
            queue: Vec::new(),
            tickets: 0,
            held: Charge::default(),
            fitting: false,
            log: Log::default(),
        }
    }

    /// The virtual clock as the instant job admission reads: one origin, then arithmetic.
    fn instant(&self) -> Instant {
        self.origin + Duration::from_secs_f64(self.now)
    }

    fn class(&self, task: TaskIx) -> Class {
        self.w.jobs[self.w.tasks[task.0].job.0].class
    }

    fn step(&mut self, event: Event) {
        for effect in self.admission.step(self.instant(), event) {
            match effect {
                Effect::Admit { job } => self.born(job_index(job)),
                Effect::WaitStarted { .. } => self.log.job_waits += 1,
                Effect::WaitEnded { .. } => {}
                Effect::Refuse { job, .. } => self.fail(job_index(job)),
                Effect::EndForMemory { job, .. } => {
                    self.log.ended_for_memory += 1;
                    self.fail(job_index(job));
                    self.step(Event::Concluded { job });
                }
            }
        }
    }

    fn arrive(&mut self) {
        for j in 0..self.jobs.len() {
            if self.jobs[j].stage != JobStage::Unborn || self.w.jobs[j].arrival > self.now + DONE {
                continue;
            }
            self.jobs[j].stage = JobStage::Asked;
            self.step(Event::Requested {
                job: job_id(j),
                class: self.w.jobs[j].class.exec(),
                ledger: self.p.jobs,
            });
        }
    }

    fn born(&mut self, j: usize) {
        self.jobs[j].stage = JobStage::Running;
        self.jobs[j].out.start = Some(self.now);
        let w = self.w;
        for &t in &w.jobs[j].tasks {
            if self.tasks[t.0].missing == 0 {
                self.make_ready(t);
            }
        }
        self.step(Event::Spawned { job: job_id(j) });
    }

    fn make_ready(&mut self, t: TaskIx) {
        self.tasks[t.0].stage = Stage::Ready;
        self.tasks[t.0].out.ready = self.now;
        let j = self.w.tasks[t.0].job.0;
        self.jobs[j].ready.insert((self.place[t.0], t));
    }

    /// Ends a job that job admission refused or ended for memory: its waiters leave the pool,
    /// its units stop and its holds return.
    fn fail(&mut self, j: usize) {
        if matches!(self.jobs[j].stage, JobStage::Done | JobStage::Failed) {
            return;
        }
        self.jobs[j].stage = JobStage::Failed;
        self.jobs[j].ready.clear();
        let w = self.w;
        let job = JobIx(j);
        let units = &self.units;
        let task_of = |owner: Owner| match owner {
            Owner::Leaf(t) => t,
            Owner::Process(id) => units[&id].task,
        };
        let keep: Vec<bool> = self
            .queue
            .iter()
            .map(|p| w.tasks[task_of(p.owner).0].job != job)
            .collect();
        let mut keep = keep.into_iter();
        self.queue.retain(|_| keep.next().unwrap_or(true));
        let ended: Vec<u64> = self
            .units
            .iter()
            .filter(|(_, unit)| w.tasks[unit.task.0].job == job)
            .map(|(&id, _)| id)
            .collect();
        for id in ended {
            self.drop_unit(id);
        }
        for &t in &w.jobs[j].tasks {
            let run = &mut self.tasks[t.0];
            if run.stage != Stage::Done {
                self.held = self.held.minus(std::mem::take(&mut run.held));
                run.stage = Stage::Failed;
            }
            self.running.remove(&t.0);
        }
    }

    fn settle(&mut self) {
        loop {
            let slotted = self.slot();
            let granted = self.grant();
            let fanned = self.fan_out();
            if !(slotted || granted || fanned) {
                break;
            }
        }
        self.observe();
    }

    fn parallel(&self, job: JobIx) -> u32 {
        self.w.jobs[job.0].parallel.max(1)
    }

    /// Nx starts ready tasks in its own order while its run has a free slot.
    fn slot(&mut self) -> bool {
        let mut moved = false;
        for j in 0..self.jobs.len() {
            if self.jobs[j].stage != JobStage::Running {
                continue;
            }
            let parallel = self.parallel(JobIx(j));
            while self.jobs[j].slotted < parallel {
                let Some((_, t)) = self.jobs[j].ready.pop_first() else {
                    break;
                };
                self.jobs[j].slotted += 1;
                self.enter(t);
                moved = true;
            }
        }
        moved
    }

    fn enter(&mut self, t: TaskIx) {
        self.tasks[t.0].stage = Stage::Slotted;
        self.tasks[t.0].out.slotted = self.now;
        match self.path(t) {
            Path::Direct { threads } => self.start(t, threads, false, Charge::default(), self.now),
            Path::PerProcess { threads } => {
                self.start(t, threads, true, Charge::default(), self.now)
            }
            Path::Reserve { threads } => {
                let charge = self.charge(t, threads, false);
                self.ask(Owner::Leaf(t), t, threads, charge);
            }
        }
    }

    fn path(&self, t: TaskIx) -> Path {
        let task = &self.w.tasks[t.0];
        let leaf = match task.runner {
            Runner::Restore => return Path::Direct { threads: 1 },
            Runner::Cargo => {
                let threads = task.width().min(self.w.host.cores);
                return Path::PerProcess { threads };
            }
            Runner::Nextest => self.p.nextest,
            Runner::Workers => self.p.workers,
            Runner::Process => self.p.process,
        };
        match leaf {
            Leaf::Reserve(threads) => match self.threads(t, threads) {
                Some(threads) => Path::Reserve { threads },
                None => Path::Direct { threads: 1 },
            },
            Leaf::PerProcess(threads) => Path::PerProcess {
                threads: self.threads(t, threads).unwrap_or(1),
            },
            Leaf::Direct(threads) => Path::Direct {
                threads: self.threads(t, threads).unwrap_or(1),
            },
        }
    }

    /// The runner's thread count; `None` for a recorded leaf that reserved nothing.
    fn threads(&self, t: TaskIx, threads: Threads) -> Option<u32> {
        let task = &self.w.tasks[t.0];
        let cores = self.w.host.cores;
        let count = match threads {
            Threads::Recorded => return task.reserve,
            Threads::PerCore => match task.runner {
                Runner::Process | Runner::Restore => 1,
                Runner::Nextest | Runner::Workers | Runner::Cargo => cores,
            },
            Threads::Share => cores.div_ceil(self.parallel(task.job)),
            Threads::Width => task.width().min(cores),
        };
        Some(count.max(1))
    }

    fn charge(&self, t: TaskIx, threads: u32, process: bool) -> Charge {
        let task = &self.w.tasks[t.0];
        let share = self.w.host.memory / u64::from(self.w.host.cores);
        let memory = match (self.p.memory, process) {
            (MemoryCharge::Mbx, false) => 0,
            (MemoryCharge::Mbx, true) => match task.runner {
                Runner::Nextest => 2 * share,
                _ => task.unit_memory,
            },
            (MemoryCharge::Measured, _) => task.unit_memory.saturating_mul(u64::from(threads)),
        };
        let cpus = match self.p.cpus {
            CpuLedger::Counted => threads,
            CpuLedger::Unbounded => 0,
        };
        Charge { cpus, memory }
    }

    /// The task's uncontended run time on `threads`: what its history says it takes.
    fn estimate(&self, t: TaskIx, threads: u32) -> Secs {
        let task = &self.w.tasks[t.0];
        let exact = task.setup + list_span(&task.units, threads);
        if self.p.estimate_spread == 0.0 {
            return exact;
        }
        let mut rng = Rng::new(u64::try_from(t.0).unwrap_or(u64::MAX) ^ 0x5EED);
        let z = (0..12).map(|_| rng.unit()).sum::<f64>() - 6.0;
        exact * (self.p.estimate_spread * z).exp()
    }

    fn ask(&mut self, owner: Owner, t: TaskIx, threads: u32, charge: Charge) {
        let class = self.class(t);
        let estimate = match owner {
            Owner::Leaf(t) => self.estimate(t, threads),
            Owner::Process(id) => self.units[&id].work,
        };
        self.tickets += 1;
        self.queue.push(Pending {
            owner,
            threads,
            request: Request {
                ticket: self.tickets,
                class,
                priority: if class == Class::Gate {
                    self.p.gate
                } else {
                    Priority::Low
                },
                since: self.now,
                rank: self.w.bottom(t),
                estimate,
                charge,
            },
            fit_since: None,
        });
    }

    fn cap(&self) -> Charge {
        Charge {
            cpus: match self.p.cpus {
                CpuLedger::Counted => self.w.host.cores,
                CpuLedger::Unbounded => u32::MAX,
            },
            memory: self.w.host.memory,
        }
    }

    /// Every grant the pool holds, with when its holder is expected to end.
    fn live(&self) -> Vec<Lease> {
        let leaves = self.running.iter().map(|&t| Lease {
            charge: self.tasks[t].held,
            ends: self.tasks[t].ends,
        });
        let processes = self.holding.iter().map(|id| Lease {
            charge: self.units[id].held,
            ends: self.units[id].ends,
        });
        leaves
            .chain(processes)
            .filter(|lease| lease.charge != Charge::default())
            .collect()
    }

    /// The least a request needs to start under the policy's rule: a moldable request one cpu.
    fn need(&self, request: &Request) -> Charge {
        match self.p.rule {
            Rule::Moldable(_) if request.charge.cpus > 0 => {
                let cpus = request.charge.cpus;
                Charge {
                    cpus: 1,
                    memory: request.charge.memory / u64::from(cpus),
                }
            }
            _ => request.charge,
        }
    }

    fn grant(&mut self) -> bool {
        if self.queue.is_empty() {
            return false;
        }
        let requests: Vec<Request> = self.queue.iter().map(|p| p.request).collect();
        let live = self.live();
        let grants = admit(self.p.rule, &requests, &live, self.cap(), self.now);
        if grants.is_empty() {
            return false;
        }
        let mut chosen: Vec<Option<Charge>> = vec![None; requests.len()];
        for &Grant { index, charge } in &grants {
            chosen[index] = Some(charge);
        }
        let lower = grants
            .iter()
            .any(|grant| requests[grant.index].class > Class::Gate);
        let passed_over = requests.iter().zip(&chosen).any(|(r, chosen)| {
            chosen.is_none() && r.class <= Class::Gate && self.need(r).fits(self.held, self.cap())
        });
        if lower && passed_over {
            self.log.gate_behind += 1;
        }
        let queue = std::mem::take(&mut self.queue);
        for (pending, chosen) in queue.into_iter().zip(chosen) {
            match chosen {
                Some(charge) => self.give(pending, charge),
                None => self.queue.push(pending),
            }
        }
        true
    }

    fn give(&mut self, pending: Pending, charge: Charge) {
        let request = pending.request;
        let waited = self.now - request.since;
        let class = request.class.index();
        self.log.longest_wait[class] = self.log.longest_wait[class].max(waited);
        if let Some(since) = pending.fit_since {
            self.log.longest_fitting_wait = self.log.longest_fitting_wait.max(self.now - since);
        }
        self.held = self.held.plus(charge);
        let wake = self.now + self.p.grant_latency;
        match pending.owner {
            Owner::Leaf(t) => {
                self.tasks[t.0].out.waited = waited;
                let threads = if request.charge.cpus == 0 {
                    pending.threads
                } else {
                    charge.cpus
                };
                self.start(t, threads, false, charge, wake);
            }
            Owner::Process(id) => {
                let unit = self.units.get_mut(&id).expect("an asking unit is listed");
                unit.held = charge;
                unit.ends = wake + unit.work;
                self.holding.insert(id);
                self.wake_or_run(id, wake);
            }
        }
    }

    fn start(&mut self, t: TaskIx, threads: u32, per_process: bool, held: Charge, wake: Secs) {
        let ends = wake + self.estimate(t, threads);
        let run = &mut self.tasks[t.0];
        run.stage = Stage::Running;
        run.out.started = Some(self.now);
        run.out.threads = threads;
        run.threads = threads.max(1);
        run.per_process = per_process;
        run.held = held;
        run.ends = ends;
        run.wake = wake;
        self.running.insert(t.0);
    }

    /// Each running leaf starts units up to its thread count, its setup alone first; a unit that
    /// asks for its own cpu waits in the pool.
    fn fan_out(&mut self) -> bool {
        let w = self.w;
        let mut moved = false;
        let running: Vec<usize> = self.running.iter().copied().collect();
        for t in running {
            let task = &w.tasks[t];
            let setup = task.setup > 0.0;
            let count = task.units.len() + usize::from(setup);
            loop {
                let run = &self.tasks[t];
                let setup_running = setup && run.next >= 1 && run.finished == 0;
                if run.wake > self.now + DONE
                    || run.next >= count
                    || run.inflight >= run.threads
                    || setup_running
                {
                    break;
                }
                let is_setup = setup && run.next == 0;
                let work = if is_setup {
                    task.setup
                } else {
                    task.units[run.next - usize::from(setup)]
                };
                let asks = run.per_process && !is_setup;
                let id = self.unit_ids;
                self.unit_ids += 1;
                self.units.insert(
                    id,
                    Unit {
                        task: TaskIx(t),
                        work,
                        phase: Phase::Asking,
                        held: Charge::default(),
                        ends: 0.0,
                    },
                );
                let run = &mut self.tasks[t];
                run.next += 1;
                run.inflight += 1;
                if asks {
                    let charge = self.charge(TaskIx(t), 1, true);
                    self.ask(Owner::Process(id), TaskIx(t), 1, charge);
                } else {
                    self.run_unit(id);
                }
                moved = true;
            }
        }
        moved
    }

    fn runnable(&self) -> u32 {
        self.cores.threads().iter().sum()
    }

    fn memory_in_use(&self) -> u64 {
        self.in_use
    }

    fn rates(&self) -> [f64; 3] {
        share(self.w.host.cores, self.cores.threads())
    }

    /// A granted unit takes its process: it holds its memory now, and a core from `wake` on.
    fn wake_or_run(&mut self, id: u64, wake: Secs) {
        if wake > self.now {
            let unit = self.units.get_mut(&id).expect("a granted unit is listed");
            unit.phase = Phase::Waking { until: wake };
            self.in_use += self.w.tasks[unit.task.0].unit_memory;
            self.waking.insert((wake.to_bits(), id));
        } else {
            self.run_unit(id);
        }
    }

    /// Puts a unit on the cores; one that was not waking takes its memory now.
    fn run_unit(&mut self, id: u64) {
        let w = self.w;
        let unit = self.units.get_mut(&id).expect("a starting unit is listed");
        let task = &w.tasks[unit.task.0];
        if !matches!(unit.phase, Phase::Waking { .. }) {
            self.in_use += task.unit_memory;
        }
        let finish = self.cores.start(w.jobs[task.job.0].class, id, unit.work);
        unit.phase = Phase::Running { finish };
    }

    /// Takes a unit out of the run: its grant returns, and its core and memory with it.
    fn drop_unit(&mut self, id: u64) -> Option<Unit> {
        let unit = self.units.remove(&id)?;
        self.held = self.held.minus(unit.held);
        self.holding.remove(&id);
        let task = &self.w.tasks[unit.task.0];
        match unit.phase {
            Phase::Asking => {}
            Phase::Waking { until } => {
                self.waking.remove(&(until.to_bits(), id));
                self.in_use -= task.unit_memory;
            }
            Phase::Running { finish } => {
                self.cores.stop(self.w.jobs[task.job.0].class, id, finish);
                self.in_use -= task.unit_memory;
            }
        }
        Some(unit)
    }

    /// The extremes at this instant, the budget, and which waiters fit what is free: a waiter
    /// that could start on what is free after the pool decided is held against work conservation,
    /// whatever the hold protects.
    fn observe(&mut self) {
        let runnable = self.runnable();
        self.log.peak_runnable = self.log.peak_runnable.max(runnable);
        self.log.peak_memory = self.log.peak_memory.max(self.memory_in_use());
        self.log.peak_held = Charge {
            cpus: self.log.peak_held.cpus.max(self.held.cpus),
            memory: self.log.peak_held.memory.max(self.held.memory),
        };
        let cap = self.cap();
        if (self.held.cpus > cap.cpus || self.held.memory > cap.memory) && self.live().len() > 1 {
            self.log.over_budget += 1;
        }
        let held = self.held;
        let now = self.now;
        let fits: Vec<bool> = self
            .queue
            .iter()
            .map(|p| self.need(&p.request).fits(held, cap))
            .collect();
        for (pending, fits) in self.queue.iter_mut().zip(fits) {
            if fits {
                pending.fit_since.get_or_insert(now);
            } else {
                if let Some(since) = pending.fit_since.take() {
                    self.log.longest_fitting_wait = self.log.longest_fitting_wait.max(now - since);
                }
            }
        }
        self.fitting = self.queue.iter().any(|p| p.fit_since.is_some());
    }

    fn next_event(&self) -> Option<Secs> {
        let mut next = self
            .cores
            .next_end(self.rates())
            .map_or(f64::INFINITY, |dt| self.now + dt);
        if let Some(&(until, _)) = self.waking.first() {
            next = next.min(f64::from_bits(until));
        }
        for &t in &self.running {
            if self.tasks[t].wake > self.now {
                next = next.min(self.tasks[t].wake);
            }
        }
        for (j, job) in self.w.jobs.iter().enumerate() {
            if self.jobs[j].stage == JobStage::Unborn {
                next = next.min(job.arrival.max(self.now));
            }
        }
        if let Rule::List(list) = self.p.rule
            && let Some(bound) = list.aging
        {
            for pending in &self.queue {
                let at = pending.request.since + bound;
                if at > self.now {
                    next = next.min(at);
                }
            }
        }
        next.is_finite().then_some(next)
    }

    fn waiting_work(&self) -> bool {
        !self.queue.is_empty()
            || self.jobs.iter().any(|job| {
                job.stage == JobStage::Asked
                    || (job.stage == JobStage::Running && !job.ready.is_empty())
            })
    }

    fn advance(&mut self, next: Secs) {
        let dt = next - self.now;
        if dt > 0.0 {
            let cores = f64::from(self.w.host.cores);
            let rates = self.rates();
            let runnable = f64::from(self.runnable());
            let busy = runnable.min(cores);
            let idle = cores - busy;
            if self.waiting_work() {
                self.log.idle_queued += idle * dt;
            }
            if self.fitting {
                self.log.idle_fitting += idle * dt;
            }
            self.log.excess += (runnable - cores).max(0.0) * dt;
            self.log.busy += busy * dt;
            let leaves: u32 = self
                .running
                .iter()
                .map(|&t| &self.tasks[t])
                .filter(|run| !run.per_process)
                .map(|run| run.held.cpus.saturating_sub(run.inflight))
                .sum();
            let waking: u32 = self
                .waking
                .iter()
                .map(|(_, id)| self.units[id].held.cpus)
                .sum();
            self.log.held_idle += f64::from(leaves + waking) * dt;
            let over = self.memory_in_use().saturating_sub(self.w.host.memory);
            self.log.memory_over += over as f64 * dt;
            self.cores.advance(rates, dt);
        }
        self.now = next;
        self.log.events += 1;
    }

    fn complete(&mut self) {
        while let Some(&(until, id)) = self.waking.first()
            && f64::from_bits(until) <= self.now + DONE
        {
            self.waking.pop_first();
            self.run_unit(id);
        }
        for id in self.cores.ended() {
            self.finish_unit(id);
        }
    }

    fn finish_unit(&mut self, id: u64) {
        let Some(unit) = self.drop_unit(id) else {
            return;
        };
        let task = &self.w.tasks[unit.task.0];
        let count = task.units.len() + usize::from(task.setup > 0.0);
        let run = &mut self.tasks[unit.task.0];
        run.inflight -= 1;
        run.finished += 1;
        if run.finished == count {
            self.finish_task(unit.task);
        }
    }

    fn finish_task(&mut self, t: TaskIx) {
        let run = &mut self.tasks[t.0];
        let held = std::mem::take(&mut run.held);
        run.stage = Stage::Done;
        run.out.end = Some(self.now);
        self.held = self.held.minus(held);
        self.running.remove(&t.0);
        let w = self.w;
        let j = w.tasks[t.0].job.0;
        self.jobs[j].slotted -= 1;
        self.jobs[j].left -= 1;
        for &s in w.successors(t) {
            let run = &mut self.tasks[s.0];
            run.missing -= 1;
            if run.missing == 0 && run.stage == Stage::Blocked {
                self.make_ready(s);
            }
        }
        if self.jobs[j].left == 0 {
            self.jobs[j].stage = JobStage::Done;
            self.jobs[j].out.end = Some(self.now);
            self.step(Event::Concluded { job: job_id(j) });
        }
    }

    /// Reports the memory pressure the kernel would, from the memory the units hold, each time
    /// its level changes: Warning from 90% of the budget, Critical at the budget.
    fn pressure(&mut self) {
        let used = self.memory_in_use();
        let budget = self.w.host.memory;
        let level = if used >= budget {
            DarwinPressureLevel::Critical
        } else if used >= budget / 10 * 9 {
            DarwinPressureLevel::Warning
        } else {
            DarwinPressureLevel::Normal
        };
        if self.level == Some(level) {
            return;
        }
        self.level = Some(level);
        self.step(Event::Pressure(Reading::Known(MemoryPressure::Darwin {
            level,
        })));
    }

    fn finish(self) -> Outcome {
        let mut log = self.log;
        log.end = self.now;
        let unfinished = self
            .tasks
            .iter()
            .filter(|run| run.stage != Stage::Done)
            .count();
        log.unfinished = u32::try_from(unfinished).unwrap_or(u32::MAX);
        Outcome {
            jobs: self.jobs.iter().map(|job| job.out).collect(),
            tasks: self.tasks.iter().map(|run| run.out).collect(),
            log,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Host, Job, Task};
    use crate::policy::{LIST_PACKING, TODAY, TOKENS};

    #[test]
    fn idle_cores_run_every_thread_at_full_speed_and_contention_splits_by_class_weight() {
        assert_eq!(share(18, [0, 9, 9]), [1.0, 1.0, 1.0]);
        let [_, gate, background] = share(18, [0, 18, 18]);
        // 18 gate and 18 background threads on 18 cores: 335:110 per thread.
        assert!((gate * 18.0 + background * 18.0 - 18.0).abs() < 1e-9);
        assert!((gate / background - 335.0 / 110.0).abs() < 1e-9);
        // A class whose share would pass a whole core runs at full speed; the rest share what is
        // left.
        let [_, gate, background] = share(18, [0, 2, 40]);
        assert_eq!(gate, 1.0);
        assert!((background * 40.0 - 16.0).abs() < 1e-9);
    }

    fn task(name: &str, runner: Runner, units: Vec<Secs>, reserve: Option<u32>) -> Task {
        Task {
            name: name.to_owned(),
            job: JobIx(0),
            deps: Vec::new(),
            runner,
            setup: 0.0,
            units,
            unit_memory: 1 << 20,
            reserve,
        }
    }

    /// A 17-cpu leaf behind ten one-cpu leaves, and five more one-cpu leaves behind it.
    fn head_of_line() -> Workload {
        let mut tasks: Vec<Task> = (0..10)
            .map(|i| task(&format!("busy{i}"), Runner::Process, vec![100.0], Some(1)))
            .collect();
        tasks.push(task("wide", Runner::Workers, vec![10.0; 17], Some(17)));
        tasks.extend(
            (0..5).map(|i| task(&format!("small{i}"), Runner::Process, vec![5.0], Some(1))),
        );
        let job = Job {
            class: Class::Gate,
            arrival: 0.0,
            parallel: 18,
            tasks: (0..tasks.len()).map(TaskIx).collect(),
        };
        Workload::new(
            Host {
                cores: 18,
                memory: 64 << 30,
            },
            vec![job],
            tasks,
        )
        .expect("a valid workload")
    }

    #[test]
    fn mbx_holds_small_leaves_behind_a_wide_head_while_cores_idle() {
        let outcome = run(&head_of_line(), &TODAY);
        // The five small leaves wait the busy ones' 100 s and the wide one's 10 s with 8 cores
        // free, then start.
        assert!(
            outcome.log.longest_fitting_wait >= 100.0,
            "{:?}",
            outcome.log
        );
        assert!(
            outcome.log.idle_fitting >= 8.0 * 100.0 - 1e-6,
            "{:?}",
            outcome.log
        );
        assert_eq!(outcome.log.unfinished, 0);
    }

    #[test]
    fn list_scheduling_backfills_the_small_leaves_and_tokens_never_hold_a_fitting_waiter() {
        for policy in [LIST_PACKING, TOKENS] {
            let outcome = run(&head_of_line(), &policy);
            assert_eq!(
                outcome.log.idle_fitting, 0.0,
                "{}: {:?}",
                policy.name, outcome.log
            );
            assert_eq!(outcome.log.unfinished, 0, "{}", policy.name);
        }
    }
}
