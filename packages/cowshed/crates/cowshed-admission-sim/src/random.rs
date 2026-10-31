//! Seeded random workloads: a gate's layered task graph with its runner mix, and agent jobs
//! arriving beside it. A seed names one workload on every host.

use crate::model::{Class, Host, Job, JobIx, Runner, Secs, Task, TaskIx, Workload};

/// SplitMix64: small, seeded and portable.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    pub(crate) fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1_u64 << 53) as f64
    }

    pub(crate) fn range(&mut self, low: f64, high: f64) -> f64 {
        low + (high - low) * self.unit()
    }

    /// Log-uniform: durations spread over orders of magnitude.
    pub(crate) fn log_range(&mut self, low: f64, high: f64) -> f64 {
        (low.ln() + (high.ln() - low.ln()) * self.unit()).exp()
    }

    pub(crate) fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n.max(1)).unwrap_or(u64::MAX)).unwrap_or(0)
    }

    pub(crate) fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }
}

pub(crate) const GB: u64 = 1 << 30;

/// The shape of a random workload.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Shape {
    pub(crate) gate_tasks: usize,
    pub(crate) agents: usize,
    /// The most tasks an agent job holds.
    pub(crate) agent_tasks: usize,
    /// Agent jobs arrive spread over this many seconds.
    pub(crate) horizon: Secs,
    /// Scales every unit's memory.
    pub(crate) memory_scale: f64,
}

/// A gate and its agents, as `seed` makes them.
pub(crate) fn workload(seed: u64, shape: Shape, host: Host) -> Workload {
    let mut rng = Rng::new(seed);
    let mut jobs = Vec::new();
    let mut tasks = Vec::new();
    let mut graph = Graph {
        rng: &mut rng,
        jobs: &mut jobs,
        tasks: &mut tasks,
        host,
        memory_scale: shape.memory_scale,
    };
    graph.job("gate", Class::Gate, 0.0, shape.gate_tasks);
    for agent in 0..shape.agents {
        let arrival = graph.rng.range(0.0, shape.horizon);
        let size = 1 + graph.rng.below(shape.agent_tasks);
        graph.job(&format!("agent{agent}"), Class::Background, arrival, size);
    }
    Workload::new(host, jobs, tasks).expect("a generated workload is valid by construction")
}

/// Agent jobs alone, appended to an existing workload's jobs and tasks.
pub(crate) fn agents(
    seed: u64,
    shape: Shape,
    host: Host,
    jobs: &mut Vec<Job>,
    tasks: &mut Vec<Task>,
) {
    let mut rng = Rng::new(seed);
    let mut graph = Graph {
        rng: &mut rng,
        jobs,
        tasks,
        host,
        memory_scale: shape.memory_scale,
    };
    for agent in 0..shape.agents {
        let arrival = graph.rng.range(0.0, shape.horizon);
        let size = 1 + graph.rng.below(shape.agent_tasks);
        graph.job(&format!("agent{agent}"), Class::Background, arrival, size);
    }
}

struct Graph<'a> {
    rng: &'a mut Rng,
    jobs: &'a mut Vec<Job>,
    tasks: &'a mut Vec<Task>,
    host: Host,
    memory_scale: f64,
}

impl Graph<'_> {
    /// A layered graph of `size` tasks, about the square root of its size deep, each task on up
    /// to three tasks of earlier layers.
    fn job(&mut self, name: &str, class: Class, arrival: Secs, size: usize) {
        let job = JobIx(self.jobs.len());
        let first = self.tasks.len();
        let layers = (size as f64).sqrt().ceil().max(1.0) as usize;
        for i in 0..size {
            let layer = i * layers / size;
            let earlier = (layer * size).div_ceil(layers);
            let mut deps = Vec::new();
            if earlier > 0 {
                for _ in 0..self.rng.below(4) {
                    let dep = TaskIx(first + self.rng.below(earlier));
                    if !deps.contains(&dep) {
                        deps.push(dep);
                    }
                }
            }
            let task = self.task(format!("{name}:t{i}"), job, deps);
            self.tasks.push(task);
        }
        self.jobs.push(Job {
            class,
            arrival,
            parallel: self.host.cores,
            tasks: (first..first + size).map(TaskIx).collect(),
        });
    }

    /// One task of a gate's mix, its reservation as bounded-exec sizes it at HEAD under
    /// `NX_PARALLEL=100%`: one cpu for a process and a prebuilt-nextest run, one per core (less
    /// one for Vitest) for a worker runner, none for Cargo.
    fn task(&mut self, name: String, job: JobIx, deps: Vec<TaskIx>) -> Task {
        let rng = &mut *self.rng;
        let scale = self.memory_scale;
        let gb = |gb: f64| (gb * scale * GB as f64) as u64;
        let cores = self.host.cores;
        let roll = rng.unit();
        let (runner, setup, units, unit_memory, reserve) = if roll < 0.15 {
            (
                Runner::Restore,
                0.0,
                vec![rng.log_range(0.01, 0.3)],
                gb(0.05),
                None,
            )
        } else if roll < 0.55 {
            let memory = gb(rng.range(0.3, 1.0));
            (
                Runner::Process,
                0.0,
                vec![rng.log_range(0.5, 120.0)],
                memory,
                Some(1),
            )
        } else if roll < 0.75 {
            let tests = 5 + rng.below(80);
            let units = (0..tests)
                .map(|_| {
                    if rng.chance(0.03) {
                        rng.log_range(2.0, 40.0)
                    } else {
                        rng.log_range(0.002, 1.5)
                    }
                })
                .collect();
            let setup = rng.range(0.3, 3.0);
            (
                Runner::Nextest,
                setup,
                units,
                gb(rng.range(0.05, 0.3)),
                Some(1),
            )
        } else if roll < 0.85 {
            let files = 10 + rng.below(80);
            let units = (0..files).map(|_| rng.log_range(0.2, 20.0)).collect();
            let setup = rng.range(0.5, 2.0);
            let workers = cores - u32::from(rng.chance(0.5));
            (
                Runner::Workers,
                setup,
                units,
                gb(rng.range(0.3, 0.8)),
                Some(workers),
            )
        } else {
            let crates = 8 + rng.below(60);
            let units = (0..crates).map(|_| rng.log_range(0.5, 40.0)).collect();
            (Runner::Cargo, 0.0, units, gb(rng.range(0.4, 2.0)), None)
        };
        Task {
            name,
            job,
            deps,
            runner,
            setup,
            units,
            unit_memory,
            reserve,
        }
    }
}
