//! The workload the simulator replays: jobs of Nx tasks, each task a list of one-thread units.

use cowshed_core::api::dto::ExecClass;

/// Virtual seconds.
pub(crate) type Secs = f64;

/// A task's index in [`Workload::tasks`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TaskIx(pub(crate) usize);

/// A job's index in [`Workload::jobs`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct JobIx(pub(crate) usize);

/// A job's class (05_gateway.md "Classes"). The derived order is the priority order.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum Class {
    Interactive,
    Gate,
    Background,
}

impl Class {
    pub(crate) const ALL: [Self; 3] = [Self::Interactive, Self::Gate, Self::Background];

    pub(crate) const fn exec(self) -> ExecClass {
        match self {
            Self::Interactive => ExecClass::Interactive,
            Self::Gate => ExecClass::Gate,
            Self::Background => ExecClass::Background,
        }
    }

    /// The CFS weight of the class's nice (the kernel's `sched_prio_to_weight`: nice 0 is 1024, +5
    /// is 335, +10 is 110), so contended cores split about 9:3:1 (05_gateway.md "Classes").
    pub(crate) const fn weight(self) -> f64 {
        match self {
            Self::Interactive => 1024.0,
            Self::Gate => 335.0,
            Self::Background => 110.0,
        }
    }

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Interactive => 0,
            Self::Gate => 1,
            Self::Background => 2,
        }
    }
}

/// What a task's command is, which decides how each stack admits it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Runner {
    /// Nx restored the task from its cache and ran nothing.
    Restore,
    /// A Cargo build: mbx admits each rustc, one unit each.
    Cargo,
    /// A nextest run of prebuilt test binaries: one unit per test process.
    Nextest,
    /// A runner that starts its own workers (`bun test --parallel`, Vitest, Stryker): one unit per
    /// worker, and no per-process admission can reach them.
    Workers,
    /// A command that runs one process: `bun test`, lint, typecheck, a script.
    Process,
}

/// One Nx task.
#[derive(Clone, Debug)]
pub(crate) struct Task {
    pub(crate) name: String,
    pub(crate) job: JobIx,
    pub(crate) deps: Vec<TaskIx>,
    pub(crate) runner: Runner,
    /// One-thread work the command does before it fans out (archive extraction, test listing).
    pub(crate) setup: Secs,
    /// The one-thread run time of each process or thread the command can run side by side, in
    /// the order it starts them.
    pub(crate) units: Vec<Secs>,
    /// Resident bytes per running unit.
    pub(crate) unit_memory: u64,
    /// The cpus today's leaf asks `mbx reserve` for, when it reserves.
    pub(crate) reserve: Option<u32>,
}

impl Task {
    /// The most units it runs side by side.
    pub(crate) fn width(&self) -> u32 {
        u32::try_from(self.units.len()).unwrap_or(u32::MAX)
    }

    /// Its whole one-thread work.
    pub(crate) fn work(&self) -> Secs {
        self.setup + self.units.iter().sum::<Secs>()
    }
}

/// One cowshed job running one Nx run.
#[derive(Clone, Debug)]
pub(crate) struct Job {
    pub(crate) class: Class,
    pub(crate) arrival: Secs,
    /// Nx's `--parallel`: how many of its tasks run at once.
    pub(crate) parallel: u32,
    /// Its tasks in the order Nx prefers to start them.
    pub(crate) tasks: Vec<TaskIx>,
}

/// The host: its cores and the memory budget admission may hand out.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Host {
    pub(crate) cores: u32,
    pub(crate) memory: u64,
}

/// A checked workload: every dependency inside its job, no cycle, every task listed once.
#[derive(Debug)]
pub(crate) struct Workload {
    pub(crate) host: Host,
    pub(crate) jobs: Vec<Job>,
    pub(crate) tasks: Vec<Task>,
    successors: Vec<Vec<TaskIx>>,
    bottom: Vec<Secs>,
}

/// Why a workload is not one.
#[derive(Debug, PartialEq)]
pub(crate) enum WorkloadError {
    NoUnits { task: String },
    Duration { task: String, value: Secs },
    UnknownJob { task: String, job: usize },
    UnknownDep { task: String, dep: usize },
    CrossJob { task: String, dep: String },
    RepeatedDep { task: String, dep: String },
    Listing { task: String },
    Cycle { task: String },
}

/// The bounds no schedule of a job can beat: its longest dependency chain at full speed, and its
/// work spread over every core.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Bound {
    pub(crate) critical_path: Secs,
    pub(crate) area: Secs,
}

impl Bound {
    pub(crate) fn lower(self) -> Secs {
        self.critical_path.max(self.area)
    }
}

impl Workload {
    pub(crate) fn new(host: Host, jobs: Vec<Job>, tasks: Vec<Task>) -> Result<Self, WorkloadError> {
        for task in &tasks {
            let name = || task.name.clone();
            if task.units.is_empty() {
                return Err(WorkloadError::NoUnits { task: name() });
            }
            if let Some(&value) = task
                .units
                .iter()
                .chain([&task.setup])
                .find(|value| !(value.is_finite() && **value >= 0.0))
            {
                return Err(WorkloadError::Duration {
                    task: name(),
                    value,
                });
            }
            if task.job.0 >= jobs.len() {
                return Err(WorkloadError::UnknownJob {
                    task: name(),
                    job: task.job.0,
                });
            }
            for (i, dep) in task.deps.iter().enumerate() {
                let Some(on) = tasks.get(dep.0) else {
                    return Err(WorkloadError::UnknownDep {
                        task: name(),
                        dep: dep.0,
                    });
                };
                if on.job != task.job {
                    return Err(WorkloadError::CrossJob {
                        task: name(),
                        dep: on.name.clone(),
                    });
                }
                if task.deps[..i].contains(dep) {
                    return Err(WorkloadError::RepeatedDep {
                        task: name(),
                        dep: on.name.clone(),
                    });
                }
            }
        }
        let mut listed = vec![false; tasks.len()];
        for (j, job) in jobs.iter().enumerate() {
            for t in &job.tasks {
                match (tasks.get(t.0), listed.get(t.0)) {
                    (Some(task), Some(false)) if task.job.0 == j => listed[t.0] = true,
                    (Some(task), _) => {
                        return Err(WorkloadError::Listing {
                            task: task.name.clone(),
                        });
                    }
                    (None, _) => {
                        return Err(WorkloadError::Listing {
                            task: format!("#{}", t.0),
                        });
                    }
                }
            }
        }
        if let Some(t) = listed.iter().position(|listed| !listed) {
            return Err(WorkloadError::Listing {
                task: tasks[t].name.clone(),
            });
        }
        let mut successors = vec![Vec::new(); tasks.len()];
        for (i, task) in tasks.iter().enumerate() {
            for dep in &task.deps {
                successors[dep.0].push(TaskIx(i));
            }
        }
        let mut missing: Vec<usize> = tasks.iter().map(|task| task.deps.len()).collect();
        let mut stack: Vec<usize> = (0..tasks.len()).filter(|&i| missing[i] == 0).collect();
        let mut order = Vec::with_capacity(tasks.len());
        while let Some(i) = stack.pop() {
            order.push(i);
            for s in &successors[i] {
                missing[s.0] -= 1;
                if missing[s.0] == 0 {
                    stack.push(s.0);
                }
            }
        }
        if let Some(t) = missing.iter().position(|&m| m > 0) {
            return Err(WorkloadError::Cycle {
                task: tasks[t].name.clone(),
            });
        }
        let mut bottom = vec![0.0; tasks.len()];
        for &i in order.iter().rev() {
            let task = &tasks[i];
            let best = task.setup + list_span(&task.units, task.width().min(host.cores));
            bottom[i] = best
                + successors[i]
                    .iter()
                    .map(|s| bottom[s.0])
                    .fold(0.0, f64::max);
        }
        Ok(Self {
            host,
            jobs,
            tasks,
            successors,
            bottom,
        })
    }

    pub(crate) fn successors(&self, task: TaskIx) -> &[TaskIx] {
        &self.successors[task.0]
    }

    /// The task's bottom level: its own best time plus the longest chain of work behind it.
    pub(crate) fn bottom(&self, task: TaskIx) -> Secs {
        self.bottom[task.0]
    }

    pub(crate) fn bound(&self, job: JobIx) -> Bound {
        let tasks = &self.jobs[job.0].tasks;
        Bound {
            critical_path: tasks.iter().map(|&t| self.bottom(t)).fold(0.0, f64::max),
            area: tasks.iter().map(|t| self.tasks[t.0].work()).sum::<Secs>()
                / f64::from(self.host.cores),
        }
    }
}

/// The span of `units` list-scheduled in order onto `lanes` threads at full speed, each unit to
/// the lane that frees first.
pub(crate) fn list_span(units: &[Secs], lanes: u32) -> Secs {
    let lanes = usize::try_from(lanes.max(1))
        .unwrap_or(usize::MAX)
        .min(units.len().max(1));
    let mut free = vec![0.0_f64; lanes];
    for &unit in units {
        let lane = free
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.total_cmp(b.1))
            .map_or(0, |(lane, _)| lane);
        free[lane] += unit;
    }
    free.into_iter().fold(0.0, f64::max)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(name: &str, deps: &[usize], units: &[Secs]) -> Task {
        Task {
            name: name.to_owned(),
            job: JobIx(0),
            deps: deps.iter().map(|&d| TaskIx(d)).collect(),
            runner: Runner::Process,
            setup: 0.0,
            units: units.to_vec(),
            unit_memory: 0,
            reserve: None,
        }
    }

    fn job(tasks: usize) -> Job {
        Job {
            class: Class::Gate,
            arrival: 0.0,
            parallel: 4,
            tasks: (0..tasks).map(TaskIx).collect(),
        }
    }

    const HOST: Host = Host {
        cores: 4,
        memory: 1 << 30,
    };

    #[test]
    fn the_bound_is_the_longest_chain_or_the_work_over_the_cores() {
        let workload = Workload::new(
            HOST,
            vec![job(3)],
            vec![
                task("a", &[], &[2.0]),
                task("b", &[0], &[1.0, 1.0, 1.0, 1.0, 1.0]),
                task("c", &[], &[1.0]),
            ],
        )
        .expect("a valid workload");
        // b's five units on four cores take two seconds after a's two.
        assert_eq!(workload.bottom(TaskIx(0)), 4.0);
        assert_eq!(
            workload.bound(JobIx(0)),
            Bound {
                critical_path: 4.0,
                area: 2.0
            }
        );
    }

    #[test]
    fn a_cycle_a_cross_job_dependency_and_an_unlisted_task_are_refused() {
        let cycle = Workload::new(
            HOST,
            vec![job(2)],
            vec![task("a", &[1], &[1.0]), task("b", &[0], &[1.0])],
        );
        assert!(matches!(cycle, Err(WorkloadError::Cycle { .. })));
        let mut other = task("b", &[0], &[1.0]);
        other.job = JobIx(1);
        let mut second = job(0);
        second.tasks = vec![TaskIx(1)];
        let cross = Workload::new(
            HOST,
            vec![job(1), second],
            vec![task("a", &[], &[1.0]), other],
        );
        assert!(matches!(cross, Err(WorkloadError::CrossJob { .. })));
        let unlisted = Workload::new(
            HOST,
            vec![job(1)],
            vec![task("a", &[], &[1.0]), task("b", &[], &[1.0])],
        );
        assert_eq!(
            unlisted.err(),
            Some(WorkloadError::Listing {
                task: "b".to_owned()
            })
        );
    }

    #[test]
    fn units_fill_the_lane_that_frees_first() {
        assert_eq!(list_span(&[3.0, 1.0, 1.0, 1.0], 2), 3.0);
        assert_eq!(list_span(&[1.0; 6], 4), 2.0);
        assert_eq!(list_span(&[5.0], 18), 5.0);
    }
}
