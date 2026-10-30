//! A recorded gate run, read from the committed fixture: the run's Nx tasks with their recorded
//! start and end, their dependencies, the `mbx reserve` each leaf made and when the pool admitted
//! it, and each prebuilt-nextest leaf's per-test times. The fixture was extracted from the Nx task
//! database, the tasks' terminal outputs and mbx's decision log, then had its project and target
//! names replaced by indices.
//!
//! Turning facts into units states its assumptions here, once:
//! - A task's own run time is its end less its admission (its start when it reserved nothing).
//! - A prebuilt-nextest leaf's units are its tests; the rest of its run, beyond its tests
//!   list-scheduled on the threads it reserved, is setup before them.
//! - A leaf that reserved more than one cpu starts its own workers: as many units as it reserved,
//!   each its whole run time, the widest it could have been.
//! - A Cargo build kept four rustc busy for its run time. [INFERENCE: the decision log holds
//!   admissions, not durations, so a build's own parallelism is not recorded.]
//! - Resident memory per running unit by runner, from the host census of the run (23.2 GB over 62
//!   processes in the gate's tree) [INFERENCE]: see [`unit_memory`].

use serde::Deserialize;

use crate::model::{Class, Host, Job, JobIx, Runner, Secs, Task, TaskIx, Workload, list_span};

const GATE_2026_10_30: &str = include_str!("../fixtures/gate-2026-10-30-2022.json");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    about: String,
    host: HostFacts,
    run_ms: u64,
    tasks: Vec<TaskFacts>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostFacts {
    cores: u32,
    memory_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskFacts {
    id: String,
    runner: RunnerFact,
    start_ms: u64,
    end_ms: u64,
    deps: Vec<usize>,
    reserve: Option<ReserveFacts>,
    tests: Option<Vec<Secs>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
enum RunnerFact {
    NextestPrebuilt,
    Cargo,
    BunTest,
    Lint,
    Tsc,
    Other,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReserveFacts {
    cpus: u32,
    enqueued_ms: u64,
    admitted_ms: u64,
    queue_position: Option<u32>,
}

/// What the run recorded of one task.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Fact {
    /// Its `mbx reserve`: the cpus, how long the pool held it and its deepest queue position.
    pub(crate) reserve: Option<(u32, Secs, Option<u32>)>,
}

/// A recorded run: its pieces, so agent jobs can join it, and its facts.
#[derive(Debug)]
pub(crate) struct Recorded {
    pub(crate) about: String,
    pub(crate) host: Host,
    pub(crate) jobs: Vec<Job>,
    pub(crate) tasks: Vec<Task>,
    pub(crate) facts: Vec<Fact>,
    /// The recorded makespan.
    pub(crate) makespan: Secs,
}

#[derive(Debug)]
pub(crate) enum FixtureError {
    Json(serde_json::Error),
    Dep { task: String, dep: usize },
    Times { task: String },
}

impl std::fmt::Display for FixtureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(error) => write!(f, "the fixture is no fixture: {error}"),
            Self::Dep { task, dep } => write!(f, "{task} depends on task {dep}, outside the run"),
            Self::Times { task } => write!(f, "{task} ends before it starts or is admitted"),
        }
    }
}

impl Recorded {
    pub(crate) fn workload(&self) -> Workload {
        Workload::new(self.host, self.jobs.clone(), self.tasks.clone())
            .expect("a recorded run's dependencies were checked when it was read")
    }
}

/// The land gate recorded on 2026-10-30 from 20:22:19Z.
pub(crate) fn gate_2026_10_30() -> Recorded {
    read(GATE_2026_10_30).expect("the committed fixture reads")
}

fn seconds(ms: u64) -> Secs {
    ms as Secs / 1000.0
}

/// Resident bytes per running unit of each runner [INFERENCE: the census's 375 MB mean per
/// process, split by what each runner is].
fn unit_memory(runner: RunnerFact, workers: bool) -> u64 {
    const MB: u64 = 1 << 20;
    match runner {
        _ if workers => 400 * MB,
        RunnerFact::NextestPrebuilt => 150 * MB,
        RunnerFact::Cargo => 1024 * MB,
        RunnerFact::BunTest => 600 * MB,
        RunnerFact::Lint => 800 * MB,
        RunnerFact::Tsc => 1200 * MB,
        RunnerFact::Other => 300 * MB,
    }
}

fn read(text: &str) -> Result<Recorded, FixtureError> {
    let fixture: Fixture = serde_json::from_str(text).map_err(FixtureError::Json)?;
    let host = Host {
        cores: fixture.host.cores,
        memory: fixture.host.memory_bytes,
    };
    let count = fixture.tasks.len();
    let mut tasks = Vec::with_capacity(count);
    let mut facts = Vec::with_capacity(count);
    for facts_of in &fixture.tasks {
        let begun = facts_of
            .reserve
            .map_or(facts_of.start_ms, |reserve| reserve.admitted_ms);
        if facts_of.end_ms < facts_of.start_ms || begun > facts_of.end_ms {
            return Err(FixtureError::Times {
                task: facts_of.id.clone(),
            });
        }
        if let Some(&dep) = facts_of.deps.iter().find(|&&dep| dep >= count) {
            return Err(FixtureError::Dep {
                task: facts_of.id.clone(),
                dep,
            });
        }
        let run = seconds(facts_of.end_ms - begun).max(0.001);
        let cpus = facts_of.reserve.map(|reserve| reserve.cpus);
        let workers =
            facts_of.runner != RunnerFact::NextestPrebuilt && cpus.is_some_and(|cpus| cpus > 1);
        let (runner, setup, units) = match facts_of.runner {
            RunnerFact::NextestPrebuilt => match facts_of.tests.as_deref() {
                Some(tests) if !tests.is_empty() => {
                    let span = list_span(tests, cpus.unwrap_or(1));
                    (Runner::Nextest, (run - span).max(0.0), tests.to_vec())
                }
                _ => (Runner::Nextest, 0.0, vec![run]),
            },
            RunnerFact::Cargo => (Runner::Cargo, 0.0, vec![run; 4]),
            _ if workers => {
                let width = usize::try_from(cpus.unwrap_or(1)).unwrap_or(1);
                (Runner::Workers, 0.0, vec![run; width])
            }
            _ => (Runner::Process, 0.0, vec![run]),
        };
        tasks.push(Task {
            name: facts_of.id.clone(),
            job: JobIx(0),
            deps: facts_of.deps.iter().map(|&dep| TaskIx(dep)).collect(),
            runner,
            setup,
            units,
            unit_memory: unit_memory(facts_of.runner, workers),
            reserve: cpus,
        });
        facts.push(Fact {
            reserve: facts_of.reserve.map(|reserve| {
                (
                    reserve.cpus,
                    seconds(reserve.admitted_ms - reserve.enqueued_ms),
                    reserve.queue_position,
                )
            }),
        });
    }
    let mut order: Vec<usize> = (0..count).collect();
    order.sort_by_key(|&i| (fixture.tasks[i].start_ms, i));
    let jobs = vec![Job {
        class: Class::Gate,
        arrival: 0.0,
        parallel: host.cores,
        tasks: order.into_iter().map(TaskIx).collect(),
    }];
    Ok(Recorded {
        about: fixture.about,
        host,
        jobs,
        tasks,
        facts,
        makespan: seconds(fixture.run_ms),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_recorded_gate_reads_with_every_leaf_reservation_and_its_wait() {
        let recorded = gate_2026_10_30();
        let workload = recorded.workload();
        assert_eq!(workload.tasks.len(), 844);
        let reserved: Vec<u32> = recorded
            .facts
            .iter()
            .filter_map(|fact| fact.reserve.map(|r| r.0))
            .collect();
        assert_eq!(reserved.len(), 233);
        assert_eq!(reserved.iter().filter(|&&cpus| cpus >= 17).count(), 8);
        let longest = recorded
            .facts
            .iter()
            .filter_map(|fact| fact.reserve.map(|r| r.1))
            .fold(0.0, f64::max);
        assert!((longest - 176.479).abs() < 1e-6, "{longest}");
        assert!(recorded.makespan > 1237.0 && recorded.makespan < 1238.0);
    }

    #[test]
    fn an_unknown_field_or_a_dependency_outside_the_run_is_refused() {
        let unknown =
            r#"{"about":"","host":{"cores":1,"memory_bytes":1},"run_ms":1,"tasks":[],"extra":1}"#;
        assert!(matches!(read(unknown), Err(FixtureError::Json(_))));
        let outside = r#"{"about":"","host":{"cores":1,"memory_bytes":1},"run_ms":1,"tasks":[
            {"id":"a","runner":"other","start_ms":0,"end_ms":1,"deps":[3],"reserve":null,"tests":null}]}"#;
        assert!(matches!(
            read(outside),
            Err(FixtureError::Dep { dep: 3, .. })
        ));
    }
}
