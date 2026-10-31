//! The scored comparison: every candidate over the recorded gate alone, the recorded gate beside
//! agent jobs, and seeded random workloads, by the brief's measures.

use std::fmt::Write as _;

use crate::engine::{Outcome, run};
use crate::fixture::{Recorded, gate_2026_10_30};
use crate::model::{Class, JobIx, Secs, Workload};
use crate::policy::{CANDIDATES, Policy};
use crate::random::{self, GB, Shape};

/// The agent load beside a gate: eight background Nx runs of up to 40 tasks over the gate's
/// first fifteen minutes.
pub(crate) const AGENTS: Shape = Shape {
    gate_tasks: 0,
    agents: 8,
    agent_tasks: 40,
    horizon: 900.0,
    memory_scale: 1.0,
};

/// The random set: a 150-task gate with six agent runs over its first five minutes.
pub(crate) const RANDOM: Shape = Shape {
    gate_tasks: 150,
    agents: 6,
    agent_tasks: 40,
    horizon: 300.0,
    memory_scale: 1.0,
};

/// The memory-heavy random set: every unit four times the memory.
pub(crate) const HEAVY: Shape = Shape {
    memory_scale: 4.0,
    ..RANDOM
};

pub(crate) const RANDOM_SEEDS: u64 = 6;

/// The recorded gate with agent jobs beside it.
pub(crate) fn with_agents(recorded: &Recorded, seed: u64) -> Workload {
    let mut jobs = recorded.jobs.clone();
    let mut tasks = recorded.tasks.clone();
    random::agents(seed, AGENTS, recorded.host, &mut jobs, &mut tasks);
    Workload::new(recorded.host, jobs, tasks)
        .expect("agent jobs join a recorded run by construction")
}

/// The gate job's latency over its lower bound.
pub(crate) fn stretch(workload: &Workload, outcome: &Outcome) -> Secs {
    let latency = outcome.jobs[0].latency().unwrap_or(Secs::INFINITY);
    latency / workload.bound(JobIx(0)).lower()
}

/// The longest any background request waited in the pool, and the slowest agent job.
fn background(workload: &Workload, outcome: &Outcome) -> (Secs, Secs) {
    let slowest = workload
        .jobs
        .iter()
        .zip(&outcome.jobs)
        .filter(|(job, _)| job.class == Class::Background)
        .map(|(_, out)| out.latency().unwrap_or(Secs::INFINITY))
        .fold(0.0, f64::max);
    (outcome.log.longest_wait[Class::Background.index()], slowest)
}

/// One candidate's scores.
#[derive(Clone, Debug)]
pub(crate) struct Row {
    pub(crate) policy: &'static str,
    pub(crate) alone: Outcome,
    pub(crate) stretch: Secs,
    pub(crate) gate_slowdown: Secs,
    pub(crate) background_wait: Secs,
    pub(crate) slowest_agent: Secs,
    pub(crate) loaded_peak_threads: u32,
    pub(crate) loaded_peak_memory: u64,
    pub(crate) random_stretch: (Secs, Secs),
    pub(crate) random_idle_fitting: Secs,
    pub(crate) random_gate_behind: u32,
    pub(crate) heavy_memory_over: f64,
    pub(crate) heavy_ended: u32,
    pub(crate) unfinished: u32,
}

pub(crate) fn score(policy: &Policy, recorded: &Recorded) -> Row {
    let workload = recorded.workload();
    let alone = run(&workload, policy);
    let alone_stretch = stretch(&workload, &alone);
    let gate_alone = alone.jobs[0].latency().unwrap_or(Secs::INFINITY);
    let loaded_workload = with_agents(recorded, 1);
    let loaded = run(&loaded_workload, policy);
    let (background_wait, slowest_agent) = background(&loaded_workload, &loaded);
    let mut random_stretch = (0.0, 0.0_f64);
    let mut random_idle_fitting = 0.0;
    let mut random_gate_behind = 0;
    let mut heavy_memory_over = 0.0;
    let mut heavy_ended = 0;
    let mut unfinished = alone.log.unfinished + loaded.log.unfinished;
    for seed in 0..RANDOM_SEEDS {
        let workload = random::workload(seed, RANDOM, recorded.host);
        let outcome = run(&workload, policy);
        let s = stretch(&workload, &outcome);
        random_stretch = (
            random_stretch.0 + s / RANDOM_SEEDS as Secs,
            random_stretch.1.max(s),
        );
        random_idle_fitting += outcome.log.idle_fitting;
        random_gate_behind += outcome.log.gate_behind;
        unfinished += outcome.log.unfinished;
        let heavy = run(&random::workload(seed, HEAVY, recorded.host), policy);
        heavy_memory_over += heavy.log.memory_over;
        heavy_ended += heavy.log.ended_for_memory;
    }
    Row {
        policy: policy.name,
        stretch: alone_stretch,
        gate_slowdown: loaded.jobs[0].latency().unwrap_or(Secs::INFINITY) / gate_alone,
        background_wait,
        slowest_agent,
        loaded_peak_threads: loaded.log.peak_runnable,
        loaded_peak_memory: loaded.log.peak_memory,
        alone,
        random_stretch,
        random_idle_fitting,
        random_gate_behind,
        heavy_memory_over,
        heavy_ended,
        unfinished,
    }
}

/// The table, as Markdown.
pub(crate) fn table(rows: &[Row], recorded: &Recorded) -> String {
    let workload = recorded.workload();
    let bound = workload.bound(JobIx(0));
    let cores = f64::from(recorded.host.cores);
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{}\n\nRecorded gate: {:.0} s; critical path {:.0} s, work over cores {:.0} s.\n",
        recorded.about, recorded.makespan, bound.critical_path, bound.area
    );
    let _ = writeln!(
        out,
        "| policy | gate makespan s | / bound | idle while queued core-s | idle while a fitting request waited core-s | longest fitting wait s | granted but idle core-s | peak threads / cores (agents) | peak memory GB (agents) | gate slowdown with agents | longest background wait s | slowest agent job s | random mean / worst stretch | random idle fitting core-s | gate passed over | heavy memory over budget GB-s | jobs ended for memory | unfinished |"
    );
    let _ = writeln!(out, "|{}", "---|".repeat(18));
    for row in rows {
        let log = &row.alone.log;
        let _ = writeln!(
            out,
            "| {} | {:.0} | {:.2} | {:.0} | {:.0} | {:.0} | {:.0} | {:.2} | {:.1} | {:.2} | {:.0} | {:.0} | {:.2} / {:.2} | {:.0} | {} | {:.0} | {} | {} |",
            row.policy,
            row.alone.jobs[0].latency().unwrap_or(Secs::INFINITY),
            row.stretch,
            log.idle_queued,
            log.idle_fitting,
            log.longest_fitting_wait,
            log.held_idle,
            f64::from(row.loaded_peak_threads) / cores,
            row.loaded_peak_memory as f64 / GB as f64,
            row.gate_slowdown,
            row.background_wait,
            row.slowest_agent,
            row.random_stretch.0,
            row.random_stretch.1,
            row.random_idle_fitting,
            row.random_gate_behind,
            row.heavy_memory_over / GB as f64,
            row.heavy_ended,
            row.unfinished,
        );
    }
    out
}

#[test]
fn score_table() {
    let recorded = gate_2026_10_30();
    let rows: Vec<Row> = CANDIDATES
        .iter()
        .map(|policy| score(policy, &recorded))
        .collect();
    println!("{}", table(&rows, &recorded));
}
