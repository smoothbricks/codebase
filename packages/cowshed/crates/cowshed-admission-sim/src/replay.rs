//! The three failures of 2026-10-30, replayed from the recorded gate: red under A, each as the
//! property it breaks, and green under the recommendation; A checked against the recorded run;
//! and the engine's property suite over random workloads.

use proptest::prelude::*;

use crate::engine::{Outcome, run};
use crate::fixture::gate_2026_10_30;
use crate::model::{JobIx, Runner, Secs, Workload, list_span};
use crate::policy::{RECOMMENDED, TODAY, TODAY_PER_CORE, TODAY_PER_TEST};
use crate::random;
use crate::score::{RANDOM, stretch};

/// How many times longer than its own span on the threads it ran prebuilt-nextest leaves of
/// twenty tests or more ran, at worst.
fn nextest_stretch(workload: &Workload, outcome: &Outcome) -> Secs {
    workload
        .tasks
        .iter()
        .zip(&outcome.tasks)
        .filter(|(task, _)| task.runner == Runner::Nextest && task.units.len() >= 20)
        .filter_map(|(task, out)| {
            let ran = out.end? - out.started?;
            Some(ran / (task.setup + list_span(&task.units, out.threads.max(1))))
        })
        .fold(0.0, f64::max)
}

/// The most prebuilt-nextest leaves that ran at once.
fn nextest_overlap(workload: &Workload, outcome: &Outcome) -> usize {
    let mut edges: Vec<(Secs, i32)> = workload
        .tasks
        .iter()
        .zip(&outcome.tasks)
        .filter(|(task, _)| task.runner == Runner::Nextest)
        .filter_map(|(_, out)| Some([(out.started?, 1), (out.end?, -1)]))
        .flatten()
        .collect();
    edges.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut running = 0_i32;
    let mut most = 0_i32;
    for (_, step) in edges {
        running += step;
        most = most.max(running);
    }
    usize::try_from(most).unwrap_or(0)
}

#[test]
fn a_replays_the_recorded_gate() {
    let recorded = gate_2026_10_30();
    let outcome = run(&recorded.workload(), &TODAY);
    let makespan = outcome.jobs[0].latency().expect("the gate ends");
    let recorded_waits: Vec<Secs> = recorded
        .facts
        .iter()
        .filter_map(|f| f.reserve.map(|r| r.1))
        .collect();
    let simulated_waits: Vec<Secs> = recorded
        .facts
        .iter()
        .zip(&outcome.tasks)
        .filter(|(fact, _)| fact.reserve.is_some())
        .map(|(_, out)| out.waited)
        .collect();
    let sum = |waits: &[Secs]| waits.iter().sum::<Secs>();
    let most = |waits: &[Secs]| waits.iter().copied().fold(0.0, f64::max);
    println!(
        "A today: makespan {makespan:.0} s against {:.0} s recorded; leaf waits {:.0} s in all against {:.0} s, longest {:.0} s against {:.0} s",
        recorded.makespan,
        sum(&simulated_waits),
        sum(&recorded_waits),
        most(&simulated_waits),
        most(&recorded_waits),
    );
    // The recorded run shared the pool with other checkouts' work, which the replay leaves out.
    assert!(
        (makespan / recorded.makespan - 1.0).abs() < 0.15,
        "{makespan} vs {}",
        recorded.makespan
    );
    assert!(most(&simulated_waits) >= 100.0);
}

/// Failure 3, 2026-10-31 00:40: leaves queued up to 2m50s with 42% of the cores idle, behind a
/// 17-cpu reservation at the head of mbx's pool. Red: requests wait while what they need is free.
#[test]
fn a_wide_head_holds_leaves_while_cores_idle_red_under_a_and_green_recommended() {
    let workload = gate_2026_10_30().workload();
    let red = run(&workload, &TODAY);
    assert!(red.log.longest_fitting_wait >= 120.0, "{:?}", red.log);
    assert!(red.log.idle_fitting >= 1500.0, "{:?}", red.log);
    let green = run(&workload, &RECOMMENDED);
    assert_eq!(green.log.longest_fitting_wait, 0.0);
    assert_eq!(green.log.idle_fitting, 0.0);
}

/// Failure 2: every prebuilt-nextest leaf reserving `--cpus 18` serialized the test targets with
/// most of the cpu idle. Red: one such leaf at a time, and its idle grant.
#[test]
fn whole_host_nextest_reserves_serialize_red_under_a_and_green_recommended() {
    let workload = gate_2026_10_30().workload();
    let red = run(&workload, &TODAY_PER_CORE);
    let green = run(&workload, &RECOMMENDED);
    println!(
        "nextest --cpus cores: {} at once, {:.0} core-s granted idle, makespan {:.0} s; recommended: {} at once, {:.0} core-s, {:.0} s",
        nextest_overlap(&workload, &red),
        red.log.held_idle,
        red.jobs[0].latency().unwrap_or(Secs::INFINITY),
        nextest_overlap(&workload, &green),
        green.log.held_idle,
        green.jobs[0].latency().unwrap_or(Secs::INFINITY),
    );
    assert_eq!(nextest_overlap(&workload, &red), 1);
    assert!(red.log.held_idle >= 3000.0, "{:?}", red.log);
    assert!(nextest_overlap(&workload, &green) > 1);
    assert!(green.jobs[0].latency() < red.jobs[0].latency().map(|l| l / 1.5));
}

/// Failure 1: per-test-process mbx tickets made trivial tests wait seconds each. Red: a nextest
/// leaf runs many times its own span.
#[test]
fn per_test_tickets_stretch_a_nextest_leaf_red_under_a_and_green_recommended() {
    let workload = gate_2026_10_30().workload();
    let red = nextest_stretch(&workload, &run(&workload, &TODAY_PER_TEST));
    let green = nextest_stretch(&workload, &run(&workload, &RECOMMENDED));
    println!("per-test tickets: a nextest leaf ran {red:.1}x its span; recommended {green:.1}x");
    assert!(red >= 5.0, "{red}");
    assert!(green <= 2.0, "{green}");
}

#[test]
fn the_recommendation_keeps_the_recorded_gate_near_its_bound() {
    let workload = gate_2026_10_30().workload();
    let outcome = run(&workload, &RECOMMENDED);
    let bound = workload.bound(JobIx(0));
    assert!(
        stretch(&workload, &outcome) <= 1.25,
        "{} against {bound:?}",
        stretch(&workload, &outcome)
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// Over random gates beside agent runs, the recommendation: no request waits while what it
    /// needs is free, no `low` request starts while a gate request that could start waits, the
    /// grants never hold more than the pool, and every task ends. No makespan bound is claimed
    /// here: beside agent runs a running grant is never preempted, so a gate request can wait on
    /// a long background holder (a seed found 2.1 times the gate's bound).
    #[test]
    fn the_recommendation_keeps_every_property(seed in any::<u64>()) {
        let recorded_host = gate_2026_10_30().host;
        let workload = random::workload(seed, RANDOM, recorded_host);
        let outcome = run(&workload, &RECOMMENDED);
        prop_assert_eq!(outcome.log.idle_fitting, 0.0);
        prop_assert_eq!(outcome.log.longest_fitting_wait, 0.0);
        prop_assert_eq!(outcome.log.gate_behind, 0);
        prop_assert_eq!(outcome.log.over_budget, 0);
        prop_assert_eq!(outcome.log.unfinished, 0);
    }
}
