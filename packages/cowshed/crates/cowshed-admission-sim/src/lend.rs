//! Cores freed after a leaf's grant reach it one case at a time (05_gateway.md "A case starts on a
//! core"), replayed over recorded windows of mbx's pool: four land gates of one consumer workspace
//! whose host test suite, a reserved nextest leaf of 69 cases, asked for 18 cores and was granted
//! 1, 12, 1 and 2. Each window is replayed twice under the pool's rule, the leaf run case by case
//! both times: sized to its grant, as it ran, and behind its gate. Every other request keeps its
//! recorded arrival and hold, so what the replay answers is what the gate does to the pool, not
//! to the run's task graph: a task that would have started sooner because the leaf ended sooner
//! still arrives when it did.

use serde::Deserialize;

use crate::admit::{Charge, Kind, Priority};
use crate::model::Secs;
use crate::policy::POOL_RULE;
use crate::pool::{Arrival, CasedLeaf, Estimate, LeafRun, Replayed, Trace, replay_cased};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    about: String,
    cap: CapFacts,
    window_ms: u64,
    leaf: LeafFacts,
    requests: Vec<RequestFacts>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapFacts {
    cpus: u32,
    memory_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeafFacts {
    request: usize,
    cases_ms: Vec<u64>,
    /// The same cases' walls from a run of the suite that was 13 wide, where the window has them.
    wide_cases_ms: Option<Vec<u64>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestFacts {
    arrival_ms: i64,
    started_ms: i64,
    hold_ms: u64,
    /// The cores it asked for, and those its admission held.
    cpus: u32,
    granted: u32,
    memory_bytes: u64,
    kind: Kind,
    priority: Band,
    predicted_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Band {
    Interactive,
    Normal,
    Low,
}

/// One recorded window: its trace, the leaf's request in it, the leaf's recorded grant and hold,
/// and its cases' walls as the narrow run and, where known, a wide run recorded them.
struct Window {
    name: &'static str,
    trace: Trace,
    leaf: usize,
    recorded_granted: u32,
    recorded_hold: Secs,
    cases: Vec<Secs>,
    wide_cases: Option<Vec<Secs>>,
}

fn window(name: &'static str, json: &str) -> Window {
    let fixture: Fixture = serde_json::from_str(json).expect("the committed window reads");
    let secs = |ms: u64| ms as Secs / 1000.0;
    let signed = |ms: i64| ms as Secs / 1000.0;
    let recorded = &fixture.requests[fixture.leaf.request];
    let (recorded_granted, recorded_hold) = (recorded.granted, secs(recorded.hold_ms));
    let requests = fixture
        .requests
        .iter()
        .map(|r| {
            // A request the window opens on keeps what it held. One the rule decides is its ask,
            // its hold that of the whole ask: the replay stretches it again by the grant it makes.
            let before = r.started_ms < 0;
            let cpus = if before { r.granted } else { r.cpus };
            let hold = if before || r.cpus == 0 {
                secs(r.hold_ms)
            } else {
                secs(r.hold_ms) * f64::from(r.granted) / f64::from(r.cpus)
            };
            Arrival {
                at: signed(r.arrival_ms),
                recorded_start: signed(r.started_ms),
                hold,
                charge: Charge {
                    cpus,
                    memory: r.memory_bytes,
                },
                kind: r.kind,
                priority: match r.priority {
                    Band::Interactive => Priority::Interactive,
                    Band::Normal => Priority::Normal,
                    Band::Low => Priority::Low,
                },
                predicted: Some(secs(r.predicted_ms)),
            }
        })
        .collect();
    Window {
        name,
        trace: Trace {
            about: fixture.about,
            cap: Charge {
                cpus: fixture.cap.cpus,
                memory: fixture.cap.memory_bytes,
            },
            window: secs(fixture.window_ms),
            requests,
        },
        leaf: fixture.leaf.request,
        recorded_granted,
        recorded_hold,
        cases: fixture.leaf.cases_ms.iter().map(|&ms| secs(ms)).collect(),
        wide_cases: fixture
            .leaf
            .wide_cases_ms
            .map(|walls| walls.iter().map(|&ms| secs(ms)).collect()),
    }
}

fn windows() -> [Window; 4] {
    [
        window(
            "2026-11-16 22:11",
            include_str!("../fixtures/pool-2026-11-16-2211.json"),
        ),
        window(
            "2026-11-16 22:58",
            include_str!("../fixtures/pool-2026-11-16-2258.json"),
        ),
        window(
            "2026-11-16 23:14",
            include_str!("../fixtures/pool-2026-11-16-2314.json"),
        ),
        window(
            "2026-11-17 00:48",
            include_str!("../fixtures/pool-2026-11-17-0048.json"),
        ),
    ]
}

fn run(window: &Window, cases: &[Secs], gate: bool) -> (Replayed, LeafRun) {
    run_up_to(window, cases, gate, Secs::INFINITY)
}

fn run_up_to(window: &Window, cases: &[Secs], gate: bool, lend_up_to: Secs) -> (Replayed, LeafRun) {
    let (replayed, leaf) = replay_cased(
        &window.trace,
        POOL_RULE,
        Estimate::Recorded,
        Some(CasedLeaf {
            request: window.leaf,
            cases,
            gate,
            lend_up_to,
        }),
    );
    (replayed, leaf.expect("a cased replay answers its leaf"))
}

/// How much later each request other than the leaf started behind the gate than without it, by
/// band: the worst and the sum of the delays, in seconds, and how many were delayed at all.
fn delays(
    window: &Window,
    today: &Replayed,
    gated: &Replayed,
    band: Priority,
) -> (Secs, Secs, usize) {
    let mut worst: Secs = 0.0;
    let mut sum = 0.0;
    let mut count = 0;
    for (i, request) in window.trace.requests.iter().enumerate() {
        if i == window.leaf || request.priority != band || request.recorded_start < 0.0 {
            continue;
        }
        let delay = gated.started[i] - today.started[i];
        if delay > 0.0 {
            worst = worst.max(delay);
            sum += delay;
            count += 1;
        }
    }
    (worst, sum, count)
}

//#region smoo/cowshed!n/a-case-starts-on-a-core

/// Calibration: sized to its grant, the replayed leaf is granted what the record says and holds
/// it for about as long, and the other requests start about when they did.
#[test]
fn sized_to_its_grant_the_replay_matches_the_record() {
    for window in windows() {
        let (replayed, leaf) = run(&window, &window.cases, false);
        let (mut error, mut worst, mut counted) = (0.0, 0.0_f64, 0);
        for (i, request) in window.trace.requests.iter().enumerate() {
            if request.recorded_start < 0.0 {
                continue;
            }
            let off = (replayed.started[i] - request.recorded_start).abs();
            error += off;
            worst = worst.max(off);
            counted += 1;
        }
        println!(
            "{}: leaf granted {} against {} recorded, held {:.0} s against {:.0} s; {} requests start {:.1} s off the record in the mean, {:.0} s at worst",
            window.name,
            leaf.granted,
            window.recorded_granted,
            leaf.end - leaf.started,
            window.recorded_hold,
            counted,
            error / f64::from(u32::try_from(counted).unwrap_or(u32::MAX)),
            worst,
        );
        assert!(replayed.started.iter().all(|start| start.is_finite()));
        assert!(!replayed.over_budget);
        assert_eq!(leaf.granted, window.recorded_granted, "{}", window.name);
        // The two windows whose cases are the leaf's own record of that run hold it for as long.
        if matches!(window.name, "2026-11-16 23:14" | "2026-11-17 00:48") {
            let held = leaf.end - leaf.started;
            assert!(
                (held / window.recorded_hold - 1.0).abs() < 0.02,
                "{held} against {}",
                window.recorded_hold
            );
        }
    }
}

/// The defect and the result: on the two gates whose leaf was granted one core it ran its cases
/// in turn; behind its gate it ends within a small factor of its longest case.
#[test]
fn a_leaf_granted_one_core_runs_its_cases_side_by_side_on_cores_freed_later() {
    for window in windows() {
        for (walls, cases) in [
            ("narrow", Some(&window.cases)),
            ("wide", window.wide_cases.as_ref()),
        ] {
            let Some(cases) = cases else { continue };
            let (_, today) = run(&window, &window.cases, false);
            let (replayed, gated) = run(&window, cases, true);
            let longest = cases.iter().copied().fold(0.0, f64::max);
            println!(
                "{} ({walls} walls): leaf held {:.0} s on {} cores sized to its grant; gated {:.0} s, {} cases on its grant and {} on a core of their own ({:.0} core-s), cases waited {:.0} s in sum; longest case {:.0} s",
                window.name,
                today.end - today.started,
                today.granted,
                gated.end - gated.started,
                gated.own,
                gated.lent,
                gated.lent_core_s,
                gated.waited_case_s,
                longest,
            );
            assert!(!replayed.over_budget);
            assert_eq!(replayed.gate_passed, 0);
            assert_eq!(
                usize::try_from(gated.own + gated.lent).ok(),
                Some(cases.len())
            );
            if today.granted == 1 {
                assert!(
                    today.end - today.started >= cases.iter().sum::<Secs>() * 0.99
                        || walls == "wide"
                );
            }
            // Granted one core with a mean of 12.6 free beside it, the leaf ends with its longest case.
            if window.name == "2026-11-16 23:14" {
                assert!(
                    gated.end - gated.started <= 1.2 * longest,
                    "{} against {longest}",
                    gated.end - gated.started
                );
            }
            // The gate never makes the leaf slower than its grant alone.
            if walls == "narrow" {
                assert!(gated.end - gated.started <= today.end - today.started + 1e-6);
            }
        }
    }
}

/// What the gate costs every other request, by band.
#[test]
fn what_other_requests_wait_behind_a_gated_leaf() {
    for window in windows() {
        let (today, _) = run(&window, &window.cases, false);
        let (gated, _) = run(&window, &window.cases, true);
        for band in [Priority::Interactive, Priority::Normal, Priority::Low] {
            let (worst, sum, count) = delays(&window, &today, &gated, band);
            println!(
                "{}: {band:?} requests started later behind the gate: {count}, {worst:.1} s at worst, {sum:.1} s in sum",
                window.name
            );
            // A lent core is a `low` request of one core: nothing of a higher band waits for more
            // than a case's end or what that moves in a full pool. Measured: 81.5 s at worst for a
            // `normal` request (22:11, the pool full for 560 s), 10.3 s for an `interactive` one.
            match band {
                Priority::Interactive => assert!(worst <= 15.0, "{worst}"),
                Priority::Normal => assert!(worst <= 90.0, "{worst}"),
                Priority::Low => assert_eq!(count, 0),
            }
        }
        let sooner: Secs = window
            .trace
            .requests
            .iter()
            .enumerate()
            .filter(|&(i, r)| i != window.leaf && r.recorded_start >= 0.0)
            .map(|(i, _)| (today.started[i] - gated.started[i]).max(0.0))
            .sum();
        println!(
            "{}: other requests started {sooner:.1} s sooner in sum; the window's last grant ends at {:.0} s sized to the grant, {:.0} s gated",
            window.name, today.end, gated.end
        );
        assert!(gated.end <= today.end + 1e-6);
        assert_eq!(gated.gate_passed, 0);
    }
}

/// The bound on a lent case's wall, swept: what the leaf gains and what others wait at each.
#[test]
fn the_longest_case_the_pool_is_asked_for() {
    for window in windows() {
        let (today, today_leaf) = run(&window, &window.cases, false);
        for bound in [10.0, 30.0, 60.0, Secs::INFINITY] {
            let (gated, leaf) = run_up_to(&window, &window.cases, true, bound);
            let (worst, sum, count) = delays(&window, &today, &gated, Priority::Normal);
            let (worst_i, sum_i, count_i) = delays(&window, &today, &gated, Priority::Interactive);
            println!(
                "{} lend<={bound:>3.0} s: leaf {:.0} s -> {:.0} s ({} own, {} lent, {:.0} core-s); normal later {count} worst {worst:.1} s sum {sum:.0} s; interactive later {count_i} worst {worst_i:.1} s sum {sum_i:.0} s; last grant ends {:.0} -> {:.0} s",
                window.name,
                today_leaf.end - today_leaf.started,
                leaf.end - leaf.started,
                leaf.own,
                leaf.lent,
                leaf.lent_core_s,
                today.end,
                gated.end,
            );
        }
    }
}

/// Which requests the gate delays most where the pool is full, and the lent cases that held
/// cores while each waited.
#[test]
fn what_the_most_delayed_requests_waited_behind() {
    let window = &windows()[0];
    let (today, _) = run(window, &window.cases, false);
    let (gated, leaf) = run(window, &window.cases, true);
    let mut delayed: Vec<(Secs, usize)> = (0..window.trace.requests.len())
        .filter(|&i| i != window.leaf && window.trace.requests[i].recorded_start >= 0.0)
        .map(|i| (gated.started[i] - today.started[i], i))
        .filter(|&(delay, _)| delay > 0.0)
        .collect();
    delayed.sort_by(|a, b| b.0.total_cmp(&a.0));
    for &(delay, i) in delayed.iter().take(8) {
        let request = &window.trace.requests[i];
        let (from, to) = (today.started[i], gated.started[i]);
        let mut behind: Vec<Secs> = leaf
            .runs
            .iter()
            .filter(|case| case.lent && case.started < to && case.started + case.wall > from)
            .map(|case| case.wall)
            .collect();
        behind.sort_by(|a, b| b.total_cmp(a));
        println!(
            "{}: request {i} ({:?} {:?}, asks {} cores, holds {:.0} s, arrived {:.0} s) started {delay:.1} s later, at {to:.0} s instead of {from:.0} s; lent cases holding a core in between, by wall: {:?}",
            window.name,
            request.priority,
            request.kind,
            request.charge.cpus,
            request.hold,
            request.at,
            behind.iter().map(|wall| wall.round()).collect::<Vec<_>>(),
        );
    }
    let mut lent: Vec<Secs> = leaf
        .runs
        .iter()
        .filter(|c| c.lent)
        .map(|c| c.wall)
        .collect();
    lent.sort_by(|a, b| b.total_cmp(a));
    println!(
        "{}: {} lent cases, the eight longest by wall: {:?}; delays over 10 s: {}",
        window.name,
        lent.len(),
        lent.iter()
            .take(8)
            .map(|wall| wall.round())
            .collect::<Vec<_>>(),
        delayed.iter().filter(|&&(delay, _)| delay > 10.0).count(),
    );
}

//#endregion smoo/cowshed!n/a-case-starts-on-a-core
