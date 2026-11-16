//! mbx's pool alone, replayed from its own logs: every admission's arrival, charge and hold, with
//! the pool's rule as the only variable. The replay is trace-driven and open-loop, as the
//! backfilling literature evaluates a rule (Mu'alem and Feitelson 2001): arrivals keep their
//! recorded times whatever the rule decides, and a grant holds its charge for its recorded hold,
//! stretched in proportion when a moldable rule grants fewer cpus than asked.

use serde::Deserialize;

use crate::admit::{Charge, Grant, Kind, Lease, Priority, Request, Rule, admit};
use crate::model::{Class, Secs};
use crate::policy::AGING;

const CONVOY: &str = include_str!("../fixtures/pool-2026-10-30-convoy.json");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    about: String,
    cap: CapFacts,
    window_ms: u64,
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
struct RequestFacts {
    arrival_ms: i64,
    started_ms: i64,
    hold_ms: u64,
    cpus: u32,
    memory_bytes: u64,
    kind: Kind,
}

/// One recorded admission.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Arrival {
    pub(crate) at: Secs,
    pub(crate) recorded_start: Secs,
    pub(crate) hold: Secs,
    pub(crate) charge: Charge,
    pub(crate) kind: Kind,
    pub(crate) priority: Priority,
    /// What mbx predicted its hold to be when it asked, where the record says.
    pub(crate) predicted: Option<Secs>,
}

#[derive(Debug)]
pub(crate) struct Trace {
    pub(crate) about: String,
    pub(crate) cap: Charge,
    pub(crate) window: Secs,
    pub(crate) requests: Vec<Arrival>,
}

/// The ten minutes of 2026-10-30 from 21:33:20Z in which one 18-cpu reservation waited 401 s for
/// the whole pool while every younger request waited behind it.
pub(crate) fn convoy() -> Trace {
    let fixture: Fixture = serde_json::from_str(CONVOY).expect("the committed pool fixture reads");
    let ms = |ms: i64| ms as Secs / 1000.0;
    Trace {
        about: fixture.about,
        cap: Charge {
            cpus: fixture.cap.cpus,
            memory: fixture.cap.memory_bytes,
        },
        window: fixture.window_ms as Secs / 1000.0,
        requests: fixture
            .requests
            .iter()
            .map(|r| Arrival {
                at: ms(r.arrival_ms),
                recorded_start: ms(r.started_ms),
                hold: r.hold_ms as Secs / 1000.0,
                charge: Charge {
                    cpus: r.cpus,
                    memory: r.memory_bytes,
                },
                kind: r.kind,
                // Every admission of the recorded window was `low`.
                priority: Priority::Low,
                predicted: None,
            })
            .collect(),
    }
}

/// What a rule knows of a request's hold before it runs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Estimate {
    /// Its recorded hold.
    Exact,
    /// Its recorded hold times a factor.
    Times(f64),
    /// Nothing: no hold has a prediction, and any might never end.
    Unknown,
    /// What mbx predicted when the request asked, as the record holds it.
    Recorded,
}

impl Estimate {
    fn of(self, request: &Arrival) -> Option<Secs> {
        match self {
            Self::Exact => Some(request.hold),
            Self::Times(factor) => Some(request.hold * factor),
            Self::Unknown => None,
            Self::Recorded => request.predicted,
        }
    }
}

/// What a rule did with a trace.
#[derive(Clone, Debug)]
pub(crate) struct Replayed {
    pub(crate) started: Vec<Secs>,
    pub(crate) granted: Vec<Charge>,
    /// Core-seconds no grant held while a request waited.
    pub(crate) idle_queued: Secs,
    /// Core-seconds no grant held while a request that could start on what was free waited.
    pub(crate) idle_fitting: Secs,
    /// The longest a request waited while it could start on what was free.
    pub(crate) longest_fitting_wait: Secs,
    /// Whether the grants together ever held more than the pool, beside the one oversize request
    /// mbx lets run alone.
    pub(crate) over_budget: bool,
    /// Steps at which a request that had waited the aging bound was passed by a younger one of its
    /// own priority or lower: starvation. A higher priority passing it is the priority order.
    pub(crate) aged_passed: u32,
    /// Steps at which a fresh `low` request started while a `normal` one that could start waited.
    pub(crate) gate_passed: u32,
    pub(crate) end: Secs,
}

impl Replayed {
    pub(crate) fn wait(&self, trace: &Trace, i: usize) -> Secs {
        self.started[i] - trace.requests[i].at
    }
}

/// The least a request needs to start under `rule`: a moldable request one cpu.
pub(crate) fn need(rule: Rule, charge: Charge) -> Charge {
    match rule {
        Rule::Moldable(_) if charge.cpus > 0 => Charge {
            cpus: 1,
            memory: charge.memory / u64::from(charge.cpus),
        },
        _ => charge,
    }
}

/// Who holds a live grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Holder {
    /// A request of the trace, for its hold.
    Request,
    /// The cased leaf's reservation, until its last case has ended.
    Leaf,
    /// One case of the leaf, on a core the pool granted it alone.
    Case,
}

struct Live {
    charge: Charge,
    estimated_end: Secs,
    end: Secs,
    holder: Holder,
}

/// A reserved nextest leaf of a trace, run case by case instead of for its recorded hold
/// (05_gateway.md "A case starts on a core").
#[derive(Clone, Copy, Debug)]
pub(crate) struct CasedLeaf<'a> {
    /// The leaf's reservation among the trace's requests.
    pub(crate) request: usize,
    /// Each case's wall, in the order its runner starts them.
    pub(crate) cases: &'a [Secs],
    /// Whether the runner starts every thread of its ask and a case that finds no core of the
    /// grant free asks the pool for one at `low`; without it the runner has as many threads as
    /// the grant.
    pub(crate) gate: bool,
    /// The longest wall a case may have to ask the pool for a core; a longer one waits for a core
    /// of the grant.
    pub(crate) lend_up_to: Secs,
}

/// One case's run: its wall, when it started, and whether on a core the pool granted it alone.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct CaseRun {
    pub(crate) wall: Secs,
    pub(crate) started: Secs,
    pub(crate) lent: bool,
}

/// What a cased leaf did.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct LeafRun {
    pub(crate) started: Secs,
    pub(crate) end: Secs,
    pub(crate) granted: u32,
    /// The cases that ran on the leaf's grant, and on a core of their own.
    pub(crate) own: u32,
    pub(crate) lent: u32,
    /// Core-seconds the cases on a core of their own held.
    pub(crate) lent_core_s: Secs,
    /// Seconds the cases waited at the gate, summed.
    pub(crate) waited_case_s: Secs,
    /// Each case's run, in the order they started.
    pub(crate) runs: Vec<CaseRun>,
}

/// A case waiting at the leaf's gate: since when, and its request in the pool, if it has one.
struct Waiting {
    case: usize,
    since: Secs,
    request: Option<usize>,
}

/// A cased leaf's state between its grant and its last case's end.
struct Gate<'a> {
    leaf: CasedLeaf<'a>,
    run: LeafRun,
    running: bool,
    /// The next case its runner starts.
    next: usize,
    waiting: Vec<Waiting>,
    /// The ends of the cases on the leaf's own cores.
    own: Vec<Secs>,
    /// The cases on a core of their own.
    lent: u32,
    done: usize,
}

/// [`replay`], with `leaf` run case by case: its reservation is held from its grant until its
/// last case has ended, as many cases at once as it was granted cores run on those, and under its
/// gate each further case its runner has a thread for asks the pool for one core at `low`,
/// predicted by its own wall, held for that wall and withdrawn when a core of the grant takes the
/// case first. A core of the grant goes to the waiting case with the longest wall.
pub(crate) fn replay_cased(
    trace: &Trace,
    rule: Rule,
    estimate: Estimate,
    leaf: Option<CasedLeaf<'_>>,
) -> (Replayed, Option<LeafRun>) {
    // The trace's requests, then one per case that asked the pool for a core.
    let mut arrivals: Vec<Arrival> = trace.requests.clone();
    let n = arrivals.len();
    let mut case_of: Vec<Option<usize>> = vec![None; n];
    let mut started = vec![Secs::NAN; n];
    let mut granted = vec![Charge::default(); n];
    let mut fit_since: Vec<Option<Secs>> = vec![None; n];
    let (mut forced, mut arriving): (Vec<usize>, Vec<usize>) =
        (0..n).partition(|&i| arrivals[i].recorded_start < 0.0);
    forced.sort_by(|&a, &b| {
        arrivals[a]
            .recorded_start
            .total_cmp(&arrivals[b].recorded_start)
    });
    arriving.sort_by(|&a, &b| arrivals[a].at.total_cmp(&arrivals[b].at).then(a.cmp(&b)));
    let (mut forced, mut arriving) = (
        forced.into_iter().peekable(),
        arriving.into_iter().peekable(),
    );
    let aging = match rule {
        Rule::List(list) => list.aging,
        Rule::Easy(easy) => easy.aging,
        Rule::Moldable(moldable) => Some(moldable.aging),
        Rule::Mbx => None,
    };
    let mut gate = leaf.map(|leaf| {
        assert!(
            arrivals[leaf.request].recorded_start >= 0.0,
            "a cased leaf asks inside the window"
        );
        Gate {
            leaf,
            run: LeafRun::default(),
            running: false,
            next: 0,
            waiting: Vec::new(),
            own: Vec::new(),
            lent: 0,
            done: 0,
        }
    });
    let mut live: Vec<Live> = Vec::new();
    let mut queue: Vec<usize> = Vec::new();
    let mut out = Replayed {
        started: Vec::new(),
        granted: Vec::new(),
        idle_queued: 0.0,
        idle_fitting: 0.0,
        longest_fitting_wait: 0.0,
        over_budget: false,
        aged_passed: 0,
        gate_passed: 0,
        end: 0.0,
    };
    let mut now = Secs::NEG_INFINITY;
    let mut fitting = false;
    loop {
        let next = [
            forced.peek().map(|&i| arrivals[i].recorded_start),
            arriving.peek().map(|&i| arrivals[i].at),
            live.iter()
                .map(|l| l.end)
                .filter(|end| end.is_finite())
                .min_by(f64::total_cmp),
            gate.as_ref()
                .and_then(|gate| gate.own.iter().copied().min_by(f64::total_cmp)),
            aging.and_then(|bound| {
                queue
                    .iter()
                    .map(|&i| arrivals[i].at + bound)
                    .filter(|&at| at > now)
                    .min_by(f64::total_cmp)
            }),
        ]
        .into_iter()
        .flatten()
        .min_by(f64::total_cmp);
        let Some(next) = next else {
            break;
        };
        if now.is_finite() && next > now {
            let used: u32 = live.iter().map(|l| l.charge.cpus).sum();
            let idle = f64::from(trace.cap.cpus.saturating_sub(used)) * (next - now);
            if !queue.is_empty() {
                out.idle_queued += idle;
            }
            if fitting {
                out.idle_fitting += idle;
            }
        }
        now = next;
        if let Some(gate) = &mut gate {
            let ended = live
                .iter()
                .filter(|l| l.holder == Holder::Case && l.end <= now)
                .count();
            gate.lent -= u32::try_from(ended).unwrap_or(u32::MAX);
            let own = gate.own.len();
            gate.own.retain(|&end| end > now);
            gate.done += ended + own - gate.own.len();
        }
        live.retain(|l| l.end > now);
        while let Some(&i) = forced.peek()
            && arrivals[i].recorded_start <= now
        {
            forced.next();
            let request = &arrivals[i];
            started[i] = request.recorded_start;
            granted[i] = request.charge;
            let end = request.recorded_start + request.hold;
            live.push(Live {
                charge: request.charge,
                estimated_end: end,
                end,
                holder: Holder::Request,
            });
        }
        while let Some(&i) = arriving.peek()
            && arrivals[i].at <= now
        {
            arriving.next();
            queue.push(i);
        }
        let mut decided = false;
        // A leaf granted in a pass puts its first cases at its gate, and those that find no core
        // of the grant ask at the same instant: one more pass admits them.
        loop {
            if let Some(gate) = gate.as_mut().filter(|gate| gate.running) {
                let ask = arrivals[gate.leaf.request].charge.cpus;
                let threads = if gate.leaf.gate {
                    ask
                } else {
                    gate.run.granted
                };
                while gate.next < gate.leaf.cases.len()
                    && gate.own.len() + gate.waiting.len()
                        < usize::try_from(threads.saturating_sub(gate.lent)).unwrap_or(usize::MAX)
                {
                    gate.waiting.push(Waiting {
                        case: gate.next,
                        since: now,
                        request: None,
                    });
                    gate.next += 1;
                }
                while gate.own.len() < usize::try_from(gate.run.granted).unwrap_or(usize::MAX)
                    && let Some(longest) = (0..gate.waiting.len()).max_by(|&a, &b| {
                        gate.leaf.cases[gate.waiting[a].case]
                            .total_cmp(&gate.leaf.cases[gate.waiting[b].case])
                            .then(b.cmp(&a))
                    })
                {
                    let waiting = gate.waiting.remove(longest);
                    if let Some(request) = waiting.request {
                        queue.retain(|&i| i != request);
                    }
                    gate.run.waited_case_s += now - waiting.since;
                    gate.run.own += 1;
                    gate.run.runs.push(CaseRun {
                        wall: gate.leaf.cases[waiting.case],
                        started: now,
                        lent: false,
                    });
                    gate.own.push(now + gate.leaf.cases[waiting.case]);
                }
                if gate.leaf.gate {
                    for waiting in gate.waiting.iter_mut().filter(|w| w.request.is_none()) {
                        let wall = gate.leaf.cases[waiting.case];
                        if wall > gate.leaf.lend_up_to {
                            continue;
                        }
                        waiting.request = Some(arrivals.len());
                        queue.push(arrivals.len());
                        arrivals.push(Arrival {
                            at: now,
                            recorded_start: now,
                            hold: wall,
                            charge: Charge { cpus: 1, memory: 0 },
                            kind: Kind::NextestTest,
                            priority: Priority::Low,
                            predicted: Some(wall),
                        });
                        case_of.push(Some(waiting.case));
                        started.push(Secs::NAN);
                        granted.push(Charge::default());
                        fit_since.push(None);
                    }
                }
                if gate.done == gate.leaf.cases.len() {
                    gate.running = false;
                    gate.run.end = now;
                    live.retain(|l| l.holder != Holder::Leaf);
                }
            }
            let mut leaf_started = false;
            if !queue.is_empty() {
                let requests: Vec<Request> = queue
                    .iter()
                    .map(|&i| {
                        let r = &arrivals[i];
                        Request {
                            ticket: u64::try_from(i).unwrap_or(u64::MAX),
                            class: if r.priority == Priority::Normal {
                                Class::Gate
                            } else {
                                Class::Background
                            },
                            priority: r.priority,
                            since: r.at,
                            rank: 0.0,
                            kind: r.kind,
                            // A case's request is named for the case, so mbx predicts it by the
                            // case's own history whatever it knows of the trace's requests.
                            predicted: if case_of[i].is_some() {
                                r.predicted
                            } else {
                                estimate.of(r)
                            },
                            charge: r.charge,
                        }
                    })
                    .collect();
                let leases: Vec<Lease> = live
                    .iter()
                    .map(|l| Lease {
                        charge: l.charge,
                        ends: l.estimated_end,
                    })
                    .collect();
                let grants = admit(rule, &requests, &leases, trace.cap, now);
                let used_before = live
                    .iter()
                    .fold(Charge::default(), |sum, l| sum.plus(l.charge));
                let aged_at = |i: usize| now - arrivals[i].at >= AGING;
                let mut chosen = vec![false; queue.len()];
                for grant in &grants {
                    chosen[grant.index] = true;
                }
                for (k, &i) in queue.iter().enumerate() {
                    if chosen[k] {
                        continue;
                    }
                    let waiting = &arrivals[i];
                    let started: Vec<&Arrival> =
                        grants.iter().map(|g| &arrivals[queue[g.index]]).collect();
                    if aged_at(i)
                        && started
                            .iter()
                            .any(|s| s.at > waiting.at && s.priority >= waiting.priority)
                    {
                        out.aged_passed += 1;
                    }
                    let fresh_low = grants.iter().any(|g| {
                        arrivals[queue[g.index]].priority == Priority::Low
                            && !aged_at(queue[g.index])
                    });
                    if waiting.priority == Priority::Normal
                        && need(rule, waiting.charge).fits(used_before, trace.cap)
                        && fresh_low
                    {
                        out.gate_passed += 1;
                    }
                }
                decided |= !grants.is_empty();
                let mut taken = vec![false; queue.len()];
                for &Grant { index, charge } in &grants {
                    let i = queue[index];
                    let request = &arrivals[i];
                    taken[index] = true;
                    started[i] = now;
                    granted[i] = charge;
                    if let Some(since) = fit_since[i] {
                        out.longest_fitting_wait = out.longest_fitting_wait.max(now - since);
                    }
                    if let Some(gate) = gate.as_mut().filter(|gate| gate.leaf.request == i) {
                        gate.running = true;
                        gate.run.started = now;
                        gate.run.granted = charge.cpus;
                        leaf_started = true;
                        live.push(Live {
                            charge,
                            estimated_end: Secs::INFINITY,
                            end: Secs::INFINITY,
                            holder: Holder::Leaf,
                        });
                        continue;
                    }
                    if let (Some(case), Some(gate)) = (case_of[i], gate.as_mut()) {
                        let at = gate
                            .waiting
                            .iter()
                            .position(|w| w.case == case)
                            .expect("a case's request is withdrawn when its case starts");
                        let waiting = gate.waiting.remove(at);
                        gate.run.waited_case_s += now - waiting.since;
                        gate.run.lent += 1;
                        gate.run.runs.push(CaseRun {
                            wall: request.hold,
                            started: now,
                            lent: true,
                        });
                        gate.run.lent_core_s += request.hold;
                        gate.lent += 1;
                        live.push(Live {
                            charge,
                            estimated_end: now + request.hold,
                            end: now + request.hold,
                            holder: Holder::Case,
                        });
                        continue;
                    }
                    let stretch = if charge.cpus == 0 || request.charge.cpus == 0 {
                        1.0
                    } else {
                        f64::from(request.charge.cpus) / f64::from(charge.cpus)
                    };
                    let hold = request.hold * stretch;
                    live.push(Live {
                        charge,
                        estimated_end: estimate
                            .of(request)
                            .map_or(Secs::INFINITY, |hold| now + hold * stretch),
                        end: now + hold,
                        holder: Holder::Request,
                    });
                }
                let mut taken = taken.into_iter();
                queue.retain(|_| !taken.next().unwrap_or(false));
            }
            if !leaf_started {
                break;
            }
        }
        let used = live
            .iter()
            .fold(Charge::default(), |sum, l| sum.plus(l.charge));
        if decided
            && (used.cpus > trace.cap.cpus || used.memory > trace.cap.memory)
            && live.len() > 1
        {
            out.over_budget = true;
        }
        fitting = false;
        for &i in &queue {
            if need(rule, arrivals[i].charge).fits(used, trace.cap) {
                fit_since[i].get_or_insert(now);
                fitting = true;
            } else {
                if let Some(since) = fit_since[i].take() {
                    out.longest_fitting_wait = out.longest_fitting_wait.max(now - since);
                }
            }
        }
        out.end = out.end.max(
            live.iter()
                .map(|l| l.end)
                .filter(|end| end.is_finite())
                .fold(now, f64::max),
        );
    }
    started.truncate(n);
    granted.truncate(n);
    out.started = started;
    out.granted = granted;
    (out, gate.map(|gate| gate.run))
}

/// Replays `trace` under `rule`. A request recorded as started before the window opened keeps its
/// recorded start: that history is not the rule's to decide.
pub(crate) fn replay(trace: &Trace, rule: Rule, estimate: Estimate) -> Replayed {
    replay_cased(trace, rule, estimate, None).0
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    use crate::admit::{Easy, List, Moldable, Order};
    use crate::policy::POOL_RULE;
    use crate::random::Rng;

    /// A random pool trace: mostly one-cpu requests, some mid-sized, a tenth for the whole pool,
    /// holds over four orders of magnitude, half charging a memory share, a fifth `normal`.
    fn random_trace(seed: u64, size: usize) -> Trace {
        let mut rng = Rng::new(seed);
        let cap = Charge {
            cpus: 18,
            memory: 116_823_110_390,
        };
        let share = cap.memory / 18;
        let mut at = 0.0;
        let requests = (0..size)
            .map(|_| {
                at += rng.log_range(0.01, 5.0);
                let roll = rng.unit();
                let cpus = if roll < 0.75 {
                    1
                } else if roll < 0.9 {
                    2 + u32::try_from(rng.below(8)).unwrap_or(0)
                } else {
                    17 + u32::try_from(rng.below(2)).unwrap_or(0)
                };
                Arrival {
                    at,
                    recorded_start: at,
                    hold: rng.log_range(0.05, 200.0),
                    charge: Charge {
                        cpus,
                        memory: if rng.chance(0.5) { share } else { 0 },
                    },
                    kind: Kind::Reservation,
                    priority: if rng.chance(0.2) {
                        Priority::Normal
                    } else {
                        Priority::Low
                    },
                    predicted: None,
                }
            })
            .collect();
        Trace {
            about: String::new(),
            cap,
            window: at,
            requests,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

        /// Work-conserving: no request waits while a cpu it could start on is free. Gate first:
        /// no fresh `low` request starts while a `normal` one that could start waits. No
        /// starvation: no request is passed by a younger one once it has waited the aging bound.
        /// The budget: the grants never hold more than the pool. And every request runs.
        #[test]
        fn the_recommended_pool_rule_keeps_every_property(seed in any::<u64>(), size in 20_usize..160) {
            let trace = random_trace(seed, size);
            let replayed = replay(&trace, POOL_RULE, Estimate::Unknown);
            prop_assert_eq!(replayed.idle_fitting, 0.0);
            prop_assert_eq!(replayed.longest_fitting_wait, 0.0);
            prop_assert_eq!(replayed.gate_passed, 0);
            prop_assert_eq!(replayed.aged_passed, 0);
            prop_assert!(!replayed.over_budget);
            prop_assert!(replayed.started.iter().all(|start| start.is_finite()));
        }
    }

    #[test]
    fn every_other_rule_breaks_a_property_on_some_trace() {
        let rules = [
            ("mbx", Rule::Mbx),
            (
                "backfill, no bound",
                Rule::List(List {
                    order: Order::Class,
                    backfill: true,
                    aging: None,
                }),
            ),
            (
                "backfill, aged drain",
                Rule::List(List {
                    order: Order::Class,
                    backfill: true,
                    aging: Some(AGING),
                }),
            ),
            (
                "EASY",
                Rule::Easy(Easy {
                    order: Order::Class,
                    aging: None,
                }),
            ),
            (
                "moldable, half floor",
                Rule::Moldable(Moldable {
                    order: Order::Class,
                    aging: AGING,
                    greedy: false,
                }),
            ),
        ];
        for (name, rule) in rules {
            let broken = (0..64).find_map(|seed| {
                let replayed = replay(&random_trace(seed, 120), rule, Estimate::Exact);
                let broken = [
                    ("work conservation", replayed.idle_fitting > 0.0),
                    ("gate first", replayed.gate_passed > 0),
                    ("no starvation", replayed.aged_passed > 0),
                    ("budget", replayed.over_budget),
                ];
                broken
                    .into_iter()
                    .find(|(_, broken)| *broken)
                    .map(|(property, _)| (seed, property))
            });
            println!("{name}: {broken:?}");
            assert!(broken.is_some(), "{name} kept every property on 64 traces");
        }
    }

    fn wide(trace: &Trace) -> usize {
        trace
            .requests
            .iter()
            .position(|r| r.kind == Kind::Reservation && r.charge.cpus == 18)
            .expect("the convoy window holds its 18-cpu reservation")
    }

    /// Requests that arrived while the 18-cpu reservation waited, and how many of them waited
    /// four minutes or more.
    fn behind(trace: &Trace, replayed: &Replayed) -> (usize, usize) {
        let head = &trace.requests[wide(trace)];
        let recorded_grant = head.recorded_start;
        let behind: Vec<usize> = (0..trace.requests.len())
            .filter(|&i| trace.requests[i].at > head.at && trace.requests[i].at < recorded_grant)
            .collect();
        let long = behind
            .iter()
            .filter(|&&i| replayed.wait(trace, i) >= 240.0)
            .count();
        (behind.len(), long)
    }

    #[test]
    fn the_recorded_convoy_replays_red_under_mbx() {
        let trace = convoy();
        assert!(
            trace.requests.iter().all(|r| r.at < trace.window),
            "{}",
            trace.about
        );
        let replayed = replay(&trace, Rule::Mbx, Estimate::Unknown);
        let head = wide(&trace);
        // Recorded: the 18-cpu reservation waited 401 s for one one-cpu test binary to end, and
        // every one of the 36 requests behind it waited 280 to 404 s with 17 cpus free.
        let recorded = trace.requests[head].recorded_start - trace.requests[head].at;
        assert!(
            (replayed.wait(&trace, head) - recorded).abs() < 1.0,
            "{} vs {recorded}",
            replayed.wait(&trace, head)
        );
        assert_eq!(behind(&trace, &replayed), (36, 36));
        assert!(
            replayed.longest_fitting_wait > 390.0,
            "{}",
            replayed.longest_fitting_wait
        );
        assert!(
            replayed.idle_fitting > 17.0 * 270.0,
            "{}",
            replayed.idle_fitting
        );
    }

    #[test]
    fn the_replay_matches_the_recorded_starts_under_mbx() {
        let trace = convoy();
        let replayed = replay(&trace, Rule::Mbx, Estimate::Unknown);
        let late = (0..trace.requests.len())
            .filter(|&i| (replayed.started[i] - trace.requests[i].recorded_start).abs() > 5.0)
            .count();
        // Open-loop: a request the recorded pool started within 5 s of the replay is the rule's
        // own decision reproduced.
        assert!(
            late * 20 < trace.requests.len(),
            "{late} of {} differ",
            trace.requests.len()
        );
    }

    #[test]
    fn rules_on_the_convoy() {
        let trace = convoy();
        let head = wide(&trace);
        let rules = [
            ("mbx", Rule::Mbx, Estimate::Unknown),
            (
                "backfill, no bound",
                Rule::List(List {
                    order: Order::Fifo,
                    backfill: true,
                    aging: None,
                }),
                Estimate::Unknown,
            ),
            (
                "backfill, aged drain",
                Rule::List(List {
                    order: Order::Fifo,
                    backfill: true,
                    aging: Some(30.0),
                }),
                Estimate::Unknown,
            ),
            (
                "EASY, exact",
                Rule::Easy(Easy {
                    order: Order::Fifo,
                    aging: None,
                }),
                Estimate::Exact,
            ),
            (
                "EASY, unknown",
                Rule::Easy(Easy {
                    order: Order::Fifo,
                    aging: None,
                }),
                Estimate::Unknown,
            ),
            (
                "EASY, twice the hold",
                Rule::Easy(Easy {
                    order: Order::Fifo,
                    aging: None,
                }),
                Estimate::Times(2.0),
            ),
            (
                "moldable",
                Rule::Moldable(Moldable {
                    order: Order::Fifo,
                    aging: 30.0,
                    greedy: false,
                }),
                Estimate::Unknown,
            ),
            (
                "moldable greedy",
                Rule::Moldable(Moldable {
                    order: Order::Fifo,
                    aging: 30.0,
                    greedy: true,
                }),
                Estimate::Unknown,
            ),
        ];
        for (name, rule, estimate) in rules {
            let replayed = replay(&trace, rule, estimate);
            let waits: Vec<Secs> = (0..trace.requests.len())
                .map(|i| replayed.wait(&trace, i))
                .collect();
            let total: Secs = waits.iter().sum();
            let worst = waits.iter().copied().fold(0.0, f64::max);
            println!(
                "{name}: head {:.0} s on {} cpus, behind {:?}, total wait {:.0} s, worst {:.0} s, idle queued {:.0}, idle fitting {:.0}, longest fitting {:.1}, end {:.0}, over {}",
                replayed.wait(&trace, head),
                replayed.granted[head].cpus,
                behind(&trace, &replayed),
                total,
                worst,
                replayed.idle_queued,
                replayed.idle_fitting,
                replayed.longest_fitting_wait,
                replayed.end,
                replayed.over_budget,
            );
        }
    }
}
