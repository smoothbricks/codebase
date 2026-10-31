//! mbx's pool alone, replayed from its own logs: every admission's arrival, charge and hold, with
//! the pool's rule as the only variable. The replay is trace-driven and open-loop, as the
//! backfilling literature evaluates a rule (Mu'alem and Feitelson 2001): arrivals keep their
//! recorded times whatever the rule decides, and a grant holds its charge for its recorded hold,
//! stretched in proportion when a moldable rule grants fewer cpus than asked.

use serde::Deserialize;

use crate::admit::{Charge, Grant, Lease, Priority, Request, Rule, admit};
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

/// What asked: mbx's observation kinds.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Kind {
    Compile,
    Link,
    Reservation,
    TestBinary,
    NextestTest,
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
    /// Nothing: every hold might never end.
    Unknown,
}

impl Estimate {
    fn of(self, hold: Secs) -> Secs {
        match self {
            Self::Exact => hold,
            Self::Times(factor) => hold * factor,
            Self::Unknown => Secs::INFINITY,
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

struct Live {
    charge: Charge,
    estimated_end: Secs,
    end: Secs,
}

/// Replays `trace` under `rule`. A request recorded as started before the window opened keeps its
/// recorded start: that history is not the rule's to decide.
pub(crate) fn replay(trace: &Trace, rule: Rule, estimate: Estimate) -> Replayed {
    let n = trace.requests.len();
    let mut started = vec![Secs::NAN; n];
    let mut granted = vec![Charge::default(); n];
    let mut fit_since: Vec<Option<Secs>> = vec![None; n];
    let (mut forced, mut arriving): (Vec<usize>, Vec<usize>) =
        (0..n).partition(|&i| trace.requests[i].recorded_start < 0.0);
    forced.sort_by(|&a, &b| {
        trace.requests[a]
            .recorded_start
            .total_cmp(&trace.requests[b].recorded_start)
    });
    arriving.sort_by(|&a, &b| {
        trace.requests[a]
            .at
            .total_cmp(&trace.requests[b].at)
            .then(a.cmp(&b))
    });
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
            forced.peek().map(|&i| trace.requests[i].recorded_start),
            arriving.peek().map(|&i| trace.requests[i].at),
            live.iter().map(|l| l.end).min_by(f64::total_cmp),
            aging.and_then(|bound| {
                queue
                    .iter()
                    .map(|&i| trace.requests[i].at + bound)
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
        live.retain(|l| l.end > now);
        while let Some(&i) = forced.peek()
            && trace.requests[i].recorded_start <= now
        {
            forced.next();
            let request = &trace.requests[i];
            started[i] = request.recorded_start;
            granted[i] = request.charge;
            let end = request.recorded_start + request.hold;
            live.push(Live {
                charge: request.charge,
                estimated_end: end,
                end,
            });
        }
        while let Some(&i) = arriving.peek()
            && trace.requests[i].at <= now
        {
            arriving.next();
            queue.push(i);
        }
        let mut decided = false;
        if !queue.is_empty() {
            let requests: Vec<Request> = queue
                .iter()
                .map(|&i| {
                    let r = &trace.requests[i];
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
                        estimate: estimate.of(r.hold),
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
            let aged_at = |i: usize| now - trace.requests[i].at >= AGING;
            let mut chosen = vec![false; queue.len()];
            for grant in &grants {
                chosen[grant.index] = true;
            }
            for (k, &i) in queue.iter().enumerate() {
                if chosen[k] {
                    continue;
                }
                let waiting = &trace.requests[i];
                let started: Vec<&Arrival> = grants
                    .iter()
                    .map(|g| &trace.requests[queue[g.index]])
                    .collect();
                if aged_at(i)
                    && started
                        .iter()
                        .any(|s| s.at > waiting.at && s.priority >= waiting.priority)
                {
                    out.aged_passed += 1;
                }
                let fresh_low = grants.iter().any(|g| {
                    trace.requests[queue[g.index]].priority == Priority::Low
                        && !aged_at(queue[g.index])
                });
                if waiting.priority == Priority::Normal
                    && need(rule, waiting.charge).fits(used_before, trace.cap)
                    && fresh_low
                {
                    out.gate_passed += 1;
                }
            }
            decided = !grants.is_empty();
            let mut taken = vec![false; queue.len()];
            for &Grant { index, charge } in &grants {
                let i = queue[index];
                let request = &trace.requests[i];
                taken[index] = true;
                started[i] = now;
                granted[i] = charge;
                if let Some(since) = fit_since[i] {
                    out.longest_fitting_wait = out.longest_fitting_wait.max(now - since);
                }
                let stretch = if charge.cpus == 0 || request.charge.cpus == 0 {
                    1.0
                } else {
                    f64::from(request.charge.cpus) / f64::from(charge.cpus)
                };
                let hold = request.hold * stretch;
                live.push(Live {
                    charge,
                    estimated_end: now + estimate.of(request.hold) * stretch,
                    end: now + hold,
                });
            }
            let mut taken = taken.into_iter();
            queue.retain(|_| !taken.next().unwrap_or(false));
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
            if need(rule, trace.requests[i].charge).fits(used, trace.cap) {
                fit_since[i].get_or_insert(now);
                fitting = true;
            } else {
                if let Some(since) = fit_since[i].take() {
                    out.longest_fitting_wait = out.longest_fitting_wait.max(now - since);
                }
            }
        }
        out.end = out.end.max(live.iter().map(|l| l.end).fold(now, f64::max));
    }
    out.started = started;
    out.granted = granted;
    out
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
