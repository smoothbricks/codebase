//! The pure admission decision: which of a pool's waiting requests start now, and with what. Every
//! rule shares one signature, so the engine and the pool replay run each over the same ledger.
//!
//! - [`Rule::Mbx`] is mbx's pool as `crates/mbx/src/scheduler.rs` implements it: requests in ticket
//!   order; an older `low` waiter never blocks a newer `normal` one; a younger request starts
//!   ahead of an older waiter only when it cannot delay that waiter's earliest possible start,
//!   judged without durations from the least set of live leases whose end would make room for it
//!   (the "passing" rule); and while a `normal` request waits, `low` requests see a quarter of
//!   each budget withheld.
//! - [`Rule::List`] is priority list scheduling (Graham; HLFET's highest bottom level first) with
//!   backfill: any request that fits starts, in priority order, except that a request which has
//!   waited the aging bound goes first and nothing fresher starts past it until it fits.
//! - [`Rule::Easy`] is EASY backfilling (Lifka 1995; Mu'alem and Feitelson 2001): the first
//!   request that does not fit gets a reservation at the earliest time the live grants' expected
//!   ends make room for it, and a later one starts now only if it ends before then or fits in what
//!   the reservation leaves over.
//! - [`Rule::Moldable`] grants a request as many of its cpus as are free: greedy, on any free cpu
//!   at once; otherwise once half of them are free, and on any free cpu once it has aged. Its
//!   aging orders requests within their class only, so a higher class always goes first. No
//!   request ever waits for the whole host.

use std::cmp::Ordering;

use crate::model::{Class, Secs};

/// mbx's two priorities (05_gateway.md "Classes": a land's gate `normal`, everything else `low`).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Priority {
    Normal,
    Low,
}

/// What a grant holds: cpus and memory bytes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Charge {
    pub(crate) cpus: u32,
    pub(crate) memory: u64,
}

impl Charge {
    pub(crate) fn plus(self, other: Self) -> Self {
        Self {
            cpus: self.cpus.saturating_add(other.cpus),
            memory: self.memory.saturating_add(other.memory),
        }
    }

    pub(crate) fn minus(self, other: Self) -> Self {
        Self {
            cpus: self.cpus.saturating_sub(other.cpus),
            memory: self.memory.saturating_sub(other.memory),
        }
    }

    /// mbx's per-axis fit: an axis nobody holds takes any request, so an oversize one runs alone.
    pub(crate) fn fits(self, used: Self, cap: Self) -> bool {
        let axis =
            |used: u64, request: u64, cap: u64| used == 0 || used.saturating_add(request) <= cap;
        axis(
            u64::from(used.cpus),
            u64::from(self.cpus),
            u64::from(cap.cpus),
        ) && axis(used.memory, self.memory, cap.memory)
    }
}

/// One request waiting in a pool.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Request {
    /// Arrival order among the waiters: mbx's ticket.
    pub(crate) ticket: u64,
    pub(crate) class: Class,
    pub(crate) priority: Priority,
    /// When it started waiting.
    pub(crate) since: Secs,
    /// Its estimated bottom level: the longest chain of work that waits on it.
    pub(crate) rank: Secs,
    /// How long its holder is expected to hold the whole charge.
    pub(crate) estimate: Secs,
    pub(crate) charge: Charge,
}

/// A live grant: what it holds and when its holder is expected to end.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Lease {
    pub(crate) charge: Charge,
    pub(crate) ends: Secs,
}

/// A start: the request's index in the queue, and what it holds, which a moldable grant makes
/// narrower than the request when fewer cpus are free.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Grant {
    pub(crate) index: usize,
    pub(crate) charge: Charge,
}

/// A pool's decision rule.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Rule {
    Mbx,
    List(List),
    Easy(Easy),
    Moldable(Moldable),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Easy {
    pub(crate) order: Order,
    /// Reserve for the first request that does not fit only once it has waited this long; before,
    /// any request that fits starts. `None` reserves at once, as EASY does.
    pub(crate) aging: Option<Secs>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Moldable {
    pub(crate) order: Order,
    /// From this wait on, a request goes before fresher ones of its class, starts on a single free
    /// cpu, and holds every request behind it while it cannot start.
    pub(crate) aging: Secs,
    /// Start on any free cpu at once, rather than on half of its want until it has aged.
    pub(crate) greedy: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct List {
    pub(crate) order: Order,
    /// Start a later request that fits while an earlier one waits.
    pub(crate) backfill: bool,
    /// A request that waited this long goes before every fresher one, and nothing fresher starts
    /// past it until it fits: the starvation bound.
    pub(crate) aging: Option<Secs>,
}

/// The priority among requests that have not aged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Order {
    /// Arrival order.
    Fifo,
    /// Class, then arrival.
    Class,
    /// Class, then the highest bottom level, then arrival.
    ClassCriticalPath,
}

/// The requests of `queue` that start now and what each holds, given the live grants and the
/// pool's capacity.
pub(crate) fn admit(
    rule: Rule,
    queue: &[Request],
    live: &[Lease],
    cap: Charge,
    now: Secs,
) -> Vec<Grant> {
    let whole = |index: usize| Grant {
        index,
        charge: queue[index].charge,
    };
    match rule {
        Rule::Mbx => {
            let charges: Vec<Charge> = live.iter().map(|lease| lease.charge).collect();
            mbx(queue, &charges, cap).into_iter().map(whole).collect()
        }
        Rule::List(list) => listed(list, queue, live, cap, now)
            .into_iter()
            .map(whole)
            .collect(),
        Rule::Easy(easy) => easy_backfill(easy, queue, live, cap, now),
        Rule::Moldable(moldable) => molded(moldable, queue, live, cap, now),
    }
}

/// Whether a request has waited `bound` by `now`.
pub(crate) fn aged(bound: Option<Secs>, request: &Request, now: Secs) -> bool {
    bound.is_some_and(|bound| now - request.since >= bound)
}

fn total(live: &[Lease]) -> Charge {
    live.iter()
        .fold(Charge::default(), |sum, lease| sum.plus(lease.charge))
}

/// The queue in priority order, each with whether it has aged.
fn ordered(order: Order, aging: Option<Secs>, queue: &[Request], now: Secs) -> Vec<(bool, usize)> {
    let mut ordered: Vec<(bool, usize)> = queue
        .iter()
        .enumerate()
        .map(|(i, r)| (aged(aging, r, now), i))
        .collect();
    ordered.sort_by(|&(a_aged, a), &(b_aged, b)| {
        precedes(order, (a_aged, &queue[a]), (b_aged, &queue[b]))
    });
    ordered
}

fn listed(list: List, queue: &[Request], live: &[Lease], cap: Charge, now: Secs) -> Vec<usize> {
    let mut used = total(live);
    let mut granted = Vec::new();
    for (is_aged, i) in ordered(list.order, list.aging, queue, now) {
        let request = &queue[i];
        if request.charge.fits(used, cap) {
            granted.push(i);
            used = used.plus(request.charge);
        } else if !list.backfill || is_aged {
            break;
        }
    }
    granted
}

/// The earliest the live grants' expected ends make room for `need`, and what is left beside it.
#[derive(Clone, Copy, Debug)]
struct Shadow {
    at: Secs,
    extra: Charge,
}

fn shadow(need: Charge, live: &[Lease], cap: Charge, now: Secs) -> Shadow {
    let mut ends: Vec<Lease> = live.to_vec();
    ends.sort_by(|a, b| a.ends.total_cmp(&b.ends));
    let mut used = total(live);
    for lease in ends {
        used = used.minus(lease.charge);
        if need.fits(used, cap) {
            return Shadow {
                at: lease.ends.max(now),
                extra: cap.minus(used).minus(need),
            };
        }
    }
    Shadow {
        at: Secs::INFINITY,
        extra: Charge::default(),
    }
}

fn easy_backfill(
    easy: Easy,
    queue: &[Request],
    live: &[Lease],
    cap: Charge,
    now: Secs,
) -> Vec<Grant> {
    let mut leases = live.to_vec();
    let mut used = total(live);
    let mut reservation: Option<Shadow> = None;
    let mut grants = Vec::new();
    for (is_aged, i) in ordered(easy.order, easy.aging, queue, now) {
        let request = &queue[i];
        if !request.charge.fits(used, cap) {
            if reservation.is_none() && (easy.aging.is_none() || is_aged) {
                reservation = Some(shadow(request.charge, &leases, cap, now));
            }
            continue;
        }
        if let Some(shadow) = &mut reservation {
            let ends_first = now + request.estimate <= shadow.at;
            let beside = request.charge.cpus <= shadow.extra.cpus
                && request.charge.memory <= shadow.extra.memory;
            if !ends_first && !beside {
                continue;
            }
            if !ends_first {
                shadow.extra = shadow.extra.minus(request.charge);
            }
        }
        grants.push(Grant {
            index: i,
            charge: request.charge,
        });
        used = used.plus(request.charge);
        leases.push(Lease {
            charge: request.charge,
            ends: now + request.estimate,
        });
    }
    grants
}

fn molded(
    moldable: Moldable,
    queue: &[Request],
    live: &[Lease],
    cap: Charge,
    now: Secs,
) -> Vec<Grant> {
    let mut used = total(live);
    let mut grants = Vec::new();
    let mut order = ordered(moldable.order, Some(moldable.aging), queue, now);
    // A stable sort by class keeps the aged-first order within each class.
    if moldable.order != Order::Fifo {
        order.sort_by(|&(_, a), &(_, b)| queue[a].class.cmp(&queue[b].class));
    }
    for (is_aged, i) in order {
        let request = &queue[i];
        let want = request.charge.cpus;
        let charge = if want == 0 {
            request.charge
        } else {
            let free = cap.minus(used);
            let per_cpu = request.charge.memory.div_ceil(u64::from(want));
            let by_memory = free
                .memory
                .checked_div(per_cpu)
                .map_or(u32::MAX, |cpus| u32::try_from(cpus).unwrap_or(u32::MAX));
            let cpus = want.min(free.cpus).min(by_memory);
            let floor = if moldable.greedy || is_aged {
                1
            } else {
                want.div_ceil(2)
            };
            if cpus < floor {
                if is_aged {
                    break;
                }
                continue;
            }
            let memory = u128::from(request.charge.memory) * u128::from(cpus) / u128::from(want);
            Charge {
                cpus,
                memory: u64::try_from(memory).unwrap_or(u64::MAX),
            }
        };
        if charge.fits(used, cap) {
            grants.push(Grant { index: i, charge });
            used = used.plus(charge);
        } else if is_aged {
            break;
        }
    }
    grants
}

fn precedes(
    order: Order,
    (a_aged, a): (bool, &Request),
    (b_aged, b): (bool, &Request),
) -> Ordering {
    b_aged
        .cmp(&a_aged)
        .then_with(|| {
            if a_aged && b_aged {
                a.since.total_cmp(&b.since)
            } else {
                Ordering::Equal
            }
        })
        .then_with(|| match order {
            Order::Fifo => Ordering::Equal,
            Order::Class => a.class.cmp(&b.class),
            Order::ClassCriticalPath => a
                .class
                .cmp(&b.class)
                .then_with(|| b.rank.total_cmp(&a.rank)),
        })
        .then_with(|| a.ticket.cmp(&b.ticket))
}

/// mbx quantizes memory to 16 MiB in its passing rule.
const MEMORY_QUANTUM: u64 = 16 << 20;

fn mbx(queue: &[Request], live: &[Charge], cap: Charge) -> Vec<usize> {
    let mut order: Vec<usize> = (0..queue.len()).collect();
    order.sort_by_key(|&i| queue[i].ticket);
    let yielded = Charge {
        cpus: yield_axis(u64::from(cap.cpus)) as u32,
        memory: yield_axis(cap.memory),
    };
    let normal_waits = queue.iter().any(|r| r.priority == Priority::Normal);
    let mut pass = Pass::new(live, cap);
    let mut granted = Vec::new();
    let mut waiting: Vec<Request> = Vec::new();
    // The older waiters a request must not delay: every one for a `low` request, only the
    // `normal` ones for a `normal` request.
    let mut all = Chain::default();
    let mut normal = Chain::default();
    for i in order {
        let request = queue[i];
        let budget = if request.priority == Priority::Low && normal_waits {
            yielded
        } else {
            cap
        };
        let older = if request.priority == Priority::Normal {
            &normal
        } else {
            &all
        };
        if request.charge.fits(pass.used, budget) && older.admits(request.charge) {
            granted.push(i);
            pass.grant(request.charge);
            all = Chain::over(&pass, waiting.iter());
            normal = Chain::over(
                &pass,
                waiting.iter().filter(|w| w.priority == Priority::Normal),
            );
        } else {
            all.push(&pass, request.charge);
            if request.priority == Priority::Normal {
                normal.push(&pass, request.charge);
            }
            waiting.push(request);
        }
    }
    granted
}

/// What a `low` request sees of an axis while a `normal` one waits: a quarter withheld, at least
/// one unit and never the whole axis.
fn yield_axis(total: u64) -> u64 {
    total - (total / 4).max(1).min(total.saturating_sub(1))
}

/// One mbx pass: the live grants, including those made earlier in this pass.
struct Pass {
    cap: Charge,
    used: Charge,
    cpus: Sums,
    memory: Sums,
}

impl Pass {
    fn new(live: &[Charge], cap: Charge) -> Self {
        let mut pass = Self {
            cap,
            used: Charge::default(),
            cpus: Sums::new(),
            memory: Sums::new(),
        };
        for &charge in live {
            pass.grant(charge);
        }
        pass
    }

    fn grant(&mut self, charge: Charge) {
        self.used = self.used.plus(charge);
        self.cpus.add(u64::from(charge.cpus));
        self.memory.add(charge.memory.div_ceil(MEMORY_QUANTUM));
    }
}

/// The ticket-ordered prefixes of a set of older waiters, as the least slack any prefix keeps
/// once the least set of live grants that could make room for it has ended.
#[derive(Clone, Copy, Debug)]
struct Chain {
    prefix: Charge,
    cpus: u64,
    memory: u64,
}

impl Default for Chain {
    fn default() -> Self {
        Self {
            prefix: Charge::default(),
            cpus: u64::MAX,
            memory: u64::MAX,
        }
    }
}

impl Chain {
    fn over<'r>(pass: &Pass, waiters: impl Iterator<Item = &'r Request>) -> Self {
        let mut chain = Self::default();
        for waiter in waiters {
            chain.push(pass, waiter.charge);
        }
        chain
    }

    fn push(&mut self, pass: &Pass, charge: Charge) {
        self.prefix = self.prefix.plus(charge);
        let free = pass.cap.minus(pass.used);
        let cpus = slack(
            u64::from(free.cpus),
            u64::from(self.prefix.cpus),
            &pass.cpus,
        );
        let memory = slack(
            free.memory / MEMORY_QUANTUM,
            self.prefix.memory.div_ceil(MEMORY_QUANTUM),
            &pass.memory,
        );
        self.cpus = self.cpus.min(cpus);
        self.memory = self.memory.min(memory);
    }

    /// Whether `charge`, started now, fits the slack of every prefix.
    fn admits(&self, charge: Charge) -> bool {
        u64::from(charge.cpus) <= self.cpus
            && (charge.memory == 0 || charge.memory.div_ceil(MEMORY_QUANTUM) <= self.memory)
    }
}

fn slack(free: u64, prefix: u64, sums: &Sums) -> u64 {
    if prefix <= free {
        return free - prefix;
    }
    let threshold = prefix - free;
    sums.least_reaching(threshold)
        .map_or(0, |sum| sum - threshold)
}

/// The subset sums of the live grants on one axis, as a bitset: bit `s` is set when some set of
/// grants holds exactly `s` units.
#[derive(Debug)]
struct Sums {
    bits: Vec<u64>,
    total: u64,
}

impl Sums {
    fn new() -> Self {
        Self {
            bits: vec![1],
            total: 0,
        }
    }

    fn add(&mut self, units: u64) {
        if units == 0 {
            return;
        }
        self.total += units;
        let words = usize::try_from(self.total / 64 + 1).expect("a pool's units fit in memory");
        self.bits.resize(words, 0);
        let shift_words = usize::try_from(units / 64).expect("a pool's units fit in memory");
        let shift_bits = (units % 64) as u32;
        for i in (shift_words..words).rev() {
            let mut moved = self.bits[i - shift_words] << shift_bits;
            if shift_bits > 0 && i > shift_words {
                moved |= self.bits[i - shift_words - 1] >> (64 - shift_bits);
            }
            self.bits[i] |= moved;
        }
    }

    /// The least subset sum that reaches `threshold`, when one does.
    fn least_reaching(&self, threshold: u64) -> Option<u64> {
        if threshold > self.total {
            return None;
        }
        let mut word = usize::try_from(threshold / 64).ok()?;
        let mut bits = self.bits[word] & (u64::MAX << (threshold % 64));
        while bits == 0 {
            word += 1;
            bits = *self.bits.get(word)?;
        }
        Some(word as u64 * 64 + u64::from(bits.trailing_zeros()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: Charge = Charge {
        cpus: 18,
        memory: 116_823_110_390,
    };

    fn request(ticket: u64, cpus: u32) -> Request {
        Request {
            ticket,
            class: Class::Background,
            priority: Priority::Low,
            since: 0.0,
            rank: 0.0,
            estimate: 5.0,
            charge: Charge { cpus, memory: 0 },
        }
    }

    fn cpus(n: u32) -> Charge {
        Charge { cpus: n, memory: 0 }
    }

    /// The indices `admit` starts, every live grant expected to end at 100 s.
    fn starts(rule: Rule, queue: &[Request], live: &[Charge], now: Secs) -> Vec<usize> {
        let live: Vec<Lease> = live
            .iter()
            .map(|&charge| Lease {
                charge,
                ends: 100.0,
            })
            .collect();
        admit(rule, queue, &live, CAP, now)
            .into_iter()
            .map(|grant| grant.index)
            .collect()
    }

    #[test]
    fn a_wide_mbx_head_holds_every_younger_request_when_one_cpu_leases_could_free_its_room() {
        // Ten one-cpu leases leave 8 free; the 17-cpu head needs 9 of them to end, and any 9 do,
        // so a younger one-cpu request has no slack and waits with 8 cpus idle.
        let live = vec![cpus(1); 10];
        let queue = [request(1, 17), request(2, 1), request(3, 1)];
        assert_eq!(starts(Rule::Mbx, &queue, &live, 0.0), Vec::<usize>::new());
        // One 10-cpu lease is the least set that frees 9, leaving 1 to spare: one younger passes.
        assert_eq!(starts(Rule::Mbx, &queue, &[cpus(10)], 0.0), vec![1]);
    }

    #[test]
    fn an_older_low_waiter_never_blocks_a_normal_request_and_low_yields_a_quarter_meanwhile() {
        let mut normal = request(2, 1);
        normal.priority = Priority::Normal;
        let queue = [request(1, 18), normal, request(3, 4)];
        // 14 free: the 18 waits, the normal one passes it, and the low 4 sees 18 - 4 = 14 cpus,
        // of which 4 + 1 are taken, so 5 + 4 > 14 is false and it fits, but it is younger than
        // the low 18 with no slack, so it waits.
        assert_eq!(starts(Rule::Mbx, &queue, &[cpus(4)], 0.0), vec![1]);
        assert_eq!(yield_axis(18), 14);
        assert_eq!(yield_axis(1), 1);
    }

    #[test]
    fn mbx_grants_in_ticket_order_while_requests_fit() {
        let queue = [request(3, 1), request(1, 8), request(2, 9)];
        assert_eq!(starts(Rule::Mbx, &queue, &[cpus(1)], 0.0), vec![1, 2]);
    }

    fn list(order: Order, aging: Option<Secs>) -> Rule {
        Rule::List(List {
            order,
            backfill: true,
            aging,
        })
    }

    #[test]
    fn list_scheduling_backfills_around_a_wide_request_until_it_ages() {
        let live = vec![cpus(1); 10];
        let queue = [request(1, 17), request(2, 1), request(3, 1)];
        assert_eq!(
            starts(list(Order::Fifo, Some(30.0)), &queue, &live, 10.0),
            vec![1, 2]
        );
        // Aged, the head holds everything fresher until 17 are free.
        assert_eq!(
            starts(list(Order::Fifo, Some(30.0)), &queue, &live, 30.0),
            Vec::<usize>::new()
        );
        assert_eq!(
            starts(list(Order::Fifo, Some(30.0)), &queue, &[cpus(1)], 30.0),
            vec![0]
        );
    }

    #[test]
    fn list_scheduling_orders_by_class_then_bottom_level() {
        let mut gate = request(3, 4);
        gate.class = Class::Gate;
        let mut deep = request(2, 4);
        deep.rank = 100.0;
        let queue = [request(1, 4), deep, gate];
        // Room for two: the gate, then the background request with the longest chain behind it.
        assert_eq!(
            starts(
                list(Order::ClassCriticalPath, None),
                &queue,
                &[cpus(10)],
                0.0
            ),
            vec![2, 1]
        );
        assert_eq!(
            starts(list(Order::Class, None), &queue, &[cpus(10)], 0.0),
            vec![2, 0]
        );
    }

    #[test]
    fn a_request_whose_memory_does_not_fit_waits_while_others_start() {
        let mut big = request(1, 1);
        big.charge.memory = 100 << 30;
        let queue = [big, request(2, 1)];
        let live = [Charge {
            cpus: 1,
            memory: 50 << 30,
        }];
        assert_eq!(starts(list(Order::Fifo, None), &queue, &live, 0.0), vec![1]);
    }

    #[test]
    fn easy_backfills_only_what_ends_before_the_wide_request_can_start() {
        // Nine of ten one-cpu leases end at 10 s, the tenth at 100 s: the 17-cpu head fits at 10 s
        // with nothing to spare, so a 5 s request backfills and a 50 s one waits.
        let mut live = vec![
            Lease {
                charge: cpus(1),
                ends: 10.0
            };
            9
        ];
        live.push(Lease {
            charge: cpus(1),
            ends: 100.0,
        });
        let mut long = request(3, 1);
        long.estimate = 50.0;
        let queue = [request(1, 17), request(2, 1), long];
        let easy = |aging| {
            Rule::Easy(Easy {
                order: Order::Fifo,
                aging,
            })
        };
        let indices = |rule, now| -> Vec<usize> {
            admit(rule, &queue, &live, CAP, now)
                .into_iter()
                .map(|grant| grant.index)
                .collect()
        };
        assert_eq!(indices(easy(None), 0.0), vec![1]);
        // With an aging bound, nothing is reserved before the head has waited it.
        assert_eq!(indices(easy(Some(30.0)), 0.0), vec![1, 2]);
        // Once the head has waited the bound, it reserves, and the 50 s request waits again.
        let aged: Vec<Request> = queue
            .iter()
            .map(|r| Request { since: -30.0, ..*r })
            .collect();
        let grants = admit(easy(Some(30.0)), &aged, &live, CAP, 0.0);
        assert_eq!(
            grants.iter().map(|grant| grant.index).collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn a_moldable_request_starts_on_half_its_want_and_on_any_free_cpu_once_aged() {
        let rule = Rule::Moldable(Moldable {
            order: Order::Fifo,
            aging: 30.0,
            greedy: false,
        });
        let queue = [request(1, 17), request(2, 1)];
        let live = vec![
            Lease {
                charge: cpus(1),
                ends: 100.0
            };
            10
        ];
        let grants = |now| admit(rule, &queue, &live, CAP, now);
        // 8 free is under half of 17: the small one starts.
        assert_eq!(
            grants(0.0),
            vec![Grant {
                index: 1,
                charge: cpus(1)
            }]
        );
        // Aged, the wide one takes the 8 and starts.
        assert_eq!(
            grants(30.0),
            vec![Grant {
                index: 0,
                charge: cpus(8)
            }]
        );
        let half = vec![
            Lease {
                charge: cpus(1),
                ends: 100.0
            };
            9
        ];
        assert_eq!(
            admit(rule, &queue, &half, CAP, 0.0),
            vec![Grant {
                index: 0,
                charge: cpus(9)
            }]
        );
    }
    #[test]
    fn subset_sums_find_the_least_set_that_reaches_a_threshold() {
        let mut sums = Sums::new();
        for units in [5, 7, 70] {
            sums.add(units);
        }
        assert_eq!(sums.least_reaching(6), Some(7));
        assert_eq!(sums.least_reaching(8), Some(12));
        assert_eq!(sums.least_reaching(13), Some(70));
        assert_eq!(sums.least_reaching(83), None);
    }
}
