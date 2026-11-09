//! `Ledger` bound to its proven model, `specs/cowshed/laws/disk_lease.bend` (05_gateway.md
//! "Disk-lifecycle lease", its laws under "Disk-lease laws"). The model's laws are proven for every
//! input sequence; this driver shows the code is the model: it runs the emitted model and `Ledger`
//! on the same inputs, the model's enumerated corpus, its seeded draws and the recorded evictions
//! of the gateway's log, and after every input compares the two ledgers (the running phase, its
//! holders, the queue), the effects the input returned and `deadline()`, then evaluates the laws'
//! checks on the code's own ledger.
//!
//! The gateway's `cowshed: disk-lease <class> wait|phase start|done` lines cannot be replayed: they
//! carry no ticket, no share and no time, so which `phase done` ends which grant is undecidable
//! once two leases of one class are in flight, and a lease whose process died leaves a start with no
//! done (6314 `storage phase start` against 6309 `done` in the log). The `disk-lease evicted` lines
//! carry the class, the other class and how long the holder had held, which is all the eviction
//! law reads, so those are the recorded fixture (`laws/evictions-gateway-stderr.jsonl`).
//!
//! The law the kernel refutes is `disk_lease_open.bend`'s `gate_io_priority`: its two tests assert
//! what the code does today, so the repair flips them.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::time::{Duration, Instant};

use cowshed_gateway_types::{DiskClass, PhaseShare};

use super::{Claim, DiskLeaseLimits, Effect, Ledger, Ticket};

mod model;

use model::{Cls, Dl, Fx, Hd, Lg, List, Mt, Op, Ph, Shr, St, Wt};

/// One model tick, as the code's clock sees it, unless a replay says otherwise.
const TICK: Duration = Duration::from_millis(100);

fn items<T: Clone>(list: &Rc<List<T>>) -> Vec<T> {
    let mut out = Vec::new();
    let mut at = list.clone();
    while let List::Con(head, tail) = &*at {
        out.push(head.clone());
        let next = tail.clone();
        at = next;
    }
    out
}

fn list<T>(items: Vec<T>) -> Rc<List<T>> {
    items
        .into_iter()
        .rev()
        .fold(Rc::new(List::Nil), |tail, head| {
            Rc::new(List::Con(head, tail))
        })
}

fn cls(class: DiskClass) -> Cls {
    match class {
        DiskClass::Storage => Cls::CStorage,
        DiskClass::Namespace => Cls::CNamespace,
    }
}

fn class_of(class: &Cls) -> DiskClass {
    match class {
        Cls::CStorage => DiskClass::Storage,
        Cls::CNamespace => DiskClass::Namespace,
    }
}

fn shr(share: PhaseShare) -> Shr {
    match share {
        PhaseShare::Shared => Shr::SShared,
        PhaseShare::Alone => Shr::SAlone,
    }
}

fn share_of(share: &Shr) -> PhaseShare {
    match share {
        Shr::SShared => PhaseShare::Shared,
        Shr::SAlone => PhaseShare::Alone,
    }
}

/// The ticket a claim names: the claims this driver builds are `t<ticket>`.
fn ticket_of(claim: &Claim) -> Ticket {
    claim.command[1..]
        .parse()
        .expect("a claim names its ticket")
}

/// What a run reached, so a green run says which distinctions it exercised.
#[derive(Clone, Copy, Debug, Default)]
struct Seen {
    asks_granted_at_once: u64,
    asks_queued: u64,
    alone_asks: u64,
    /// Waiters granted into the running phase after a holder left.
    refills: u64,
    /// A drained phase handed to a waiting class.
    handovers: u64,
    evictions: u64,
    waiter_leaves: u64,
    holder_leaves: u64,
    unknown_leaves: u64,
    /// Inputs after which the same phase ran with a waiter of the other class waiting.
    held_shut: u64,
    /// Expires after which the other class waited with the phase's holders in place.
    expires_checked: u64,
}

impl Seen {
    fn add(&mut self, other: Self) {
        self.asks_granted_at_once += other.asks_granted_at_once;
        self.asks_queued += other.asks_queued;
        self.alone_asks += other.alone_asks;
        self.refills += other.refills;
        self.handovers += other.handovers;
        self.evictions += other.evictions;
        self.waiter_leaves += other.waiter_leaves;
        self.holder_leaves += other.holder_leaves;
        self.unknown_leaves += other.unknown_leaves;
        self.held_shut += other.held_shut;
        self.expires_checked += other.expires_checked;
    }

    /// Every distinction the laws turn on was reached at least `least` times.
    fn assert_reaches_all(&self, least: u64) {
        let Self {
            asks_granted_at_once,
            asks_queued,
            alone_asks,
            refills,
            handovers,
            evictions,
            waiter_leaves,
            holder_leaves,
            unknown_leaves,
            held_shut,
            expires_checked,
        } = *self;
        for (name, count) in [
            ("asks granted at once", asks_granted_at_once),
            ("asks queued", asks_queued),
            ("alone asks", alone_asks),
            ("refills into a running phase", refills),
            ("phase handovers", handovers),
            ("evictions", evictions),
            ("waiter leaves", waiter_leaves),
            ("holder leaves", holder_leaves),
            (
                "leaves of a ticket the ledger no longer knows",
                unknown_leaves,
            ),
            ("inputs held shut by a waiting class", held_shut),
            ("expires with the other class waiting", expires_checked),
        ] {
            assert!(
                count >= least,
                "the run reached `{name}` {count} times, fewer than {least}: {self:?}"
            );
        }
    }
}

/// One ledger fed to the model and to `Ledger` alike.
struct Pair {
    tick: Duration,
    model: St,
    ledger: Ledger,
    origin: Instant,
    now: u64,
    last: u64,
    /// The class each ticket asked: `Holder` keeps none, the phase's class stands for it.
    classes: HashMap<Ticket, DiskClass>,
    effects: Vec<Effect>,
    inputs: Vec<String>,
    seen: Seen,
}

impl Pair {
    /// A ledger of `c0 + 1` places and a bound of `b0 + 1` ticks (the model's `empty`).
    fn new(c0: u64, b0: u64) -> Self {
        Self::ticking(TICK, c0, b0)
    }

    fn ticking(tick: Duration, c0: u64, b0: u64) -> Self {
        let limits = DiskLeaseLimits {
            per_class: NonZeroUsize::new(usize::try_from(c0 + 1).expect("cap fits")).unwrap(),
            phase_bound: tick * u32::try_from(b0 + 1).expect("bound fits"),
        };
        Self {
            tick,
            model: model::empty(c0, b0),
            ledger: Ledger::new(limits),
            origin: Instant::now(),
            now: 0,
            last: 0,
            classes: HashMap::new(),
            effects: Vec::new(),
            inputs: Vec::new(),
            seen: Seen::default(),
        }
    }

    fn at(&self, tick: u64) -> Instant {
        self.origin + self.tick * u32::try_from(tick).expect("tick fits")
    }

    /// How many ticks a duration the pair's inputs produced lasts.
    fn ticks(&self, duration: Duration) -> u64 {
        assert_eq!(
            duration.as_nanos() % self.tick.as_nanos(),
            0,
            "{duration:?} is not whole ticks"
        );
        u64::try_from(duration.as_nanos() / self.tick.as_nanos()).expect("ticks fit")
    }

    fn tick_of(&self, instant: Instant) -> u64 {
        self.ticks(instant.duration_since(self.origin))
    }

    /// The code's ledger as the model's value: cap, bound, the running phase with its holders
    /// newest first (tickets rise with grants), and the queue oldest first.
    fn code_lg(&self) -> Lg {
        let cap = self.ledger.limits.per_class.get() as u64;
        let bound = self.ticks(self.ledger.limits.phase_bound);
        let phase = match &self.ledger.phase {
            None => Ph::PhIdle,
            Some(phase) => {
                let mut holders: Vec<_> = phase.holders.iter().collect();
                holders.sort_by_key(|holder| std::cmp::Reverse(holder.ticket));
                Ph::PhRun(
                    cls(phase.class),
                    Mt::Mt(
                        self.tick_of(phase.started),
                        phase.members as u64,
                        phase.evicted as u64,
                    ),
                    list(
                        holders
                            .into_iter()
                            .map(|holder| {
                                Hd::Hd(
                                    holder.ticket,
                                    cls(self.classes[&holder.ticket]),
                                    shr(holder.share),
                                    self.tick_of(holder.granted),
                                )
                            })
                            .collect(),
                    ),
                )
            }
        };
        let waiting = list(
            self.ledger
                .waiting
                .iter()
                .map(|waiter| Wt::Wt(waiter.ticket, cls(waiter.class), shr(waiter.share)))
                .collect(),
        );
        Lg::Lg(cap, bound, phase, waiting)
    }

    /// What the last input returned, as the model's effects. The holders a pass evicts are listed
    /// in the order the phase happens to keep them, which the code does not promise, so each
    /// pass's evictions are ordered by ticket on both sides.
    fn code_fx(&self) -> Vec<Fx> {
        order(
            self.effects
                .iter()
                .map(|effect| match effect {
                    Effect::Grant(ticket) => Fx::FxGrant(*ticket),
                    Effect::Evict {
                        class,
                        claim,
                        held,
                        waiting,
                    } => Fx::FxEvict(
                        ticket_of(claim),
                        cls(*class),
                        self.ticks(*held),
                        cls(*waiting),
                    ),
                    Effect::PhaseEnded {
                        class,
                        elapsed,
                        members,
                        evicted,
                    } => Fx::FxEnd(
                        cls(*class),
                        self.ticks(*elapsed),
                        *members as u64,
                        *evicted as u64,
                    ),
                })
                .collect(),
        )
    }

    fn code_deadline(&self) -> Dl {
        self.ledger
            .deadline()
            .map_or(Dl::DlNone, |at| Dl::DlAt(self.tick_of(at)))
    }

    fn refused(&self, what: &str) -> String {
        format!("{what}, after the inputs [{}]", self.inputs.join(", "))
    }

    /// Let `ticks` pass: the model's clock and the driver's, which `Ledger` reads at the next input.
    fn advance(&mut self, ticks: u64) {
        self.inputs.push(format!("DTick x{ticks}"));
        for _ in 0..ticks {
            self.model = model::step(Op::DTick, self.model.clone());
        }
        self.now += ticks;
        self.effects.clear();
    }

    /// Feed `op` to both; then hold them to the same ledger, the same effects and the same deadline,
    /// and the code's own ledger to the laws.
    fn input(&mut self, op: &Op) -> Result<(), String> {
        self.inputs.push(model::show_Op(op));
        self.effects.clear();
        let before = self.code_lg();
        let now = self.at(self.now);
        match op {
            Op::DAsk(class, share) => {
                self.last += 1;
                let ticket = self.last;
                let class = class_of(class);
                self.classes.insert(ticket, class);
                if matches!(share, Shr::SAlone) {
                    self.seen.alone_asks += 1;
                }
                self.ledger.ask(
                    now,
                    ticket,
                    class,
                    share_of(share),
                    Claim::new(&format!("t{ticket}"), Some(7)),
                    &mut self.effects,
                );
                if self.effects.is_empty() {
                    self.seen.asks_queued += 1;
                } else {
                    self.seen.asks_granted_at_once += 1;
                }
            }
            Op::DLeave(back) => {
                let ticket = self.last.saturating_sub(*back);
                let waits = self.ledger.waiting.iter().any(|w| w.ticket == ticket);
                let holds = self
                    .ledger
                    .phase
                    .as_ref()
                    .is_some_and(|p| p.holders.iter().any(|h| h.ticket == ticket));
                match (waits, holds) {
                    (true, _) => self.seen.waiter_leaves += 1,
                    (false, true) => self.seen.holder_leaves += 1,
                    (false, false) => self.seen.unknown_leaves += 1,
                }
                self.ledger.leave(now, ticket, &mut self.effects);
                self.count_handover();
            }
            Op::DExpire => {
                self.ledger.expire(now, &mut self.effects);
                self.seen.evictions += self
                    .effects
                    .iter()
                    .filter(|e| matches!(e, Effect::Evict { .. }))
                    .count() as u64;
                self.count_handover();
            }
            Op::DTick => {
                self.now += 1;
            }
        }
        self.model = model::step(op.clone(), self.model.clone());
        let after = self.code_lg();
        let model_lg = model::lg(self.model.clone());
        if model_lg != after {
            return Err(self.refused(&format!(
                "the ledgers differ: model {}, Ledger {}",
                model::show_Lg(&model_lg),
                model::show_Lg(&after)
            )));
        }
        let (model_fx, code_fx) = (order(items(&model::fx(self.model.clone()))), self.code_fx());
        if model_fx != code_fx {
            return Err(self.refused(&format!(
                "the effects differ: model {}, Ledger {}",
                model::show_ListFx(&list(model_fx)),
                model::show_ListFx(&list(code_fx))
            )));
        }
        if model::last(self.model.clone()) != self.last
            || model::now(self.model.clone()) != self.now
        {
            return Err(
                self.refused("the model's ticket or tick counter drifted from the driver's")
            );
        }
        let (model_deadline, code_deadline) = (model::deadline(model_lg), self.code_deadline());
        if model_deadline != code_deadline {
            return Err(self.refused(&format!(
                "deadline differs: model {}, Ledger {}",
                model::show_Dl(&model_deadline),
                model::show_Dl(&code_deadline)
            )));
        }
        self.check_laws(op, &before, &after)
    }

    /// A grant in a step that ended no phase went into the running phase; one after a phase ended
    /// is a handover.
    fn count_handover(&mut self) {
        let ended = self
            .effects
            .iter()
            .any(|e| matches!(e, Effect::PhaseEnded { .. }));
        let grants = self
            .effects
            .iter()
            .filter(|e| matches!(e, Effect::Grant(_)))
            .count() as u64;
        if ended && grants > 0 {
            self.seen.handovers += 1;
        } else {
            self.seen.refills += grants;
        }
    }

    /// The laws' checks on the code's own ledger: each is a function the proven laws state.
    fn check_laws(&mut self, op: &Op, before: &Lg, after: &Lg) -> Result<(), String> {
        let fails = |law: &str| {
            self.refused(&format!(
                "{law} fails on Ledger's ledger {}",
                model::show_Lg(after)
            ))
        };
        if !model::excl_b(after.clone()) {
            return Err(fails("exclusion"));
        }
        if !model::nowait_b(after.clone()) {
            return Err(fails("no_phase_no_wait"));
        }
        if !model::live_b(after.clone()) {
            return Err(fails("phase_has_holder"));
        }
        if !model::chain_b(model::waiting_of(after.clone()), 0, self.last) {
            return Err(fails("fifo"));
        }
        if !model::hok_ph(after.clone(), self.last) {
            return Err(fails("no_overtaking"));
        }
        if !model::starve_b(before.clone(), after.clone()) {
            return Err(self.refused(&format!(
                "no_starvation fails: {} became {}",
                model::show_Lg(before),
                model::show_Lg(after)
            )));
        }
        if matches!(op, Op::DExpire) && !model::young_after(after.clone(), self.now) {
            return Err(fails("bounded_hold"));
        }
        if let (
            Lg::Lg(_, _, Ph::PhRun(class, Mt::Mt(started, ..), _), _),
            Lg::Lg(_, _, Ph::PhRun(class_after, Mt::Mt(started_after, ..), _), waiting),
        ) = (before, after)
            && class == class_after
            && started == started_after
            && items(waiting)
                .iter()
                .any(|Wt::Wt(_, waiter, _)| *waiter != *class)
        {
            self.seen.held_shut += 1;
            if matches!(op, Op::DExpire) {
                self.seen.expires_checked += 1;
            }
        }
        Ok(())
    }

    fn gate_ok(&self, gates: &[u64]) -> bool {
        model::gate_ok_b(self.code_lg(), list(gates.to_vec()))
    }

    fn run(c0: u64, b0: u64, ops: &Rc<List<Op>>) -> Result<Self, String> {
        let mut pair = Self::new(c0, b0);
        items(ops).iter().try_for_each(|op| pair.input(op))?;
        Ok(pair)
    }
}

/// The evictions of a pass in ticket order, everything else as it came.
fn order(mut effects: Vec<Fx>) -> Vec<Fx> {
    let evictions = effects
        .iter()
        .take_while(|effect| matches!(effect, Fx::FxEvict(..)))
        .count();
    effects[..evictions].sort_by_key(|effect| match effect {
        Fx::FxEvict(ticket, ..) => *ticket,
        _ => unreachable!("take_while kept evictions"),
    });
    effects
}

/// Every enumerated input sequence of the model's corpus. Each check is evaluated on the code's own
/// ledger after every input, so the corpus needs no second run of the model's laws on itself.
#[test]
fn the_ledger_is_the_model_on_its_corpus() {
    let mut seen = Seen::default();
    for (c0, b0, ops) in model::corpus_exclusion_check() {
        let pair = Pair::run(c0, b0, &ops)
            .unwrap_or_else(|why| panic!("cap {} bound {}: {why}", c0 + 1, b0 + 1));
        seen.add(pair.seen);
    }
    cowshed_os::eprintln!("corpus reached {seen:?}");
}

/// The model's seeded draws: longer sequences, the same stream every target draws. The runs
/// together reach every distinction the laws turn on, so no check ran vacuously.
#[test]
fn the_ledger_is_the_model_on_seeded_inputs() {
    let mut rng = model::Rng(model::SEED_EXCLUSION_CHECK);
    let mut seen = Seen::default();
    for _ in 0..model::PROPS_EXCLUSION_CHECK {
        let c0 = model::gen_Nat(&mut rng, model::PROP_LEN_EXCLUSION_CHECK);
        let b0 = model::gen_Nat(&mut rng, model::PROP_LEN_EXCLUSION_CHECK);
        let ops = model::gen_ListOp(&mut rng, model::PROP_LEN_EXCLUSION_CHECK);
        let pair = Pair::run(c0, b0, &ops)
            .unwrap_or_else(|why| panic!("cap {} bound {}: {why}", c0 + 1, b0 + 1));
        seen.add(pair.seen);
    }
    cowshed_os::eprintln!("seeded draws reached {seen:?}");
    seen.assert_reaches_all(10);
}

/// The gateway's `disk-lease evicted a <class> holder after <held> while <class> waited` lines,
/// one record each, in log order, from `~/Library/Logs/cowshed/daemon-stderr.log` (the log has no
/// timestamps, so the fixture is every eviction in the file, 2026-08-17 to 2026-11-09). Each
/// record replays as the episode it names, at one tick per second and the default bound of ten
/// ticks: a holder of the class, a waiter of the other, `held` seconds passing, an expire. The
/// ledger must not evict a second before the bound and must evict at `held`, and the model and
/// `Ledger` must agree on the ledger and the effects throughout.
#[test]
fn recorded_evictions_replay_through_the_model_and_ledger() {
    #[derive(serde::Deserialize)]
    struct Eviction {
        class: String,
        waiting: String,
        held_us: u64,
    }
    let class = |name: &str| match name {
        "storage" => Cls::CStorage,
        "namespace" => Cls::CNamespace,
        other => panic!("a recorded class the replay does not know: {other}"),
    };
    let mut records = 0;
    for line in include_str!("laws/evictions-gateway-stderr.jsonl").lines() {
        let Eviction {
            class: holder,
            waiting,
            held_us,
        } = serde_json::from_str(line).expect("a recorded eviction");
        let (holder, waiting) = (class(&holder), class(&waiting));
        assert_ne!(
            model::show_Cls(&holder),
            model::show_Cls(&waiting),
            "{line}"
        );
        let held = held_us / 1_000_000;
        assert!(held >= 10, "an eviction before the bound: {line}");
        // the default cap of eight places and bound of ten seconds, a tick a second
        let mut pair = Pair::ticking(Duration::from_secs(1), 7, 9);
        pair.input(&Op::DAsk(holder.clone(), Shr::SShared)).unwrap();
        pair.input(&Op::DAsk(waiting, Shr::SShared)).unwrap();
        pair.advance(9);
        pair.input(&Op::DExpire).unwrap();
        assert!(pair.effects.is_empty(), "evicted before the bound: {line}");
        pair.advance(held - 9);
        pair.input(&Op::DExpire).unwrap();
        let evicted = pair
            .effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Evict { class, held, .. } => Some((*class, *held)),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no eviction at the recorded hold: {line}"));
        assert_eq!(
            evicted,
            (class_of(&holder), Duration::from_secs(held)),
            "{line}"
        );
        assert!(
            pair.effects.iter().any(|e| matches!(e, Effect::Grant(2))),
            "the waiter was not granted when its holder was evicted: {line}"
        );
        records += 1;
    }
    assert_eq!(records, 1194, "the fixture holds the evictions of the log");
}

/// The trace the open law `gate_io_priority` is refuted by (disk_lease.bend proves it as
/// `gate_io_priority_refuted`): a background namespace lease holds the ledger and the gate's storage
/// lease, ticket 2, waits behind it. `Ledger::ask` takes a class, a share and a claim, nothing says
/// whose lease it is, so the repair (GATEWAY-IO-PRIORITY) flips this test.
#[test]
fn a_gates_lease_waits_behind_a_background_lease() {
    let ops = list(vec![
        Op::DAsk(Cls::CNamespace, Shr::SShared),
        Op::DAsk(Cls::CStorage, Shr::SShared),
    ]);
    let pair = Pair::run(0, 0, &ops).unwrap();
    assert!(
        !pair.gate_ok(&[2]),
        "the gate's lease, ticket 2, was granted or never waited"
    );
    assert!(
        pair.ledger.waiting.iter().any(|waiter| waiter.ticket == 2),
        "ticket 2 waits"
    );
    assert!(
        pair.gate_ok(&[1, 2]),
        "behind a gate's own lease it is no violation"
    );
    // across classes the next phase goes to the other class, ahead of an older waiter: tickets 1 and
    // 2 are gates' leases and 3 a background one, and when the first holder leaves the older gate's
    // lease, ticket 2, waits behind the younger background lease, ticket 3
    let ops = list(vec![
        Op::DAsk(Cls::CNamespace, Shr::SAlone),
        Op::DAsk(Cls::CNamespace, Shr::SShared),
        Op::DAsk(Cls::CStorage, Shr::SAlone),
        Op::DLeave(2),
    ]);
    let pair = Pair::run(1, 0, &ops).unwrap();
    assert!(
        !pair.gate_ok(&[1, 2]),
        "the older gate's lease waits behind a younger background one"
    );
    assert!(
        pair.ledger
            .phase
            .as_ref()
            .is_some_and(|phase| phase.holders.iter().any(|holder| holder.ticket == 3)),
        "the younger background lease, ticket 3, holds"
    );
}

/// The seeded draws that refute `gate_io_priority` shrink, one input dropped at a time, to the two
/// asks above. Draws are (cap, bound, inputs, the tickets that are gates').
#[test]
fn the_seeded_counterexamples_to_gate_io_priority_shrink_to_two_asks() {
    /// An input and, for an ask, whether it is a gate's lease.
    type Labelled = (Op, bool);
    fn violates(c0: u64, b0: u64, inputs: &[Labelled]) -> bool {
        let mut pair = Pair::new(c0, b0);
        let mut gates = Vec::new();
        let mut asked = 0;
        for (op, gate) in inputs {
            if matches!(op, Op::DAsk(..)) {
                asked += 1;
                if *gate {
                    gates.push(asked);
                }
            }
            pair.input(op).unwrap();
        }
        !pair.gate_ok(&gates)
    }
    let mut rng = model::Rng(model::SEED_EXCLUSION_CHECK);
    let (mut draws, mut refuted) = (0, 0);
    let mut first = None;
    for _ in 0..model::PROPS_EXCLUSION_CHECK {
        let c0 = model::gen_Nat(&mut rng, model::PROP_LEN_EXCLUSION_CHECK);
        let b0 = model::gen_Nat(&mut rng, model::PROP_LEN_EXCLUSION_CHECK);
        let ops = items(&model::gen_ListOp(
            &mut rng,
            model::PROP_LEN_EXCLUSION_CHECK,
        ));
        let gates = items(&model::gen_ListNat(
            &mut rng,
            model::PROP_LEN_EXCLUSION_CHECK,
        ));
        let mut asked = 0;
        let labelled: Vec<Labelled> = ops
            .into_iter()
            .map(|op| {
                let gate = matches!(op, Op::DAsk(..)) && {
                    asked += 1;
                    gates.contains(&asked)
                };
                (op, gate)
            })
            .collect();
        draws += 1;
        if violates(c0, b0, &labelled) {
            refuted += 1;
            first.get_or_insert((c0, b0, labelled));
        }
    }
    let (c0, b0, mut inputs) = first.expect("a seeded draw refutes gate_io_priority");
    cowshed_os::eprintln!("{refuted} of {draws} seeded draws refute gate_io_priority");
    // each step drops one input or turns one gate's lease into a background one, while the ledger
    // still makes a gate's lease wait behind a background lease
    while let Some(smaller) = (0..inputs.len())
        .map(|skip| {
            let mut smaller = inputs.clone();
            smaller.remove(skip);
            smaller
        })
        .chain((0..inputs.len()).filter(|at| inputs[*at].1).map(|at| {
            let mut smaller = inputs.clone();
            smaller[at].1 = false;
            smaller
        }))
        .find(|candidate| violates(c0, b0, candidate))
    {
        inputs = smaller;
    }
    let shown = inputs
        .iter()
        .map(|(op, gate)| {
            format!(
                "{}{}",
                model::show_Op(op),
                if *gate { " (gate)" } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(inputs.len(), 2, "shrunk to [{shown}]");
    assert!(
        inputs.iter().all(|(op, _)| matches!(op, Op::DAsk(..))) && inputs[1].1,
        "two asks, the second a gate's: [{shown}]"
    );
}
