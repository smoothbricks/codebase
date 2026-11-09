//! `Ledger` bound to its proven model, `specs/cowshed/laws/task_server.bend` (05_gateway.md "The
//! task server schedules work units", its laws under "Task-server laws"). The model's laws are
//! proven for every input sequence; this driver shows the code is the model: it runs the emitted
//! model and `Ledger` on the same inputs, the model's enumerated corpus and its seeded draws, and
//! after every input compares the two queues, the ready orders, the holders, the units a pass
//! grants, and evaluates the laws' checks on the code's own state. No gateway stderr log holds a
//! `cowshed: timing task-server select` line and no in-repo caller sends a `task-admit`, so there
//! is no recorded fixture to replay. Plain cases follow the drivers: a land is ordered behind the
//! aged waiter it queues behind, and a request that omits its class is background.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cowshed_gateway_types::host_status::{DarwinPressureLevel, MemoryPressure};
use cowshed_gateway_types::{LeaseClass, TaskAdmitRequest, TaskClass, TaskOutcome};

use super::{Effect, Ledger, LeverageObservation, State, TaskFacts, TaskServerLimits};

mod model;

use model::{Cls, Dl, Hold, List, Ls, Op, St, U, Wire};

const NORMAL: MemoryPressure = MemoryPressure::Darwin {
    level: DarwinPressureLevel::Normal,
};

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

fn class_of(cls: &Cls) -> TaskClass {
    match cls {
        Cls::CGate => TaskClass::Gate,
        Cls::CInter => TaskClass::Interactive,
        Cls::CBack => TaskClass::Background,
    }
}

fn cls_of(class: TaskClass) -> Cls {
    match class {
        TaskClass::Gate => Cls::CGate,
        TaskClass::Interactive => Cls::CInter,
        TaskClass::Background => Cls::CBack,
    }
}

fn leases_of(ls: &Ls) -> Vec<LeaseClass> {
    let Ls::Ls(cowshed_new, image, land) = ls;
    [
        (*cowshed_new, LeaseClass::CowshedNew),
        (*image, LeaseClass::Image),
        (*land, LeaseClass::LandPublication),
    ]
    .into_iter()
    .filter_map(|(held, class)| held.then_some(class))
    .collect()
}

fn ls_of(leases: &[LeaseClass]) -> Ls {
    Ls::Ls(
        leases.contains(&LeaseClass::CowshedNew),
        leases.contains(&LeaseClass::Image),
        leases.contains(&LeaseClass::LandPublication),
    )
}

/// The unit the model calls `id`, and the units that depend on it so that its leverage is what the
/// model says: the code counts the live units that depend on a unit, the model carries the count.
fn unit_name(id: u64) -> String {
    format!("u{id}")
}

fn dependent_name(id: u64, index: u64) -> String {
    format!("d{id}-{index}")
}

fn real_id(name: &str) -> Option<u64> {
    name.strip_prefix('u')?.parse().ok()
}

fn facts(
    name: String,
    class: TaskClass,
    urgent: bool,
    deadline: Option<u64>,
    leases: Vec<LeaseClass>,
    deps: Vec<Arc<str>>,
) -> TaskFacts {
    TaskFacts {
        unit: Arc::from(name),
        owner: Arc::from("owner"),
        incarnation: None,
        deps,
        leases,
        priority: 0,
        urgent,
        class,
        deadline,
        caps: None,
        command: Arc::from("nx run"),
        pid: None,
    }
}

fn without_entry(unit: &U) -> U {
    let U::U(id, cls, urgent, leverage, deadline, ls, admitted, _) = unit.clone();
    U::U(id, cls, urgent, leverage, deadline, ls, admitted, 0)
}

/// One schedule fed to the model and to `Ledger` alike.
struct Pair {
    model: St,
    ledger: Ledger,
    origin: Instant,
    now_ms: u64,
    bound_ms: u64,
    ticket: u64,
    /// How many dependents each live unit was admitted with.
    dependents: HashMap<u64, u64>,
    inputs: Vec<String>,
}

impl Pair {
    fn new(bound: u64) -> Self {
        Self {
            model: model::empty(bound),
            ledger: Ledger::new(TaskServerLimits {
                age_bound: Duration::from_millis(bound),
                ..TaskServerLimits::default()
            }),
            origin: Instant::now(),
            now_ms: 0,
            bound_ms: bound,
            ticket: 0,
            dependents: HashMap::new(),
            inputs: Vec::new(),
        }
    }

    fn at(&self) -> Instant {
        self.origin + Duration::from_millis(self.now_ms)
    }

    /// The code's queued units as the model's values, in admission order: each one's number, class,
    /// urgent override, leverage as the code counts it, deadline, leases, admission time and entry.
    fn code_units(&self) -> Vec<U> {
        let leverage = self.ledger.leverage();
        let mut queued: Vec<(u64, U)> = self
            .ledger
            .units
            .values()
            .filter(|unit| matches!(unit.state, State::Queued))
            .filter_map(|unit| {
                let id = real_id(&unit.facts.unit)?;
                let waited = unit.admitted.duration_since(self.origin).as_millis();
                Some((
                    unit.entry,
                    U::U(
                        id,
                        cls_of(unit.facts.class),
                        unit.facts.urgent,
                        u64::try_from(leverage.get(&unit.facts.unit).copied().unwrap_or(0))
                            .expect("a leverage fits a Nat"),
                        unit.facts.deadline.map_or(Dl::DNone, Dl::DAt),
                        ls_of(&unit.facts.leases),
                        u64::try_from(waited).expect("an admission time fits a Nat"),
                        unit.entry,
                    ),
                ))
            })
            .collect();
        queued.sort_by_key(|(entry, _)| *entry);
        queued.into_iter().map(|(_, unit)| unit).collect()
    }

    fn code_selected(&self) -> Vec<u64> {
        let mut selected: Vec<u64> = self
            .ledger
            .units
            .values()
            .filter(|unit| matches!(unit.state, State::Selected { .. }))
            .filter_map(|unit| real_id(&unit.facts.unit))
            .collect();
        selected.sort_unstable();
        selected
    }

    /// Who holds `class`, newest first as the model lists them.
    fn code_holders(&self, class: LeaseClass) -> Vec<u64> {
        self.ledger
            .holders
            .get(&class)
            .map(|held| {
                held.iter()
                    .filter_map(|ticket| {
                        let unit = self.ledger.unit_of(*ticket)?;
                        real_id(&unit.facts.unit)
                    })
                    .rev()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The code's ready order as the unit numbers the model orders.
    fn code_ready(&self) -> Vec<u64> {
        let mut observation = LeverageObservation::new();
        self.ledger
            .ready_order(self.at(), &mut observation)
            .into_iter()
            .filter_map(|ticket| {
                let unit = self.ledger.unit_of(ticket)?;
                real_id(&unit.facts.unit)
            })
            .collect()
    }

    /// The code's whole state as the model's value, for the laws' checks to read.
    fn code_state(&self) -> St {
        St::St(
            list(self.code_units()),
            list(self.code_selected()),
            Hold::Hold(
                list(self.code_holders(LeaseClass::CowshedNew)),
                list(self.code_holders(LeaseClass::Image)),
                list(self.code_holders(LeaseClass::LandPublication)),
            ),
            self.now_ms,
            self.bound_ms,
            self.ledger.next_entry,
            list(Vec::new()),
        )
    }

    /// Feed `op` to both; then hold them to the same queue, ready order, holders and grants, and
    /// the laws' checks on the code's state.
    fn input(&mut self, op: &Op) -> Result<(), String> {
        self.inputs.push(model::show_Op(op));
        let granted = self.feed(op);
        self.model = model::step(op.clone(), self.model.clone());
        if let Some(code_granted) = granted {
            let model_granted = items(&model::granted(self.model.clone()));
            if model_granted != code_granted {
                return Err(self.refused(&format!(
                    "a pass grants {model_granted:?} in the model and {code_granted:?} in Ledger"
                )));
            }
        }
        let model_queue: Vec<U> = items(&model::queue(self.model.clone()))
            .iter()
            .map(without_entry)
            .collect();
        let code_queue: Vec<U> = self.code_units().iter().map(without_entry).collect();
        if model_queue != code_queue {
            return Err(self.refused(&format!(
                "the queues differ: model {}, Ledger {}",
                model::show_ListU(&list(model_queue)),
                model::show_ListU(&list(code_queue))
            )));
        }
        let (model_ready, code_ready) =
            (items(&model::ready(self.model.clone())), self.code_ready());
        if model_ready != code_ready {
            return Err(self.refused(&format!(
                "the ready orders differ: model {model_ready:?}, Ledger {code_ready:?}"
            )));
        }
        let mut model_selected = items(&model::selected(self.model.clone()));
        model_selected.sort_unstable();
        if model_selected != self.code_selected() {
            return Err(self.refused(&format!(
                "the running units differ: model {model_selected:?}, Ledger {:?}",
                self.code_selected()
            )));
        }
        let code_state = self.code_state();
        if model::show_Hold(&model::hold_of(self.model.clone()))
            != model::show_Hold(&model::hold_of(code_state.clone()))
        {
            return Err(self.refused(&format!(
                "the holders differ: model {}, Ledger {}",
                model::show_Hold(&model::hold_of(self.model.clone())),
                model::show_Hold(&model::hold_of(code_state))
            )));
        }
        self.laws_hold(op)
    }

    /// Perform `op` on `Ledger`; a pass answers the units it granted, in order.
    fn feed(&mut self, op: &Op) -> Option<Vec<u64>> {
        let now = self.at();
        match op {
            Op::OAdmit(id, cls, urgent, leverage, deadline, ls) => {
                self.ticket += 1;
                let named = facts(
                    unit_name(*id),
                    class_of(cls),
                    *urgent,
                    match deadline {
                        Dl::DNone => None,
                        Dl::DAt(at) => Some(*at),
                    },
                    leases_of(ls),
                    Vec::new(),
                );
                if self.ledger.admit(now, self.ticket, named).is_ok() {
                    for index in 0..*leverage {
                        self.ticket += 1;
                        let dependent = facts(
                            dependent_name(*id, index),
                            TaskClass::Background,
                            false,
                            None,
                            Vec::new(),
                            vec![Arc::from(unit_name(*id))],
                        );
                        self.ledger
                            .admit(now, self.ticket, dependent)
                            .expect("a dependent of a live unit is admitted");
                    }
                    self.dependents.insert(*id, *leverage);
                }
                None
            }
            Op::OTick(by) => {
                self.now_ms += by;
                None
            }
            Op::OSelect => {
                let mut effects = Vec::new();
                let mut observation = LeverageObservation::new();
                self.ledger
                    .select(now, &mut observation, || Ok(NORMAL), &mut effects);
                Some(
                    effects
                        .iter()
                        .filter_map(|effect| match effect {
                            Effect::Select { ticket, .. } => {
                                real_id(&self.ledger.unit_of(*ticket)?.facts.unit)
                            }
                            Effect::Release { .. } => None,
                        })
                        .collect(),
                )
            }
            Op::ODone(id) => {
                let mut effects = Vec::new();
                if self
                    .ledger
                    .terminal(&unit_name(*id), TaskOutcome::Failed, &mut effects)
                    .is_ok()
                {
                    for index in 0..self.dependents.remove(id).unwrap_or(0) {
                        self.ledger
                            .terminal(
                                &dependent_name(*id, index),
                                TaskOutcome::Failed,
                                &mut effects,
                            )
                            .expect("a dependent of a unit is live until the unit is done");
                    }
                }
                None
            }
        }
    }

    /// The laws' checks, evaluated on the code's own state.
    fn laws_hold(&self, op: &Op) -> Result<(), String> {
        let state = self.code_state();
        for (law, holds) in [
            ("lease_exclusion", model::holds_ok_b(state.clone())),
            ("aged_first", model::aged_first_b(state.clone())),
            ("gate_first", model::gate_first_b(state.clone())),
            ("no_starve", model::no_starve_b(state.clone())),
            (
                "lands_behind_older_aged",
                model::lands_behind_b(state.clone()),
            ),
            ("grants_when_free", model::grants_b(state.clone())),
        ] {
            if !holds {
                return Err(self.refused(&format!(
                    "{law} fails on Ledger's state {}",
                    model::show_St(&state)
                )));
            }
        }
        if let Op::ODone(id) = op
            && model::held(*id, model::hold_of(state.clone()))
        {
            return Err(self.refused(&format!(
                "done_frees fails: unit {id} still holds a place in {}",
                model::show_St(&state)
            )));
        }
        Ok(())
    }

    fn refused(&self, what: &str) -> String {
        format!(
            "{what}, after the inputs [{}] with age bound {}",
            self.inputs.join(", "),
            self.bound_ms
        )
    }

    fn run(bound: u64, ops: &Rc<List<Op>>) -> Result<Self, String> {
        let mut pair = Self::new(bound);
        for op in items(ops) {
            pair.input(&op)?;
        }
        Ok(pair)
    }

    /// Whether Ledger's ready order keeps every land behind the aged units admitted before it.
    fn lands_behind_older_aged(&self) -> bool {
        model::lands_behind_b(self.code_state())
    }
}

type Seeded = (u64, u64, u64);

/// Each proven law's seed, draw count and draw length: the same streams every target draws.
const SEEDED: [Seeded; 6] = [
    (
        model::SEED_LEASE_EXCLUSION,
        model::PROPS_LEASE_EXCLUSION,
        model::PROP_LEN_LEASE_EXCLUSION,
    ),
    (
        model::SEED_AGED_FIRST,
        model::PROPS_AGED_FIRST,
        model::PROP_LEN_AGED_FIRST,
    ),
    (
        model::SEED_GATE_FIRST,
        model::PROPS_GATE_FIRST,
        model::PROP_LEN_GATE_FIRST,
    ),
    (
        model::SEED_NO_STARVE,
        model::PROPS_NO_STARVE,
        model::PROP_LEN_NO_STARVE,
    ),
    (
        model::SEED_LANDS_BEHIND_OLDER_AGED,
        model::PROPS_LANDS_BEHIND_OLDER_AGED,
        model::PROP_LEN_LANDS_BEHIND_OLDER_AGED,
    ),
    (
        model::SEED_GRANTS_WHEN_FREE,
        model::PROPS_GRANTS_WHEN_FREE,
        model::PROP_LEN_GRANTS_WHEN_FREE,
    ),
];

/// The model's own law checks on one input sequence.
fn model_laws_hold(ops: &Rc<List<Op>>, bound: u64) -> Result<(), String> {
    for (law, (holds, value)) in [
        (
            "lease_exclusion",
            model::law_lease_exclusion(ops.clone(), bound),
        ),
        ("aged_first", model::law_aged_first(ops.clone(), bound)),
        ("gate_first", model::law_gate_first(ops.clone(), bound)),
        ("no_starve", model::law_no_starve(ops.clone(), bound)),
        (
            "lands_behind_older_aged",
            model::law_lands_behind_older_aged(ops.clone(), bound),
        ),
        (
            "grants_when_free",
            model::law_grants_when_free(ops.clone(), bound),
        ),
    ] {
        if !holds {
            return Err(format!(
                "the emitted model breaks {law} on {} with age bound {bound}: {value}",
                model::show_ListOp(ops)
            ));
        }
    }
    Ok(())
}

/// Every enumerated input sequence of the model's corpus, and the model's own law checks on it.
#[test]
fn ledger_is_the_model_on_its_corpus() {
    for (ops, bound) in model::corpus_lease_exclusion() {
        Pair::run(bound, &ops).unwrap();
        model_laws_hold(&ops, bound).unwrap();
    }
    for (ops, bound, id) in model::corpus_done_frees() {
        Pair::run(bound, &ops).unwrap();
        let (holds, value) = model::law_done_frees(ops.clone(), bound, id);
        assert!(
            holds,
            "the emitted model breaks done_frees on {} with age bound {bound}, unit {id}: {value}",
            model::show_ListOp(&ops)
        );
    }
}

/// The model's seeded draws: longer schedules, the same stream every target draws.
#[test]
fn ledger_is_the_model_on_seeded_inputs() {
    for (seed, props, len) in SEEDED {
        let mut rng = model::Rng(seed);
        for _ in 0..props {
            let ops = model::gen_ListOp(&mut rng, len);
            let bound = model::gen_Nat(&mut rng, len);
            Pair::run(bound, &ops).unwrap();
            model_laws_hold(&ops, bound).unwrap();
        }
    }
    let mut rng = model::Rng(model::SEED_DONE_FREES);
    for _ in 0..model::PROPS_DONE_FREES {
        let ops = model::gen_ListOp(&mut rng, model::PROP_LEN_DONE_FREES);
        let bound = model::gen_Nat(&mut rng, model::PROP_LEN_DONE_FREES);
        let id = model::gen_Nat(&mut rng, model::PROP_LEN_DONE_FREES);
        Pair::run(bound, &ops).unwrap();
        let (holds, value) = model::law_done_frees(ops.clone(), bound, id);
        assert!(
            holds,
            "the emitted model breaks done_frees on {} with age bound {bound}, unit {id}: {value}",
            model::show_ListOp(&ops)
        );
    }
}

fn admit(id: u64, cls: Cls, leases: Ls) -> Op {
    Op::OAdmit(id, cls, false, 0, Dl::DNone, leases)
}

const NO_LEASE: Ls = Ls::Ls(false, false, false);
const IMAGE: Ls = Ls::Ls(false, true, false);
const LAND: Ls = Ls::Ls(false, false, true);
const IMAGE_LAND: Ls = Ls::Ls(false, true, true);

/// Lands queue behind older aged waiters (05_gateway.md "The task server schedules work units"): a
/// land that has waited no time is ordered behind a unit that has waited the age bound.
#[test]
fn lands_behind_older_aged_a_fresh_land_follows_the_aged_waiter_in_the_ready_order() {
    let ops = list(vec![
        admit(0, Cls::CBack, NO_LEASE),
        Op::OTick(1),
        admit(1, Cls::CGate, LAND),
    ]);
    let pair = Pair::run(1, &ops).unwrap();
    assert_eq!(
        pair.code_ready(),
        [0, 1],
        "the aged unit (0, waited 1 against a bound of 1) is ahead of the land (1, waited 0)"
    );
    assert!(pair.lands_behind_older_aged());
}

/// Among the aged a land waits behind an aged unit that waited longer: the aged run in the order
/// they waited.
#[test]
fn lands_behind_older_aged_an_aged_land_follows_the_aged_waiter_that_waited_longer() {
    let ops = list(vec![
        admit(0, Cls::CGate, NO_LEASE),
        Op::OTick(1),
        admit(1, Cls::CGate, LAND),
        Op::OTick(1),
    ]);
    let pair = Pair::run(1, &ops).unwrap();
    assert_eq!(
        pair.code_ready(),
        [0, 1],
        "both have waited the bound; the unit (0) waited 2 and the land (1) waited 1"
    );
    assert!(pair.lands_behind_older_aged());
}

/// The order's effect on selection: when a land and an aged unit want the same lease class, the
/// aged unit takes it and the land waits its turn.
#[test]
fn lands_behind_older_aged_a_fresh_land_leaves_the_lease_to_the_aged_waiter() {
    let ops = list(vec![
        admit(0, Cls::CBack, IMAGE),
        Op::OTick(1),
        admit(1, Cls::CGate, IMAGE_LAND),
        Op::OSelect,
    ]);
    let pair = Pair::run(1, &ops).unwrap();
    assert_eq!(
        pair.code_selected(),
        [0],
        "the aged unit (0) holds the image window; the land (1) waits for it"
    );
    assert_eq!(pair.code_holders(LeaseClass::Image), [0]);
}

/// The wire line a `task-admit` request is, with the class the model says it names, if any.
fn wire_line(wire: &Wire) -> String {
    let class = match wire {
        Wire::WOmit => String::new(),
        Wire::WClass(cls) => format!(
            r#","class":{}"#,
            serde_json::to_string(&class_of(cls)).expect("a class is a string")
        ),
    };
    format!(r#"{{"op":"task-admit","unit":"u","owner":"o"{class},"command":"c"}}"#)
}

fn admitted_as(wire: &Wire) -> Cls {
    cls_of(
        crate::control::admitted_class(wire_line(wire).as_bytes())
            .expect("a task-admit line is read as an admission"),
    )
}

/// `TaskAdmitIn` is the model's `wire_class` on the model's corpus and draws: a named class is
/// kept, an omitted one is background.
#[test]
fn the_wire_class_is_the_model_on_its_corpus_and_draws() {
    let mut wires: Vec<Wire> = model::corpus_omitted_class_is_background()
        .into_iter()
        .map(|(wire,)| wire)
        .collect();
    let mut rng = model::Rng(model::SEED_OMITTED_CLASS_IS_BACKGROUND);
    wires.extend(
        (0..model::PROPS_OMITTED_CLASS_IS_BACKGROUND)
            .map(|_| model::gen_Wire(&mut rng, model::PROP_LEN_OMITTED_CLASS_IS_BACKGROUND)),
    );
    for wire in wires {
        assert_eq!(
            model::show_Cls(&admitted_as(&wire)),
            model::show_Cls(&model::wire_class(wire.clone())),
            "TaskAdmitIn admits {} as the model says",
            model::show_Wire(&wire)
        );
    }
}

/// An omitted class is background (05_gateway.md "Classes": ordinary commands default to
/// background): a `task-admit` line without a class is admitted as background, the line
/// `TaskAdmitRequest::new` writes leaves the class off, and a request that names a class, gate
/// included, writes it and is admitted as it.
#[test]
fn omitted_class_is_background_a_task_admit_without_a_class_is_admitted_as_background() {
    let omitted = Wire::WOmit;
    assert_eq!(
        crate::control::admitted_class(wire_line(&omitted).as_bytes()),
        Some(TaskClass::Background),
        "a task-admit line without a class is admitted as background"
    );
    assert!(
        model::law_omitted_class_is_background(omitted).0,
        "the model's class for an omitted one is the spec's"
    );
    let mut request = TaskAdmitRequest::new("u", "o", "c");
    let line = serde_json::to_string(&request).expect("a request encodes");
    assert!(!line.contains("class"), "{line}");
    assert_eq!(
        crate::control::admitted_class(line.as_bytes()),
        Some(TaskClass::Background)
    );
    for class in [
        TaskClass::Gate,
        TaskClass::Interactive,
        TaskClass::Background,
    ] {
        request.class = class;
        let line = serde_json::to_string(&request).expect("a request encodes");
        assert_eq!(
            crate::control::admitted_class(line.as_bytes()),
            Some(class),
            "{line}"
        );
    }
}
