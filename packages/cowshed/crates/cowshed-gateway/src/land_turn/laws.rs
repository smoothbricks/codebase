//! `Turns` bound to its proven model, `specs/cowshed/laws/land_turn.bend` (05_gateway.md "Land
//! turns", its laws under "Land-turn laws"). The model's laws are proven for every input
//! sequence; this driver shows the code is the model: it runs the emitted model and `Turns` on the
//! same inputs, the model's enumerated corpus, its seeded draws and the recorded queues of
//! 2026-10-21 to 2026-10-27, and after every input compares the two queues ticket by ticket, its
//! asker included, the answer to every bind, whether every ask is numbered or parked, the last
//! number given, the fences, the waiting installs and the parked asks' askers in order, and
//! evaluates the laws' checks on the code's own queue.

use std::rc::Rc;

use super::{BindRefusal, Parked, Turns};
use cowshed_core::api::dto::LandTurnName;
use cowshed_core::land_turn::{TurnAsk, TurnClass};

// Regenerate with bun run --filter @smoothbricks/cowshed emit-land-turn-law.
#[rustfmt::skip]
mod model;

use model::{Bound, E, List, Op, P, St};

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

fn shown(queue: &Rc<List<E>>) -> String {
    model::show_ListE(queue)
}

/// One queue fed to the model and to `Turns` alike.
struct Pair {
    repo: LandTurnName,
    model: St,
    turns: Turns,
    now_unix_ms: u64,
    inputs: Vec<String>,
    /// The keys of the installs that wait, in the order they asked: a `TWaitLeave` is the first
    /// of them leaving, since the model counts waiting installs and names none.
    waiting: std::collections::VecDeque<u64>,
    /// The key the next waiting install is given.
    installs: u64,
}

impl Pair {
    fn new(repo: &str) -> Self {
        Self {
            repo: LandTurnName::new(repo).expect("a land-turn name"),
            model: model::empty(),
            turns: Turns::default(),
            now_unix_ms: 1,
            inputs: Vec::new(),
            waiting: std::collections::VecDeque::new(),
            installs: 0,
        }
    }

    /// The code's queue as the model's value: each ticket's number, whether its asker still holds
    /// its place, its bound pids in bind order, and whether it holds the turn.
    fn code_queue(&self) -> Rc<List<E>> {
        list(
            self.turns
                .queues
                .get(&self.repo)
                .map(|entries| {
                    entries
                        .iter()
                        .map(|entry| {
                            E::E(
                                entry.ticket.number,
                                u64::from(entry.ticket.pid),
                                entry.asker,
                                list(
                                    entry
                                        .ticket
                                        .bound
                                        .iter()
                                        .map(|&pid| u64::from(pid))
                                        .collect(),
                                ),
                                entry.held,
                                entry.bounded,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
        )
    }

    fn model_queue(&self) -> Rc<List<E>> {
        model::queue(self.model.clone())
    }

    /// Feed `op` to both, `outcome` being how a departing asker ended its hold; then hold them
    /// to the same queue, the same bind and ask answers, the same numbering, fences, waiting
    /// installs and parked asks, and the laws.
    fn input(&mut self, op: &Op, outcome: Option<&str>) -> Result<(), String> {
        self.now_unix_ms += 1;
        self.inputs.push(model::show_Op(op));
        let pid = |p: u64| u32::try_from(p).expect("a model pid fits a pid");
        let answers = match op {
            Op::TAsk(asker, bounded) => {
                let workspace = format!("ws{}", self.inputs.len());
                let asked = self.turns.ask(
                    u64::try_from(self.inputs.len()).expect("an input count fits a key"),
                    TurnAsk {
                        repo: self.repo.clone(),
                        command: format!("land {workspace}"),
                        workspace,
                        class: bounded.then_some(TurnClass::Bounded),
                    },
                    pid(*asker),
                    self.now_unix_ms,
                );
                let (code, model) = (
                    matches!(asked, Err(Parked)),
                    !model::inside(*asker, self.model_queue())
                        && (model::fences(self.model.clone()) > 0
                            || (model::waiting(self.model.clone()) > 0
                                && (!*bounded || !model::has_unbounded(self.model_queue())))),
                );
                if code != model {
                    let said = |parked: bool| if parked { "parked" } else { "numbered" };
                    return Err(self.refused(&format!(
                        "the ask is {} by Turns and {} by the model",
                        said(code),
                        said(model)
                    )));
                }
                None
            }
            Op::TFence => {
                self.turns.fence();
                None
            }
            Op::TUnfence => {
                self.turns.unfence(self.now_unix_ms);
                None
            }
            Op::TWait => {
                let install = self.installs;
                self.installs += 1;
                if !self.turns.wait_fence(install, None, self.now_unix_ms) {
                    self.waiting.push_back(install);
                }
                None
            }
            Op::TWaitLeave => {
                if let Some(install) = self.waiting.pop_front() {
                    self.turns.wait_left(install, self.now_unix_ms);
                }
                None
            }
            Op::TBind(number, p) => {
                let code = match self.turns.bind(*number, pid(*p)) {
                    Ok(_) => Bound::BGranted,
                    Err(BindRefusal::Gone) => Bound::BGone,
                    Err(BindRefusal::Waiting) => Bound::BWaiting,
                };
                Some((model::find(*number, self.model_queue()), code))
            }
            Op::TExit(number, p) => {
                let settled = self.turns.exited(*number, pid(*p), self.now_unix_ms);
                self.waiting
                    .retain(|install| !settled.fenced.contains(install));
                None
            }
            Op::TLeave(number) => {
                let settled =
                    self.turns
                        .left(*number, outcome.map(str::to_owned), self.now_unix_ms);
                self.waiting
                    .retain(|install| !settled.fenced.contains(install));
                None
            }
        };
        self.model = model::step(op.clone(), self.model.clone());
        let (model_queue, code_queue) = (self.model_queue(), self.code_queue());
        if let Some((model_answer, code_answer)) = answers
            && model_answer != code_answer
        {
            return Err(self.refused(&format!(
                "the bind is answered {} by the model and {} by Turns",
                model::show_Bound(&model_answer),
                model::show_Bound(&code_answer)
            )));
        }
        if model_queue != code_queue {
            return Err(self.refused(&format!(
                "the queues differ: model {}, Turns {}",
                shown(&model_queue),
                shown(&code_queue)
            )));
        }
        let model_numbering = (
            model::counter(self.model.clone()),
            model::fences(self.model.clone()),
            model::waiting(self.model.clone()),
            items(&model::parked(self.model.clone()))
                .into_iter()
                .map(|P::P(pid, bounded)| (pid, bounded))
                .collect::<Vec<_>>(),
        );
        let code_numbering = (
            self.turns.numbered,
            self.turns.fences,
            u64::try_from(self.turns.waiting.len()).expect("a count fits u64"),
            self.turns
                .parked
                .iter()
                .map(|parked| (u64::from(parked.pid), parked.ask.is_bounded()))
                .collect::<Vec<_>>(),
        );
        if model_numbering != code_numbering {
            return Err(self.refused(&format!(
                "the model's last number, fences, waiting installs and parked asks are \
                 {model_numbering:?}, Turns' {code_numbering:?}"
            )));
        }
        if !model::turn_ok_b(code_queue.clone()) {
            return Err(self.refused(&format!(
                "one_holder fails on Turns' queue {}",
                shown(&code_queue)
            )));
        }
        if !model::chain_b(code_queue.clone(), 0, self.turns.numbered) {
            return Err(self.refused(&format!(
                "fifo fails on Turns' queue {} (last number {})",
                shown(&code_queue),
                self.turns.numbered
            )));
        }
        Ok(())
    }

    fn refused(&self, what: &str) -> String {
        format!("{what}, after the inputs [{}]", self.inputs.join(", "))
    }

    fn run(repo: &str, ops: &Rc<List<Op>>) -> Result<(), String> {
        let mut pair = Self::new(repo);
        items(ops).iter().try_for_each(|op| pair.input(op, None))
    }
}

/// Every enumerated input sequence of the model's corpus, and the model's own law checks on it.
#[test]
fn turns_are_the_model_on_its_corpus() {
    for (ops,) in model::corpus_one_holder_check() {
        Pair::run("corpus", &ops).unwrap();
        for (holds, value) in [
            model::law_one_holder_check(ops.clone()),
            model::law_fifo_check(ops.clone()),
        ] {
            assert!(
                holds,
                "the emitted model breaks its own law on {}: {value}",
                model::show_ListOp(&ops)
            );
        }
    }
}

/// The model's seeded draws: longer sequences, the same stream every target draws.
#[test]
fn turns_are_the_model_on_seeded_inputs() {
    let mut rng = model::Rng(model::SEED_ONE_HOLDER_CHECK);
    for _ in 0..model::PROPS_ONE_HOLDER_CHECK {
        let ops = model::gen_ListOp(&mut rng, model::PROP_LEN_ONE_HOLDER_CHECK);
        Pair::run("seeded", &ops).unwrap();
    }
}

/// The recorded schedule of 2026-10-28 13:52:39Z (05_gateway.md "The read fences the turns") as
/// the queue sees it under the fence: the install's read finds no ticket and fences, the land's ask
/// is parked and numbered nothing, the lift numbers it on its own connection, and the land holds
/// the turn and binds its gate; a later install's read finds that holder and sets no fence, so the
/// next land queues behind it.
#[test]
fn the_recorded_fenced_schedule_replays_through_the_model_and_turns() {
    let mut pair = Pair::new("acme");
    for op in [
        Op::TFence,
        Op::TAsk(41, false),
        Op::TUnfence,
        Op::TBind(1, 7),
        Op::TFence,
        Op::TAsk(42, false),
        Op::TExit(1, 7),
    ] {
        pair.input(&op, None).unwrap();
    }
    pair.input(&Op::TLeave(1), Some("exited 0")).unwrap();
    assert_eq!(
        (pair.turns.numbered, pair.turns.fences),
        (2, 0),
        "the parked ask was numbered once, on the lift, and the read that found a holder fenced \
         nothing"
    );
    assert_eq!(shown(&pair.code_queue()), shown(&pair.model_queue()));
    let E::E(holder, _, _, _, held, _) = items(&pair.code_queue())[0].clone();
    assert_eq!((holder, held), (2, true), "the next land holds the turn");
}

/// The install of land 124380eca9, 2026-11-10, as the waiting install meets it: a land holds the
/// turn, the install waits, the lands that ask after it are parked, the install is fenced the step
/// the ticket before it ends, and its lift numbers the parked lands in arrival order. Before the
/// wait it deferred three times, each rerun losing the gap to the next land's ask.
#[test]
fn the_recorded_deferred_install_waits_in_order_through_the_model_and_turns() {
    let mut pair = Pair::new("smoothbricks");
    for op in [Op::TAsk(41, false), Op::TWait, Op::TAsk(42, false)] {
        pair.input(&op, None).unwrap();
    }
    pair.input(&Op::TLeave(1), Some("exited 0")).unwrap();
    pair.input(&Op::TAsk(43, false), None).unwrap();
    assert_eq!(
        (
            pair.turns.fences,
            pair.turns.waiting.len(),
            pair.turns.parked.len()
        ),
        (1, 0, 2),
        "the install is fenced and both later lands are parked behind it"
    );
    assert!(
        items(&pair.code_queue()).is_empty(),
        "no land holds the turn"
    );
    pair.input(&Op::TUnfence, None).unwrap();
    assert_eq!(
        items(&pair.code_queue())
            .into_iter()
            .map(|E::E(number, _, _, _, held, _)| (number, held))
            .collect::<Vec<_>>(),
        [(2, true), (3, false)],
        "the parked lands are numbered in arrival order once the install's fence lifts"
    );
}

//#region smoo/cowshed!n/read-fences-turns
/// Recorded 2026-11-16 (05_gateway.md "The read fences the turns"): a land holds its turn, the
/// install of the land before it waits for that turn, and the land asks its ref turn inside its
/// turn. That ask is numbered, though a land that asked from outside a moment earlier is parked;
/// the land ends both turns, which fences the install, and the install's lift numbers the parked
/// land. As recorded, the ref ask was parked too, and the land and the install each waited for
/// the other for 24 minutes.
#[test]
fn the_recorded_ref_turn_under_a_waiting_install_replays_through_the_model_and_turns() {
    let mut pair = Pair::new("smoothbricks");
    for op in [
        Op::TAsk(41, false),
        Op::TWait,
        Op::TAsk(42, false),
        Op::TAsk(41, false),
    ] {
        pair.input(&op, None).unwrap();
    }
    assert_eq!(
        (
            items(&pair.code_queue())
                .into_iter()
                .map(|E::E(number, asker, _, _, held, _)| (number, asker, held))
                .collect::<Vec<_>>(),
            pair.turns.waiting.len(),
            pair.turns.parked.len()
        ),
        (vec![(1, 41, true), (2, 41, false)], 1, 1),
        "the land's ask inside its turn is numbered, and the ask from outside is parked"
    );
    pair.input(&Op::TLeave(2), Some("delivered")).unwrap();
    pair.input(&Op::TLeave(1), Some("exited 0")).unwrap();
    assert_eq!(
        (pair.turns.fences, pair.turns.waiting.len()),
        (1, 0),
        "the install is fenced the step the land ends its turn"
    );
    pair.input(&Op::TUnfence, None).unwrap();
    assert_eq!(
        items(&pair.code_queue())
            .into_iter()
            .map(|E::E(number, asker, _, _, held, _)| (number, asker, held))
            .collect::<Vec<_>>(),
        [(3, 42, true)],
        "the land parked behind the install holds the turn once its fence lifts"
    );
}
//#endregion smoo/cowshed!n/read-fences-turns
/// The recorded queues, 2026-10-21 to 2026-10-27 (the land-turn records of six repositories'
/// `turns.log`, before the queues moved into the gateway): every `taken` is the ticket the model
/// and `Turns` both hand the turn to.
#[test]
fn recorded_turns_replay_through_the_model_and_turns() {
    #[derive(serde::Deserialize)]
    struct Record {
        q: String,
        e: String,
        n: u64,
    }
    let mut pairs: std::collections::BTreeMap<String, (Pair, std::collections::HashMap<u64, u64>)> =
        std::collections::BTreeMap::new();
    for line in include_str!("laws/turns-2026-10.jsonl").lines() {
        let Record { q, e, n } = serde_json::from_str(line).expect("a recorded land-turn event");
        let (pair, numbers) = pairs
            .entry(q.clone())
            .or_insert_with(|| (Pair::new(&q), std::collections::HashMap::new()));
        match e.as_str() {
            "queued" => {
                // Each recorded land is a process of its own: no ask comes from inside a turn.
                pair.input(&Op::TAsk(n, false), None).unwrap();
                numbers.insert(n, pair.turns.numbered);
            }
            "taken" => {
                let number = numbers[&n];
                let held = |queue: Rc<List<E>>| {
                    items(&queue)
                        .first()
                        .is_some_and(|E::E(first, _, _, _, held, _)| *first == number && *held)
                };
                assert!(
                    held(pair.model_queue()) && held(pair.code_queue()),
                    "{q} ticket {n} took the turn the model holds for another: model {}, Turns {}",
                    shown(&pair.model_queue()),
                    shown(&pair.code_queue())
                );
            }
            "finished" => pair
                .input(&Op::TLeave(numbers[&n]), Some("exited 0"))
                .unwrap(),
            "abandoned" => pair.input(&Op::TLeave(numbers[&n]), None).unwrap(),
            other => panic!("{q}: a land-turn event the replay does not know: {other}"),
        }
    }
    assert_eq!(pairs.len(), 6, "six repositories' queues were recorded");
}

/// Option B law-driver check (05_gateway.md "The read fences the turns"): while an install waits
/// behind a non-bounded land ticket, a bounded ask from outside enters the queue (not parked), and
/// once the land ticket leaves leaving only bounded tickets, a later bounded ask parks until the
/// install is fenced and lifts its fence.
#[test]
fn a_bounded_ask_enters_behind_a_land_while_an_install_waits_and_parks_once_the_land_leaves() {
    let mut pair = Pair::new("acme");
    pair.input(&Op::TAsk(1, false), None).unwrap();
    pair.input(&Op::TWait, None).unwrap();
    pair.input(&Op::TAsk(2, true), None).unwrap();
    assert_eq!(
        (pair.turns.numbered, pair.turns.parked.len()),
        (2, 0),
        "bounded ask behind a held land while an install waits must be numbered, not parked"
    );
    // Land leaves: only bounded ticket 2 remains, so a later bounded ask must park.
    pair.input(&Op::TLeave(1), Some("exited 0")).unwrap();
    pair.input(&Op::TAsk(3, true), None).unwrap();
    assert_eq!(
        (pair.turns.numbered, pair.turns.parked.len()),
        (2, 1),
        "bounded ask must park once only bounded tickets remain"
    );
    // Bounded ticket 2 leaves: install is fenced; lift admits parked bounded ask 3.
    pair.input(&Op::TLeave(2), Some("committed")).unwrap();
    assert_eq!((pair.turns.fences, pair.turns.waiting.len()), (1, 0));
    pair.input(&Op::TUnfence, None).unwrap();
    assert_eq!(
        items(&pair.code_queue())
            .into_iter()
            .map(|E::E(number, asker, _, _, held, bounded)| (number, asker, held, bounded))
            .collect::<Vec<_>>(),
        [(3, 3, true, true)],
        "the parked bounded ask holds the turn as bounded once the install's fence lifts"
    );
}
