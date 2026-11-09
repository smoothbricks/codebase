//! `Turns` bound to its proven model, `specs/cowshed/laws/land_turn.bend` (05_gateway.md "Land
//! turns", its laws under "Land-turn laws"). The model's laws are proven for every input
//! sequence; this driver shows the code is the model: it runs the emitted model and `Turns` on the
//! same inputs, the model's enumerated corpus, its seeded draws and the recorded queues of
//! 2026-10-21 to 2026-10-27, and after every input compares the two queues ticket by ticket,
//! the answer to every bind and every ask under a fence, the last number given and the fences,
//! and evaluates the laws' checks on the code's own queue.

use std::rc::Rc;

use super::{BindRefusal, Fenced, Turns};
use cowshed_core::api::dto::LandTurnName;
use cowshed_core::land_turn::TurnAsk;

// Emitted by bend-emit from the checked model; never edited or formatted by hand.
#[rustfmt::skip]
mod model;

use model::{Bound, E, List, Op, St};

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
}

impl Pair {
    fn new(repo: &str) -> Self {
        Self {
            repo: LandTurnName::new(repo).expect("a land-turn name"),
            model: model::empty(),
            turns: Turns::default(),
            now_unix_ms: 1,
            inputs: Vec::new(),
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
    /// to the same queue, the same bind and ask answers, the same numbering and fences, and the
    /// laws.
    fn input(&mut self, op: &Op, outcome: Option<&str>) -> Result<(), String> {
        self.now_unix_ms += 1;
        self.inputs.push(model::show_Op(op));
        let pid = |p: u64| u32::try_from(p).expect("a model pid fits a pid");
        let answers = match op {
            Op::TAsk => {
                let workspace = format!("ws{}", self.inputs.len());
                let asked = self.turns.ask(
                    TurnAsk {
                        repo: self.repo.clone(),
                        command: format!("land {workspace}"),
                        workspace,
                    },
                    4242,
                    self.now_unix_ms,
                );
                let (code, model) = (
                    matches!(asked, Err(Fenced)),
                    model::fences(self.model.clone()) > 0,
                );
                if code != model {
                    let said = |fenced: bool| if fenced { "fenced" } else { "numbered" };
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
                self.turns.unfence();
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
                self.turns.exited(*number, pid(*p), self.now_unix_ms);
                None
            }
            Op::TLeave(number) => {
                self.turns
                    .left(*number, outcome.map(str::to_owned), self.now_unix_ms);
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
        );
        if model_numbering != (self.turns.numbered, self.turns.fences) {
            return Err(self.refused(&format!(
                "the model's last number and fences are {model_numbering:?}, Turns' ({}, {})",
                self.turns.numbered, self.turns.fences
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
/// is refused and numbered nothing, the restart lifts the fence, the land asks again and holds the
/// turn and binds its gate; a later install's read finds that holder and sets no fence, so the next
/// land queues behind it.
#[test]
fn the_recorded_fenced_schedule_replays_through_the_model_and_turns() {
    let mut pair = Pair::new("acme");
    for op in [
        Op::TFence,
        Op::TAsk,
        Op::TUnfence,
        Op::TAsk,
        Op::TBind(1, 7),
        Op::TFence,
        Op::TAsk,
        Op::TExit(1, 7),
    ] {
        pair.input(&op, None).unwrap();
    }
    pair.input(&Op::TLeave(1), Some("exited 0")).unwrap();
    assert_eq!(
        (pair.turns.numbered, pair.turns.fences),
        (2, 0),
        "the fenced ask was never numbered, and the read that found a holder fenced nothing"
    );
    assert_eq!(shown(&pair.code_queue()), shown(&pair.model_queue()));
    let E::E(holder, _, _, held) = items(&pair.code_queue())[0].clone();
    assert_eq!((holder, held), (2, true), "the next land holds the turn");
}
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
                pair.input(&Op::TAsk, None).unwrap();
                numbers.insert(n, pair.turns.numbered);
            }
            "taken" => {
                let number = numbers[&n];
                let held = |queue: Rc<List<E>>| {
                    items(&queue)
                        .first()
                        .is_some_and(|E::E(first, _, _, held)| *first == number && *held)
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
