//! `restart_blockers`, `refusal` and `read_judged` bound to their proven model,
//! `specs/cowshed/laws/install_restart.bend` (05_gateway.md "An install refuses while a land runs",
//! "The read fences the turns"). The model's restart laws are stated over every interleaving of
//! one install and any number of lands, and the install's read of the blockers is the model's
//! `blockers` and `refuses`; with the fence, whether the install goes on under a fenced queue is
//! its `read_fences`, the ReadBlockers step (`the_fenced_read_is_the_read_step`). This driver
//! shows the code is that judgement: it runs the emitted model and the code on the same queues —
//! the model's corpus and seeded draws, the queues the land-turn model
//! (`specs/cowshed/laws/land_turn.bend`) reaches on its own corpus and draws, and the installs
//! refused from 2026-10-27 to 2026-11-06 — and compares the holders named, whether the install
//! refuses, the words it refuses with, and whether it goes on fenced.

use std::rc::Rc;

use super::{Busy, read_judged, refusal, restart_blockers};
use cowshed_core::api::dto::{LandTurnName, LandTurnQueue};
use cowshed_core::land_turn::Ticket;

mod model;

/// The land-turn model the gateway's `Turns` is bound to (cowshed-gateway
/// `src/land_turn/laws.rs`): its queues are the ones `land-turns` lists.
#[path = "../../../cowshed-gateway/src/land_turn/laws/model.rs"]
mod land_turn_model;

use model::{List, RQ};

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

/// The land-turn model's own lists: the two emitted models each carry a `List`.
fn turn_items<T: Clone>(list: &Rc<land_turn_model::List<T>>) -> Vec<T> {
    let mut out = Vec::new();
    let mut at = list.clone();
    while let land_turn_model::List::Con(head, tail) = &*at {
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

fn ticket(number: u64, workspace: &str, pid: u32) -> Ticket {
    Ticket {
        number,
        pid,
        workspace: workspace.to_owned(),
        asked_unix_ms: 0,
        command: format!("cowshed land {workspace}"),
        bound: Vec::new(),
    }
}

/// The model's queues as `land-turns` answers them: repository `r` is named `repo<r>`, and each
/// ticket's land is `ws<number>`.
fn code_queues(queues: &Rc<List<RQ>>) -> Vec<LandTurnQueue> {
    items(queues)
        .into_iter()
        .map(|RQ::RQ(repo, tickets)| LandTurnQueue {
            repo: LandTurnName::new(format!("repo{repo}")).expect("a land-turn name"),
            tickets: items(&tickets)
                .into_iter()
                .map(|number| {
                    let pid = u32::try_from(number + 1).expect("a model ticket fits a pid");
                    ticket(number, &format!("ws{number}"), pid)
                })
                .collect(),
            abandoned: Vec::new(),
        })
        .collect()
}

fn holders(busy: &[Busy]) -> Vec<u64> {
    busy.iter()
        .map(|Busy::LandTurn { holder, .. }| holder.number)
        .collect()
}

/// Hold the code to the model on `queues`: the same holders in the same order, a refusal exactly
/// when the model refuses, naming every holder, and the install going on under the fence exactly
/// when the model's read fences.
fn judged_alike(queues: &Rc<List<RQ>>) -> Result<Vec<Busy>, String> {
    let shown = model::show_ListRQ(queues);
    let code = code_queues(queues);
    let busy = restart_blockers(&code);
    let model_holders = items(&model::blockers(queues.clone()));
    if holders(&busy) != model_holders {
        return Err(format!(
            "on {shown} the code names holders {:?}, the model {model_holders:?}",
            holders(&busy)
        ));
    }
    let refused = refusal(&busy);
    if refused.is_some() != model::refuses(queues.clone()) {
        return Err(format!(
            "on {shown} the code {} and the model {}",
            if refused.is_some() {
                "refuses"
            } else {
                "restarts"
            },
            if model::refuses(queues.clone()) {
                "refuses"
            } else {
                "restarts"
            }
        ));
    }
    if let Some(error) = refused {
        if error.code != cowshed_core::ErrorCode::Conflict {
            return Err(format!(
                "on {shown} the refusal is {:?}, not a conflict",
                error.code
            ));
        }
        if let Some(missing) = busy
            .iter()
            .map(ToString::to_string)
            .find(|named| !error.message.contains(named.as_str()))
        {
            return Err(format!(
                "on {shown} the refusal does not name {missing}: {}",
                error.message
            ));
        }
    }
    // The gateway fences exactly when no queue has a ticket (cowshed-gateway `Turns::fence`, bound
    // to `land_turn.bend`'s `fence_only_when_free`); the install goes on only under that fence.
    let gateway_fenced = code.iter().all(|queue| queue.tickets.is_empty());
    let goes_on = read_judged(&code, gateway_fenced).is_ok();
    if goes_on != model::read_fences(queues.clone()) {
        return Err(format!(
            "on {shown} the code {} and the model's read {}",
            if goes_on { "goes on fenced" } else { "refuses" },
            if model::read_fences(queues.clone()) {
                "fences"
            } else {
                "fences nothing"
            }
        ));
    }
    Ok(busy)
}

/// Every enumerated case of the model's definedness law, the law itself on the model, and the
/// code on the same queues with the held turn and without it.
#[test]
fn the_blockers_are_the_model_on_its_corpus() {
    let corpus = model::corpus_a_held_turn_refuses();
    assert!(!corpus.is_empty());
    for (repo, holder, behind, rest) in corpus {
        let (holds, value) =
            model::law_a_held_turn_refuses(repo, holder, behind.clone(), rest.clone());
        assert!(holds, "the emitted model breaks its own law: {value}");
        let held = Rc::new(List::Con(
            RQ::RQ(repo, Rc::new(List::Con(holder, behind))),
            rest.clone(),
        ));
        judged_alike(&held).unwrap();
        judged_alike(&rest).unwrap();
    }
}

/// The model's seeded draws of queues.
#[test]
fn the_blockers_are_the_model_on_seeded_queues() {
    let mut rng = model::Rng(model::SEED_A_HELD_TURN_REFUSES);
    for _ in 0..model::PROPS_A_HELD_TURN_REFUSES {
        let queues = model::gen_ListRQ(&mut rng, model::PROP_LEN_A_HELD_TURN_REFUSES);
        judged_alike(&queues).unwrap();
    }
}

/// A land-turn model queue as `land-turns` lists it: its ticket numbers in order, and the one
/// that holds the turn.
fn reached(ops: &Rc<land_turn_model::List<land_turn_model::Op>>) -> (Vec<u64>, Option<u64>) {
    let queue = turn_items(&land_turn_model::queue(land_turn_model::run(
        ops.clone(),
        land_turn_model::empty(),
    )));
    let numbers = queue
        .iter()
        .map(|land_turn_model::E::E(number, _, _, _)| *number)
        .collect();
    let held = queue
        .iter()
        .find(|land_turn_model::E::E(_, _, _, held)| *held)
        .map(|land_turn_model::E::E(number, _, _, _)| *number);
    (numbers, held)
}

/// Two repositories' queues the land-turn model reaches: the install is refused exactly while one
/// of them has a holder, and it names that queue's holding ticket.
fn reached_pair(
    first: &Rc<land_turn_model::List<land_turn_model::Op>>,
    second: &Rc<land_turn_model::List<land_turn_model::Op>>,
) -> Result<(), String> {
    let pair = [reached(first), reached(second)];
    let queues = list(
        pair.iter()
            .zip(0..)
            .map(|((numbers, _), repo)| RQ::RQ(repo, list(numbers.clone())))
            .collect(),
    );
    let busy = judged_alike(&queues)?;
    let held: Vec<u64> = pair.iter().filter_map(|(_, held)| *held).collect();
    if holders(&busy) != held {
        return Err(format!(
            "the install names {:?} where the land-turn model holds {held:?}, on {} and {}",
            holders(&busy),
            land_turn_model::show_ListOp(first),
            land_turn_model::show_ListOp(second)
        ));
    }
    Ok(())
}

/// The queues the land-turn model reaches on its corpus, two repositories at a time.
#[test]
fn the_blockers_are_the_model_on_land_turn_queues() {
    let corpus = land_turn_model::corpus_one_holder_check();
    for (index, (first,)) in corpus.iter().enumerate() {
        let (second,) = &corpus[(index * 7 + 1) % corpus.len()];
        reached_pair(first, second).unwrap();
    }
}

/// The queues the land-turn model reaches on its seeded draws.
#[test]
fn the_blockers_are_the_model_on_seeded_land_turn_queues() {
    let mut rng = land_turn_model::Rng(land_turn_model::SEED_ONE_HOLDER_CHECK);
    for _ in 0..land_turn_model::PROPS_ONE_HOLDER_CHECK {
        let first =
            land_turn_model::gen_ListOp(&mut rng, land_turn_model::PROP_LEN_ONE_HOLDER_CHECK);
        let second =
            land_turn_model::gen_ListOp(&mut rng, land_turn_model::PROP_LEN_ONE_HOLDER_CHECK);
        reached_pair(&first, &second).unwrap();
    }
}

/// The installs refused from 2026-10-27 to 2026-11-06: each `install deferred` outcome
/// `tooling/land-install.sh` wrote to a land's after-step log names the holders it found. Rebuilt
/// as those queues, the code refuses with exactly the recorded words, and the model names the
/// same tickets. The private sibling repository and its lands' names are replaced in the
/// fixture, consistently, since this repository is public.
#[test]
fn recorded_refusals_replay_through_the_model_and_the_code() {
    #[derive(serde::Deserialize)]
    struct Holder {
        workspace: String,
        repo: String,
        ticket: u64,
        pid: u32,
    }
    #[derive(serde::Deserialize)]
    struct Refused {
        at: String,
        blockers: Vec<Holder>,
        said: String,
    }
    let mut replayed = 0;
    for line in include_str!("laws/refusals-2026-10.jsonl").lines() {
        let Refused { at, blockers, said } =
            serde_json::from_str(line).expect("a recorded install refusal");
        let queues: Vec<LandTurnQueue> = blockers
            .iter()
            .map(|holder| LandTurnQueue {
                repo: LandTurnName::new(holder.repo.clone()).expect("a recorded land-turn name"),
                tickets: vec![ticket(holder.ticket, &holder.workspace, holder.pid)],
                abandoned: Vec::new(),
            })
            .collect();
        let busy = restart_blockers(&queues);
        let words: Vec<String> = busy.iter().map(ToString::to_string).collect();
        assert_eq!(
            words.join("; "),
            said,
            "{at}: the code names the blockers otherwise"
        );
        let error = refusal(&busy).unwrap_or_else(|| panic!("{at}: the code restarts"));
        assert!(
            error.message.contains(&words.join(", ")),
            "{at}: {}",
            error.message
        );
        let modelled = list(
            blockers
                .iter()
                .zip(0..)
                .map(|(holder, repo)| RQ::RQ(repo, list(vec![holder.ticket])))
                .collect(),
        );
        assert!(model::refuses(modelled.clone()), "{at}: the model restarts");
        assert_eq!(
            items(&model::blockers(modelled)),
            holders(&busy),
            "{at}: the model names other holders"
        );
        replayed += 1;
    }
    assert_eq!(replayed, 15, "fifteen refused installs were recorded");
}
