//! `select` bound to its proven model, `specs/cowshed/laws/zfs_substrate.bend` (09_substrates.md,
//! selection). The model's laws are proven for every main entry, adoption, declared root and set
//! of roots; this driver shows the code is the model. It runs the emitted model and `select` on
//! the same inputs (the model's enumerated corpus and its seeded draws, each also with the main
//! dataset of every root it names placed at the main, and a dataset beside one, and with each
//! serving root declared), and compares the root chosen or the refusal, and that the serving and
//! the holding roots are read exactly when the model's selection depends on them.

use std::cell::Cell;
use std::rc::Rc;

use cowshed_helper::name::Root;

use super::{MainEntry, Unserved, select};

// Regenerate with bun run --filter @smoothbricks/cowshed emit-zfs-substrate-law.
#[rustfmt::skip]
mod model;

use model::{List, Main, Named, R, Sel};

/// The user every model root names.
const UID: u32 = 501;

fn items<T: Clone>(list: &Rc<List<T>>) -> Vec<T> {
    match &**list {
        List::Nil => Vec::new(),
        List::Con(head, tail) => std::iter::once(head.clone()).chain(items(tail)).collect(),
    }
}

fn list<T>(items: Vec<T>) -> Rc<List<T>> {
    items
        .into_iter()
        .rev()
        .fold(Rc::new(List::Nil), |tail, head| {
            Rc::new(List::Con(head, tail))
        })
}

/// A model component as a name component: the components the rule names, `.trash` for the hidden
/// id, and a plain name for every other.
fn component(n: u64) -> String {
    match n {
        0 => "cowshed".to_owned(),
        1 => "projects".to_owned(),
        2 => "main".to_owned(),
        3 => "build".to_owned(),
        4 => ".trash".to_owned(),
        other => format!("c{other}"),
    }
}

fn name(components: &Rc<List<u64>>) -> String {
    items(components)
        .into_iter()
        .map(component)
        .collect::<Vec<_>>()
        .join("/")
}

fn root_name(r: &R) -> String {
    name(&model::root_names(r.clone()))
}

fn roots(rs: &Rc<List<R>>) -> Vec<Root> {
    items(rs)
        .iter()
        .map(|r| Root::parse(&root_name(r)).expect("a model root is a cowshed root"))
        .collect()
}

fn entry(main: &Main) -> MainEntry {
    match main {
        Main::MFile => MainEntry::File,
        Main::MPlace(dataset) => MainEntry::Place(name(dataset)),
        Main::MHome => MainEntry::Home,
        Main::MAbsent => MainEntry::Absent,
    }
}

/// A selection as both sides can state it.
#[derive(Debug, PartialEq)]
enum Chosen {
    Images,
    Root(String),
    NoRoot,
    Several(Vec<String>),
    Unserving(String),
}

fn model_chosen(selected: &Sel) -> Chosen {
    match selected {
        Sel::Images => Chosen::Images,
        Sel::Datasets(root) => Chosen::Root(name(root)),
        Sel::NoRoot => Chosen::NoRoot,
        Sel::Several(roots) => Chosen::Several(items(roots).iter().map(root_name).collect()),
        Sel::Unserving(root) => Chosen::Unserving(root_name(root)),
    }
}

fn code_chosen(selected: Result<Option<Root>, Unserved>) -> Chosen {
    match selected {
        Ok(None) => Chosen::Images,
        Ok(Some(root)) => Chosen::Root(root.dataset().to_string()),
        Err(Unserved::NoRoot { uid }) => {
            assert_eq!(uid, UID, "the refusal names the user");
            Chosen::NoRoot
        }
        Err(Unserved::Several { uid, roots }) => {
            assert_eq!(uid, UID, "the refusal names the user");
            Chosen::Several(
                roots
                    .iter()
                    .map(|root| root.dataset().to_string())
                    .collect(),
            )
        }
        Err(Unserved::DeclaredNotServing { root, uid }) => {
            assert_eq!(uid, UID, "the refusal names the user");
            Chosen::Unserving(root.dataset().to_string())
        }
    }
}

/// Hold `select` to the model on one input: the same choice, and the serving and the holding roots
/// read exactly when the model needs them. Answers the choice.
fn selected_alike(
    main: &Main,
    adopt: bool,
    declared: &Named,
    serving: &Rc<List<R>>,
    holding: &Rc<List<R>>,
) -> Result<Chosen, String> {
    let (served, held) = (Cell::new(false), Cell::new(false));
    let code = code_chosen(select(
        entry(main),
        adopt,
        match declared {
            Named::Unnamed => None,
            Named::Name(r) => {
                Some(Root::parse(&root_name(r)).expect("a model root is a cowshed root"))
            }
        },
        || {
            served.set(true);
            Ok((UID, roots(serving)))
        },
        || {
            held.set(true);
            Ok((UID, roots(holding)))
        },
    ));
    let model = model_chosen(&model::select(
        main.clone(),
        adopt,
        declared.clone(),
        serving.clone(),
        holding.clone(),
    ));
    let reads = (served.get(), held.get());
    let model_reads = (
        model::needs_serving(main.clone(), adopt),
        model::needs_holding(main.clone()),
    );
    if code == model && reads == model_reads {
        Ok(code)
    } else {
        Err(format!(
            "{} adopting {adopt} declaring {} with serving {} and holding {}: select chooses \
             {code:?} reading (serving, holding) {reads:?}, the model {model:?} reading \
             {model_reads:?}",
            model::show_Main(main),
            model::show_Named(declared),
            model::show_ListR(serving),
            model::show_ListR(holding)
        ))
    }
}

/// The input as drawn, with the main dataset of every root it names placed at the main, and a
/// dataset beside that one: the model's draws never reach a main dataset's depth; and the drawn
/// main and an absent one with each serving root declared, which the draws rarely reach (a
/// declaration decides only an absent main). Answers how many inputs chose a root.
fn selected_alike_placed(
    main: &Main,
    adopt: bool,
    declared: &Named,
    serving: &Rc<List<R>>,
    holding: &Rc<List<R>>,
) -> Result<usize, String> {
    let mut rooted = 0;
    let mut mains = vec![main.clone()];
    for r in items(serving).into_iter().chain(items(holding)) {
        for tail in [
            vec![1, 5, 6, 2],
            vec![1, 5, 6, 4],
            vec![1, 5, 6, 2, 0],
            vec![5, 5, 6, 2],
        ] {
            mains.push(Main::MPlace(model::cat(
                model::root_names(r.clone()),
                list(tail),
            )));
        }
    }
    let declarations = items(serving).into_iter().map(Named::Name);
    let declared_mains = [main.clone(), Main::MAbsent];
    let inputs = mains.iter().map(|main| (main, declared.clone())).chain(
        declarations.flat_map(|named| declared_mains.iter().map(move |main| (main, named.clone()))),
    );
    for (main, declared) in inputs {
        if let Chosen::Root(_) = selected_alike(main, adopt, &declared, serving, holding)? {
            rooted += 1;
        }
    }
    Ok(rooted)
}

//#region smoo/cowshed!n/zfs-substrate-selection
/// Every enumerated input of the model's corpus, and the model's own law on it.
#[test]
fn the_store_selects_as_the_model_on_its_corpus() {
    let mut rooted = 0;
    for (main, adopt, declared, serving, holding) in model::corpus_a_file_main_check() {
        let (holds, value) = model::law_a_file_main_check(
            main.clone(),
            adopt,
            declared.clone(),
            serving.clone(),
            holding.clone(),
        );
        assert!(
            holds,
            "the model breaks its own law on {}: {value}",
            model::show_Main(&main)
        );
        rooted += selected_alike_placed(&main, adopt, &declared, &serving, &holding).unwrap();
    }
    assert!(rooted > 0, "the corpus chooses no root");
}

/// The model's seeded draws: the same stream every target draws.
#[test]
fn the_store_selects_as_the_model_on_seeded_inputs() {
    let mut rng = model::Rng(model::SEED_A_FILE_MAIN_CHECK);
    for _ in 0..model::PROPS_A_FILE_MAIN_CHECK {
        let main = model::gen_Main(&mut rng, model::PROP_LEN_A_FILE_MAIN_CHECK);
        let adopt = model::gen_Bool(&mut rng, model::PROP_LEN_A_FILE_MAIN_CHECK);
        let declared = model::gen_Named(&mut rng, model::PROP_LEN_A_FILE_MAIN_CHECK);
        let serving = model::gen_ListR(&mut rng, model::PROP_LEN_A_FILE_MAIN_CHECK);
        let holding = model::gen_ListR(&mut rng, model::PROP_LEN_A_FILE_MAIN_CHECK);
        selected_alike_placed(&main, adopt, &declared, &serving, &holding).unwrap();
    }
}
//#endregion smoo/cowshed!n/zfs-substrate-selection
