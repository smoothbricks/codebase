//! `plan` and `admit` bound to their proven model, `specs/cowshed/laws/zfs_substrate.bend`
//! (09_substrates.md, "Privileged helper"). The model's laws are proven for every root, owner,
//! caller and verb; this driver shows the code is the model. It runs the emitted model and the
//! helper's pure core on the same inputs (the model's enumerated corpus and its seeded draws, each
//! verb also with its datasets placed below the root, and every build dataset of a root), and
//! compares whether the verb is admitted, every dataset its `zfs` command names, and whether the
//! root's owner names the caller.

use std::rc::Rc;

use cowshed_helper::name::Root;

use crate::verb::{admit, plan};

// Regenerate with bun run --filter @smoothbricks/cowshed emit-zfs-substrate-law.
#[rustfmt::skip]
mod model;

use model::{List, Owner, Plan, R, Verb};

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

fn root(r: &R) -> Root {
    Root::parse(&name(&model::root_names(r.clone()))).expect("a model root is a cowshed root")
}

fn snapshot(dataset: &Rc<List<u64>>, snap: u64) -> String {
    format!("{}@s{snap}", name(dataset))
}

/// The helper's arguments after the root for `verb`; `set` sets one allowed property.
fn arguments(verb: &Verb) -> Vec<String> {
    let flag = |on: bool, flag: &str| on.then(|| flag.to_owned());
    match verb {
        Verb::VCheck => vec!["check".to_owned()],
        Verb::VCreate(d) => vec!["create".to_owned(), name(d)],
        Verb::VClone(origin, snap, d) => {
            vec!["clone".to_owned(), snapshot(origin, *snap), name(d)]
        }
        Verb::VSnapshot(d, snap) => vec!["snapshot".to_owned(), snapshot(d, *snap)],
        Verb::VDestroy(d) => vec!["destroy".to_owned(), name(d)],
        Verb::VDestroySnap(d, snap, defer) => ["destroy".to_owned(), snapshot(d, *snap)]
            .into_iter()
            .chain(flag(*defer, "--defer"))
            .collect(),
        Verb::VRename(from, to) => vec!["rename".to_owned(), name(from), name(to)],
        Verb::VRenameSnap(d, from, to) => {
            vec!["rename".to_owned(), snapshot(d, *from), snapshot(d, *to)]
        }
        Verb::VPromote(d) => vec!["promote".to_owned(), name(d)],
        Verb::VMount(d) => vec!["mount".to_owned(), name(d)],
        Verb::VUnmount(d, force) => ["unmount".to_owned(), name(d)]
            .into_iter()
            .chain(flag(*force, "--force"))
            .collect(),
        Verb::VSet(d) => vec!["set".to_owned(), name(d), "atime=off".to_owned()],
    }
}

/// `verb` with every dataset it names placed below `r`'s root: the model's draws name datasets
/// anywhere, mostly outside it. An empty name placed there is the root itself.
fn placed(r: &R, verb: &Verb) -> Verb {
    let root = model::root_names(r.clone());
    let below = |d: &Rc<List<u64>>| model::cat(root.clone(), d.clone());
    match verb {
        Verb::VCheck => Verb::VCheck,
        Verb::VCreate(d) => Verb::VCreate(below(d)),
        Verb::VClone(origin, snap, d) => Verb::VClone(below(origin), *snap, below(d)),
        Verb::VSnapshot(d, snap) => Verb::VSnapshot(below(d), *snap),
        Verb::VDestroy(d) => Verb::VDestroy(below(d)),
        Verb::VDestroySnap(d, snap, defer) => Verb::VDestroySnap(below(d), *snap, *defer),
        Verb::VRename(from, to) => Verb::VRename(below(from), below(to)),
        Verb::VRenameSnap(d, from, to) => Verb::VRenameSnap(below(d), *from, *to),
        Verb::VPromote(d) => Verb::VPromote(below(d)),
        Verb::VMount(d) => Verb::VMount(below(d)),
        Verb::VUnmount(d, force) => Verb::VUnmount(below(d), *force),
        Verb::VSet(d) => Verb::VSet(below(d)),
    }
}

/// The datasets a `zfs` command names, a snapshot's dataset included, in order.
fn datasets(zfs: &[String]) -> Vec<String> {
    zfs.iter()
        .skip(1)
        .filter(|argument| !argument.starts_with('-') && !argument.contains('='))
        .map(|argument| {
            argument
                .split_once('@')
                .map_or(argument.as_str(), |(dataset, _)| dataset)
                .to_owned()
        })
        .collect()
}

/// Hold `plan` to the model on `verb` at `r`'s root: refused alike, or naming the same datasets.
/// Answers whether it was admitted.
fn planned_alike(r: &R, verb: &Verb) -> Result<bool, String> {
    let model = match model::plan(r.clone(), verb.clone()) {
        Plan::Refused => None,
        Plan::Runs(named) => Some(items(&named).iter().map(name).collect::<Vec<_>>()),
    };
    let code = plan(&root(r), &arguments(verb))
        .ok()
        .map(|plan| plan.zfs.as_deref().map_or_else(Vec::new, datasets));
    if model == code {
        Ok(model.is_some())
    } else {
        Err(format!(
            "{} at {}: the model plans {model:?}, the helper {code:?}",
            model::show_Verb(verb),
            model::show_R(r)
        ))
    }
}

/// The root's owner as `zfs get -H -o value,source` reads it.
fn read(owner: &Owner) -> Vec<(String, String)> {
    match owner {
        Owner::Unread => Vec::new(),
        Owner::Read(value, local) => vec![(
            value.to_string(),
            if *local {
                "local"
            } else {
                "inherited from tank"
            }
            .to_owned(),
        )],
    }
}

/// Hold the helper's admission to the model: the owner names the caller exactly when the model's
/// does, and the helper runs the verb exactly when the model admits it.
fn admitted_alike(r: &R, owner: &Owner, caller: u64, verb: &Verb) -> Result<(), String> {
    let uid = u32::try_from(caller).expect("a model uid fits a uid");
    let names = admit(&root(r), &read(owner), uid).is_ok();
    let admits = names && plan(&root(r), &arguments(verb)).is_ok();
    let (model_names, model_admits) = (
        model::names(owner.clone(), caller),
        model::admits(r.clone(), owner.clone(), caller, verb.clone()),
    );
    if (names, admits) == (model_names, model_admits) {
        Ok(())
    } else {
        Err(format!(
            "{} by {caller} under {} at {}: the helper names the caller {names} and admits {admits}, \
             the model {model_names} and {model_admits}",
            model::show_Verb(verb),
            model::show_Owner(owner),
            model::show_R(r)
        ))
    }
}

//#region smoo/cowshed!n/zfs-helper-admission
/// Every enumerated root and verb of the model's corpus, the verb as drawn and placed below the
/// root, and the model's own containment law on it.
#[test]
fn the_helper_plans_as_the_model_on_its_corpus() {
    let mut admitted = 0;
    for (r, verb) in model::corpus_a_plan_stays_below_its_root() {
        let (holds, value) = model::law_a_plan_stays_below_its_root(r.clone(), verb.clone());
        assert!(
            holds,
            "the model breaks its own law on {}: {value}",
            model::show_Verb(&verb)
        );
        planned_alike(&r, &verb).unwrap();
        admitted += usize::from(planned_alike(&r, &placed(&r, &verb)).unwrap());
    }
    assert!(admitted > 0, "the corpus admits no verb below its root");
}

/// The model's seeded draws: the same stream every target draws.
#[test]
fn the_helper_plans_as_the_model_on_seeded_inputs() {
    let mut rng = model::Rng(model::SEED_A_PLAN_STAYS_BELOW_ITS_ROOT);
    for _ in 0..model::PROPS_A_PLAN_STAYS_BELOW_ITS_ROOT {
        let r = model::gen_R(&mut rng, model::PROP_LEN_A_PLAN_STAYS_BELOW_ITS_ROOT);
        let verb = model::gen_Verb(&mut rng, model::PROP_LEN_A_PLAN_STAYS_BELOW_ITS_ROOT);
        planned_alike(&r, &verb).unwrap();
        planned_alike(&r, &placed(&r, &verb)).unwrap();
    }
}

/// Promote takes a build dataset, `<root>/projects/<owner>/<repo>/build/<id>` with an id that is
/// not hidden, and nothing beside one: the model's draws never reach a build dataset's depth.
#[test]
fn the_helper_promotes_only_a_build_dataset_as_the_model_does() {
    let mut promoted = 0;
    let roots = [
        R::R(5, list(Vec::new())),
        R::R(5, list(vec![6, 0])),
        R::R(0, list(vec![1, 3])),
    ];
    for r in &roots {
        for (o, n, id) in
            (0..6).flat_map(|o| (0..6).flat_map(move |n| (0..6).map(move |id| (o, n, id))))
        {
            for tail in [
                vec![1, o, n, 3, id],
                vec![1, o, n, 2, id],
                vec![1, o, n, 3],
                vec![1, o, n, 3, id, 5],
                vec![o, o, n, 3, id],
            ] {
                let verb = Verb::VPromote(model::cat(model::root_names(r.clone()), list(tail)));
                promoted += usize::from(planned_alike(r, &verb).unwrap());
            }
        }
    }
    assert!(promoted > 0, "no build dataset was promoted");
}

/// The model's admission corpus: the owner as read, the caller, and the verb as drawn and placed
/// below the root, and the model's own law that only an owner naming the caller admits.
#[test]
fn the_helper_admits_as_the_model_on_its_corpus() {
    let mut admitted = 0;
    for (r, owner, caller, verb) in model::corpus_admit_check() {
        let (holds, value) = model::law_admit_check(r.clone(), owner.clone(), caller, verb.clone());
        assert!(
            holds,
            "the model breaks its own law on {}: {value}",
            model::show_Owner(&owner)
        );
        let placed = placed(&r, &verb);
        admitted_alike(&r, &owner, caller, &verb).unwrap();
        admitted_alike(&r, &owner, caller, &placed).unwrap();
        admitted += usize::from(model::admits(r.clone(), owner.clone(), caller, placed));
    }
    assert!(admitted > 0, "the corpus admits no verb");
}

#[test]
fn the_helper_admits_as_the_model_on_seeded_inputs() {
    let mut rng = model::Rng(model::SEED_ADMIT_CHECK);
    for _ in 0..model::PROPS_ADMIT_CHECK {
        let r = model::gen_R(&mut rng, model::PROP_LEN_ADMIT_CHECK);
        let owner = model::gen_Owner(&mut rng, model::PROP_LEN_ADMIT_CHECK);
        let caller = model::gen_Nat(&mut rng, model::PROP_LEN_ADMIT_CHECK);
        let verb = model::gen_Verb(&mut rng, model::PROP_LEN_ADMIT_CHECK);
        admitted_alike(&r, &owner, caller, &verb).unwrap();
        admitted_alike(&r, &owner, caller, &placed(&r, &verb)).unwrap();
    }
}
//#endregion smoo/cowshed!n/zfs-helper-admission
