//! `root_steps` and `named_parent` bound to their proven model, `specs/cowshed/laws/zfs_substrate.bend`
//! (01b_zfs.md, "One pool, one root"; 09_substrates.md, "Privileged helper"). The model's laws are
//! proven for every named parent, set of serving roots, caller, platform and evidence; this driver
//! shows the code is the model. It runs the emitted model and the code on the same inputs (the
//! model's corpus and its seeded draws, each also with every root it reads right for the caller,
//! and with each property of those wrong in turn), and compares the steps, that the evidence is
//! read for exactly the roots the model reads and in its order, that a creation runs with its
//! properties in the planned order, and that running a refusal's remedy has the root taken.

use std::rc::Rc;

use cowshed_helper::name::Root;

use super::{
    NamedParentError, ROOT_APPLE_PROPERTIES, Read, RootEvidence, RootStep, create_command,
    named_parent, root_steps,
};
use crate::storage::bootstrap::ZFS_OWNER_PROPERTY;

// Regenerate with bun run --filter @smoothbricks/cowshed emit-zfs-substrate-law.
#[rustfmt::skip]
mod model;

use model::{Ev, Fix, List, Named, Owner, Parent, R, Step, Wrong};

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

/// A model component as a name component: the components the rule names, and a plain name for
/// every other.
fn component(n: u64) -> String {
    match n {
        0 => "cowshed".to_owned(),
        1 => "projects".to_owned(),
        2 => "main".to_owned(),
        3 => "build".to_owned(),
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

/// The parent a model root lies in, `pool/parents…`.
fn parent_name(r: &R) -> String {
    let R::R(pool, parents) = r;
    name(&list(
        std::iter::once(*pool).chain(items(parents)).collect(),
    ))
}

fn uid(u: u64) -> u32 {
    u32::try_from(u).expect("a model uid fits a uid")
}

/// The owner and the four com.apple properties, in the order the model's `W` names them.
fn properties() -> [&'static str; 5] {
    let [devdisk, browse, mimic, ignoreowner] = ROOT_APPLE_PROPERTIES;
    [ZFS_OWNER_PROPERTY, devdisk, browse, mimic, ignoreowner]
}

/// A property as `zfs get` reads it: a model value, set locally or inherited.
fn read(owner: &Owner, value: impl Fn(u64) -> String) -> Option<Read> {
    match owner {
        Owner::Unread => None,
        Owner::Read(n, local) => Some(Read {
            value: value(*n),
            source: if *local { "local" } else { "inherited from c9" }.to_owned(),
        }),
    }
}

/// A com.apple model value: 0 is `off`.
fn apple_value(n: u64) -> String {
    match n {
        0 => "off".to_owned(),
        1 => "on".to_owned(),
        other => format!("v{other}"),
    }
}

fn evidence(ev: &Ev) -> RootEvidence {
    match ev {
        Ev::Absent => RootEvidence::Absent,
        Ev::Orphan => RootEvidence::Orphan,
        Ev::Present(owner, devdisk, browse, mimic, ignoreowner) => RootEvidence::Present {
            owner: read(owner, |n| n.to_string()),
            apple: [devdisk, browse, mimic, ignoreowner].map(|p| read(p, apple_value)),
        },
    }
}

/// A step as both sides can state it: a refusal by the properties its reason names and its
/// exact remedy.
#[derive(Debug, PartialEq)]
enum Stated {
    Nothing,
    NameAPool,
    Create {
        argv: Vec<String>,
    },
    Take(String),
    Refuse {
        root: String,
        wrong: Vec<&'static str>,
        remedy: String,
    },
}

/// The argv the model's creation of `root` runs, spelled from 01b_zfs.md: `mountpoint=none`, the
/// caller's owner, and on macOS the four com.apple properties `off`.
fn creation_argv(root: &str, uid: u32, apple: bool) -> Vec<String> {
    let mut argv = vec!["zfs", "create", "-o", "mountpoint=none", "-o"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    argv.push(format!("cowshed:owner={uid}"));
    if apple {
        for property in ROOT_APPLE_PROPERTIES {
            argv.extend(["-o".to_owned(), format!("{property}=off")]);
        }
    }
    argv.push(root.to_owned());
    argv
}

fn model_stated(step: &Step, uid: u32, apple: bool) -> Stated {
    match step {
        Step::SNothing => Stated::Nothing,
        Step::SNameAPool => Stated::NameAPool,
        Step::SCreate(r, created_apple) => Stated::Create {
            argv: creation_argv(&name(r), uid, *created_apple),
        },
        Step::STake(r) => Stated::Take(name(r)),
        Step::SRefuse(r, Wrong::W(owner, devdisk, browse, mimic, ignoreowner)) => {
            let wrong: Vec<&'static str> = properties()
                .into_iter()
                .zip([owner, devdisk, browse, mimic, ignoreowner])
                .filter_map(|(property, wrong)| wrong.then_some(property))
                .collect();
            let settings = wrong
                .iter()
                .map(|property| {
                    let value = if *property == ZFS_OWNER_PROPERTY {
                        uid.to_string()
                    } else {
                        "off".to_owned()
                    };
                    format!("{property}={value}")
                })
                .collect::<Vec<_>>()
                .join(" ");
            Stated::Refuse {
                root: name(r),
                wrong,
                remedy: format!("zfs set {settings} {}", name(r)),
            }
        }
        // A serving root that is gone: refused, naming no property, with the command that creates
        // it, which is the administrator's to run.
        Step::SGone(r) => Stated::Refuse {
            root: name(r),
            wrong: Vec::new(),
            remedy: creation_argv(&name(r), uid, apple).join(" "),
        },
        // A root whose parent does not exist: refused, naming no property, with the one command
        // that makes its parent.
        Step::SOrphan(r, fix) => Stated::Refuse {
            root: name(r),
            wrong: Vec::new(),
            remedy: match fix {
                Fix::Import(pool) => format!("zpool import {}", component(*pool)),
                Fix::MakeParent(parent) => {
                    format!("zfs create -o mountpoint=none {}", name(parent))
                }
            },
        },
    }
}

fn code_stated(step: &RootStep) -> Stated {
    match step {
        RootStep::Nothing => Stated::Nothing,
        RootStep::NameAPool => Stated::NameAPool,
        RootStep::Create { .. } => Stated::Create {
            argv: create_command(step).expect("a Create has a command"),
        },
        RootStep::Take { root } => Stated::Take(root.clone()),
        RootStep::Refuse { root, why, remedy } => {
            let acted_on = remedy
                .rsplit(' ')
                .next()
                .expect("a remedy names what it acts on");
            assert!(
                why.contains(acted_on),
                "{why:?} names {acted_on}, which {remedy:?} acts on"
            );
            Stated::Refuse {
                root: root.clone(),
                wrong: properties()
                    .into_iter()
                    .filter(|property| why.contains(&format!("{property} is")))
                    .collect(),
                remedy: remedy.clone(),
            }
        }
    }
}

/// `evidence` with every property the remedy `zfs set <p>=<v> … <root>` names set locally.
fn remedied(remedy: &str, evidence: &RootEvidence) -> RootEvidence {
    let RootEvidence::Present { owner, apple } = evidence else {
        panic!("only an existing root is remedied by zfs set");
    };
    let (mut owner, mut apple) = (owner.clone(), apple.clone());
    let words: Vec<&str> = remedy.split(' ').collect();
    let [_zfs, "set", settings @ .., _root] = words.as_slice() else {
        panic!("{remedy:?} is not zfs set");
    };
    for setting in settings {
        let (property, value) = setting.split_once('=').expect("a setting is p=v");
        let set = Some(Read {
            value: value.to_owned(),
            source: "local".to_owned(),
        });
        match ROOT_APPLE_PROPERTIES
            .iter()
            .position(|apple| *apple == property)
        {
            Some(index) => apple[index] = set,
            None => {
                assert_eq!(property, ZFS_OWNER_PROPERTY, "{remedy:?}");
                owner = set;
            }
        }
    }
    RootEvidence::Present { owner, apple }
}

/// What the corpus reached, so a driver that compares nothing fails.
#[derive(Default)]
struct Reached {
    created: usize,
    taken: usize,
    refused: usize,
    orphaned: usize,
}

/// Hold `root_steps` to the model on one input: the same steps; the evidence read for exactly the
/// roots `reads` names, in order; and every refusal of an existing root taken once its remedy runs.
fn stepped_alike(
    zfs: bool,
    named: &Named,
    serving: &Rc<List<R>>,
    u: u64,
    apple: bool,
    read: &Rc<List<Ev>>,
    reached: &mut Reached,
) -> Result<(), String> {
    let caller = uid(u);
    let named_root = match named {
        Named::Unnamed => None,
        Named::Name(r) => Some(root(r)),
    };
    let serving_roots: Vec<Root> = items(serving).iter().map(root).collect();
    let mut given = items(read).into_iter().map(|ev| evidence(&ev));
    let mut asked = Vec::new();
    let steps = root_steps(
        zfs,
        named_root.as_ref(),
        &serving_roots,
        caller,
        apple,
        |root| {
            let evidence = given.next().unwrap_or(RootEvidence::Absent);
            asked.push((root.clone(), evidence.clone()));
            Ok::<_, ()>(evidence)
        },
    )
    .expect("the evidence reads");
    let code: Vec<Stated> = steps.iter().map(code_stated).collect();
    let model: Vec<Stated> = items(&model::root_steps(
        zfs,
        named.clone(),
        serving.clone(),
        u,
        apple,
        read.clone(),
    ))
    .iter()
    .map(|step| model_stated(step, caller, apple))
    .collect();
    let code_reads: Vec<String> = asked
        .iter()
        .map(|(root, _)| root.dataset().to_string())
        .collect();
    let model_reads: Vec<String> = items(&model::reads(zfs, named.clone(), serving.clone()))
        .iter()
        .map(name)
        .collect();
    if code != model || code_reads != model_reads {
        return Err(format!(
            "zfs {zfs}, named {}, serving {}, uid {u}, apple {apple}, evidence {}: root_steps \
             reads {code_reads:?} and steps {code:?}, the model reads {model_reads:?} and steps \
             {model:?}",
            model::show_Named(named),
            model::show_ListR(serving),
            model::show_ListEv(read)
        ));
    }
    if model_reads.is_empty() {
        return Ok(());
    }
    for (step, (root, evidence)) in steps.iter().zip(&asked) {
        match (step, evidence) {
            (RootStep::Create { .. }, _) => reached.created += 1,
            (RootStep::Take { .. }, _) => reached.taken += 1,
            (RootStep::Refuse { remedy, .. }, RootEvidence::Present { .. }) => {
                reached.refused += 1;
                let mended = remedied(remedy, evidence);
                let after = root_steps(true, Some(root), &[], caller, apple, |_| {
                    Ok::<_, ()>(mended.clone())
                })
                .expect("the evidence reads");
                if after
                    != [RootStep::Take {
                        root: root.dataset().to_string(),
                    }]
                {
                    return Err(format!(
                        "{remedy:?} on {evidence:?} leaves {after:?}, not the root taken"
                    ));
                }
            }
            // Once the remedy makes its parent, the root is absent beneath an existing parent:
            // named, setup creates it.
            (RootStep::Refuse { remedy, .. }, RootEvidence::Orphan) => {
                reached.orphaned += 1;
                let after = root_steps(true, Some(root), &[], caller, apple, |_| {
                    Ok::<_, ()>(RootEvidence::Absent)
                })
                .expect("the evidence reads");
                if !matches!(after.as_slice(), [RootStep::Create { root: created, .. }] if *created == root.dataset().to_string())
                {
                    return Err(format!(
                        "{remedy:?} for {root:?} leaves {after:?}, not the root created"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Evidence right for `u`: its owner and its com.apple properties set locally as they should be.
fn right(u: u64) -> [Owner; 5] {
    [
        Owner::Read(u, true),
        Owner::Read(0, true),
        Owner::Read(0, true),
        Owner::Read(0, true),
        Owner::Read(0, true),
    ]
}

/// The evidence as drawn, every root read right for the caller, and each property of those wrong
/// in turn (another value, then inherited): the draws rarely reach a root that is right.
fn variants(read: &Rc<List<Ev>>, reads: usize, u: u64) -> Vec<Rc<List<Ev>>> {
    let present = |p: [Owner; 5]| {
        let [owner, devdisk, browse, mimic, ignoreowner] = p;
        list(vec![
            Ev::Present(owner, devdisk, browse, mimic, ignoreowner);
            reads.max(1)
        ])
    };
    let mut all = vec![
        read.clone(),
        present(right(u)),
        list(vec![Ev::Orphan; reads.max(1)]),
    ];
    for index in 0..5 {
        for wrong in [Owner::Read(if index == 0 { u + 1 } else { 1 }, true), {
            let Owner::Read(value, _) = right(u)[index].clone() else {
                unreachable!("right evidence is read");
            };
            Owner::Read(value, false)
        }] {
            let mut p = right(u);
            p[index] = wrong;
            all.push(present(p));
        }
    }
    all
}

fn stepped_alike_varied(
    zfs: bool,
    named: &Named,
    serving: &Rc<List<R>>,
    u: u64,
    apple: bool,
    read: &Rc<List<Ev>>,
    reached: &mut Reached,
) -> Result<(), String> {
    let reads = items(&model::reads(zfs, named.clone(), serving.clone())).len();
    for read in variants(read, reads, u) {
        stepped_alike(zfs, named, serving, u, apple, &read, reached)?;
    }
    Ok(())
}

/// Hold `named_parent` to the model's `parent`, the parents spelled plainly and spelled loosely.
fn named_alike(cli: &Named, declared: &Named) -> Result<(), String> {
    let model = match model::parent(cli.clone(), declared.clone()) {
        Parent::Agreed(Named::Unnamed) => Ok(None),
        Parent::Agreed(Named::Name(r)) => Ok(Some(root(&r))),
        Parent::Disagree(x, y) => Err(NamedParentError::Conflict {
            cli: parent_name(&x),
            declared: parent_name(&y),
        }),
    };
    let spelled = |named: &Named, loose: bool| match named {
        Named::Unnamed => None,
        Named::Name(r) if loose => Some(format!(" {}/ ", parent_name(r))),
        Named::Name(r) => Some(parent_name(r)),
    };
    for loose in [false, true] {
        let (cli_parent, declared_parent) = (spelled(cli, loose), spelled(declared, !loose));
        let code = named_parent(cli_parent.as_deref(), declared_parent.as_deref());
        if code != model {
            return Err(format!(
                "{cli_parent:?} and {declared_parent:?}: named_parent answers {code:?}, the model \
                 {model:?} ({})",
                model::show_Parent(&model::parent(cli.clone(), declared.clone()))
            ));
        }
    }
    Ok(())
}

//#region smoo/cowshed!n/zfs-root-setup
/// Every enumerated input of the model's corpus, and the model's own law on it.
#[test]
fn setup_serves_the_root_as_the_model_on_its_corpus() {
    let mut reached = Reached::default();
    for (zfs, named, serving, u, apple, read) in model::corpus_created_check() {
        let (holds, value) =
            model::law_created_check(zfs, named.clone(), serving.clone(), u, apple, read.clone());
        assert!(
            holds,
            "the model breaks its own law on {}: {value}",
            model::show_Named(&named)
        );
        stepped_alike_varied(zfs, &named, &serving, u, apple, &read, &mut reached).unwrap();
    }
    assert!(
        reached.created > 0 && reached.taken > 0 && reached.refused > 0 && reached.orphaned > 0,
        "the corpus creates {}, takes {}, refuses {} and finds orphaned {} roots",
        reached.created,
        reached.taken,
        reached.refused,
        reached.orphaned
    );
}

/// The model's seeded draws: the same stream every target draws.
#[test]
fn setup_serves_the_root_as_the_model_on_seeded_inputs() {
    let mut reached = Reached::default();
    let mut rng = model::Rng(model::SEED_CREATED_CHECK);
    for _ in 0..model::PROPS_CREATED_CHECK {
        let zfs = model::gen_Bool(&mut rng, model::PROP_LEN_CREATED_CHECK);
        let named = model::gen_Named(&mut rng, model::PROP_LEN_CREATED_CHECK);
        let serving = model::gen_ListR(&mut rng, model::PROP_LEN_CREATED_CHECK);
        let u = model::gen_Nat(&mut rng, model::PROP_LEN_CREATED_CHECK);
        let apple = model::gen_Bool(&mut rng, model::PROP_LEN_CREATED_CHECK);
        let read = model::gen_ListEv(&mut rng, model::PROP_LEN_CREATED_CHECK);
        stepped_alike_varied(zfs, &named, &serving, u, apple, &read, &mut reached).unwrap();
    }
}

/// The parent the command line and `.cowshed.toml` name, on the model's corpus and draws.
#[test]
fn the_named_parent_is_the_models() {
    for (cli, declared) in model::corpus_parent_check() {
        let (holds, value) = model::law_parent_check(cli.clone(), declared.clone());
        assert!(holds, "the model breaks its own law: {value}");
        named_alike(&cli, &declared).unwrap();
        named_alike(&cli, &cli).unwrap();
    }
    let mut rng = model::Rng(model::SEED_PARENT_CHECK);
    for _ in 0..model::PROPS_PARENT_CHECK {
        let cli = model::gen_Named(&mut rng, model::PROP_LEN_PARENT_CHECK);
        let declared = model::gen_Named(&mut rng, model::PROP_LEN_PARENT_CHECK);
        named_alike(&cli, &declared).unwrap();
    }
}
//#endregion smoo/cowshed!n/zfs-root-setup
