//! The ZFS root `cowshed setup` serves the user from (01b_zfs.md, "One pool, one root";
//! 09_substrates.md, "Privileged helper"): the parent the operator names, and whether setup
//! creates, takes or refuses `<parent>/cowshed`. A pure core over what `zfs get` reads; each
//! platform's setup reads that evidence and executes or reports the steps.
//! `specs/cowshed/laws/zfs_substrate.bend` proves its laws, and `laws.rs` holds this code to the
//! model on its corpus and seeded draws.

use cowshed_helper::name::{NameError, ROOT_COMPONENT, Root};
use thiserror::Error;

use super::{ZFS, ZFS_OWNER_PROPERTY};

/// The macOS properties of "No disk per dataset" (01b_zfs.md): `off`, set locally on the root,
/// and inherited by every dataset below it.
pub const ROOT_APPLE_PROPERTIES: [&str; 4] = [
    "com.apple.devdisk",
    "com.apple.browse",
    "com.apple.mimic",
    "com.apple.ignoreowner",
];

/// What each com.apple property holds on the root.
const OFF: &str = "off";
/// The source `zfs get` reports for a property set on the dataset itself.
const LOCAL: &str = "local";

/// One property as `zfs get -H -o value,source` reads it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Read {
    pub value: String,
    pub source: String,
}

/// What `zfs get` reads of a root: no such dataset beneath an existing parent, no such dataset
/// and no parent either (an orphan: `zfs create` beneath it could only fail), or its `cowshed:owner` and its com.apple
/// properties in [`ROOT_APPLE_PROPERTIES`] order, each `None` when it is not set.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "one is read per root and matched at once; boxing Present would allocate for nothing"
)]
pub enum RootEvidence {
    Absent,
    Orphan,
    Present {
        owner: Option<Read>,
        apple: [Option<Read>; 4],
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootStep {
    /// The host has no zfs: nothing to say or do.
    Nothing,
    /// zfs is there, nothing is named and no root names the caller: say how to name a pool; never
    /// guess one.
    NameAPool,
    /// Create the root with exactly these `-o` properties, in order: mountpoint=none,
    /// cowshed:owner=<uid>, and, when `apple`, the four com.apple properties =off.
    Create {
        root: String,
        properties: Vec<(String, String)>,
    },
    /// The root exists with cowshed:owner local and equal to uid (and, when `apple`, all four
    /// com.apple local and off): taken, nothing changes.
    Take { root: String },
    /// The root is wrong: `why` names each wrong property, and `remedy` is the exact admin command
    /// that makes it right, `zfs set <p>=<v> … <root>` setting only the wrong ones. A serving root
    /// `zfs get` no longer finds is refused too, never created, with the command that creates it,
    /// and so is a root whose parent does not exist, with the one command that makes that parent.
    Refuse {
        root: String,
        why: String,
        remedy: String,
    },
}

/// Why the parents the operator names name no root.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NamedParentError {
    /// The command line and `.cowshed.toml` name different parents: cowshed never picks one.
    #[error(
        "the ZFS parent the command line names, {cli:?}, is not the one .cowshed.toml declares, {declared:?}"
    )]
    Conflict { cli: String, declared: String },
    /// `<parent>/cowshed` is no dataset name.
    #[error(
        "the ZFS parent {parent:?} names no cowshed root {parent}/{}: {error}",
        ROOT_COMPONENT
    )]
    Invalid { parent: String, error: NameError },
}

/// The root named by a parent on the command line and one a `.cowshed.toml` declares: either
/// alone, or both when they agree; a disagreement names both. Surrounding whitespace and trailing
/// `/` carry no meaning in a parent.
pub fn named_parent(
    cli: Option<&str>,
    declared: Option<&str>,
) -> Result<Option<Root>, NamedParentError> {
    let parent = match (cli.map(spelled), declared.map(spelled)) {
        (None, None) => return Ok(None),
        (Some(cli), Some(declared)) if cli != declared => {
            return Err(NamedParentError::Conflict {
                cli: cli.to_owned(),
                declared: declared.to_owned(),
            });
        }
        (Some(parent), _) | (None, Some(parent)) => parent,
    };
    Root::parse(&format!("{parent}/{ROOT_COMPONENT}"))
        .map(Some)
        .map_err(|error| NamedParentError::Invalid {
            parent: parent.to_owned(),
            error,
        })
}

fn spelled(parent: &str) -> &str {
    parent.trim().trim_end_matches('/')
}

/// The step(s) setup takes for the ZFS root:
/// - `!zfs` gives [Nothing].
/// - With `named` Some(root), read the evidence of that root only, then Create, Take or Refuse.
/// - With `named` None, and `serving` the roots whose cowshed:owner names the caller (already
///   read): none gives [NameAPool]; otherwise one Take or Refuse per serving root, from the
///   evidence of each, in order. Nothing unnamed is ever created.
///
/// `apple` is true on macOS.
pub fn root_steps<E>(
    zfs: bool,
    named: Option<&Root>,
    serving: &[Root],
    uid: u32,
    apple: bool,
    mut evidence: impl FnMut(&Root) -> Result<RootEvidence, E>,
) -> Result<Vec<RootStep>, E> {
    if !zfs {
        return Ok(vec![RootStep::Nothing]);
    }
    if let Some(root) = named {
        return Ok(vec![match evidence(root)? {
            RootEvidence::Absent => creation(root, uid, apple),
            RootEvidence::Orphan => orphan(root),
            RootEvidence::Present { owner, apple: read } => judge(root, uid, apple, owner, read),
        }]);
    }
    if serving.is_empty() {
        return Ok(vec![RootStep::NameAPool]);
    }
    serving
        .iter()
        .map(|root| {
            Ok(match evidence(root)? {
                RootEvidence::Absent => gone(root, uid, apple),
                RootEvidence::Orphan => orphan(root),
                RootEvidence::Present { owner, apple: read } => {
                    judge(root, uid, apple, owner, read)
                }
            })
        })
        .collect()
}

/// The argv of a [`RootStep::Create`], `zfs create -o <p>=<v> … <root>` with its properties in
/// order; `None` for every other step.
pub fn create_command(step: &RootStep) -> Option<Vec<String>> {
    let RootStep::Create { root, properties } = step else {
        return None;
    };
    Some(
        [ZFS, "create"]
            .map(str::to_owned)
            .into_iter()
            .chain(
                properties
                    .iter()
                    .flat_map(|(property, value)| ["-o".to_owned(), format!("{property}={value}")]),
            )
            .chain([root.clone()])
            .collect(),
    )
}

/// The properties the root holds for `uid`, in the order a remedy sets them: its owner, and on
/// macOS the four com.apple properties.
fn required(uid: u32, apple: bool) -> impl Iterator<Item = (&'static str, String)> {
    std::iter::once((ZFS_OWNER_PROPERTY, uid.to_string())).chain(
        ROOT_APPLE_PROPERTIES
            .into_iter()
            .filter(move |_| apple)
            .map(|property| (property, OFF.to_owned())),
    )
}

fn creation(root: &Root, uid: u32, apple: bool) -> RootStep {
    RootStep::Create {
        root: root.dataset().to_string(),
        properties: std::iter::once(("mountpoint", "none".to_owned()))
            .chain(required(uid, apple))
            .map(|(property, value)| (property.to_owned(), value))
            .collect(),
    }
}

/// A root the caller's `cowshed:owner` placed among the serving roots, gone when `zfs get` read it.
/// A root whose parent does not exist: `zfs create` beneath it could only fail, after the
/// administrator's authorization was spent on it. It is refused with the one command that makes
/// that parent, the operator's to run: the operator owns the pool, and only the named parent is
/// touched (01b_zfs.md, "One pool, one root"), so never `zfs create -p`.
fn orphan(root: &Root) -> RootStep {
    let name = root.dataset().as_str();
    let (parent, _) = name
        .rsplit_once('/')
        .expect("a root is named <parent>/cowshed");
    let (why, remedy) = if parent.contains('/') {
        (
            format!("the parent {parent} of {name} does not exist"),
            format!("{ZFS} create -o mountpoint=none {parent}"),
        )
    } else {
        (
            format!("the pool {parent} is not imported or does not exist"),
            format!("zpool import {parent}"),
        )
    };
    RootStep::Refuse {
        root: name.to_owned(),
        why,
        remedy,
    }
}
fn gone(root: &Root, uid: u32, apple: bool) -> RootStep {
    let create = create_command(&creation(root, uid, apple))
        .expect("a creation is a Create")
        .join(" ");
    RootStep::Refuse {
        root: root.dataset().to_string(),
        why: format!(
            "ZFS root {} names uid {uid} as its owner, but zfs get no longer finds it",
            root.dataset()
        ),
        remedy: create,
    }
}

/// An existing root: taken when every required property is set locally to its value, else
/// refused naming each one that is not.
fn judge(
    root: &Root,
    uid: u32,
    apple: bool,
    owner: Option<Read>,
    read: [Option<Read>; 4],
) -> RootStep {
    let wrong: Vec<((&str, String), Option<Read>)> = required(uid, apple)
        .zip(std::iter::once(owner).chain(read))
        .filter(|((_, value), read)| {
            !read
                .as_ref()
                .is_some_and(|read| read.value == *value && read.source == LOCAL)
        })
        .collect();
    if wrong.is_empty() {
        return RootStep::Take {
            root: root.dataset().to_string(),
        };
    }
    let why = wrong
        .iter()
        .map(|((property, _), read)| match read {
            Some(Read { value, source }) => format!("{property} is {value} ({source})"),
            None => format!("{property} is not set"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let settings = wrong
        .iter()
        .map(|((property, value), _)| format!("{property}={value}"))
        .collect::<Vec<_>>()
        .join(" ");
    RootStep::Refuse {
        root: root.dataset().to_string(),
        why: format!(
            "ZFS root {} does not serve uid {uid}: {why}",
            root.dataset()
        ),
        remedy: format!("{ZFS} set {settings} {}", root.dataset()),
    }
}

#[cfg(test)]
mod laws;

#[cfg(test)]
mod tests {
    use super::*;

    fn local(value: &str) -> Option<Read> {
        Some(Read {
            value: value.to_owned(),
            source: LOCAL.to_owned(),
        })
    }

    /// A parent is named however the operator spells it, refused when it names no root.
    #[test]
    fn a_parent_is_named_as_spelled_and_refused_when_it_names_no_root() {
        let tank = Root::parse("tank/a/cowshed").unwrap();
        assert_eq!(
            named_parent(Some(" tank/a/ "), Some("tank/a")),
            Ok(Some(tank))
        );
        assert_eq!(
            named_parent(Some("tank/../x"), None),
            Err(NamedParentError::Invalid {
                parent: "tank/../x".to_owned(),
                error: NameError::Component("..".to_owned()),
            })
        );
    }

    /// The creation runs as one argv in the planned order; a wrong root's remedy sets only what
    /// is wrong, and its refusal names each.
    #[test]
    fn the_root_is_created_in_order_and_a_wrong_one_names_its_remedy() {
        let root = Root::parse("tank/cowshed").unwrap();
        let steps = |evidence: RootEvidence| {
            root_steps(true, Some(&root), &[], 501, true, |_| {
                Ok::<_, ()>(evidence.clone())
            })
            .unwrap()
        };
        assert_eq!(
            create_command(&steps(RootEvidence::Absent)[0]).unwrap(),
            [
                "zfs",
                "create",
                "-o",
                "mountpoint=none",
                "-o",
                "cowshed:owner=501",
                "-o",
                "com.apple.devdisk=off",
                "-o",
                "com.apple.browse=off",
                "-o",
                "com.apple.mimic=off",
                "-o",
                "com.apple.ignoreowner=off",
                "tank/cowshed",
            ]
        );
        assert_eq!(
            steps(RootEvidence::Present {
                owner: Some(Read {
                    value: "501".to_owned(),
                    source: "inherited from tank".to_owned(),
                }),
                apple: [local("off"), local("on"), local("off"), None],
            }),
            [RootStep::Refuse {
                root: "tank/cowshed".to_owned(),
                why: "ZFS root tank/cowshed does not serve uid 501: cowshed:owner is 501 \
                      (inherited from tank), com.apple.browse is on (local), \
                      com.apple.ignoreowner is not set"
                    .to_owned(),
                remedy: "zfs set cowshed:owner=501 com.apple.browse=off com.apple.ignoreowner=off \
                         tank/cowshed"
                    .to_owned(),
            }]
        );
    }
}
