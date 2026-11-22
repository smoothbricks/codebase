//! The fixed verb set: a caller's arguments become the argument vector of at most one `zfs`
//! command, or a refusal. Pure: nothing here runs a command or reads the host.

use std::fmt;
use std::path::{Component, Path, PathBuf};

use cowshed_helper::name::{Dataset, NameError, Root, Snapshot};

/// The user property on a root dataset that names the uid cowshed serves there. Only whoever
/// administers the root's parent can set it, so no verb accepts it.
pub const OWNER_PROPERTY: &str = "cowshed:owner";

/// Native properties a verb may set, each taking one word ([`word`]). Everything else is refused
/// by name: `zoned`, `sharenfs`, `overlay`, `setuid`, `devices`, `exec`, `encryption` and
/// `keylocation` among them are never cowshed's to change.
const WORD_PROPERTIES: [&str; 13] = [
    "atime",
    "canmount",
    "compression",
    "dedup",
    "dnodesize",
    "recordsize",
    "refquota",
    "sync",
    "xattr",
    "com.apple.browse",
    "com.apple.devdisk",
    "com.apple.ignoreowner",
    "com.apple.mimic",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Refusal {
    Usage,
    UnknownVerb(String),
    Name {
        argument: String,
        error: NameError,
    },
    Outside {
        dataset: String,
    },
    NotABuildVolume {
        dataset: String,
    },
    Property {
        property: String,
    },
    Value {
        property: String,
        value: String,
    },
    Mountpoint {
        value: String,
    },
    /// The root's owner, read as `(value, source)`, is not set locally to the caller's uid.
    Owner {
        root: Dataset,
        value: String,
        source: String,
        uid: u32,
    },
    /// The root's owner could not be read as one value with its source.
    OwnerUnread {
        root: Dataset,
    },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => f.write_str(USAGE),
            Self::UnknownVerb(verb) => write!(f, "{verb:?} is not a verb\n{USAGE}"),
            Self::Name { argument, error } => write!(f, "{argument:?}: {error}"),
            Self::Outside { dataset } => write!(f, "{dataset} is not below the cowshed root"),
            Self::NotABuildVolume { dataset } => write!(
                f,
                "{dataset} is not a build dataset (<root>/projects/<owner>/<repo>/build/<id>)"
            ),
            Self::Property { property } => write!(f, "property {property:?} is not settable"),
            Self::Value { property, value } => {
                write!(f, "{value:?} is not a value of {property}")
            }
            Self::Mountpoint { value } => write!(
                f,
                "mountpoint {value:?} is neither `none` nor an absolute path without `.`, `..` or a repeated `/`"
            ),
            Self::Owner {
                root,
                value,
                source,
                uid,
            } => write!(
                f,
                "{root} has {OWNER_PROPERTY}={value} ({source}); it must be set locally to the caller's uid, {uid}"
            ),
            Self::OwnerUnread { root } => write!(f, "cannot read {OWNER_PROPERTY} of {root}"),
        }
    }
}

pub const USAGE: &str = "usage: cowshed-helper <root> <verb> …
  check
  create   <dataset> [<property>=<value>…]
  clone    <dataset>@<snapshot> <dataset> [<property>=<value>…]
  snapshot <dataset>@<snapshot>
  destroy  <dataset>|<dataset>@<snapshot> [--defer]
  rename   <dataset> <dataset> | <dataset>@<snapshot> <dataset>@<snapshot>
  promote  <build dataset>
  mount    <dataset>
  unmount  <dataset> [--force]
  set      <dataset> <property>=<value>…";

/// What one accepted invocation does.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Plan {
    /// The arguments of the one `zfs` command, or `None` for `check`, which runs none: the
    /// helper's own preconditions (it runs as root, and the root names the caller) are the whole
    /// of what it proves.
    pub zfs: Option<Vec<String>>,
    /// Every mountpoint path the command sets, for the host's ownership check.
    pub mountpoints: Vec<PathBuf>,
    /// The dataset `create` made: its mounted root goes to the caller, since a new dataset's
    /// root directory is root's. A clone needs none, its root carries its origin's owner.
    pub own: Option<Dataset>,
}

struct Properties {
    options: Vec<String>,
    mountpoints: Vec<PathBuf>,
}

/// One or more lowercase letters, digits, `.`, `_` or `-`: every enumerated or numeric property
/// value cowshed sets (`off`, `zstd-1`, `128K` as `131072`, `sa`).
fn word(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

/// `none`, or an absolute path of normal components only, so the string ZFS stores is the path
/// the host check walks.
fn mountpoint(value: &str) -> Result<Option<PathBuf>, Refusal> {
    if value == "none" {
        return Ok(None);
    }
    let refused = || Refusal::Mountpoint {
        value: value.to_owned(),
    };
    let path = Path::new(value);
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) {
        return Err(refused());
    }
    let mut rebuilt = PathBuf::from("/");
    for part in components {
        match part {
            Component::Normal(name) => rebuilt.push(name),
            _ => return Err(refused()),
        }
    }
    // `components` normalizes `a//b`, `a/./b` and a trailing `/`; refuse them rather than store
    // a string that differs from the path that was checked.
    if rebuilt.as_os_str() != path.as_os_str() || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(refused());
    }
    Ok(Some(rebuilt))
}

fn properties(arguments: &[&str]) -> Result<Properties, Refusal> {
    let mut parsed = Properties {
        options: Vec::with_capacity(arguments.len()),
        mountpoints: Vec::new(),
    };
    for argument in arguments {
        let (property, value) = argument.split_once('=').ok_or(Refusal::Usage)?;
        let bad_value = || Refusal::Value {
            property: property.to_owned(),
            value: value.to_owned(),
        };
        if property == "mountpoint" {
            parsed.mountpoints.extend(mountpoint(value)?);
        } else if WORD_PROPERTIES.contains(&property) {
            if !word(value) {
                return Err(bad_value());
            }
        } else if let Some(name) = property.strip_prefix("cowshed:")
            && property != OWNER_PROPERTY
            && word(name)
        {
            if value.bytes().any(|byte| byte.is_ascii_control()) {
                return Err(bad_value());
            }
        } else {
            return Err(Refusal::Property {
                property: property.to_owned(),
            });
        }
        parsed.options.push((*argument).to_owned());
    }
    Ok(parsed)
}

fn dataset(root: &Root, argument: &str) -> Result<Dataset, Refusal> {
    let dataset = Dataset::parse(argument).map_err(|error| Refusal::Name {
        argument: argument.to_owned(),
        error,
    })?;
    if root.holds(&dataset) {
        Ok(dataset)
    } else {
        Err(Refusal::Outside {
            dataset: argument.to_owned(),
        })
    }
}

fn snapshot(root: &Root, argument: &str) -> Result<Snapshot, Refusal> {
    let snapshot = Snapshot::parse(argument).map_err(|error| Refusal::Name {
        argument: argument.to_owned(),
        error,
    })?;
    if root.holds(snapshot.dataset()) {
        Ok(snapshot)
    } else {
        Err(Refusal::Outside {
            dataset: snapshot.dataset().to_string(),
        })
    }
}

fn options(properties: &Properties) -> impl Iterator<Item = String> + '_ {
    properties
        .options
        .iter()
        .flat_map(|option| ["-o".to_owned(), option.clone()])
}

fn command<const N: usize>(head: [&str; N]) -> Vec<String> {
    head.map(str::to_owned).to_vec()
}

/// Admit `uid` at `root` when the root's own `cowshed:owner`, read as the `(value, source)` lines
/// of `zfs get`, is set locally to that uid (09_substrates.md, "Privileged helper"): the property
/// on the root itself is the whole authorization. An inherited, received or default owner, another
/// uid, or a read that is not one line, admits nothing.
pub fn admit(root: &Root, owner: &[(String, String)], uid: u32) -> Result<(), Refusal> {
    match owner {
        [(value, source)] if *value == uid.to_string() && source == "local" => Ok(()),
        [(value, source)] => Err(Refusal::Owner {
            root: root.dataset().clone(),
            value: value.clone(),
            source: source.clone(),
            uid,
        }),
        _ => Err(Refusal::OwnerUnread {
            root: root.dataset().clone(),
        }),
    }
}
/// The plan for `arguments`, the helper's arguments after the root. No verb ever passes `-r`,
/// `-R`, `-f` to a destroy, or `-p`: one invocation touches the datasets it names and no other.
pub fn plan(root: &Root, arguments: &[String]) -> Result<Plan, Refusal> {
    let (verb, rest) = arguments.split_first().ok_or(Refusal::Usage)?;
    let rest: Vec<&str> = rest.iter().map(String::as_str).collect();
    let plain = |zfs: Vec<String>| Plan {
        zfs: Some(zfs),
        mountpoints: Vec::new(),
        own: None,
    };
    match (verb.as_str(), rest.as_slice()) {
        ("check", []) => Ok(Plan {
            zfs: None,
            mountpoints: Vec::new(),
            own: None,
        }),
        ("create", [name, set @ ..]) => {
            let created = dataset(root, name)?;
            let set = properties(set)?;
            let mut zfs = command(["create"]);
            zfs.extend(options(&set));
            zfs.push(created.to_string());
            Ok(Plan {
                zfs: Some(zfs),
                mountpoints: set.mountpoints,
                own: Some(created),
            })
        }
        ("clone", [origin, name, set @ ..]) => {
            let origin = snapshot(root, origin)?;
            let clone = dataset(root, name)?;
            let set = properties(set)?;
            let mut zfs = command(["clone"]);
            zfs.extend(options(&set));
            zfs.extend([origin.as_str().to_owned(), clone.to_string()]);
            Ok(Plan {
                zfs: Some(zfs),
                mountpoints: set.mountpoints,
                own: None,
            })
        }
        ("snapshot", [name]) => {
            let mut zfs = command(["snapshot"]);
            zfs.push(snapshot(root, name)?.as_str().to_owned());
            Ok(plain(zfs))
        }
        ("destroy", [name, flags @ ..]) => {
            let mut zfs = command(["destroy"]);
            match flags {
                [] => {}
                // A deferred destroy exists for snapshots only (01b_zfs.md, "A superseded seed
                // is destroyed deferred").
                ["--defer"] if name.contains('@') => zfs.push("-d".to_owned()),
                _ => return Err(Refusal::Usage),
            }
            zfs.push(if name.contains('@') {
                snapshot(root, name)?.as_str().to_owned()
            } else {
                dataset(root, name)?.to_string()
            });
            Ok(plain(zfs))
        }
        // A snapshot keeps its dataset: only its own name changes, which is how a fork's origin
        // follows the fork into the trash.
        ("rename", [from, to]) if from.contains('@') || to.contains('@') => {
            let (from, to) = (snapshot(root, from)?, snapshot(root, to)?);
            if from.dataset() != to.dataset() {
                return Err(Refusal::Usage);
            }
            let mut zfs = command(["rename"]);
            zfs.extend([from.as_str().to_owned(), to.as_str().to_owned()]);
            Ok(plain(zfs))
        }
        ("rename", [from, to]) => {
            let mut zfs = command(["rename"]);
            zfs.extend([
                dataset(root, from)?.to_string(),
                dataset(root, to)?.to_string(),
            ]);
            Ok(plain(zfs))
        }
        ("promote", [name]) => {
            let promoted = dataset(root, name)?;
            if !root.holds_build_volume(&promoted) {
                return Err(Refusal::NotABuildVolume {
                    dataset: promoted.to_string(),
                });
            }
            let mut zfs = command(["promote"]);
            zfs.push(promoted.to_string());
            Ok(plain(zfs))
        }
        ("mount", [name]) => {
            let mut zfs = command(["mount"]);
            zfs.push(dataset(root, name)?.to_string());
            Ok(plain(zfs))
        }
        ("unmount", [name, flags @ ..]) => {
            let mut zfs = command(["unmount"]);
            match flags {
                [] => {}
                ["--force"] => zfs.push("-f".to_owned()),
                _ => return Err(Refusal::Usage),
            }
            zfs.push(dataset(root, name)?.to_string());
            Ok(plain(zfs))
        }
        ("set", [name, set @ ..]) if !set.is_empty() => {
            let target = dataset(root, name)?;
            let set = properties(set)?;
            let mut zfs = command(["set"]);
            zfs.extend(set.options.iter().cloned());
            zfs.push(target.to_string());
            Ok(Plan {
                zfs: Some(zfs),
                mountpoints: set.mountpoints,
                own: None,
            })
        }
        (
            "check" | "create" | "clone" | "snapshot" | "destroy" | "rename" | "promote" | "mount"
            | "unmount" | "set",
            _,
        ) => Err(Refusal::Usage),
        (other, _) => Err(Refusal::UnknownVerb(other.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROOT: &str = "tank/dev/cowshed";
    const REPO: &str = "tank/dev/cowshed/projects/acme/widget";

    fn run(arguments: &[&str]) -> Result<Plan, Refusal> {
        let arguments: Vec<String> = arguments.iter().map(|&text| text.to_owned()).collect();
        plan(&Root::parse(ROOT).unwrap(), &arguments)
    }

    fn zfs(arguments: &[&str]) -> Vec<String> {
        run(arguments)
            .unwrap_or_else(|refusal| panic!("{arguments:?}: {refusal}"))
            .zfs
            .unwrap_or_else(|| panic!("{arguments:?} runs no zfs command"))
    }

    #[test]
    fn check_runs_no_command_and_takes_no_argument() {
        let main = format!("{REPO}/main");
        assert_eq!(
            run(&["check"]),
            Ok(Plan {
                zfs: None,
                mountpoints: Vec::new(),
                own: None,
            })
        );
        let cases: Vec<Vec<&str>> = vec![
            vec!["check", &main],
            vec!["check", ROOT],
            vec!["check", "--force"],
            vec!["check", "mountpoint=/home/u/w"],
            vec!["check", ""],
        ];
        for arguments in cases {
            assert_eq!(run(&arguments), Err(Refusal::Usage), "{arguments:?}");
        }
        // Every other verb runs a command: `check` is the only one that proves and changes
        // nothing.
        for arguments in [
            vec!["mount", main.as_str()],
            vec!["unmount", main.as_str()],
            vec!["set", main.as_str(), "canmount=off"],
        ] {
            assert!(run(&arguments).unwrap().zfs.is_some(), "{arguments:?}");
        }
        assert_eq!(
            run(&["Check"]),
            Err(Refusal::UnknownVerb("Check".to_owned()))
        );
    }

    #[test]
    fn each_verb_becomes_exactly_one_zfs_command() {
        let main = format!("{REPO}/main");
        let raven = format!("{REPO}/ws/raven");
        let trash = format!("{REPO}/ws/.trash/raven-1");
        let fork = format!("{main}@cowshed:fork-raven");
        let build = format!("{REPO}/build/7");
        let seed = format!("{build}@seed-1");
        let retired_fork = format!("{main}@cowshed:fork-raven-1");
        let cases: Vec<(Vec<&str>, Vec<&str>)> = vec![
            (
                vec![
                    "create",
                    &main,
                    "mountpoint=/home/u/widget",
                    "cowshed:role=main",
                ],
                vec![
                    "create",
                    "-o",
                    "mountpoint=/home/u/widget",
                    "-o",
                    "cowshed:role=main",
                    &main,
                ],
            ),
            (vec!["create", &main], vec!["create", &main]),
            (vec!["snapshot", &fork], vec!["snapshot", &fork]),
            (
                vec!["clone", &fork, &raven, "refquota=1073741824"],
                vec!["clone", "-o", "refquota=1073741824", &fork, &raven],
            ),
            (vec!["unmount", &raven], vec!["unmount", &raven]),
            (
                vec!["unmount", &raven, "--force"],
                vec!["unmount", "-f", &raven],
            ),
            (
                vec!["set", &raven, "canmount=off", "mountpoint=none"],
                vec!["set", "canmount=off", "mountpoint=none", &raven],
            ),
            (
                vec!["rename", &raven, &trash],
                vec!["rename", &raven, &trash],
            ),
            (
                vec!["rename", &fork, &retired_fork],
                vec!["rename", &fork, &retired_fork],
            ),
            (vec!["destroy", &trash], vec!["destroy", &trash]),
            (vec!["destroy", &fork], vec!["destroy", &fork]),
            (
                vec!["destroy", &seed, "--defer"],
                vec!["destroy", "-d", &seed],
            ),
            (vec!["promote", &build], vec!["promote", &build]),
            (vec!["mount", &main], vec!["mount", &main]),
        ];
        for (arguments, expected) in cases {
            assert_eq!(zfs(&arguments), expected, "{arguments:?}");
        }
    }

    #[test]
    fn only_create_hands_its_dataset_to_the_caller() {
        let main = format!("{REPO}/main");
        let fork = format!("{main}@cowshed:fork-a");
        let created = run(&["create", &main, "mountpoint=/home/u/w"]).unwrap();
        assert_eq!(
            created.own.map(|dataset| dataset.to_string()),
            Some(main.clone())
        );
        assert_eq!(created.mountpoints, [PathBuf::from("/home/u/w")]);
        let cloned = run(&["clone", &fork, &format!("{REPO}/ws/a"), "mountpoint=none"]).unwrap();
        assert_eq!(cloned.own, None);
        assert!(cloned.mountpoints.is_empty());
        let set = run(&["set", &main, "mountpoint=/home/u/x"]).unwrap();
        assert_eq!(set.mountpoints, [PathBuf::from("/home/u/x")]);
    }

    #[test]
    fn a_dataset_outside_the_root_is_refused_on_every_verb() {
        let inside = format!("{REPO}/main");
        let inside_snapshot = format!("{inside}@s");
        for outside in [
            "tank/dev/cowshed",
            "tank/dev",
            "tank",
            "tank/dev/other/x",
            "z/cowshed/x",
        ] {
            let snapshot = format!("{outside}@s");
            let cases: Vec<Vec<&str>> = vec![
                vec!["create", outside],
                vec!["snapshot", &snapshot],
                vec!["clone", &snapshot, &inside],
                vec!["clone", &inside_snapshot, outside],
                vec!["destroy", outside],
                vec!["destroy", &snapshot],
                vec!["rename", outside, &inside],
                vec!["rename", &inside, outside],
                vec!["promote", outside],
                vec!["mount", outside],
                vec!["unmount", outside],
                vec!["set", outside, "canmount=off"],
            ];
            for arguments in cases {
                assert!(
                    matches!(run(&arguments), Err(Refusal::Outside { .. })),
                    "{arguments:?}: {:?}",
                    run(&arguments)
                );
            }
        }
    }

    #[test]
    fn refusals() {
        let main = format!("{REPO}/main");
        let raven = format!("{REPO}/ws/raven");
        let snapshot = format!("{main}@s");
        let property = |name: &str| Refusal::Property {
            property: name.to_owned(),
        };
        let cases: Vec<(Vec<&str>, Refusal)> = vec![
            (vec![], Refusal::Usage),
            (vec!["create"], Refusal::Usage),
            (vec!["list", &main], Refusal::UnknownVerb("list".to_owned())),
            (
                vec!["send", &snapshot],
                Refusal::UnknownVerb("send".to_owned()),
            ),
            // Recursion and force never reach `zfs destroy`.
            (vec!["destroy", &main, "-r"], Refusal::Usage),
            (vec!["destroy", &main, "-R"], Refusal::Usage),
            (vec!["destroy", &main, "-f"], Refusal::Usage),
            (vec!["destroy", &main, "--defer"], Refusal::Usage),
            (vec!["rename", &main], Refusal::Usage),
            (vec!["set", &main], Refusal::Usage),
            (vec!["set", &main, "canmount"], Refusal::Usage),
            (
                vec!["promote", &raven],
                Refusal::NotABuildVolume {
                    dataset: raven.clone(),
                },
            ),
            (
                vec!["promote", &main],
                Refusal::NotABuildVolume {
                    dataset: main.clone(),
                },
            ),
            (vec!["set", &main, "zoned=off"], property("zoned")),
            (vec!["set", &main, "sharenfs=on"], property("sharenfs")),
            (vec!["set", &main, "overlay=on"], property("overlay")),
            (
                vec!["create", &main, "encryption=on"],
                property("encryption"),
            ),
            (
                vec!["set", &main, "cowshed:owner=0"],
                property("cowshed:owner"),
            ),
            (vec!["set", &main, "cowshed:=x"], property("cowshed:")),
            (vec!["set", &main, "other:role=x"], property("other:role")),
            (
                vec!["set", &main, "canmount=off -o zoned=off"],
                Refusal::Value {
                    property: "canmount".to_owned(),
                    value: "off -o zoned=off".to_owned(),
                },
            ),
            (
                vec!["set", &main, "refquota=-1"],
                Refusal::Value {
                    property: "refquota".to_owned(),
                    value: "-1".to_owned(),
                },
            ),
        ];
        for (arguments, expected) in cases {
            assert_eq!(run(&arguments), Err(expected), "{arguments:?}");
        }
        for value in [
            "relative", "/a/../b", "/a/./b", "/a//b", "/a/", "", "legacy", "/a\nb",
        ] {
            let option = format!("mountpoint={value}");
            assert_eq!(
                run(&["set", &main, &option]),
                Err(Refusal::Mountpoint {
                    value: value.to_owned()
                }),
                "{value:?}"
            );
        }
    }

    #[test]
    fn only_an_owner_set_locally_to_the_caller_admits_and_a_refusal_names_both() {
        let root = Root::parse(ROOT).unwrap();
        let read = |value: &str, source: &str| vec![(value.to_owned(), source.to_owned())];
        assert_eq!(admit(&root, &read("501", "local"), 501), Ok(()));
        for (owner, refusal) in [
            (
                read("501", "inherited from tank/dev"),
                "tank/dev/cowshed has cowshed:owner=501 (inherited from tank/dev); it must be set locally to the caller's uid, 501",
            ),
            (
                read("502", "local"),
                "tank/dev/cowshed has cowshed:owner=502 (local); it must be set locally to the caller's uid, 501",
            ),
            (Vec::new(), "cannot read cowshed:owner of tank/dev/cowshed"),
            (
                [read("501", "local"), read("501", "local")].concat(),
                "cannot read cowshed:owner of tank/dev/cowshed",
            ),
        ] {
            assert_eq!(
                admit(&root, &owner, 501).map_err(|refused| refused.to_string()),
                Err(refusal.to_owned()),
                "{owner:?}"
            );
        }
    }
}
