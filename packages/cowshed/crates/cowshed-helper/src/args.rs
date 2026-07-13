use std::ffi::OsString;

use crate::validate::{Dataset, Mountpoint, Snapshot};

pub const USAGE: &str = "usage: cowshed-helper <create DATASET | clone SNAPSHOT DATASET | snapshot SNAPSHOT | destroy DATASET_OR_SNAPSHOT | mount DATASET | unmount DATASET | set-mountpoint DATASET ABSOLUTE_PATH | rollback SNAPSHOT | swap CURRENT REPLACEMENT ASIDE>";

#[derive(Debug)]
pub enum Operation {
    Create { dataset: Dataset },
    Clone { snapshot: Snapshot, dataset: Dataset },
    Snapshot { snapshot: Snapshot },
    Destroy { target: DestroyTarget },
    Mount { dataset: Dataset },
    Unmount { dataset: Dataset },
    SetMountpoint { dataset: Dataset, mountpoint: Mountpoint },
    Rollback { snapshot: Snapshot },
    Swap { current: Dataset, replacement: Dataset, aside: Dataset },
}

#[derive(Debug)]
pub enum DestroyTarget {
    Dataset(Dataset),
    Snapshot(Snapshot),
}

impl DestroyTarget {
    pub fn as_os_str(&self) -> &std::ffi::OsStr {
        match self {
            Self::Dataset(value) => value.as_os_str(),
            Self::Snapshot(value) => value.as_os_str(),
        }
    }
}

pub fn parse(mut args: impl Iterator<Item = OsString>, pool: &str) -> Result<Operation, String> {
    let verb = utf8(args.next(), "verb")?;
    let remaining: Vec<OsString> = args.collect();

    match verb.as_str() {
        "create" => {
            exact_arity(&remaining, 1, &verb)?;
            Ok(Operation::Create { dataset: Dataset::parse(&remaining[0], pool)? })
        }
        "clone" => {
            exact_arity(&remaining, 2, &verb)?;
            Ok(Operation::Clone {
                snapshot: Snapshot::parse(&remaining[0], pool)?,
                dataset: Dataset::parse(&remaining[1], pool)?,
            })
        }
        "snapshot" => {
            exact_arity(&remaining, 1, &verb)?;
            Ok(Operation::Snapshot { snapshot: Snapshot::parse(&remaining[0], pool)? })
        }
        "destroy" => {
            exact_arity(&remaining, 1, &verb)?;
            let text = utf8(Some(remaining[0].clone()), "destroy target")?;
            let target = if text.contains('@') {
                DestroyTarget::Snapshot(Snapshot::parse(&remaining[0], pool)?)
            } else {
                DestroyTarget::Dataset(Dataset::parse(&remaining[0], pool)?)
            };
            Ok(Operation::Destroy { target })
        }
        "mount" => {
            exact_arity(&remaining, 1, &verb)?;
            Ok(Operation::Mount { dataset: Dataset::parse(&remaining[0], pool)? })
        }
        "unmount" => {
            exact_arity(&remaining, 1, &verb)?;
            Ok(Operation::Unmount { dataset: Dataset::parse(&remaining[0], pool)? })
        }
        "set-mountpoint" => {
            exact_arity(&remaining, 2, &verb)?;
            Ok(Operation::SetMountpoint {
                dataset: Dataset::parse(&remaining[0], pool)?,
                mountpoint: Mountpoint::parse(&remaining[1])?,
            })
        }
        "rollback" => {
            exact_arity(&remaining, 1, &verb)?;
            Ok(Operation::Rollback { snapshot: Snapshot::parse(&remaining[0], pool)? })
        }
        "swap" => {
            exact_arity(&remaining, 3, &verb)?;
            let current = Dataset::parse(&remaining[0], pool)?;
            let replacement = Dataset::parse(&remaining[1], pool)?;
            let aside = Dataset::parse(&remaining[2], pool)?;
            if current == replacement || current == aside || replacement == aside {
                return Err("swap datasets must be distinct".into());
            }
            if current.parent() != replacement.parent() || current.parent() != aside.parent() {
                return Err("swap datasets must have the same parent".into());
            }
            Ok(Operation::Swap { current, replacement, aside })
        }
        _ => Err(format!("unknown verb {verb:?}; expected create, clone, snapshot, destroy, mount, unmount, set-mountpoint, rollback, or swap")),
    }
}

fn exact_arity(args: &[OsString], expected: usize, verb: &str) -> Result<(), String> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(format!("verb {verb:?} expects exactly {expected} argument(s), got {}", args.len()))
    }
}

fn utf8(value: Option<OsString>, description: &str) -> Result<String, String> {
    value
        .ok_or_else(|| format!("missing {description}"))?
        .into_string()
        .map_err(|_| format!("{description} must be valid UTF-8"))
}
