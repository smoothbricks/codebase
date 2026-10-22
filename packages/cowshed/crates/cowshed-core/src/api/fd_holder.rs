//! The wire shape of `cowshed fd-holders` (02_workspaces.md "Descriptor holders: `cowshed
//! fd-holders`"): every process with a descriptor on one of the kernel objects a caller names, the
//! objects named as `hold`'s `started.output` names them. The generated TypeScript validator
//! reads it, and a host connection's answer decodes into the same value.

use serde::{Deserialize, Serialize};

use cowshed_os::process::DescriptorName;

use super::held_lock::DecimalU64;
use super::hold::KernelAddress;
use crate::build_volume::nx::{ObjectCensus, ObjectHold};

/// One kernel object, as `lsof -F di` and `hold`'s `started.output` name it
/// ([`super::hold::HeldDescriptor`]): a Darwin pipe end or socket by its address, anything else,
/// and on Linux every descriptor, by its inode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum FdIdentity {
    /// lsof's `d` field: the address Darwin names a pipe end or a socket by.
    Device { device: KernelAddress },
    /// lsof's `i` field: the inode, of a file, and on Linux of a pipe or a socket too, whose two
    /// ends share it.
    Inode { inode: DecimalU64 },
}

impl FdIdentity {
    /// The name a descriptor on this object has in every process holding it.
    pub const fn name(self) -> DescriptorName {
        match self {
            Self::Device { device } => DescriptorName::Device(device.get()),
            Self::Inode { inode } => DescriptorName::Inode(inode.get()),
        }
    }
}

/// One process with a descriptor on a named object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FdHolder {
    pub pid: i32,
    /// The kernel's identity of the process's life, read before and after its descriptors: on
    /// Darwin its unique id (`p_uniqueid`), on Linux its start time in clock ticks since boot.
    /// With `pid` it names this process alone, never a later one given the pid.
    pub unique_id: DecimalU64,
    /// Its argument vector joined by spaces, each argument decoded lossily (or its executable's
    /// path, or `command unreadable`): never an identity.
    pub command: String,
    /// Its descriptors on the named objects, ascending.
    pub descriptors: Vec<i32>,
    /// The named objects those descriptors are on, each once, in the order of its first
    /// descriptor.
    pub matches: Vec<FdIdentity>,
}

/// Every process with a descriptor on one of the named objects: `cowshed fd-holders`'s one JSON
/// object, and what `host.fdHolders` answers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FdHolders {
    pub holders: Vec<FdHolder>,
    /// The processes whose descriptors the kernel withheld from this user (another user's, unless
    /// the census runs as root). Any of them may hold an object: counted, never reported as
    /// holding none.
    pub uninspected: u32,
}

impl From<ObjectHold<FdIdentity>> for FdHolder {
    fn from(hold: ObjectHold<FdIdentity>) -> Self {
        let mut matches = Vec::new();
        let descriptors = hold
            .descriptors
            .into_iter()
            .map(|(descriptor, identity)| {
                if !matches.contains(&identity) {
                    matches.push(identity);
                }
                descriptor
            })
            .collect();
        Self {
            pid: hold.holder.pid,
            unique_id: DecimalU64::new(hold.life),
            command: hold.holder.command,
            descriptors,
            matches,
        }
    }
}

impl From<ObjectCensus<FdIdentity>> for FdHolders {
    fn from(census: ObjectCensus<FdIdentity>) -> Self {
        Self {
            holders: census.holds.into_iter().map(FdHolder::from).collect(),
            uninspected: census.uninspected,
        }
    }
}

#[cfg(test)]
mod tests {
    use cowshed_os::process::Holder;

    use super::super::hold::HeldDescriptor;
    use super::*;

    const PIPE: FdIdentity = FdIdentity::Device {
        device: KernelAddress::new(0x1c8_cfde_e8d6_f6f3),
    };
    const FILE: FdIdentity = FdIdentity::Inode {
        inode: DecimalU64::new(7),
    };

    /// The object has its frozen spelling, the one `started.output` names it by, so a caller
    /// passes on what `hold` said without translating it; the answer round-trips, a unique id past
    /// 2^53 a decimal string.
    #[test]
    fn the_answer_has_its_frozen_spelling() {
        for (identity, held) in [
            (
                PIPE,
                HeldDescriptor::Device {
                    device: KernelAddress::new(0x1c8_cfde_e8d6_f6f3),
                },
            ),
            (
                FILE,
                HeldDescriptor::Inode {
                    inode: DecimalU64::new(7),
                },
            ),
        ] {
            assert_eq!(
                serde_json::to_value(identity).expect("serialize"),
                serde_json::to_value(held).expect("serialize")
            );
        }
        let answer = FdHolders {
            holders: vec![FdHolder {
                pid: 42,
                unique_id: DecimalU64::new(u64::MAX),
                command: "sleep 600".to_owned(),
                descriptors: vec![1, 2],
                matches: vec![PIPE, FILE],
            }],
            uninspected: 3,
        };
        let wire = serde_json::to_string(&answer).expect("serialize");
        assert_eq!(
            wire,
            r#"{"holders":[{"pid":42,"uniqueId":"18446744073709551615","command":"sleep 600","descriptors":[1,2],"matches":[{"kind":"device","device":"0x1c8cfdee8d6f6f3"},{"kind":"inode","inode":"7"}]}],"uninspected":3}"#
        );
        assert_eq!(
            serde_json::from_str::<FdHolders>(&wire).expect("decode"),
            answer
        );
    }

    /// A holder names each object it holds once, in the order of its first descriptor on it, and
    /// every descriptor on one in ascending order, as the census read them.
    #[test]
    fn a_holder_names_each_object_once() {
        let holder = FdHolder::from(ObjectHold {
            holder: Holder {
                pid: 42,
                command: "sh".to_owned(),
            },
            life: 9,
            descriptors: vec![(1, FILE), (2, PIPE), (5, FILE)],
        });
        assert_eq!(holder.descriptors, [1, 2, 5]);
        assert_eq!(holder.matches, [FILE, PIPE]);
        assert_eq!(holder.unique_id, DecimalU64::new(9));
        assert_eq!(FILE.name(), DescriptorName::Inode(7));
        assert_eq!(PIPE.name(), DescriptorName::Device(0x1c8_cfde_e8d6_f6f3));
    }
}
