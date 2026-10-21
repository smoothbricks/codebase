//! The wire shapes of a held lock (02_workspaces.md "Held locks"): its observation, as
//! `cowshed lock-holders` prints it, and the waiter's events, as `cowshed lock --state-fd` writes
//! them; the generated TypeScript validators read both.

use std::fmt;
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A kernel identifier (a device, an inode, a process's unique id) on the wire as its exact
/// decimal string. A JSON number past 2^53 loses precision in JavaScript, and a reader that
/// compares identities must compare them exactly, so none is ever a number. The scalar carries the
/// exact upper bound: a canonical decimal string is at most `u64::MAX` when it is shorter than 20
/// digits, or 20 digits that compare no greater than `18446744073709551615`, which compares digit
/// strings of one length in place, with no conversion.
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "display",
        scalar = "string & tags.Pattern<'^(0|[1-9][0-9]*)$'> & tags.TagBase<{ kind: 'u64Decimal'; target: 'string'; value: '18446744073709551615'; validate: '$input.length < 20 || ($input.length === 20 && $input <= \"18446744073709551615\")' }>"
    )
)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DecimalU64(u64);

impl DecimalU64 {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The value a canonical decimal spells: digits only, no sign, no leading zero but zero's own.
    pub fn parse(text: &str) -> Option<Self> {
        let canonical = !text.is_empty()
            && text.bytes().all(|byte| byte.is_ascii_digit())
            && (text.len() == 1 || !text.starts_with('0'));
        if !canonical {
            return None;
        }
        text.parse().ok().map(Self)
    }
}

impl fmt::Display for DecimalU64 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl Serialize for DecimalU64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Written through Display straight into the output: no string is built.
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DecimalU64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Decimal;
        impl serde::de::Visitor<'_> for Decimal {
            type Value = DecimalU64;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a canonical decimal string of a u64")
            }

            fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<DecimalU64, E> {
                DecimalU64::parse(text)
                    .ok_or_else(|| E::invalid_value(serde::de::Unexpected::Str(text), &self))
            }
        }
        deserializer.deserialize_str(Decimal)
    }
}

/// The inode a lock is on: the device and inode numbers `stat(2)` gives it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockInode {
    pub dev: DecimalU64,
    pub ino: DecimalU64,
}

impl fmt::Display for LockInode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.dev, self.ino)
    }
}

/// What the kernel shows of one descriptor on the locked inode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LockCustody {
    /// Linux: the descriptor's open file description holds the lock (its fdinfo `lock:` line).
    Holds,
    /// Linux: the descriptor holds no lock: a waiter blocked in `flock(2)`, or an open that never
    /// asked for one.
    Open,
    /// Darwin: the descriptor of the life the holder record names, proven by its kernel unique id:
    /// it recorded itself once granted and empties the record before it lets go.
    Recorded,
    /// A descriptor on the inode whose custody cannot be proven: Darwin exposes no lock state;
    /// Linux may refuse inspection. It may hold the lock or be waiting for it, but is never
    /// counted as a holder or signalled without proof.
    Unexposed,
}

/// A process with a descriptor on the locked inode.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockDescriptor {
    pub pid: i32,
    /// The kernel's unique id of the process (`p_uniqueid`); Linux has none to give.
    pub unique_id: Option<DecimalU64>,
    pub command: String,
    pub custody: LockCustody,
}

/// A lock file's path, exactly as the caller named it: its bytes as tagged bytes, `utf8` when they
/// are UTF-8 and canonical standard base64 otherwise, so no path is refused or mangled.
#[cfg_attr(any(), cowshed_api(wire = "TaggedBytesRef"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockPath(PathBuf);

impl LockPath {
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl Serialize for LockPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        super::dto::serialize_tagged_bytes(self.0.as_os_str().as_bytes(), serializer)
    }
}

/// A lock file's content, the holder record as written, as its exact bytes in tagged form: a
/// record is a claim a reader parses, so it is never shown decoded lossily or refused for not
/// being UTF-8. Empty when there is none.
#[cfg_attr(any(), cowshed_api(wire = "TaggedBytesRef"))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LockRecord(Vec<u8>);

impl LockRecord {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for LockRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        super::dto::serialize_tagged_bytes(&self.0, serializer)
    }
}

/// Who the kernel shows holding a lock file, read in one pass (`cowshed lock-holders`). Every
/// field serializes whatever bytes the path and the file hold, so the object is always written
/// whole.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockObservation {
    pub path: LockPath,
    /// The inode the path names; `None` when no lock file exists.
    pub inode: Option<LockInode>,
    /// Whether the kernel shows the inode exclusively locked.
    pub locked: bool,
    /// The holder record on the inode as written, empty when there is none.
    pub record: LockRecord,
    /// The descriptor of the holder the record names, once the kernel proves it: on Linux it holds
    /// the lock; on Darwin its unique id is the record's.
    pub owner: Option<LockDescriptor>,
    /// Every other process with a descriptor on the inode, and what the kernel shows of it. The
    /// observing process is not among them.
    pub descriptors: Vec<LockDescriptor>,
}

/// One event a `cowshed lock` waiter writes on its `--state-fd` channel, one JSON object per line:
/// a reason or a path carrying a newline stays one line. `failed`, `commandFailed` and `signalled`
/// are terminal failures; `acquired` is the last line on a normal run, with the channel retained
/// until the command ends. A failed handoff closes the channel before joining, retaining custody.
/// `signalled` and `notSignalled` answer SIGUSR1 while `queued`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum LockWaiterEvent {
    /// Another holds the lock; this waiter's `flock(2)` on `inode` is pending.
    Queued { inode: LockInode },
    /// The kernel granted the lock on `inode` and the command it guards has been spawned holding
    /// it and this controller has relinquished the command's standard streams. The command's
    /// exit does not change this event; a later wrapper failure is `CommandFailed`.
    Acquired { inode: LockInode },
    /// The command was spawned, but this controller could not hand off stdio, report acquisition,
    /// forward a signal, or wait for its exit. Unlike `Failed`, this does not prove no command ran.
    CommandFailed { reason: String },
    /// The waiter cannot run its command: a setup failure, the lock not taken, or the command's
    /// spawn failed. No command runs.
    Failed { reason: String },
    /// The waiter signalled its owned owner `pid` and refuses its step: it never runs the command.
    Signalled { pid: i32 },
    /// The waiter signalled nothing, for `reason`, and keeps its place in the queue.
    NotSignalled { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An observation is one whole object whatever bytes its path and record hold: UTF-8 keeps its
    /// text under the `utf8` tag, and bytes that are not UTF-8 go out as the canonical base64 of
    /// exactly those bytes.
    #[test]
    fn an_observation_carries_its_path_and_record_as_exact_bytes() {
        use base64::Engine as _;
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt as _;
        let observation = |path: &[u8], record: &[u8]| LockObservation {
            path: LockPath::new(PathBuf::from(OsStr::from_bytes(path))),
            inode: None,
            locked: false,
            record: LockRecord::new(record.to_vec()),
            owner: None,
            descriptors: Vec::new(),
        };
        let wire = serde_json::to_string(&observation(b"/t/exact.lock", b"")).expect("serialize");
        assert_eq!(
            wire,
            r#"{"path":{"encoding":"utf8","data":"/t/exact.lock"},"inode":null,"locked":false,"record":{"encoding":"utf8","data":""},"owner":null,"descriptors":[]}"#
        );
        let path = b"/t/\xff\xfe.lock";
        let record = b"{\"pid\":7,\"command\":\"a\xffb\"}";
        let wire = serde_json::to_value(observation(path, record)).expect("serialize");
        for (field, bytes) in [("path", &path[..]), ("record", &record[..])] {
            assert_eq!(wire[field]["encoding"], "base64", "{wire}");
            let data = wire[field]["data"].as_str().expect("base64 text");
            assert_eq!(
                base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .expect("canonical base64"),
                bytes,
                "{wire}"
            );
        }
    }

    /// Every u64, the high ones JavaScript numbers cannot hold among them, goes out as its exact
    /// decimal string and comes back as itself; nothing but a canonical decimal string is read.
    #[test]
    fn a_kernel_identifier_round_trips_exactly_as_a_decimal_string() {
        let mut values = vec![
            0,
            1,
            9,
            10,
            (1 << 53) - 1,
            1 << 53,
            (1 << 53) + 1,
            u64::MAX - 1,
            u64::MAX,
        ];
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for _ in 0..4096 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            values.push(state);
        }
        for value in values {
            let wire = serde_json::to_string(&DecimalU64::new(value)).expect("serialize");
            assert_eq!(wire, format!("\"{value}\""));
            assert_eq!(
                serde_json::from_str::<DecimalU64>(&wire).expect("deserialize"),
                DecimalU64::new(value)
            );
        }
        for refused in [
            "1",
            "\"\"",
            "\"01\"",
            "\"-1\"",
            "\"+1\"",
            "\"1.0\"",
            "\"18446744073709551616\"",
        ] {
            assert!(
                serde_json::from_str::<DecimalU64>(refused).is_err(),
                "{refused}"
            );
        }
    }

    /// Every waiter event is one line on the wire, whatever newlines its reason carries, and reads
    /// back as itself; an event of no kind, or with a field it does not have, is refused.
    #[test]
    fn a_waiter_event_is_one_line_and_round_trips() {
        let inode = LockInode {
            dev: DecimalU64::new(u64::MAX),
            ino: DecimalU64::new(1 << 53),
        };
        let events = [
            LockWaiterEvent::Queued { inode },
            LockWaiterEvent::Acquired { inode },
            LockWaiterEvent::CommandFailed {
                reason: "cannot wait for the command: interrupted\n".to_owned(),
            },
            LockWaiterEvent::Failed {
                reason: "cannot open the lock /a\nb: denied\r".to_owned(),
            },
            LockWaiterEvent::Signalled { pid: 7 },
            LockWaiterEvent::NotSignalled {
                reason: "the lock path\nnames no file".to_owned(),
            },
        ];
        for event in events {
            let wire = serde_json::to_string(&event).expect("serialize");
            assert!(!wire.contains(['\n', '\r']), "{wire}");
            assert_eq!(
                serde_json::from_str::<LockWaiterEvent>(&wire).expect("deserialize"),
                event
            );
        }
        assert_eq!(
            serde_json::to_string(&LockWaiterEvent::NotSignalled {
                reason: "x".to_owned()
            })
            .expect("serialize"),
            r#"{"kind":"notSignalled","reason":"x"}"#
        );
        for refused in [
            r#"{"kind":"granted","inode":{"dev":"1","ino":"2"}}"#,
            r#"{"kind":"signalled","pid":7,"inode":{"dev":"1","ino":"2"}}"#,
            r#"{"kind":"queued","inode":{"dev":1,"ino":"2"}}"#,
            "QUEUED 1 2",
        ] {
            assert!(
                serde_json::from_str::<LockWaiterEvent>(refused).is_err(),
                "{refused}"
            );
        }
    }
}
