//! The wire shapes of `cowshed hold` (11_shell.md "Held leaders: `cowshed hold`"): the events its
//! `--state-fd` writes, one JSON object per line that the generated TypeScript validators read,
//! and the requests its `--control-fd` reads, one text line each.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::dto::ExitStatus;
use super::held_lock::DecimalU64;

/// A signal a holder sends its leader's group, named as `kill -s` takes it, without `SIG`: the
/// standard signals a caller ends, stops, continues or notifies a process group with. The wire and
/// the control line spell it the same way.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HoldSignal {
    Hup,
    Int,
    Quit,
    Abrt,
    Kill,
    Usr1,
    Usr2,
    Alrm,
    Term,
    Stop,
    Cont,
    Tstp,
    Ttin,
    Ttou,
    Winch,
}

impl HoldSignal {
    /// Every signal, in the order above.
    pub const ALL: [Self; 15] = [
        Self::Hup,
        Self::Int,
        Self::Quit,
        Self::Abrt,
        Self::Kill,
        Self::Usr1,
        Self::Usr2,
        Self::Alrm,
        Self::Term,
        Self::Stop,
        Self::Cont,
        Self::Tstp,
        Self::Ttin,
        Self::Ttou,
        Self::Winch,
    ];

    /// Its name, as the wire and a control line spell it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Hup => "HUP",
            Self::Int => "INT",
            Self::Quit => "QUIT",
            Self::Abrt => "ABRT",
            Self::Kill => "KILL",
            Self::Usr1 => "USR1",
            Self::Usr2 => "USR2",
            Self::Alrm => "ALRM",
            Self::Term => "TERM",
            Self::Stop => "STOP",
            Self::Cont => "CONT",
            Self::Tstp => "TSTP",
            Self::Ttin => "TTIN",
            Self::Ttou => "TTOU",
            Self::Winch => "WINCH",
        }
    }

    /// Its number on this platform.
    pub const fn number(self) -> libc::c_int {
        match self {
            Self::Hup => libc::SIGHUP,
            Self::Int => libc::SIGINT,
            Self::Quit => libc::SIGQUIT,
            Self::Abrt => libc::SIGABRT,
            Self::Kill => libc::SIGKILL,
            Self::Usr1 => libc::SIGUSR1,
            Self::Usr2 => libc::SIGUSR2,
            Self::Alrm => libc::SIGALRM,
            Self::Term => libc::SIGTERM,
            Self::Stop => libc::SIGSTOP,
            Self::Cont => libc::SIGCONT,
            Self::Tstp => libc::SIGTSTP,
            Self::Ttin => libc::SIGTTIN,
            Self::Ttou => libc::SIGTTOU,
            Self::Winch => libc::SIGWINCH,
        }
    }

    /// The signal `name` spells exactly; `None` for any other bytes.
    pub fn named(name: &[u8]) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|signal| signal.name().as_bytes() == name)
    }

    /// The signal numbered `number` on this platform; `None` for one this set does not hold.
    pub fn numbered(number: libc::c_int) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|signal| signal.number() == number)
    }
}

impl fmt::Display for HoldSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// One request a holder reads on its `--control-fd`, one line each: `signal <SIG>` or `release`.
/// The end of the descriptor is a release too, so a caller that dies releases.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HoldRequest {
    /// Send the signal to the leader's whole group, only while the leader is held unreaped.
    Signal(HoldSignal),
    /// Nothing more is wanted of the leader: reap it once it has exited.
    Release,
}

impl HoldRequest {
    /// The request one control line spells, without its newline; the reason it is no request
    /// otherwise, naming the line's exact bytes.
    pub fn parse(line: &[u8]) -> Result<Self, String> {
        if line == b"release" {
            return Ok(Self::Release);
        }
        line.strip_prefix(b"signal ")
            .and_then(HoldSignal::named)
            .map(Self::Signal)
            .ok_or_else(|| {
                format!(
                    "control line {:?} is neither `signal <SIG>`, with SIG one of {}, nor `release`",
                    String::from_utf8_lossy(line),
                    HoldSignal::ALL.map(HoldSignal::name).join(" ")
                )
            })
    }
}

/// The line that spells the request, without its newline.
impl fmt::Display for HoldRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Signal(signal) => write!(formatter, "signal {signal}"),
            Self::Release => formatter.write_str("release"),
        }
    }
}

/// A kernel address as lsof's `d` field prints it: `0x` and its lowercase hexadecimal digits,
/// with no leading zero (`0x1c8cfdee8d6f6f3`, `0x0`), measured against lsof on Darwin 25.6.
/// Darwin names a pipe or a socket by it; the address is the kernel's permuted one, the value
/// every process reading that descriptor's description is shown.
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "display",
        scalar = "string & tags.Pattern<'^0x(0|[1-9a-f][0-9a-f]{0,15})$'>"
    )
)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct KernelAddress(u64);

impl KernelAddress {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The address lsof's spelling names: `0x`, then one to 16 lowercase hexadecimal digits with
    /// no leading zero but zero's own.
    pub fn parse(text: &str) -> Option<Self> {
        let digits = text.strip_prefix("0x")?;
        let canonical = (1..=16).contains(&digits.len())
            && (digits.len() == 1 || !digits.starts_with('0'))
            && digits
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        if !canonical {
            return None;
        }
        u64::from_str_radix(digits, 16).ok().map(Self)
    }
}

impl fmt::Display for KernelAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "0x{:x}", self.0)
    }
}

impl Serialize for KernelAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for KernelAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Address;
        impl serde::de::Visitor<'_> for Address {
            type Value = KernelAddress;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("0x and lowercase hexadecimal digits with no leading zero")
            }

            fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<KernelAddress, E> {
                KernelAddress::parse(text)
                    .ok_or_else(|| E::invalid_value(serde::de::Unexpected::Str(text), &self))
            }
        }
        deserializer.deserialize_str(Address)
    }
}

/// The kernel's name of one descriptor the leader was handed, read from the holder's own copy,
/// as `lsof -F di` prints it ([`cowshed_os::process::DescriptorName`]).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HeldDescriptor {
    /// lsof's `d` field: the address Darwin names a pipe or a socket by.
    Device { device: KernelAddress },
    /// lsof's `i` field: the inode, of a file, and on Linux of a pipe or a socket too.
    Inode { inode: DecimalU64 },
    /// The holder had no such descriptor open, so neither had the leader.
    Closed,
    /// Open, of a Darwin kind named by neither field (a kqueue, a POSIX semaphore or shared memory
    /// object, ...): its `PROX_FDTYPE_*` number.
    Unnamed { fd_type: u32 },
}

impl From<cowshed_os::process::DescriptorName> for HeldDescriptor {
    fn from(name: cowshed_os::process::DescriptorName) -> Self {
        use cowshed_os::process::DescriptorName;
        match name {
            DescriptorName::Device(address) => Self::Device {
                device: KernelAddress::new(address),
            },
            DescriptorName::Inode(inode) => Self::Inode {
                inode: DecimalU64::new(inode),
            },
            DescriptorName::Closed => Self::Closed,
            DescriptorName::Unnamed { fd_type } => Self::Unnamed { fd_type },
        }
    }
}

/// The descriptors the leader was handed as its standard output and error.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeldOutput {
    pub stdout: HeldDescriptor,
    pub stderr: HeldDescriptor,
}

/// The leader's birth identity, observed by the holder, its parent, before anything could reap
/// it ([`crate::runtime::job_groups::Birth`]).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HeldBirth {
    /// The leader's rusage birth clock and its start time, the identity no later process given its
    /// pid shares.
    Observed {
        birth: DecimalU64,
        start: DecimalU64,
    },
    /// Its identity could not be read, so its group is never signalled.
    Unobserved { reason: String },
}

impl From<&crate::runtime::job_groups::Birth> for HeldBirth {
    fn from(birth: &crate::runtime::job_groups::Birth) -> Self {
        use crate::runtime::job_groups::Birth;
        match birth {
            Birth::Observed(leader) => Self::Observed {
                birth: DecimalU64::new(leader.birth()),
                start: DecimalU64::new(leader.start()),
            },
            Birth::Unobserved { reason, .. } => Self::Unobserved {
                reason: reason.clone(),
            },
        }
    }
}

/// One event a `cowshed hold` holder writes on its `--state-fd`, one JSON object per line: a
/// reason carrying a newline stays one line. `started` is the first line of a leader that runs,
/// and `failed` is always a last line.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HoldEvent {
    /// The leader runs as `pid`, leading a process group and session the holder is outside of;
    /// `output` names the descriptors it was handed as 1 and 2.
    Started {
        pid: i32,
        birth: HeldBirth,
        output: HeldOutput,
    },
    /// The leader exited with `status`, observed without reaping it: it stays held, and its group
    /// id names its group alone, until it is released.
    Exited { status: ExitStatus },
    /// `signal` was sent to the leader's whole group while the leader was held.
    Signalled { signal: HoldSignal },
    /// `signal` was sent to nothing, for `reason`: the leader was reaped, was never identified, or
    /// the kernel refused the signal.
    NotSignalled { signal: HoldSignal, reason: String },
    /// The holder failed before its leader ran: no command runs. Always the last line.
    Failed { reason: String },
    /// The holder failed once its leader `pid` ran (before `started`, when it could not give up
    /// its standard streams, or after): it released the leader. Signal answers and `exited` may
    /// follow; a `commandFailed` saying the leader's exit could not be observed is the last line.
    CommandFailed { pid: i32, reason: String },
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn signal() -> impl Strategy<Value = HoldSignal> {
        proptest::sample::select(HoldSignal::ALL.to_vec())
    }

    fn request() -> impl Strategy<Value = HoldRequest> {
        prop_oneof![
            signal().prop_map(HoldRequest::Signal),
            Just(HoldRequest::Release)
        ]
    }

    fn descriptor() -> impl Strategy<Value = HeldDescriptor> {
        prop_oneof![
            any::<u64>().prop_map(|address| HeldDescriptor::Device {
                device: KernelAddress::new(address)
            }),
            any::<u64>().prop_map(|inode| HeldDescriptor::Inode {
                inode: DecimalU64::new(inode)
            }),
            Just(HeldDescriptor::Closed),
            any::<u32>().prop_map(|fd_type| HeldDescriptor::Unnamed { fd_type }),
        ]
    }

    fn event() -> impl Strategy<Value = HoldEvent> {
        prop_oneof![
            (
                any::<i32>(),
                prop_oneof![
                    (any::<u64>(), any::<u64>()).prop_map(|(birth, start)| HeldBirth::Observed {
                        birth: DecimalU64::new(birth),
                        start: DecimalU64::new(start)
                    }),
                    any::<String>().prop_map(|reason| HeldBirth::Unobserved { reason }),
                ],
                descriptor(),
                descriptor()
            )
                .prop_map(|(pid, birth, stdout, stderr)| HoldEvent::Started {
                    pid,
                    birth,
                    output: HeldOutput { stdout, stderr }
                }),
            prop_oneof![
                any::<i32>().prop_map(|code| ExitStatus::Exited { code }),
                (any::<i32>(), any::<bool>()).prop_map(|(signal, core_dumped)| {
                    ExitStatus::Signaled {
                        signal,
                        core_dumped,
                    }
                }),
            ]
            .prop_map(|status| HoldEvent::Exited { status }),
            signal().prop_map(|signal| HoldEvent::Signalled { signal }),
            (signal(), any::<String>())
                .prop_map(|(signal, reason)| HoldEvent::NotSignalled { signal, reason }),
            any::<String>().prop_map(|reason| HoldEvent::Failed { reason }),
            (any::<i32>(), any::<String>())
                .prop_map(|(pid, reason)| HoldEvent::CommandFailed { pid, reason }),
        ]
    }

    proptest! {
        /// Every request reads back from the line it spells, and the line names it alone.
        #[test]
        fn a_request_reads_back_from_its_line(request in request()) {
            prop_assert_eq!(HoldRequest::parse(request.to_string().as_bytes()), Ok(request));
        }

        /// A line is a request only when it spells one exactly: any other bytes are refused,
        /// naming the line, and never read as a signal or a release.
        #[test]
        fn any_other_line_is_refused(line in proptest::collection::vec(any::<u8>(), 0..24)) {
            let spelled = HoldSignal::ALL
                .into_iter()
                .map(HoldRequest::Signal)
                .chain([HoldRequest::Release])
                .find(|request| request.to_string().as_bytes() == line.as_slice());
            match HoldRequest::parse(&line) {
                Ok(request) => prop_assert_eq!(Some(request), spelled),
                Err(reason) => {
                    prop_assert_eq!(spelled, None);
                    let shown = format!("{:?}", String::from_utf8_lossy(&line));
                    prop_assert!(reason.contains(&shown));
                }
            }
        }

        /// A kernel address is spelled as lsof's `d` field and read back exactly; no other
        /// spelling of it is read.
        #[test]
        fn a_kernel_address_reads_back_only_from_its_spelling(address in any::<u64>()) {
            let spelled = KernelAddress::new(address).to_string();
            prop_assert_eq!(spelled.clone(), format!("0x{address:x}"));
            prop_assert_eq!(KernelAddress::parse(&spelled), Some(KernelAddress::new(address)));
            if spelled.bytes().any(|byte| (b'a'..=b'f').contains(&byte)) {
                prop_assert_eq!(KernelAddress::parse(&format!("0x{address:X}")), None);
            }
            if address >> 60 == 0 {
                prop_assert_eq!(KernelAddress::parse(&format!("0x{address:016x}")), None);
            }
        }

        /// Every event is one line of JSON, and reads back as itself.
        #[test]
        fn an_event_is_one_line_that_reads_back(event in event()) {
            let line = serde_json::to_string(&event).expect("encode");
            prop_assert!(!line.contains('\n'));
            prop_assert_eq!(serde_json::from_str::<HoldEvent>(&line).expect("decode"), event);
        }
    }

    /// Each signal's number is this platform's, and names it back.
    #[test]
    fn a_signal_is_numbered_as_the_platform_numbers_it() {
        for signal in HoldSignal::ALL {
            assert_eq!(HoldSignal::numbered(signal.number()), Some(signal));
            assert_eq!(HoldSignal::named(signal.name().as_bytes()), Some(signal));
            assert_eq!(
                serde_json::to_string(&signal).expect("encode"),
                format!("\"{}\"", signal.name())
            );
        }
        assert_eq!(HoldSignal::numbered(0), None);
        assert_eq!(HoldSignal::named(b"SIGKILL"), None);
        assert_eq!(HoldSignal::named(b"kill"), None);
    }

    /// lsof's own spellings read back, and zero is `0x0`.
    #[test]
    fn a_kernel_address_is_spelled_as_lsof_prints_it() {
        for spelled in ["0x1c8cfdee8d6f6f3", "0x7e50709be2ba7212", "0x0"] {
            let address = KernelAddress::parse(spelled).expect("lsof's spelling");
            assert_eq!(address.to_string(), spelled);
        }
        for refused in [
            "0x",
            "0x00",
            "0x01c8cfdee8d6f6f3",
            "0x10000000000000000",
            "1c8c",
            "0X1c",
        ] {
            assert_eq!(KernelAddress::parse(refused), None, "{refused}");
        }
    }

    /// The wire spells each event as the frozen projection says (07_api.md "Frozen wire
    /// projections").
    #[test]
    fn the_events_have_their_frozen_spelling() {
        let started = HoldEvent::Started {
            pid: 42,
            birth: HeldBirth::Observed {
                birth: DecimalU64::new(7),
                start: DecimalU64::new(8),
            },
            output: HeldOutput {
                stdout: HeldDescriptor::Device {
                    device: KernelAddress::new(0x7e50_709b_e2ba_7212),
                },
                stderr: HeldDescriptor::Inode {
                    inode: DecimalU64::new(340),
                },
            },
        };
        assert_eq!(
            serde_json::to_string(&started).unwrap(),
            r#"{"kind":"started","pid":42,"birth":{"kind":"observed","birth":"7","start":"8"},"output":{"stdout":{"kind":"device","device":"0x7e50709be2ba7212"},"stderr":{"kind":"inode","inode":"340"}}}"#
        );
        assert_eq!(
            serde_json::to_string(&HoldEvent::Exited {
                status: ExitStatus::Signaled {
                    signal: 9,
                    core_dumped: false
                }
            })
            .unwrap(),
            r#"{"kind":"exited","status":{"kind":"signaled","signal":9,"coreDumped":false}}"#
        );
        assert_eq!(
            serde_json::to_string(&HoldEvent::NotSignalled {
                signal: HoldSignal::Kill,
                reason: "reaped".to_owned()
            })
            .unwrap(),
            r#"{"kind":"notSignalled","signal":"KILL","reason":"reaped"}"#
        );
        assert_eq!(
            serde_json::to_string(&HoldEvent::CommandFailed {
                pid: 42,
                reason: "lost".to_owned()
            })
            .unwrap(),
            r#"{"kind":"commandFailed","pid":42,"reason":"lost"}"#
        );
        assert_eq!(
            serde_json::to_string(&HeldDescriptor::Closed).unwrap(),
            r#"{"kind":"closed"}"#
        );
    }
}
