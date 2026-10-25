//! The RAM disks the gateway attaches and detaches for a caller (specs/cowshed/05_gateway.md,
//! "RAM disks").
//!
//! A legacy DiskImages RAM disk lives in the `diskimages-helper` that `hdiutil attach ram://<n>`
//! forks. Forked inside a job, the helper is that job's member, and the job's retirement ends it,
//! and the disk with it, under every other user of the disk. A host-shared RAM disk is therefore
//! attached by the gateway, which no job contains. The request names a size and nothing else: no
//! argv, path or option reaches `hdiutil` from the wire.
//!
//! The attach is a lease the connection holds. A client writes one [`RamLeaseRequest`] line and
//! keeps the connection open; the gateway answers one line whose `ramDisk` is a
//! [`RamDiskAttached`], having recorded the disk first. The client then writes one [`RamLeaseEnd`]
//! line: `keep` once the disk is its own (the gateway forgets it), or `release` (the gateway
//! detaches it and answers how). A connection that ends without either, its owner dead or gone
//! past its deadline, leaves the detach to the gateway, and a gateway that stops holding a lease
//! leaves it to the next gateway's startup scan, so no attach outlives every owner.
//!
//! A kept disk is detached by its owner: a client writes one [`RamDetachRequest`], naming the
//! whole disk and the I/O Registry entry it proved its ownership by, and the gateway answers one
//! line whose `ramDetach` is a [`RamDetached`]. A requester that goes away never cancels it.

use std::fmt;
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

/// The control operation that attaches a RAM disk and holds it for the connection that asked
/// until that connection keeps or releases it ([`RamLeaseRequest`]).
pub const RAM_LEASE_OP: &str = "ram-lease";

/// The one-shot control operation that detaches a RAM disk by its registry entry.
pub const RAM_DETACH_OP: &str = "ram-detach";

/// The bytes of one sector of a `ram://<sectors>` disk.
pub const RAM_DISK_SECTOR_BYTES: u64 = 512;

/// A RAM disk's size in sectors, positive by construction. Whether it fits the host's memory is
/// the attacher's to judge: only the host knows its memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RamDiskSectors(NonZeroU64);

impl RamDiskSectors {
    pub const fn new(sectors: NonZeroU64) -> Self {
        Self(sectors)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Its size in bytes; `None` past `u64`.
    pub const fn bytes(self) -> Option<u64> {
        self.0.get().checked_mul(RAM_DISK_SECTOR_BYTES)
    }
}

impl fmt::Display for RamDiskSectors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} sectors", self.get())
    }
}

/// The line a client writes, `{"op":"ram-lease","sectors":<n>}`, keeping the connection open: the
/// lease lasts until its [`RamLeaseEnd`] line, or until the connection ends.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct RamLeaseRequest {
    op: &'static str,
    pub sectors: RamDiskSectors,
}

impl RamLeaseRequest {
    pub const fn new(sectors: RamDiskSectors) -> Self {
        Self {
            op: RAM_LEASE_OP,
            sectors,
        }
    }
}

/// How a held lease ends: `keep` or `release`, written as one [`RamLeaseEndLine`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RamLeaseEnd {
    /// The disk is the caller's from here: the gateway forgets it and answers `ok`.
    Keep,
    /// The gateway detaches the disk now and answers its `ramDetach`.
    Release,
}

/// The one line that ends a held lease, `{"end":"keep"}` or `{"end":"release"}`, which the client
/// half-closes after. Any other line, or none, ends the lease as its owner's absence does: the
/// gateway detaches the disk.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RamLeaseEndLine {
    pub end: RamLeaseEnd,
}

/// A RAM disk's whole disk, `/dev/disk<N>`: what detaching it names.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RamDiskDevice(String);

impl RamDiskDevice {
    pub fn parse(text: &str) -> Result<Self, InvalidRamDiskDevice> {
        match text.strip_prefix("/dev/disk") {
            Some(unit) if !unit.is_empty() && unit.bytes().all(|byte| byte.is_ascii_digit()) => {
                Ok(Self(text.to_owned()))
            }
            _ => Err(InvalidRamDiskDevice(text.to_owned())),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RamDiskDevice {
    type Error = InvalidRamDiskDevice;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::parse(&text)
    }
}

impl From<RamDiskDevice> for String {
    fn from(device: RamDiskDevice) -> Self {
        device.0
    }
}

impl fmt::Display for RamDiskDevice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Text that is not a whole disk's device path.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{0:?} is not a whole disk (/dev/disk<N>)")]
pub struct InvalidRamDiskDevice(String);

/// An answer's `ramDisk`: the whole disk the attach made and the I/O Registry entry the gateway read
/// it as right after the attach, the identity a registry-bound detach names.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RamDiskAttached {
    pub device: RamDiskDevice,
    pub registry_entry: RamDiskEntry,
}

/// Why no RAM disk was attached.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RamAttachRefusal {
    /// The disk would not fit the host's memory: a RAM disk larger than memory is swap.
    #[error("a RAM disk of {sectors} does not fit the host's {memory} bytes of memory")]
    TooLarge {
        sectors: RamDiskSectors,
        memory: u64,
    },
    /// The attach ran and failed, or could not run; the text says which and why.
    #[error("{0}")]
    Failed(String),
}

/// An I/O Registry entry id: the identity a disk keeps from its attach until it leaves the
/// registry, which no later disk reuses. Written `0x` and lowercase hex without leading zeros, as
/// `ioreg` prints it, and nonzero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RamDiskEntry(NonZeroU64);

impl RamDiskEntry {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// The one spelling of an entry: any other (`0X`, uppercase, a leading zero, no digits, zero,
    /// more than 64 bits) is refused rather than read as some entry.
    pub fn parse(text: &str) -> Result<Self, InvalidRamDiskEntry> {
        text.strip_prefix("0x")
            .filter(|digits| {
                !digits.is_empty()
                    && !digits.starts_with('0')
                    && digits
                        .bytes()
                        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            })
            .and_then(|digits| u64::from_str_radix(digits, 16).ok())
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or_else(|| InvalidRamDiskEntry(text.to_owned()))
    }
}

impl TryFrom<String> for RamDiskEntry {
    type Error = InvalidRamDiskEntry;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        Self::parse(&text)
    }
}

impl From<RamDiskEntry> for String {
    fn from(entry: RamDiskEntry) -> Self {
        entry.to_string()
    }
}

impl fmt::Display for RamDiskEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:#x}", self.0)
    }
}

/// Text that is not a registry entry id.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{0:?} is not a registry entry id (0x and lowercase hex, no leading zeros, nonzero)")]
pub struct InvalidRamDiskEntry(String);

/// The line a client writes, `{"op":"ram-detach","device":"/dev/diskN","registryEntry":"0x…"}`,
/// and half-closes after. There is no force: a disk still in use stays attached.
#[derive(Clone, Debug, Serialize)]
pub struct RamDetachRequest {
    op: &'static str,
    pub device: RamDiskDevice,
    #[serde(rename = "registryEntry")]
    pub registry_entry: RamDiskEntry,
}

impl RamDetachRequest {
    pub const fn new(device: RamDiskDevice, registry_entry: RamDiskEntry) -> Self {
        Self {
            op: RAM_DETACH_OP,
            device,
            registry_entry,
        }
    }
}

/// How a reaped `hdiutil detach` ended: `{"exit":<code>}` or `{"signal":<number>}`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RamDetachStatus {
    Exit(i32),
    Signal(i32),
}

impl fmt::Display for RamDetachStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exit(code) => write!(formatter, "exit {code}"),
            Self::Signal(signal) => write!(formatter, "signal {signal}"),
        }
    }
}

/// An answer's `ramDetach`: what is known of the detach, by its `outcome`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "kebab-case")]
pub enum RamDetached {
    /// `hdiutil` ran and was reaped; only exit 0 detached the disk.
    Terminal {
        status: RamDetachStatus,
        stdout: String,
        stderr: String,
        #[serde(rename = "stdoutTruncated")]
        stdout_truncated: bool,
        #[serde(rename = "stderrTruncated")]
        stderr_truncated: bool,
    },
    /// Refused before any spawn: nothing was detached.
    NotStarted { reason: String },
    /// `hdiutil` may have run, but its end was not observed: the disk is possibly detached.
    Unknown { reason: String },
}

impl RamDetached {
    /// A reaped detach's answer, each stream lossy UTF-8 of at most `limit` bytes, cut at a
    /// character boundary, with whether it was cut. Only the first `limit` bytes are decoded.
    pub fn terminal(status: RamDetachStatus, stdout: &[u8], stderr: &[u8], limit: usize) -> Self {
        let (stdout, stdout_truncated) = head(stdout, limit);
        let (stderr, stderr_truncated) = head(stderr, limit);
        Self::Terminal {
            status,
            stdout,
            stderr,
            stdout_truncated,
            stderr_truncated,
        }
    }
}

/// The first `limit` bytes of `bytes` as lossy UTF-8, cut back to a character boundary when a
/// replacement character grew them past `limit`, and whether anything was cut.
fn head(bytes: &[u8], limit: usize) -> (String, bool) {
    let text = String::from_utf8_lossy(&bytes[..bytes.len().min(limit)]);
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (
        text[..end].to_owned(),
        bytes.len() > limit || end < text.len(),
    )
}

/// The gateway's stderr line for an outcome: `terminal status=…`, `not-started cause=…` or
/// `unknown cause=…`.
impl fmt::Display for RamDetached {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Terminal { status, .. } => write!(formatter, "terminal status={status}"),
            Self::NotStarted { reason } => write!(formatter, "not-started cause={reason:?}"),
            Self::Unknown { reason } => write!(formatter, "unknown cause={reason:?}"),
        }
    }
}

/// What attaches and detaches RAM disks for the gateway: the daemon's disk runner, which the gateway
/// does not link. Blocking; the gateway calls each on a blocking thread no connection owns, since
/// neither is ever interrupted.
pub trait RamDisks: fmt::Debug + Send + Sync {
    /// Attach a RAM disk of `sectors` and read the registry entry it registers as. An attach whose
    /// entry cannot be read is detached again before the refusal answers: no disk leaves here
    /// without an identity to detach it by.
    fn attach(&self, sectors: RamDiskSectors) -> Result<RamDiskAttached, RamAttachRefusal>;

    /// Detach the disk `entry` registers as `device`, spawning only while it still does inside
    /// the host disk admission, and answer what is known of it. Each output stream the answer
    /// carries is bounded to `output_limit` bytes ([`RamDetached::terminal`]).
    fn detach(
        &self,
        device: &RamDiskDevice,
        entry: RamDiskEntry,
        output_limit: usize,
    ) -> RamDetached;

    /// Whether `entry` is still registered at all. An entry id is never reused, so only its absence
    /// proves the disk left the registry: what settles a detach whose outcome alone does not.
    fn registered(&self, entry: RamDiskEntry) -> std::io::Result<bool>;

    /// The RAM disks of `sectors` this user attached that nothing mounts: what an attach whose
    /// lease a previous gateway never recorded may have left.
    fn unmounted(&self, sectors: RamDiskSectors) -> std::io::Result<Vec<RamDiskAttached>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_device_is_a_whole_disk_and_a_size_its_sectors() {
        let sectors = RamDiskSectors::new(NonZeroU64::new(131_072).unwrap());
        assert_eq!(sectors.bytes(), Some(64 * 1024 * 1024));
        assert_eq!(RamDiskSectors::new(NonZeroU64::MAX).bytes(), None);
        assert_eq!(
            RamDiskDevice::parse("/dev/disk7").unwrap().as_str(),
            "/dev/disk7"
        );
        for refused in [
            "/dev/disk7s1",
            "disk7",
            "/dev/disk",
            "/dev/disk7 ",
            "/dev/rdisk7",
        ] {
            assert!(RamDiskDevice::parse(refused).is_err(), "{refused}");
        }
    }

    #[test]
    fn an_entry_has_one_spelling_and_round_trips_through_it() {
        let entry = RamDiskEntry::parse("0x100000abc").unwrap();
        assert_eq!(entry.get(), 0x1_0000_0abc);
        assert_eq!(entry.to_string(), "0x100000abc");
        assert_eq!(String::from(entry), "0x100000abc");
        assert_eq!(
            RamDiskEntry::parse("0xffffffffffffffff").unwrap().get(),
            u64::MAX
        );
        for refused in [
            "",
            "0x",
            "0x0",
            "0x00abc",
            "0X100000abc",
            "0x100000ABC",
            "100000abc",
            "0x1ffffffffffffffff",
            "0x100000abc ",
            "+0x1",
            "0x-1",
        ] {
            assert!(RamDiskEntry::parse(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn a_terminal_answer_carries_at_most_its_limit_of_each_stream_and_says_when_cut() {
        let whole =
            RamDetached::terminal(RamDetachStatus::Exit(0), b"\"disk7\" ejected.\n", b"", 64);
        assert_eq!(
            whole,
            RamDetached::Terminal {
                status: RamDetachStatus::Exit(0),
                stdout: "\"disk7\" ejected.\n".to_owned(),
                stderr: String::new(),
                stdout_truncated: false,
                stderr_truncated: false,
            }
        );
        // Cut inside a two-byte character: the decoded head never passes the limit.
        let cut = RamDetached::terminal(RamDetachStatus::Signal(9), b"ab", "xé".as_bytes(), 2);
        assert_eq!(
            cut,
            RamDetached::Terminal {
                status: RamDetachStatus::Signal(9),
                stdout: "ab".to_owned(),
                stderr: "x".to_owned(),
                stdout_truncated: false,
                stderr_truncated: true,
            }
        );
        // An invalid byte decodes to a three-byte replacement character, still within the limit.
        let RamDetached::Terminal {
            stdout,
            stdout_truncated,
            ..
        } = RamDetached::terminal(RamDetachStatus::Exit(1), b"a\xff", b"", 3)
        else {
            unreachable!("terminal() builds a terminal answer");
        };
        assert!(stdout.len() <= 3 && stdout_truncated, "{stdout:?}");
        assert_eq!(
            RamDetached::NotStarted {
                reason: "gone".to_owned()
            }
            .to_string(),
            "not-started cause=\"gone\""
        );
    }
}
