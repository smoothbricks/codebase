use crate::api::process::{JobContainment, OutlivedProcess};
use crate::error::CowshedError;
use crate::land_turn::Ticket;
use crate::metadata::DetachedWorkspaceMetadata;
pub use crate::metadata::{
    EgressMode, EgressRule, GrantSet, Platform, PortBlock, RepoRule, SimVerb, WorkspaceIncarnation,
    WorkspaceName, WorkspaceRole,
};
use crate::project_retirement::ProjectRetirementReport;
use crate::repository::{BoundIdentity, RepoId};
use crate::storage::bootstrap::{HostSetupReport, UninstallReport};
use crate::storage::lifecycle::{DerivedWorkspace, MountRefusal, MountState};
use base64::Engine;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Digest;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::io::AsyncRead;

/// The class an exec is admitted under (05_gateway.md "Classes"), fixed at the job's birth. It
/// sets the nice the job's command runs at and whether memory pressure makes the job wait or ends
/// it; it grants no CPU, since the kernel schedules every class.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExecClass {
    /// Every other exec, the default: nice +10. It waits while the host reports memory pressure,
    /// and macOS ends the youngest such job first at Critical.
    #[default]
    Background,
    /// Work a person or model is waiting on, declared by the requester that awaits its result: it
    /// runs at the supervisor's own nice and never waits on memory pressure. A worker may ask for it.
    Interactive,
    /// A land's gate job, a train's proof and the jobs a land's steps start: nice +5, never
    /// CPU-limited, and never made to wait or ended for memory. Only the trusted coordinator route admits it.
    Gate,
    /// A process that stays up to serve requests -- the Nx daemon a shed's supervisor keeps, a
    /// worker that runs each request it is sent -- and is idle between them. It runs at background
    /// nice, starts at once, and macOS ends it first at Critical memory pressure (05_gateway.md "Classes").
    Resident,
    /// A resident that serves work a person or model is waiting on -- a worker whose requests are
    /// focused checks a session awaits. It differs from [`ExecClass::Resident`] in running at the
    /// supervisor's own nice, and it is never ended for memory (05_gateway.md "Classes").
    InteractiveResident,
}

impl ExecClass {
    /// Whether this class is admitted as a resident: a process that stays up to serve requests and
    /// is idle between them (05_gateway.md "Classes").
    pub const fn is_resident(self) -> bool {
        matches!(self, Self::Resident | Self::InteractiveResident)
    }
}

pub const MAX_JOB_ID: u64 = (1_u64 << 53) - 1;
pub const MAX_INLINE_OUTPUT_BYTES: usize = 64 * 1024;
pub const MAX_OUTPUT_SUMMARY_BYTES: usize = 16 * 1024;
pub const MAX_COMMAND_ARG_BYTES: usize = 128 * 1024;
pub const MAX_ARGV_BYTES: usize = 1024 * 1024;
/// The most bytes one tail returns per stream: a tail's slice travels as inline [`BinaryData`].
pub const MAX_JOB_TAIL_BYTES: usize = MAX_INLINE_OUTPUT_BYTES;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DtoError {
    #[error("job id must be in 1..={MAX_JOB_ID}, got {0}")]
    InvalidJobId(u64),
    /// A row's `state` is attached or detached (07_api.md, "Frozen wire projections"), and a
    /// workspace whose mount does not prove itself the workspace's is neither, so it has no row
    /// (01a_apfs.md, "Ownership, identity, and the volume label"): a listing names it as an
    /// [`UnprovenWorkspace`], and a read of that one workspace refuses with this.
    #[error("workspace {name} is neither attached nor detached: {0}", name = .0.workspace.name())]
    UnprovenMount(Box<MountRefusal>),
    #[error("invalid git object id {0:?}")]
    InvalidGitOid(String),
    #[error("invalid UTC timestamp {0:?}")]
    InvalidTimestamp(String),
    #[error("invalid workspace-relative path {0:?}")]
    InvalidWorkspacePath(PathBuf),
    #[error("invalid ref lock path {0:?}: it must be an absolute UTF-8 path with no NUL")]
    InvalidRefLockPath(PathBuf),
    #[error(
        "invalid land-turn queue {0:?}: it must be 1 to 64 of A-Z a-z 0-9 . _ -, not starting with `.`"
    )]
    InvalidLandTurnName(String),
    #[error("invalid trace id {0:?}")]
    InvalidTraceId(String),
    #[error("invalid span id {0:?}")]
    InvalidSpanId(String),
    #[error("invalid job projection: {0}")]
    InvalidJobProjection(&'static str),
    #[error("binary output exceeds the {MAX_INLINE_OUTPUT_BYTES}-byte inline DTO limit")]
    InlineOutputTooLarge,
    #[error("invalid binary output encoding")]
    InvalidBinaryEncoding,
    #[error("command argument exceeds the {MAX_COMMAND_ARG_BYTES}-byte limit")]
    CommandArgumentTooLarge,
    #[error("command argv exceeds the {MAX_ARGV_BYTES}-byte total limit")]
    CommandArgvTooLarge,
    #[error("command argv must contain a non-empty argv[0]")]
    InvalidCommandArgv,
    #[error("command argument contains NUL")]
    CommandArgumentContainsNul,
    #[error("invalid command argument encoding")]
    InvalidCommandArgumentEncoding,
    #[error("command argument cannot be represented exactly on this platform")]
    InvalidPlatformCommandArgument,
    #[error("a tail's bytes per stream must be in 1..={MAX_JOB_TAIL_BYTES}, got {0}")]
    InvalidTailBytes(u32),
    #[error("invalid SHA-256 digest {0:?}")]
    InvalidSha256Digest(String),
    #[error("invalid stream projection: {0}")]
    InvalidStreamProjection(&'static str),
    #[error("invalid branch name {0:?}")]
    InvalidBranchName(String),
    #[error("invalid fully-qualified git ref {0:?}")]
    InvalidGitRef(String),
    #[error(
        "a script needs exactly one more part than values, got {parts} parts and {values} values"
    )]
    ScriptShape { parts: usize, values: usize },
    #[error("script text or a script value contains NUL")]
    ScriptContainsNul,
    #[error("script parts and values exceed the {MAX_ARGV_BYTES}-byte total limit")]
    ScriptTooLarge,
    #[error("a command is exactly one of argv and script")]
    CommandShape,
    #[error("an admission key is 1 to {MAX_ADMISSION_KEY_BYTES} UTF-8 bytes, got {0}")]
    InvalidAdmissionKey(usize),
}

#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "get",
        scalar = "number & tags.Type<'uint64'> & tags.Minimum<1> & tags.Maximum<9007199254740991>"
    )
)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobId(u64);

impl JobId {
    pub fn new(value: u64) -> Result<Self, DtoError> {
        if (1..=MAX_JOB_ID).contains(&value) {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidJobId(value))
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for JobId {
    type Error = DtoError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for JobId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for JobId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "as-str",
        scalar = "string & tags.Pattern<'^([0-9a-f]{40}|[0-9a-f]{64})$'>"
    )
)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GitOid(String);

impl GitOid {
    pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        if matches!(value.len(), 40 | 64) && is_lowercase_hex(&value) {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidGitOid(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for GitOid {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for GitOid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for GitOid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for GitOid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Longest admission key, in UTF-8 bytes: room for an executor's own operation identity.
pub const MAX_ADMISSION_KEY_BYTES: usize = 4096;

/// The caller's name for one idempotent exec within a workspace incarnation (07_api "Keyed
/// admission"): 1 to [`MAX_ADMISSION_KEY_BYTES`] bytes of UTF-8, compared byte for byte. It grants
/// no authority and means nothing in another incarnation.
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "as-str",
        scalar = "string & tags.MinLength<1> & tags.TagBase<{ kind: 'utf8Bytes'; target: 'string'; value: 4096; validate: '(() => { let bytes = 0; for (let index = 0; index < $input.length; index += 1) { const code = $input.charCodeAt(index); if (code < 0x80) bytes += 1; else if (code < 0x800) bytes += 2; else if (code >= 0xd800 && code <= 0xdbff && index + 1 < $input.length) { const low = $input.charCodeAt(index + 1); if (low >= 0xdc00 && low <= 0xdfff) { bytes += 4; index += 1; } else bytes += 3; } else bytes += 3; if (bytes > 4096) return false; } return true; })()' }>"
    )
)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AdmissionKey(String);

impl AdmissionKey {
    pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        if (1..=MAX_ADMISSION_KEY_BYTES).contains(&value.len()) {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidAdmissionKey(value.len()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AdmissionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for AdmissionKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AdmissionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// The UTC second `value` names, when it is an RFC 3339 timestamp: what [`UtcTimestamp`] accepts.
fn rfc3339_utc_seconds(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || !value.is_ascii()
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return None;
    }
    let digits = |start: usize, end: usize| -> Option<u32> {
        bytes
            .get(start..end)?
            .iter()
            .try_fold(0_u32, |value, byte| {
                byte.is_ascii_digit()
                    .then_some(value * 10 + u32::from(byte - b'0'))
            })
    };
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        digits(0, 4),
        digits(5, 7),
        digits(8, 10),
        digits(11, 13),
        digits(14, 16),
        digits(17, 19),
    ) else {
        return None;
    };
    let leap_year = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year => 29,
        2 => 28,
        _ => return None,
    };
    let (time_end, offset_seconds) = if bytes.last() == Some(&b'Z') {
        (bytes.len() - 1, 0_i64)
    } else if bytes.len() >= 25 {
        let sign = bytes.len() - 6;
        let (Some(offset_hour), Some(offset_minute)) =
            (digits(sign + 1, sign + 3), digits(sign + 4, sign + 6))
        else {
            return None;
        };
        if !matches!(bytes[sign], b'+' | b'-')
            || bytes[sign + 3] != b':'
            || offset_hour > 23
            || offset_minute > 59
        {
            return None;
        }
        let magnitude = i64::from(offset_hour * 3600 + offset_minute * 60);
        let offset = if bytes[sign] == b'+' {
            magnitude
        } else {
            -magnitude
        };
        (sign, offset)
    } else {
        return None;
    };
    let fraction_valid = match bytes.get(19..time_end) {
        Some([]) => true,
        Some([b'.', fraction @ ..]) => {
            !fraction.is_empty() && fraction.iter().all(u8::is_ascii_digit)
        }
        _ => false,
    };
    let calendar_valid = (1..=max_day).contains(&day) && hour <= 23 && minute <= 59;
    let second_valid = second <= 59
        || (second == 60
            && calendar_valid
            && is_published_leap_second(year, month, day, hour, minute, offset_seconds));
    (calendar_valid && second_valid && fraction_valid).then(|| {
        days_from_civil(year, month, day) * 86_400
            + i64::from(hour) * 3_600
            + i64::from(minute) * 60
            + i64::from(second)
            - offset_seconds
    })
}

fn days_from_civil(year: u32, month: u32, day: u32) -> i64 {
    let mut year = i64::from(year);
    let month = i64::from(month);
    year -= i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn is_published_leap_second(
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    offset_seconds: i64,
) -> bool {
    const INSERTION_DATES: &[(u32, u32, u32)] = &[
        (1972, 6, 30),
        (1972, 12, 31),
        (1973, 12, 31),
        (1974, 12, 31),
        (1975, 12, 31),
        (1976, 12, 31),
        (1977, 12, 31),
        (1978, 12, 31),
        (1979, 12, 31),
        (1981, 6, 30),
        (1982, 6, 30),
        (1983, 6, 30),
        (1985, 6, 30),
        (1987, 12, 31),
        (1989, 12, 31),
        (1990, 12, 31),
        (1992, 6, 30),
        (1993, 6, 30),
        (1994, 6, 30),
        (1995, 12, 31),
        (1997, 6, 30),
        (1998, 12, 31),
        (2005, 12, 31),
        (2008, 12, 31),
        (2012, 6, 30),
        (2015, 6, 30),
        (2016, 12, 31),
    ];

    let utc_after_leap = days_from_civil(year, month, day) * 86_400
        + i64::from(hour) * 3_600
        + i64::from(minute) * 60
        + 60
        - offset_seconds;
    INSERTION_DATES.iter().any(|&(year, month, day)| {
        utc_after_leap == (days_from_civil(year, month, day) + 1) * 86_400
    })
}

#[cfg_attr(any(), cowshed_api(accessor = "as-str", scalar = "string"))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UtcTimestamp(String);

impl UtcTimestamp {
    pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        let valid = rfc3339_utc_seconds(&value).is_some();
        if valid {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidTimestamp(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The UTC second this timestamp names, offset applied.
    pub fn epoch_seconds(&self) -> i64 {
        rfc3339_utc_seconds(&self.0).expect("a UtcTimestamp is validated when it is made")
    }
}

impl Serialize for UtcTimestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for UtcTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[cfg_attr(any(), cowshed_api(accessor = "as-str", scalar = "string"))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkspacePath(PathBuf);

impl WorkspacePath {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, DtoError> {
        let path = path.into();
        let valid = path.to_str().is_some_and(|text| {
            !text.is_empty()
                && !text.starts_with('/')
                && !text.contains('\0')
                && !text.contains('\\')
                && text
                    .split('/')
                    .all(|component| !matches!(component, "" | "." | ".."))
        });
        if valid {
            Ok(Self(path))
        } else {
            Err(DtoError::InvalidWorkspacePath(path))
        }
    }

    /// The path's spelling as UTF-8 text, borrowed. `new` admits only paths whose bytes are valid
    /// UTF-8, so this never fails, never allocates, and never replaces a byte lossily.
    pub fn as_str(&self) -> &str {
        self.0
            .to_str()
            .expect("WorkspacePath construction guarantees UTF-8")
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }

    pub fn into_path_buf(self) -> PathBuf {
        self.0
    }
}

impl Serialize for WorkspacePath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for WorkspacePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(PathBuf::from(String::deserialize(deserializer)?))
            .map_err(serde::de::Error::custom)
    }
}

/// The absolute host path of the lock file that orders every move of a land target's branch: the
/// caller's, shared with the writers that move that branch outside cowshed (02_workspaces.md
/// "`cowshed land`" step 3). Absolute, because the land opens it in its own process, whatever
/// directory the caller named it from.
#[cfg_attr(any(), cowshed_api(accessor = "as-str", scalar = "string"))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RefLockPath(PathBuf);

impl RefLockPath {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, DtoError> {
        let path = path.into();
        let valid = path
            .to_str()
            .is_some_and(|text| text.starts_with('/') && !text.contains('\0') && text != "/");
        if valid {
            Ok(Self(path))
        } else {
            Err(DtoError::InvalidRefLockPath(path))
        }
    }

    /// The path's spelling as UTF-8 text, borrowed; `new` admits only UTF-8 paths.
    pub fn as_str(&self) -> &str {
        self.0
            .to_str()
            .expect("RefLockPath construction guarantees UTF-8")
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl Serialize for RefLockPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RefLockPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(PathBuf::from(String::deserialize(deserializer)?))
            .map_err(serde::de::Error::custom)
    }
}

/// The name of one land-turn queue (05_gateway.md "Land turns"): a repository's land turn, or the
/// ref turn `<repo>.ref` its other branch writers take ("A ref turn"). It is the queue's directory
/// name, so it is 1 to 64 of `[A-Za-z0-9._-]`, not starting with a dot: no name reaches the
/// queue's parent or a hidden file.
#[cfg_attr(any(), cowshed_api(accessor = "as-str", scalar = "string"))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LandTurnName(String);

impl LandTurnName {
    pub fn new(name: impl Into<String>) -> Result<Self, DtoError> {
        let name = name.into();
        let valid = (1..=64).contains(&name.len())
            && !name.starts_with('.')
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
        if valid {
            Ok(Self(name))
        } else {
            Err(DtoError::InvalidLandTurnName(name))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LandTurnName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for LandTurnName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for LandTurnName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

macro_rules! hex_identifier {
    ($name:ident, $bytes:literal, $error:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
                let value = value.into();
                if value.len() == $bytes * 2
                    && value.bytes().any(|byte| byte != b'0')
                    && is_lowercase_hex(&value)
                {
                    Ok(Self(value))
                } else {
                    Err(DtoError::$error(value))
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

hex_identifier!(TraceId, 16, InvalidTraceId);
hex_identifier!(SpanId, 8, InvalidSpanId);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraceContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
}

impl TraceContext {
    /// A fresh root: a random trace and a random span in it.
    pub fn mint() -> Self {
        let trace = uuid::Uuid::new_v4().simple().to_string();
        let span = uuid::Uuid::new_v4().simple().to_string();
        Self {
            trace_id: TraceId::new(trace).expect("UUID simple form is a nonzero trace id"),
            span_id: SpanId::new(&span[..16]).expect("UUID prefix is a nonzero span id"),
        }
    }

    /// The context a W3C `traceparent` of version `00` names (`00-<trace>-<span>-<flags>`, in
    /// lowercase hex, neither id all zeros), as a traced caller exports it in `TRACEPARENT`;
    /// `None` for any other value, which a participant ignores and starts a fresh trace for.
    pub fn from_traceparent(value: &str) -> Option<Self> {
        let mut fields = value.split('-');
        let (Some("00"), Some(trace), Some(span), Some(flags), None) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            return None;
        };
        if flags.len() != 2 || !is_lowercase_hex(flags) {
            return None;
        }
        Some(Self {
            trace_id: TraceId::new(trace).ok()?,
            span_id: SpanId::new(span).ok()?,
        })
    }
}

/// One lifecycle step of a controller call that asked for its steps, reported when the step starts
/// and again when it ends (`cowshed_os::timing`). Step ids are unique within the call. A step's
/// `parent` is the step it runs inside, absent at the call's top level, and is always reported
/// started before any step inside it; a step ends before its parent does.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "event",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum StepReport {
    Started {
        step: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent: Option<u32>,
        /// The verb or layer the step belongs to: `fork`, `apfs`.
        scope: String,
        /// The step within its scope: `links`, `canonical/mount`.
        name: String,
    },
    Ended {
        step: u32,
        /// Why the step failed; absent when it succeeded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

impl From<cowshed_os::timing::Step> for StepReport {
    fn from(step: cowshed_os::timing::Step) -> Self {
        match step {
            cowshed_os::timing::Step::Started {
                step,
                parent,
                scope,
                name,
            } => Self::Started {
                step,
                parent,
                scope,
                name,
            },
            cowshed_os::timing::Step::Ended { step, error } => Self::Ended { step, error },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkspaceState {
    Attached,
    Detached,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceInfo {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub role: WorkspaceRole,
    pub mount: PathBuf,
    pub state: WorkspaceState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_commit: Option<GitOid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<UtcTimestamp>,
    pub snapshot_stale: bool,
    /// Filled in only for a caller that asked to pay for the measurement (`project.list` with a
    /// landing selection); absent everywhere else, so `default` is what keeps decoding those honest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub landing: Option<WorkspaceLanding>,
}

/// A listing row is its workspace's information; landing measurement and the landed filter
/// (`crate::landing`) read and annotate rows through this.
impl AsRef<Self> for WorkspaceInfo {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl AsMut<Self> for WorkspaceInfo {
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

impl WorkspaceInfo {
    /// Project one exact, validated sidecar and its authoritative lifecycle facts into the API DTO.
    ///
    /// Callers must obtain `metadata` through `gateway_inventory::read_current_metadata`; that
    /// reader proves the sidecar is active, no-follow, and names this exact workspace incarnation.
    pub fn from_current_metadata(
        derived: &DerivedWorkspace,
        mount: PathBuf,
        metadata: &DetachedWorkspaceMetadata,
    ) -> Result<Self, DtoError> {
        let snapshot = &metadata.info_snapshot;
        let base_commit = Some(GitOid::new(snapshot.base_commit.clone())?);
        let created_at = Some(UtcTimestamp::new(snapshot.created_at.clone())?);
        Ok(Self {
            repo_id: derived.workspace.repo().clone(),
            workspace: derived.workspace.name().clone(),
            workspace_incarnation: derived.workspace.incarnation().clone(),
            role: derived.workspace.role(),
            mount,
            state: match &derived.mount_state {
                MountState::Detached => WorkspaceState::Detached,
                MountState::Mounted { .. } => WorkspaceState::Attached,
                MountState::Unproven(refusal) => {
                    return Err(DtoError::UnprovenMount(refusal.clone()));
                }
            },
            branch: snapshot.branch.clone(),
            base_commit,
            created_at,
            snapshot_stale: snapshot.stale,
            landing: None,
        })
    }
}
/// What a landing measurement found for one workspace.
///
/// Present only when a caller asked for it: reading it costs one process per workspace, which a
/// default listing must not pay.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceLanding {
    /// Paths the working tree reports as added, changed, deleted, or untracked.
    ///
    /// `None` means the tree could not be read, which is a different fact from a clean tree and is
    /// reported as a different fact. Measured independently of the target, because an uncommitted
    /// tree is worth knowing about even when the target cannot be resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dirty_files: Option<u64>,
    pub commits: LandingCommits,
    /// The workspace's other local branches, measured the way `commits` measures its head.
    pub branches: BranchLandings,
}

impl WorkspaceLanding {
    /// Whether retiring the workspace destroys no commit: its head is held by the target or kept by
    /// the ref `cowshed push` wrote for its branch ([`LandingCommits::fully_landed`]), and every
    /// other local branch is held by the target's repository or preserved by a branch there or a
    /// push ref in main's.
    pub fn fully_landed(&self) -> bool {
        self.commits.fully_landed() && self.branches.fully_landed()
    }
}

/// What the workspace's local branches other than the checked-out one hold that the target does
/// not. A branch passes when its tip equals or is an ancestor of the target repository's branch of
/// the same name or of its `push` ref `refs/cowshed/<ws>/heads/<branch>` in main's repository,
/// where `cowshed push` writes it whichever target the workspace lands into, or when the target
/// branch holds its content the way [`LandingCommits`] judges a head; every other branch is listed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum BranchLandings {
    /// Every local branch was judged; `unlanded` names each one that did not pass.
    #[serde(rename_all = "camelCase")]
    Measured { unlanded: Vec<BranchLanding> },
    /// The branches could not be listed or judged, and nothing may be inferred: every consumer
    /// treats this exactly as it treats an unlanded branch.
    #[serde(rename_all = "camelCase")]
    Indeterminate { reason: String },
}

impl BranchLandings {
    /// Whether no local branch carries a commit the target lacks.
    pub fn fully_landed(&self) -> bool {
        matches!(self, Self::Measured { unlanded } if unlanded.is_empty())
    }
}

/// One local branch whose commits the target does not hold and no target ref preserves.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BranchLanding {
    /// The branch's short name, `refs/heads/` dropped.
    pub branch: String,
    pub tip: GitOid,
    /// The branch measured against the target like a head: never `fully_landed` here.
    pub commits: LandingCommits,
}

/// Where a workspace's commits stand relative to the branch that outlives it.
///
/// A sum rather than a struct of optionals so that "measured" and "could not measure" cannot be
/// confused by a reader or a script: there is no shape in which a count is absent but a verdict is
/// still available, and none in which the absence of a count reads as zero.
///
/// The verdict is content or patch identity (02_workspaces.md, `rm` step 1): a head the target
/// contains is landed, and otherwise each commit must be held by the target's content (every file
/// the workspace changed since its merge base with the target already carries that change there)
/// or by a patch-equivalent commit of the target. A cherry-pick, a squash, or a pick whose conflict
/// was resolved on the way in all land the work under other commits.
///
/// A head the target does not hold also passes when the ref `cowshed push` wrote for its
/// checked-out branch keeps it ([`PushRef`]): retiring the workspace then loses no commit, that ref
/// being in main's repository, which outlives the image. The counts stay what the target holds, so
/// a listing still says how far the work is from landing; only the verdict reads the ref.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "camelCase")]
pub enum LandingCommits {
    /// Compared against `target_branch` as it stands *now* in the project's main workspace.
    /// `landed + unlanded` is the number of commits in `target..HEAD`.
    #[serde(rename_all = "camelCase")]
    Measured {
        target_branch: String,
        target_head: GitOid,
        /// Commits the target holds neither by content nor by patch identity: each changes a file
        /// `unlanded_paths` names. For a measurement recorded before files were named, those
        /// `git cherry` found unheld. Zero, or a head `preserved_by` keeps, is what authorizes
        /// removing the workspace without `--abandon`.
        unlanded: u64,
        /// Commits whose every change the target already holds, however it got there: by ancestry,
        /// cherry-pick, squash-merge, a history rewrite, or a conflict-resolved pick.
        landed: u64,
        /// Commits the target has that the workspace does not.
        behind: u64,
        /// The files whose change the target does not hold, or that the measurement predates them.
        /// A record without the field is one a build before named files wrote: it reads as
        /// [`UnlandedPaths::PredatesPaths`] and is written back without the field, as it was.
        #[serde(
            default = "UnlandedPaths::predating",
            skip_serializing_if = "UnlandedPaths::is_predating"
        )]
        unlanded_paths: UnlandedPaths,
        /// The ref `cowshed push` wrote in main's repository for the checked-out branch, when it
        /// keeps a head the target does not hold: the head is that ref's commit or an ancestor of
        /// it, so retiring the workspace loses no commit and needs no `--abandon`. `unlanded` still
        /// counts what the target lacks. Absent when the target holds every commit (no ref is
        /// read), when `push` wrote none for the branch, when the head has work the ref lacks, and
        /// for a detached head, which is on no branch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        preserved_by: Option<PushRef>,
    },
    /// Nothing was compared and nothing may be inferred. This never means landed: every consumer
    /// treats it exactly as it treats unlanded work.
    #[serde(rename_all = "camelCase")]
    Indeterminate { reason: String },
}

impl LandingCommits {
    /// Whether the target branch holds every change the head carries, by ancestry, content or patch
    /// identity. A land's fold asks this and never [`Self::fully_landed`]: a fold is work the target
    /// carries, and a head only a push ref keeps is work the target lacks.
    pub const fn held_by_target(&self) -> bool {
        matches!(self, Self::Measured { unlanded: 0, .. })
    }

    /// Whether retiring the workspace destroys none of the head's commits: the target holds every
    /// change ([`Self::held_by_target`]), or the ref `cowshed push` wrote for its branch keeps the
    /// head.
    pub const fn fully_landed(&self) -> bool {
        matches!(
            self,
            Self::Measured { unlanded: 0, .. }
                | Self::Measured {
                    preserved_by: Some(_),
                    ..
                }
        )
    }

    /// The ref that keeps a head the target does not hold, when one does.
    pub const fn preserved_by(&self) -> Option<&PushRef> {
        match self {
            Self::Measured { preserved_by, .. } => preserved_by.as_ref(),
            Self::Indeterminate { .. } => None,
        }
    }

    /// The tip the workspace was compared against, when a comparison happened.
    pub const fn target_head(&self) -> Option<&GitOid> {
        match self {
            Self::Measured { target_head, .. } => Some(target_head),
            Self::Indeterminate { .. } => None,
        }
    }
}

/// A ref `cowshed push` wrote in main's repository, `refs/cowshed/<ws>/heads/<branch>`
/// (02_workspaces.md, `push`), and the commit it names.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushRef {
    /// The full ref name, in main's repository.
    pub reference: String,
    /// The commit the ref names: the head it keeps, or a descendant of it.
    pub tip: GitOid,
}

/// Which files a landing measurement found the target lacking the change of.
///
/// Three facts are kept apart: no file unheld (`Named` and empty), these files unheld (`Named`),
/// and a measurement a build recorded before it named files (`PredatesPaths`): that build judged
/// commits by patch identity and recorded no file, so nothing about files may be inferred from it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum UnlandedPaths {
    /// Each file whose change the target does not hold, in path order, and why.
    Named { paths: Vec<UnlandedPath> },
    /// Recorded by a build from before measurements named files (02_workspaces.md, `rm` step 1).
    PredatesPaths,
}

impl UnlandedPaths {
    const fn predating() -> Self {
        Self::PredatesPaths
    }

    const fn is_predating(&self) -> bool {
        matches!(self, Self::PredatesPaths)
    }
}

/// One file the workspace changed since its merge base with the target whose change the target
/// does not hold.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnlandedPath {
    /// Repository-relative, as git names it; bytes that are not UTF-8 read as U+FFFD.
    pub path: String,
    pub change: UnlandedChange,
}

/// Why the target does not hold the workspace's change to one file. `base` is the file at the
/// merge base, `here` at the workspace head, `target` at the target tip.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum UnlandedChange {
    /// The workspace changed the file and the target's version lacks that change: merging `base`
    /// to `here` into `target` would change it.
    Missing,
    /// The target changed the same or adjacent lines too, or both sides added the file, and the
    /// target's version of those lines lacks the workspace's.
    Conflict,
    /// The workspace adds the file and the target does not have it.
    Added,
    /// The workspace deletes the file and the target still has it.
    Deleted,
    /// The workspace changes the file and the target deleted it.
    DeletedInTarget,
    /// The workspace changed the file's mode (executable bit, symlink, submodule) and the target's
    /// mode is another.
    Mode,
    /// Git cannot merge the versions line by line (binary content), so nothing shows the target
    /// holds the change. `reason` is git's own.
    Unmergeable { reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectWorkspaces {
    pub repo_id: RepoId,
    pub workspaces: Vec<WorkspaceInfo>,
    /// Each workspace of the project whose mount is unproven, which has no row; omitted when none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unproven: Vec<UnprovenWorkspace>,
}

/// A published workspace whose mount does not prove itself the workspace's. It is neither attached
/// nor detached, so a listing names it here instead of giving it a [`WorkspaceInfo`] row (07_api.md,
/// "Frozen wire projections"; 01a_apfs.md, "Ownership, identity, and the volume label").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnprovenWorkspace {
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    /// The workspace's canonical mount path, the one its row would carry.
    pub mount: PathBuf,
    /// The refusal that left the mount unproven, as `doctor`'s `mount-identity` finding says it.
    pub reason: String,
}

impl UnprovenWorkspace {
    /// The workspace `refusal` refused, whose canonical mount path is `mount`.
    pub fn of(refusal: &MountRefusal, mount: PathBuf) -> Self {
        Self {
            workspace: refusal.workspace.name().clone(),
            workspace_incarnation: refusal.workspace.incarnation().clone(),
            mount,
            reason: refusal.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MountResult {
    pub workspace: WorkspaceName,
    pub mount: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_commit: Option<GitOid>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyResult {}

/// The answer to `cowshed rekey <ws>`: the rotated workspace identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RekeyResult {
    pub workspace: String,
    pub workspace_incarnation: String,
    pub revision: u64,
    /// The consumed quarantine entry, when the quarantine path ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone_removed: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SlotResult {
    pub slot: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FindingSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Finding {
    pub code: String,
    pub severity: FindingSeverity,
    pub message: String,
    pub hint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DoctorReport {
    pub healthy: bool,
    pub findings: Vec<Finding>,
}

impl DoctorReport {
    /// `healthy` is exactly "no Error-severity finding". CLI and runtime both
    /// answer that question here so a warning-only report cannot disagree with
    /// itself across the two authors.
    pub fn from_findings(findings: Vec<Finding>) -> Self {
        Self {
            healthy: !findings
                .iter()
                .any(|finding| finding.severity == FindingSeverity::Error),
            findings,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum GcReason {
    RetiredWorkspace,
    OrphanStagingImage,
    OrphanStagingMetadata,
    OrphanStagingMount,
    OrphanSessionImage,
    OrphanMountpoint,
    /// The `<ws>-<tip>.bundle` recovery bundle `rm --abandon` keeps in `sessions/.trash/`, the
    /// only copy of the commits it holds. A candidate only under a sweep that explicitly authorizes
    /// it (`GcOptions.abandoned`).
    AbandonedBundle,
    /// An abandon bundle every commit of which main holds by patch identity (02_workspaces.md,
    /// `rm` step 1): it keeps nothing main lacks, so a plain sweep drops it.
    HeldBundle,
    /// A build volume no checkout links: a checkout's live volume its checkout no longer links,
    /// or one superseded under its owner once no job holds it (16_build_volumes.md, "Garbage
    /// collection").
    UnlinkedBuildVolume,
    /// A seed that is not its target's latest, or whose target retired.
    SupersededSeed,
    /// A build volume registered to its owner before its image existed whose creation was
    /// interrupted: its registrant is gone, so it is released by its exact id.
    InterruptedBuildVolume,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GcCandidate {
    pub identity: Sha256Digest,
    pub path: PathBuf,
    pub bytes: u64,
    pub reason: GcReason,
}

/// One candidate left for a later sweep, with the exact reason this run could not reclaim it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GcDeferred {
    pub path: PathBuf,
    pub diagnostic: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GcReport {
    pub examined: u64,
    pub reclaimed: u64,
    /// Staging entries an operation still holds the lifecycle lock for: not garbage yet.
    pub retained_active: u64,
    pub released_bytes: u64,
    pub dry_run: bool,
    pub candidates: Vec<GcCandidate>,
    pub deferred: Vec<GcDeferred>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JobState {
    Queued,
    Running,
    /// The job's leader has ended and the supervisor is retiring the rest of its process
    /// membership. Not terminal: a waiter keeps waiting, and no exit, duration or frozen sample
    /// is published until the membership is proven empty. Draining lives only in a supervisor's
    /// memory; a durable record is never draining.
    Draining,
    Exited,
    Signaled,
    Killed,
    OutputLimit,
    Failed,
}

impl JobState {
    /// Whether the job has ended: `Exited`, `Signaled`, `Killed`, `OutputLimit` and `Failed` are
    /// final, while `Queued`, `Running` and `Draining` are not. The match is exhaustive, so a new
    /// state has to say which it is.
    pub fn is_terminal(self) -> bool {
        match self {
            Self::Queued | Self::Running | Self::Draining => false,
            Self::Exited | Self::Signaled | Self::Killed | Self::OutputLimit | Self::Failed => true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputSummary {
    pub version: u16,
    pub text: String,
    pub truncated: bool,
}

/// One immutable, byte-exact operating-system command argument.
///
/// On Unix, serde preserves the bytes returned by `OsStrExt::as_bytes`: valid UTF-8 uses the
/// `utf8` tag and every other sequence uses canonical standard base64. No conversion is lossy.
#[cfg_attr(any(), cowshed_api(wire = "TaggedBytesRef"))]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommandArg(OsString);

impl CommandArg {
    pub fn new(value: impl Into<OsString>) -> Self {
        Self(value.into())
    }

    pub fn as_os_str(&self) -> &OsStr {
        &self.0
    }

    pub fn into_os_string(self) -> OsString {
        self.0
    }

    pub fn validate(&self) -> Result<(), DtoError> {
        let bytes = command_arg_bytes(self.as_os_str())?;
        if bytes.len() > MAX_COMMAND_ARG_BYTES {
            return Err(DtoError::CommandArgumentTooLarge);
        }
        if bytes.contains(&0) {
            return Err(DtoError::CommandArgumentContainsNul);
        }
        Ok(())
    }

    /// Borrows the tagged projection [`Serialize`] writes for this argument.
    ///
    /// Valid UTF-8 is `utf8` and borrows those bytes. Every other sequence is canonical
    /// standard base64. Validation failure is [`DtoError`] and does not call `use_wire`.
    pub fn with_wire<R>(
        &self,
        use_wire: impl FnOnce(&TaggedBytesRef<'_>) -> R,
    ) -> Result<R, DtoError> {
        self.validate()?;
        let bytes = command_arg_bytes(self.as_os_str())?;
        Ok(project_tagged_bytes(bytes, use_wire))
    }
}

impl From<OsString> for CommandArg {
    fn from(value: OsString) -> Self {
        Self(value)
    }
}

impl From<String> for CommandArg {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for CommandArg {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<&OsStr> for CommandArg {
    fn from(value: &OsStr) -> Self {
        Self(value.to_owned())
    }
}

impl From<CommandArg> for OsString {
    fn from(value: CommandArg) -> Self {
        value.into_os_string()
    }
}

impl AsRef<OsStr> for CommandArg {
    fn as_ref(&self) -> &OsStr {
        self.as_os_str()
    }
}

/// Validates process-level argv invariants without allocating or rewriting an argument.
pub fn validate_command_argv(argv: &[CommandArg]) -> Result<(), DtoError> {
    let first = argv.first().ok_or(DtoError::InvalidCommandArgv)?;
    if command_arg_bytes(first.as_os_str())?.is_empty() {
        return Err(DtoError::InvalidCommandArgv);
    }
    let mut total = 0_usize;
    for argument in argv {
        argument.validate()?;
        total = total
            .checked_add(command_arg_bytes(argument.as_os_str())?.len())
            .ok_or(DtoError::CommandArgvTooLarge)?;
        if total > MAX_ARGV_BYTES {
            return Err(DtoError::CommandArgvTooLarge);
        }
    }
    Ok(())
}

#[cfg(unix)]
fn command_arg_bytes(value: &OsStr) -> Result<&[u8], DtoError> {
    Ok(value.as_bytes())
}

#[cfg(not(unix))]
fn command_arg_bytes(value: &OsStr) -> Result<&[u8], DtoError> {
    value
        .to_str()
        .map(str::as_bytes)
        .ok_or(DtoError::InvalidPlatformCommandArgument)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaggedBytesEncoding {
    Utf8,
    Base64,
}

#[derive(Serialize)]
pub struct TaggedBytesRef<'a> {
    pub encoding: TaggedBytesEncoding,
    pub data: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaggedBytesWire {
    encoding: TaggedBytesEncoding,
    data: String,
}

enum TaggedBytesError {
    TooLarge,
    InvalidEncoding,
}

fn project_tagged_bytes<R>(bytes: &[u8], use_wire: impl FnOnce(&TaggedBytesRef<'_>) -> R) -> R {
    match std::str::from_utf8(bytes) {
        Ok(data) => use_wire(&TaggedBytesRef {
            encoding: TaggedBytesEncoding::Utf8,
            data,
        }),
        Err(_) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            use_wire(&TaggedBytesRef {
                encoding: TaggedBytesEncoding::Base64,
                data: &encoded,
            })
        }
    }
}

pub(crate) fn serialize_tagged_bytes<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    project_tagged_bytes(bytes, |proxy| proxy.serialize(serializer))
}

/// Reads the tagged form [`serialize_tagged_bytes`] writes, refusing more than `maximum` bytes.
pub(crate) fn deserialize_tagged_bytes<'de, D>(
    deserializer: D,
    maximum: usize,
) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    decode_tagged_bytes(TaggedBytesWire::deserialize(deserializer)?, maximum).map_err(|error| {
        match error {
            TaggedBytesError::TooLarge => {
                serde::de::Error::custom(format!("tagged bytes exceed {maximum} bytes"))
            }
            TaggedBytesError::InvalidEncoding => {
                serde::de::Error::custom(DtoError::InvalidBinaryEncoding)
            }
        }
    })
}

fn decode_tagged_bytes(wire: TaggedBytesWire, maximum: usize) -> Result<Vec<u8>, TaggedBytesError> {
    match wire.encoding {
        TaggedBytesEncoding::Utf8 => {
            if wire.data.len() > maximum {
                return Err(TaggedBytesError::TooLarge);
            }
            Ok(wire.data.into_bytes())
        }
        TaggedBytesEncoding::Base64 => {
            let maximum_encoded = maximum.div_ceil(3) * 4;
            if wire.data.len() > maximum_encoded {
                return Err(TaggedBytesError::TooLarge);
            }
            let padding = wire
                .data
                .as_bytes()
                .iter()
                .rev()
                .take_while(|&&byte| byte == b'=')
                .count();
            if !wire.data.len().is_multiple_of(4) || padding > 2 {
                return Err(TaggedBytesError::InvalidEncoding);
            }
            let decoded_len = wire.data.len() / 4 * 3 - padding;
            if decoded_len > maximum {
                return Err(TaggedBytesError::TooLarge);
            }
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(wire.data.as_bytes())
                .map_err(|_| TaggedBytesError::InvalidEncoding)?;
            if base64::engine::general_purpose::STANDARD.encode(&bytes) != wire.data
                || std::str::from_utf8(&bytes).is_ok()
            {
                return Err(TaggedBytesError::InvalidEncoding);
            }
            Ok(bytes)
        }
    }
}

impl Serialize for CommandArg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.with_wire(|proxy| proxy.serialize(serializer)) {
            Ok(serialized) => serialized,
            Err(error) => Err(serde::ser::Error::custom(error)),
        }
    }
}

impl<'de> Deserialize<'de> for CommandArg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = decode_tagged_bytes(
            TaggedBytesWire::deserialize(deserializer)?,
            MAX_COMMAND_ARG_BYTES,
        )
        .map_err(|error| match error {
            TaggedBytesError::TooLarge => {
                serde::de::Error::custom(DtoError::CommandArgumentTooLarge)
            }
            TaggedBytesError::InvalidEncoding => {
                serde::de::Error::custom(DtoError::InvalidCommandArgumentEncoding)
            }
        })?;
        command_arg_from_decoded_bytes(bytes).map_err(serde::de::Error::custom)
    }
}

fn command_arg_from_decoded_bytes(bytes: Vec<u8>) -> Result<CommandArg, DtoError> {
    if bytes.len() > MAX_COMMAND_ARG_BYTES {
        return Err(DtoError::CommandArgumentTooLarge);
    }
    if bytes.contains(&0) {
        return Err(DtoError::CommandArgumentContainsNul);
    }
    #[cfg(unix)]
    let value = OsString::from_vec(bytes);
    #[cfg(not(unix))]
    let value = String::from_utf8(bytes)
        .map(OsString::from)
        .map_err(|_| DtoError::InvalidPlatformCommandArgument)?;
    Ok(CommandArg(value))
}

/// Exact, bounded bytes for control-oriented JSON.
///
/// Valid UTF-8 is emitted as tagged UTF-8. Every other byte sequence is emitted as tagged
/// base64. The tag makes the wire representation unambiguous and deserialization never performs
/// lossy conversion.
#[cfg_attr(any(), cowshed_api(wire = "TaggedBytesRef"))]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BinaryData(Vec<u8>);

impl BinaryData {
    pub fn new(data: impl Into<Vec<u8>>) -> Result<Self, DtoError> {
        let data = data.into();
        if data.len() > MAX_INLINE_OUTPUT_BYTES {
            return Err(DtoError::InlineOutputTooLarge);
        }
        Ok(Self(data))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl Serialize for BinaryData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_tagged_bytes(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for BinaryData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = decode_tagged_bytes(
            TaggedBytesWire::deserialize(deserializer)?,
            MAX_INLINE_OUTPUT_BYTES,
        )
        .map_err(|error| match error {
            TaggedBytesError::TooLarge => serde::de::Error::custom(DtoError::InlineOutputTooLarge),
            TaggedBytesError::InvalidEncoding => {
                serde::de::Error::custom(DtoError::InvalidBinaryEncoding)
            }
        })?;
        Self::new(bytes).map_err(serde::de::Error::custom)
    }
}

/// A fixed-width SHA-256 digest, serialized as 64 lowercase hexadecimal characters.
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "display",
        scalar = "string & tags.Pattern<'^[0-9a-f]{64}$'>"
    )
)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn compute(bytes: &[u8]) -> Self {
        Self(sha2::Sha256::digest(bytes).into())
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        hex_lower(&self.0)
    }

    pub fn from_hex(value: &str) -> Result<Self, DtoError> {
        if value.len() != 64 || !is_lowercase_hex(value) {
            return Err(DtoError::InvalidSha256Digest(value.to_owned()));
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let high = hex_nibble(pair[0]);
            let low = hex_nibble(pair[1]);
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }
}

pub(crate) fn is_lowercase_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("Sha256Digest::from_hex validates hexadecimal input"),
    }
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_hex(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ProtectedOutput {
    Inline { data: BinaryData },
    File { path: WorkspacePath },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OutputStorage {
    Captured {
        artifact: ProtectedOutput,
    },
    Redirect {
        source: WorkspacePath,
        artifact: ProtectedOutput,
    },
}

impl OutputStorage {
    pub fn artifact(&self) -> &ProtectedOutput {
        match self {
            Self::Captured { artifact } | Self::Redirect { artifact, .. } => artifact,
        }
    }
}

#[cfg_attr(any(), cowshed_api(wire = "StreamInfoRef"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamInfo {
    pub storage: OutputStorage,
    pub bytes: u64,
    pub sha256: Sha256Digest,
    pub summary: OutputSummary,
}

impl StreamInfo {
    pub fn validate(&self) -> Result<(), DtoError> {
        if self.summary.version == 0 {
            return Err(DtoError::InvalidStreamProjection(
                "summary version must be non-zero",
            ));
        }
        if self.summary.text.len() > MAX_OUTPUT_SUMMARY_BYTES {
            return Err(DtoError::InvalidStreamProjection(
                "summary text exceeds the bounded DTO limit",
            ));
        }
        if let ProtectedOutput::Inline { data } = self.storage.artifact() {
            if self.bytes != data.as_bytes().len() as u64 {
                return Err(DtoError::InvalidStreamProjection(
                    "inline byte count does not match data",
                ));
            }
            if self.sha256 != Sha256Digest::compute(data.as_bytes()) {
                return Err(DtoError::InvalidStreamProjection(
                    "inline SHA-256 does not match data",
                ));
            }
        }
        Ok(())
    }

    fn validate_for(&self, job_id: JobId, leaf: &str) -> Result<(), DtoError> {
        self.validate()?;
        if let ProtectedOutput::File { path } = self.storage.artifact() {
            let expected = format!(".cowshed/job/{}/{leaf}", job_id.get());
            if path.as_path() != Path::new(&expected) {
                return Err(DtoError::InvalidStreamProjection(
                    "protected file path does not match job identity and stream",
                ));
            }
        }
        Ok(())
    }
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamInfoRef<'a> {
    storage: &'a OutputStorage,
    bytes: u64,
    sha256: Sha256Digest,
    summary: &'a OutputSummary,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StreamInfoWire {
    storage: OutputStorage,
    bytes: u64,
    sha256: Sha256Digest,
    summary: OutputSummary,
}

impl Serialize for StreamInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(serde::ser::Error::custom)?;
        StreamInfoRef {
            storage: &self.storage,
            bytes: self.bytes,
            sha256: self.sha256,
            summary: &self.summary,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StreamInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = StreamInfoWire::deserialize(deserializer)?;
        let value = Self {
            storage: wire.storage,
            bytes: wire.bytes,
            sha256: wire.sha256,
            summary: wire.summary,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ExitStatus {
    Exited { code: i32 },
    Signaled { signal: i32, core_dumped: bool },
}

/// The disk tool `host.diskCommand` runs, as `disk-command` names it: never a path.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiskCommandTool {
    Diskutil,
    Hdiutil,
}

/// One event of a `host.diskCommand` stream. The runner collects the tool's output and delivers
/// it once the tool has ended, as `disk-command` does: its whole stdout in order, in chunks of at
/// most one inline frame, then its whole stderr the same way, then its exit, the last event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DiskCommandEvent {
    Stdout {
        data: BinaryData,
    },
    Stderr {
        data: BinaryData,
    },
    /// The tool's status as `disk-command` exits with it: its exit code, or 128 plus the signal
    /// that ended it.
    Exit {
        code: i32,
    },
}

impl ExitStatus {
    /// The status as a shell reports it: the exit code, or 128 plus the signal that ended it.
    pub const fn shell_code(&self) -> i32 {
        match self {
            Self::Exited { code } => *code,
            Self::Signaled { signal, .. } => 128_i32.saturating_add(*signal),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputLimitInfo {
    pub limit_bytes: u64,
    pub crossing_bytes: u64,
}

/// A position in a job's two output journals: how many admitted bytes of each stream precede it.
/// A stream's admitted byte count is the cursor just past its end.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobJournalCursor {
    pub stdout: u64,
    pub stderr: u64,
}

/// How many bytes a tail returns per stream: at least one, at most 64 KiB.
#[cfg_attr(
    any(),
    cowshed_api(
        accessor = "get",
        scalar = "number & tags.Type<'uint32'> & tags.Minimum<1> & tags.Maximum<65536>"
    )
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobTailBytes(u32);

impl JobTailBytes {
    pub fn new(value: u32) -> Result<Self, DtoError> {
        if value != 0 && usize::try_from(value).is_ok_and(|bytes| bytes <= MAX_JOB_TAIL_BYTES) {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidTailBytes(value))
        }
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Serialize for JobTailBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.0)
    }
}

impl<'de> Deserialize<'de> for JobTailBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u32::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// The bounds of one tail, applied to each stream on its own.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobTailLimits {
    pub bytes_per_stream: JobTailBytes,
    pub lines_per_stream: std::num::NonZeroU32,
}

/// A bounded raw slice of each stream and the cursor that continues after it. A slice after a
/// cursor is the start of what follows it; the latest tail is the end of what was admitted.
/// `*Truncated` says admitted bytes of the window the request named lie outside the slice:
/// after it when a cursor named the start, before it for the latest tail.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobTail {
    pub stdout: BinaryData,
    pub stderr: BinaryData,
    pub next: JobJournalCursor,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

/// The TCP ports on which processes of the job's group listen, read from the kernel at
/// `sampledAt`: IPv4 and IPv6 alike, ascending, each once. A port another process holds, or a
/// socket that is only bound or connected, is never one of them; nothing probes a port.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobListeningPorts {
    pub job_id: JobId,
    pub sampled_at: UtcTimestamp,
    pub ports: Vec<u16>,
}

pub const CONTROLLER_COMMITMENT_VERSION: u16 = 2;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceIntroducedCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceRetiredCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdmissionCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub grant_revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    pub grant_revision: u64,
    pub stdout_bytes: u64,
    pub stdout_sha256: Sha256Digest,
    pub stderr_bytes: u64,
    pub stderr_sha256: Sha256Digest,
    pub batch_sha256: Sha256Digest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_limit: Option<OutputLimitInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForkCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    pub source_incarnation: WorkspaceIncarnation,
    pub destination_incarnation: WorkspaceIncarnation,
}

/// One Nx task that missed the cache when a land re-ran its check in the target on the adopted
/// build volume (16_build_volumes.md, Land step 7): a defect in the project's build
/// configuration, recorded once per miss so a coordinator can turn each into fix work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandAdoptionCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    /// The workspace that landed; its build volume is now the target's.
    pub landing_incarnation: WorkspaceIncarnation,
    pub target_incarnation: WorkspaceIncarnation,
    pub landed_head: GitOid,
    /// The Nx task id (`project:target`, or `project:target:configuration`).
    pub task: String,
    /// The task hash Nx computed in the target.
    pub task_hash: String,
    /// The digest of the task's hash inputs as `nx show target inputs` named them; the land
    /// report carries the inputs themselves.
    pub inputs_digest: Sha256Digest,
}

/// One land's actual build-volume outcome, including a seeded-but-skipped adoption.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandOutcomeCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    pub landing_incarnation: WorkspaceIncarnation,
    pub target_incarnation: WorkspaceIncarnation,
    pub landed_head: GitOid,
    pub seeded: bool,
    /// None only when adoption succeeded; otherwise the report's complete refusal.
    pub skip: Option<AdoptionSkip>,
}

/// Versioned controller-owned existence, lifecycle, order, and lineage evidence.
///
/// Protected payload bytes and artifact paths deliberately do not appear in any variant.
#[cfg_attr(any(), cowshed_api(wire = "ControllerCommitmentRef"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControllerCommitment {
    WorkspaceIntroduced(WorkspaceIntroducedCommitment),
    WorkspaceRetired(WorkspaceRetiredCommitment),
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Fork(ForkCommitment),
    LandAdoption(LandAdoptionCommitment),
    LandOutcome(LandOutcomeCommitment),
}

impl ControllerCommitment {
    pub fn version(&self) -> u16 {
        match self {
            Self::WorkspaceIntroduced(value) => value.version,
            Self::WorkspaceRetired(value) => value.version,
            Self::Admission(value) => value.version,
            Self::Terminal(value) => value.version,
            Self::Fork(value) => value.version,
            Self::LandAdoption(value) => value.version,
            Self::LandOutcome(value) => value.version,
        }
    }

    pub fn order(&self) -> u64 {
        match self {
            Self::WorkspaceIntroduced(value) => value.order,
            Self::WorkspaceRetired(value) => value.order,
            Self::Admission(value) => value.order,
            Self::Terminal(value) => value.order,
            Self::Fork(value) => value.order,
            Self::LandAdoption(value) => value.order,
            Self::LandOutcome(value) => value.order,
        }
    }

    pub fn repo_id(&self) -> &RepoId {
        match self {
            Self::WorkspaceIntroduced(value) => &value.repo_id,
            Self::WorkspaceRetired(value) => &value.repo_id,
            Self::Admission(value) => &value.repo_id,
            Self::Terminal(value) => &value.repo_id,
            Self::Fork(value) => &value.repo_id,
            Self::LandAdoption(value) => &value.repo_id,
            Self::LandOutcome(value) => &value.repo_id,
        }
    }

    pub fn validate(&self) -> Result<(), DtoError> {
        if self.version() != CONTROLLER_COMMITMENT_VERSION {
            return Err(DtoError::InvalidJobProjection(
                "unsupported controller commitment version",
            ));
        }
        if self.order() == 0 {
            return Err(DtoError::InvalidJobProjection(
                "controller commitment order must be positive",
            ));
        }
        match self {
            Self::Terminal(value) if !value.state.is_terminal() => Err(
                DtoError::InvalidJobProjection("terminal commitment must contain a terminal state"),
            ),
            Self::Terminal(value)
                if value.output_limit.is_some() != matches!(value.state, JobState::OutputLimit) =>
            {
                Err(DtoError::InvalidJobProjection(
                    "terminal output limit evidence must agree with state",
                ))
            }
            Self::Terminal(value)
                if value
                    .output_limit
                    .as_ref()
                    .is_some_and(|limit| limit.crossing_bytes <= limit.limit_bytes) =>
            {
                Err(DtoError::InvalidJobProjection(
                    "terminal output limit crossing must exceed its limit",
                ))
            }
            Self::Fork(value) if value.source_incarnation == value.destination_incarnation => {
                Err(DtoError::InvalidJobProjection(
                    "fork source and destination incarnations must differ",
                ))
            }
            Self::LandAdoption(value) if value.landing_incarnation == value.target_incarnation => {
                Err(DtoError::InvalidJobProjection(
                    "land adoption landing and target incarnations must differ",
                ))
            }
            Self::LandAdoption(value) if value.task.is_empty() || value.task_hash.is_empty() => {
                Err(DtoError::InvalidJobProjection(
                    "land adoption task and hash must be named",
                ))
            }
            Self::LandOutcome(value) if value.landing_incarnation == value.target_incarnation => {
                Err(DtoError::InvalidJobProjection(
                    "land outcome landing and target incarnations must differ",
                ))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum ControllerCommitmentRef<'a> {
    WorkspaceIntroduced(&'a WorkspaceIntroducedCommitment),
    WorkspaceRetired(&'a WorkspaceRetiredCommitment),
    Admission(&'a AdmissionCommitment),
    Terminal(&'a TerminalCommitment),
    Fork(&'a ForkCommitment),
    LandAdoption(&'a LandAdoptionCommitment),
    LandOutcome(&'a LandOutcomeCommitment),
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum ControllerCommitmentWire {
    WorkspaceIntroduced(WorkspaceIntroducedCommitment),
    WorkspaceRetired(WorkspaceRetiredCommitment),
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Fork(ForkCommitment),
    LandAdoption(LandAdoptionCommitment),
    LandOutcome(LandOutcomeCommitment),
}

impl Serialize for ControllerCommitment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(serde::ser::Error::custom)?;
        match self {
            Self::WorkspaceIntroduced(value) => ControllerCommitmentRef::WorkspaceIntroduced(value),
            Self::WorkspaceRetired(value) => ControllerCommitmentRef::WorkspaceRetired(value),
            Self::Admission(value) => ControllerCommitmentRef::Admission(value),
            Self::Terminal(value) => ControllerCommitmentRef::Terminal(value),
            Self::Fork(value) => ControllerCommitmentRef::Fork(value),
            Self::LandAdoption(value) => ControllerCommitmentRef::LandAdoption(value),
            Self::LandOutcome(value) => ControllerCommitmentRef::LandOutcome(value),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ControllerCommitment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = match ControllerCommitmentWire::deserialize(deserializer)? {
            ControllerCommitmentWire::WorkspaceIntroduced(value) => {
                Self::WorkspaceIntroduced(value)
            }
            ControllerCommitmentWire::WorkspaceRetired(value) => Self::WorkspaceRetired(value),
            ControllerCommitmentWire::Admission(value) => Self::Admission(value),
            ControllerCommitmentWire::Terminal(value) => Self::Terminal(value),
            ControllerCommitmentWire::Fork(value) => Self::Fork(value),
            ControllerCommitmentWire::LandAdoption(value) => Self::LandAdoption(value),
            ControllerCommitmentWire::LandOutcome(value) => Self::LandOutcome(value),
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StdinKind {
    Empty,
    Inline,
    Stream,
    WorkspaceFile,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StdinInfo {
    pub kind: StdinKind,
    pub bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<WorkspacePath>,
    pub complete: bool,
}

#[cfg_attr(any(), cowshed_api(wire = "JobInfoRef"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobInfo {
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    pub pid: Option<u32>,
    pub grant_revision: u64,
    pub command: ExecCommand,
    pub cwd: Option<WorkspacePath>,
    pub started: UtcTimestamp,
    pub duration_ms: Option<u64>,
    /// The latest sample once the job owns a process; frozen at terminal publication. Absent
    /// before any process of the job exists: a job that never spawned has nothing to sample.
    pub resources: Option<crate::api::resources::JobResourceSample>,
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub trace: TraceContext,
    pub output_limit: Option<OutputLimitInfo>,
    pub stdin: StdinInfo,
    /// Set only for a job that failed before its command ran, for a reason its exit status
    /// cannot name.
    pub failure: Option<JobFailure>,
    /// The class the job was admitted under (05_gateway.md "Classes"): background unless its
    /// requester declared it interactive, a trusted coordinator route admitted it as a gate, or
    /// it was admitted as a resident or an interactive resident.
    pub class: ExecClass,
    /// Where the job's command ran: the workspace sandbox, or a coordinator's host gate.
    pub scope: ExecScope,
    /// The nice the job's command was observed at right after its spawn set it: interactive runs
    /// at 0, a gate at +5, background and a resident at +10. Absent before the command exists and
    /// for a job whose command was never observed; never inferred from the class.
    pub nice: Option<i32>,
    /// What held the job's process membership while its retirement proved the membership empty,
    /// and so how far that proof reaches ([`JobContainment`]). Stated once retirement has begun
    /// and its proof exists: `draining` or terminal. Absent while the job queues or runs, for a
    /// job that never owned a process, and for one whose supervisor owns no native membership:
    /// absence is no proof, and never stands for `kernel`.
    pub containment: Option<JobContainment>,
    /// The live members retirement signalled, including a live leader on cancellation, before the
    /// membership was proven empty. Empty proves none were ended only when `outlived_incomplete`
    /// is false; always empty without `containment`.
    pub outlived: Vec<OutlivedProcess>,
    /// Retirement may have ended members absent from `outlived`; false without `containment`.
    pub outlived_incomplete: bool,
}

impl JobInfo {
    pub fn validate(&self) -> Result<(), DtoError> {
        self.command.validate()?;
        if self.failure.is_some() && self.state != JobState::Failed {
            return Err(DtoError::InvalidJobProjection(
                "failure is present only for the failed state",
            ));
        }
        if self.nice.is_some() && self.state == JobState::Queued {
            return Err(DtoError::InvalidJobProjection(
                "a queued job owns no command to observe a nice for",
            ));
        }
        let terminal = self.state.is_terminal();
        if terminal != self.duration_ms.is_some() {
            return Err(DtoError::InvalidJobProjection(
                "durationMs must be present exactly for terminal states",
            ));
        }
        if matches!(self.state, JobState::OutputLimit) != self.output_limit.is_some() {
            return Err(DtoError::InvalidJobProjection(
                "outputLimit must be present exactly for the outputLimit state",
            ));
        }
        // A job states what retiring its process membership found only once that retirement has
        // begun, and only if it owned a process (a pid): a queued or running job has retired
        // nothing, and one that never spawned has no membership to retire.
        let retirement_begun = match self.state {
            JobState::Queued | JobState::Running => false,
            JobState::Draining
            | JobState::Exited
            | JobState::Signaled
            | JobState::Killed
            | JobState::OutputLimit
            | JobState::Failed => true,
        };
        if (self.containment.is_some() || !self.outlived.is_empty() || self.outlived_incomplete)
            && !(retirement_begun && self.pid.is_some())
        {
            return Err(DtoError::InvalidJobProjection(
                "containment, outlived and outlivedIncomplete are stated only for a job that owned a process and \
                 has begun retiring its membership",
            ));
        }
        if self.containment.is_none() && (!self.outlived.is_empty() || self.outlived_incomplete) {
            return Err(DtoError::InvalidJobProjection(
                "outlived processes and incomplete metadata need the containment that retired them",
            ));
        }
        if !self.outlived.iter().all(OutlivedProcess::consistent) {
            return Err(DtoError::InvalidJobProjection(
                "an outlived process names its image or none: an empty one stands for neither",
            ));
        }
        if let Some(resources) = &self.resources {
            if self.state == JobState::Queued {
                return Err(DtoError::InvalidJobProjection(
                    "a queued job owns no process to sample",
                ));
            }
            if resources.job_id != self.job_id {
                return Err(DtoError::InvalidJobProjection(
                    "resources must sample the job that carries them",
                ));
            }
            if !resources.consistent() {
                return Err(DtoError::InvalidJobProjection(
                    "resources must agree with themselves: wallMs is wallUs in whole \
                     milliseconds, rssPeakBytes at least rssBytes, and each stream's lines fit its \
                     bytes",
                ));
            }
        }
        match (&self.state, &self.exit) {
            (JobState::Exited, Some(ExitStatus::Exited { .. }))
            | (JobState::Signaled, Some(ExitStatus::Signaled { .. }))
            // Cancellation is the supervisor's decision; the child may handle
            // SIGTERM and exit normally. Preserve the actual wait status.
            | (JobState::Killed, Some(_))
            | (JobState::OutputLimit | JobState::Failed, _)
            | (JobState::Queued | JobState::Running | JobState::Draining, None) => {}
            // Killed before its command existed -- withdrawn while queued for CPU admission
            // (11_shell.md "Job admission and class placement"): it never ran, so it
            // has no status. A job that owned a process has a pid and keeps the status it ended
            // with, so "never ran" and "ran and was killed" stay distinct.
            (JobState::Killed, None) if self.pid.is_none() => {}
            _ => {
                return Err(DtoError::InvalidJobProjection(
                    "exit kind does not agree with job state",
                ));
            }
        }
        self.stdout.validate_for(self.job_id, "out")?;
        self.stderr.validate_for(self.job_id, "err")
    }
}

#[cfg_attr(any(), cowshed_api(exclusive = "argv,script"))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobInfoRef<'a> {
    repo_id: &'a RepoId,
    workspace_incarnation: &'a WorkspaceIncarnation,
    job_id: JobId,
    state: JobState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<u32>,
    grant_revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    argv: Option<&'a [CommandArg]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    script: Option<&'a ScriptCommand>,
    cwd: &'a Option<WorkspacePath>,
    started: &'a UtcTimestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<&'a crate::api::resources::JobResourceSample>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit: Option<&'a ExitStatus>,
    stdout: &'a StreamInfo,
    stderr: &'a StreamInfo,
    trace: &'a TraceContext,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_limit: Option<&'a OutputLimitInfo>,
    stdin: &'a StdinInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<JobFailure>,
    class: ExecClass,
    scope: ExecScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    nice: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    containment: Option<&'a JobContainment>,
    #[serde(skip_serializing_if = "<[OutlivedProcess]>::is_empty")]
    outlived: &'a [OutlivedProcess],
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    outlived_incomplete: bool,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RequiredJobCwd {
    Nested(WorkspacePath),
    Root(()),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JobInfoWire {
    repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
    state: JobState,
    pid: Option<u32>,
    grant_revision: u64,
    #[serde(default)]
    argv: Option<Vec<CommandArg>>,
    #[serde(default)]
    script: Option<ScriptCommand>,
    cwd: RequiredJobCwd,
    started: UtcTimestamp,
    duration_ms: Option<u64>,
    #[serde(default)]
    resources: Option<crate::api::resources::JobResourceSample>,
    exit: Option<ExitStatus>,
    stdout: StreamInfo,
    stderr: StreamInfo,
    trace: TraceContext,
    output_limit: Option<OutputLimitInfo>,
    stdin: StdinInfo,
    #[serde(default)]
    failure: Option<JobFailure>,
    class: ExecClass,
    scope: ExecScope,
    #[serde(default)]
    nice: Option<i32>,
    #[serde(default)]
    containment: Option<JobContainment>,
    #[serde(default)]
    outlived: Vec<OutlivedProcess>,
    #[serde(default)]
    outlived_incomplete: bool,
}

/// The wire's two mutually exclusive command spellings, `argv` and `script`, as one command.
impl Serialize for JobInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(serde::ser::Error::custom)?;
        JobInfoRef {
            repo_id: &self.repo_id,
            workspace_incarnation: &self.workspace_incarnation,
            job_id: self.job_id,
            state: self.state,
            pid: self.pid,
            grant_revision: self.grant_revision,
            argv: self.command.argv(),
            script: self.command.script(),
            cwd: &self.cwd,
            started: &self.started,
            duration_ms: self.duration_ms,
            resources: self.resources.as_ref(),
            exit: self.exit.as_ref(),
            stdout: &self.stdout,
            stderr: &self.stderr,
            trace: &self.trace,
            output_limit: self.output_limit.as_ref(),
            stdin: &self.stdin,
            failure: self.failure,
            class: self.class,
            scope: self.scope,
            nice: self.nice,
            containment: self.containment.as_ref(),
            outlived: &self.outlived,
            outlived_incomplete: self.outlived_incomplete,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JobInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = JobInfoWire::deserialize(deserializer)?;
        let command =
            ExecCommand::from_fields(wire.argv, wire.script).map_err(serde::de::Error::custom)?;
        let value = Self {
            repo_id: wire.repo_id,
            workspace_incarnation: wire.workspace_incarnation,
            job_id: wire.job_id,
            state: wire.state,
            pid: wire.pid,
            grant_revision: wire.grant_revision,
            command,
            cwd: match wire.cwd {
                RequiredJobCwd::Nested(path) => Some(path),
                RequiredJobCwd::Root(()) => None,
            },
            started: wire.started,
            duration_ms: wire.duration_ms,
            resources: wire.resources,
            exit: wire.exit,
            stdout: wire.stdout,
            stderr: wire.stderr,
            trace: wire.trace,
            output_limit: wire.output_limit,
            stdin: wire.stdin,
            failure: wire.failure,
            class: wire.class,
            scope: wire.scope,
            nice: wire.nice,
            containment: wire.containment,
            outlived: wire.outlived,
            outlived_incomplete: wire.outlived_incomplete,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

/// A job's terminal record as the workspace's durable records hold it: what any supervisor of the
/// workspace incarnation answers for a job, including one an earlier supervisor ran and sealed —
/// which [`JobInfo`], whose start, cwd, trace and stdin only the running supervisor knew, cannot
/// describe once that supervisor is gone.
#[cfg_attr(any(), cowshed_api(wire = "SealedJobRef"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedJob {
    pub job_id: JobId,
    pub state: JobState,
    pub exit: Option<ExitStatus>,
    pub failure: Option<JobFailure>,
    pub duration_ms: Option<u64>,
    /// The job's terminal resource sample, exactly as its last [`JobInfo::resources`]; absent
    /// for a job that never owned a process.
    pub resources: Option<crate::api::resources::JobResourceSample>,
    pub output_limit: Option<OutputLimitInfo>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    /// The class the job was admitted under, as JobInfo.class.
    pub class: ExecClass,
    /// Where the job's command ran, as JobInfo.scope.
    pub scope: ExecScope,
    /// The nice the job's command was observed at, as JobInfo.nice.
    pub nice: Option<i32>,
    /// What held the job's process membership while retirement proved it empty, as
    /// JobInfo.containment: absent for a job that never owned a process, for one whose supervisor
    /// owns no native membership, and for a record sealed without the proof.
    pub containment: Option<JobContainment>,
    /// The live members retirement signalled, as JobInfo.outlived.
    pub outlived: Vec<OutlivedProcess>,
    /// Retirement may have ended members absent from `outlived`, as JobInfo.outlived_incomplete.
    pub outlived_incomplete: bool,
}

impl SealedJob {
    pub fn validate(&self) -> Result<(), DtoError> {
        if (self.containment.is_some() || !self.outlived.is_empty() || self.outlived_incomplete)
            && !self.state.is_terminal()
        {
            return Err(DtoError::InvalidJobProjection(
                "a sealed retirement receipt belongs only to a terminal job",
            ));
        }
        if self.containment.is_none() && (!self.outlived.is_empty() || self.outlived_incomplete) {
            return Err(DtoError::InvalidJobProjection(
                "outlived processes and incomplete metadata need the containment that retired them",
            ));
        }
        if !self.outlived.iter().all(OutlivedProcess::consistent) {
            return Err(DtoError::InvalidJobProjection(
                "an outlived process names its image or none: an empty one stands for neither",
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SealedJobRef<'a> {
    job_id: JobId,
    state: JobState,
    exit: &'a Option<ExitStatus>,
    failure: Option<JobFailure>,
    duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<&'a crate::api::resources::JobResourceSample>,
    output_limit: &'a Option<OutputLimitInfo>,
    stdout: &'a StreamInfo,
    stderr: &'a StreamInfo,
    class: ExecClass,
    scope: ExecScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    nice: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    containment: Option<&'a JobContainment>,
    #[serde(skip_serializing_if = "<[OutlivedProcess]>::is_empty")]
    outlived: &'a [OutlivedProcess],
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    outlived_incomplete: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SealedJobWire {
    job_id: JobId,
    state: JobState,
    exit: Option<ExitStatus>,
    failure: Option<JobFailure>,
    duration_ms: Option<u64>,
    #[serde(default)]
    resources: Option<crate::api::resources::JobResourceSample>,
    output_limit: Option<OutputLimitInfo>,
    stdout: StreamInfo,
    stderr: StreamInfo,
    class: ExecClass,
    scope: ExecScope,
    #[serde(default)]
    nice: Option<i32>,
    #[serde(default)]
    containment: Option<JobContainment>,
    #[serde(default)]
    outlived: Vec<OutlivedProcess>,
    #[serde(default)]
    outlived_incomplete: bool,
}

impl Serialize for SealedJob {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(serde::ser::Error::custom)?;
        SealedJobRef {
            job_id: self.job_id,
            state: self.state,
            exit: &self.exit,
            failure: self.failure,
            duration_ms: self.duration_ms,
            resources: self.resources.as_ref(),
            output_limit: &self.output_limit,
            stdout: &self.stdout,
            stderr: &self.stderr,
            class: self.class,
            scope: self.scope,
            nice: self.nice,
            containment: self.containment.as_ref(),
            outlived: &self.outlived,
            outlived_incomplete: self.outlived_incomplete,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SealedJob {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SealedJobWire::deserialize(deserializer)?;
        let value = Self {
            job_id: wire.job_id,
            state: wire.state,
            exit: wire.exit,
            failure: wire.failure,
            duration_ms: wire.duration_ms,
            resources: wire.resources,
            output_limit: wire.output_limit,
            stdout: wire.stdout,
            stderr: wire.stderr,
            class: wire.class,
            scope: wire.scope,
            nice: wire.nice,
            containment: wire.containment,
            outlived: wire.outlived,
            outlived_incomplete: wire.outlived_incomplete,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

#[cfg_attr(any(), cowshed_api(wire = "ExecRecordRef"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecRecord {
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    pub argv: Vec<CommandArg>,
    pub cwd: WorkspacePath,
    pub env_hash: u64,
    pub grant_revision: u64,
    pub trace: TraceContext,
    pub started: UtcTimestamp,
    pub duration_ms: u64,
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub stdin: StdinInfo,
    pub output_limit: Option<OutputLimitInfo>,
}

impl ExecRecord {
    pub fn validate(&self) -> Result<(), DtoError> {
        validate_command_argv(&self.argv)?;
        if !self.state.is_terminal() {
            return Err(DtoError::InvalidJobProjection(
                "ExecRecord must contain a terminal job state",
            ));
        }
        if matches!(self.state, JobState::OutputLimit) != self.output_limit.is_some() {
            return Err(DtoError::InvalidJobProjection(
                "outputLimit must be present exactly for the outputLimit state",
            ));
        }
        match (&self.state, &self.exit) {
            (JobState::Exited, Some(ExitStatus::Exited { .. }))
            | (JobState::Signaled, Some(ExitStatus::Signaled { .. }))
            | (JobState::Killed, Some(_))
            | (JobState::OutputLimit | JobState::Failed, _) => {}
            _ => {
                return Err(DtoError::InvalidJobProjection(
                    "exit kind does not agree with exec record state",
                ));
            }
        }
        self.stdout.validate_for(self.job_id, "out")?;
        self.stderr.validate_for(self.job_id, "err")
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecRecordRef<'a> {
    repo_id: &'a RepoId,
    workspace_incarnation: &'a WorkspaceIncarnation,
    job_id: JobId,
    state: JobState,
    argv: &'a [CommandArg],
    cwd: &'a WorkspacePath,
    env_hash: u64,
    grant_revision: u64,
    trace: &'a TraceContext,
    started: &'a UtcTimestamp,
    duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit: Option<&'a ExitStatus>,
    stdout: &'a StreamInfo,
    stderr: &'a StreamInfo,
    stdin: &'a StdinInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_limit: Option<&'a OutputLimitInfo>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecRecordWire {
    repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
    state: JobState,
    argv: Vec<CommandArg>,
    cwd: WorkspacePath,
    env_hash: u64,
    grant_revision: u64,
    trace: TraceContext,
    started: UtcTimestamp,
    duration_ms: u64,
    exit: Option<ExitStatus>,
    stdout: StreamInfo,
    stderr: StreamInfo,
    stdin: StdinInfo,
    output_limit: Option<OutputLimitInfo>,
}

impl Serialize for ExecRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(serde::ser::Error::custom)?;
        ExecRecordRef {
            repo_id: &self.repo_id,
            workspace_incarnation: &self.workspace_incarnation,
            job_id: self.job_id,
            state: self.state,
            argv: &self.argv,
            cwd: &self.cwd,
            env_hash: self.env_hash,
            grant_revision: self.grant_revision,
            trace: &self.trace,
            started: &self.started,
            duration_ms: self.duration_ms,
            exit: self.exit.as_ref(),
            stdout: &self.stdout,
            stderr: &self.stderr,
            stdin: &self.stdin,
            output_limit: self.output_limit.as_ref(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExecRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExecRecordWire::deserialize(deserializer)?;
        let value = Self {
            repo_id: wire.repo_id,
            workspace_incarnation: wire.workspace_incarnation,
            job_id: wire.job_id,
            state: wire.state,
            argv: wire.argv,
            cwd: wire.cwd,
            env_hash: wire.env_hash,
            grant_revision: wire.grant_revision,
            trace: wire.trace,
            started: wire.started,
            duration_ms: wire.duration_ms,
            exit: wire.exit,
            stdout: wire.stdout,
            stderr: wire.stderr,
            stdin: wire.stdin,
            output_limit: wire.output_limit,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

/// Where the child's stdin comes from.
///
/// Frontends cannot carry this enum on their wire — `Stream` holds a live reader — so each keeps
/// its own spelling and maps into it here:
///
/// - CLI flags (`cowshed-cli/src/runtime.rs`, `exec_command`): no flag → `Empty`,
///   `--stdin` → `Stream`, `--stdin-file` → `WorkspaceFile`, `--stdin-base64` → `Inline`;
///   `Open` has no flag.
/// - napi wire (`cowshed-napi/src/lib.rs`, `NapiExecRequest`): `stdin` string → `Inline`,
///   `stdinWorkspacePath` → `WorkspaceFile`, neither → `Empty`; `Stream` and `Open` have no wire
///   spelling.
///
/// Each frontend pins its mapping with an exhaustive-match seam (`cli_stdin_spelling`,
/// `wire_stdin_spelling`), so adding a variant here breaks those matches at compile time instead
/// of silently becoming a mode neither frontend can produce.
pub enum StdinSource {
    Empty,
    Inline(Bytes),
    Stream(Pin<Box<dyn AsyncRead + Send>>),
    WorkspaceFile(WorkspacePath),
    /// Stdin stays open after admission with no source behind it: the job's attachment writes it
    /// through `JobStdin::write` and ends it, once, through `JobStdin::close`. Its `StdinInfo`
    /// kind is `stream`.
    Open,
}

impl fmt::Debug for StdinSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("Empty"),
            Self::Inline(bytes) => formatter.debug_tuple("Inline").field(&bytes.len()).finish(),
            Self::Stream(_) => formatter.write_str("Stream(<async reader>)"),
            Self::WorkspaceFile(path) => {
                formatter.debug_tuple("WorkspaceFile").field(path).finish()
            }
            Self::Open => formatter.write_str("Open"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RunSandboxMode {
    #[default]
    ReadWrite,
    ReadOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PublicationPolicy {
    CreateNew,
    Replace,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputPublication {
    pub path: WorkspacePath,
    pub policy: PublicationPolicy,
}
/// What a job runs.
#[cfg_attr(any(), serde(rename_all = "camelCase"))]
#[cfg_attr(any(), cowshed_api(name = "ExecCommand"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecCommand {
    /// A byte-exact argv, executed directly.
    Argv(Vec<CommandArg>),
    /// Bash-compatible shell text with values substituted as words (11_shell.md).
    Script(ScriptCommand),
}

impl ExecCommand {
    /// The command of a wire object that flattens it into optional `argv` and `script` fields.
    pub fn from_fields(
        argv: Option<Vec<CommandArg>>,
        script: Option<ScriptCommand>,
    ) -> Result<Self, DtoError> {
        match (argv, script) {
            (Some(argv), None) => Ok(Self::Argv(argv)),
            (None, Some(script)) => Ok(Self::Script(script)),
            (Some(_), Some(_)) | (None, None) => Err(DtoError::CommandShape),
        }
    }

    pub fn validate(&self) -> Result<(), DtoError> {
        match self {
            Self::Argv(argv) => validate_command_argv(argv),
            Self::Script(script) => script.validate(),
        }
    }

    pub fn argv(&self) -> Option<&[CommandArg]> {
        match self {
            Self::Argv(argv) => Some(argv),
            Self::Script(_) => None,
        }
    }

    pub fn script(&self) -> Option<&ScriptCommand> {
        match self {
            Self::Argv(_) => None,
            Self::Script(script) => Some(script),
        }
    }
}

/// A script as a template: literal shell text `parts` interleaved with `values`, exactly one
/// more part than values. Values never become shell text; each is substituted where it stands
/// as a whole word or as part of one (`crate::script`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScriptCommand {
    parts: Vec<String>,
    values: Vec<ScriptValue>,
}

/// One value substituted into a script.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum ScriptValue {
    /// Exactly one word, or one piece of a word.
    Word(String),
    /// One word per element where the value is a whole bare word; joined by single spaces into
    /// one word anywhere else.
    Words(Vec<String>),
}

impl ScriptCommand {
    pub fn new(parts: Vec<String>, values: Vec<ScriptValue>) -> Result<Self, DtoError> {
        let script = Self { parts, values };
        script.validate()?;
        Ok(script)
    }

    pub fn parts(&self) -> &[String] {
        &self.parts
    }

    pub fn values(&self) -> &[ScriptValue] {
        &self.values
    }

    fn validate(&self) -> Result<(), DtoError> {
        if self.parts.len() != self.values.len() + 1 {
            return Err(DtoError::ScriptShape {
                parts: self.parts.len(),
                values: self.values.len(),
            });
        }
        let strings = self
            .parts
            .iter()
            .chain(self.values.iter().flat_map(|value| match value {
                ScriptValue::Word(word) => std::slice::from_ref(word),
                ScriptValue::Words(words) => words.as_slice(),
            }));
        let mut total = 0_usize;
        for string in strings {
            if string.contains('\0') {
                return Err(DtoError::ScriptContainsNul);
            }
            total = total
                .checked_add(string.len())
                .filter(|total| *total <= MAX_ARGV_BYTES)
                .ok_or(DtoError::ScriptTooLarge)?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScriptCommandWire {
    parts: Vec<String>,
    values: Vec<ScriptValue>,
}

impl<'de> Deserialize<'de> for ScriptCommand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ScriptCommandWire::deserialize(deserializer)?;
        Self::new(wire.parts, wire.values).map_err(serde::de::Error::custom)
    }
}

/// Why a job failed when no command's own status explains it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JobFailure {
    /// The script did not parse; nothing ran.
    ScriptSyntax,
    /// The supervisor running the job ended without seeing it end. Recovery sealed its terminal
    /// record; any process group it could not safely identify remains diagnosed in the ledger.
    SupervisorLost,
    /// The job's admission failed before its command ran: its class is not the caller's to ask,
    /// its jobserver fifo could not be made, or the host's memory pressure, which a background
    /// start waits on, could not be read. The cause is on stderr.
    CpuAdmission,
    /// The nice of the job's owned command could not be set, so the command never ran.
    Priority,
    /// A background or resident job was ended because the host reached critical memory pressure
    /// (macOS); its stderr names the pressure level and the job's resident memory (05_gateway.md
    /// "Memory pressure").
    Memory,
    /// The job's command was never launched for any other reason: a program, `#!` interpreter
    /// or `env` name proven absent before the spawn, a held child the observer refused before
    /// its exec, or a host that could not launch it (it could not be started or broke protocol).
    /// Its exit is the refusal's wrapper code; whatever a host process owned meanwhile, the
    /// command never ran. The cause is on stderr. A cold activation that itself fails is not
    /// this: the job ends with the activation's own status (11_shell.md).
    Unlaunched,
}

#[derive(Debug)]
pub struct ExecRequest {
    pub command: ExecCommand,
    pub cwd: Option<WorkspacePath>,
    pub mode: RunSandboxMode,
    pub env: HashMap<String, String>,
    pub trace: Option<TraceContext>,
    pub stdin: StdinSource,
    pub stdout_copy: Option<OutputPublication>,
    pub stderr_copy: Option<OutputPublication>,
    /// Names this exec for idempotent admission: a repeat with the same key and request answers
    /// the job the first admitted, and never spawns again.
    pub admission_key: Option<AdmissionKey>,
    /// The class the command is admitted under (05_gateway.md "Classes"): background by default,
    /// interactive when its requester awaits its result, a gate when a trusted coordinator route
    /// asks for one, or a resident.
    pub class: ExecClass,
    /// The most compiler and test threads the command's jobserver holds; `None` holds the host's
    /// cores, and a larger want is held to them (`JobScheduling::requested`, 05_gateway.md
    /// "Parallelism inside a job").
    pub want: Option<NonZeroUsize>,
    /// Where the command runs: the workspace sandbox, or a coordinator's host boundary outside it.
    pub scope: ExecScope,
}

/// Where an exec's command runs (07_api.md "Exec and grants").
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExecScope {
    /// Inside the workspace's sandbox, as every exec runs.
    #[default]
    Workspace,
    /// A trusted coordinator's argv one-shot at the host boundary, outside the workspace sandbox,
    /// at whatever class it names: the land's real-APFS gate, or host unit work at background.
    /// Only the coordinator route admits it; the class alone decides its CPU admission.
    HostGate,
}

/// Public environment names permitted in JobAdmission.env records for every exec scope
/// (07_api.md "Keyed admission and restart attachment"). This is a record projection, not an
/// execution allowlist: custom operator and project variables may travel in requests and reach
/// jobs under the existing runtime reserved/credential rules. The full authored non-withheld
/// environment is bound by the admission's SHA-256 commitment, not by this public projection.
pub const HOST_GATE_ENV_NAMES: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "SHELL",
    "TMPDIR",
    "LANG",
    "TERM",
    "TZ",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "BUN_INSTALL",
    "DEVELOPER_DIR",
];

/// The locale category prefix (`LC_ALL`, `LC_CTYPE`, …) also permitted in public env records.
pub const HOST_GATE_LOCALE_PREFIX: &str = "LC_";

/// Whether JobAdmission.env may record this name in any exec scope, never whether a job may
/// execute with it. Only the name decides, never the value: a listed name or a locale category
/// (`LC_` and one or more of A-Z, 0-9 and `_`).
pub fn host_gate_env_name_allowed(name: &str) -> bool {
    HOST_GATE_ENV_NAMES.contains(&name)
        || name
            .strip_prefix(HOST_GATE_LOCALE_PREFIX)
            .is_some_and(|category| {
                !category.is_empty()
                    && category.bytes().all(|byte| {
                        byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                    })
            })
}

/// A host exec runs outside every session, whose cwd and environment a worker controls
/// (07_api.md "Task server scheduling and trusted host gates"), whatever its class. Environment
/// names do not restrict execution; the public-name policy applies only to records. Other scopes
/// may use sessions.
pub fn validate_host_gate_request(scope: ExecScope, in_session: bool) -> Result<(), CowshedError> {
    if scope == ExecScope::HostGate && in_session {
        return Err(CowshedError::usage(
            "a host exec runs outside every session: it takes no session's cwd or environment",
            "run the host exec without a session",
        ));
    }
    Ok(())
}

fn valid_ref_name(value: &str) -> bool {
    !value.is_empty()
        && value != "@"
        && !value.starts_with('-')
        && !value.starts_with('.')
        && !value.ends_with('/')
        && !value.ends_with('.')
        && !value.ends_with(".lock")
        && !value.contains("..")
        && !value.contains("@{")
        && !value.contains("//")
        && value.split('/').all(|component| {
            !component.is_empty() && !component.starts_with('.') && !component.ends_with(".lock")
        })
        && !value.bytes().any(|byte| {
            byte <= b' '
                || byte == 0x7f
                || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
        })
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BranchName(String);

impl BranchName {
    pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        if !value.starts_with("refs/") && valid_ref_name(&value) {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidBranchName(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GitRef(String);

impl GitRef {
    pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        if value.starts_with("refs/") && valid_ref_name(&value) {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidGitRef(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! string_domain_serde {
    ($name:ident) => {
        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

string_domain_serde!(BranchName);
string_domain_serde!(GitRef);

#[cfg_attr(any(), serde(rename_all = "camelCase"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RevisionTarget {
    Branch(BranchName),
    Ref(GitRef),
    Oid(GitOid),
}
impl RevisionTarget {
    /// Parses the exact CLI revision grammar without accepting git rev expressions.
    pub fn parse_cli(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        if matches!(value.len(), 40 | 64) && is_lowercase_hex(&value) {
            GitOid::new(value).map(Self::Oid)
        } else if value.starts_with("refs/") {
            GitRef::new(value).map(Self::Ref)
        } else {
            BranchName::new(value).map(Self::Branch)
        }
    }
}

impl Serialize for RevisionTarget {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::Branch(branch) => map.serialize_entry("branch", branch)?,
            Self::Ref(git_ref) => map.serialize_entry("ref", git_ref)?,
            Self::Oid(oid) => map.serialize_entry("oid", oid)?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for RevisionTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("revision target must be an object"))?;
        if object.len() != 1 {
            return Err(serde::de::Error::custom(
                "revision target requires exactly one discriminator",
            ));
        }
        if let Some(value) = object.get("branch") {
            return serde_json::from_value(value.clone())
                .map(Self::Branch)
                .map_err(serde::de::Error::custom);
        }
        if let Some(value) = object.get("ref") {
            return serde_json::from_value(value.clone())
                .map(Self::Ref)
                .map_err(serde::de::Error::custom);
        }
        if let Some(value) = object.get("oid") {
            return serde_json::from_value(value.clone())
                .map(Self::Oid)
                .map_err(serde::de::Error::custom);
        }
        Err(serde::de::Error::custom(
            "revision target requires branch, ref, or oid",
        ))
    }
}

#[cfg_attr(any(), serde(rename_all = "camelCase"))]
#[cfg_attr(any(), cowshed_api(unit_value = "true"))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpectedRefHead {
    Missing,
    Oid(GitOid),
}

impl Serialize for ExpectedRefHead {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::Missing => map.serialize_entry("missing", &true)?,
            Self::Oid(oid) => map.serialize_entry("oid", oid)?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for ExpectedRefHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("expected an object"))?;
        if object.len() != 1 {
            return Err(serde::de::Error::custom(
                "expected exactly one ref-head discriminator",
            ));
        }
        if object.get("missing") == Some(&serde_json::Value::Bool(true)) {
            return Ok(Self::Missing);
        }
        if let Some(oid) = object.get("oid") {
            return GitOid::new(
                oid.as_str()
                    .ok_or_else(|| serde::de::Error::custom("oid must be a string"))?,
            )
            .map(Self::Oid)
            .map_err(serde::de::Error::custom);
        }
        Err(serde::de::Error::custom(
            "expected {missing:true} or {oid:string}",
        ))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct AdoptOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_id: Option<RepoId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<String>,
    pub quarantine: bool,
    /// Explicitly replace an adopted main through verified, retained-source re-adoption.
    /// False keeps ordinary adoption idempotent.
    pub re_adopt: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ForkOptions {
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    pub revision: Option<RevisionTarget>,
    pub clean: bool,
    pub browse: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<u32>,
    /// Also register this workspace as a remote in main's repository. Off by default: it is
    /// host-side state that accumulates one entry per workspace and outlives an interrupted retire.
    pub register: bool,
    /// Mint a registered linked worktree of main's repository instead of a standalone clone: one
    /// object store and one ref namespace, at the cost of requiring main mounted.
    pub git_worktree: bool,
    /// The caller's edit is pure Markdown, disjoint from code edits: the fork is refused, because
    /// such an edit belongs in place in the checkout and no workspace should be minted for it.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub markdown_only: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct AttachOptions {
    pub browse: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_path: Option<PathBuf>,
    /// One attach of an aggregate (`attach` with no name, `attach --all`): it makes no room under
    /// IOKit's IOMedia ceiling, since detaching some sessions to attach others frees nothing
    /// (02_workspaces.md, "Making room for an attach").
    pub aggregate: bool,
}

/// Which of a workspace's volumes `cowshed resize` grows: its image, or its build volume and the
/// workspace's seed with it (16_build_volumes.md, "Substrate").
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResizeVolume {
    Workspace,
    Build,
}

/// What `cowshed resize` changed, with both capacities as the image itself reports them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResizeResult {
    pub workspace: WorkspaceName,
    pub volume: ResizeVolume,
    pub previous_capacity: String,
    pub capacity: String,
}

/// What `cowshed defrag` changed: the image's extents before and after its data was rewritten
/// contiguously, and the data bytes the rewrite copied.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DefragmentResult {
    pub workspace: WorkspaceName,
    pub previous_extents: u64,
    pub extents: u64,
    pub bytes: u64,
}

/// What `cowshed cache clear` deleted from one checkout's Nx state on its build volume
/// (16_build_volumes.md, "Clearing a checkout's Nx state"): the entries directly in every
/// `.nx/cache` and every `.nx/workspace-data` directory the volume holds, deleted together
/// under the volume's exclusive claim with the checkout's Nx closed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NxStateClearResult {
    pub workspace: WorkspaceName,
    pub volume: String,
    pub cache_entries: u64,
    pub workspace_data_entries: u64,
}

/// What `cowshed cache clear` deleted from the project's accepted-task store the gateway serves
/// (03_caches.md, "Nx accepted-task artifact service"): the archives in its entries directory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NxStoreClearResult {
    pub repo_id: RepoId,
    pub entries: u64,
}

/// `cowshed cache clear`: the checkout's Nx state and the project's gateway store, each with
/// the entries it deleted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CacheClearReport {
    pub workspace: NxStateClearResult,
    pub store: NxStoreClearResult,
}

/// What `cowshed rm` is allowed to override.
///
/// The two overrides are deliberately separate, because they authorize losses of different kinds.
/// `force` covers *transient* state — a dirty tree, an in-progress merge, the workspace's own running jobs — none of
/// which is unrecoverable content. `abandon` is the only thing that authorizes destroying commits
/// the project's main branch does not contain, and it is spelled out in full for exactly that
/// reason: a script that reaches for `force` to get past a stuck workspace must not thereby
/// acquire the power to delete work that has no other home.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveOptions {
    pub force: bool,
    pub restore: bool,
    pub abandon: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct GcOptions {
    pub dry_run: bool,
    /// Also reclaim the project trash's abandon bundles — `sessions/.trash/<ws>-<tip>.bundle`,
    /// the only copy of their commits (01_storage.md). Never implied.
    pub abandoned: bool,
}

/// Authorization for removing a whole adopted project (`Coordinator::remove_project`). `force` and
/// `abandon` mean for every session workspace what they mean for one (`RemoveOptions`); `abandon`
/// also authorizes deleting the abandon bundles in the project's trash, which are then the only
/// copy of those commits, and bundling main's unpreserved commits into the restored checkout.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveProjectOptions {
    pub force: bool,
    pub abandon: bool,
}

/// The workspace a unit lands into, or rebases onto, with the incarnation it had when it was
/// resolved: a lane base, or main.
///
/// Built only from a [`crate::WorkspaceRef`] ([`crate::WorkspaceRef::target`]), never from a name,
/// so a caller cannot aim a land at a workspace it did not resolve. The incarnation is what makes
/// that stick: a lane base removed and recreated under the same name is a different workspace,
/// and land and rebase refuse it rather than deliver into it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceTarget {
    workspace: WorkspaceName,
    incarnation: WorkspaceIncarnation,
}

impl WorkspaceTarget {
    pub(crate) fn new(workspace: WorkspaceName, incarnation: WorkspaceIncarnation) -> Self {
        Self {
            workspace,
            incarnation,
        }
    }

    pub fn workspace(&self) -> &WorkspaceName {
        &self.workspace
    }

    pub fn incarnation(&self) -> &WorkspaceIncarnation {
        &self.incarnation
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RebaseOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onto: Option<RevisionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_workspace_incarnation: Option<WorkspaceIncarnation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_source_head: Option<GitOid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_onto_head: Option<GitOid>,
    /// Answer what the rebase would do and write nothing (`--plan`): [`RebaseReport::Planned`].
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub plan: bool,
    /// Commits to leave out of the replay (`--drop`). One the destination does not hold, by content
    /// or patch identity, is refused unless `drop_unheld` names the loss; a commit the last plan
    /// cowshed recorded carried and the branch has since lost is named here the same way.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub drop: Vec<GitOid>,
    /// `--drop-unheld`: drop the commits of `drop` the destination does not hold, losing them.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub drop_unheld: bool,
    /// Finish or undo the rebase this verb stopped at a conflict (`--continue`, `--abort`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<RebaseResume>,
}

/// The rebase a conflict stopped, carried on (02_workspaces.md, "`cowshed rebase`").
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RebaseResume {
    /// Stage the resolved paths and replay the rest of the plan.
    Continue,
    /// Put the branch back at the head the rebase started from.
    Abort,
}

/// One commit of the branch a rebase replays, and what the rebase does with it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReplayedCommit {
    pub oid: GitOid,
    pub subject: String,
    pub step: ReplayStep,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ReplayStep {
    /// The commit replays onto the destination.
    Replay,
    /// A plan's replay that conflicts when it is replayed after the commits before it: these
    /// paths, read from an in-memory merge.
    ConflictExpected { paths: Vec<String> },
    /// The destination holds the commit's change, so the replay leaves it out.
    Held { by: HeldBy },
    /// `--drop` with `--drop-unheld`: left out though nothing else holds its change.
    DroppedUnheld,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "proof",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HeldBy {
    /// `holder`, a commit of the destination, is its patch-equivalent.
    PatchIdentity { holder: GitOid },
    /// The destination's files already carry its change; `holder` changed them last.
    Content { holder: GitOid },
    /// Its replay changed nothing: the destination and the commits replayed before it carry it.
    Emptied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct LandOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check: Option<Vec<String>>,
    pub retire: bool,
    pub push_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_workspace_incarnation: Option<WorkspaceIncarnation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_source_head: Option<GitOid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_target_head: Option<ExpectedRefHead>,
    /// The lock file the target's other branch writers hold; land holds it from re-reading the
    /// target head through the fast-forward, and for nothing else (02_workspaces.md
    /// "`cowshed land`" step 3).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_lock: Option<RefLockPath>,
    /// The land-turn queue the target's other branch writers take while they move it, its ref
    /// turn; land takes it for the same span as `ref_lock`, and the two are exclusive
    /// (02_workspaces.md "`cowshed land`" step 3, 05_gateway.md "A ref turn").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_turn: Option<LandTurnName>,
    /// `Ungated` folds a unit into its lane base by Git alone: it rebases the unit onto the lane
    /// base's branch (nothing when already based), then fast-forwards and retires, with no check,
    /// no seed freeze, no adoption (02_workspaces.md "Lanes"). Refused into main, with `check`,
    /// and with `push_only`.
    #[serde(skip_serializing_if = "LandMode::is_gated")]
    pub mode: LandMode,
    /// Run only land's fences before its check and stop, mutating nothing: no land lease, no
    /// ref lock, no attach, no check (02_workspaces.md "`cowshed land`", `--preflight`). A
    /// refusal is the land's own; a pass answers [`LandReport::Preflighted`].
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub preflight: bool,
}

impl Default for LandOptions {
    fn default() -> Self {
        Self {
            target_branch: None,
            check: None,
            retire: true,
            push_only: false,
            expected_workspace_incarnation: None,
            expected_source_head: None,
            expected_target_head: None,
            ref_lock: None,
            ref_turn: None,
            mode: LandMode::Gated,
            preflight: false,
        }
    }
}

/// Whether a land runs the target's build-state steps (02_workspaces.md "Lanes";
/// 16_build_volumes.md "Ungated folds").
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LandMode {
    /// The full close-out: checks, seed freeze and adoption into the target.
    #[default]
    Gated,
    /// A unit's fold into its lane base: the rebase onto its branch, the fast-forward and the
    /// retire, Git alone. The lane
    /// base keeps its build volume and seed; the unit's volume retires with the unit. Only a
    /// land into main gates, so main refuses it.
    Ungated,
}

impl LandMode {
    pub fn is_gated(&self) -> bool {
        matches!(self, Self::Gated)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct PushOptions {
    /// The workspace's local branch to preserve; `cowshed/<ws>` when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_workspace_incarnation: Option<WorkspaceIncarnation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_source_head: Option<GitOid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_destination_head: Option<ExpectedRefHead>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct GrantDelta {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deny_write: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deny: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress: Vec<EgressRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repos: Vec<RepoRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sim: Vec<SimVerb>,
    /// macOS only: `grant` admits connects from every job of the workspace to any loopback TCP
    /// port (`GrantSet::loopback_any`), and `revoke` withdraws that. False changes nothing. Like
    /// a filesystem grant it advances the grant revision, and the next exec runs under the
    /// widened profile. Linux refuses it: each workspace's private loopback already reaches every
    /// port.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub loopback_any: bool,
    /// macOS only: the minimum number of service ports the workspace's port block must hold,
    /// excluding the gateway listener at its base. Growth is monotone: a count the block already
    /// holds is a no-op on its own, and `revoke` refuses the field. The block grows into a
    /// larger aligned block containing it when one is free; otherwise the workspace moves to a
    /// larger disjoint block and keeps the old one reserved (`GrantSet::retained_port_blocks`)
    /// until it retires, unless a later block contains it. Either
    /// way growth needs an idle workspace: it is refused at once, never queued, while any job is
    /// active, and the next exec starts under the new block's profile and environment. Linux has
    /// no port block — each workspace's private loopback already holds the whole port space — so
    /// it refuses the field rather than record a block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_ports: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

/// A change to the project's standing policy: read grants, egress grants and
/// workspace-relative denies. A write allow stays per-workspace (`GrantDelta`).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ProjectGrantDelta {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deny_write: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deny: Vec<PathBuf>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress: Vec<EgressRule>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

pub use crate::project_policy::ProjectGrants;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushReport {
    pub source_head: GitOid,
    pub destination_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_destination_head: Option<GitOid>,
}

/// What `cowshed doc-commit` committed to main (02_workspaces.md "`cowshed doc-commit`"): the
/// commit, and exactly the paths it holds, sorted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DocCommitReport {
    pub commit: GitOid,
    pub paths: Vec<String>,
}

/// Preservation-only delivery is distinct from a target that actually landed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum LandReport {
    Delivered(PushReport),
    Landed(LandedReport),
    Preflighted(LandPreflight),
}

/// What a land preflight read at the fences it passed: the three values the land after it pins
/// as `expectedWorkspaceIncarnation`, `expectedSourceHead` and `expectedTargetHead`, so the
/// land refuses if any of them moved in between.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandPreflight {
    pub workspace_incarnation: WorkspaceIncarnation,
    pub source_head: GitOid,
    pub target_branch: String,
    pub target_head: ExpectedRefHead,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandedReport {
    pub landed_head: GitOid,
    pub target_branch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_target_head: Option<GitOid>,
    pub target_was_checked_out: bool,
    pub retired: bool,
    /// What the land did with build volumes (16_build_volumes.md, Land steps 4–7).
    pub build_volume: LandBuildVolume,

    /// The sessions the land folded (02_workspaces.md, `land` step 5, "Folds"), each retired or
    /// kept; empty when it retired nothing of its own.
    pub folded: Vec<FoldedWorkspace>,
}

/// A session a land folded: its own change touched a path the landed range changed, and the
/// target now holds it by `rm`'s measurement (02_workspaces.md, `land` step 5, "Folds").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FoldedWorkspace {
    pub workspace: WorkspaceName,
    pub head: GitOid,
    pub outcome: FoldOutcome,
}

/// What the land did with a folded session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum FoldOutcome {
    Retired,
    Kept { reason: FoldKept },
}

/// Why a folded session was kept: it is the next `rm`'s, or the next land's that touches it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum FoldKept {
    /// Its tree carries `files` uncommitted changes: tracked ones, and untracked files no ignore
    /// rule covers.
    Dirty { files: u64 },
    /// Its retirement refused, word for word: a supervisor running a job or holding an open shell,
    /// a process holding its checkout, a land or `rm` holding it, or anything else `rm` refuses.
    Refused {
        code: crate::error::ErrorCode,
        message: String,
    },
}

/// A land's build-volume steps: the landing workspace quiesced, the target's seed frozen from
/// its volume, the target adopting it, and the adoption checked (2b).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandBuildVolume {
    /// What step 5.4 did with the target's seed.
    pub seed: LandSeed,
    pub adoption: Adoption,
}

/// The target's seed after a land (16_build_volumes.md, Land step 5.4).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum LandSeed {
    /// The land skipped its adoption before freezing a seed; the target's seed is as it was.
    Unchanged,
    /// The target's seed is frozen from the landing volume; `compaction` is what the land did
    /// about compacting it.
    Frozen { compaction: LandSeedCompaction },
    /// Required storage capture admission was refused, so the target retains its previous seed
    /// while adoption goes ahead. The next fork may capture the adopted volume once that admission
    /// succeeds; open handles on exempt contributed caches do not refuse capture.
    SealBusy { reason: String },
}

/// What a land that froze its target's seed did about compacting it (16_build_volumes.md, "Seed
/// compaction"). None of these fails the land.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum LandSeedCompaction {
    /// The target is not main; only main's seed is compacted.
    NotMain,
    /// A compaction by this cowshed build failed `secondsAgo` seconds ago with `error`, so none
    /// was started: no fork or land of this build starts one until a compaction succeeds.
    Held {
        error: String,
        #[serde(rename = "secondsAgo")]
        seconds_ago: u64,
    },
    /// No necessary main-seed capture or attach/mount operation exceeded ten seconds.
    NotAdvised,
    /// Process `pid` checks the current seed and compacts it in the background when due,
    /// its stderr appended to `log`. No foreground extent count is made.
    Started { pid: u32, log: PathBuf },
    /// Deciding or starting the compaction failed with `error`. The failure is recorded, so the
    /// next lands of this build hold back for it.
    Failed { error: String },
}
/// What one seed compaction did (16_build_volumes.md, "Seed compaction"): the result of the
/// internal `coordinator.compactSeed`, run by the process a fork or land starts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum SeedCompactionReport {
    /// Another process holds the project's compaction turn.
    Running,
    /// The seed holds `extents`, under the bound: left as it is.
    Below { extents: u64 },
    /// Main has no published seed, none any more once the copy was done, or its backend keeps no
    /// extent map.
    NoSeed,
    /// The copy, or a catch-up of it, stopped once `bytes` were written, because `why`; the seed
    /// is as it was.
    Stopped { bytes: u64, why: String },
    /// `replacement`, a contiguous copy of `seed`, is main's seed now: `bytes` were written into
    /// it, its catch-ups' included.
    Compacted {
        seed: String,
        replacement: String,
        /// The extents of the seed first copied: `caughtUp[0].from`, or `seed` when nothing was
        /// caught up.
        before: u64,
        /// The extents of `replacement`.
        after: u64,
        bytes: u64,
        #[serde(rename = "elapsedMs")]
        elapsed_ms: u64,
        /// Each catch-up of the copy, in order, to a seed that replaced the one it held: the
        /// first one's `from` is the seed copied, the last one's `to` is `seed`.
        #[serde(rename = "caughtUp")]
        caught_up: Vec<SeedCatchUp>,
        main: MainLineage,
    },
}

/// One catch-up of a seed compaction's copy (16_build_volumes.md, "Seed compaction"): the copy
/// held seed `from`'s bytes and holds `to`'s, which replaced it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SeedCatchUp {
    pub from: String,
    pub to: String,
    /// Data bytes written where `to` does not share `from`'s blocks.
    pub written: u64,
    /// Bytes the copy held where `to` has a hole, punched.
    pub punched: u64,
    pub elapsed_ms: u64,
}

/// What a seed compaction did with main's live build volume once the compacted seed was
/// published (16_build_volumes.md, "Seed compaction").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum MainLineage {
    /// Main's build volume `previous` was replaced by `volume`, a clone of the compacted seed:
    /// main writes on in contiguous space, and the seeds captured from it stay compact.
    Moved { previous: String, volume: String },
    /// Main's build volume was left as it is, because `why`: it holds a write the seed lacks, a
    /// job, an Nx holder or an open file holds it, main is detached, or its seed changed first.
    Kept { why: String },
}

impl std::fmt::Display for SeedCompactionReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => {
                formatter.write_str("another process is compacting this project's seed")
            }
            Self::Below { extents } => {
                write!(
                    formatter,
                    "the seed holds {extents} extents, under the bound: left as it is"
                )
            }
            Self::NoSeed => formatter.write_str("there is no seed with an extent map to compact"),
            Self::Stopped { bytes, why } => write!(
                formatter,
                "stopped after writing {bytes} bytes: {why}; the seed is as it was"
            ),
            Self::Compacted {
                seed,
                replacement,
                before,
                after,
                bytes,
                elapsed_ms,
                caught_up,
                main,
            } => {
                let copied = caught_up.first().map_or(seed, |first| &first.from);
                write!(
                    formatter,
                    "seed {copied} ({before} extents) is copied into {replacement} ({after} extents), which replaces seed {seed}: {bytes} bytes written in {elapsed_ms} ms"
                )?;
                for SeedCatchUp {
                    from,
                    to,
                    written,
                    punched,
                    elapsed_ms,
                } in caught_up
                {
                    write!(
                        formatter,
                        "; caught up from seed {from} to {to}: {written} bytes written and {punched} punched in {elapsed_ms} ms"
                    )?;
                }
                match main {
                    MainLineage::Moved { previous, volume } => write!(
                        formatter,
                        "; main's build volume {previous} is replaced by {volume}, a clone of the compacted seed"
                    ),
                    MainLineage::Kept { why } => {
                        write!(formatter, "; main's build volume is kept: {why}")
                    }
                }
            }
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Adoption {
    /// The target's build link names the landing volume now (step 6), exactly as it was quiesced;
    /// `check` is step 7.
    Adopted {
        #[serde(rename = "elapsedMs")]
        elapsed_ms: u64,
        check: AdoptionCheck,
    },
    /// The target kept its own volume and builds the landed delta itself. Never wrong, only
    /// slower.
    Skipped { reason: AdoptionSkip },
}

impl Adoption {
    pub fn is_adopted(&self) -> bool {
        matches!(self, Self::Adopted { .. })
    }
}

/// What `cowshed rebase` did. A rebase is Git alone (02_workspaces.md, "`cowshed rebase`").
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RebaseReport {
    /// The branch now sits on `onto` at `oid`; `commits` is the range it replayed, oldest first,
    /// each with what became of it.
    Rebased {
        oid: GitOid,
        onto: GitOid,
        commits: Vec<ReplayedCommit>,
    },
    /// `--plan`: what a rebase of `oid` onto `onto` would do. Nothing was written.
    Planned {
        oid: GitOid,
        onto: GitOid,
        commits: Vec<ReplayedCommit>,
    },
    /// `--abort`: the branch is back at `oid`, the head the stopped rebase started from.
    Aborted { oid: GitOid },
}

impl RebaseReport {
    /// The branch head the report leaves: the rebased head, the planned one unmoved, or the
    /// head an abort restored.
    pub fn oid(&self) -> &GitOid {
        match self {
            Self::Rebased { oid, .. } | Self::Planned { oid, .. } | Self::Aborted { oid } => oid,
        }
    }
}

impl ReplayedCommit {
    /// What a replay did, as every rebase says it: `replayed N, dropped K held (<oid>…), dropped U
    /// unheld (<oid>…)` (02_workspaces.md, "`cowshed rebase`"), commits abbreviated.
    pub fn summary(commits: &[Self]) -> String {
        let named = |pick: fn(&ReplayStep) -> bool| {
            let oids: Vec<&str> = commits
                .iter()
                .filter(|commit| pick(&commit.step))
                .map(|commit| abbreviated(&commit.oid))
                .collect();
            (oids.len(), oids.join(", "))
        };
        let replayed = commits
            .iter()
            .filter(|commit| {
                matches!(
                    commit.step,
                    ReplayStep::Replay | ReplayStep::ConflictExpected { .. }
                )
            })
            .count();
        let dropped = |what: &str, (count, oids): (usize, String)| {
            if count == 0 {
                format!("dropped 0 {what}")
            } else {
                format!("dropped {count} {what} ({oids})")
            }
        };
        format!(
            "replayed {replayed}, {}, {}",
            dropped(
                "held",
                named(|step| matches!(step, ReplayStep::Held { .. }))
            ),
            dropped(
                "unheld",
                named(|step| matches!(step, ReplayStep::DroppedUnheld))
            ),
        )
    }

    /// A plan as `--plan` prints it: one line per commit, oldest first, with its step and why, then
    /// the counts.
    pub fn plan_lines(commits: &[Self]) -> Vec<String> {
        let mut lines: Vec<String> = commits
            .iter()
            .map(|commit| {
                let (step, why) = match &commit.step {
                    ReplayStep::Replay => ("replay", String::new()),
                    ReplayStep::ConflictExpected { paths } => (
                        "conflict-expected",
                        format!(" (conflicts in {})", paths.join(", ")),
                    ),
                    ReplayStep::Held { by } => (
                        "held-drop",
                        match by {
                            HeldBy::PatchIdentity { holder } => {
                                format!(" (held by {}, its patch twin)", abbreviated(holder))
                            }
                            HeldBy::Content { holder } => {
                                format!(
                                    " (held by content; {} changed it last)",
                                    abbreviated(holder)
                                )
                            }
                            HeldBy::Emptied => String::from(" (its replay changed nothing)"),
                        },
                    ),
                    ReplayStep::DroppedUnheld => ("drop-unheld", String::from(" (lost)")),
                };
                format!(
                    "{step:<17} {} {}{why}",
                    abbreviated(&commit.oid),
                    commit.subject
                )
            })
            .collect();
        let conflicts = commits
            .iter()
            .filter(|commit| matches!(commit.step, ReplayStep::ConflictExpected { .. }))
            .count();
        lines.push(format!(
            "{}; {conflicts} conflict(s) expected",
            Self::summary(commits)
        ));
        lines
    }
}

/// The first ten hex digits of a commit, as cowshed names commits to people.
fn abbreviated(oid: &GitOid) -> &str {
    oid.as_str().get(..10).unwrap_or(oid.as_str())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum AdoptionSkip {
    /// The landing workspace links no build volume.
    NoLandingVolume,
    /// The target links no build volume.
    NoTargetVolume,
    /// A process of the landing workspace still holds its Nx task database (step 4).
    LandingHeld {
        database: PathBuf,
        holders: Vec<DatabaseHolder>,
    },
    /// An Nx process held or waited on the landing task database's open lock when capture began.
    LandingOpening {
        database: PathBuf,
        holders: Vec<DatabaseHolder>,
    },
    /// The landing workspace's Nx daemon outlived its stop.
    LandingDaemonStayed { daemon: DatabaseHolder },
    /// The landing daemon record or its job ledger cannot establish a safe stop plan.
    LandingDaemonUnverifiable { record: PathBuf, reason: String },
    /// A host Cargo build holds a landing profile's lock immediately before the seed freezes.
    LandingBuilding {
        lock: PathBuf,
        holders: Vec<DatabaseHolder>,
    },
    /// A process holds the target's Nx task database (steps 5 and 6).
    TargetHeld {
        database: PathBuf,
        holders: Vec<DatabaseHolder>,
    },
    /// An Nx process was opening the target's Nx task database as the adoption began (step 6):
    /// it held, or waited on, the lock stock Nx opens the database under.
    TargetOpening {
        database: PathBuf,
        holders: Vec<DatabaseHolder>,
    },
    /// The target's Nx daemon outlived its stop.
    TargetDaemonStayed { daemon: DatabaseHolder },
    /// The landing volume could not be grown to the target's capacity without forcing a
    /// filesystem holder off it (step 4); adopting it would shrink the target.
    LandingVolumeBusy { reason: String },
    /// The land was ungated: the target kept its volume and seed, and the landing workspace's
    /// volume retires with it.
    Ungated,
}

impl AdoptionSkip {
    /// The stable name telemetry records for the reason.
    pub fn name(&self) -> &'static str {
        match self {
            Self::NoLandingVolume => "noLandingVolume",
            Self::NoTargetVolume => "noTargetVolume",
            Self::LandingHeld { .. } => "landingHeld",
            Self::LandingOpening { .. } => "landingOpening",
            Self::LandingDaemonStayed { .. } => "landingDaemonStayed",
            Self::LandingDaemonUnverifiable { .. } => "landingDaemonUnverifiable",
            Self::LandingBuilding { .. } => "landingBuilding",
            Self::TargetHeld { .. } => "targetHeld",
            Self::TargetOpening { .. } => "targetOpening",
            Self::TargetDaemonStayed { .. } => "targetDaemonStayed",
            Self::LandingVolumeBusy { .. } => "landingVolumeBusy",
            Self::Ungated => "ungated",
        }
    }
}

impl fmt::Display for AdoptionSkip {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let holders = |formatter: &mut fmt::Formatter<'_>, holders: &[DatabaseHolder]| {
            for (index, holder) in holders.iter().enumerate() {
                let separator = if index == 0 { "" } else { ", " };
                write!(
                    formatter,
                    "{separator}pid {} ({})",
                    holder.pid, holder.command
                )?;
            }
            Ok(())
        };
        match self {
            Self::NoLandingVolume => {
                formatter.write_str("the landing workspace has no build volume")
            }
            Self::NoTargetVolume => formatter.write_str("the target has no build volume"),
            Self::LandingHeld {
                database,
                holders: held,
            } => {
                write!(
                    formatter,
                    "the landing workspace's {} is open in ",
                    database.display()
                )?;
                holders(formatter, held)
            }
            Self::LandingBuilding {
                lock,
                holders: held,
            } => {
                write!(
                    formatter,
                    "a Cargo build holds the landing profile {}",
                    lock.display()
                )?;
                if !held.is_empty() {
                    formatter.write_str(": ")?;
                }
                holders(formatter, held)
            }
            Self::TargetHeld {
                database,
                holders: held,
            } => {
                write!(formatter, "the target's {} is open in ", database.display())?;
                holders(formatter, held)
            }
            Self::LandingOpening {
                database,
                holders: held,
            }
            | Self::TargetOpening {
                database,
                holders: held,
            } => {
                write!(
                    formatter,
                    "an Nx process was opening the {}'s {}: ",
                    if matches!(self, Self::LandingOpening { .. }) {
                        "landing workspace"
                    } else {
                        "target"
                    },
                    database.display()
                )?;
                holders(formatter, held)
            }
            Self::LandingDaemonStayed { daemon } => write!(
                formatter,
                "the landing workspace's Nx daemon pid {} ({}) is still running; it was not stopped",
                daemon.pid, daemon.command
            ),
            Self::LandingDaemonUnverifiable { record, reason } => write!(
                formatter,
                "the landing workspace's Nx daemon record {} could not be verified: {reason}; no process was signalled",
                record.display()
            ),
            Self::TargetDaemonStayed { daemon } => write!(
                formatter,
                "the target's Nx daemon pid {} ({}) did not exit after SIGTERM",
                daemon.pid, daemon.command
            ),
            Self::LandingVolumeBusy { reason } => write!(
                formatter,
                "the landing build volume could not be grown to the target's capacity: {reason}"
            ),
            Self::Ungated => formatter.write_str(
                "an ungated land carries no build volume: the target keeps its own, and the landing workspace's retires with it",
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatabaseHolder {
    pub pid: i32,
    pub command: String,
}

/// Step 7: cache-only output restoration for the Nx runs validated before delivery. No check is rerun.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdoptionCheck {
    pub hits: u64,
    /// Checked tasks not restored, including uncacheable tasks and absent local results.
    pub misses: Vec<CacheMiss>,
    /// Checks without an attributed source summary: counted neither as hits nor as misses.
    pub unattributed: Vec<UnattributedCheck>,
    /// Cache-only restoration helpers that failed in the target, never rerun check commands.
    pub failed: Vec<FailedCheck>,
}

/// A landed check without an attributable run summary in the landing workspace's cache.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnattributedCheck {
    pub check: String,
    /// The cache, as the checkout spells it (`.nx/cache`).
    pub cache: String,
    pub reason: UnattributedRun,
}

/// Why a run summary is not the check's own (16_build_volumes.md, Land step 7).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum UnattributedRun {
    /// The cache holds no run summary.
    NoSummary,
    /// The summary cannot be read as stock Nx writes it.
    Unreadable { error: String },
    /// Its run began before the check was spawned: the check ran no Nx there, or an earlier run
    /// wrote last.
    BeganBeforeCheck { start: String },
    /// Its run ended after the check exited: another run wrote last.
    EndedAfterCheck { end: String },
    /// Its command is not one the check spells.
    ForeignCommand { command: String },
    /// It has no task for these targets its command names.
    Uncovered { targets: Vec<String> },
}

impl UnattributedRun {
    /// The stable name telemetry records for the reason.
    pub fn name(&self) -> &'static str {
        match self {
            Self::NoSummary => "noSummary",
            Self::Unreadable { .. } => "unreadable",
            Self::BeganBeforeCheck { .. } => "beganBeforeCheck",
            Self::EndedAfterCheck { .. } => "endedAfterCheck",
            Self::ForeignCommand { .. } => "foreignCommand",
            Self::Uncovered { .. } => "uncovered",
        }
    }
}

impl fmt::Display for UnattributedRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSummary => formatter.write_str("no Nx run summary"),
            Self::Unreadable { error } => {
                write!(formatter, "the run summary is unreadable: {error}")
            }
            Self::BeganBeforeCheck { start } => {
                write!(
                    formatter,
                    "the last Nx run began before the check, at {start}"
                )
            }
            Self::EndedAfterCheck { end } => {
                write!(formatter, "the last Nx run ended after the check, at {end}")
            }
            Self::ForeignCommand { command } => {
                write!(
                    formatter,
                    "the last Nx run was `{command}`, which the check does not spell"
                )
            }
            Self::Uncovered { targets } => write!(
                formatter,
                "the last Nx run has no task for {}",
                targets.join(", ")
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FailedCheck {
    pub check: String,
    /// The cache-only helper failure, including its available stderr.
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit: Option<i32>,
}

/// One Nx task that missed the cache in the target after adoption, with the inputs Nx hashes
/// for it (`nx show target inputs`) and their digest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CacheMiss {
    pub task: String,
    /// The hash validated in the landing workspace, never a substituted target hash.
    pub hash: String,
    /// Why this checked result could not be restored without executing the task.
    pub reason: String,
    /// Each input category Nx reports (`files`, `environment`, `external`, …) and its entries;
    /// empty when Nx could not name them, with `inputs_error` saying why.
    pub inputs: std::collections::BTreeMap<String, Vec<String>>,
    pub inputs_digest: Sha256Digest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs_error: Option<String>,
}

/// The commits a `--abandon` removal destroyed, and where they are kept.
///
/// Every field is what a reader needs to *undo* the abandonment: the tip that is gone, the branch
/// it never reached and where that branch stood, how much history went with it, and the bundle to
/// fetch from, which is the only copy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AbandonedWork {
    pub head: GitOid,
    pub target_branch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_head: Option<GitOid>,
    /// Commits reachable from `head` or any of `branches` and not from the target.
    pub unlanded_commits: u64,
    /// The local branches besides `head` whose unheld commits went into the bundle as refs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<BranchLanding>,
    /// What keeps the abandoned tips besides the bundle (02_workspaces.md, `rm` step 1): nothing
    /// for a session's abandonment, which writes the field as absent and reads that absence as
    /// [`AbandonedPreservation::BundleOnly`].
    #[serde(
        default = "AbandonedPreservation::bundle_only",
        skip_serializing_if = "AbandonedPreservation::is_bundle_only"
    )]
    pub preserved: AbandonedPreservation,
    pub bundle: PathBuf,
    /// The commit the abandonment wrote on top of `head` of the work tree's uncommitted work, in
    /// the bundle under `refs/cowshed/uncommitted` (02_workspaces.md, `rm` step 1); absent when
    /// the tree was clean.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncommitted: Option<GitOid>,
}

/// What keeps an abandonment's tips besides its bundle.
///
/// Three facts are kept apart, because each tells a reader something else about where the
/// commits are: the bundle alone, main itself being what was abandoned, and the refs an earlier
/// build also wrote into main's repository.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum AbandonedPreservation {
    /// Recorded by an earlier build, which also kept every tip in main's repository under
    /// `refs/cowshed/<ws>/abandoned/<head>/`. No build writes or reads those refs now; the record is
    /// read for what that removal did.
    Preserved { refs: Vec<PreservedRef> },
    /// Main's own `--restore --abandon`: main's repository is what is being restored, so there is
    /// no main to keep refs in; the bundle in the restored checkout's Git directory holds them.
    NothingToPreserve,
    /// The bundle is the only copy (01_storage.md, "Retention"): every session abandonment.
    BundleOnly,
}

impl AbandonedPreservation {
    const fn bundle_only() -> Self {
        Self::BundleOnly
    }

    const fn is_bundle_only(&self) -> bool {
        matches!(self, Self::BundleOnly)
    }
}

/// One tip an earlier build's abandonment kept in main's repository.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreservedRef {
    /// `HEAD`, or the branch's short name.
    pub name: String,
    pub tip: GitOid,
    /// The full ref name in main's repository that holds `tip`.
    pub reference: String,
}

/// What `cowshed rm` destroyed beyond the workspace image itself.
///
/// `abandoned` is `None` for every removal whose commits the target branch already contained —
/// the ordinary case, where the image is the only thing that dies.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveReport {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abandoned: Option<AbandonedWork>,
}

/// One session workspace a project removal retired, with what its removal destroyed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemovedWorkspace {
    pub workspace: WorkspaceName,
    pub report: RemoveReport,
}

/// What removing a project did, in order: the session workspaces it retired, the collection
/// that reclaimed their images, the abandon bundles it deleted, and main's restore.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveProjectReport {
    pub removed: Vec<RemovedWorkspace>,
    pub collected: GcReport,
    pub deleted_bundles: Vec<PathBuf>,
    pub restored: RemoveReport,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MirrorInfo {
    pub url: String,
    pub mirror: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewayStatus {
    /// Whether launchd has the gateway agent loaded.
    pub installed: bool,
    /// Whether the control socket answered a status request.
    pub running: bool,
    pub socket: PathBuf,
    pub cli_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daemon_version: Option<String>,
    pub active_workspaces: u64,
    /// Why the answering daemon is draining. A draining daemon refuses every new session, so the
    /// gateway is not healthy while this is present, however promptly its socket answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_cause: Option<String>,
    /// How far the answering daemon's startup pass has got while it still mounts projects and
    /// restores their sessions. Its socket answers, but every command that needs a workspace is
    /// refused until this is gone, so the gateway is not healthy yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healing: Option<cowshed_gateway_types::StartupHeal>,
    /// The workspace supervisors from before the answering daemon started that it is still
    /// recovering. It serves meanwhile; only a command for one of their workspaces is refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovering: Option<cowshed_gateway_types::SupervisorRecovery>,
    /// Present when the answering daemon runs other bytes than this CLI. Both report version
    /// 0.1.0 across every build, so only the executables' contents can tell them apart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_daemon: Option<StaleDaemonBinary>,
    /// The host's scheduling as its kernel, its workspace supervisors and its store report it
    /// (05_gateway.md "Status"), read by the CLI beside the daemon. Absent from an install's
    /// health wait, which reads none of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduling: Option<HostScheduling>,
    /// True while an install holds the install slot (`dev.cowshed/install.lock`), read without
    /// taking it (05_gateway.md "A pending install is visible"). Absent on the wire while free.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pending_install: bool,
    /// The gateway's land queues with a ticket, one per repository, each first ticket holding its
    /// turn. Read without taking a turn; empty when no land is queued or the queues could not be read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub land_turns: Vec<LandTurnQueue>,
    /// Why `land_turns` could not be read, when reading it failed; the rest of the status is still
    /// served.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub land_turns_error: Option<String>,
}

/// One repository's land queue as the gateway lists it (05_gateway.md "Land turns"): its tickets in
/// turn order, the first of which holds the turn, and the tickets abandoned in the last day.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandTurnQueue {
    pub repo: LandTurnName,
    pub tickets: Vec<Ticket>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub abandoned: Vec<crate::land_turn::Abandonment>,
}

/// The daemon and CLI executables, when their contents differ.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StaleDaemonBinary {
    /// SHA-256 of the daemon's executable, or `None` for a daemon too old to report it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daemon_sha256: Option<String>,
    /// SHA-256 of the executable of the CLI that asked.
    pub cli_sha256: String,
}

/// What `gateway status` reads of the host's scheduling (05_gateway.md "Status"). Every number is
/// a reading of the kernel, a supervisor's job list or a store listing, never a count of cowshed's
/// own, and a reading that failed says why instead of standing in as zero.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostScheduling {
    /// Load, memory pressure and swap, as the kernel reports them.
    pub readings: cowshed_gateway_types::host_status::HostReadings,
    /// The jobs every live workspace supervisor lists.
    pub jobs: HostJobs,
    /// Released images the store's `.deleting` directories hold for the gateway's cleanup.
    pub image_cleanup: ImageCleanup,
}

/// The jobs of every live workspace supervisor on the host, counted by class and state.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HostJobs {
    /// One row per class and state that has a job: queued, running or draining.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub counts: Vec<JobCount>,
    /// The supervisors whose job list could not be read: their jobs are in no count.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unanswered: Vec<UnansweredSupervisor>,
}

/// How many of the host's live jobs have one class and one state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobCount {
    pub class: ExecClass,
    pub state: JobState,
    pub jobs: u64,
}

/// A workspace supervisor socket whose job list could not be read, and why.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnansweredSupervisor {
    pub socket: PathBuf,
    pub error: String,
}

/// The released images waiting for the gateway's one-at-a-time cleanup (05_gateway.md "Deleting
/// released images"), as a listing of the store's `.deleting` directories finds them.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ImageCleanup {
    pub waiting: u64,
    /// Each directory the listing could not read, with why: its images are in no count.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unread: Vec<String>,
    /// The store's free-space floor in percent of its capacity, cowshed's one copy of the number
    /// (`release::STORE_FLOOR_PERCENT`); a watchdog judging the store reads it here.
    pub floor_percent: u64,
    /// The store's free space in percent of its capacity, rounded down; absent when it could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_percent: Option<u64>,
    /// The cleanup frees its next entry by one unlink: the store is under its floor plus margin and
    /// something waits (05_gateway.md "Deleting released images").
    pub pressure: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuditDecision {
    Allowed,
    Denied,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuditEvent {
    pub timestamp: UtcTimestamp,
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub workspace: WorkspaceName,
    pub action: String,
    pub decision: AuditDecision,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub trace: TraceContext,
}

/// Whether one skill install target was rewritten or already held the shipped bytes.
///
/// The distinction is the idempotence proof: a second install of an unchanged skill
/// reports `Unchanged` for every target and touches no file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SkillInstallStatus {
    Written,
    Unchanged,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SkillInstall {
    pub harness: String,
    pub path: PathBuf,
    pub status: SkillInstallStatus,
}

/// One recorded build-state link of a checkout: the tool path in the checkout and the volume path
/// that holds it (16_build_volumes.md).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildStateLink {
    pub checkout: PathBuf,
    pub volume: PathBuf,
}

/// `cowshed build-state` and `host.buildState`: every recorded build-state link of a checkout.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildStateListing {
    pub paths: Vec<BuildStateLink>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SkillInstallReport {
    pub skill: String,
    pub installs: Vec<SkillInstall>,
}

/// One approved gateway-backed registry route, as reported to an operator.
///
/// Non-secret by construction: the origin and its scope, which environment variable names the
/// route was enrolled from and are therefore withheld from every child, and whether a usable
/// credential is installed. The secret is never read into this shape, not even to be discarded —
/// `installed` is the whole answer a report needs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialRoute {
    pub repo_id: RepoId,
    pub origin: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path_prefixes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub withheld_env_names: Vec<String>,
    pub installed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CredentialReport {
    pub routes: Vec<CredentialRoute>,
}

/// A project's repository identities after `cowshed identity add`: the project, whether the named
/// remote was newly bound, the identity it names, and every identity the binding now holds — each
/// one a URL workspaces fetch from the local clone.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IdentityReport {
    pub repo_id: RepoId,
    pub added: bool,
    pub identity: BoundIdentity,
    pub identities: Vec<BoundIdentity>,
}

mod result_body_seal {
    pub trait Sealed {}
}

pub trait ResultBody:
    result_body_seal::Sealed + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

impl<T> ResultBody for T where
    T: result_body_seal::Sealed + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

macro_rules! result_bodies {
    ($($type:ty),+ $(,)?) => {
        $(impl result_body_seal::Sealed for $type {})+
    };
}

result_bodies!(
    EmptyResult,
    MountResult,
    DoctorReport,
    GcReport,
    RekeyResult,
    RebaseReport,
    ResizeResult,
    DefragmentResult,
    NxStateClearResult,
    CacheClearReport,
    SlotResult,
    WorkspaceInfo,
    JobInfo,
    ExecRecord,
    PushReport,
    LandReport,
    DocCommitReport,
    RemoveReport,
    RemoveProjectReport,
    GrantSet,
    ProjectGrants,
    GatewayStatus,
    MirrorInfo,
    AuditEvent,
    SkillInstallReport,
    CredentialReport,
    IdentityReport,
    HostSetupReport,
    UninstallReport,
    ProjectRetirementReport,
    Vec<WorkspaceInfo>,
    Vec<ProjectWorkspaces>,
    Vec<JobInfo>,
    Vec<ExecRecord>,
    Vec<AuditEvent>,
);

#[derive(Clone, Debug, Eq, PartialEq)]
enum EnvelopeBody<T> {
    Success(T),
    Failure(CowshedError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonEnvelope<T: ResultBody> {
    body: EnvelopeBody<T>,
}

impl<T: ResultBody> JsonEnvelope<T> {
    pub fn success(result: T) -> Self {
        Self {
            body: EnvelopeBody::Success(result),
        }
    }

    pub fn failure(error: CowshedError) -> Self {
        Self {
            body: EnvelopeBody::Failure(error),
        }
    }

    pub fn result(&self) -> Option<&T> {
        match &self.body {
            EnvelopeBody::Success(result) => Some(result),
            EnvelopeBody::Failure(_) => None,
        }
    }

    pub fn error(&self) -> Option<&CowshedError> {
        match &self.body {
            EnvelopeBody::Success(_) => None,
            EnvelopeBody::Failure(error) => Some(error),
        }
    }
}

impl<T: ResultBody> Serialize for JsonEnvelope<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        match &self.body {
            EnvelopeBody::Success(result) => {
                map.serialize_entry("ok", &true)?;
                map.serialize_entry("result", result)?;
            }
            EnvelopeBody::Failure(error) => {
                map.serialize_entry("ok", &false)?;
                map.serialize_entry("error", error)?;
            }
        }
        map.end()
    }
}

impl<'de, T: ResultBody> Deserialize<'de> for JsonEnvelope<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("JSON envelope must be an object"))?;
        let ok = object
            .get("ok")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| serde::de::Error::custom("JSON envelope requires boolean ok"))?;
        if object.len() != 2 {
            return Err(serde::de::Error::custom(
                "JSON envelope must have exactly two fields",
            ));
        }
        if ok {
            let result = object
                .get("result")
                .ok_or_else(|| serde::de::Error::custom("successful envelope requires result"))?;
            serde_json::from_value(result.clone())
                .map(Self::success)
                .map_err(serde::de::Error::custom)
        } else {
            let error = object
                .get("error")
                .ok_or_else(|| serde::de::Error::custom("failed envelope requires error"))?;
            serde_json::from_value(error.clone())
                .map(Self::failure)
                .map_err(serde::de::Error::custom)
        }
    }
}
