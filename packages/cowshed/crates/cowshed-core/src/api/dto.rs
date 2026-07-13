use crate::error::CowshedError;
pub use crate::metadata::{
    EgressMode, EgressRule, GrantSet, ImageFormat, Platform, PortBlock, RepoRule, SimVerb,
    WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use crate::repository::RepoId;
use base64::Engine;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Digest;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::io::AsyncRead;

pub const MAX_JOB_ID: u64 = (1_u64 << 53) - 1;
pub const MAX_INLINE_OUTPUT_BYTES: usize = 64 * 1024;
pub const MAX_OUTPUT_SUMMARY_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DtoError {
    #[error("job id must be in 1..={MAX_JOB_ID}, got {0}")]
    InvalidJobId(u64),
    #[error("invalid git object id {0:?}")]
    InvalidGitOid(String),
    #[error("invalid UTC timestamp {0:?}")]
    InvalidTimestamp(String),
    #[error("invalid workspace-relative path {0:?}")]
    InvalidWorkspacePath(PathBuf),
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
    #[error("invalid SHA-256 digest {0:?}")]
    InvalidSha256Digest(String),
    #[error("invalid stream projection: {0}")]
    InvalidStreamProjection(&'static str),
    #[error("invalid branch name {0:?}")]
    InvalidBranchName(String),
    #[error("invalid fully-qualified git ref {0:?}")]
    InvalidGitRef(String),
}

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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GitOid(String);

impl GitOid {
    pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        if matches!(value.len(), 40 | 64)
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidGitOid(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
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

fn is_rfc3339_utc(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || !value.is_ascii()
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
    {
        return false;
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
        return false;
    };
    let leap_year = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap_year => 29,
        2 => 28,
        _ => return false,
    };
    let (time_end, offset_seconds) = if bytes.last() == Some(&b'Z') {
        (bytes.len() - 1, 0_i64)
    } else if bytes.len() >= 25 {
        let sign = bytes.len() - 6;
        let (Some(offset_hour), Some(offset_minute)) =
            (digits(sign + 1, sign + 3), digits(sign + 4, sign + 6))
        else {
            return false;
        };
        if !matches!(bytes[sign], b'+' | b'-')
            || bytes[sign + 3] != b':'
            || offset_hour > 23
            || offset_minute > 59
        {
            return false;
        }
        let magnitude = i64::from(offset_hour * 3600 + offset_minute * 60);
        let offset = if bytes[sign] == b'+' {
            magnitude
        } else {
            -magnitude
        };
        (sign, offset)
    } else {
        return false;
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
    calendar_valid && second_valid && fraction_valid
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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UtcTimestamp(String);

impl UtcTimestamp {
    pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
        let value = value.into();
        let valid = is_rfc3339_utc(&value);
        if valid {
            Ok(Self(value))
        } else {
            Err(DtoError::InvalidTimestamp(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
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
        serializer.serialize_str(
            self.0
                .to_str()
                .expect("WorkspacePath construction guarantees UTF-8"),
        )
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

macro_rules! hex_identifier {
    ($name:ident, $bytes:literal, $error:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, DtoError> {
                let value = value.into();
                if value.len() == $bytes * 2
                    && value.bytes().any(|byte| byte != b'0')
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkspaceState {
    Attached,
    Detached,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointInfo {
    pub label: String,
    pub revision: u64,
    pub pinned: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceInfo {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub role: WorkspaceRole,
    pub image_format: ImageFormat,
    pub mount: PathBuf,
    pub state: WorkspaceState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_commit: Option<GitOid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<UtcTimestamp>,
    pub checkpoints: Vec<CheckpointInfo>,
    pub snapshot_stale: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EnsureAction {
    AlreadyMounted,
    Attached,
    Healed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnsureReport {
    pub workspace: WorkspaceName,
    pub mount: PathBuf,
    pub action: EnsureAction,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointResult {
    pub label: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RevisionResult {
    pub oid: GitOid,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GcReport {
    pub examined: u64,
    pub reclaimed: u64,
    pub retained_pinned: u64,
    pub freed_bytes: u64,
    pub dry_run: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum JobState {
    Queued,
    Running,
    Exited,
    Signaled,
    Killed,
    OutputLimit,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputSummary {
    pub version: u16,
    pub text: String,
    pub truncated: bool,
}

/// Exact, bounded bytes for control-oriented JSON.
///
/// Valid UTF-8 is emitted as tagged UTF-8. Every other byte sequence is emitted as tagged
/// base64. The tag makes the wire representation unambiguous and deserialization never performs
/// lossy conversion.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BinaryEncoding {
    Utf8,
    Base64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BinaryDataRef<'a> {
    encoding: BinaryEncoding,
    data: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BinaryDataWire {
    encoding: BinaryEncoding,
    data: String,
}

impl Serialize for BinaryData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match std::str::from_utf8(&self.0) {
            Ok(data) => BinaryDataRef {
                encoding: BinaryEncoding::Utf8,
                data,
            }
            .serialize(serializer),
            Err(_) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(&self.0);
                BinaryDataRef {
                    encoding: BinaryEncoding::Base64,
                    data: &encoded,
                }
                .serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for BinaryData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = BinaryDataWire::deserialize(deserializer)?;
        let bytes = match wire.encoding {
            BinaryEncoding::Utf8 => wire.data.into_bytes(),
            BinaryEncoding::Base64 => base64::engine::general_purpose::STANDARD
                .decode(wire.data)
                .map_err(|_| serde::de::Error::custom(DtoError::InvalidBinaryEncoding))?,
        };
        Self::new(bytes).map_err(serde::de::Error::custom)
    }
}

/// A fixed-width SHA-256 digest, serialized as 64 lowercase hexadecimal characters.
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
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
        }
        output
    }

    pub fn from_hex(value: &str) -> Result<Self, DtoError> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DtoError::InvalidSha256Digest(value.to_owned()));
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_nibble(pair[0]);
            let low = hex_nibble(pair[1]);
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputLimitInfo {
    pub limit_bytes: u64,
    pub crossing_bytes: u64,
}
pub const CONTROLLER_COMMITMENT_VERSION: u16 = 1;

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
pub struct CheckpointCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    pub origin_incarnation: WorkspaceIncarnation,
    pub checkpoint_id: String,
    pub barrier_id: u64,
    pub manifest_batch_sha256: Sha256Digest,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RestoreCommitment {
    pub version: u16,
    pub order: u64,
    pub repo_id: RepoId,
    pub source_checkpoint: String,
    pub source_incarnation: WorkspaceIncarnation,
    pub destination_incarnation: WorkspaceIncarnation,
}

/// Versioned controller-owned existence, lifecycle, order, and lineage evidence.
///
/// Protected payload bytes and artifact paths deliberately do not appear in any variant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControllerCommitment {
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Checkpoint(CheckpointCommitment),
    Fork(ForkCommitment),
    Restore(RestoreCommitment),
}

impl ControllerCommitment {
    pub fn version(&self) -> u16 {
        match self {
            Self::Admission(value) => value.version,
            Self::Terminal(value) => value.version,
            Self::Checkpoint(value) => value.version,
            Self::Fork(value) => value.version,
            Self::Restore(value) => value.version,
        }
    }

    pub fn order(&self) -> u64 {
        match self {
            Self::Admission(value) => value.order,
            Self::Terminal(value) => value.order,
            Self::Checkpoint(value) => value.order,
            Self::Fork(value) => value.order,
            Self::Restore(value) => value.order,
        }
    }

    pub fn repo_id(&self) -> &RepoId {
        match self {
            Self::Admission(value) => &value.repo_id,
            Self::Terminal(value) => &value.repo_id,
            Self::Checkpoint(value) => &value.repo_id,
            Self::Fork(value) => &value.repo_id,
            Self::Restore(value) => &value.repo_id,
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
            Self::Terminal(value)
                if matches!(value.state, JobState::Queued | JobState::Running) =>
            {
                Err(DtoError::InvalidJobProjection(
                    "terminal commitment must contain a terminal state",
                ))
            }
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
            Self::Checkpoint(value) if !valid_commitment_id(&value.checkpoint_id) => Err(
                DtoError::InvalidJobProjection("checkpoint commitment id is invalid"),
            ),
            Self::Checkpoint(value) if value.barrier_id == 0 => Err(
                DtoError::InvalidJobProjection("checkpoint barrier id must be positive"),
            ),
            Self::Fork(value) if value.source_incarnation == value.destination_incarnation => {
                Err(DtoError::InvalidJobProjection(
                    "fork source and destination incarnations must differ",
                ))
            }
            Self::Restore(value) if !valid_commitment_id(&value.source_checkpoint) => Err(
                DtoError::InvalidJobProjection("restore source checkpoint id is invalid"),
            ),
            Self::Restore(value) if value.source_incarnation == value.destination_incarnation => {
                Err(DtoError::InvalidJobProjection(
                    "restore source and destination incarnations must differ",
                ))
            }
            _ => Ok(()),
        }
    }
}

fn valid_commitment_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum ControllerCommitmentRef<'a> {
    Admission(&'a AdmissionCommitment),
    Terminal(&'a TerminalCommitment),
    Checkpoint(&'a CheckpointCommitment),
    Fork(&'a ForkCommitment),
    Restore(&'a RestoreCommitment),
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum ControllerCommitmentWire {
    Admission(AdmissionCommitment),
    Terminal(TerminalCommitment),
    Checkpoint(CheckpointCommitment),
    Fork(ForkCommitment),
    Restore(RestoreCommitment),
}

impl Serialize for ControllerCommitment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(serde::ser::Error::custom)?;
        match self {
            Self::Admission(value) => ControllerCommitmentRef::Admission(value),
            Self::Terminal(value) => ControllerCommitmentRef::Terminal(value),
            Self::Checkpoint(value) => ControllerCommitmentRef::Checkpoint(value),
            Self::Fork(value) => ControllerCommitmentRef::Fork(value),
            Self::Restore(value) => ControllerCommitmentRef::Restore(value),
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
            ControllerCommitmentWire::Admission(value) => Self::Admission(value),
            ControllerCommitmentWire::Terminal(value) => Self::Terminal(value),
            ControllerCommitmentWire::Checkpoint(value) => Self::Checkpoint(value),
            ControllerCommitmentWire::Fork(value) => Self::Fork(value),
            ControllerCommitmentWire::Restore(value) => Self::Restore(value),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobInfo {
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    pub pid: Option<u32>,
    pub grant_revision: u64,
    pub argv: Vec<String>,
    pub cwd: WorkspacePath,
    pub started: UtcTimestamp,
    pub duration_ms: Option<u64>,
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub trace: TraceContext,
    pub output_limit: Option<OutputLimitInfo>,
    pub stdin: StdinInfo,
}

impl JobInfo {
    pub fn validate(&self) -> Result<(), DtoError> {
        let terminal = !matches!(self.state, JobState::Queued | JobState::Running);
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
        match (&self.state, &self.exit) {
            (JobState::Exited, Some(ExitStatus::Exited { .. }))
            | (JobState::Signaled | JobState::Killed, Some(ExitStatus::Signaled { .. }))
            | (JobState::OutputLimit | JobState::Failed, _)
            | (JobState::Queued | JobState::Running, None) => {}
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
    argv: &'a [String],
    cwd: &'a WorkspacePath,
    started: &'a UtcTimestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exit: Option<&'a ExitStatus>,
    stdout: &'a StreamInfo,
    stderr: &'a StreamInfo,
    trace: &'a TraceContext,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_limit: Option<&'a OutputLimitInfo>,
    stdin: &'a StdinInfo,
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
    argv: Vec<String>,
    cwd: WorkspacePath,
    started: UtcTimestamp,
    duration_ms: Option<u64>,
    exit: Option<ExitStatus>,
    stdout: StreamInfo,
    stderr: StreamInfo,
    trace: TraceContext,
    output_limit: Option<OutputLimitInfo>,
    stdin: StdinInfo,
}

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
            argv: &self.argv,
            cwd: &self.cwd,
            started: &self.started,
            duration_ms: self.duration_ms,
            exit: self.exit.as_ref(),
            stdout: &self.stdout,
            stderr: &self.stderr,
            trace: &self.trace,
            output_limit: self.output_limit.as_ref(),
            stdin: &self.stdin,
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
        let value = Self {
            repo_id: wire.repo_id,
            workspace_incarnation: wire.workspace_incarnation,
            job_id: wire.job_id,
            state: wire.state,
            pid: wire.pid,
            grant_revision: wire.grant_revision,
            argv: wire.argv,
            cwd: wire.cwd,
            started: wire.started,
            duration_ms: wire.duration_ms,
            exit: wire.exit,
            stdout: wire.stdout,
            stderr: wire.stderr,
            trace: wire.trace,
            output_limit: wire.output_limit,
            stdin: wire.stdin,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecRecord {
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    pub argv: Vec<String>,
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
        if matches!(self.state, JobState::Queued | JobState::Running) {
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
            | (JobState::Signaled | JobState::Killed, Some(ExitStatus::Signaled { .. }))
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
    argv: &'a [String],
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
    argv: Vec<String>,
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

pub enum StdinSource {
    Empty,
    Inline(Bytes),
    Stream(Pin<Box<dyn AsyncRead + Send>>),
    WorkspaceFile(WorkspacePath),
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
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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

#[derive(Debug)]
pub struct ExecRequest {
    pub argv: Vec<String>,
    pub cwd: Option<WorkspacePath>,
    pub mode: RunSandboxMode,
    pub env: HashMap<String, String>,
    pub trace: Option<TraceContext>,
    pub stdin: StdinSource,
    pub stdout_copy: Option<OutputPublication>,
    pub stderr_copy: Option<OutputPublication>,
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
        if matches!(value.len(), 40 | 64)
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_format: Option<ImageFormat>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<RevisionTarget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_workspace: Option<WorkspaceName>,
    pub browse: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct AttachOptions {
    pub browse: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub keep: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveOptions {
    pub force: bool,
    pub restore: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct GcOptions {
    pub dry_run: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RebaseOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onto: Option<RevisionTarget>,
    pub fresh: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_workspace_incarnation: Option<WorkspaceIncarnation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_source_head: Option<GitOid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_onto_head: Option<GitOid>,
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
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct PushOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
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
    pub egress: Vec<EgressRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub repos: Vec<RepoRule>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sim: Vec<SimVerb>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushReport {
    pub source_head: GitOid,
    pub destination_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_destination_head: Option<GitOid>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LandReport {
    pub landed_head: GitOid,
    pub target_branch: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_target_head: Option<GitOid>,
    pub target_was_checked_out: bool,
    pub retired: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MirrorInfo {
    pub url: String,
    pub mirror: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointQuota {
    pub max_count: u32,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GatewayStatus {
    pub running: bool,
    pub socket: PathBuf,
    pub cache_entries: u64,
    pub cache_bytes: u64,
    pub active_workspaces: u64,
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
    EnsureReport,
    DoctorReport,
    GcReport,
    CheckpointResult,
    RevisionResult,
    SlotResult,
    WorkspaceInfo,
    JobInfo,
    ExecRecord,
    PushReport,
    LandReport,
    GrantSet,
    GatewayStatus,
    MirrorInfo,
    AuditEvent,
    Vec<WorkspaceInfo>,
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
