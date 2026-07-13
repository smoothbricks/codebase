use crate::error::CowshedError;
pub use crate::metadata::{
    EgressMode, EgressRule, GrantSet, ImageFormat, Platform, PortBlock, RepoRule, SimVerb,
    WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use crate::repository::RepoId;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::io::AsyncRead;

pub const MAX_JOB_ID: u64 = (1_u64 << 53) - 1;

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
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || *bytes.last().unwrap_or(&0) != b'Z'
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
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    let fraction_valid = match bytes.get(19..bytes.len() - 1) {
        Some([]) => true,
        Some([b'.', digits @ ..]) => !digits.is_empty() && digits.iter().all(u8::is_ascii_digit),
        _ => false,
    };
    (1..=max_day).contains(&day) && hour <= 23 && minute <= 59 && second <= 59 && fraction_valid
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamInfo {
    pub path: WorkspacePath,
    pub bytes: u64,
    pub summary: OutputSummary,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobInfo {
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub grant_revision: u64,
    pub argv: Vec<String>,
    pub cwd: WorkspacePath,
    pub started: UtcTimestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub trace: TraceContext,
    #[serde(skip_serializing_if = "Option::is_none")]
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
            | (JobState::Queued | JobState::Running, None) => Ok(()),
            _ => Err(DtoError::InvalidJobProjection(
                "exit kind does not agree with job state",
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit: Option<ExitStatus>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
    pub stdin: StdinInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_limit: Option<OutputLimitInfo>,
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

#[derive(Debug)]
pub struct ExecRequest {
    pub argv: Vec<String>,
    pub cwd: Option<WorkspacePath>,
    pub mode: RunSandboxMode,
    pub env: HashMap<String, String>,
    pub trace: Option<TraceContext>,
    pub stdin: StdinSource,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RevisionTarget {
    Branch {
        branch: String,
    },
    Ref {
        #[serde(rename = "ref")]
        git_ref: String,
    },
    Oid {
        oid: GitOid,
    },
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoveOptions {
    pub force: bool,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonEnvelope<T> {
    Success(T),
    Failure(CowshedError),
}

impl<T: Serialize> Serialize for JsonEnvelope<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        match self {
            Self::Success(result) => {
                map.serialize_entry("ok", &true)?;
                map.serialize_entry("result", result)?;
            }
            Self::Failure(error) => {
                map.serialize_entry("ok", &false)?;
                map.serialize_entry("error", error)?;
            }
        }
        map.end()
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for JsonEnvelope<T> {
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
                .map(Self::Success)
                .map_err(serde::de::Error::custom)
        } else {
            let error = object
                .get("error")
                .ok_or_else(|| serde::de::Error::custom("failed envelope requires error"))?;
            serde_json::from_value(error.clone())
                .map(Self::Failure)
                .map_err(serde::de::Error::custom)
        }
    }
}
