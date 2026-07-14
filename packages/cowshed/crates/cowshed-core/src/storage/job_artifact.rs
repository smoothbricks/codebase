use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, ListArray, RecordBatch, StringArray, StructArray,
    UInt64Array, new_null_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Fields, Schema};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::api::dto::{
    AdmissionCommitment, BinaryData, CheckpointCommitment, CommandArg, ControllerCommitment,
    DtoError, ForkCommitment, JobId, JobState, MAX_ARGV_BYTES, MAX_COMMAND_ARG_BYTES,
    MAX_INLINE_OUTPUT_BYTES, OutputLimitInfo, OutputPublication, OutputStorage, OutputSummary,
    ProtectedOutput, RestoreCommitment, Sha256Digest, StreamInfo, TerminalCommitment,
    WorkspaceIntroducedCommitment, WorkspacePath, WorkspaceRetiredCommitment,
    validate_command_argv,
};
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;
mod publication;
use crate::storage::verify_no_symlinks;

const RECORD_MAGIC: &[u8; 8] = b"CSARROW1";
const BATCH_MAGIC: &[u8; 8] = b"CSBATCH1";
const BATCH_TRAILER: &[u8; 8] = b"CSEND001";
const FRAME_HEADER_BYTES: usize = 24;
const FRAME_OVERHEAD_BYTES: usize = FRAME_HEADER_BYTES + 32 + BATCH_TRAILER.len();
const MAX_RECORD_BATCH_BYTES: u64 = 8_388_608;
const IO_BUFFER_BYTES: usize = 65_536;
const RECORD_SCHEMA_VERSION: u64 = 2;
#[cfg(unix)]
const SECURE_DIRECTORY_OPEN_FLAGS: libc::c_int =
    libc::O_DIRECTORY + libc::O_NOFOLLOW + libc::O_CLOEXEC;
#[cfg(unix)]
const SECURE_FILE_OPEN_FLAGS: libc::c_int = libc::O_NOFOLLOW + libc::O_CLOEXEC;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationStage {
    ValidateDestination,
    CreateTemporary,
    Clone,
    Copy,
    Sync,
    Publish,
    Cleanup,
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum ArtifactError {
    #[error("invalid artifact configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("artifact I/O failed for {path}: {message}")]
    Io { path: PathBuf, message: String },
    #[error("artifact Arrow IPC failure: {0}")]
    Arrow(String),
    #[error("artifact integrity failure at byte {offset}: {message}")]
    Integrity { offset: u64, message: String },
    #[error("artifact write integrity failure: {0}")]
    WriteIntegrity(String),
    #[error("artifact token conflict for job {job_id:?}: {message}")]
    TokenConflict {
        job_id: JobId,
        message: &'static str,
    },
    #[error("failed to secure redirect descriptor for {path}: {message}")]
    RedirectDescriptor { path: PathBuf, message: String },
    #[error(
        "artifact recovery retention budget {limit_bytes} bytes exceeded by {required_bytes} bytes"
    )]
    RecoveryBudgetExceeded {
        limit_bytes: usize,
        required_bytes: usize,
    },
    #[error("artifact stream has {bytes} bytes, exceeding convenience read limit {limit_bytes}")]
    StreamTooLarge { limit_bytes: u64, bytes: u64 },
    #[error("inline buffer allocation failed")]
    BufferAllocation,
    #[error("invalid terminal artifact state: {0}")]
    InvalidTerminalState(&'static str),
    #[error("output publication failed during {stage:?} for {path}: {message}")]
    Publication {
        path: PathBuf,
        stage: PublicationStage,
        message: String,
    },
    #[error(transparent)]
    Dto(#[from] DtoError),
}

fn io_error(path: &Path, source: io::Error) -> ArtifactError {
    ArtifactError::Io {
        path: path.to_owned(),
        message: source.to_string(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactConfig {
    pub inline_cap_bytes: usize,
    pub supervisor_buffer_budget_bytes: usize,
    pub combined_output_quota_bytes: u64,
    pub retained_recovery_budget_bytes: usize,
    pub admitted_historical_incarnations: BTreeSet<WorkspaceIncarnation>,
}

impl ArtifactConfig {
    pub fn validate(&self) -> Result<(), ArtifactError> {
        if self.inline_cap_bytes > MAX_INLINE_OUTPUT_BYTES {
            return Err(ArtifactError::InvalidConfig(
                "inline cap exceeds the bounded public DTO limit",
            ));
        }
        if self.retained_recovery_budget_bytes == 0 {
            return Err(ArtifactError::InvalidConfig(
                "retained recovery budget must be positive",
            ));
        }
        if self.combined_output_quota_bytes == 0 {
            return Err(ArtifactError::InvalidConfig(
                "combined output quota must be positive",
            ));
        }
        Ok(())
    }
}

impl Default for ArtifactConfig {
    fn default() -> Self {
        Self {
            inline_cap_bytes: 64 * 1024,
            supervisor_buffer_budget_bytes: 8 * 1024 * 1024,
            combined_output_quota_bytes: 1024 * 1024 * 1024,
            retained_recovery_budget_bytes: 64 * 1024 * 1024,
            admitted_historical_incarnations: BTreeSet::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamKind {
    Stdout,
    Stderr,
}

impl StreamKind {
    fn leaf(self) -> &'static str {
        match self {
            Self::Stdout => "out",
            Self::Stderr => "err",
        }
    }
}

#[derive(Debug)]
pub enum StreamTarget {
    Captured,
    /// A trusted supervisor-owned descriptor that receives a live copy of admitted bytes.
    ///
    /// The canonical artifact is captured independently by the buffer/spill pipeline. The
    /// artifact layer never reads or seeks this descriptor and marks it close-on-exec.
    Redirect {
        source: WorkspacePath,
        descriptor: File,
    },
}

#[derive(Debug)]
pub struct OutputTargets {
    pub stdout: StreamTarget,
    pub stderr: StreamTarget,
}

impl Default for OutputTargets {
    fn default() -> Self {
        Self {
            stdout: StreamTarget::Captured,
            stderr: StreamTarget::Captured,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobArtifactRecord {
    pub repo_id: RepoId,
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub sequence: u64,
    pub state: JobState,
    pub grant_revision: u64,
    pub argv: Vec<CommandArg>,
    pub output_limit: Option<OutputLimitInfo>,
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
}

impl JobArtifactRecord {
    pub fn validate(&self) -> Result<(), ArtifactError> {
        validate_command_argv(&self.argv)?;
        if self.sequence == 0 {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: "record sequence must be positive".into(),
            });
        }
        if self.output_limit.is_some() != matches!(self.state, JobState::OutputLimit) {
            return Err(integrity(
                0,
                "terminal output limit evidence must agree with record state",
            ));
        }
        if self
            .output_limit
            .as_ref()
            .is_some_and(|limit| limit.crossing_bytes <= limit.limit_bytes)
        {
            return Err(integrity(
                0,
                "output limit crossing must exceed its configured limit",
            ));
        }
        self.stdout.validate()?;
        self.stderr.validate()?;
        validate_protected_path(self.job_id, StreamKind::Stdout, &self.stdout)?;
        validate_protected_path(self.job_id, StreamKind::Stderr, &self.stderr)?;
        Ok(())
    }
}

fn validate_protected_path(
    job_id: JobId,
    stream: StreamKind,
    info: &StreamInfo,
) -> Result<(), ArtifactError> {
    if let ProtectedOutput::File { path } = info.storage.artifact() {
        let expected = format!(".cowshed/job/{}/{}", job_id.get(), stream.leaf());
        if path.as_path() != Path::new(&expected) {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: format!(
                    "protected {} path does not match job {}",
                    stream.leaf(),
                    job_id.get()
                ),
            });
        }
    }
    Ok(())
}

fn validate_visible_path(
    job_id: JobId,
    stream: StreamKind,
    info: &VisibleStreamCommitment,
) -> Result<(), ArtifactError> {
    let Some(path) = &info.protected_path else {
        return Ok(());
    };
    let expected = format!(".cowshed/job/{}/{}", job_id.get(), stream.leaf());
    if path.as_path() != Path::new(&expected) {
        return Err(integrity(
            0,
            &format!(
                "visible {} path does not match job {}",
                stream.leaf(),
                job_id.get()
            ),
        ));
    }
    Ok(())
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum VisibleStorageKind {
    CapturedInline,
    CapturedFile,
    RedirectInline,
    RedirectFile,
}

impl VisibleStorageKind {
    fn is_file(self) -> bool {
        matches!(self, Self::CapturedFile | Self::RedirectFile)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VisibleStreamCommitment {
    pub storage_kind: VisibleStorageKind,
    pub bytes: u64,
    pub sha256: Sha256Digest,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protected_path: Option<WorkspacePath>,
}

impl VisibleStreamCommitment {
    pub fn from_stream(stream: &StreamInfo) -> Self {
        let (storage_kind, protected_path) = match &stream.storage {
            OutputStorage::Captured {
                artifact: ProtectedOutput::Inline { .. },
            } => (VisibleStorageKind::CapturedInline, None),
            OutputStorage::Captured {
                artifact: ProtectedOutput::File { path },
            } => (VisibleStorageKind::CapturedFile, Some(path.clone())),
            OutputStorage::Redirect {
                artifact: ProtectedOutput::Inline { .. },
                ..
            } => (VisibleStorageKind::RedirectInline, None),
            OutputStorage::Redirect {
                artifact: ProtectedOutput::File { path },
                ..
            } => (VisibleStorageKind::RedirectFile, Some(path.clone())),
        };
        Self {
            storage_kind,
            bytes: stream.bytes,
            sha256: stream.sha256,
            protected_path,
        }
    }

    fn validate(&self) -> Result<(), ArtifactError> {
        if self.storage_kind.is_file() != self.protected_path.is_some() {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: "visible stream protected path does not match storage kind".into(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VisibleJobCommitment {
    pub workspace_incarnation: WorkspaceIncarnation,
    pub job_id: JobId,
    pub state: JobState,
    pub stdout: VisibleStreamCommitment,
    pub stderr: VisibleStreamCommitment,
}

impl VisibleJobCommitment {
    fn from_record(record: &JobArtifactRecord) -> (JobId, Self) {
        (
            record.job_id,
            Self {
                workspace_incarnation: record.workspace_incarnation.clone(),
                job_id: record.job_id,
                state: record.state,
                stdout: VisibleStreamCommitment::from_stream(&record.stdout),
                stderr: VisibleStreamCommitment::from_stream(&record.stderr),
            },
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointManifestRecord {
    pub version: u16,
    pub repo_id: RepoId,
    pub origin_incarnation: WorkspaceIncarnation,
    pub barrier_id: u64,
    pub visible_jobs: Vec<VisibleJobCommitment>,
    pub records_sha256: Sha256Digest,
}

impl CheckpointManifestRecord {
    pub fn validate(&self) -> Result<(), ArtifactError> {
        if self.version != RECORD_SCHEMA_VERSION as u16 || self.barrier_id == 0 {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: "invalid checkpoint manifest version or barrier".into(),
            });
        }
        let mut keys = BTreeSet::new();
        for job in &self.visible_jobs {
            job.stdout.validate()?;
            job.stderr.validate()?;
            validate_visible_path(job.job_id, StreamKind::Stdout, &job.stdout)?;
            validate_visible_path(job.job_id, StreamKind::Stderr, &job.stderr)?;
            if !keys.insert((job.workspace_incarnation.clone(), job.job_id)) {
                return Err(ArtifactError::Integrity {
                    offset: 0,
                    message: "checkpoint manifest contains a duplicate job".into(),
                });
            }
            if matches!(job.state, JobState::Queued | JobState::Running)
                && (!job.stdout.storage_kind.is_file() || !job.stderr.storage_kind.is_file())
            {
                return Err(ArtifactError::Integrity {
                    offset: 0,
                    message: "running manifest streams must be promoted to protected files".into(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "protected records stay inline to avoid an allocation per durable frame"
)]
pub enum ProtectedRecord {
    Job(JobArtifactRecord),
    CheckpointManifest(CheckpointManifestRecord),
}

impl ProtectedRecord {
    fn validate(&self) -> Result<(), ArtifactError> {
        match self {
            Self::Job(value) => value.validate(),
            Self::CheckpointManifest(value) => value.validate(),
        }
    }

    fn repo_id(&self) -> &RepoId {
        match self {
            Self::Job(value) => &value.repo_id,
            Self::CheckpointManifest(value) => &value.repo_id,
        }
    }

    fn origin_incarnation(&self) -> &WorkspaceIncarnation {
        match self {
            Self::Job(value) => &value.workspace_incarnation,
            Self::CheckpointManifest(value) => &value.origin_incarnation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredFrame {
    pub record: ProtectedRecord,
    pub batch_sha256: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryReport {
    pub frames: Vec<RecoveredFrame>,
    pub truncated_bytes: u64,
    pub next_job_id: JobId,
}

pub struct ArtifactStore {
    workspace_root: PathBuf,
    token_namespace: Sha256Digest,
    repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation,
    config: ArtifactConfig,
    budget: BufferBudget,
    next_job_id: u64,
    next_sequence: u64,
    last_checkpoint_barrier: u64,
    live_jobs: BTreeMap<JobId, LiveJobState>,
    committed_jobs: BTreeMap<JobId, JobArtifactRecord>,
    recovery: RecoveryReport,
}

#[derive(Debug)]
struct BufferBudget {
    used: usize,
    limit: usize,
}

impl BufferBudget {
    fn try_reserve(&mut self, bytes: usize) -> bool {
        if self
            .used
            .checked_add(bytes)
            .is_some_and(|next| next <= self.limit)
        {
            self.used += bytes;
            true
        } else {
            false
        }
    }

    fn release(&mut self, bytes: usize) {
        self.used = self
            .used
            .checked_sub(bytes)
            .expect("artifact stream releases only its own reservation");
    }
}
fn reconcile_capacity_growth(
    budget: &mut BufferBudget,
    reserved_growth: usize,
    actual_growth: usize,
) -> bool {
    match actual_growth.cmp(&reserved_growth) {
        std::cmp::Ordering::Greater => budget.try_reserve(actual_growth - reserved_growth),
        std::cmp::Ordering::Less => {
            budget.release(reserved_growth - actual_growth);
            true
        }
        std::cmp::Ordering::Equal => true,
    }
}

impl ArtifactStore {
    pub fn open(
        workspace_root: impl Into<PathBuf>,
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
        config: ArtifactConfig,
    ) -> Result<Self, ArtifactError> {
        config.validate()?;
        let workspace_root = workspace_root.into();
        let records_path = records_path(&workspace_root);
        let mut recovery =
            recover_records_with_budget(&records_path, config.retained_recovery_budget_bytes)?;
        for frame in &recovery.frames {
            if frame.record.repo_id() != &repo_id {
                return Err(ArtifactError::Integrity {
                    offset: 0,
                    message: format!(
                        "record repo identity {} does not match attached repo {}",
                        frame.record.repo_id(),
                        repo_id
                    ),
                });
            }
            if frame.record.origin_incarnation() != &workspace_incarnation
                && !config
                    .admitted_historical_incarnations
                    .contains(frame.record.origin_incarnation())
            {
                return Err(ArtifactError::Integrity {
                    offset: 0,
                    message: format!(
                        "record incarnation {} is neither current nor controller-admitted history",
                        frame.record.origin_incarnation()
                    ),
                });
            }
        }
        validate_recovered_files(&workspace_root, &workspace_incarnation, &recovery)?;
        let directory_max = scan_job_directories(&workspace_root)?;
        let record_max = recovery
            .frames
            .iter()
            .filter_map(|frame| match &frame.record {
                ProtectedRecord::Job(record) => Some(record.job_id.get()),
                ProtectedRecord::CheckpointManifest(_) => None,
            })
            .max()
            .unwrap_or(0);
        let maximum = directory_max.max(record_max);
        let next_job_id = maximum
            .checked_add(1)
            .ok_or(ArtifactError::InvalidConfig("job id allocation exhausted"))?;
        recovery.next_job_id = JobId::new(next_job_id)?;
        let next_sequence = recovery
            .frames
            .iter()
            .filter_map(|frame| match &frame.record {
                ProtectedRecord::Job(record) => Some(record.sequence),
                ProtectedRecord::CheckpointManifest(_) => None,
            })
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(ArtifactError::InvalidConfig(
                "record sequence allocation exhausted",
            ))?;
        let last_checkpoint_barrier = recovery
            .frames
            .iter()
            .filter_map(|frame| match &frame.record {
                ProtectedRecord::CheckpointManifest(manifest)
                    if manifest.origin_incarnation == workspace_incarnation =>
                {
                    Some(manifest.barrier_id)
                }
                _ => None,
            })
            .max()
            .unwrap_or(0);
        let mut committed_jobs = BTreeMap::new();
        for frame in &recovery.frames {
            let ProtectedRecord::Job(record) = &frame.record else {
                continue;
            };
            if record.workspace_incarnation == workspace_incarnation
                && !matches!(record.state, JobState::Queued | JobState::Running)
                && committed_jobs
                    .insert(record.job_id, record.clone())
                    .is_some()
            {
                return Err(integrity(
                    0,
                    "duplicate terminal artifact record for current job id",
                ));
            }
        }
        let token_namespace = Sha256Digest::compute(workspace_root.as_os_str().as_encoded_bytes());
        Ok(Self {
            workspace_root,
            token_namespace,
            repo_id,
            workspace_incarnation,
            budget: BufferBudget {
                used: 0,
                limit: config.supervisor_buffer_budget_bytes,
            },
            next_job_id,
            next_sequence,
            last_checkpoint_barrier,
            live_jobs: BTreeMap::new(),
            committed_jobs,
            recovery,
            config,
        })
    }

    pub fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    pub fn next_job_id(&self) -> Result<JobId, ArtifactError> {
        JobId::new(self.next_job_id).map_err(ArtifactError::from)
    }

    pub fn buffered_bytes(&self) -> usize {
        self.budget.used
    }

    pub fn begin_job(
        &mut self,
        job_id: JobId,
        grant_revision: u64,
        argv: &[CommandArg],
        mut targets: OutputTargets,
    ) -> Result<JobArtifactToken, ArtifactError> {
        validate_command_argv(argv)?;
        if job_id.get() != self.next_job_id {
            return Err(ArtifactError::TokenConflict {
                job_id,
                message: "job id is not the next expected artifact id",
            });
        }
        if self.live_jobs.contains_key(&job_id) || self.committed_jobs.contains_key(&job_id) {
            return Err(ArtifactError::TokenConflict {
                job_id,
                message: "job id already has artifact state",
            });
        }
        let following_job_id = self
            .next_job_id
            .checked_add(1)
            .ok_or(ArtifactError::InvalidConfig("job id allocation exhausted"))?;
        secure_redirect_target(&self.workspace_root, &mut targets.stdout)?;
        secure_redirect_target(&self.workspace_root, &mut targets.stderr)?;
        let admission = JobArtifactRecord {
            repo_id: self.repo_id.clone(),
            workspace_incarnation: self.workspace_incarnation.clone(),
            job_id,
            sequence: 0,
            state: JobState::Running,
            grant_revision,
            argv: argv.to_vec(),
            output_limit: None,
            stdout: empty_stream(&targets.stdout)?,
            stderr: empty_stream(&targets.stderr)?,
        };
        self.append_record(admission)?;
        let replaced = self.live_jobs.insert(
            job_id,
            LiveJobState {
                grant_revision,
                argv: argv.to_vec(),
                stdout: StreamWriterState::new(StreamKind::Stdout, targets.stdout),
                stderr: StreamWriterState::new(StreamKind::Stderr, targets.stderr),
                quota: QuotaLedger {
                    accepted: 0,
                    limit: self.config.combined_output_quota_bytes,
                    crossing: None,
                },
                failed: false,
            },
        );
        debug_assert!(replaced.is_none(), "validated job ids are unique");
        self.next_job_id = following_job_id;
        Ok(JobArtifactToken {
            job_id,
            namespace: self.token_namespace,
        })
    }

    fn append_record(
        &mut self,
        mut record: JobArtifactRecord,
    ) -> Result<(JobArtifactRecord, Sha256Digest), ArtifactError> {
        if !matches!(record.state, JobState::Queued | JobState::Running)
            && self.committed_jobs.contains_key(&record.job_id)
        {
            return Err(integrity(
                0,
                "duplicate terminal artifact record for job id",
            ));
        }
        record.sequence = self.next_sequence;
        record.validate()?;
        let batch = protected_record_to_batch(&ProtectedRecord::Job(record.clone()))?;
        let payload = encode_batch(&batch)?;
        let digest = Sha256Digest::compute(&payload);
        append_framed_batch(&records_path(&self.workspace_root), &payload, digest)?;
        self.next_sequence =
            self.next_sequence
                .checked_add(1)
                .ok_or(ArtifactError::InvalidConfig(
                    "record sequence allocation exhausted",
                ))?;
        if !matches!(record.state, JobState::Queued | JobState::Running) {
            self.committed_jobs.insert(record.job_id, record.clone());
        }
        Ok((record, digest))
    }

    pub fn checkpoint(
        &mut self,
        barrier_id: u64,
    ) -> Result<SealedCheckpointManifest, ArtifactError> {
        if barrier_id == 0 {
            return Err(integrity(0, "checkpoint barrier id must be positive"));
        }
        if barrier_id <= self.last_checkpoint_barrier {
            return Err(integrity(
                0,
                "checkpoint barrier id must increase monotonically",
            ));
        }
        let mut visible = self
            .committed_jobs
            .values()
            .map(VisibleJobCommitment::from_record)
            .collect::<BTreeMap<_, _>>();
        let workspace_root = &self.workspace_root;
        let workspace_incarnation = &self.workspace_incarnation;
        let budget = &mut self.budget;
        for (&job_id, state) in &mut self.live_jobs {
            let commitment =
                state.durable_commitment(workspace_root, budget, workspace_incarnation, job_id)?;
            visible.insert(job_id, commitment);
        }
        let path = records_path(&self.workspace_root);
        let records_sha256 = match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.len() == 0 => Sha256Digest::compute(RECORD_MAGIC),
            Ok(_) => hash_file_incrementally(&path)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Sha256Digest::compute(RECORD_MAGIC)
            }
            Err(error) => return Err(io_error(&path, error)),
        };
        let record = CheckpointManifestRecord {
            version: RECORD_SCHEMA_VERSION as u16,
            repo_id: self.repo_id.clone(),
            origin_incarnation: self.workspace_incarnation.clone(),
            barrier_id,
            visible_jobs: visible.into_values().collect(),
            records_sha256,
        };
        record.validate()?;
        let batch =
            protected_record_to_batch(&ProtectedRecord::CheckpointManifest(record.clone()))?;
        let payload = encode_batch(&batch)?;
        let manifest_batch_sha256 = Sha256Digest::compute(&payload);
        append_framed_batch(&path, &payload, manifest_batch_sha256)?;
        self.last_checkpoint_barrier = barrier_id;
        Ok(SealedCheckpointManifest {
            record,
            manifest_batch_sha256,
        })
    }

    pub fn records_prefix_sha256(&self) -> Result<Sha256Digest, ArtifactError> {
        hash_file_incrementally(&records_path(&self.workspace_root))
    }
}

fn empty_stream(target: &StreamTarget) -> Result<StreamInfo, ArtifactError> {
    let artifact = ProtectedOutput::Inline {
        data: BinaryData::new(Vec::new())?,
    };
    Ok(StreamInfo {
        storage: match target {
            StreamTarget::Captured => OutputStorage::Captured { artifact },
            StreamTarget::Redirect { source, .. } => OutputStorage::Redirect {
                source: source.clone(),
                artifact,
            },
        },
        bytes: 0,
        sha256: Sha256Digest::compute(&[]),
        summary: safe_output_summary(),
    })
}

fn safe_output_summary() -> OutputSummary {
    OutputSummary {
        version: 1,
        text: String::new(),
        truncated: false,
    }
}

#[cfg(test)]
static FAIL_REDIRECT_FCNTL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn secure_redirect_target(
    workspace_root: &Path,
    target: &mut StreamTarget,
) -> Result<(), ArtifactError> {
    let StreamTarget::Redirect { source, descriptor } = target else {
        return Ok(());
    };
    let path = workspace_root.join(source.as_path());
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        #[cfg(test)]
        let fail_fcntl = FAIL_REDIRECT_FCNTL.swap(false, std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fail_fcntl = false;
        let result = if fail_fcntl {
            -1
        } else {
            unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) }
        };
        if result == -1 {
            return Err(ArtifactError::RedirectDescriptor {
                path,
                message: io::Error::last_os_error().to_string(),
            });
        }
    }
    #[cfg(not(unix))]
    let _ = (path, descriptor);
    Ok(())
}

#[derive(Debug)]
struct QuotaLedger {
    accepted: u64,
    limit: u64,
    crossing: Option<u64>,
}

impl QuotaLedger {
    fn preview(&self, requested: usize) -> QuotaAdmission {
        if let Some(crossing) = self.crossing {
            return QuotaAdmission {
                accepted: 0,
                total: self.accepted,
                crossing: Some(crossing),
            };
        }
        let requested_u64 = u64::try_from(requested).unwrap_or(u64::MAX);
        let observed = self.accepted.saturating_add(requested_u64);
        let accepted = if observed <= self.limit {
            requested
        } else {
            usize::try_from(self.limit.saturating_sub(self.accepted))
                .unwrap_or(usize::MAX)
                .min(requested)
        };
        QuotaAdmission {
            accepted,
            total: self.accepted.saturating_add(accepted as u64),
            crossing: (observed > self.limit).then_some(observed),
        }
    }

    fn commit(&mut self, admission: QuotaAdmission) {
        self.accepted = admission.total;
        self.crossing = admission.crossing;
    }

    fn output_limit(&self) -> Option<OutputLimitInfo> {
        self.crossing.map(|crossing_bytes| OutputLimitInfo {
            limit_bytes: self.limit,
            crossing_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct QuotaAdmission {
    accepted: usize,
    total: u64,
    crossing: Option<u64>,
}

struct LiveJobState {
    grant_revision: u64,
    argv: Vec<CommandArg>,
    stdout: StreamWriterState,
    stderr: StreamWriterState,
    quota: QuotaLedger,
    failed: bool,
}

impl LiveJobState {
    fn durable_commitment(
        &mut self,
        workspace_root: &Path,
        budget: &mut BufferBudget,
        workspace_incarnation: &WorkspaceIncarnation,
        job_id: JobId,
    ) -> Result<VisibleJobCommitment, ArtifactError> {
        if self.failed {
            return Err(ArtifactError::TokenConflict {
                job_id,
                message: "job artifact stream failed; abort is required",
            });
        }
        let stdout = self.stdout.durable_prefix(workspace_root, budget, job_id)?;
        let stderr = self.stderr.durable_prefix(workspace_root, budget, job_id)?;
        Ok(VisibleJobCommitment {
            workspace_incarnation: workspace_incarnation.clone(),
            job_id,
            state: JobState::Running,
            stdout: VisibleStreamCommitment::from_stream(&stdout),
            stderr: VisibleStreamCommitment::from_stream(&stderr),
        })
    }

    fn release_budget(&mut self, budget: &mut BufferBudget) {
        self.stdout.release_reserved(budget);
        self.stderr.release_reserved(budget);
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct JobArtifactToken {
    job_id: JobId,
    namespace: Sha256Digest,
}

impl JobArtifactToken {
    pub fn job_id(&self) -> JobId {
        self.job_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendOutcome {
    pub accepted_bytes: usize,
    pub output_limit: Option<OutputLimitInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedJobArtifacts {
    pub record: JobArtifactRecord,
    pub terminal_batch_sha256: Sha256Digest,
    pub output_limit: Option<OutputLimitInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedJobArtifacts {
    pub sealed: SealedJobArtifacts,
    pub stdout_publication: Option<Result<(), ArtifactError>>,
    pub stderr_publication: Option<Result<(), ArtifactError>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedCheckpointManifest {
    pub record: CheckpointManifestRecord,
    pub manifest_batch_sha256: Sha256Digest,
}

impl ArtifactStore {
    fn validate_token(&self, token: &JobArtifactToken) -> Result<(), ArtifactError> {
        if token.namespace != self.token_namespace {
            return Err(ArtifactError::TokenConflict {
                job_id: token.job_id,
                message: "token belongs to another artifact store",
            });
        }
        Ok(())
    }

    pub fn append(
        &mut self,
        token: &JobArtifactToken,
        stream: StreamKind,
        bytes: &[u8],
    ) -> Result<AppendOutcome, ArtifactError> {
        self.validate_token(token)?;
        let workspace_root = &self.workspace_root;
        let inline_cap_bytes = self.config.inline_cap_bytes;
        let budget = &mut self.budget;
        let live = self
            .live_jobs
            .get_mut(&token.job_id)
            .ok_or(ArtifactError::TokenConflict {
                job_id: token.job_id,
                message: "token does not name a live job",
            })?;
        if live.failed {
            return Err(ArtifactError::TokenConflict {
                job_id: token.job_id,
                message: "job artifact stream failed; abort is required",
            });
        }
        let admission = live.quota.preview(bytes.len());
        if admission.accepted != 0 {
            let writer = match stream {
                StreamKind::Stdout => &mut live.stdout,
                StreamKind::Stderr => &mut live.stderr,
            };
            if let Err(error) = writer.append(
                workspace_root,
                inline_cap_bytes,
                budget,
                token.job_id,
                &bytes[..admission.accepted],
            ) {
                live.failed = true;
                return Err(error);
            }
        }
        live.quota.commit(admission);
        Ok(AppendOutcome {
            accepted_bytes: admission.accepted,
            output_limit: live.quota.output_limit(),
        })
    }

    pub fn output_limit(
        &self,
        token: &JobArtifactToken,
    ) -> Result<Option<OutputLimitInfo>, ArtifactError> {
        self.validate_token(token)?;
        let live = self
            .live_jobs
            .get(&token.job_id)
            .ok_or(ArtifactError::TokenConflict {
                job_id: token.job_id,
                message: "token does not name a live job",
            })?;
        Ok(live.quota.output_limit())
    }

    /// Makes both captured prefixes durable before the supervisor acknowledges backgrounding.
    pub fn prepare_background(&mut self, token: &JobArtifactToken) -> Result<(), ArtifactError> {
        self.validate_token(token)?;
        let workspace_root = &self.workspace_root;
        let budget = &mut self.budget;
        let live = self
            .live_jobs
            .get_mut(&token.job_id)
            .ok_or(ArtifactError::TokenConflict {
                job_id: token.job_id,
                message: "token does not name a live job",
            })?;
        if live.failed {
            return Err(ArtifactError::TokenConflict {
                job_id: token.job_id,
                message: "job artifact stream failed; abort is required",
            });
        }
        live.stdout
            .durable_prefix(workspace_root, budget, token.job_id)?;
        live.stderr
            .durable_prefix(workspace_root, budget, token.job_id)?;
        Ok(())
    }

    pub fn finish(
        &mut self,
        token: JobArtifactToken,
        state: JobState,
    ) -> Result<SealedJobArtifacts, ArtifactError> {
        self.validate_token(&token)?;
        let mut live =
            self.live_jobs
                .remove(&token.job_id)
                .ok_or(ArtifactError::TokenConflict {
                    job_id: token.job_id,
                    message: "token does not name a live job",
                })?;
        let output_limit = live.quota.output_limit();
        let result = (|| {
            if live.failed {
                return Err(ArtifactError::TokenConflict {
                    job_id: token.job_id,
                    message: "job artifact stream failed; abort was required",
                });
            }
            if matches!(state, JobState::Queued | JobState::Running) {
                return Err(ArtifactError::InvalidTerminalState(
                    "finish requires a terminal job state",
                ));
            }
            if live.quota.crossing.is_some() != matches!(state, JobState::OutputLimit) {
                return Err(ArtifactError::InvalidTerminalState(
                    "output-limit state must agree with quota crossing",
                ));
            }
            let stdout =
                live.stdout
                    .finish(&self.workspace_root, &mut self.budget, token.job_id)?;
            let stderr =
                live.stderr
                    .finish(&self.workspace_root, &mut self.budget, token.job_id)?;
            let record = JobArtifactRecord {
                repo_id: self.repo_id.clone(),
                workspace_incarnation: self.workspace_incarnation.clone(),
                job_id: token.job_id,
                sequence: 0,
                state,
                grant_revision: live.grant_revision,
                argv: live.argv.clone(),
                output_limit: output_limit.clone(),
                stdout,
                stderr,
            };
            let (record, terminal_batch_sha256) = self.append_record(record)?;
            Ok(SealedJobArtifacts {
                record,
                terminal_batch_sha256,
                output_limit,
            })
        })();
        if result.is_err() {
            live.release_budget(&mut self.budget);
        }
        result
    }

    pub fn abort(&mut self, token: JobArtifactToken) -> Result<(), ArtifactError> {
        self.validate_token(&token)?;
        let mut live =
            self.live_jobs
                .remove(&token.job_id)
                .ok_or(ArtifactError::TokenConflict {
                    job_id: token.job_id,
                    message: "token does not name a live job",
                })?;
        live.release_budget(&mut self.budget);
        Ok(())
    }

    /// Establishes the durable terminal record before independently attempting publications.
    pub fn finish_and_publish(
        &mut self,
        token: JobArtifactToken,
        state: JobState,
        stdout: Option<OutputPublication>,
        stderr: Option<OutputPublication>,
    ) -> Result<CompletedJobArtifacts, ArtifactError> {
        let sealed = self.finish(token, state)?;
        let job_id = sealed.record.job_id;
        let stdout_publication = stdout
            .as_ref()
            .map(|request| self.publish_output(job_id, StreamKind::Stdout, request));
        let stderr_publication = stderr
            .as_ref()
            .map(|request| self.publish_output(job_id, StreamKind::Stderr, request));
        Ok(CompletedJobArtifacts {
            sealed,
            stdout_publication,
            stderr_publication,
        })
    }

    pub fn publish_output(
        &mut self,
        job_id: JobId,
        stream: StreamKind,
        publication: &OutputPublication,
    ) -> Result<(), ArtifactError> {
        let record =
            self.committed_jobs
                .get(&job_id)
                .ok_or_else(|| ArtifactError::Publication {
                    path: publication.path.as_path().to_owned(),
                    stage: PublicationStage::ValidateDestination,
                    message: "job has no durable terminal artifact record".into(),
                })?;
        let info = match stream {
            StreamKind::Stdout => &record.stdout,
            StreamKind::Stderr => &record.stderr,
        };
        publication::publish(&self.workspace_root, info, publication)
    }
}

struct StreamWriterState {
    kind: StreamKind,
    target: StreamTarget,
    buffer: Option<Vec<u8>>,
    reserved: usize,
    protected_file: Option<File>,
    bytes: u64,
    hasher: Sha256,
}

impl StreamWriterState {
    fn new(kind: StreamKind, target: StreamTarget) -> Self {
        Self {
            kind,
            target,
            buffer: Some(Vec::new()),
            reserved: 0,
            protected_file: None,
            bytes: 0,
            hasher: Sha256::new(),
        }
    }

    fn append(
        &mut self,
        workspace_root: &Path,
        inline_cap_bytes: usize,
        budget: &mut BufferBudget,
        job_id: JobId,
        bytes: &[u8],
    ) -> Result<(), ArtifactError> {
        if self.buffer.is_some() {
            if !self.try_buffer_append(inline_cap_bytes, budget, bytes)? {
                self.transition_from_buffer(workspace_root, budget, job_id)?;
                let path = protected_absolute(workspace_root, job_id, self.kind);
                self.protected_file
                    .as_mut()
                    .ok_or_else(|| {
                        ArtifactError::WriteIntegrity(
                            "spill transition has no canonical file".into(),
                        )
                    })?
                    .write_all(bytes)
                    .map_err(|error| io_error(&path, error))?;
            }
        } else {
            let path = protected_absolute(workspace_root, job_id, self.kind);
            self.protected_file
                .as_mut()
                .expect("spill file exists")
                .write_all(bytes)
                .map_err(|error| io_error(&path, error))?;
        }
        if let StreamTarget::Redirect { source, descriptor } = &mut self.target {
            let path = workspace_root.join(source.as_path());
            descriptor
                .write_all(bytes)
                .map_err(|error| io_error(&path, error))?;
        }
        self.bytes = self.bytes.checked_add(bytes.len() as u64).ok_or(
            ArtifactError::InvalidTerminalState("stream byte count overflow"),
        )?;
        self.hasher.update(bytes);
        Ok(())
    }

    fn try_buffer_append(
        &mut self,
        inline_cap_bytes: usize,
        budget: &mut BufferBudget,
        bytes: &[u8],
    ) -> Result<bool, ArtifactError> {
        let buffer = self.buffer.as_mut().expect("buffer state was checked");
        let Some(desired_len) = buffer.len().checked_add(bytes.len()) else {
            return Ok(false);
        };
        if desired_len > inline_cap_bytes {
            return Ok(false);
        }
        let old_capacity = buffer.capacity();
        let reserved_growth = desired_len.saturating_sub(old_capacity);
        if !budget.try_reserve(reserved_growth) {
            return Ok(false);
        }
        if buffer
            .try_reserve_exact(desired_len - buffer.len())
            .is_err()
        {
            budget.release(reserved_growth);
            return Err(ArtifactError::BufferAllocation);
        }
        let actual_growth = buffer.capacity().saturating_sub(old_capacity);
        if !reconcile_capacity_growth(budget, reserved_growth, actual_growth) {
            budget.release(reserved_growth);
            return Ok(false);
        }
        let Some(new_reserved) = self.reserved.checked_add(actual_growth) else {
            budget.release(actual_growth);
            return Err(ArtifactError::BufferAllocation);
        };
        self.reserved = new_reserved;
        buffer.extend_from_slice(bytes);
        Ok(true)
    }

    fn transition_from_buffer(
        &mut self,
        workspace_root: &Path,
        budget: &mut BufferBudget,
        job_id: JobId,
    ) -> Result<(), ArtifactError> {
        let buffered = self.buffer.as_ref().expect("transition requires a buffer");
        let path = protected_absolute(workspace_root, job_id, self.kind);
        let mut file = create_protected_file(workspace_root, job_id, self.kind)?;
        if let Err(error) = file.write_all(buffered) {
            self.protected_file = Some(file);
            return Err(io_error(&path, error));
        }
        self.protected_file = Some(file);
        self.buffer = None;
        self.release_reserved(budget);
        Ok(())
    }

    fn durable_prefix(
        &mut self,
        workspace_root: &Path,
        budget: &mut BufferBudget,
        job_id: JobId,
    ) -> Result<StreamInfo, ArtifactError> {
        if self.buffer.is_some() {
            self.transition_from_buffer(workspace_root, budget, job_id)?;
        }
        let path = protected_absolute(workspace_root, job_id, self.kind);
        let file = self.protected_file.as_mut().ok_or_else(|| {
            ArtifactError::WriteIntegrity("durable prefix has no canonical file".into())
        })?;
        file.flush().map_err(|error| io_error(&path, error))?;
        set_sealed_permissions(file, &path)?;
        file.sync_all().map_err(|error| io_error(&path, error))?;
        verify_private_file_mode(
            &path,
            &file.metadata().map_err(|error| io_error(&path, error))?,
            true,
        )?;
        let artifact = ProtectedOutput::File {
            path: protected_relative(job_id, self.kind)?,
        };
        let storage = match &self.target {
            StreamTarget::Captured => OutputStorage::Captured { artifact },
            StreamTarget::Redirect { source, .. } => OutputStorage::Redirect {
                source: source.clone(),
                artifact,
            },
        };
        Ok(StreamInfo {
            storage,
            bytes: self.bytes,
            sha256: Sha256Digest::from_bytes(self.hasher.clone().finalize().into()),
            summary: safe_output_summary(),
        })
    }

    fn finish(
        &mut self,
        workspace_root: &Path,
        budget: &mut BufferBudget,
        job_id: JobId,
    ) -> Result<StreamInfo, ArtifactError> {
        let digest = Sha256Digest::from_bytes(self.hasher.clone().finalize().into());
        let target = std::mem::replace(&mut self.target, StreamTarget::Captured);
        if let StreamTarget::Redirect { source, descriptor } = &target {
            let source_absolute = workspace_root.join(source.as_path());
            descriptor
                .sync_all()
                .map_err(|error| io_error(&source_absolute, error))?;
        }
        let artifact = if let Some(buffer) = self.buffer.take() {
            self.release_reserved(budget);
            ProtectedOutput::Inline {
                data: BinaryData::new(buffer)?,
            }
        } else {
            let path = protected_relative(job_id, self.kind)?;
            seal_file(
                self.protected_file
                    .take()
                    .expect("spilled stream has a canonical file"),
                &protected_absolute(workspace_root, job_id, self.kind),
            )?;
            verify_file_content(workspace_root, &path, self.bytes, digest)?;
            ProtectedOutput::File { path }
        };
        let storage = match target {
            StreamTarget::Captured => OutputStorage::Captured { artifact },
            StreamTarget::Redirect { source, .. } => OutputStorage::Redirect { source, artifact },
        };
        let info = StreamInfo {
            storage,
            bytes: self.bytes,
            sha256: digest,
            summary: safe_output_summary(),
        };
        info.validate()?;
        Ok(info)
    }

    fn release_reserved(&mut self, budget: &mut BufferBudget) {
        if self.reserved != 0 {
            budget.release(self.reserved);
            self.reserved = 0;
        }
    }
}

fn protected_relative(job_id: JobId, stream: StreamKind) -> Result<WorkspacePath, ArtifactError> {
    Ok(WorkspacePath::new(format!(
        ".cowshed/job/{}/{}",
        job_id.get(),
        stream.leaf()
    ))?)
}

fn protected_absolute(workspace_root: &Path, job_id: JobId, stream: StreamKind) -> PathBuf {
    workspace_root
        .join(".cowshed")
        .join("job")
        .join(job_id.get().to_string())
        .join(stream.leaf())
}

fn create_protected_file(
    workspace_root: &Path,
    job_id: JobId,
    stream: StreamKind,
) -> Result<File, ArtifactError> {
    let path = protected_absolute(workspace_root, job_id, stream);
    verify_no_symlinks(workspace_root, &path).map_err(|error| ArtifactError::Integrity {
        offset: 0,
        message: error.to_string(),
    })?;
    let job_root = ensure_private_job_root(workspace_root)?;
    let parent = job_root.join(job_id.get().to_string());
    ensure_private_directory(&parent)?;
    verify_no_symlinks(workspace_root, &path).map_err(|error| ArtifactError::Integrity {
        offset: 0,
        message: error.to_string(),
    })?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&path)
        .map_err(|error| io_error(&path, error))?;
    reject_hardlink(
        &path,
        &file.metadata().map_err(|error| io_error(&path, error))?,
    )?;
    verify_private_file_mode(
        &path,
        &file.metadata().map_err(|error| io_error(&path, error))?,
        false,
    )?;
    sync_parent_directory(&path)?;
    Ok(file)
}

fn set_sealed_permissions(file: &File, path: &Path) -> Result<(), ArtifactError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o400))
            .map_err(|error| io_error(path, error))?;
    }
    #[cfg(not(unix))]
    {
        let mut permissions = file
            .metadata()
            .map_err(|error| io_error(path, error))?
            .permissions();
        permissions.set_readonly(true);
        file.set_permissions(permissions)
            .map_err(|error| io_error(path, error))?;
    }
    Ok(())
}

fn seal_file(mut file: File, path: &Path) -> Result<(), ArtifactError> {
    file.flush().map_err(|error| io_error(path, error))?;
    set_sealed_permissions(&file, path)?;
    file.sync_all().map_err(|error| io_error(path, error))?;
    verify_private_file_mode(
        path,
        &file.metadata().map_err(|error| io_error(path, error))?,
        true,
    )?;
    Ok(())
}

fn verify_file_content(
    workspace_root: &Path,
    relative: &WorkspacePath,
    expected_bytes: u64,
    expected_digest: Sha256Digest,
) -> Result<(), ArtifactError> {
    read_file_verified(
        workspace_root,
        relative,
        expected_bytes,
        expected_digest,
        false,
    )
    .map(|_| ())
}

fn read_file_verified(
    workspace_root: &Path,
    relative: &WorkspacePath,
    expected_bytes: u64,
    expected_digest: Sha256Digest,
    collect: bool,
) -> Result<Vec<u8>, ArtifactError> {
    let path = workspace_root.join(relative.as_path());
    let mut file = open_artifact_no_follow(workspace_root, relative)?;
    let metadata = file.metadata().map_err(|error| io_error(&path, error))?;
    if !metadata.is_file() || !metadata.permissions().readonly() {
        return Err(ArtifactError::Integrity {
            offset: 0,
            message: format!("protected artifact {} is not sealed", path.display()),
        });
    }
    reject_hardlink(&path, &metadata)?;
    verify_private_file_mode(&path, &metadata, true)?;
    let mut hasher = Sha256::new();
    let mut observed = 0_u64;
    let mut output = Vec::new();
    let mut buffer = [0_u8; IO_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error(&path, error))?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(read as u64)
            .ok_or_else(|| integrity(0, "protected artifact byte count overflow"))?;
        if observed > expected_bytes {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: format!(
                    "sealed artifact {} exceeds its committed byte count",
                    path.display()
                ),
            });
        }
        hasher.update(&buffer[..read]);
        if collect {
            output
                .try_reserve(read)
                .map_err(|_| ArtifactError::BufferAllocation)?;
            output.extend_from_slice(&buffer[..read]);
        }
    }
    let observed_digest = Sha256Digest::from_bytes(hasher.finalize().into());
    if observed != expected_bytes || observed_digest != expected_digest {
        return Err(ArtifactError::Integrity {
            offset: 0,
            message: format!(
                "sealed artifact {} does not match committed bytes and digest",
                path.display()
            ),
        });
    }
    Ok(output)
}

fn open_artifact_no_follow(
    workspace_root: &Path,
    relative: &WorkspacePath,
) -> Result<File, ArtifactError> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use std::os::unix::ffi::OsStrExt;

        let absolute = workspace_root.join(relative.as_path());
        let root_name = CString::new(workspace_root.as_os_str().as_bytes())
            .map_err(|_| integrity(0, "workspace root contains a NUL byte"))?;
        let root_fd = unsafe { libc::open(root_name.as_ptr(), SECURE_DIRECTORY_OPEN_FLAGS) };
        if root_fd == -1 {
            return Err(io_error(workspace_root, io::Error::last_os_error()));
        }
        let mut directory = unsafe { OwnedFd::from_raw_fd(root_fd) };
        let components = relative.as_path().components().collect::<Vec<_>>();
        if components.is_empty() {
            return Err(integrity(0, "protected artifact path is empty"));
        }
        for (index, component) in components.iter().enumerate() {
            let std::path::Component::Normal(name) = component else {
                return Err(integrity(0, "protected artifact path is not canonical"));
            };
            let name = CString::new(name.as_bytes())
                .map_err(|_| integrity(0, "protected artifact path contains a NUL byte"))?;
            let last = index + 1 == components.len();
            let flags = if last {
                SECURE_FILE_OPEN_FLAGS
            } else {
                SECURE_DIRECTORY_OPEN_FLAGS
            };
            let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
            if fd == -1 {
                return Err(io_error(&absolute, io::Error::last_os_error()));
            }
            let opened = unsafe { OwnedFd::from_raw_fd(fd) };
            if last {
                return Ok(File::from(opened));
            }
            directory = opened;
        }
        unreachable!("non-empty protected artifact path returns its final component")
    }

    #[cfg(not(unix))]
    {
        let path = workspace_root.join(relative.as_path());
        verify_no_symlinks(workspace_root, &path)
            .map_err(|error| integrity(0, &error.to_string()))?;
        OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|error| io_error(&path, error))
    }
}

fn hash_file_incrementally(path: &Path) -> Result<Sha256Digest, ArtifactError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|error| io_error(path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; IO_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error(path, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(Sha256Digest::from_bytes(hasher.finalize().into()))
}

enum VerifiedStreamSource<'a> {
    Inline { bytes: &'a [u8], offset: usize },
    File(File),
}

pub struct VerifiedStreamReader<'a> {
    path: PathBuf,
    source: VerifiedStreamSource<'a>,
    expected_bytes: u64,
    expected_digest: Sha256Digest,
    observed_bytes: u64,
    hasher: Sha256,
    verified: bool,
}

impl VerifiedStreamReader<'_> {
    pub fn read_chunk(&mut self, output: &mut [u8]) -> Result<usize, ArtifactError> {
        if output.is_empty() {
            return Ok(0);
        }
        let read = match &mut self.source {
            VerifiedStreamSource::Inline { bytes, offset } => {
                let available = &bytes[*offset..];
                let read = available.len().min(output.len());
                output[..read].copy_from_slice(&available[..read]);
                *offset += read;
                read
            }
            VerifiedStreamSource::File(file) => file
                .read(output)
                .map_err(|error| io_error(&self.path, error))?,
        };
        if read == 0 {
            let digest = Sha256Digest::from_bytes(self.hasher.clone().finalize().into());
            if self.observed_bytes != self.expected_bytes || digest != self.expected_digest {
                return Err(integrity(
                    0,
                    "artifact stream does not match committed bytes and digest",
                ));
            }
            self.verified = true;
            return Ok(0);
        }
        self.observed_bytes = self
            .observed_bytes
            .checked_add(read as u64)
            .ok_or_else(|| integrity(0, "artifact stream byte count overflow"))?;
        if self.observed_bytes > self.expected_bytes {
            return Err(integrity(
                0,
                "artifact stream exceeds its committed byte count",
            ));
        }
        self.hasher.update(&output[..read]);
        Ok(read)
    }

    pub fn finish(mut self) -> Result<(), ArtifactError> {
        let mut buffer = [0_u8; IO_BUFFER_BYTES];
        while self.read_chunk(&mut buffer)? != 0 {}
        Ok(())
    }

    pub fn is_verified(&self) -> bool {
        self.verified
    }
}

pub fn open_stream_reader<'a>(
    workspace_root: &Path,
    stream: &'a StreamInfo,
) -> Result<VerifiedStreamReader<'a>, ArtifactError> {
    stream.validate()?;
    let (path, source) = match stream.storage.artifact() {
        ProtectedOutput::Inline { data } => {
            if data.as_bytes().len() as u64 != stream.bytes
                || Sha256Digest::compute(data.as_bytes()) != stream.sha256
            {
                return Err(integrity(
                    0,
                    "inline artifact does not match committed bytes and digest",
                ));
            }
            (
                PathBuf::from("<inline>"),
                VerifiedStreamSource::Inline {
                    bytes: data.as_bytes(),
                    offset: 0,
                },
            )
        }
        ProtectedOutput::File { path } => {
            let absolute = workspace_root.join(path.as_path());
            let file = open_artifact_no_follow(workspace_root, path)?;
            let metadata = file
                .metadata()
                .map_err(|error| io_error(&absolute, error))?;
            if !metadata.is_file() || !metadata.permissions().readonly() {
                return Err(integrity(
                    0,
                    "protected artifact is not a sealed regular file",
                ));
            }
            reject_hardlink(&absolute, &metadata)?;
            verify_private_file_mode(&absolute, &metadata, true)?;
            if metadata.len() != stream.bytes {
                return Err(integrity(
                    0,
                    "protected artifact length differs from its commitment",
                ));
            }
            (absolute, VerifiedStreamSource::File(file))
        }
    };
    Ok(VerifiedStreamReader {
        path,
        source,
        expected_bytes: stream.bytes,
        expected_digest: stream.sha256,
        observed_bytes: 0,
        hasher: Sha256::new(),
        verified: false,
    })
}

pub fn read_stream(workspace_root: &Path, stream: &StreamInfo) -> Result<Vec<u8>, ArtifactError> {
    let limit = MAX_INLINE_OUTPUT_BYTES as u64;
    if stream.bytes > limit {
        return Err(ArtifactError::StreamTooLarge {
            limit_bytes: limit,
            bytes: stream.bytes,
        });
    }
    let capacity = usize::try_from(stream.bytes).map_err(|_| ArtifactError::StreamTooLarge {
        limit_bytes: limit,
        bytes: stream.bytes,
    })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| ArtifactError::BufferAllocation)?;
    let mut reader = open_stream_reader(workspace_root, stream)?;
    let mut buffer = [0_u8; IO_BUFFER_BYTES];
    loop {
        let read = reader.read_chunk(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
}

fn validate_recovered_files(
    workspace_root: &Path,
    current_incarnation: &WorkspaceIncarnation,
    recovery: &RecoveryReport,
) -> Result<(), ArtifactError> {
    let mut terminal_jobs = BTreeSet::new();
    let mut latest_manifest = None;
    for frame in &recovery.frames {
        match &frame.record {
            ProtectedRecord::Job(record)
                if &record.workspace_incarnation == current_incarnation
                    && !matches!(record.state, JobState::Queued | JobState::Running) =>
            {
                if !terminal_jobs.insert(record.job_id) {
                    return Err(integrity(
                        0,
                        "duplicate terminal artifact record for current job id",
                    ));
                }
                validate_stream_file(workspace_root, &record.stdout)?;
                validate_stream_file(workspace_root, &record.stderr)?;
            }
            ProtectedRecord::CheckpointManifest(manifest)
                if &manifest.origin_incarnation == current_incarnation =>
            {
                latest_manifest = Some(manifest);
            }
            _ => {}
        }
    }
    if let Some(manifest) = latest_manifest {
        for job in &manifest.visible_jobs {
            if &job.workspace_incarnation != current_incarnation
                || terminal_jobs.contains(&job.job_id)
            {
                continue;
            }
            validate_visible_file(workspace_root, &job.stdout)?;
            validate_visible_file(workspace_root, &job.stderr)?;
        }
    }
    Ok(())
}

fn validate_stream_file(workspace_root: &Path, stream: &StreamInfo) -> Result<(), ArtifactError> {
    if let ProtectedOutput::File { path } = stream.storage.artifact() {
        verify_file_content(workspace_root, path, stream.bytes, stream.sha256)?;
    }
    Ok(())
}

fn validate_visible_file(
    workspace_root: &Path,
    stream: &VisibleStreamCommitment,
) -> Result<(), ArtifactError> {
    if let Some(path) = &stream.protected_path {
        verify_file_content(workspace_root, path, stream.bytes, stream.sha256)?;
    }
    Ok(())
}
#[cfg(unix)]
fn reject_hardlink(path: &Path, metadata: &fs::Metadata) -> Result<(), ArtifactError> {
    use std::os::unix::fs::MetadataExt;
    if metadata.nlink() != 1 {
        return Err(ArtifactError::Integrity {
            offset: 0,
            message: format!(
                "protected artifact {} has {} hardlink aliases",
                path.display(),
                metadata.nlink()
            ),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn reject_hardlink(_path: &Path, _metadata: &fs::Metadata) -> Result<(), ArtifactError> {
    Ok(())
}

fn records_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(".cowshed/job/records.arrow")
}
fn verify_records_layout(path: &Path) -> Result<(), ArtifactError> {
    let job_root = path
        .parent()
        .ok_or_else(|| integrity(0, "records path has no job parent"))?;
    let cowshed_root = job_root
        .parent()
        .ok_or_else(|| integrity(0, "records path has no .cowshed parent"))?;
    let workspace_root = cowshed_root
        .parent()
        .ok_or_else(|| integrity(0, "records path has no workspace parent"))?;
    if path.file_name().and_then(|name| name.to_str()) != Some("records.arrow")
        || job_root.file_name().and_then(|name| name.to_str()) != Some("job")
        || cowshed_root.file_name().and_then(|name| name.to_str()) != Some(".cowshed")
    {
        return Err(integrity(0, "records path is not canonical"));
    }
    verify_no_symlinks(workspace_root, path).map_err(|error| ArtifactError::Integrity {
        offset: 0,
        message: error.to_string(),
    })?;
    if job_root.exists() {
        verify_private_directory(job_root)?;
    }
    Ok(())
}
fn ensure_private_directory(path: &Path) -> Result<(), ArtifactError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(integrity(0, "protected directory is not a real directory"));
            }
            verify_private_directory(path)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or_else(|| integrity(0, "protected directory has no parent"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                builder
                    .create(path)
                    .map_err(|error| io_error(path, error))?;
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                    .map_err(|error| io_error(path, error))?;
            }
            #[cfg(not(unix))]
            fs::create_dir(path).map_err(|error| io_error(path, error))?;
            sync_directory(parent)?;
            Ok(())
        }
        Err(error) => Err(io_error(path, error)),
    }
}

fn ensure_private_job_root(workspace_root: &Path) -> Result<PathBuf, ArtifactError> {
    let cowshed = workspace_root.join(".cowshed");
    if !cowshed.exists() {
        ensure_private_directory(&cowshed)?;
    } else {
        let metadata = fs::symlink_metadata(&cowshed).map_err(|error| io_error(&cowshed, error))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(integrity(0, ".cowshed is not a real directory"));
        }
    }
    let job = cowshed.join("job");
    ensure_private_directory(&job)?;
    Ok(job)
}

fn verify_private_directory(path: &Path) -> Result<(), ArtifactError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::symlink_metadata(path)
            .map_err(|error| io_error(path, error))?
            .permissions()
            .mode()
            & 0o777;
        if mode != 0o700 {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: format!("protected directory {} has mode {mode:o}", path.display()),
            });
        }
    }
    Ok(())
}

fn verify_private_file_mode(
    path: &Path,
    metadata: &fs::Metadata,
    sealed: bool,
) -> Result<(), ArtifactError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let expected = if sealed { 0o400 } else { 0o600 };
        let mode = metadata.permissions().mode() & 0o777;
        if mode != expected {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: format!(
                    "protected file {} has mode {mode:o}, expected {expected:o}",
                    path.display()
                ),
            });
        }
    }
    Ok(())
}

fn sync_parent_directory(path: &Path) -> Result<(), ArtifactError> {
    let parent = path
        .parent()
        .ok_or_else(|| integrity(0, "protected path has no parent"))?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), ArtifactError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC);
    }
    let directory = options.open(path).map_err(|error| io_error(path, error))?;
    directory.sync_all().map_err(|error| io_error(path, error))
}

fn scan_job_directories(workspace_root: &Path) -> Result<u64, ArtifactError> {
    let root = workspace_root.join(".cowshed/job");
    verify_no_symlinks(workspace_root, &root).map_err(|error| ArtifactError::Integrity {
        offset: 0,
        message: error.to_string(),
    })?;
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(io_error(&root, error)),
    };
    verify_private_directory(&root)?;
    let mut maximum = 0;
    for entry in entries {
        let entry = entry.map_err(|error| io_error(&root, error))?;
        let file_type = entry
            .file_type()
            .map_err(|error| io_error(&entry.path(), error))?;
        if !file_type.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if name == "0" || name.starts_with('0') || !name.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        if let Ok(value) = name.parse::<u64>()
            && JobId::new(value).is_ok()
        {
            verify_private_directory(&entry.path())?;
            maximum = maximum.max(value);
        }
    }
    Ok(maximum)
}

fn append_framed_batch(
    path: &Path,
    payload: &[u8],
    digest: Sha256Digest,
) -> Result<(), ArtifactError> {
    append_framed_batch_impl(path, payload, digest, None)
}

fn append_framed_batch_impl(
    path: &Path,
    payload: &[u8],
    digest: Sha256Digest,
    mut fail_after_bytes: Option<usize>,
) -> Result<(), ArtifactError> {
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| ArtifactError::Arrow("record batch is too large".into()))?;
    if payload_len > MAX_RECORD_BATCH_BYTES {
        return Err(ArtifactError::Arrow("record batch is too large".into()));
    }
    verify_records_layout(path)?;
    let parent = path.parent().expect("records file has a parent");
    let workspace_root = parent
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| integrity(0, "records path has no workspace root"))?;
    ensure_private_job_root(workspace_root)?;
    verify_records_layout(path)?;
    let existing = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(integrity(0, "records path is not a regular protected file"));
            }
            reject_hardlink(path, &metadata)?;
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(io_error(path, error)),
    };
    let mut options = OpenOptions::new();
    options.create(true).append(true).read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(|error| io_error(path, error))?;
    if !existing {
        sync_parent_directory(path)?;
    }
    let metadata = file.metadata().map_err(|error| io_error(path, error))?;
    reject_hardlink(path, &metadata)?;
    verify_private_file_mode(path, &metadata, false)?;
    let original_len = metadata.len();
    let write_result = (|| -> io::Result<()> {
        if !existing || original_len == 0 {
            write_append_bytes(&mut file, RECORD_MAGIC, &mut fail_after_bytes)?;
        }
        write_append_bytes(&mut file, BATCH_MAGIC, &mut fail_after_bytes)?;
        write_append_bytes(&mut file, &payload_len.to_le_bytes(), &mut fail_after_bytes)?;
        write_append_bytes(
            &mut file,
            &(!payload_len).to_le_bytes(),
            &mut fail_after_bytes,
        )?;
        write_append_bytes(&mut file, payload, &mut fail_after_bytes)?;
        write_append_bytes(&mut file, digest.as_bytes(), &mut fail_after_bytes)?;
        write_append_bytes(&mut file, BATCH_TRAILER, &mut fail_after_bytes)?;
        file.flush()?;
        file.sync_data()
    })();
    if let Err(error) = write_result {
        return Err(rollback_append(&mut file, path, original_len, error));
    }
    Ok(())
}

fn write_append_bytes(
    file: &mut File,
    bytes: &[u8],
    fail_after_bytes: &mut Option<usize>,
) -> io::Result<()> {
    if let Some(remaining) = fail_after_bytes {
        if *remaining < bytes.len() {
            file.write_all(&bytes[..*remaining])?;
            *remaining = 0;
            return Err(io::Error::other("injected records append failure"));
        }
        *remaining -= bytes.len();
    }
    file.write_all(bytes)
}

fn rollback_append(
    file: &mut File,
    path: &Path,
    original_len: u64,
    primary: io::Error,
) -> ArtifactError {
    let rollback = file
        .set_len(original_len)
        .and_then(|_| file.seek(SeekFrom::Start(original_len)).map(|_| ()))
        .and_then(|_| file.sync_data());
    match rollback {
        Ok(()) => io_error(path, primary),
        Err(rollback) => ArtifactError::WriteIntegrity(format!(
            "records append failed ({primary}) and rollback failed ({rollback})"
        )),
    }
}

pub fn recover_records(path: &Path) -> Result<RecoveryReport, ArtifactError> {
    recover_records_with_budget(path, 64 * 1024 * 1024)
}

pub fn recover_records_with_budget(
    path: &Path,
    retained_budget_bytes: usize,
) -> Result<RecoveryReport, ArtifactError> {
    if retained_budget_bytes == 0 {
        return Err(ArtifactError::InvalidConfig(
            "retained recovery budget must be positive",
        ));
    }
    verify_records_layout(path)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(RecoveryReport {
                frames: Vec::new(),
                truncated_bytes: 0,
                next_job_id: JobId::new(1).expect("one is a valid job id"),
            });
        }
        Err(error) => return Err(io_error(path, error)),
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(integrity(0, "records path is not a regular protected file"));
    }
    reject_hardlink(path, &metadata)?;
    verify_private_file_mode(path, &metadata, false)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(|error| io_error(path, error))?;
    let original_len = file
        .metadata()
        .map_err(|error| io_error(path, error))?
        .len();
    if original_len < RECORD_MAGIC.len() as u64 {
        let mut partial = [0_u8; RECORD_MAGIC.len()];
        let length = usize::try_from(original_len)
            .map_err(|_| integrity(0, "records length does not fit this platform"))?;
        file.read_exact(&mut partial[..length])
            .map_err(|error| io_error(path, error))?;
        if RECORD_MAGIC.starts_with(&partial[..length]) {
            file.set_len(0).map_err(|error| io_error(path, error))?;
            file.sync_data().map_err(|error| io_error(path, error))?;
            return Ok(RecoveryReport {
                frames: Vec::new(),
                truncated_bytes: original_len,
                next_job_id: JobId::new(1).expect("one is a valid job id"),
            });
        }
        return Err(integrity(0, "invalid records file magic"));
    }
    let mut magic = [0_u8; RECORD_MAGIC.len()];
    file.read_exact(&mut magic)
        .map_err(|error| io_error(path, error))?;
    if &magic != RECORD_MAGIC {
        return Err(integrity(0, "invalid records file magic"));
    }

    let mut offset = RECORD_MAGIC.len();
    let mut retained = 0_usize;
    let mut records = Vec::new();
    let mut prefix_hasher = Sha256::new();
    prefix_hasher.update(RECORD_MAGIC);
    let mut last_sequence = 0_u64;
    let mut terminal_jobs = BTreeSet::new();
    while (offset as u64) < original_len {
        let frame_start = offset;
        let remaining = usize::try_from(original_len - offset as u64).map_err(|_| {
            integrity(
                frame_start,
                "remaining records length does not fit platform",
            )
        })?;
        if remaining < FRAME_HEADER_BYTES {
            let mut partial = vec![0_u8; remaining];
            file.read_exact(&mut partial)
                .map_err(|error| io_error(path, error))?;
            if is_valid_incomplete_frame_header(&partial) {
                truncate_incomplete(&mut file, path, frame_start)?;
                break;
            }
            return Err(integrity(frame_start, "invalid complete batch header"));
        }

        let mut header = [0_u8; FRAME_HEADER_BYTES];
        file.read_exact(&mut header)
            .map_err(|error| io_error(path, error))?;
        if &header[..8] != BATCH_MAGIC {
            return Err(integrity(frame_start, "invalid complete batch magic"));
        }
        let length = u64::from_le_bytes(header[8..16].try_into().expect("eight bytes"));
        let complement = u64::from_le_bytes(header[16..24].try_into().expect("eight bytes"));
        if complement != !length || length > MAX_RECORD_BATCH_BYTES {
            return Err(integrity(frame_start, "invalid complete batch length"));
        }
        let payload_len = usize::try_from(length).map_err(|_| {
            integrity(
                frame_start,
                "record batch length does not fit this platform",
            )
        })?;
        let frame_len = FRAME_OVERHEAD_BYTES
            .checked_add(payload_len)
            .ok_or_else(|| integrity(frame_start, "record batch length overflow"))?;
        if remaining < frame_len {
            truncate_incomplete(&mut file, path, frame_start)?;
            break;
        }
        let required =
            retained
                .checked_add(frame_len)
                .ok_or(ArtifactError::RecoveryBudgetExceeded {
                    limit_bytes: retained_budget_bytes,
                    required_bytes: usize::MAX,
                })?;
        if required > retained_budget_bytes {
            return Err(ArtifactError::RecoveryBudgetExceeded {
                limit_bytes: retained_budget_bytes,
                required_bytes: required,
            });
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(payload_len)
            .map_err(|_| ArtifactError::BufferAllocation)?;
        payload.resize(payload_len, 0);
        file.read_exact(&mut payload)
            .map_err(|error| io_error(path, error))?;
        let mut digest_bytes = [0_u8; 32];
        file.read_exact(&mut digest_bytes)
            .map_err(|error| io_error(path, error))?;
        let mut trailer = [0_u8; BATCH_TRAILER.len()];
        file.read_exact(&mut trailer)
            .map_err(|error| io_error(path, error))?;
        if &trailer != BATCH_TRAILER {
            return Err(integrity(frame_start, "invalid complete batch trailer"));
        }
        let expected_digest = Sha256Digest::from_bytes(digest_bytes);
        if Sha256Digest::compute(&payload) != expected_digest {
            return Err(integrity(frame_start, "complete batch digest mismatch"));
        }
        let batch = decode_single_batch(&payload)
            .map_err(|error| integrity(frame_start, &format!("invalid Arrow IPC: {error}")))?;
        let record = batch_to_protected_record(&batch)
            .map_err(|error| integrity(frame_start, &error.to_string()))?;
        record
            .validate()
            .map_err(|error| integrity(frame_start, &error.to_string()))?;
        match &record {
            ProtectedRecord::Job(record) => {
                if record.sequence <= last_sequence {
                    return Err(integrity(
                        frame_start,
                        "record sequences are not strictly increasing",
                    ));
                }
                last_sequence = record.sequence;
                if !matches!(record.state, JobState::Queued | JobState::Running)
                    && !terminal_jobs.insert((record.workspace_incarnation.clone(), record.job_id))
                {
                    return Err(integrity(
                        frame_start,
                        "duplicate terminal artifact record for job id",
                    ));
                }
            }
            ProtectedRecord::CheckpointManifest(manifest) => {
                let observed = Sha256Digest::from_bytes(prefix_hasher.clone().finalize().into());
                if manifest.records_sha256 != observed {
                    return Err(integrity(
                        frame_start,
                        "checkpoint manifest records prefix digest mismatch",
                    ));
                }
            }
        }
        records
            .try_reserve(1)
            .map_err(|_| ArtifactError::BufferAllocation)?;
        records.push(RecoveredFrame {
            record,
            batch_sha256: expected_digest,
        });
        prefix_hasher.update(header);
        prefix_hasher.update(&payload);
        prefix_hasher.update(digest_bytes);
        prefix_hasher.update(trailer);
        retained = required;
        offset = offset
            .checked_add(frame_len)
            .ok_or_else(|| integrity(frame_start, "records offset overflow"))?;
    }

    let next = records
        .iter()
        .filter_map(|frame| match &frame.record {
            ProtectedRecord::Job(record) => Some(record.job_id.get()),
            ProtectedRecord::CheckpointManifest(_) => None,
        })
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| integrity(0, "job id allocation exhausted"))?;
    Ok(RecoveryReport {
        frames: records,
        truncated_bytes: original_len.saturating_sub(offset as u64),
        next_job_id: JobId::new(next)?,
    })
}

fn is_valid_incomplete_frame_header(bytes: &[u8]) -> bool {
    if bytes.len() < BATCH_MAGIC.len() {
        return BATCH_MAGIC.starts_with(bytes);
    }
    if &bytes[..BATCH_MAGIC.len()] != BATCH_MAGIC {
        return false;
    }
    if bytes.len() < 16 {
        return true;
    }
    let length = u64::from_le_bytes(bytes[8..16].try_into().expect("eight bytes"));
    if length > MAX_RECORD_BATCH_BYTES {
        return false;
    }
    if bytes.len() == 16 {
        return true;
    }
    let complement = (!length).to_le_bytes();
    complement.starts_with(&bytes[16..])
}

fn integrity(offset: usize, message: &str) -> ArtifactError {
    ArtifactError::Integrity {
        offset: offset as u64,
        message: message.to_owned(),
    }
}

fn truncate_incomplete(file: &mut File, path: &Path, offset: usize) -> Result<(), ArtifactError> {
    file.set_len(offset as u64)
        .map_err(|error| io_error(path, error))?;
    file.seek(SeekFrom::Start(offset as u64))
        .map_err(|error| io_error(path, error))?;
    file.sync_data().map_err(|error| io_error(path, error))?;
    Ok(())
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, ArtifactError> {
    let mut payload = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut payload, &batch.schema())
            .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
        writer
            .write(batch)
            .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
    }
    Ok(payload)
}

fn decode_single_batch(payload: &[u8]) -> Result<RecordBatch, ArtifactError> {
    let mut reader = StreamReader::try_new(Cursor::new(payload), None)
        .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
    let batch = reader
        .next()
        .ok_or_else(|| ArtifactError::Arrow("Arrow stream contains no batch".into()))?
        .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
    if reader.next().is_some() {
        return Err(ArtifactError::Arrow(
            "framed Arrow payload contains more than one batch".into(),
        ));
    }
    Ok(batch)
}

pub fn protected_record_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("record_kind", DataType::Utf8, false),
        field("record_version", DataType::UInt64, false),
        field("repo_id", DataType::Utf8, false),
        field("workspace_incarnation", DataType::Utf8, true),
        field("job_id", DataType::UInt64, true),
        field("sequence", DataType::UInt64, true),
        field("state", DataType::Utf8, true),
        field("grant_revision", DataType::UInt64, true),
        field("stdout_storage_kind", DataType::Utf8, true),
        field("stdout_source_path", DataType::Utf8, true),
        field("stdout_inline_bytes", DataType::Binary, true),
        field("stdout_protected_path", DataType::Utf8, true),
        field("stdout_bytes", DataType::UInt64, true),
        field("stdout_sha256", DataType::Binary, true),
        field("stdout_summary_version", DataType::UInt64, true),
        field("stdout_summary_text", DataType::Utf8, true),
        field("stdout_summary_truncated", DataType::Boolean, true),
        field("stderr_storage_kind", DataType::Utf8, true),
        field("stderr_source_path", DataType::Utf8, true),
        field("stderr_inline_bytes", DataType::Binary, true),
        field("stderr_protected_path", DataType::Utf8, true),
        field("stderr_bytes", DataType::UInt64, true),
        field("stderr_sha256", DataType::Binary, true),
        field("stderr_summary_version", DataType::UInt64, true),
        field("stderr_summary_text", DataType::Utf8, true),
        field("stderr_summary_truncated", DataType::Boolean, true),
        field("origin_incarnation", DataType::Utf8, true),
        field("barrier_id", DataType::UInt64, true),
        field("visible_jobs", visible_jobs_type(), true),
        field("records_sha256", DataType::Binary, true),
        field("output_limit_bytes", DataType::UInt64, true),
        field("output_crossing_bytes", DataType::UInt64, true),
        field(
            "argv",
            DataType::List(Arc::new(field("item", DataType::Binary, false))),
            true,
        ),
    ]))
}

fn field(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable)
}

fn visible_stream_fields() -> Fields {
    Fields::from(vec![
        field("storage_kind", DataType::Utf8, false),
        field("bytes", DataType::UInt64, false),
        field("sha256", DataType::Binary, false),
        field("protected_path", DataType::Utf8, true),
    ])
}

fn visible_jobs_type() -> DataType {
    let stream_fields = visible_stream_fields();
    let job_fields = Fields::from(vec![
        field("workspace_incarnation", DataType::Utf8, false),
        field("job_id", DataType::UInt64, false),
        field("state", DataType::Utf8, false),
        field("stdout", DataType::Struct(stream_fields.clone()), false),
        field("stderr", DataType::Struct(stream_fields), false),
    ]);
    DataType::List(Arc::new(field("item", DataType::Struct(job_fields), false)))
}

struct FlatStorage<'a> {
    kind: &'static str,
    source: Option<&'a str>,
    inline: Option<&'a [u8]>,
    protected: Option<&'a str>,
}

fn flatten_storage(storage: &OutputStorage) -> FlatStorage<'_> {
    let (kind, source, artifact) = match storage {
        OutputStorage::Captured { artifact } => ("captured", None, artifact),
        OutputStorage::Redirect { source, artifact } => (
            "redirect",
            Some(
                source
                    .as_path()
                    .to_str()
                    .expect("WorkspacePath is always UTF-8"),
            ),
            artifact,
        ),
    };
    match artifact {
        ProtectedOutput::Inline { data } => FlatStorage {
            kind,
            source,
            inline: Some(data.as_bytes()),
            protected: None,
        },
        ProtectedOutput::File { path } => FlatStorage {
            kind,
            source,
            inline: None,
            protected: Some(
                path.as_path()
                    .to_str()
                    .expect("WorkspacePath is always UTF-8"),
            ),
        },
    }
}

#[cfg(unix)]
fn artifact_arg_bytes(value: &OsStr) -> Result<&[u8], ArtifactError> {
    Ok(value.as_bytes())
}

#[cfg(not(unix))]
fn artifact_arg_bytes(value: &OsStr) -> Result<&[u8], ArtifactError> {
    value
        .to_str()
        .map(str::as_bytes)
        .ok_or(ArtifactError::Dto(DtoError::InvalidPlatformCommandArgument))
}

fn command_argv_array(argv: &[CommandArg]) -> Result<ListArray, ArtifactError> {
    validate_command_argv(argv)?;
    let mut encoded = Vec::with_capacity(argv.len());
    for argument in argv {
        encoded.push(artifact_arg_bytes(argument.as_os_str())?);
    }
    let values = BinaryArray::from_iter_values(encoded);
    let count = i32::try_from(argv.len())
        .map_err(|_| ArtifactError::Arrow("too many command arguments".into()))?;
    Ok(ListArray::new(
        Arc::new(field("item", DataType::Binary, false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, count])),
        Arc::new(values),
        None,
    ))
}

fn job_record_to_batch(record: &JobArtifactRecord) -> Result<RecordBatch, ArtifactError> {
    let stdout = flatten_storage(&record.stdout.storage);
    let stderr = flatten_storage(&record.stderr.storage);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["job"])),
        Arc::new(UInt64Array::from(vec![RECORD_SCHEMA_VERSION])),
        Arc::new(StringArray::from(vec![record.repo_id.as_str()])),
        Arc::new(StringArray::from(vec![Some(
            record.workspace_incarnation.as_str(),
        )])),
        Arc::new(UInt64Array::from(vec![Some(record.job_id.get())])),
        Arc::new(UInt64Array::from(vec![Some(record.sequence)])),
        Arc::new(StringArray::from(vec![Some(state_name(record.state))])),
        Arc::new(UInt64Array::from(vec![Some(record.grant_revision)])),
        Arc::new(StringArray::from(vec![Some(stdout.kind)])),
        Arc::new(StringArray::from(vec![stdout.source])),
        Arc::new(BinaryArray::from(vec![stdout.inline])),
        Arc::new(StringArray::from(vec![stdout.protected])),
        Arc::new(UInt64Array::from(vec![Some(record.stdout.bytes)])),
        Arc::new(BinaryArray::from(vec![Some(
            record.stdout.sha256.as_bytes().as_slice(),
        )])),
        Arc::new(UInt64Array::from(vec![Some(
            record.stdout.summary.version as u64,
        )])),
        Arc::new(StringArray::from(vec![Some(
            record.stdout.summary.text.as_str(),
        )])),
        Arc::new(BooleanArray::from(vec![Some(
            record.stdout.summary.truncated,
        )])),
        Arc::new(StringArray::from(vec![Some(stderr.kind)])),
        Arc::new(StringArray::from(vec![stderr.source])),
        Arc::new(BinaryArray::from(vec![stderr.inline])),
        Arc::new(StringArray::from(vec![stderr.protected])),
        Arc::new(UInt64Array::from(vec![Some(record.stderr.bytes)])),
        Arc::new(BinaryArray::from(vec![Some(
            record.stderr.sha256.as_bytes().as_slice(),
        )])),
        Arc::new(UInt64Array::from(vec![Some(
            record.stderr.summary.version as u64,
        )])),
        Arc::new(StringArray::from(vec![Some(
            record.stderr.summary.text.as_str(),
        )])),
        Arc::new(BooleanArray::from(vec![Some(
            record.stderr.summary.truncated,
        )])),
        Arc::new(StringArray::from(vec![Option::<&str>::None])),
        Arc::new(UInt64Array::from(vec![Option::<u64>::None])),
        new_null_array(&visible_jobs_type(), 1),
        Arc::new(BinaryArray::from(vec![Option::<&[u8]>::None])),
        Arc::new(UInt64Array::from(vec![
            record.output_limit.as_ref().map(|limit| limit.limit_bytes),
        ])),
        Arc::new(UInt64Array::from(vec![
            record
                .output_limit
                .as_ref()
                .map(|limit| limit.crossing_bytes),
        ])),
        Arc::new(command_argv_array(&record.argv)?),
    ];
    RecordBatch::try_new(protected_record_schema(), columns)
        .map_err(|error| ArtifactError::Arrow(error.to_string()))
}

fn batch_to_job_record(batch: &RecordBatch) -> Result<JobArtifactRecord, ArtifactError> {
    if batch.num_rows() != 1 || batch.schema() != protected_record_schema() {
        return Err(ArtifactError::Arrow(
            "protected batch must contain one row with the canonical schema".into(),
        ));
    }
    if string(batch, 0)?.value(0) != "job" {
        return Err(ArtifactError::Arrow("protected record is not a job".into()));
    }
    let version = uint64(batch, 1)?.value(0);
    if version != RECORD_SCHEMA_VERSION {
        return Err(ArtifactError::Arrow(format!(
            "unsupported job record version {version}"
        )));
    }
    require_job_columns(batch, 0)?;
    let repo_id = RepoId::parse(string(batch, 2)?.value(0))
        .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
    let workspace_incarnation = WorkspaceIncarnation::new(string(batch, 3)?.value(0))
        .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
    let job_id = JobId::new(uint64(batch, 4)?.value(0))?;
    let sequence = uint64(batch, 5)?.value(0);
    let state = parse_state(string(batch, 6)?.value(0))?;
    let grant_revision = uint64(batch, 7)?.value(0);
    let stdout = decode_stream(batch, 8)?;
    let stderr = decode_stream(batch, 17)?;
    let output_limit = match (batch.column(30).is_null(0), batch.column(31).is_null(0)) {
        (true, true) => None,
        (false, false) => Some(OutputLimitInfo {
            limit_bytes: uint64(batch, 30)?.value(0),
            crossing_bytes: uint64(batch, 31)?.value(0),
        }),
        _ => {
            return Err(ArtifactError::Arrow(
                "output limit columns must both be null or present".into(),
            ));
        }
    };
    let argv = decode_command_argv(batch, 32)?;
    Ok(JobArtifactRecord {
        repo_id,
        workspace_incarnation,
        job_id,
        sequence,
        state,
        grant_revision,
        argv,
        output_limit,
        stdout,
        stderr,
    })
}

fn visible_storage_name(kind: VisibleStorageKind) -> &'static str {
    match kind {
        VisibleStorageKind::CapturedInline => "captured-inline",
        VisibleStorageKind::CapturedFile => "captured-file",
        VisibleStorageKind::RedirectInline => "redirect-inline",
        VisibleStorageKind::RedirectFile => "redirect-file",
    }
}

fn parse_visible_storage(value: &str) -> Result<VisibleStorageKind, ArtifactError> {
    match value {
        "captured-inline" => Ok(VisibleStorageKind::CapturedInline),
        "captured-file" => Ok(VisibleStorageKind::CapturedFile),
        "redirect-inline" => Ok(VisibleStorageKind::RedirectInline),
        "redirect-file" => Ok(VisibleStorageKind::RedirectFile),
        _ => Err(ArtifactError::Arrow(format!(
            "unknown visible storage kind {value:?}"
        ))),
    }
}

fn visible_stream_array(
    jobs: &[VisibleJobCommitment],
    select: impl Fn(&VisibleJobCommitment) -> &VisibleStreamCommitment,
) -> StructArray {
    let values: Vec<_> = jobs.iter().map(select).collect();
    StructArray::new(
        visible_stream_fields(),
        vec![
            Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|value| visible_storage_name(value.storage_kind))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                values.iter().map(|value| value.bytes).collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from(
                values
                    .iter()
                    .map(|value| Some(value.sha256.as_bytes().as_slice()))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|value| {
                        value.protected_path.as_ref().map(|path| {
                            path.as_path()
                                .to_str()
                                .expect("WorkspacePath is always UTF-8")
                        })
                    })
                    .collect::<Vec<_>>(),
            )),
        ],
        None,
    )
}

fn visible_jobs_array(jobs: &[VisibleJobCommitment]) -> Result<ListArray, ArtifactError> {
    let stream_fields = visible_stream_fields();
    let job_fields = Fields::from(vec![
        field("workspace_incarnation", DataType::Utf8, false),
        field("job_id", DataType::UInt64, false),
        field("state", DataType::Utf8, false),
        field("stdout", DataType::Struct(stream_fields.clone()), false),
        field("stderr", DataType::Struct(stream_fields), false),
    ]);
    let values = StructArray::new(
        job_fields.clone(),
        vec![
            Arc::new(StringArray::from(
                jobs.iter()
                    .map(|job| job.workspace_incarnation.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                jobs.iter().map(|job| job.job_id.get()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                jobs.iter()
                    .map(|job| state_name(job.state))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(visible_stream_array(jobs, |job| &job.stdout)),
            Arc::new(visible_stream_array(jobs, |job| &job.stderr)),
        ],
        None,
    );
    let count = i32::try_from(jobs.len())
        .map_err(|_| ArtifactError::Arrow("too many visible jobs".into()))?;
    Ok(ListArray::new(
        Arc::new(field("item", DataType::Struct(job_fields), false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, count])),
        Arc::new(values),
        None,
    ))
}

fn checkpoint_manifest_to_batch(
    record: &CheckpointManifestRecord,
) -> Result<RecordBatch, ArtifactError> {
    record.validate()?;
    let schema = protected_record_schema();
    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    columns.push(Arc::new(StringArray::from(vec!["checkpointManifest"])));
    columns.push(Arc::new(UInt64Array::from(vec![record.version as u64])));
    columns.push(Arc::new(StringArray::from(vec![record.repo_id.as_str()])));
    for index in 3..26 {
        columns.push(new_null_array(schema.field(index).data_type(), 1));
    }
    columns.push(Arc::new(StringArray::from(vec![Some(
        record.origin_incarnation.as_str(),
    )])));
    columns.push(Arc::new(UInt64Array::from(vec![Some(record.barrier_id)])));
    columns.push(Arc::new(visible_jobs_array(&record.visible_jobs)?));
    columns.push(Arc::new(BinaryArray::from(vec![Some(
        record.records_sha256.as_bytes().as_slice(),
    )])));
    columns.push(new_null_array(schema.field(30).data_type(), 1));
    columns.push(new_null_array(schema.field(31).data_type(), 1));
    columns.push(new_null_array(schema.field(32).data_type(), 1));
    RecordBatch::try_new(schema, columns).map_err(|error| ArtifactError::Arrow(error.to_string()))
}

fn protected_record_to_batch(record: &ProtectedRecord) -> Result<RecordBatch, ArtifactError> {
    match record {
        ProtectedRecord::Job(value) => job_record_to_batch(value),
        ProtectedRecord::CheckpointManifest(value) => checkpoint_manifest_to_batch(value),
    }
}

fn batch_to_protected_record(batch: &RecordBatch) -> Result<ProtectedRecord, ArtifactError> {
    if batch.num_rows() != 1 || batch.schema() != protected_record_schema() {
        return Err(ArtifactError::Arrow(
            "protected batch must contain one row with the canonical schema".into(),
        ));
    }
    match string(batch, 0)?.value(0) {
        "job" => Ok(ProtectedRecord::Job(batch_to_job_record(batch)?)),
        "checkpointManifest" => Ok(ProtectedRecord::CheckpointManifest(
            batch_to_checkpoint_manifest(batch)?,
        )),
        value => Err(ArtifactError::Arrow(format!(
            "unknown protected record kind {value:?}"
        ))),
    }
}

fn batch_to_checkpoint_manifest(
    batch: &RecordBatch,
) -> Result<CheckpointManifestRecord, ArtifactError> {
    require_protected_columns(batch, 0, &[26, 27, 28, 29])?;
    let version = u16::try_from(uint64(batch, 1)?.value(0))
        .map_err(|_| ArtifactError::Arrow("manifest version does not fit u16".into()))?;
    let repo_id = RepoId::parse(string(batch, 2)?.value(0))
        .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
    let origin_incarnation = WorkspaceIncarnation::new(string(batch, 26)?.value(0))
        .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
    let jobs = list(batch, 28)?;
    let values = downcast::<StructArray>(jobs.values().as_ref(), "visible_jobs.values")?;
    let mut visible_jobs = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        visible_jobs.push(VisibleJobCommitment {
            workspace_incarnation: WorkspaceIncarnation::new(
                struct_string(values, "workspace_incarnation")?.value(row),
            )
            .map_err(|error| ArtifactError::Arrow(error.to_string()))?,
            job_id: JobId::new(struct_u64(values, "job_id")?.value(row))?,
            state: parse_state(struct_string(values, "state")?.value(row))?,
            stdout: decode_visible_stream(struct_struct(values, "stdout")?, row)?,
            stderr: decode_visible_stream(struct_struct(values, "stderr")?, row)?,
        });
    }
    let digest: [u8; 32] = binary(batch, 29)?
        .value(0)
        .try_into()
        .map_err(|_| ArtifactError::Arrow("records digest is not 32 bytes".into()))?;
    let record = CheckpointManifestRecord {
        version,
        repo_id,
        origin_incarnation,
        barrier_id: uint64(batch, 27)?.value(0),
        visible_jobs,
        records_sha256: Sha256Digest::from_bytes(digest),
    };
    record.validate()?;
    Ok(record)
}

fn decode_visible_stream(
    values: &StructArray,
    row: usize,
) -> Result<VisibleStreamCommitment, ArtifactError> {
    let paths = struct_string(values, "protected_path")?;
    let digest: [u8; 32] = struct_binary(values, "sha256")?
        .value(row)
        .try_into()
        .map_err(|_| ArtifactError::Arrow("visible stream digest is not 32 bytes".into()))?;
    let value = VisibleStreamCommitment {
        storage_kind: parse_visible_storage(struct_string(values, "storage_kind")?.value(row))?,
        bytes: struct_u64(values, "bytes")?.value(row),
        sha256: Sha256Digest::from_bytes(digest),
        protected_path: (!paths.is_null(row))
            .then(|| WorkspacePath::new(paths.value(row)))
            .transpose()?,
    };
    value.validate()?;
    Ok(value)
}

fn decode_stream(batch: &RecordBatch, offset: usize) -> Result<StreamInfo, ArtifactError> {
    let kind = string(batch, offset)?.value(0);
    let source_array = string(batch, offset + 1)?;
    let inline_array = binary(batch, offset + 2)?;
    let protected_array = string(batch, offset + 3)?;
    let source = (!source_array.is_null(0))
        .then(|| WorkspacePath::new(source_array.value(0)))
        .transpose()?;
    let inline = (!inline_array.is_null(0))
        .then(|| BinaryData::new(inline_array.value(0).to_vec()))
        .transpose()?;
    let protected = (!protected_array.is_null(0))
        .then(|| WorkspacePath::new(protected_array.value(0)))
        .transpose()?;
    let artifact = match (inline, protected) {
        (Some(data), None) => ProtectedOutput::Inline { data },
        (None, Some(path)) => ProtectedOutput::File { path },
        _ => {
            return Err(ArtifactError::Arrow(
                "stream must have exactly one inline or protected artifact".into(),
            ));
        }
    };
    let storage = match (kind, source) {
        ("captured", None) => OutputStorage::Captured { artifact },
        ("redirect", Some(source)) => OutputStorage::Redirect { source, artifact },
        _ => {
            return Err(ArtifactError::Arrow(
                "stream storage kind and source path disagree".into(),
            ));
        }
    };
    let digest_bytes = binary(batch, offset + 5)?.value(0);
    let digest: [u8; 32] = digest_bytes
        .try_into()
        .map_err(|_| ArtifactError::Arrow("SHA-256 column is not 32 bytes".into()))?;
    let summary_version = uint64(batch, offset + 6)?.value(0);
    let summary_version = u16::try_from(summary_version)
        .map_err(|_| ArtifactError::Arrow("summary version does not fit u16".into()))?;
    Ok(StreamInfo {
        storage,
        bytes: uint64(batch, offset + 4)?.value(0),
        sha256: Sha256Digest::from_bytes(digest),
        summary: OutputSummary {
            version: summary_version,
            text: string(batch, offset + 7)?.value(0).to_owned(),
            truncated: boolean(batch, offset + 8)?.value(0),
        },
    })
}

fn decode_command_argv(
    batch: &RecordBatch,
    index: usize,
) -> Result<Vec<CommandArg>, ArtifactError> {
    let values = list(batch, index)?.value(0);
    let values = downcast::<BinaryArray>(values.as_ref(), "argv.values")?;
    if values.is_empty() || values.value(0).is_empty() {
        return Err(ArtifactError::Dto(DtoError::InvalidCommandArgv));
    }
    let mut total = 0_usize;
    for row in 0..values.len() {
        if values.is_null(row) {
            return Err(ArtifactError::Arrow(
                "command argv contains a null argument".into(),
            ));
        }
        let bytes = values.value(row);
        if bytes.len() > MAX_COMMAND_ARG_BYTES {
            return Err(ArtifactError::Dto(DtoError::CommandArgumentTooLarge));
        }
        if bytes.contains(&0) {
            return Err(ArtifactError::Dto(DtoError::CommandArgumentContainsNul));
        }
        total = total
            .checked_add(bytes.len())
            .ok_or(ArtifactError::Dto(DtoError::CommandArgvTooLarge))?;
        if total > MAX_ARGV_BYTES {
            return Err(ArtifactError::Dto(DtoError::CommandArgvTooLarge));
        }
        #[cfg(not(unix))]
        std::str::from_utf8(bytes)
            .map_err(|_| ArtifactError::Dto(DtoError::InvalidPlatformCommandArgument))?;
    }
    let mut argv = Vec::new();
    argv.try_reserve_exact(values.len())
        .map_err(|_| ArtifactError::BufferAllocation)?;
    for row in 0..values.len() {
        let bytes = values.value(row);
        #[cfg(unix)]
        let value = OsString::from_vec(bytes.to_vec());
        #[cfg(not(unix))]
        let value = OsString::from(
            std::str::from_utf8(bytes)
                .map_err(|_| ArtifactError::Dto(DtoError::InvalidPlatformCommandArgument))?,
        );
        argv.push(CommandArg::from(value));
    }
    validate_command_argv(&argv)?;
    Ok(argv)
}

fn downcast<'a, T: 'static>(array: &'a dyn Array, name: &str) -> Result<&'a T, ArtifactError> {
    (array.as_any() as &dyn Any)
        .downcast_ref::<T>()
        .ok_or_else(|| ArtifactError::Arrow(format!("column {name} has the wrong type")))
}

fn uint64(batch: &RecordBatch, index: usize) -> Result<&UInt64Array, ArtifactError> {
    downcast(
        batch.column(index).as_ref(),
        batch.schema().field(index).name(),
    )
}

fn string(batch: &RecordBatch, index: usize) -> Result<&StringArray, ArtifactError> {
    downcast(
        batch.column(index).as_ref(),
        batch.schema().field(index).name(),
    )
}

fn binary(batch: &RecordBatch, index: usize) -> Result<&BinaryArray, ArtifactError> {
    downcast(
        batch.column(index).as_ref(),
        batch.schema().field(index).name(),
    )
}

fn boolean(batch: &RecordBatch, index: usize) -> Result<&BooleanArray, ArtifactError> {
    downcast(
        batch.column(index).as_ref(),
        batch.schema().field(index).name(),
    )
}

fn list(batch: &RecordBatch, index: usize) -> Result<&ListArray, ArtifactError> {
    downcast(
        batch.column(index).as_ref(),
        batch.schema().field(index).name(),
    )
}

fn struct_string<'a>(
    values: &'a StructArray,
    name: &str,
) -> Result<&'a StringArray, ArtifactError> {
    let column = values
        .column_by_name(name)
        .ok_or_else(|| ArtifactError::Arrow(format!("missing struct field {name}")))?;
    downcast(column.as_ref(), name)
}

fn struct_u64<'a>(values: &'a StructArray, name: &str) -> Result<&'a UInt64Array, ArtifactError> {
    let column = values
        .column_by_name(name)
        .ok_or_else(|| ArtifactError::Arrow(format!("missing struct field {name}")))?;
    downcast(column.as_ref(), name)
}

fn struct_binary<'a>(
    values: &'a StructArray,
    name: &str,
) -> Result<&'a BinaryArray, ArtifactError> {
    let column = values
        .column_by_name(name)
        .ok_or_else(|| ArtifactError::Arrow(format!("missing struct field {name}")))?;
    downcast(column.as_ref(), name)
}

fn struct_struct<'a>(
    values: &'a StructArray,
    name: &str,
) -> Result<&'a StructArray, ArtifactError> {
    let column = values
        .column_by_name(name)
        .ok_or_else(|| ArtifactError::Arrow(format!("missing struct field {name}")))?;
    downcast(column.as_ref(), name)
}

fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Queued => "queued",
        JobState::Running => "running",
        JobState::Exited => "exited",
        JobState::Signaled => "signaled",
        JobState::Killed => "killed",
        JobState::OutputLimit => "outputLimit",
        JobState::Failed => "failed",
    }
}

fn parse_state(value: &str) -> Result<JobState, ArtifactError> {
    match value {
        "queued" => Ok(JobState::Queued),
        "running" => Ok(JobState::Running),
        "exited" => Ok(JobState::Exited),
        "signaled" => Ok(JobState::Signaled),
        "killed" => Ok(JobState::Killed),
        "outputLimit" => Ok(JobState::OutputLimit),
        "failed" => Ok(JobState::Failed),
        _ => Err(ArtifactError::Arrow(format!("unknown job state {value:?}"))),
    }
}

pub fn controller_commitment_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("commitment_kind", DataType::Utf8, false),
        field("commitment_version", DataType::UInt64, false),
        field("commitment_order", DataType::UInt64, false),
        field("repo_id", DataType::Utf8, false),
        field("workspace_incarnation", DataType::Utf8, true),
        field("job_id", DataType::UInt64, true),
        field("grant_revision", DataType::UInt64, true),
        field("state", DataType::Utf8, true),
        field("stdout_bytes", DataType::UInt64, true),
        field("stdout_sha256", DataType::Binary, true),
        field("stderr_bytes", DataType::UInt64, true),
        field("stderr_sha256", DataType::Binary, true),
        field("batch_sha256", DataType::Binary, true),
        field("origin_incarnation", DataType::Utf8, true),
        field("checkpoint_id", DataType::Utf8, true),
        field("barrier_id", DataType::UInt64, true),
        field("manifest_batch_sha256", DataType::Binary, true),
        field("source_incarnation", DataType::Utf8, true),
        field("destination_incarnation", DataType::Utf8, true),
        field("source_checkpoint", DataType::Utf8, true),
        field("output_limit_bytes", DataType::UInt64, true),
        field("output_crossing_bytes", DataType::UInt64, true),
    ]))
}

struct ControllerFlatRow<'a> {
    kind: &'static str,
    version: u64,
    order: u64,
    repo_id: &'a str,
    workspace_incarnation: Option<&'a str>,
    job_id: Option<u64>,
    grant_revision: Option<u64>,
    state: Option<&'static str>,
    stdout_bytes: Option<u64>,
    stdout_sha256: Option<&'a [u8]>,
    stderr_bytes: Option<u64>,
    stderr_sha256: Option<&'a [u8]>,
    batch_sha256: Option<&'a [u8]>,
    origin_incarnation: Option<&'a str>,
    output_limit_bytes: Option<u64>,
    output_crossing_bytes: Option<u64>,
    checkpoint_id: Option<&'a str>,
    barrier_id: Option<u64>,
    manifest_batch_sha256: Option<&'a [u8]>,
    source_incarnation: Option<&'a str>,
    destination_incarnation: Option<&'a str>,
    source_checkpoint: Option<&'a str>,
}

fn flatten_controller(value: &ControllerCommitment) -> ControllerFlatRow<'_> {
    let mut row = ControllerFlatRow {
        kind: "",
        version: value.version() as u64,
        order: value.order(),
        repo_id: value.repo_id().as_str(),
        workspace_incarnation: None,
        job_id: None,
        grant_revision: None,
        state: None,
        stdout_bytes: None,
        stdout_sha256: None,
        stderr_bytes: None,
        stderr_sha256: None,
        batch_sha256: None,
        origin_incarnation: None,
        output_limit_bytes: None,
        output_crossing_bytes: None,
        checkpoint_id: None,
        barrier_id: None,
        manifest_batch_sha256: None,
        source_incarnation: None,
        destination_incarnation: None,
        source_checkpoint: None,
    };
    match value {
        ControllerCommitment::WorkspaceIntroduced(value) => {
            row.kind = "workspaceIntroduced";
            row.workspace_incarnation = Some(value.workspace_incarnation.as_str());
        }
        ControllerCommitment::WorkspaceRetired(value) => {
            row.kind = "workspaceRetired";
            row.workspace_incarnation = Some(value.workspace_incarnation.as_str());
        }
        ControllerCommitment::Admission(value) => {
            row.kind = "admission";
            row.workspace_incarnation = Some(value.workspace_incarnation.as_str());
            row.job_id = Some(value.job_id.get());
            row.grant_revision = Some(value.grant_revision);
        }
        ControllerCommitment::Terminal(value) => {
            row.kind = "terminal";
            row.workspace_incarnation = Some(value.workspace_incarnation.as_str());
            row.job_id = Some(value.job_id.get());
            row.grant_revision = Some(value.grant_revision);
            row.output_limit_bytes = value.output_limit.as_ref().map(|limit| limit.limit_bytes);
            row.output_crossing_bytes = value
                .output_limit
                .as_ref()
                .map(|limit| limit.crossing_bytes);
            row.state = Some(state_name(value.state));
            row.stdout_bytes = Some(value.stdout_bytes);
            row.stdout_sha256 = Some(value.stdout_sha256.as_bytes());
            row.stderr_bytes = Some(value.stderr_bytes);
            row.stderr_sha256 = Some(value.stderr_sha256.as_bytes());
            row.batch_sha256 = Some(value.batch_sha256.as_bytes());
        }
        ControllerCommitment::Checkpoint(value) => {
            row.kind = "checkpoint";
            row.origin_incarnation = Some(value.origin_incarnation.as_str());
            row.checkpoint_id = Some(&value.checkpoint_id);
            row.barrier_id = Some(value.barrier_id);
            row.manifest_batch_sha256 = Some(value.manifest_batch_sha256.as_bytes());
        }
        ControllerCommitment::Fork(value) => {
            row.kind = "fork";
            row.source_incarnation = Some(value.source_incarnation.as_str());
            row.destination_incarnation = Some(value.destination_incarnation.as_str());
        }
        ControllerCommitment::Restore(value) => {
            row.kind = "restore";
            row.source_incarnation = Some(value.source_incarnation.as_str());
            row.destination_incarnation = Some(value.destination_incarnation.as_str());
            row.source_checkpoint = Some(&value.source_checkpoint);
        }
    }
    row
}

pub fn controller_commitments_to_batch(
    commitments: &[ControllerCommitment],
) -> Result<RecordBatch, ArtifactError> {
    for commitment in commitments {
        commitment.validate()?;
    }
    let rows: Vec<_> = commitments.iter().map(flatten_controller).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.version).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.order).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.repo_id).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.workspace_incarnation)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.job_id).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.grant_revision)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.state).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.stdout_bytes).collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from(
            rows.iter().map(|row| row.stdout_sha256).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.stderr_bytes).collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from(
            rows.iter().map(|row| row.stderr_sha256).collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from(
            rows.iter().map(|row| row.batch_sha256).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.origin_incarnation)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.checkpoint_id).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.barrier_id).collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from(
            rows.iter()
                .map(|row| row.manifest_batch_sha256)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.source_incarnation)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.destination_incarnation)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.source_checkpoint)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.output_limit_bytes)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.output_crossing_bytes)
                .collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(controller_commitment_schema(), columns)
        .map_err(|error| ArtifactError::Arrow(error.to_string()))
}

pub fn controller_commitments_from_batch(
    batch: &RecordBatch,
    prior: &CommitmentPriorContext,
) -> Result<Vec<ControllerCommitment>, ArtifactError> {
    if batch.schema() != controller_commitment_schema() {
        return Err(ArtifactError::Arrow(
            "controller commitment schema mismatch".into(),
        ));
    }
    let mut values = Vec::new();
    values
        .try_reserve(batch.num_rows())
        .map_err(|_| ArtifactError::BufferAllocation)?;
    for row in 0..batch.num_rows() {
        let kind = required_string(batch, 0, row)?;
        let version = u16::try_from(required_u64(batch, 1, row)?)
            .map_err(|_| ArtifactError::Arrow("commitment version does not fit u16".into()))?;
        let order = required_u64(batch, 2, row)?;
        let repo_id = RepoId::parse(required_string(batch, 3, row)?)
            .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
        let value = match kind {
            "workspaceIntroduced" => {
                require_variant_columns(batch, row, &[4])?;
                ControllerCommitment::WorkspaceIntroduced(WorkspaceIntroducedCommitment {
                    version,
                    order,
                    repo_id,
                    workspace_incarnation: required_incarnation(batch, 4, row)?,
                })
            }
            "workspaceRetired" => {
                require_variant_columns(batch, row, &[4])?;
                ControllerCommitment::WorkspaceRetired(WorkspaceRetiredCommitment {
                    version,
                    order,
                    repo_id,
                    workspace_incarnation: required_incarnation(batch, 4, row)?,
                })
            }
            "admission" => {
                require_variant_columns(batch, row, &[4, 5, 6])?;
                ControllerCommitment::Admission(AdmissionCommitment {
                    version,
                    order,
                    repo_id,
                    workspace_incarnation: required_incarnation(batch, 4, row)?,
                    job_id: JobId::new(required_u64(batch, 5, row)?)?,
                    grant_revision: required_u64(batch, 6, row)?,
                })
            }
            "terminal" => {
                let has_limit = !batch.column(20).is_null(row);
                let selected: &[usize] = if has_limit {
                    &[4, 5, 6, 7, 8, 9, 10, 11, 12, 20, 21]
                } else {
                    &[4, 5, 6, 7, 8, 9, 10, 11, 12]
                };
                require_variant_columns(batch, row, selected)?;
                let output_limit = if has_limit {
                    Some(OutputLimitInfo {
                        limit_bytes: uint64(batch, 20)?.value(row),
                        crossing_bytes: uint64(batch, 21)?.value(row),
                    })
                } else {
                    None
                };
                ControllerCommitment::Terminal(TerminalCommitment {
                    version,
                    order,
                    repo_id,
                    workspace_incarnation: required_incarnation(batch, 4, row)?,
                    job_id: JobId::new(required_u64(batch, 5, row)?)?,
                    state: parse_state(required_string(batch, 7, row)?)?,
                    grant_revision: required_u64(batch, 6, row)?,
                    stdout_bytes: required_u64(batch, 8, row)?,
                    stdout_sha256: required_digest(batch, 9, row)?,
                    stderr_bytes: required_u64(batch, 10, row)?,
                    stderr_sha256: required_digest(batch, 11, row)?,
                    batch_sha256: required_digest(batch, 12, row)?,
                    output_limit,
                })
            }
            "checkpoint" => {
                require_variant_columns(batch, row, &[13, 14, 15, 16])?;
                ControllerCommitment::Checkpoint(CheckpointCommitment {
                    version,
                    order,
                    repo_id,
                    origin_incarnation: required_incarnation(batch, 13, row)?,
                    checkpoint_id: required_string(batch, 14, row)?.to_owned(),
                    barrier_id: required_u64(batch, 15, row)?,
                    manifest_batch_sha256: required_digest(batch, 16, row)?,
                })
            }
            "fork" => {
                require_variant_columns(batch, row, &[17, 18])?;
                ControllerCommitment::Fork(ForkCommitment {
                    version,
                    order,
                    repo_id,
                    source_incarnation: required_incarnation(batch, 17, row)?,
                    destination_incarnation: required_incarnation(batch, 18, row)?,
                })
            }
            "restore" => {
                require_variant_columns(batch, row, &[17, 18, 19])?;
                ControllerCommitment::Restore(RestoreCommitment {
                    version,
                    order,
                    repo_id,
                    source_checkpoint: required_string(batch, 19, row)?.to_owned(),
                    source_incarnation: required_incarnation(batch, 17, row)?,
                    destination_incarnation: required_incarnation(batch, 18, row)?,
                })
            }
            _ => {
                return Err(ArtifactError::Arrow(format!(
                    "unknown controller commitment kind {kind:?}"
                )));
            }
        };
        value.validate()?;
        values.push(value);
    }
    validate_commitments(prior, &values)?;
    Ok(values)
}

fn required_string(batch: &RecordBatch, column: usize, row: usize) -> Result<&str, ArtifactError> {
    let array = string(batch, column)?;
    if array.is_null(row) {
        return Err(ArtifactError::Arrow(format!(
            "required column {} is null",
            batch.schema().field(column).name()
        )));
    }
    Ok(array.value(row))
}

fn required_u64(batch: &RecordBatch, column: usize, row: usize) -> Result<u64, ArtifactError> {
    let array = uint64(batch, column)?;
    if array.is_null(row) {
        return Err(ArtifactError::Arrow(format!(
            "required column {} is null",
            batch.schema().field(column).name()
        )));
    }
    Ok(array.value(row))
}

fn required_digest(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<Sha256Digest, ArtifactError> {
    let array = binary(batch, column)?;
    if array.is_null(row) {
        return Err(ArtifactError::Arrow(format!(
            "required column {} is null",
            batch.schema().field(column).name()
        )));
    }
    let bytes: [u8; 32] = array
        .value(row)
        .try_into()
        .map_err(|_| ArtifactError::Arrow("commitment digest is not 32 bytes".into()))?;
    Ok(Sha256Digest::from_bytes(bytes))
}

fn required_incarnation(
    batch: &RecordBatch,
    column: usize,
    row: usize,
) -> Result<WorkspaceIncarnation, ArtifactError> {
    WorkspaceIncarnation::new(required_string(batch, column, row)?)
        .map_err(|error| ArtifactError::Arrow(error.to_string()))
}

fn require_variant_columns(
    batch: &RecordBatch,
    row: usize,
    selected: &[usize],
) -> Result<(), ArtifactError> {
    for column in 4..batch.num_columns() {
        let should_be_present = selected.contains(&column);
        if should_be_present == batch.column(column).is_null(row) {
            return Err(ArtifactError::Arrow(format!(
                "commitment variant nullability mismatch for {}",
                batch.schema().field(column).name()
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct RepositoryCommitmentContext {
    active_incarnations: BTreeSet<WorkspaceIncarnation>,
    introduced_incarnations: BTreeSet<WorkspaceIncarnation>,
    retired_incarnations: BTreeSet<WorkspaceIncarnation>,
    admissions: BTreeMap<DurableJobKey, u64>,
    terminals: BTreeSet<DurableJobKey>,
    checkpoints: BTreeMap<String, WorkspaceIncarnation>,
    last_barriers: BTreeMap<WorkspaceIncarnation, u64>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DurableJobKey {
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
}

#[derive(Clone, Debug)]
pub struct CommitmentPriorContext {
    last_order: u64,
    repositories: BTreeMap<RepoId, RepositoryCommitmentContext>,
}

impl CommitmentPriorContext {
    pub fn new(
        repo_id: RepoId,
        known_incarnations: impl IntoIterator<Item = WorkspaceIncarnation>,
    ) -> Self {
        let repository = RepositoryCommitmentContext {
            active_incarnations: known_incarnations.into_iter().collect(),
            ..RepositoryCommitmentContext::default()
        };
        Self {
            last_order: 0,
            repositories: BTreeMap::from([(repo_id, repository)]),
        }
    }

    pub fn last_order(&self) -> u64 {
        self.last_order
    }

    pub(crate) fn is_introduced(
        &self,
        repo_id: &RepoId,
        incarnation: &WorkspaceIncarnation,
    ) -> bool {
        self.repositories
            .get(repo_id)
            .is_some_and(|repository| repository.introduced_incarnations.contains(incarnation))
    }

    pub(crate) fn is_retired(&self, repo_id: &RepoId, incarnation: &WorkspaceIncarnation) -> bool {
        self.repositories
            .get(repo_id)
            .is_some_and(|repository| repository.retired_incarnations.contains(incarnation))
    }
}

pub fn validate_commitments(
    prior: &CommitmentPriorContext,
    commitments: &[ControllerCommitment],
) -> Result<CommitmentPriorContext, ArtifactError> {
    let mut context = prior.clone();
    for commitment in commitments {
        commitment.validate()?;
        let expected_order =
            context
                .last_order
                .checked_add(1)
                .ok_or_else(|| ArtifactError::Integrity {
                    offset: 0,
                    message: "controller commitment order overflow".into(),
                })?;
        if commitment.order() != expected_order {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: "controller commitment order is not contiguous".into(),
            });
        }
        context.last_order = commitment.order();
        let repository = context
            .repositories
            .entry(commitment.repo_id().clone())
            .or_default();
        match commitment {
            ControllerCommitment::WorkspaceIntroduced(value) => {
                if repository
                    .introduced_incarnations
                    .contains(&value.workspace_incarnation)
                    || repository
                        .retired_incarnations
                        .contains(&value.workspace_incarnation)
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message:
                            "workspace incarnation is introduced more than once or after retirement"
                                .into(),
                    });
                }
                repository
                    .introduced_incarnations
                    .insert(value.workspace_incarnation.clone());
                repository
                    .active_incarnations
                    .insert(value.workspace_incarnation.clone());
            }
            ControllerCommitment::WorkspaceRetired(value) => {
                if repository
                    .retired_incarnations
                    .contains(&value.workspace_incarnation)
                    || !repository
                        .active_incarnations
                        .remove(&value.workspace_incarnation)
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "workspace retirement has no active source or is duplicated"
                            .into(),
                    });
                }
                repository
                    .retired_incarnations
                    .insert(value.workspace_incarnation.clone());
            }
            ControllerCommitment::Admission(value) => {
                if !repository
                    .active_incarnations
                    .contains(&value.workspace_incarnation)
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "job admission references an unknown incarnation".into(),
                    });
                }
                let key = DurableJobKey {
                    workspace_incarnation: value.workspace_incarnation.clone(),
                    job_id: value.job_id,
                };
                if repository
                    .admissions
                    .insert(key, value.grant_revision)
                    .is_some()
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "job has more than one admission commitment".into(),
                    });
                }
            }
            ControllerCommitment::Terminal(value) => {
                if !repository
                    .active_incarnations
                    .contains(&value.workspace_incarnation)
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "terminal commitment references an unknown or retired workspace"
                            .into(),
                    });
                }
                let key = DurableJobKey {
                    workspace_incarnation: value.workspace_incarnation.clone(),
                    job_id: value.job_id,
                };
                let Some(grant_revision) = repository.admissions.get(&key) else {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "terminal commitment precedes job admission".into(),
                    });
                };
                if *grant_revision != value.grant_revision {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "terminal grant revision differs from admission".into(),
                    });
                }
                if !repository.terminals.insert(key) {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "job has more than one terminal commitment".into(),
                    });
                }
            }
            ControllerCommitment::Checkpoint(value) => {
                if !repository
                    .active_incarnations
                    .contains(&value.origin_incarnation)
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "checkpoint references an unknown origin incarnation".into(),
                    });
                }
                let previous = repository
                    .last_barriers
                    .entry(value.origin_incarnation.clone())
                    .or_insert(0);
                if value.barrier_id <= *previous {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "checkpoint barrier is not monotonic for its origin".into(),
                    });
                }
                *previous = value.barrier_id;
                if repository
                    .checkpoints
                    .insert(
                        value.checkpoint_id.clone(),
                        value.origin_incarnation.clone(),
                    )
                    .is_some()
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "checkpoint id is not unique within its repository".into(),
                    });
                }
            }
            ControllerCommitment::Fork(value) => {
                if !repository
                    .active_incarnations
                    .contains(&value.source_incarnation)
                    || repository
                        .introduced_incarnations
                        .contains(&value.destination_incarnation)
                    || repository
                        .retired_incarnations
                        .contains(&value.destination_incarnation)
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "fork lineage parent is absent or destination already exists"
                            .into(),
                    });
                }
                repository
                    .introduced_incarnations
                    .insert(value.destination_incarnation.clone());
                repository
                    .active_incarnations
                    .insert(value.destination_incarnation.clone());
            }
            ControllerCommitment::Restore(value) => {
                if repository.checkpoints.get(&value.source_checkpoint)
                    != Some(&value.source_incarnation)
                    || !repository
                        .active_incarnations
                        .contains(&value.source_incarnation)
                    || repository
                        .introduced_incarnations
                        .contains(&value.destination_incarnation)
                    || repository
                        .retired_incarnations
                        .contains(&value.destination_incarnation)
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message:
                            "restore lineage checkpoint is absent, source is retired, or destination already exists"
                                .into(),
                    });
                }
                repository
                    .active_incarnations
                    .remove(&value.source_incarnation);
                repository
                    .retired_incarnations
                    .insert(value.source_incarnation.clone());
                repository
                    .introduced_incarnations
                    .insert(value.destination_incarnation.clone());
                repository
                    .active_incarnations
                    .insert(value.destination_incarnation.clone());
            }
        }
    }
    Ok(context)
}

pub fn reconcile_commitments(
    recovery: &RecoveryReport,
    prior: &CommitmentPriorContext,
    commitments: &[ControllerCommitment],
) -> Result<CommitmentPriorContext, ArtifactError> {
    let context = validate_commitments(prior, commitments)?;
    for commitment in commitments {
        match commitment {
            ControllerCommitment::Admission(value) => {
                let matched = recovery.frames.iter().any(|frame| {
                    matches!(
                        &frame.record,
                        ProtectedRecord::Job(record)
                            if record.repo_id == value.repo_id
                                && record.workspace_incarnation == value.workspace_incarnation
                                && record.job_id == value.job_id
                                && record.grant_revision == value.grant_revision
                                && record.state == JobState::Running
                    )
                });
                if !matched {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "admission commitment has no protected admission record".into(),
                    });
                }
            }
            ControllerCommitment::Terminal(value) => {
                let matched = recovery.frames.iter().any(|frame| {
                    matches!(
                        &frame.record,
                        ProtectedRecord::Job(record)
                            if frame.batch_sha256 == value.batch_sha256
                                && record.repo_id == value.repo_id
                                && record.workspace_incarnation == value.workspace_incarnation
                                && record.job_id == value.job_id
                                && record.state == value.state
                                && record.grant_revision == value.grant_revision
                                && record.stdout.bytes == value.stdout_bytes
                                && record.stdout.sha256 == value.stdout_sha256
                                && record.stderr.bytes == value.stderr_bytes
                                && record.stderr.sha256 == value.stderr_sha256
                                && record.output_limit == value.output_limit
                    )
                });
                if !matched {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "terminal commitment does not match protected batch and streams"
                            .into(),
                    });
                }
            }
            ControllerCommitment::Checkpoint(value) => {
                let matched = recovery.frames.iter().any(|frame| {
                    matches!(
                        &frame.record,
                        ProtectedRecord::CheckpointManifest(record)
                            if frame.batch_sha256 == value.manifest_batch_sha256
                                && record.repo_id == value.repo_id
                                && record.origin_incarnation == value.origin_incarnation
                                && record.barrier_id == value.barrier_id
                    )
                });
                if !matched {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "checkpoint commitment does not match protected manifest".into(),
                    });
                }
            }
            ControllerCommitment::WorkspaceIntroduced(_)
            | ControllerCommitment::WorkspaceRetired(_)
            | ControllerCommitment::Fork(_)
            | ControllerCommitment::Restore(_) => {}
        }
    }
    Ok(context)
}

fn require_job_columns(batch: &RecordBatch, row: usize) -> Result<(), ArtifactError> {
    const REQUIRED: &[usize] = &[
        3, 4, 5, 6, 7, 8, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24, 25, 32,
    ];
    for &column in REQUIRED {
        if batch.column(column).is_null(row) {
            return Err(ArtifactError::Arrow(format!(
                "required job column {} is null",
                batch.schema().field(column).name()
            )));
        }
    }
    for column in 26..30 {
        if !batch.column(column).is_null(row) {
            return Err(ArtifactError::Arrow(format!(
                "job record contains manifest column {}",
                batch.schema().field(column).name()
            )));
        }
    }
    Ok(())
}

fn require_protected_columns(
    batch: &RecordBatch,
    row: usize,
    selected: &[usize],
) -> Result<(), ArtifactError> {
    for column in 3..batch.num_columns() {
        let should_be_present = selected.contains(&column);
        if should_be_present == batch.column(column).is_null(row) {
            return Err(ArtifactError::Arrow(format!(
                "protected record {} nullability mismatch for {}",
                string(batch, 0)?.value(row),
                batch.schema().field(column).name()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::dto::{
        AdmissionCommitment, CONTROLLER_COMMITMENT_VERSION, CheckpointCommitment, RestoreCommitment,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn repo() -> RepoId {
        RepoId::parse("acme/widget").unwrap()
    }

    fn incarnation() -> WorkspaceIncarnation {
        WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap()
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-artifact-unit-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        root
    }

    fn store_at(root: &Path, config: ArtifactConfig) -> ArtifactStore {
        ArtifactStore::open(root, repo(), incarnation(), config).unwrap()
    }

    fn inline_stream(bytes: &[u8]) -> StreamInfo {
        StreamInfo {
            storage: OutputStorage::Captured {
                artifact: ProtectedOutput::Inline {
                    data: BinaryData::new(bytes.to_vec()).unwrap(),
                },
            },
            bytes: bytes.len() as u64,
            sha256: Sha256Digest::compute(bytes),
            summary: safe_output_summary(),
        }
    }

    fn valid_job_record(job_id: u64) -> JobArtifactRecord {
        JobArtifactRecord {
            repo_id: repo(),
            workspace_incarnation: incarnation(),
            job_id: JobId::new(job_id).unwrap(),
            sequence: 1,
            state: JobState::Exited,
            grant_revision: 1,
            argv: vec!["true".into()],
            output_limit: None,
            stdout: inline_stream(b""),
            stderr: inline_stream(b""),
        }
    }

    fn append_protected_record(root: &Path, record: ProtectedRecord) {
        let batch = protected_record_to_batch(&record).unwrap();
        let payload = encode_batch(&batch).unwrap();
        append_framed_batch(
            &records_path(root),
            &payload,
            Sha256Digest::compute(&payload),
        )
        .unwrap();
    }

    #[test]
    fn record_and_manifest_validation_reject_every_invalid_boundary() {
        let valid = valid_job_record(1);
        let mut invalid_records = Vec::new();

        let mut zero_sequence = valid.clone();
        zero_sequence.sequence = 0;
        invalid_records.push(("zero sequence", zero_sequence));

        let mut missing_limit = valid.clone();
        missing_limit.state = JobState::OutputLimit;
        invalid_records.push(("missing output-limit evidence", missing_limit));

        let mut unexpected_limit = valid.clone();
        unexpected_limit.output_limit = Some(OutputLimitInfo {
            limit_bytes: 4,
            crossing_bytes: 5,
        });
        invalid_records.push(("unexpected output-limit evidence", unexpected_limit));

        for crossing_bytes in [3, 4] {
            let mut non_crossing_limit = valid.clone();
            non_crossing_limit.state = JobState::OutputLimit;
            non_crossing_limit.output_limit = Some(OutputLimitInfo {
                limit_bytes: 4,
                crossing_bytes,
            });
            invalid_records.push(("non-crossing output limit", non_crossing_limit));
        }

        let mut wrong_protected_path = valid.clone();
        wrong_protected_path.stdout = StreamInfo {
            storage: OutputStorage::Captured {
                artifact: ProtectedOutput::File {
                    path: WorkspacePath::new(".cowshed/job/2/out").unwrap(),
                },
            },
            bytes: 0,
            sha256: Sha256Digest::compute(b""),
            summary: safe_output_summary(),
        };
        invalid_records.push(("wrong protected path", wrong_protected_path));

        for (case, record) in invalid_records {
            assert!(
                matches!(record.validate(), Err(ArtifactError::Integrity { .. })),
                "{case} must be rejected"
            );
            assert!(
                matches!(
                    ProtectedRecord::Job(record).validate(),
                    Err(ArtifactError::Integrity { .. })
                ),
                "{case} must also be rejected through protected-record validation"
            );
        }

        let visible = VisibleJobCommitment {
            workspace_incarnation: incarnation(),
            job_id: JobId::new(1).unwrap(),
            state: JobState::Exited,
            stdout: VisibleStreamCommitment::from_stream(&inline_stream(b"")),
            stderr: VisibleStreamCommitment::from_stream(&inline_stream(b"")),
        };
        let valid_manifest = CheckpointManifestRecord {
            version: RECORD_SCHEMA_VERSION as u16,
            repo_id: repo(),
            origin_incarnation: incarnation(),
            barrier_id: 1,
            visible_jobs: vec![visible.clone()],
            records_sha256: Sha256Digest::compute(RECORD_MAGIC),
        };
        valid_manifest.validate().unwrap();

        let mut manifests = Vec::new();
        for (version, barrier_id) in [(0, 1), (1, 0), (0, 0)] {
            let mut manifest = valid_manifest.clone();
            manifest.version = version;
            manifest.barrier_id = barrier_id;
            manifests.push(("version/barrier boundary", manifest));
        }

        let mut duplicate = valid_manifest.clone();
        duplicate.visible_jobs.push(visible.clone());
        manifests.push(("duplicate visible job", duplicate));

        let mut wrong_path = valid_manifest.clone();
        wrong_path.visible_jobs[0].stdout = VisibleStreamCommitment {
            storage_kind: VisibleStorageKind::CapturedFile,
            bytes: 0,
            sha256: Sha256Digest::compute(b""),
            protected_path: Some(WorkspacePath::new(".cowshed/job/2/out").unwrap()),
        };
        manifests.push(("wrong visible protected path", wrong_path));

        let mut missing_path = valid_manifest.clone();
        missing_path.visible_jobs[0].stdout.storage_kind = VisibleStorageKind::CapturedFile;
        manifests.push(("file commitment without path", missing_path));

        let mut running_inline = valid_manifest.clone();
        running_inline.visible_jobs[0].state = JobState::Running;
        manifests.push(("running inline prefix", running_inline));

        let mut running_stdout_only = valid_manifest.clone();
        running_stdout_only.visible_jobs[0].state = JobState::Running;
        running_stdout_only.visible_jobs[0].stdout = VisibleStreamCommitment {
            storage_kind: VisibleStorageKind::CapturedFile,
            bytes: 0,
            sha256: Sha256Digest::compute(b""),
            protected_path: Some(WorkspacePath::new(".cowshed/job/1/out").unwrap()),
        };
        manifests.push(("running with only stdout durable", running_stdout_only));

        let mut running_stderr_only = valid_manifest.clone();
        running_stderr_only.visible_jobs[0].state = JobState::Running;
        running_stderr_only.visible_jobs[0].stderr = VisibleStreamCommitment {
            storage_kind: VisibleStorageKind::CapturedFile,
            bytes: 0,
            sha256: Sha256Digest::compute(b""),
            protected_path: Some(WorkspacePath::new(".cowshed/job/1/err").unwrap()),
        };
        manifests.push(("running with only stderr durable", running_stderr_only));

        let mut inline_with_path = valid_manifest.clone();
        inline_with_path.visible_jobs[0].stdout.protected_path =
            Some(WorkspacePath::new(".cowshed/job/1/out").unwrap());
        manifests.push(("inline commitment with protected path", inline_with_path));

        for (case, manifest) in manifests {
            assert!(
                matches!(manifest.validate(), Err(ArtifactError::Integrity { .. })),
                "{case} must be rejected"
            );
            assert!(
                matches!(
                    ProtectedRecord::CheckpointManifest(manifest).validate(),
                    Err(ArtifactError::Integrity { .. })
                ),
                "{case} must also be rejected through protected-record validation"
            );
        }
    }

    #[test]
    fn terminal_records_and_tokens_reject_duplicate_stale_and_foreign_use() {
        let root = temp_root("append-guards");
        let mut store = store_at(&root, ArtifactConfig::default());
        let job_id = store.next_job_id().unwrap();
        let token = store
            .begin_job(job_id, 1, &["true".into()], OutputTargets::default())
            .unwrap();
        let namespace = token.namespace;
        let sealed = store.finish(token, JobState::Exited).unwrap();
        let records = records_path(&root);
        let sealed_len = fs::metadata(&records).unwrap().len();

        assert!(matches!(
            store.append_record(sealed.record.clone()),
            Err(ArtifactError::Integrity { message, .. })
                if message.contains("duplicate terminal")
        ));
        assert_eq!(fs::metadata(&records).unwrap().len(), sealed_len);

        let stale = JobArtifactToken { job_id, namespace };
        assert!(matches!(
            store.append(&stale, StreamKind::Stdout, b"late"),
            Err(ArtifactError::TokenConflict { .. })
        ));

        let foreign_root = temp_root("foreign-token");
        let mut foreign = store_at(&foreign_root, ArtifactConfig::default());
        let foreign_job_id = foreign.next_job_id().unwrap();
        let foreign_token = foreign
            .begin_job(
                foreign_job_id,
                1,
                &["true".into()],
                OutputTargets::default(),
            )
            .unwrap();
        assert!(matches!(
            store.append(&foreign_token, StreamKind::Stdout, b"foreign"),
            Err(ArtifactError::TokenConflict { message, .. })
                if message.contains("another artifact store")
        ));
        foreign.abort(foreign_token).unwrap();

        drop(store);
        fs::remove_dir_all(root).unwrap();
        fs::remove_dir_all(foreign_root).unwrap();
    }

    #[test]
    fn checkpoint_hashes_missing_and_exactly_empty_records_as_magic_prefix() {
        for (case, precreate_empty) in [("missing", false), ("empty", true)] {
            let root = temp_root(case);
            let mut store = store_at(&root, ArtifactConfig::default());
            if precreate_empty {
                use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

                let path = records_path(&root);
                ensure_private_job_root(&root).unwrap();
                OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o600)
                    .open(&path)
                    .unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
                assert_eq!(fs::metadata(&path).unwrap().len(), 0);
            }

            let checkpoint = store.checkpoint(1).unwrap();
            assert_eq!(
                checkpoint.record.records_sha256,
                Sha256Digest::compute(RECORD_MAGIC),
                "{case} records must commit the initialized magic prefix"
            );
            drop(store);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn checkpoint_preserves_non_not_found_metadata_errors() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_root("checkpoint-metadata-error");
        let mut store = store_at(&root, ArtifactConfig::default());
        let job_root = ensure_private_job_root(&root).unwrap();
        fs::set_permissions(&job_root, fs::Permissions::from_mode(0o000)).unwrap();

        let error = store.checkpoint(1).unwrap_err();
        assert!(
            matches!(error, ArtifactError::Io { path, .. } if path == records_path(&root)),
            "checkpoint must return the records metadata error without attempting publication"
        );

        fs::set_permissions(&job_root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reopened_checkpoint_barriers_are_scoped_to_their_manifest_origin() {
        let historical = WorkspaceIncarnation::new("1198f2c0b7e34dc795f17b238b331c80").unwrap();
        let historical_root = temp_root("historical-manifest");
        append_protected_record(
            &historical_root,
            ProtectedRecord::CheckpointManifest(CheckpointManifestRecord {
                version: RECORD_SCHEMA_VERSION as u16,
                repo_id: repo(),
                origin_incarnation: historical.clone(),
                barrier_id: 50,
                visible_jobs: Vec::new(),
                records_sha256: Sha256Digest::compute(RECORD_MAGIC),
            }),
        );
        assert!(matches!(
            ArtifactStore::open(
                &historical_root,
                repo(),
                incarnation(),
                ArtifactConfig::default()
            ),
            Err(ArtifactError::Integrity { .. })
        ));
        let mut historical_store = ArtifactStore::open(
            &historical_root,
            repo(),
            incarnation(),
            ArtifactConfig {
                admitted_historical_incarnations: BTreeSet::from([historical]),
                ..ArtifactConfig::default()
            },
        )
        .unwrap();
        historical_store.checkpoint(1).unwrap();
        drop(historical_store);
        fs::remove_dir_all(historical_root).unwrap();

        let current_root = temp_root("current-manifest");
        let mut current_store = store_at(&current_root, ArtifactConfig::default());
        current_store.checkpoint(7).unwrap();
        drop(current_store);
        let mut reopened = store_at(&current_root, ArtifactConfig::default());
        assert!(matches!(
            reopened.checkpoint(7),
            Err(ArtifactError::Integrity { .. })
        ));
        reopened.checkpoint(8).unwrap();
        drop(reopened);
        fs::remove_dir_all(current_root).unwrap();
    }

    #[test]
    fn buffer_budget_reservation_boundaries_never_wrap_or_overcommit() {
        let cases = [
            (0, 0, 0, true, 0),
            (0, 0, 1, false, 0),
            (0, 1, 1, true, 1),
            (1, 1, 0, true, 1),
            (1, 1, 1, false, 1),
            (3, 4, 1, true, 4),
            (3, 4, 2, false, 3),
            (usize::MAX, usize::MAX, 1, false, usize::MAX),
        ];
        for (used, limit, requested, accepted, expected_used) in cases {
            let mut budget = BufferBudget { used, limit };
            assert_eq!(
                budget.try_reserve(requested),
                accepted,
                "used={used}, limit={limit}, requested={requested}"
            );
            assert_eq!(budget.used, expected_used);
        }

        let mut budget = BufferBudget { used: 4, limit: 4 };
        for release in [0, 1, 3] {
            budget.release(release);
        }
        assert_eq!(budget.used, 0);

        let cases = [
            (3, 5, 3, 5, true, 5),
            (3, 4, 3, 5, false, 3),
            (3, 5, 3, 2, true, 2),
            (5, 5, 5, 2, true, 2),
            (3, 5, 3, 3, true, 3),
        ];
        for (used, limit, reserved, actual, accepted, expected_used) in cases {
            let mut budget = BufferBudget { used, limit };
            assert_eq!(
                reconcile_capacity_growth(&mut budget, reserved, actual),
                accepted,
                "used={used}, limit={limit}, reserved={reserved}, actual={actual}"
            );
            assert_eq!(budget.used, expected_used);
        }
    }

    #[test]
    fn framed_batch_limit_accepts_exact_boundary_and_rejects_first_byte_over() {
        let cases = [
            (2 * 1024 + 9, true),
            (MAX_RECORD_BATCH_BYTES as usize, true),
            (MAX_RECORD_BATCH_BYTES as usize + 1, false),
        ];
        for (length, accepted) in cases {
            let root = temp_root(&format!("frame-size-{length}"));
            let path = records_path(&root);
            let payload = vec![0x5a; length];
            let result = append_framed_batch(&path, &payload, Sha256Digest::compute(&payload));
            assert_eq!(
                result.is_ok(),
                accepted,
                "payload length {length} has the wrong admission result"
            );
            if accepted {
                assert_eq!(
                    fs::metadata(&path).unwrap().len(),
                    (RECORD_MAGIC.len() + FRAME_OVERHEAD_BYTES + length) as u64
                );
            }
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn protected_create_and_open_descriptors_enforce_metadata_and_cloexec() {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let root = temp_root("protected-descriptors");
        let job_id = JobId::new(1).unwrap();
        let mut file = create_protected_file(&root, job_id, StreamKind::Stdout).unwrap();
        let path = protected_absolute(&root, job_id, StreamKind::Stdout);
        let metadata = file.metadata().unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(metadata.len(), 0);
        assert_ne!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        file.write_all(b"protected").unwrap();
        seal_file(file, &path).unwrap();

        let relative = protected_relative(job_id, StreamKind::Stdout).unwrap();
        let opened = open_artifact_no_follow(&root, &relative).unwrap();
        assert_ne!(
            unsafe { libc::fcntl(opened.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let metadata = opened.metadata().unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o400);
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(metadata.len(), b"protected".len() as u64);
        drop(opened);
        use std::os::unix::fs::symlink;
        let hash_alias = root.join("hash-alias");
        symlink(&path, &hash_alias).unwrap();
        assert!(matches!(
            hash_file_incrementally(&hash_alias),
            Err(ArtifactError::Io { .. })
        ));
        fs::remove_dir_all(root).unwrap();
    }

    fn admission(order: u64) -> ControllerCommitment {
        ControllerCommitment::Admission(AdmissionCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order,
            repo_id: repo(),
            workspace_incarnation: incarnation(),
            job_id: JobId::new(1).unwrap(),
            grant_revision: 1,
        })
    }

    #[test]
    fn commitment_order_rejects_gaps_and_overflow() {
        let prior = CommitmentPriorContext::new(repo(), [incarnation()]);
        assert!(matches!(
            validate_commitments(&prior, &[admission(2)]),
            Err(ArtifactError::Integrity { .. })
        ));

        let mut exhausted = prior;
        exhausted.last_order = u64::MAX;
        assert!(matches!(
            validate_commitments(&exhausted, &[admission(u64::MAX)]),
            Err(ArtifactError::Integrity { .. })
        ));
    }

    #[test]
    fn failed_frame_append_rolls_back_before_later_append() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-frame-rollback-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).unwrap();
        let path = records_path(&root);
        let first = b"first complete payload";
        append_framed_batch(&path, first, Sha256Digest::compute(first)).unwrap();
        let before = fs::read(&path).unwrap();

        let failed = b"must not remain";
        assert!(matches!(
            append_framed_batch_impl(&path, failed, Sha256Digest::compute(failed), Some(11),),
            Err(ArtifactError::Io { .. })
        ));
        assert_eq!(fs::read(&path).unwrap(), before);

        let later = b"later complete payload";
        append_framed_batch(&path, later, Sha256Digest::compute(later)).unwrap();
        let after = fs::read(&path).unwrap();
        assert_eq!(&after[..before.len()], before.as_slice());
        assert_eq!(
            &after[before.len()..before.len() + BATCH_MAGIC.len()],
            BATCH_MAGIC
        );
        assert!(!after.windows(failed.len()).any(|window| window == failed));

        fs::remove_file(&path).unwrap();
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn buffer_budget_tracks_actual_capacity_and_releases_on_spill_boundaries() {
        let root = temp_root("buffer-capacity");
        let mut budget = BufferBudget { used: 0, limit: 4 };
        let mut stream = StreamWriterState::new(StreamKind::Stdout, StreamTarget::Captured);
        let job_id = JobId::new(1).unwrap();
        stream
            .append(&root, 4, &mut budget, job_id, b"abc")
            .unwrap();
        assert_eq!(stream.reserved, stream.buffer.as_ref().unwrap().capacity());
        assert_eq!(budget.used, stream.reserved);
        assert!(budget.used <= 4);
        stream.append(&root, 4, &mut budget, job_id, b"d").unwrap();
        assert_eq!(stream.buffer.as_ref().unwrap().len(), 4);
        assert_eq!(budget.used, stream.reserved);
        stream.append(&root, 4, &mut budget, job_id, b"e").unwrap();
        assert!(stream.buffer.is_none());
        assert_eq!(budget.used, 0);
        assert_eq!(fs::read(root.join(".cowshed/job/1/out")).unwrap(), b"abcde");
        drop(stream);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn first_create_rollback_keeps_private_retryable_records_layout() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_root("first-rollback");
        let path = records_path(&root);
        let failed = b"failed-first-frame";
        assert!(matches!(
            append_framed_batch_impl(&path, failed, Sha256Digest::compute(failed), Some(0)),
            Err(ArtifactError::Io { .. })
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        assert_eq!(
            fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let retry = b"retry-frame";
        append_framed_batch(&path, retry, Sha256Digest::compute(retry)).unwrap();
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.starts_with(RECORD_MAGIC));
        assert_eq!(
            &bytes[RECORD_MAGIC.len()..RECORD_MAGIC.len() + BATCH_MAGIC.len()],
            BATCH_MAGIC
        );

        let too_large = vec![0_u8; MAX_RECORD_BATCH_BYTES as usize + 1];
        assert!(matches!(
            append_framed_batch(&path, &too_large, Sha256Digest::compute(&too_large)),
            Err(ArtifactError::Arrow(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn streaming_recovery_checks_header_complement_lengths_and_incomplete_tail() {
        let root = temp_root("stream-headers");
        let mut store = store_at(&root, ArtifactConfig::default());
        let token = store
            .begin_job(
                JobId::new(1).unwrap(),
                1,
                &["true".into()],
                OutputTargets::default(),
            )
            .unwrap();
        store.abort(token).unwrap();
        drop(store);
        let path = records_path(&root);
        let valid = fs::read(&path).unwrap();
        let valid_len = valid.len();

        let mut bad_complement = valid.clone();
        bad_complement[RECORD_MAGIC.len() + 16] ^= 1;
        fs::write(&path, &bad_complement).unwrap();
        assert!(matches!(
            recover_records(&path),
            Err(ArtifactError::Integrity { .. })
        ));

        let mut excessive = valid.clone();
        let length = MAX_RECORD_BATCH_BYTES + 1;
        excessive[RECORD_MAGIC.len() + 8..RECORD_MAGIC.len() + 16]
            .copy_from_slice(&length.to_le_bytes());
        excessive[RECORD_MAGIC.len() + 16..RECORD_MAGIC.len() + 24]
            .copy_from_slice(&(!length).to_le_bytes());
        fs::write(&path, &excessive).unwrap();
        assert!(matches!(
            recover_records(&path),
            Err(ArtifactError::Integrity { .. })
        ));

        let mut incomplete = valid;
        let declared = 10_u64;
        incomplete.extend_from_slice(BATCH_MAGIC);
        incomplete.extend_from_slice(&declared.to_le_bytes());
        incomplete.extend_from_slice(&(!declared).to_le_bytes());
        incomplete.extend_from_slice(b"abc");
        fs::write(&path, &incomplete).unwrap();
        let recovered = recover_records(&path).unwrap();
        assert_eq!(recovered.truncated_bytes, (FRAME_HEADER_BYTES + 3) as u64);
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_len as u64);

        let mut invalid_tail = fs::read(&path).unwrap();
        invalid_tail.extend_from_slice(b"BAD");
        fs::write(&path, invalid_tail).unwrap();
        assert!(matches!(
            recover_records(&path),
            Err(ArtifactError::Integrity { .. })
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovery_rejects_wrong_manifest_prefix_and_duplicate_terminal_ids() {
        let wrong_root = temp_root("wrong-prefix");
        let mut wrong_store = store_at(&wrong_root, ArtifactConfig::default());
        let wrong_token = wrong_store
            .begin_job(
                JobId::new(1).unwrap(),
                1,
                &["true".into()],
                OutputTargets::default(),
            )
            .unwrap();
        let wrong = CheckpointManifestRecord {
            version: RECORD_SCHEMA_VERSION as u16,
            repo_id: repo(),
            origin_incarnation: incarnation(),
            barrier_id: 1,
            visible_jobs: Vec::new(),
            records_sha256: Sha256Digest::compute(b"wrong"),
        };
        let batch = protected_record_to_batch(&ProtectedRecord::CheckpointManifest(wrong)).unwrap();
        let payload = encode_batch(&batch).unwrap();
        append_framed_batch(
            &records_path(&wrong_root),
            &payload,
            Sha256Digest::compute(&payload),
        )
        .unwrap();
        assert!(matches!(
            recover_records(&records_path(&wrong_root)),
            Err(ArtifactError::Integrity { message, .. })
                if message.contains("prefix digest")
        ));
        wrong_store.abort(wrong_token).unwrap();
        drop(wrong_store);
        fs::remove_dir_all(wrong_root).unwrap();

        let duplicate_root = temp_root("duplicate-terminal");
        let mut duplicate_store = store_at(&duplicate_root, ArtifactConfig::default());
        let duplicate_token = duplicate_store
            .begin_job(
                JobId::new(1).unwrap(),
                1,
                &["true".into()],
                OutputTargets::default(),
            )
            .unwrap();
        let sealed = duplicate_store
            .finish(duplicate_token, JobState::Exited)
            .unwrap();
        let mut duplicate = sealed.record.clone();
        duplicate.sequence += 1;
        let batch = protected_record_to_batch(&ProtectedRecord::Job(duplicate)).unwrap();
        let payload = encode_batch(&batch).unwrap();
        append_framed_batch(
            &records_path(&duplicate_root),
            &payload,
            Sha256Digest::compute(&payload),
        )
        .unwrap();
        assert!(matches!(
            recover_records(&records_path(&duplicate_root)),
            Err(ArtifactError::Integrity { message, .. })
                if message.contains("duplicate terminal")
        ));
        drop(duplicate_store);
        fs::remove_dir_all(duplicate_root).unwrap();
    }

    #[test]
    fn checkpoint_snapshots_only_messages_sequenced_before_the_barrier() {
        let root = temp_root("actor-checkpoint-order");
        let mut store = store_at(&root, ArtifactConfig::default());
        let token = store
            .begin_job(
                JobId::new(1).unwrap(),
                1,
                &["true".into()],
                OutputTargets::default(),
            )
            .unwrap();
        store
            .append(&token, StreamKind::Stdout, b"before-checkpoint")
            .unwrap();
        let first = store.checkpoint(1).unwrap();
        let first_visible = &first.record.visible_jobs[0].stdout;
        assert_eq!(first_visible.bytes, b"before-checkpoint".len() as u64);
        assert_eq!(
            first_visible.sha256,
            Sha256Digest::compute(b"before-checkpoint")
        );

        store.append(&token, StreamKind::Stdout, b"-after").unwrap();
        let second = store.checkpoint(2).unwrap();
        let second_visible = &second.record.visible_jobs[0].stdout;
        assert_eq!(
            second_visible.bytes,
            b"before-checkpoint-after".len() as u64
        );
        assert_eq!(
            second_visible.sha256,
            Sha256Digest::compute(b"before-checkpoint-after")
        );
        assert_eq!(
            first.record.visible_jobs[0].stdout.bytes,
            b"before-checkpoint".len() as u64
        );

        let sealed = store.finish(token, JobState::Exited).unwrap();
        assert_eq!(
            read_stream(&root, &sealed.record.stdout).unwrap(),
            b"before-checkpoint-after"
        );
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn redirect_fcntl_failure_is_typed_and_precedes_admission() {
        let root = temp_root("fcntl-failure");
        let source = WorkspacePath::new("redirect.log").unwrap();
        let descriptor = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(root.join(source.as_path()))
            .unwrap();
        let mut store = store_at(&root, ArtifactConfig::default());
        FAIL_REDIRECT_FCNTL.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            store.begin_job(
                JobId::new(1).unwrap(),
                1,
                &["true".into()],
                OutputTargets {
                    stdout: StreamTarget::Redirect { source, descriptor },
                    stderr: StreamTarget::Captured,
                },
            ),
            Err(ArtifactError::RedirectDescriptor { .. })
        ));
        assert!(!records_path(&root).exists());
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn commitment_validation_rejects_missing_ids_and_checkpoint_lineage() {
        let prior = CommitmentPriorContext::new(repo(), [incarnation()]);
        let digest = Sha256Digest::compute(b"digest");
        let terminal = ControllerCommitment::Terminal(TerminalCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 1,
            repo_id: repo(),
            workspace_incarnation: incarnation(),
            job_id: JobId::new(1).unwrap(),
            state: JobState::Exited,
            grant_revision: 1,
            stdout_bytes: 0,
            stdout_sha256: Sha256Digest::compute(&[]),
            stderr_bytes: 0,
            stderr_sha256: Sha256Digest::compute(&[]),
            batch_sha256: digest,
            output_limit: None,
        });
        assert!(matches!(
            validate_commitments(&prior, &[terminal]),
            Err(ArtifactError::Integrity { .. })
        ));

        let invalid_id = ControllerCommitment::Checkpoint(CheckpointCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 1,
            repo_id: repo(),
            origin_incarnation: incarnation(),
            checkpoint_id: String::new(),
            barrier_id: 1,
            manifest_batch_sha256: digest,
        });
        assert!(matches!(
            validate_commitments(&prior, &[invalid_id]),
            Err(ArtifactError::Dto(_))
        ));

        let destination = WorkspaceIncarnation::new("1198f2c0b7e34dc795f17b238b331c80").unwrap();
        let missing_lineage = ControllerCommitment::Restore(RestoreCommitment {
            version: CONTROLLER_COMMITMENT_VERSION,
            order: 1,
            repo_id: repo(),
            source_checkpoint: "absent".into(),
            source_incarnation: incarnation(),
            destination_incarnation: destination,
        });
        assert!(matches!(
            validate_commitments(&prior, &[missing_lineage]),
            Err(ArtifactError::Integrity { .. })
        ));

        let checkpoints = [
            ControllerCommitment::Checkpoint(CheckpointCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order: 1,
                repo_id: repo(),
                origin_incarnation: incarnation(),
                checkpoint_id: "one".into(),
                barrier_id: 2,
                manifest_batch_sha256: digest,
            }),
            ControllerCommitment::Checkpoint(CheckpointCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order: 2,
                repo_id: repo(),
                origin_incarnation: incarnation(),
                checkpoint_id: "two".into(),
                barrier_id: 2,
                manifest_batch_sha256: digest,
            }),
        ];
        assert!(matches!(
            validate_commitments(&prior, &checkpoints),
            Err(ArtifactError::Integrity { .. })
        ));
    }
}
