use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
    AdmissionCommitment, BinaryData, CheckpointCommitment, ControllerCommitment, DtoError,
    ForkCommitment, JobId, JobState, MAX_INLINE_OUTPUT_BYTES, OutputLimitInfo, OutputStorage,
    OutputSummary, ProtectedOutput, RestoreCommitment, Sha256Digest, StreamInfo,
    TerminalCommitment, WorkspacePath,
};
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;
use crate::storage::verify_no_symlinks;

const RECORD_MAGIC: &[u8; 8] = b"CSARROW1";
const BATCH_MAGIC: &[u8; 8] = b"CSBATCH1";
const BATCH_TRAILER: &[u8; 8] = b"CSEND001";
const FRAME_HEADER_BYTES: usize = 24;
const FRAME_OVERHEAD_BYTES: usize = FRAME_HEADER_BYTES + 32 + BATCH_TRAILER.len();
const MAX_RECORD_BATCH_BYTES: u64 = 8 * 1024 * 1024;
const RECORD_SCHEMA_VERSION: u64 = 1;

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
    #[error("combined output quota {limit_bytes} crossed at {crossing_bytes} bytes")]
    OutputQuotaExceeded {
        limit_bytes: u64,
        crossing_bytes: u64,
    },
    #[error("artifact writer is poisoned: {0}")]
    WriterPoisoned(String),
    #[error("inline buffer allocation failed")]
    BufferAllocation,
    #[error("invalid terminal artifact state: {0}")]
    InvalidTerminalState(&'static str),
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
    pub admitted_historical_incarnations: BTreeSet<WorkspaceIncarnation>,
}

impl ArtifactConfig {
    pub fn validate(&self) -> Result<(), ArtifactError> {
        if self.inline_cap_bytes > MAX_INLINE_OUTPUT_BYTES {
            return Err(ArtifactError::InvalidConfig(
                "inline cap exceeds the bounded public DTO limit",
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
    /// A trusted supervisor-owned descriptor that interposes the live redirect.
    ///
    /// It must be opened for both reading and writing. The artifact layer never reopens
    /// `source`; terminal sealing seeks and copies only this descriptor.
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
    pub stdout: StreamInfo,
    pub stderr: StreamInfo,
}

impl JobArtifactRecord {
    pub fn validate(&self) -> Result<(), ArtifactError> {
        if self.sequence == 0 {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: "record sequence must be positive".into(),
            });
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

#[derive(Clone)]
pub struct ArtifactStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    workspace_root: PathBuf,
    repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation,
    config: ArtifactConfig,
    budget: Mutex<BufferBudget>,
    allocation: Mutex<AllocationState>,
    records_lock: Mutex<()>,
    records_poison: Mutex<Option<ArtifactError>>,
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

#[derive(Debug)]
struct AllocationState {
    next_job_id: u64,
    next_sequence: u64,
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
        let mut recovery = recover_records(&records_path)?;
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
        let next = maximum
            .checked_add(1)
            .ok_or(ArtifactError::InvalidConfig("job id allocation exhausted"))?;
        let next_job_id = JobId::new(next)?;
        recovery.next_job_id = next_job_id;
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

        Ok(Self {
            inner: Arc::new(StoreInner {
                workspace_root,
                repo_id,
                workspace_incarnation,
                budget: Mutex::new(BufferBudget {
                    used: 0,
                    limit: config.supervisor_buffer_budget_bytes,
                }),
                allocation: Mutex::new(AllocationState {
                    next_job_id: next,
                    next_sequence,
                }),
                records_lock: Mutex::new(()),
                records_poison: Mutex::new(None),
                recovery,
                config,
            }),
        })
    }

    pub fn recovery(&self) -> &RecoveryReport {
        &self.inner.recovery
    }

    pub fn buffered_bytes(&self) -> usize {
        self.inner.budget.lock().expect("buffer budget lock").used
    }

    pub fn start_job(
        &self,
        grant_revision: u64,
        targets: OutputTargets,
    ) -> Result<JobArtifactWriter, ArtifactError> {
        let job_id = {
            let mut allocation = self.inner.allocation.lock().expect("allocation lock");
            let job_id = JobId::new(allocation.next_job_id)?;
            allocation.next_job_id = allocation
                .next_job_id
                .checked_add(1)
                .ok_or(ArtifactError::InvalidConfig("job id allocation exhausted"))?;
            job_id
        };

        let empty_stdout = empty_stream(&targets.stdout)?;
        let empty_stderr = empty_stream(&targets.stderr)?;
        let admission = JobArtifactRecord {
            repo_id: self.inner.repo_id.clone(),
            workspace_incarnation: self.inner.workspace_incarnation.clone(),
            job_id,
            sequence: 0,
            state: JobState::Running,
            grant_revision,
            stdout: empty_stdout,
            stderr: empty_stderr,
        };
        self.append_record(admission)?;

        Ok(JobArtifactWriter {
            store: self.clone(),
            job_id,
            grant_revision,
            stdout: StreamWriterState::new(StreamKind::Stdout, targets.stdout),
            stderr: StreamWriterState::new(StreamKind::Stderr, targets.stderr),
            quota: QuotaLedger {
                accepted: 0,
                limit: self.inner.config.combined_output_quota_bytes,
                crossing: None,
            },
            sealed: false,
            poisoned: None,
        })
    }

    fn append_record(
        &self,
        mut record: JobArtifactRecord,
    ) -> Result<(JobArtifactRecord, Sha256Digest), ArtifactError> {
        if let Some(error) = self
            .inner
            .records_poison
            .lock()
            .expect("records poison lock")
            .clone()
        {
            return Err(error);
        }
        let _guard = self.inner.records_lock.lock().expect("records lock");
        let mut allocation = self.inner.allocation.lock().expect("allocation lock");
        record.sequence = allocation.next_sequence;
        record.validate()?;
        let batch = protected_record_to_batch(&ProtectedRecord::Job(record.clone()))?;
        let payload = encode_batch(&batch)?;
        let digest = Sha256Digest::compute(&payload);
        if let Err(error) =
            append_framed_batch(&records_path(&self.inner.workspace_root), &payload, digest)
        {
            if matches!(error, ArtifactError::WriterPoisoned(_)) {
                *self
                    .inner
                    .records_poison
                    .lock()
                    .expect("records poison lock") = Some(error.clone());
            }
            return Err(error);
        }
        allocation.next_sequence =
            allocation
                .next_sequence
                .checked_add(1)
                .ok_or(ArtifactError::InvalidConfig(
                    "record sequence allocation exhausted",
                ))?;
        Ok((record, digest))
    }

    pub fn append_checkpoint_manifest(
        &self,
        record: CheckpointManifestRecord,
    ) -> Result<Sha256Digest, ArtifactError> {
        if let Some(error) = self
            .inner
            .records_poison
            .lock()
            .expect("records poison lock")
            .clone()
        {
            return Err(error);
        }
        if record.repo_id != self.inner.repo_id
            || record.origin_incarnation != self.inner.workspace_incarnation
        {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: "checkpoint manifest identity does not match current workspace".into(),
            });
        }
        record.validate()?;
        let _guard = self.inner.records_lock.lock().expect("records lock");
        let batch = protected_record_to_batch(&ProtectedRecord::CheckpointManifest(record))?;
        let payload = encode_batch(&batch)?;
        let digest = Sha256Digest::compute(&payload);
        if let Err(error) =
            append_framed_batch(&records_path(&self.inner.workspace_root), &payload, digest)
        {
            if matches!(error, ArtifactError::WriterPoisoned(_)) {
                *self
                    .inner
                    .records_poison
                    .lock()
                    .expect("records poison lock") = Some(error.clone());
            }
            return Err(error);
        }
        Ok(digest)
    }

    pub fn records_prefix_sha256(&self) -> Result<Sha256Digest, ArtifactError> {
        hash_file_incrementally(&records_path(&self.inner.workspace_root))
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
        summary: OutputSummary {
            version: 1,
            text: String::new(),
            truncated: false,
        },
    })
}

#[derive(Debug)]
struct QuotaLedger {
    accepted: u64,
    limit: u64,
    crossing: Option<u64>,
}

impl QuotaLedger {
    fn preview(&self, requested: usize) -> Result<QuotaAdmission, ArtifactError> {
        if let Some(crossing_bytes) = self.crossing {
            return Err(ArtifactError::OutputQuotaExceeded {
                limit_bytes: self.limit,
                crossing_bytes,
            });
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
        Ok(QuotaAdmission {
            accepted,
            total: self.accepted.saturating_add(accepted as u64),
            crossing: (observed > self.limit).then_some(observed),
        })
    }

    fn commit(&mut self, admission: QuotaAdmission) {
        self.accepted = admission.total;
        self.crossing = admission.crossing;
    }
}

#[derive(Clone, Copy, Debug)]
struct QuotaAdmission {
    accepted: usize,
    total: u64,
    crossing: Option<u64>,
}

pub struct JobArtifactWriter {
    store: ArtifactStore,
    job_id: JobId,
    grant_revision: u64,
    stdout: StreamWriterState,
    stderr: StreamWriterState,
    quota: QuotaLedger,
    sealed: bool,
    poisoned: Option<ArtifactError>,
}

impl JobArtifactWriter {
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn write_stdout(&mut self, bytes: &[u8]) -> Result<(), ArtifactError> {
        self.write(StreamKind::Stdout, bytes)
    }

    pub fn write_stderr(&mut self, bytes: &[u8]) -> Result<(), ArtifactError> {
        self.write(StreamKind::Stderr, bytes)
    }

    fn write(&mut self, stream: StreamKind, bytes: &[u8]) -> Result<(), ArtifactError> {
        if let Some(error) = &self.poisoned {
            return Err(error.clone());
        }
        let admission = self.quota.preview(bytes.len())?;
        if admission.accepted != 0 {
            let state = match stream {
                StreamKind::Stdout => &mut self.stdout,
                StreamKind::Stderr => &mut self.stderr,
            };
            if let Err(error) = state.append(&self.store, self.job_id, &bytes[..admission.accepted])
            {
                let poisoned = ArtifactError::WriterPoisoned(error.to_string());
                self.poisoned = Some(poisoned.clone());
                return Err(poisoned);
            }
        }
        self.quota.commit(admission);
        match admission.crossing {
            Some(crossing_bytes) => Err(ArtifactError::OutputQuotaExceeded {
                limit_bytes: self.quota.limit,
                crossing_bytes,
            }),
            None => Ok(()),
        }
    }

    pub fn output_limit(&self) -> Option<OutputLimitInfo> {
        self.quota.crossing.map(|crossing_bytes| OutputLimitInfo {
            limit_bytes: self.quota.limit,
            crossing_bytes,
        })
    }

    pub fn seal(
        mut self,
        state: JobState,
        stdout_summary: OutputSummary,
        stderr_summary: OutputSummary,
    ) -> Result<SealedJobArtifacts, ArtifactError> {
        if let Some(error) = &self.poisoned {
            return Err(error.clone());
        }
        if matches!(state, JobState::Queued | JobState::Running) {
            return Err(ArtifactError::InvalidTerminalState(
                "seal requires a terminal job state",
            ));
        }
        if self.quota.crossing.is_some() != matches!(state, JobState::OutputLimit) {
            return Err(ArtifactError::InvalidTerminalState(
                "output-limit state must agree with quota crossing",
            ));
        }

        let stdout = self
            .stdout
            .finish(&self.store, self.job_id, stdout_summary)?;
        let stderr = self
            .stderr
            .finish(&self.store, self.job_id, stderr_summary)?;
        let record = JobArtifactRecord {
            repo_id: self.store.inner.repo_id.clone(),
            workspace_incarnation: self.store.inner.workspace_incarnation.clone(),
            job_id: self.job_id,
            sequence: 0,
            state,
            grant_revision: self.grant_revision,
            stdout,
            stderr,
        };
        let (record, terminal_batch_sha256) = self.store.append_record(record)?;
        self.sealed = true;
        Ok(SealedJobArtifacts {
            record,
            terminal_batch_sha256,
            output_limit: self.output_limit(),
        })
    }
}

impl Drop for JobArtifactWriter {
    fn drop(&mut self) {
        if !self.sealed {
            self.stdout.release_budget(&self.store);
            self.stderr.release_budget(&self.store);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedJobArtifacts {
    pub record: JobArtifactRecord,
    pub terminal_batch_sha256: Sha256Digest,
    pub output_limit: Option<OutputLimitInfo>,
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
        store: &ArtifactStore,
        job_id: JobId,
        bytes: &[u8],
    ) -> Result<(), ArtifactError> {
        if let StreamTarget::Redirect { source, descriptor } = &mut self.target {
            let path = store.inner.workspace_root.join(source.as_path());
            descriptor
                .write_all(bytes)
                .map_err(|error| io_error(&path, error))?;
        }

        if self.buffer.is_some() {
            if !self.try_buffer_append(store, bytes)? {
                self.transition_from_buffer(store, job_id)?;
                if matches!(&self.target, StreamTarget::Captured) {
                    let path = protected_absolute(store, job_id, self.kind);
                    self.protected_file
                        .as_mut()
                        .ok_or_else(|| {
                            ArtifactError::WriterPoisoned(
                                "captured spill transition has no file".into(),
                            )
                        })?
                        .write_all(bytes)
                        .map_err(|error| io_error(&path, error))?;
                }
            }
        } else if matches!(&self.target, StreamTarget::Captured) {
            let path = protected_absolute(store, job_id, self.kind);
            self.protected_file
                .as_mut()
                .expect("captured spill file exists")
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
        store: &ArtifactStore,
        bytes: &[u8],
    ) -> Result<bool, ArtifactError> {
        let buffer = self.buffer.as_mut().expect("buffer state was checked");
        let Some(desired_len) = buffer.len().checked_add(bytes.len()) else {
            return Ok(false);
        };
        if desired_len > store.inner.config.inline_cap_bytes {
            return Ok(false);
        }
        let old_capacity = buffer.capacity();
        let reserved_growth = desired_len.saturating_sub(old_capacity);
        let mut budget = store.inner.budget.lock().expect("buffer budget lock");
        if !budget.try_reserve(reserved_growth) {
            return Ok(false);
        }
        if buffer.try_reserve_exact(desired_len - buffer.len()).is_err() {
            budget.release(reserved_growth);
            return Err(ArtifactError::BufferAllocation);
        }
        let actual_growth = buffer.capacity().saturating_sub(old_capacity);
        if actual_growth > reserved_growth
            && !budget.try_reserve(actual_growth - reserved_growth)
        {
            budget.release(reserved_growth);
            return Ok(false);
        }
        if actual_growth < reserved_growth {
            budget.release(reserved_growth - actual_growth);
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
        store: &ArtifactStore,
        job_id: JobId,
    ) -> Result<(), ArtifactError> {
        let buffered = self.buffer.as_ref().expect("transition requires a buffer");
        if matches!(&self.target, StreamTarget::Captured) {
            let path = protected_absolute(store, job_id, self.kind);
            let mut file = create_protected_file(store, job_id, self.kind)?;
            if let Err(error) = file.write_all(buffered) {
                self.protected_file = Some(file);
                return Err(io_error(&path, error));
            }
            self.protected_file = Some(file);
        }
        self.buffer = None;
        self.release_reserved(store);
        Ok(())
    }

    fn finish(
        &mut self,
        store: &ArtifactStore,
        job_id: JobId,
        summary: OutputSummary,
    ) -> Result<StreamInfo, ArtifactError> {
        let digest = Sha256Digest::from_bytes(self.hasher.clone().finalize().into());
        let target = std::mem::replace(&mut self.target, StreamTarget::Captured);
        let storage = match target {
            StreamTarget::Captured => {
                let artifact = if let Some(buffer) = self.buffer.take() {
                    self.release_reserved(store);
                    ProtectedOutput::Inline {
                        data: BinaryData::new(buffer)?,
                    }
                } else {
                    let path = protected_relative(job_id, self.kind)?;
                    seal_file(
                        self.protected_file
                            .take()
                            .expect("spilled captured stream has a file"),
                        &protected_absolute(store, job_id, self.kind),
                    )?;
                    ProtectedOutput::File { path }
                };
                OutputStorage::Captured { artifact }
            }
            StreamTarget::Redirect {
                source,
                mut descriptor,
            } => {
                let source_absolute = store.inner.workspace_root.join(source.as_path());
                descriptor
                    .sync_all()
                    .map_err(|error| io_error(&source_absolute, error))?;

                let artifact = if let Some(buffer) = self.buffer.take() {
                    self.release_reserved(store);
                    ProtectedOutput::Inline {
                        data: BinaryData::new(buffer)?,
                    }
                } else {
                    descriptor
                        .seek(SeekFrom::Start(0))
                        .map_err(|error| io_error(&source_absolute, error))?;
                    let protected_absolute = protected_absolute(store, job_id, self.kind);
                    let mut protected = create_protected_file(store, job_id, self.kind)?;
                    io::copy(&mut descriptor, &mut protected)
                        .map_err(|error| io_error(&protected_absolute, error))?;
                    seal_file(protected, &protected_absolute)?;
                    verify_file_content(&protected_absolute, self.bytes, digest)?;
                    ProtectedOutput::File {
                        path: protected_relative(job_id, self.kind)?,
                    }
                };
                OutputStorage::Redirect { source, artifact }
            }
        };
        let info = StreamInfo {
            storage,
            bytes: self.bytes,
            sha256: digest,
            summary,
        };
        info.validate()?;
        Ok(info)
    }

    fn release_reserved(&mut self, store: &ArtifactStore) {
        if self.reserved != 0 {
            store
                .inner
                .budget
                .lock()
                .expect("buffer budget lock")
                .release(self.reserved);
            self.reserved = 0;
        }
    }

    fn release_budget(&mut self, store: &ArtifactStore) {
        self.release_reserved(store);
    }
}

fn protected_relative(job_id: JobId, stream: StreamKind) -> Result<WorkspacePath, ArtifactError> {
    Ok(WorkspacePath::new(format!(
        ".cowshed/job/{}/{}",
        job_id.get(),
        stream.leaf()
    ))?)
}

fn protected_absolute(store: &ArtifactStore, job_id: JobId, stream: StreamKind) -> PathBuf {
    store
        .inner
        .workspace_root
        .join(".cowshed")
        .join("job")
        .join(job_id.get().to_string())
        .join(stream.leaf())
}

fn create_protected_file(
    store: &ArtifactStore,
    job_id: JobId,
    stream: StreamKind,
) -> Result<File, ArtifactError> {
    let path = protected_absolute(store, job_id, stream);
    verify_no_symlinks(&store.inner.workspace_root, &path).map_err(|error| {
        ArtifactError::Integrity {
            offset: 0,
            message: error.to_string(),
        }
    })?;
    let job_root = ensure_private_job_root(&store.inner.workspace_root)?;
    let parent = job_root.join(job_id.get().to_string());
    ensure_private_directory(&parent)?;
    verify_no_symlinks(&store.inner.workspace_root, &path).map_err(|error| {
        ArtifactError::Integrity {
            offset: 0,
            message: error.to_string(),
        }
    })?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
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

fn seal_file(mut file: File, path: &Path) -> Result<(), ArtifactError> {
    file.flush().map_err(|error| io_error(path, error))?;
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
    file.sync_all().map_err(|error| io_error(path, error))?;
    verify_private_file_mode(
        path,
        &file.metadata().map_err(|error| io_error(path, error))?,
        true,
    )?;
    drop(file);
    Ok(())
}

fn verify_file_content(
    path: &Path,
    expected_bytes: u64,
    expected_digest: Sha256Digest,
) -> Result<(), ArtifactError> {
    read_file_verified(path, expected_bytes, expected_digest, false).map(|_| ())
}

fn read_file_verified(
    path: &Path,
    expected_bytes: u64,
    expected_digest: Sha256Digest,
    collect: bool,
) -> Result<Vec<u8>, ArtifactError> {
    let metadata = fs::metadata(path).map_err(|error| io_error(path, error))?;
    if !metadata.is_file() || !metadata.permissions().readonly() {
        return Err(ArtifactError::Integrity {
            offset: 0,
            message: format!("protected artifact {} is not sealed", path.display()),
        });
    }
    reject_hardlink(path, &metadata)?;
    verify_private_file_mode(path, &metadata, true)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(|error| io_error(path, error))?;
    let mut hasher = Sha256::new();
    let mut observed = 0_u64;
    let mut output = Vec::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| io_error(path, error))?;
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

fn hash_file_incrementally(path: &Path) -> Result<Sha256Digest, ArtifactError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(|error| io_error(path, error))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
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

pub fn read_stream(workspace_root: &Path, stream: &StreamInfo) -> Result<Vec<u8>, ArtifactError> {
    stream.validate()?;
    let bytes = match stream.storage.artifact() {
        ProtectedOutput::Inline { data } => data.as_bytes().to_vec(),
        ProtectedOutput::File { path } => {
            let absolute = workspace_root.join(path.as_path());
            verify_no_symlinks(workspace_root, &absolute).map_err(|error| {
                ArtifactError::Integrity {
                    offset: 0,
                    message: error.to_string(),
                }
            })?;
            read_file_verified(&absolute, stream.bytes, stream.sha256, true)?
        }
    };
    if matches!(stream.storage.artifact(), ProtectedOutput::Inline { .. })
        && (bytes.len() as u64 != stream.bytes || Sha256Digest::compute(&bytes) != stream.sha256)
    {
        return Err(ArtifactError::Integrity {
            offset: 0,
            message: "artifact bytes do not match committed length and digest".into(),
        });
    }
    Ok(bytes)
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
    if !existing {
        sync_parent_directory(path)?;
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
        Err(rollback) => ArtifactError::WriterPoisoned(format!(
            "records append failed ({primary}) and rollback failed ({rollback})"
        )),
    }
}

pub fn recover_records(path: &Path) -> Result<RecoveryReport, ArtifactError> {
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
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| io_error(path, error))?;
    if bytes.len() < RECORD_MAGIC.len() {
        if RECORD_MAGIC.starts_with(&bytes) {
            file.set_len(0).map_err(|error| io_error(path, error))?;
            return Ok(RecoveryReport {
                frames: Vec::new(),
                truncated_bytes: original_len,
                next_job_id: JobId::new(1).expect("one is a valid job id"),
            });
        }
        return Err(integrity(0, "invalid records file magic"));
    }
    if &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(integrity(0, "invalid records file magic"));
    }

    let mut offset = RECORD_MAGIC.len();
    let mut records = Vec::new();
    while offset < bytes.len() {
        let frame_start = offset;
        let remaining = &bytes[offset..];
        if remaining.len() < FRAME_HEADER_BYTES {
            if is_valid_incomplete_frame_header(remaining) {
                truncate_incomplete(&mut file, path, frame_start)?;
                break;
            }
            return Err(integrity(frame_start, "invalid complete batch header"));
        }
        if &remaining[..8] != BATCH_MAGIC {
            return Err(integrity(frame_start, "invalid complete batch magic"));
        }
        let length = u64::from_le_bytes(remaining[8..16].try_into().expect("eight bytes"));
        let complement = u64::from_le_bytes(remaining[16..24].try_into().expect("eight bytes"));
        if length ^ complement != u64::MAX || length > MAX_RECORD_BATCH_BYTES {
            return Err(integrity(frame_start, "invalid complete batch length"));
        }
        let frame_len = FRAME_OVERHEAD_BYTES
            .checked_add(usize::try_from(length).map_err(|_| {
                integrity(
                    frame_start,
                    "record batch length does not fit this platform",
                )
            })?)
            .ok_or_else(|| integrity(frame_start, "record batch length overflow"))?;
        if remaining.len() < frame_len {
            truncate_incomplete(&mut file, path, frame_start)?;
            break;
        }
        let payload_start = FRAME_HEADER_BYTES;
        let payload_end = payload_start + length as usize;
        let digest_end = payload_end + 32;
        let trailer_end = digest_end + BATCH_TRAILER.len();
        if &remaining[digest_end..trailer_end] != BATCH_TRAILER {
            return Err(integrity(frame_start, "invalid complete batch trailer"));
        }
        let payload = &remaining[payload_start..payload_end];
        let expected_digest = Sha256Digest::from_bytes(
            remaining[payload_end..digest_end]
                .try_into()
                .expect("32-byte digest"),
        );
        if Sha256Digest::compute(payload) != expected_digest {
            return Err(integrity(frame_start, "complete batch digest mismatch"));
        }
        let batch = decode_single_batch(payload)
            .map_err(|error| integrity(frame_start, &format!("invalid Arrow IPC: {error}")))?;
        let record = batch_to_protected_record(&batch)
            .map_err(|error| integrity(frame_start, &error.to_string()))?;
        record
            .validate()
            .map_err(|error| integrity(frame_start, &error.to_string()))?;
        records.push(RecoveredFrame {
            record,
            batch_sha256: expected_digest,
        });
        offset += frame_len;
    }

    let mut last_sequence = 0;
    for frame in &records {
        if let ProtectedRecord::Job(record) = &frame.record {
            if record.sequence <= last_sequence {
                return Err(integrity(0, "record sequences are not strictly increasing"));
            }
            last_sequence = record.sequence;
        }
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
    Ok(JobArtifactRecord {
        repo_id,
        workspace_incarnation,
        job_id,
        sequence,
        state,
        grant_revision,
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
        checkpoint_id: None,
        barrier_id: None,
        manifest_batch_sha256: None,
        source_incarnation: None,
        destination_incarnation: None,
        source_checkpoint: None,
    };
    match value {
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
    ];
    RecordBatch::try_new(controller_commitment_schema(), columns)
        .map_err(|error| ArtifactError::Arrow(error.to_string()))
}

pub fn controller_commitments_from_batch(
    batch: &RecordBatch,
) -> Result<Vec<ControllerCommitment>, ArtifactError> {
    if batch.schema() != controller_commitment_schema() {
        return Err(ArtifactError::Arrow(
            "controller commitment schema mismatch".into(),
        ));
    }
    let mut values = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let kind = required_string(batch, 0, row)?;
        let version = u16::try_from(required_u64(batch, 1, row)?)
            .map_err(|_| ArtifactError::Arrow("commitment version does not fit u16".into()))?;
        let order = required_u64(batch, 2, row)?;
        let repo_id = RepoId::parse(required_string(batch, 3, row)?)
            .map_err(|error| ArtifactError::Arrow(error.to_string()))?;
        let value = match kind {
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
                require_variant_columns(batch, row, &[4, 5, 6, 7, 8, 9, 10, 11, 12])?;
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DurableJobKey {
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
}

#[derive(Clone, Debug)]
pub struct CommitmentPriorContext {
    repo_id: RepoId,
    last_order: u64,
    known_incarnations: BTreeSet<WorkspaceIncarnation>,
    admissions: BTreeMap<DurableJobKey, u64>,
    terminals: BTreeSet<DurableJobKey>,
    checkpoints: BTreeMap<String, WorkspaceIncarnation>,
    last_barriers: BTreeMap<WorkspaceIncarnation, u64>,
}

impl CommitmentPriorContext {
    pub fn new(
        repo_id: RepoId,
        known_incarnations: impl IntoIterator<Item = WorkspaceIncarnation>,
    ) -> Self {
        Self {
            repo_id,
            last_order: 0,
            known_incarnations: known_incarnations.into_iter().collect(),
            admissions: BTreeMap::new(),
            terminals: BTreeSet::new(),
            checkpoints: BTreeMap::new(),
            last_barriers: BTreeMap::new(),
        }
    }

    pub fn last_order(&self) -> u64 {
        self.last_order
    }
}

pub fn validate_commitments(
    prior: &CommitmentPriorContext,
    commitments: &[ControllerCommitment],
) -> Result<CommitmentPriorContext, ArtifactError> {
    let mut context = prior.clone();
    for commitment in commitments {
        commitment.validate()?;
        if commitment.repo_id() != &context.repo_id {
            return Err(ArtifactError::Integrity {
                offset: 0,
                message: "controller commitment belongs to a foreign repository".into(),
            });
        }
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
        match commitment {
            ControllerCommitment::Admission(value) => {
                if !context
                    .known_incarnations
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
                if context
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
                let key = DurableJobKey {
                    workspace_incarnation: value.workspace_incarnation.clone(),
                    job_id: value.job_id,
                };
                let Some(grant_revision) = context.admissions.get(&key) else {
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
                if !context.terminals.insert(key) {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "job has more than one terminal commitment".into(),
                    });
                }
            }
            ControllerCommitment::Checkpoint(value) => {
                if !context
                    .known_incarnations
                    .contains(&value.origin_incarnation)
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "checkpoint references an unknown origin incarnation".into(),
                    });
                }
                let previous = context
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
                if context
                    .checkpoints
                    .insert(
                        value.checkpoint_id.clone(),
                        value.origin_incarnation.clone(),
                    )
                    .is_some()
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "checkpoint id is not globally unique".into(),
                    });
                }
            }
            ControllerCommitment::Fork(value) => {
                if !context
                    .known_incarnations
                    .contains(&value.source_incarnation)
                    || !context
                        .known_incarnations
                        .insert(value.destination_incarnation.clone())
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message: "fork lineage parent is absent or destination already exists"
                            .into(),
                    });
                }
            }
            ControllerCommitment::Restore(value) => {
                if context.checkpoints.get(&value.source_checkpoint)
                    != Some(&value.source_incarnation)
                    || !context
                        .known_incarnations
                        .insert(value.destination_incarnation.clone())
                {
                    return Err(ArtifactError::Integrity {
                        offset: 0,
                        message:
                            "restore lineage checkpoint is absent or destination already exists"
                                .into(),
                    });
                }
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
            ControllerCommitment::Fork(_) | ControllerCommitment::Restore(_) => {}
        }
    }
    Ok(context)
}

fn require_job_columns(batch: &RecordBatch, row: usize) -> Result<(), ArtifactError> {
    const REQUIRED: &[usize] = &[3, 4, 5, 6, 7, 8, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24, 25];
    for &column in REQUIRED {
        if batch.column(column).is_null(row) {
            return Err(ArtifactError::Arrow(format!(
                "required job column {} is null",
                batch.schema().field(column).name()
            )));
        }
    }
    for column in 26..batch.num_columns() {
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
    use crate::api::dto::{AdmissionCommitment, CONTROLLER_COMMITMENT_VERSION};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn repo() -> RepoId {
        RepoId::parse("acme/widget").unwrap()
    }

    fn incarnation() -> WorkspaceIncarnation {
        WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap()
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
}
