//! Controller audit records — telemetry, never authority.
//!
//! Every lifecycle act the controller performs (introduce / retire a workspace, admit a job and
//! seal its terminal state, checkpoint, fork, restore) is emitted as one typed
//! [`ControllerCommitment`] record to an [`AuditSink`]. Nothing reads the sink back for a
//! decision: authority is the image inventory under `/private/cowshed/store` (the `.asif` images,
//! their mounts, and the marker each image carries — incarnation, lineage), the host-side grant
//! policy files, and the controller lock. The sink exists so an operator can ask "what did the controller
//! do" after the fact, and so a supervising runtime can route the same records into its own
//! durable log.
//!
//! Three sinks: [`ArrowAuditSink`] writes one sealed Arrow IPC segment per record under the
//! telemetry root — private file, fsync, `rename(2)` without replace, directory sync — the
//! standalone CLI's default (a job's admission and terminal records take one `fsync(2)` and no
//! `F_FULLFSYNC`, see [`commitment_durability`]); [`NullAuditSink`] discards; and any external
//! implementation of the trait a host injects (an embedding runtime routes the records into its
//! own durable log). Segment names
//! are `commitment-<order>-<writer>.arrow` with a writer-local, monotone `order` and a fresh
//! writer id per process, so concurrent controllers never contend and no lock is needed.

use std::ffi::CString;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::Path;

use thiserror::Error;
use uuid::Uuid;

use crate::api::dto::{
    AdmissionCommitment, CONTROLLER_COMMITMENT_VERSION, CheckpointCommitment, ControllerCommitment,
    ForkCommitment, GitOid, JobId, JobState, LandAdoptionCommitment, OutputLimitInfo,
    RestoreCommitment, Sha256Digest, TerminalCommitment, WorkspaceIntroducedCommitment,
    WorkspaceRetiredCommitment,
};
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;
use crate::storage::job_artifact::write_controller_commitment;
use crate::storage::trace_segment::TelemetryDate;

use crate::fsio::{
    Durability, TemporaryAt, create_private_file_at, open_directory_nofollow,
    open_or_create_child_directory, rename_noreplace,
};

/// How durable one commitment is before it is acknowledged. A job's admission and terminal
/// commitments are written for every exec and stay the job's own: they survive the writer's death
/// (`fsync(2)`), and power loss — which ends every job — may take them, leaving a lost job for the
/// supervisor to seal. Every other commitment records lifecycle and survives power loss.
fn commitment_durability(commitment: &ControllerCommitment) -> Durability {
    match commitment {
        ControllerCommitment::Admission(_) | ControllerCommitment::Terminal(_) => {
            Durability::Device
        }
        ControllerCommitment::WorkspaceIntroduced(_)
        | ControllerCommitment::WorkspaceRetired(_)
        | ControllerCommitment::Checkpoint(_)
        | ControllerCommitment::Fork(_)
        | ControllerCommitment::Restore(_)
        | ControllerCommitment::LandAdoption(_) => Durability::PowerLoss,
    }
}

const SEGMENT_PREFIX: &str = "commitment-";

/// One controller act, before the sink assigns it a writer-local order. It crosses a supervisor
/// socket as JSON when a controller forwards a supervisor's commitments into its own sink.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum CommitmentDraft {
    WorkspaceIntroduced {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
    },
    WorkspaceRetired {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
    },
    Admission {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
        job_id: JobId,
        grant_revision: u64,
    },
    Terminal {
        repo_id: RepoId,
        workspace_incarnation: WorkspaceIncarnation,
        job_id: JobId,
        state: JobState,
        grant_revision: u64,
        stdout_bytes: u64,
        stdout_sha256: Sha256Digest,
        stderr_bytes: u64,
        stderr_sha256: Sha256Digest,
        batch_sha256: Sha256Digest,
        output_limit: Option<OutputLimitInfo>,
    },
    Checkpoint {
        repo_id: RepoId,
        origin_incarnation: WorkspaceIncarnation,
        checkpoint_id: String,
        barrier_id: u64,
        manifest_batch_sha256: Sha256Digest,
    },
    Fork {
        repo_id: RepoId,
        source_incarnation: WorkspaceIncarnation,
        destination_incarnation: WorkspaceIncarnation,
    },
    Restore {
        repo_id: RepoId,
        source_checkpoint: String,
        source_incarnation: WorkspaceIncarnation,
        replaced_incarnation: WorkspaceIncarnation,
        destination_incarnation: WorkspaceIncarnation,
    },
    LandAdoption {
        repo_id: RepoId,
        landing_incarnation: WorkspaceIncarnation,
        target_incarnation: WorkspaceIncarnation,
        landed_head: GitOid,
        task: String,
        task_hash: String,
        inputs_digest: Sha256Digest,
    },
}

impl CommitmentDraft {
    pub fn into_commitment(self, order: u64) -> ControllerCommitment {
        match self {
            Self::WorkspaceIntroduced {
                repo_id,
                workspace_incarnation,
            } => ControllerCommitment::WorkspaceIntroduced(WorkspaceIntroducedCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                workspace_incarnation,
            }),
            Self::WorkspaceRetired {
                repo_id,
                workspace_incarnation,
            } => ControllerCommitment::WorkspaceRetired(WorkspaceRetiredCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                workspace_incarnation,
            }),
            Self::Admission {
                repo_id,
                workspace_incarnation,
                job_id,
                grant_revision,
            } => ControllerCommitment::Admission(AdmissionCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                workspace_incarnation,
                job_id,
                grant_revision,
            }),
            Self::Terminal {
                repo_id,
                workspace_incarnation,
                job_id,
                state,
                grant_revision,
                stdout_bytes,
                stdout_sha256,
                stderr_bytes,
                stderr_sha256,
                batch_sha256,
                output_limit,
            } => ControllerCommitment::Terminal(TerminalCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                workspace_incarnation,
                job_id,
                state,
                grant_revision,
                stdout_bytes,
                stdout_sha256,
                stderr_bytes,
                stderr_sha256,
                batch_sha256,
                output_limit,
            }),
            Self::Checkpoint {
                repo_id,
                origin_incarnation,
                checkpoint_id,
                barrier_id,
                manifest_batch_sha256,
            } => ControllerCommitment::Checkpoint(CheckpointCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                origin_incarnation,
                checkpoint_id,
                barrier_id,
                manifest_batch_sha256,
            }),
            Self::Fork {
                repo_id,
                source_incarnation,
                destination_incarnation,
            } => ControllerCommitment::Fork(ForkCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                source_incarnation,
                destination_incarnation,
            }),
            Self::Restore {
                repo_id,
                source_checkpoint,
                source_incarnation,
                replaced_incarnation,
                destination_incarnation,
            } => ControllerCommitment::Restore(RestoreCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                source_checkpoint,
                source_incarnation,
                replaced_incarnation,
                destination_incarnation,
            }),
            Self::LandAdoption {
                repo_id,
                landing_incarnation,
                target_incarnation,
                landed_head,
                task,
                task_hash,
                inputs_digest,
            } => ControllerCommitment::LandAdoption(LandAdoptionCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                landing_incarnation,
                target_incarnation,
                landed_head,
                task,
                task_hash,
                inputs_digest,
            }),
        }
    }
}

/// Where controller audit records go. The sink never gates a controller decision: a failing
/// sink is reported through [`AuditSinkError`] to the publisher, which counts it for `doctor`,
/// and the act it describes has already happened.
pub trait AuditSink: Send {
    /// Durably record one controller act. Implementations assign their own ordering.
    fn record(&mut self, draft: CommitmentDraft) -> Result<(), AuditSinkError>;

    /// A short, stable name for reports (`arrow`, `off`, or the host's).
    fn name(&self) -> &'static str;
}

/// The sink selection a host makes when it opens a project.
pub enum ContinuityAudit {
    /// Sealed Arrow segments under the store's `telemetry/` directory — the standalone default.
    Arrow,
    /// No audit trail.
    Off,
    /// A host-provided sink, e.g. one that writes into the embedding runtime's durable log.
    External(Box<dyn AuditSink>),
}

impl ContinuityAudit {
    /// Read `COWSHED_CONTINUITY_AUDIT` (`arrow` | `off`); unset means `Arrow`.
    pub fn from_environment() -> Result<Self, AuditSinkError> {
        match std::env::var("COWSHED_CONTINUITY_AUDIT") {
            Ok(value) if value == "off" => Ok(Self::Off),
            Ok(value) if value == "arrow" => Ok(Self::Arrow),
            Ok(value) => Err(AuditSinkError::Integrity {
                message: format!(
                    "COWSHED_CONTINUITY_AUDIT is {value:?}; the values are `arrow` (default) and `off`"
                ),
            }),
            Err(_) => Ok(Self::Arrow),
        }
    }

    pub fn into_sink(self, telemetry_root: &Path) -> Result<Box<dyn AuditSink>, AuditSinkError> {
        match self {
            Self::Arrow => Ok(Box::new(ArrowAuditSink::open(telemetry_root)?)),
            Self::Off => Ok(Box::new(NullAuditSink)),
            Self::External(sink) => Ok(sink),
        }
    }
}

impl fmt::Debug for ContinuityAudit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arrow => formatter.write_str("ContinuityAudit::Arrow"),
            Self::Off => formatter.write_str("ContinuityAudit::Off"),
            Self::External(sink) => write!(formatter, "ContinuityAudit::External({})", sink.name()),
        }
    }
}

/// Discards every record.
pub struct NullAuditSink;

impl AuditSink for NullAuditSink {
    fn record(&mut self, _draft: CommitmentDraft) -> Result<(), AuditSinkError> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "off"
    }
}

/// Publication checkpoints exposed only to make crash behavior deterministic under test.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitmentPublicationPoint {
    BeforeRename,
    AfterRenameAndDirectorySync,
}

/// The clock and durability operations used by [`ArrowAuditSink`].
///
/// Production callers use [`ArrowAuditSink::open`]. This seam lets focused tests inject a UTC
/// date and failures at the two crash-relevant publication boundaries.
pub trait AuditSinkEnvironment: Send {
    fn utc_date(&self) -> io::Result<TelemetryDate>;

    fn sync_directory(&self, directory: &File) -> io::Result<()> {
        directory.sync_all()
    }

    fn publication_point(&self, _point: CommitmentPublicationPoint) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AuditSinkError {
    #[error("audit sink I/O failed during {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("audit sink integrity failure: {message}")]
    Integrity { message: String },
}

/// One sealed Arrow segment per record under `<telemetry root>/<UTC date>/`.
///
/// Writes are the only operation: a temporary file created `O_EXCL` with mode 0600, written,
/// synced to the commitment's durability, renamed without replace to its sealed name, and — for
/// a commitment that must survive power loss — the date directory synced. A segment that exists
/// is complete; a crash leaves at most a temporary the next writer never reads.
pub struct ArrowAuditSink {
    root: File,
    writer_id: Uuid,
    next_order: u64,
    environment: Box<dyn AuditSinkEnvironment>,
}

impl ArrowAuditSink {
    pub fn open(telemetry_root: impl AsRef<Path>) -> Result<Self, AuditSinkError> {
        Self::open_with_environment(telemetry_root, Box::new(SystemEnvironment))
    }

    #[doc(hidden)]
    pub fn open_with_environment(
        telemetry_root: impl AsRef<Path>,
        environment: Box<dyn AuditSinkEnvironment>,
    ) -> Result<Self, AuditSinkError> {
        let root = open_or_create_directory_chain(telemetry_root.as_ref())?;
        Ok(Self {
            root,
            writer_id: Uuid::new_v4(),
            next_order: 1,
            environment,
        })
    }

    pub fn writer_id(&self) -> Uuid {
        self.writer_id
    }

    /// The order the next record will carry.
    pub fn next_order(&self) -> u64 {
        self.next_order
    }

    fn seal(&mut self, commitment: &ControllerCommitment) -> Result<(), AuditSinkError> {
        let order = commitment.order();
        let date = self
            .environment
            .utc_date()
            .map_err(|source| io_failure("reading UTC date", source))?;
        let date_name = CString::new(date.to_string()).expect("a formatted date contains no NUL");
        let (date_directory, created) = open_or_create_child_directory(&self.root, &date_name)
            .map_err(|source| io_failure("creating commitment date directory", source))?;
        if created {
            self.environment
                .sync_directory(&self.root)
                .map_err(|source| io_failure("syncing telemetry root", source))?;
        }

        let sealed_name = segment_name(order, self.writer_id);
        // The sealed name already carries order and writer id, so the shared temp grammar keeps
        // crash residue both diagnosable and recognizable by the one sweeper predicate.
        let temporary_name =
            crate::fsio::temp_name(std::ffi::OsStr::new(&sealed_name), Uuid::new_v4().simple());
        let temporary = CString::new(temporary_name.as_encoded_bytes())
            .map_err(|_| integrity("temporary segment name contains NUL"))?;
        let sealed = CString::new(sealed_name.as_bytes())
            .map_err(|_| integrity("sealed segment name contains NUL"))?;
        let mut file = create_private_file_at(&date_directory, &temporary)
            .map_err(|source| io_failure("creating temporary commitment segment", source))?;
        let cleanup = TemporaryAt::new(&date_directory, &temporary);
        write_controller_commitment(&mut file, commitment)
            .map_err(|error| integrity(error.to_string()))?;
        file.flush()
            .map_err(|source| io_failure("flushing audit segment", source))?;
        let durability = commitment_durability(commitment);
        durability
            .sync_file(&file)
            .map_err(|source| io_failure("syncing audit segment", source))?;
        drop(file);

        self.environment
            .publication_point(CommitmentPublicationPoint::BeforeRename)
            .map_err(|source| io_failure("before audit segment rename", source))?;
        rename_noreplace(
            date_directory.as_raw_fd(),
            temporary.as_c_str(),
            sealed.as_c_str(),
        )
        .map_err(|source| io_failure("publishing audit segment", source))?;
        cleanup.disarm();
        if durability == Durability::PowerLoss {
            self.environment
                .sync_directory(&date_directory)
                .map_err(|source| io_failure("syncing audit directory", source))?;
        }
        self.environment
            .publication_point(CommitmentPublicationPoint::AfterRenameAndDirectorySync)
            .map_err(|source| io_failure("after audit segment rename", source))?;
        Ok(())
    }
}

impl AuditSink for ArrowAuditSink {
    fn record(&mut self, draft: CommitmentDraft) -> Result<(), AuditSinkError> {
        let order = self.next_order;
        let commitment = draft.into_commitment(order);
        self.seal(&commitment)?;
        self.next_order = order
            .checked_add(1)
            .ok_or_else(|| integrity("audit record order overflow for this writer"))?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "arrow"
    }
}

struct SystemEnvironment;

impl AuditSinkEnvironment for SystemEnvironment {
    fn utc_date(&self) -> io::Result<TelemetryDate> {
        let mut timestamp: libc::time_t = 0;
        if unsafe { libc::time(&mut timestamp) } == -1 {
            return Err(io::Error::last_os_error());
        }
        let seconds = u64::try_from(timestamp)
            .map_err(|_| io::Error::other("UTC timestamp is before the epoch"))?;
        Ok(TelemetryDate::from_unix_seconds(seconds))
    }
}

/// Sealed segment name: `commitment-<order>-<writer>.arrow`.
pub fn segment_name(order: u64, writer: Uuid) -> String {
    format!("{SEGMENT_PREFIX}{order:020}-{}.arrow", writer.hyphenated())
}

fn open_or_create_directory_chain(path: &Path) -> Result<File, AuditSinkError> {
    if path.as_os_str().is_empty() {
        return Err(integrity("telemetry root is empty"));
    }
    fs::create_dir_all(path).map_err(|source| io_failure("creating telemetry root", source))?;
    open_directory_nofollow(path)
        .map_err(|source| io_failure("opening telemetry root without following links", source))
}

fn io_failure(operation: &'static str, source: io::Error) -> AuditSinkError {
    AuditSinkError::Io { operation, source }
}

fn integrity(message: impl Into<String>) -> AuditSinkError {
    AuditSinkError::Integrity {
        message: message.into(),
    }
}
