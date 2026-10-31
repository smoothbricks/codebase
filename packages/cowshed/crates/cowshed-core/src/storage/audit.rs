//! Controller audit records — telemetry, never authority.
//!
//! Every lifecycle act the controller performs (introduce / retire a workspace, admit a job and
//! seal its terminal state, fork) is emitted as one typed
//! [`ControllerCommitment`] record to an [`AuditSink`]. Nothing reads the sink back for a
//! decision: authority is the image inventory under `/private/cowshed/store` (the `.dmg` images,
//! their mounts, and the marker each image carries — incarnation, lineage), the host-side grant
//! policy files, and the controller lock. The sink exists so an operator can ask "what did the controller
//! do" after the fact, and so a supervising runtime can route the same records into its own
//! durable log.
//!
//! Three sinks: [`ArrowAuditSink`] writes one sealed Arrow IPC segment per [`CommitmentGroup`]
//! under the telemetry root — private file, fsync, `rename(2)` without replace, directory sync —
//! the standalone CLI's default (a group of only job admission and terminal records is not synced
//! at all, see [`commitment_durability`]); [`NullAuditSink`] discards;
//! and any external implementation of the trait a host injects (an embedding runtime routes the
//! records into its own durable log). Segment names are `commitment-<order>-<writer>.arrow`,
//! `order` being the writer-local, monotone order of the segment's first record, with a fresh
//! writer id per process, so concurrent controllers never contend and no lock is needed.
//!
//! A group is the records queued together: the publisher takes every record already waiting
//! when the previous group's sync returns, up to [`GROUP_RECORDS`], and never waits for more. One
//! sync makes the whole group durable at the strongest durability any of its records requires,
//! and every record of it is acknowledged only after that sync returns.

use std::ffi::CString;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use uuid::Uuid;

use crate::api::dto::{
    AdmissionCommitment, AdoptionSkip, CONTROLLER_COMMITMENT_VERSION, ControllerCommitment,
    ForkCommitment, GitOid, JobId, JobState, LandAdoptionCommitment, LandOutcomeCommitment,
    OutputLimitInfo, Sha256Digest, TerminalCommitment, WorkspaceIntroducedCommitment,
    WorkspaceRetiredCommitment,
};
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;
use crate::storage::job_artifact::write_controller_commitments;
use crate::storage::trace_segment::TelemetryDate;

use cowshed_os::fsio::{
    Durability, TemporaryAt, create_private_file_at, open_directory_nofollow,
    open_or_create_child_directory, rename_noreplace,
};

/// How durable one commitment is before it is acknowledged. A job's admission and terminal
/// commitments are written for every exec and stay the job's own: written, they are in the page
/// cache and survive the writer's death, and none is synced; a host crash ends every job anyway,
/// leaving a lost job for the supervisor to seal (11_shell.md, "Durability of a job's own
/// records"). Every other commitment records lifecycle and survives power loss.
fn commitment_durability(commitment: &ControllerCommitment) -> Durability {
    match commitment {
        ControllerCommitment::Admission(_) | ControllerCommitment::Terminal(_) => {
            Durability::Process
        }
        ControllerCommitment::WorkspaceIntroduced(_)
        | ControllerCommitment::WorkspaceRetired(_)
        | ControllerCommitment::Fork(_)
        | ControllerCommitment::LandAdoption(_)
        | ControllerCommitment::LandOutcome(_) => Durability::PowerLoss,
    }
}

/// The durability a group's one sync gives every record in it: power loss if any record must
/// survive it, else none. A group never weakens the barrier any of its records requires.
fn group_durability(commitments: &[ControllerCommitment]) -> Durability {
    if commitments
        .iter()
        .any(|commitment| commitment_durability(commitment) == Durability::PowerLoss)
    {
        Durability::PowerLoss
    } else {
        Durability::Process
    }
}

/// The most records one group, and so one segment and one sync, carries.
pub const GROUP_RECORDS: usize = 64;

const SEGMENT_PREFIX: &str = "commitment-";

/// Controller acts recorded as one unit, in the order they were performed: never empty, never
/// more than [`GROUP_RECORDS`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitmentGroup {
    drafts: Vec<CommitmentDraft>,
}

impl CommitmentGroup {
    /// A group of `draft` alone.
    pub fn one(draft: CommitmentDraft) -> Self {
        Self {
            drafts: vec![draft],
        }
    }

    /// Append `draft`, or hand it back when the group is full.
    pub fn try_push(&mut self, draft: Box<CommitmentDraft>) -> Result<(), Box<CommitmentDraft>> {
        if self.drafts.len() >= GROUP_RECORDS {
            return Err(draft);
        }
        self.drafts.push(*draft);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.drafts.len()
    }

    /// Always false: a group holds at least one draft.
    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn into_drafts(self) -> Vec<CommitmentDraft> {
        self.drafts
    }
}

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
    Fork {
        repo_id: RepoId,
        source_incarnation: WorkspaceIncarnation,
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
    LandOutcome {
        repo_id: RepoId,
        landing_incarnation: WorkspaceIncarnation,
        target_incarnation: WorkspaceIncarnation,
        landed_head: GitOid,
        seeded: bool,
        skip: Option<AdoptionSkip>,
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
            Self::LandOutcome {
                repo_id,
                landing_incarnation,
                target_incarnation,
                landed_head,
                seeded,
                skip,
            } => ControllerCommitment::LandOutcome(LandOutcomeCommitment {
                version: CONTROLLER_COMMITMENT_VERSION,
                order,
                repo_id,
                landing_incarnation,
                target_incarnation,
                landed_head,
                seeded,
                skip,
            }),
        }
    }
}

/// Where controller audit records go. The sink never gates a controller decision: a failing
/// sink is reported through [`AuditSinkError`] to the publisher, which counts it for `doctor`,
/// and the act it describes has already happened.
pub trait AuditSink: Send {
    /// Durably record `group` as one unit, assigning each act its own order: `Ok` means every
    /// record of it is durable, `Err` that none is acknowledged as recorded.
    fn record(&mut self, group: CommitmentGroup) -> Result<(), AuditSinkError>;

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
    fn record(&mut self, _group: CommitmentGroup) -> Result<(), AuditSinkError> {
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

/// One sealed Arrow segment per [`CommitmentGroup`] under `<telemetry root>/<UTC date>/`.
///
/// Writes are the only operation: a temporary file created `O_EXCL` with mode 0600, written,
/// synced to the group's durability, renamed without replace to its sealed name, and — for a
/// group that must survive power loss — the date directory synced. A segment that exists is
/// complete; a crash leaves at most a temporary the next writer never reads.
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

    fn seal(&mut self, commitments: &[ControllerCommitment]) -> Result<(), AuditSinkError> {
        let order = commitments
            .first()
            .ok_or_else(|| integrity("a commitment group holds at least one record"))?
            .order();
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
        let temporary_name = cowshed_os::fsio::temp_name(
            std::ffi::OsStr::new(&sealed_name),
            Uuid::new_v4().simple(),
        );
        let temporary = CString::new(temporary_name.as_encoded_bytes())
            .map_err(|_| integrity("temporary segment name contains NUL"))?;
        let sealed = CString::new(sealed_name.as_bytes())
            .map_err(|_| integrity("sealed segment name contains NUL"))?;
        let mut file = create_private_file_at(&date_directory, &temporary)
            .map_err(|source| io_failure("creating temporary commitment segment", source))?;
        let cleanup = TemporaryAt::new(&date_directory, &temporary);
        write_controller_commitments(&mut file, commitments)
            .map_err(|error| integrity(error.to_string()))?;
        file.flush()
            .map_err(|source| io_failure("flushing audit segment", source))?;
        let durability = group_durability(commitments);
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
            date_directory.as_raw_fd(),
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
    fn record(&mut self, group: CommitmentGroup) -> Result<(), AuditSinkError> {
        let mut commitments = Vec::with_capacity(group.len());
        let mut order = self.next_order;
        for draft in group.into_drafts() {
            commitments.push(draft.into_commitment(order));
            order = order
                .checked_add(1)
                .ok_or_else(|| integrity("audit record order overflow for this writer"))?;
        }
        self.seal(&commitments)?;
        self.next_order = order;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "arrow"
    }
}

struct SystemEnvironment;

impl AuditSinkEnvironment for SystemEnvironment {
    fn utc_date(&self) -> io::Result<TelemetryDate> {
        // std's clock, not `libc::time`: `time_t` is deprecated on musl targets, and the
        // seconds since the epoch are all a calendar date needs.
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| io::Error::other("UTC timestamp is before the epoch"))?;
        Ok(TelemetryDate::from_unix_seconds(since_epoch.as_secs()))
    }
}

/// Sealed segment name: `commitment-<order>-<writer>.arrow`, `order` being its first record's.
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
