//! Controller audit records — telemetry, never authority.
//!
//! Every lifecycle act the controller performs (introduce / retire a workspace, admit a job and
//! seal its terminal state, checkpoint, fork, restore) is emitted as one typed
//! [`ControllerCommitment`] record to an [`AuditSink`]. Nothing reads the sink back for a
//! decision: authority is the image inventory under `~/.cowshed/` (the sparseimages, their
//! mounts, and the marker each image carries — incarnation, lineage), the host-side grant policy
//! files, and the controller lock. The sink exists so an operator can ask "what did the controller
//! do" after the fact, and so a supervising runtime can route the same records into its own
//! durable log.
//!
//! Three sinks: [`ArrowAuditSink`] writes one sealed Arrow IPC segment per record under the
//! telemetry root — private file, fsync, `rename(2)` without replace, directory sync — the
//! standalone CLI's default; [`NullAuditSink`] discards; and any external implementation of the
//! trait a host injects (Containium routes the records to PTMCART from its side). Segment names
//! are `commitment-<order>-<writer>.arrow` with a writer-local, monotone `order` and a fresh
//! writer id per process, so concurrent controllers never contend and no lock is needed.

use std::ffi::{CStr, CString};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use arrow_ipc::writer::StreamWriter;
use thiserror::Error;
use uuid::Uuid;

use crate::api::dto::{
    AdmissionCommitment, CONTROLLER_COMMITMENT_VERSION, CheckpointCommitment, ControllerCommitment,
    ForkCommitment, JobId, JobState, OutputLimitInfo, RestoreCommitment, Sha256Digest,
    TerminalCommitment, WorkspaceIntroducedCommitment, WorkspaceRetiredCommitment,
};
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;
use crate::storage::job_artifact::controller_commitments_to_batch;

const SEGMENT_PREFIX: &str = "commitment-";

/// One controller act, before the sink assigns it a writer-local order.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// A host-provided sink; Containium injects a PTMCART-backed one here.
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

/// A validated UTC calendar date used as a telemetry partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitmentDate {
    year: u16,
    month: u8,
    day: u8,
}

impl CommitmentDate {
    pub fn new(year: u16, month: u8, day: u8) -> Result<Self, AuditSinkError> {
        if valid_calendar_date(year, month, day) {
            Ok(Self { year, month, day })
        } else {
            Err(AuditSinkError::Integrity {
                message: "invalid UTC commitment date".into(),
            })
        }
    }
}

impl fmt::Display for CommitmentDate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.year, self.month, self.day
        )
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
    fn utc_date(&self) -> io::Result<CommitmentDate>;

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
/// fsynced, renamed without replace to its sealed name, and the date directory synced. A segment
/// that exists is complete; a crash leaves at most a temporary the next writer never reads.
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
        let batch = controller_commitments_to_batch(std::slice::from_ref(commitment))
            .map_err(|error| integrity(error.to_string()))?;
        let date = self
            .environment
            .utc_date()
            .map_err(|source| io_failure("reading UTC date", source))?;
        let (date_directory, created) =
            open_or_create_child_directory(&self.root, &date.to_string())?;
        if created {
            self.environment
                .sync_directory(&self.root)
                .map_err(|source| io_failure("syncing telemetry root", source))?;
        }

        let sealed_name = segment_name(order, self.writer_id);
        let temporary_name = format!(
            ".commitment-{order:020}-{}-{}.tmp",
            self.writer_id.hyphenated(),
            Uuid::new_v4().hyphenated()
        );
        let temporary = CString::new(temporary_name.as_bytes())
            .map_err(|_| integrity("temporary segment name contains NUL"))?;
        let sealed = CString::new(sealed_name.as_bytes())
            .map_err(|_| integrity("sealed segment name contains NUL"))?;
        let mut file = create_new_file_at(&date_directory, &temporary)?;
        let mut cleanup = TemporaryCleanup::new(date_directory.as_raw_fd(), temporary.clone());
        {
            let mut writer = StreamWriter::try_new(&mut file, &batch.schema())
                .map_err(|error| integrity(error.to_string()))?;
            writer
                .write(&batch)
                .map_err(|error| integrity(error.to_string()))?;
            writer
                .finish()
                .map_err(|error| integrity(error.to_string()))?;
        }
        file.flush()
            .map_err(|source| io_failure("flushing audit segment", source))?;
        file.sync_all()
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
        self.environment
            .sync_directory(&date_directory)
            .map_err(|source| io_failure("syncing audit directory", source))?;
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
    fn utc_date(&self) -> io::Result<CommitmentDate> {
        let mut timestamp: libc::time_t = 0;
        if unsafe { libc::time(&mut timestamp) } == -1 {
            return Err(io::Error::last_os_error());
        }
        let mut broken_down = std::mem::MaybeUninit::<libc::tm>::uninit();
        if unsafe { libc::gmtime_r(&timestamp, broken_down.as_mut_ptr()) }.is_null() {
            return Err(io::Error::last_os_error());
        }
        let broken_down = unsafe { broken_down.assume_init() };
        let year = u16::try_from(broken_down.tm_year + 1900)
            .map_err(|_| io::Error::other("UTC year is outside the supported range"))?;
        let month = u8::try_from(broken_down.tm_mon + 1)
            .map_err(|_| io::Error::other("UTC month is outside the supported range"))?;
        let day = u8::try_from(broken_down.tm_mday)
            .map_err(|_| io::Error::other("UTC day is outside the supported range"))?;
        CommitmentDate::new(year, month, day).map_err(|error| io::Error::other(error.to_string()))
    }
}

/// Sealed segment name: `commitment-<order>-<writer>.arrow`.
pub fn segment_name(order: u64, writer: Uuid) -> String {
    format!("{SEGMENT_PREFIX}{order:020}-{}.arrow", writer.hyphenated())
}

fn valid_calendar_date(year: u16, month: u8, day: u8) -> bool {
    if year == 0 || !(1..=12).contains(&month) {
        return false;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    (1..=days).contains(&day)
}

fn open_or_create_directory_chain(path: &Path) -> Result<File, AuditSinkError> {
    if path.as_os_str().is_empty() {
        return Err(integrity("telemetry root is empty"));
    }
    fs::create_dir_all(path).map_err(|source| io_failure("creating telemetry root", source))?;
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| integrity("telemetry root contains NUL"))?;
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        Err(io_failure(
            "opening telemetry root without following links",
            io::Error::last_os_error(),
        ))
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn open_or_create_child_directory(
    parent: &File,
    name: &str,
) -> Result<(File, bool), AuditSinkError> {
    let name = CString::new(name).map_err(|_| integrity("date directory contains NUL"))?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    let created = if result == 0 {
        true
    } else if io::Error::last_os_error().kind() == io::ErrorKind::AlreadyExists {
        false
    } else {
        return Err(io_failure(
            "creating commitment date directory",
            io::Error::last_os_error(),
        ));
    };
    let directory = open_directory_at(parent.as_raw_fd(), name.as_c_str()).map_err(|source| {
        io_failure(
            "opening commitment date directory without following links",
            source,
        )
    })?;
    Ok((directory, created))
}

fn open_directory_at(parent: RawFd, name: &CStr) -> io::Result<File> {
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn create_new_file_at(directory: &File, name: &CStr) -> Result<File, AuditSinkError> {
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        Err(io_failure(
            "creating temporary commitment segment",
            io::Error::last_os_error(),
        ))
    } else if unsafe { libc::fchmod(fd, 0o600) } != 0 {
        let source = io::Error::last_os_error();
        unsafe {
            libc::close(fd);
            libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0);
        }
        Err(io_failure(
            "setting temporary commitment segment mode",
            source,
        ))
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(target_os = "macos")]
fn rename_noreplace(directory: RawFd, temporary: &CStr, sealed: &CStr) -> io::Result<()> {
    let result = unsafe {
        libc::renameatx_np(
            directory,
            temporary.as_ptr(),
            directory,
            sealed.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn rename_noreplace(directory: RawFd, temporary: &CStr, sealed: &CStr) -> io::Result<()> {
    const RENAME_NOREPLACE: libc::c_uint = 1;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            directory,
            temporary.as_ptr(),
            directory,
            sealed.as_ptr(),
            RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rename_noreplace(_directory: RawFd, _temporary: &CStr, _sealed: &CStr) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic create-new rename is unsupported",
    ))
}

struct TemporaryCleanup {
    directory: RawFd,
    name: CString,
    armed: bool,
}

impl TemporaryCleanup {
    fn new(directory: RawFd, name: CString) -> Self {
        Self {
            directory,
            name,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TemporaryCleanup {
    fn drop(&mut self) {
        if self.armed {
            unsafe {
                libc::unlinkat(self.directory, self.name.as_ptr(), 0);
            }
        }
    }
}

fn io_failure(operation: &'static str, source: io::Error) -> AuditSinkError {
    AuditSinkError::Io { operation, source }
}

fn integrity(message: impl Into<String>) -> AuditSinkError {
    AuditSinkError::Integrity {
        message: message.into(),
    }
}
