//! A workspace supervisor's job spans (13_telemetry.md, "Job span segments").
//!
//! Each job is one lmao span carrying the trace context the controller minted or adopted for it
//! (`JobInfo.trace`): a `span-start` row at admission and a `span-ok`/`span-err` row at its
//! terminal state, each sealed as its own segment `job-<order:020>-<writer>.arrow` under
//! `<telemetry root>/<yyyy-mm-dd>/` by [`crate::storage::trace_segment`]. The span's lmao address
//! is thread [`job_span_thread`] of the job's durable key, span 1; the W3C span id rides beside it
//! as `w3c_span_id`. `env_hash` is not computed by the supervisor yet, so no row carries it.
//!
//! Telemetry never gates a job. The actor hands a row to [`JobSpanPublisher::record`], which
//! queues it without waiting; one writer task seals rows in order, off the actor and off the
//! async workers. A row the queue refuses or the writer fails to seal is counted in
//! [`TraceHealth`], never dropped silently.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::api::dto::{ExitStatus, JobId, JobInfo, JobState, TraceContext};
use crate::error::CowshedError;
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;
use crate::storage::job_artifact::state_name;
use crate::storage::trace_segment::{
    EntryType, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError,
    seal_segment, trace_batch,
};

/// The span name both rows of a job span carry.
pub const JOB_SPAN_MESSAGE: &str = "cowshed.job";
/// A job span is the first span of its job's lmao thread ([`job_span_thread`]).
pub const JOB_SPAN_ID: u32 = 1;
/// Rows waiting for the writer: two per job, so this covers a burst of concurrent admissions and
/// terminals far beyond what a workspace's actor queue admits at once. A full queue refuses rows
/// rather than delaying the actor.
const QUEUE_CAPACITY: usize = 256;

/// The lmao thread a job's spans are written on: the first eight bytes of a domain-separated
/// BLAKE3 of the job's durable key `(repo_id, workspace_incarnation, job_id)` (13_telemetry.md,
/// "Trace context propagation"). A workspace-local `job_id` alone repeats across workspaces, and an
/// adopted trace context is shared across them, so jobs that agree on both still get distinct
/// addresses; a writer derives the same thread from the key alone, with nothing to keep.
pub fn job_span_thread(
    repo_id: &RepoId,
    workspace_incarnation: &WorkspaceIncarnation,
    job_id: JobId,
) -> u64 {
    let mut hasher = blake3::Hasher::new_derive_key("cowshed job span thread v1");
    for part in [repo_id.as_str(), workspace_incarnation.as_str()] {
        let length = u64::try_from(part.len()).expect("a key part fits in u64");
        hasher.update(&length.to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.update(&job_id.get().to_le_bytes());
    let mut thread = [0; 8];
    hasher.finalize_xof().fill(&mut thread);
    u64::from_le_bytes(thread)
}

/// What a supervisor's job-span writer did: rows sealed, rows refused, and the last refusal.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraceHealth {
    pub recorded: u64,
    pub failed: u64,
    pub last_failure: Option<String>,
}

/// How a job's span ended: `span-ok` only for a clean exit with status 0.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpanOutcome {
    Ok,
    Err,
}

/// The edge of a job's span one row records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobSpanEdge {
    Start,
    End {
        state: JobState,
        outcome: SpanOutcome,
    },
}

impl JobSpanEdge {
    /// The terminal edge of a job that ended in `state` with `exit`.
    pub fn end(state: JobState, exit: Option<&ExitStatus>) -> Self {
        let outcome = match (state, exit) {
            (JobState::Exited, Some(ExitStatus::Exited { code: 0 })) => SpanOutcome::Ok,
            _ => SpanOutcome::Err,
        };
        Self::End { state, outcome }
    }

    fn entry_type(self) -> EntryType {
        match self {
            Self::Start => EntryType::SpanStart,
            Self::End {
                outcome: SpanOutcome::Ok,
                ..
            } => EntryType::SpanOk,
            Self::End {
                outcome: SpanOutcome::Err,
                ..
            } => EntryType::SpanErr,
        }
    }

    fn state(self) -> Option<JobState> {
        match self {
            Self::Start => None,
            Self::End { state, .. } => Some(state),
        }
    }
}

/// One row of a job's span: the job's identity as the actor held it when the job crossed `edge`.
struct JobSpanRow {
    at: SystemTime,
    trace: TraceContext,
    repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
    grant_revision: u64,
    edge: JobSpanEdge,
}

enum Request {
    Row(Box<JobSpanRow>),
    Health { reply: oneshot::Sender<TraceHealth> },
}

#[derive(Debug, Error)]
enum JobSpanError {
    #[error("job span time is before the Unix epoch")]
    BeforeEpoch,
    #[error("job span time exceeds i64 nanoseconds")]
    BeyondNanoseconds,
    #[error(transparent)]
    Segment(#[from] TraceSegmentError),
}

/// The queue into one supervisor's job-span writer. Clones share the writer and its health.
#[derive(Clone)]
pub struct JobSpanPublisher {
    sender: mpsc::Sender<Request>,
    health: Arc<Mutex<TraceHealth>>,
}

impl std::fmt::Debug for JobSpanPublisher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobSpanPublisher")
            .finish_non_exhaustive()
    }
}

impl JobSpanPublisher {
    /// Start a writer sealing segments under `telemetry_root`, named for a fresh writer id.
    pub fn start(telemetry_root: PathBuf) -> Self {
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let health = Arc::new(Mutex::new(TraceHealth::default()));
        tokio::spawn(write_rows(
            telemetry_root,
            Uuid::new_v4(),
            receiver,
            Arc::clone(&health),
        ));
        Self { sender, health }
    }

    /// Queue the row for `info` crossing `edge` now, without waiting. A queue that is full or a
    /// writer that stopped refuses it, and the refusal is counted; a refused row is never built.
    pub fn record(&self, info: &JobInfo, edge: JobSpanEdge) {
        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(error) => {
                let reason = match error {
                    mpsc::error::TrySendError::Full(()) => "the job span queue is full",
                    mpsc::error::TrySendError::Closed(()) => "the job span writer stopped",
                };
                refuse(&self.health, reason.to_owned());
                return;
            }
        };
        permit.send(Request::Row(Box::new(JobSpanRow {
            at: SystemTime::now(),
            trace: info.trace.clone(),
            repo_id: info.repo_id.clone(),
            workspace_incarnation: info.workspace_incarnation.clone(),
            job_id: info.job_id,
            grant_revision: info.grant_revision,
            edge,
        })));
    }

    /// The writer's health once every row queued before this call is sealed or refused.
    pub async fn health(&self) -> crate::error::Result<TraceHealth> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(Request::Health { reply })
            .await
            .map_err(|_| writer_stopped())?;
        receive.await.map_err(|_| writer_stopped())
    }
}

fn writer_stopped() -> CowshedError {
    CowshedError::internal("the job span writer stopped")
}

fn lock(health: &Mutex<TraceHealth>) -> MutexGuard<'_, TraceHealth> {
    health.lock().unwrap_or_else(PoisonError::into_inner)
}

fn refuse(health: &Mutex<TraceHealth>, reason: String) {
    let mut health = lock(health);
    health.failed = health.failed.saturating_add(1);
    health.last_failure = Some(reason);
}

/// Seal each row in queue order. Sealing syncs files and directories, so it runs on the blocking
/// pool; the order advances on every attempt, so a name a failed attempt may have published is
/// never reused.
async fn write_rows(
    root: PathBuf,
    writer: Uuid,
    mut receiver: mpsc::Receiver<Request>,
    health: Arc<Mutex<TraceHealth>>,
) {
    let mut order: u64 = 1;
    while let Some(request) = receiver.recv().await {
        match request {
            Request::Row(row) => {
                let root = root.clone();
                let sealed =
                    tokio::task::spawn_blocking(move || seal_row(&root, writer, order, &row)).await;
                order = order.saturating_add(1);
                match sealed {
                    Ok(Ok(())) => {
                        let mut health = lock(&health);
                        health.recorded = health.recorded.saturating_add(1);
                    }
                    Ok(Err(error)) => refuse(&health, error.to_string()),
                    Err(error) => {
                        refuse(&health, format!("the job span seal task failed: {error}"))
                    }
                }
            }
            Request::Health { reply } => {
                // A caller that stopped waiting has nothing left to tell.
                let _ = reply.send(lock(&health).clone());
            }
        }
    }
}

fn seal_row(root: &Path, writer: Uuid, order: u64, row: &JobSpanRow) -> Result<(), JobSpanError> {
    let since_epoch = row
        .at
        .duration_since(UNIX_EPOCH)
        .map_err(|_| JobSpanError::BeforeEpoch)?;
    let timestamp_ns =
        i64::try_from(since_epoch.as_nanos()).map_err(|_| JobSpanError::BeyondNanoseconds)?;
    let batch = job_span_batch(row, timestamp_ns)?;
    seal_segment(
        root,
        TelemetryDate::from_unix_seconds(since_epoch.as_secs()),
        &format!("job-{order:020}-{writer}"),
        &batch,
    )?;
    Ok(())
}

fn job_span_batch(row: &JobSpanRow, timestamp_ns: i64) -> Result<RecordBatch, TraceSegmentError> {
    let mut system = SystemColumns::with_capacity(1);
    system.push(SystemRow {
        timestamp_ns,
        trace_id: row.trace.trace_id.as_str(),
        span: SpanAddress {
            thread_id: job_span_thread(&row.repo_id, &row.workspace_incarnation, row.job_id),
            span_id: JOB_SPAN_ID,
        },
        parent: None,
        entry_type: row.edge.entry_type(),
        message: Some(JOB_SPAN_MESSAGE),
    })?;
    let w3c_span_id = u64::from_str_radix(row.trace.span_id.as_str(), 16)
        .expect("a SpanId is exactly 16 lowercase hex digits");
    let custom: Vec<(Field, ArrayRef)> = vec![
        (
            Field::new("repo_id", DataType::Utf8, false),
            Arc::new(StringArray::from(vec![row.repo_id.as_str()])),
        ),
        (
            Field::new("workspace_incarnation", DataType::Utf8, false),
            Arc::new(StringArray::from(vec![row.workspace_incarnation.as_str()])),
        ),
        (
            Field::new("job_id", DataType::UInt64, false),
            Arc::new(UInt64Array::from(vec![row.job_id.get()])),
        ),
        (
            Field::new("grant_revision", DataType::UInt64, false),
            Arc::new(UInt64Array::from(vec![row.grant_revision])),
        ),
        (
            Field::new("w3c_span_id", DataType::UInt64, false),
            Arc::new(UInt64Array::from(vec![w3c_span_id])),
        ),
        (
            Field::new("job_state", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![row.edge.state().map(state_name)])),
        ),
    ];
    trace_batch(system, custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_clean_zero_exit_ends_a_span_ok() {
        let zero = ExitStatus::Exited { code: 0 };
        let one = ExitStatus::Exited { code: 1 };
        let killed = ExitStatus::Signaled {
            signal: 9,
            core_dumped: false,
        };
        assert_eq!(
            JobSpanEdge::end(JobState::Exited, Some(&zero)).entry_type(),
            EntryType::SpanOk
        );
        for (state, exit) in [
            (JobState::Exited, Some(&one)),
            (JobState::Signaled, Some(&killed)),
            (JobState::Killed, Some(&zero)),
            (JobState::OutputLimit, Some(&zero)),
            (JobState::Failed, None),
        ] {
            assert_eq!(
                JobSpanEdge::end(state, exit).entry_type(),
                EntryType::SpanErr,
                "{state:?} with {exit:?}"
            );
        }
        assert_eq!(JobSpanEdge::Start.entry_type(), EntryType::SpanStart);
        assert_eq!(JobSpanEdge::Start.state(), None);
    }

    /// Jobs that share an adopted trace and a workspace-local id are still distinct spans: the
    /// thread follows the whole durable key, and only it.
    #[test]
    fn a_job_s_span_thread_is_its_durable_key_s() {
        let repo = RepoId::parse("acme/widget").unwrap();
        let other_repo = RepoId::parse("acme/gadget").unwrap();
        let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
        let other_incarnation =
            WorkspaceIncarnation::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
        let one = JobId::new(1).unwrap();
        let two = JobId::new(2).unwrap();
        let thread = job_span_thread(&repo, &incarnation, one);
        assert_eq!(thread, job_span_thread(&repo, &incarnation, one));
        for (repo, incarnation, job) in [
            (&repo, &other_incarnation, one),
            (&other_repo, &incarnation, one),
            (&repo, &incarnation, two),
        ] {
            assert_ne!(
                thread,
                job_span_thread(repo, incarnation, job),
                "{repo:?} {incarnation:?} {job:?}"
            );
        }
        assert_ne!(thread, one.get(), "not the workspace-local job id");
    }
}
