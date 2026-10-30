//! A workspace supervisor's job and process spans (13_telemetry.md, "Job span segments",
//! "Process-tree spans").
//!
//! Each job is one lmao span carrying the trace context the controller minted or adopted for it
//! (`JobInfo.trace`): a `span-start` row at admission and a `span-ok`/`span-err` row at its
//! terminal state. The span's lmao address is thread [`job_span_thread`] of the job's durable key,
//! span 1; the W3C span id rides beside it as `w3c_span_id`. `env_hash` is not computed by the
//! supervisor yet, so no row carries it. The end row also states what the job's accounting source
//! charged its cgroup for memory ([`charged_memory_at`]), when that source has such a counter; no
//! process span carries it.
//!
//! Each row also states the host and volume facts of its boundary ([`super::job_span_facts`]):
//! the start row the host as admission read it, the end row the job's terminal resource sample.
//!
//! A job whose terminal sample states unattributed usage ([`UnattributedUsage`]) also records the
//! `job.unattributed` row of the same span, just before its end row: the sample's reconciliation
//! whole in six columns that every other row of the span leaves null.
//!
//! The job's processes are `process.run` spans of the same thread, hanging from the job span.
//! The native observer projects each emitted fold batch through [`super::process_spans`] and
//! hands it to [`JobSpanPublisher::record_process`].
//!
//! One writer gathers both kinds of row and seals what it gathered once per [`SEAL_INTERVAL`]
//! ([`write_rows`]): the job rows as one `job-<order:020>-<writer>.arrow` segment, then the
//! process rows as one `process-<order:020>-<writer>.arrow` segment, each holding its rows in
//! queue order, under `<telemetry root>/<yyyy-mm-dd>/` by [`crate::storage::trace_segment`]. A row
//! is telemetry, not a durability event: sealing one segment per job row, each a full flush of the
//! segment and of its directory, cost 22.9 ms of flushing per exec in a supervisor sample of 203
//! back-to-back execs on 2026-10-30 (load about 10), competing with the job's own record writes. A
//! retiring supervisor seals what was gathered before it is answered; rows still gathered when the
//! writer's process dies are lost, as process rows are.
//!
//! Telemetry never gates a job. The actor hands a row to [`JobSpanPublisher::record`], which
//! queues it without waiting; the writer task gathers and seals in order, off the actor and off
//! the async workers. A row the queue refuses, the writer cannot encode or gathers past its bound,
//! a segment the writer fails to seal, and an event the projection cannot place, are counted in
//! [`TraceHealth`], never dropped silently.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use uuid::Uuid;

use crate::api::dto::{ExitStatus, JobId, JobInfo, JobState, TraceContext, TraceId};
use crate::api::resources::{ChargedMemoryUsage, HostLoadSample, VolumeUsedBytesDelta};
use crate::error::CowshedError;
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;
use crate::runtime::job_span_facts::{HostColumnError, HostColumns, SpanFacts};
use crate::runtime::job_unattributed::{
    AccountingSource, CoverageKind, JOB_UNATTRIBUTED_MESSAGE, UnattributedUsage,
};
use crate::runtime::process_spans::{ProcessSpanError, ProcessSpanRow, seal_process_batch};
use crate::storage::job_artifact::state_name;
use crate::storage::trace_columns::{
    Enum, I64, Optional, Required, U16, U32, U64, Utf8, trace_columns,
};
use crate::storage::trace_segment::{
    EntryType, SpanAddress, SystemColumns, SystemRow, TelemetryDate, TraceSegmentError,
    seal_segment, trace_batch,
};

/// The span name both rows of a job span carry.
pub const JOB_SPAN_MESSAGE: &str = "cowshed.job";
/// A job span is the first span of its job's lmao thread ([`job_span_thread`]); its process spans
/// follow it.
pub const JOB_SPAN_ID: u32 = 1;
/// Requests waiting for the writer: one job row or one fold batch each. A job queues up to three
/// rows, so this covers a burst of concurrent admissions and terminals far beyond what a
/// workspace's actor queue admits at once. A full queue refuses rows rather than delaying the
/// actor.
const QUEUE_CAPACITY: usize = 256;

/// How long a writer gathers rows before it seals them: the first row of either kind it gathers
/// arms one deadline this far away, and when it passes the gathered job rows seal as one segment
/// and the gathered process rows as another. A row is not a durability event, and sealing a
/// segment is a full-drive flush of it and of its directory (`Durability::PowerLoss`). One seal
/// per process fold batch made 26 seals and about 52 `F_FULLFSYNC`s a second host-wide, 5-8 s of
/// flushing per wall second at 0.10-0.16 s each, which saturated the drive under every other
/// process on it (measured 2026-10-21). One seal per job row, each a full flush of the segment
/// and of its directory, cost 22.9 ms of flushing per exec in a supervisor sample of 203
/// back-to-back execs on 2026-10-30 (load about 10), competing with the job's own record writes. So the writer seals on this interval, and the durability of each seal is
/// unchanged. Rows still gathered when the process dies are lost, as the gateway's one-batch audit
/// window is (13_telemetry.md, "Job span segments", "Process segments").
pub const SEAL_INTERVAL: Duration = Duration::from_secs(10);

/// The most job rows a writer gathers in one interval. A row past it is refused and counted in
/// the trace health, never sealed early, as a process row past [`PROCESS_GATHER_ROWS`] is: the
/// interval is the seal rate and this bound is only the memory a burst of jobs may hold
/// (13_telemetry.md, "Job span segments").
const JOB_GATHER_ROWS: usize = 16_384;

/// The most process rows a writer gathers in one interval. A row past it is refused and counted
/// in the trace health, never sealed early: the interval is the seal rate and this bound is only
/// the memory a burst may hold, so a busy host loses rows rather than flushing the drive more
/// often because a lot happened (13_telemetry.md, "Process segments").
const PROCESS_GATHER_ROWS: usize = 16_384;

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

/// What a supervisor's span writer did: the segments it sealed, what it refused, and the last
/// refusal.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TraceHealth {
    /// Segments sealed, never rows: one seal makes at most one job segment and one process
    /// segment, however many rows each holds.
    pub recorded: u64,
    /// Refusals: each row or fold batch the queue refused, each row the writer could not encode
    /// or gathered past its bound, each event the projection could not place, and each row of a
    /// segment whose seal failed.
    pub failed: u64,
    /// Why the last refusal was refused.
    pub last_failure: Option<String>,
}

/// How a span ended: `span-ok` only for a clean exit with status 0.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpanOutcome {
    Ok,
    Err,
}

impl SpanOutcome {
    /// The outcome of a process that ended with `exit`.
    pub fn of_exit(exit: &ExitStatus) -> Self {
        match exit {
            ExitStatus::Exited { code: 0 } => Self::Ok,
            ExitStatus::Exited { .. } | ExitStatus::Signaled { .. } => Self::Err,
        }
    }
}

/// The edge of a job's span one row records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobSpanEdge {
    Start {
        /// The host as the supervisor read it at admission, when no resource sample exists yet;
        /// `None` when that read failed.
        host: Option<HostLoadSample>,
    },
    End {
        state: JobState,
        outcome: SpanOutcome,
    },
}

impl JobSpanEdge {
    /// The terminal edge of a job that ended in `state` with `exit`.
    pub fn end(state: JobState, exit: Option<&ExitStatus>) -> Self {
        let outcome = match (state, exit) {
            (JobState::Exited, Some(exit)) => SpanOutcome::of_exit(exit),
            _ => SpanOutcome::Err,
        };
        Self::End { state, outcome }
    }

    fn entry_type(self) -> EntryType {
        match self {
            Self::Start { .. } => EntryType::SpanStart,
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
            Self::Start { .. } => None,
            Self::End { state, .. } => Some(state),
        }
    }
}

/// What one sealed row of a job's span records: one of its edges, or the unattributed usage its
/// terminal sample states, which its end edge brings with it so that no caller records the one
/// beside the other.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RowKind {
    Edge(JobSpanEdge),
    Unattributed(UnattributedUsage),
}

impl RowKind {
    fn entry_type(self) -> EntryType {
        match self {
            Self::Edge(edge) => edge.entry_type(),
            Self::Unattributed(_) => EntryType::Info,
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::Edge(_) => JOB_SPAN_MESSAGE,
            Self::Unattributed(_) => JOB_UNATTRIBUTED_MESSAGE,
        }
    }

    fn state(self) -> Option<JobState> {
        match self {
            Self::Edge(edge) => edge.state(),
            Self::Unattributed(_) => None,
        }
    }

    fn unattributed(self) -> Option<UnattributedUsage> {
        match self {
            Self::Edge(_) => None,
            Self::Unattributed(usage) => Some(usage),
        }
    }
}

/// One row of a job's span: the job's identity as the actor held it when the job crossed the edge
/// its `kind` records.
struct JobSpanRow {
    at: SystemTime,
    trace: TraceContext,
    repo_id: RepoId,
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
    grant_revision: u64,
    /// The host and volume facts of this row's boundary.
    facts: SpanFacts,
    kind: RowKind,
    /// What the row states of the memory charged to the job ([`charged_memory_at`]).
    charged_memory: Option<ChargedMemoryUsage>,
}

/// The memory charged to the job that `kind`'s row states: the terminal sample's cgroup counters
/// on the end row, and nothing on start or unattributed rows, or for a source with no
/// charged-memory counter. The sample's resident memory never stands in for it.
fn charged_memory_at(kind: RowKind, info: &JobInfo) -> Option<ChargedMemoryUsage> {
    match kind {
        RowKind::Edge(JobSpanEdge::End { .. }) => info
            .resources
            .as_ref()
            .and_then(|sample| sample.accounting.charged_memory().copied()),
        RowKind::Edge(JobSpanEdge::Start { .. }) | RowKind::Unattributed(_) => None,
    }
}

/// What one sealed segment holds: the rows of one kind a writer gathered in one interval, in
/// queue order, never none.
enum Segment {
    Jobs(Vec<JobCells>),
    Processes(Vec<ProcessSpanRow>),
}

impl Segment {
    /// How many rows the segment holds: what a failed seal refuses.
    fn rows(&self) -> usize {
        match self {
            Self::Jobs(rows) => rows.len(),
            Self::Processes(rows) => rows.len(),
        }
    }
}

/// What the writer's queue carries: one job row, the rows of one fold batch, or a health read.
enum Request {
    Job(Box<JobSpanRow>),
    Processes(Vec<ProcessSpanRow>),
    Health { reply: oneshot::Sender<TraceHealth> },
}

#[derive(Debug, Error)]
enum JobSpanError {
    #[error("span time is before the Unix epoch")]
    BeforeEpoch,
    #[error("span time exceeds i64 nanoseconds")]
    BeyondNanoseconds,
    #[error("a job segment holds no rows")]
    NoRows,
    #[error(transparent)]
    Segment(#[from] TraceSegmentError),
    #[error(transparent)]
    Host(#[from] HostColumnError),
    #[error(transparent)]
    Process(#[from] ProcessSpanError),
}

/// The queue into one supervisor's span writer. Clones share the writer and its health.
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

    /// Queue the rows for `info` crossing `edge` now, without waiting: the edge's own and, before a
    /// job's end, the row of the unattributed usage its terminal sample states, if it states any.
    /// A queue that is full or a writer that stopped refuses a row, and the refusal is counted; a
    /// refused row is never built. An end row states the host and volume facts of
    /// `info.resources`, the job's terminal sample; a start row states the host its edge carries;
    /// the unattributed-usage row states none.
    pub fn record(&self, info: &JobInfo, edge: JobSpanEdge) {
        if let JobSpanEdge::End { .. } = edge
            && let Some(usage) = info
                .resources
                .as_ref()
                .and_then(|sample| UnattributedUsage::of(&sample.accounting))
        {
            self.enqueue(info, RowKind::Unattributed(usage));
        }
        self.enqueue(info, RowKind::Edge(edge));
    }

    fn enqueue(&self, info: &JobInfo, kind: RowKind) {
        let Some(permit) = self.reserve() else {
            return;
        };
        let facts = match kind {
            RowKind::Edge(JobSpanEdge::Start { host }) => SpanFacts::start(host),
            RowKind::Edge(JobSpanEdge::End { .. }) => SpanFacts::end(info.resources.as_ref()),
            RowKind::Unattributed(_) => SpanFacts::default(),
        };
        permit.send(Request::Job(Box::new(JobSpanRow {
            at: SystemTime::now(),
            trace: info.trace.clone(),
            repo_id: info.repo_id.clone(),
            workspace_incarnation: info.workspace_incarnation.clone(),
            job_id: info.job_id,
            grant_revision: info.grant_revision,
            facts,
            kind,
            charged_memory: charged_memory_at(kind, info),
        })));
    }

    /// Queue all projected rows of one emitted fold batch for the writer, which seals them with
    /// the other rows of its interval ([`SEAL_INTERVAL`]). Each refused event is counted
    /// and contributes no row; successful rows remain in the same batch.
    pub fn record_process(
        &self,
        projected: impl IntoIterator<Item = Result<Option<ProcessSpanRow>, ProcessSpanError>>,
    ) {
        let mut rows = Vec::new();
        for result in projected {
            match result {
                Ok(Some(row)) => rows.push(row),
                Ok(None) => {}
                Err(error) => refuse(&self.health, error.to_string()),
            }
        }
        if rows.is_empty() {
            return;
        }
        let Some(permit) = self.reserve() else {
            return;
        };
        permit.send(Request::Processes(rows));
    }

    /// A slot in the writer's queue; `None`, with the refusal counted, when the queue is full or
    /// the writer stopped. A row is built only once it has a slot.
    fn reserve(&self) -> Option<mpsc::Permit<'_, Request>> {
        match self.sender.try_reserve() {
            Ok(permit) => Some(permit),
            Err(error) => {
                let reason = match error {
                    mpsc::error::TrySendError::Full(()) => "the job span queue is full",
                    mpsc::error::TrySendError::Closed(()) => "the job span writer stopped",
                };
                refuse(&self.health, reason.to_owned());
                None
            }
        }
    }

    /// The writer's health once every row queued before this call is sealed or refused: the
    /// writer seals what it gathered before it answers.
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
    refuse_rows(health, 1, reason);
}

/// Count `rows` refusals at once, under one lock.
fn refuse_rows(health: &Mutex<TraceHealth>, rows: usize, reason: String) {
    let mut health = lock(health);
    health.failed = health
        .failed
        .saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
    health.last_failure = Some(reason);
}

/// Gather rows and seal them in order. The first row of either kind arms one deadline,
/// [`SEAL_INTERVAL`] away. When it passes, before a health answer, which promises every row
/// queued before it, and when the queue closes, the writer seals the gathered job rows as one
/// segment and then the gathered process rows as another, each in queue order, so nothing queued
/// is left behind by a writer that ends in order. No count seals early: rows past
/// [`JOB_GATHER_ROWS`] or [`PROCESS_GATHER_ROWS`] in one interval are refused. A job row is
/// encoded as it is gathered ([`JobCells::of`]), so a row no column can state is refused alone.
/// Sealing syncs files and directories, so it runs on the blocking pool; the order advances on
/// every attempt, so a name a failed attempt may have published is never reused.
async fn write_rows(
    root: PathBuf,
    writer: Uuid,
    mut receiver: mpsc::Receiver<Request>,
    health: Arc<Mutex<TraceHealth>>,
) {
    let mut sealer = Sealer {
        root,
        writer,
        order: 1,
        health,
    };
    let mut gathered = Gathered::default();
    loop {
        let due = gathered.due;
        let request = tokio::select! {
            request = receiver.recv() => request,
            () = tokio::time::sleep_until(due.unwrap_or_else(Instant::now)), if due.is_some() => {
                sealer.seal_gathered(&mut gathered).await;
                continue;
            }
        };
        match request {
            Some(Request::Job(row)) => match JobCells::of(*row) {
                Ok(cells) => {
                    gathered.arm();
                    gather(
                        &sealer.health,
                        &mut gathered.jobs,
                        std::iter::once(cells),
                        JOB_GATHER_ROWS,
                        "job",
                    )
                }
                Err(error) => refuse(&sealer.health, error.to_string()),
            },
            Some(Request::Processes(rows)) => {
                gathered.arm();
                gather(
                    &sealer.health,
                    &mut gathered.processes,
                    rows.into_iter(),
                    PROCESS_GATHER_ROWS,
                    "process",
                );
            }
            Some(Request::Health { reply }) => {
                sealer.seal_gathered(&mut gathered).await;
                // A caller that stopped waiting has nothing left to tell.
                let _ = reply.send(lock(&sealer.health).clone());
            }
            None => {
                sealer.seal_gathered(&mut gathered).await;
                return;
            }
        }
    }
}

/// What a writer gathered since its last seal, each kind in queue order, and when it seals.
#[derive(Default)]
struct Gathered {
    jobs: Vec<JobCells>,
    processes: Vec<ProcessSpanRow>,
    /// [`SEAL_INTERVAL`] after the first row that arrived since the last seal; `None` until one
    /// does.
    due: Option<Instant>,
}

impl Gathered {
    /// Arm the deadline, unless a row that arrived earlier in this interval armed it.
    fn arm(&mut self) {
        self.due
            .get_or_insert_with(|| Instant::now() + SEAL_INTERVAL);
    }
}

/// Gather `rows` into `gathered`, up to `bound` rows of `kind` an interval, and count each row past
/// it as refused: the bound is the memory one interval may hold, never a reason to seal early.
fn gather<T>(
    health: &Mutex<TraceHealth>,
    gathered: &mut Vec<T>,
    rows: impl ExactSizeIterator<Item = T>,
    bound: usize,
    kind: &str,
) {
    let room = bound.saturating_sub(gathered.len());
    if rows.len() > room {
        refuse_rows(
            health,
            rows.len() - room,
            format!("{bound} {kind} rows are already gathered this interval"),
        );
    }
    gathered.extend(rows.take(room));
}

/// One writer's sealing state: where its segments go, its name and its next order.
struct Sealer {
    root: PathBuf,
    writer: Uuid,
    order: u64,
    health: Arc<Mutex<TraceHealth>>,
}

impl Sealer {
    /// Seal what `gathered` holds and disarm its deadline: its job rows as one segment, then its
    /// process rows as another, each only if there are any.
    async fn seal_gathered(&mut self, gathered: &mut Gathered) {
        gathered.due = None;
        if !gathered.jobs.is_empty() {
            self.seal(Segment::Jobs(std::mem::take(&mut gathered.jobs)))
                .await;
        }
        if !gathered.processes.is_empty() {
            self.seal(Segment::Processes(std::mem::take(&mut gathered.processes)))
                .await;
        }
    }

    /// Seal `segment` as this writer's next order. A failed seal refuses each row it held.
    async fn seal(&mut self, segment: Segment) {
        let (root, writer, order) = (self.root.clone(), self.writer, self.order);
        let rows = segment.rows();
        let sealed =
            tokio::task::spawn_blocking(move || seal(&root, writer, order, &segment)).await;
        self.order = self.order.saturating_add(1);
        match sealed {
            Ok(Ok(())) => {
                let mut health = lock(&self.health);
                health.recorded = health.recorded.saturating_add(1);
            }
            Ok(Err(error)) => refuse_rows(&self.health, rows, error.to_string()),
            Err(error) => refuse_rows(
                &self.health,
                rows,
                format!("the job span seal task failed: {error}"),
            ),
        }
    }
}

/// Seal `segment` as `order` of `writer` under `root`: a job segment's telemetry partition is the
/// UTC date of its first row, as a process segment's is.
fn seal(root: &Path, writer: Uuid, order: u64, segment: &Segment) -> Result<(), JobSpanError> {
    match segment {
        Segment::Jobs(rows) => {
            let first = rows.first().ok_or(JobSpanError::NoRows)?;
            seal_segment(
                root,
                first.date,
                &format!("job-{order:020}-{writer}"),
                &job_span_batch(rows)?,
            )?;
        }
        Segment::Processes(rows) => {
            seal_process_batch(root, writer, order, rows)?;
        }
    }
    Ok(())
}

trace_columns! {
    /// One row of a job span's custom columns (13_telemetry.md, "Job span segments", "Process-tree
    /// spans"): the job's durable key and W3C span id, the grant revision it ran under and, on its
    /// end row, its state; then the host's load and cores at the row's boundary and, on the end
    /// row, the signed change of the job's volumes. A null cell is an absent fact, never zero. The
    /// unattributed-usage row alone states the six `unattributed_*` cells.
    pub struct JobColumns, fields: job_fields, arrays: job_column_arrays;
    repo_id: Required<Utf8>,
    workspace_incarnation: Required<Utf8>,
    job_id: Required<U64>,
    grant_revision: Required<U64>,
    w3c_span_id: Required<U64>,
    job_state: Optional<Utf8>,
    /// The workspace volume's used-bytes change since admission, on the end row: signed, never
    /// clamped, and null when the volume's usage was unavailable.
    disk_ws_delta_bytes: Optional<I64>,
    /// The build volume's used-bytes change since admission, on the end row: null when the job
    /// ran with no build volume or its usage was unavailable.
    disk_build_delta_bytes: Optional<I64>,
    /// The host's one-minute load in whole milliloads, at the start and end boundaries.
    load1_milli: Optional<U32>,
    /// The host's online cores, at the same boundaries.
    cores: Optional<U16>,
    /// The bytes the job's cgroup is charged now (`memory.current`): anonymous memory, file cache
    /// and kernel charges together, never resident memory, which only process spans carry. Null
    /// on the start row, for a source with no charged-memory counter and for a job with no
    /// terminal sample.
    charged_memory_current_bytes: Optional<U64>,
    /// The most the job's cgroup was ever charged (`memory.peak`), in bytes, on the same rows.
    charged_memory_peak_bytes: Optional<U64>,
    /// What the job's independent total counted that its observed processes did not, in signed
    /// microseconds of CPU: negative where the processes counted more. Set only on the unattributed
    /// usage row, which states a terminal sample's reconciliation whole.
    unattributed_cpu_us: Optional<I64>,
    /// The same difference in storage bytes read, null where the source or the processes had no
    /// bytes to compare.
    unattributed_io_read_bytes: Optional<I64>,
    /// The same difference in storage bytes written, null where nothing could be compared.
    unattributed_io_write_bytes: Optional<I64>,
    /// The independent source that counted the job's totals.
    unattributed_source: Optional<Enum<AccountingSource>>,
    /// How completely the observed processes covered the job.
    unattributed_coverage: Optional<Enum<CoverageKind>>,
    /// The process a coverage gap's evidence names, null for a complete tree and for lost events.
    unattributed_gap_pid: Optional<U32>,
}

/// A gathered job row as the cells its segment holds. The writer encodes each row as it gathers
/// it, so a row no column can state is refused alone, and a segment of encoded rows refuses none
/// of them.
struct JobCells {
    timestamp_ns: i64,
    /// The row's UTC date: its segment's partition when it is the segment's first row.
    date: TelemetryDate,
    trace_id: TraceId,
    span: SpanAddress,
    kind: RowKind,
    columns: JobColumns,
}

impl JobCells {
    /// `row` as its cells: refused when its time precedes the epoch or passes `i64` nanoseconds,
    /// or when no column can state its host.
    fn of(row: JobSpanRow) -> Result<Self, JobSpanError> {
        let since_epoch = row
            .at
            .duration_since(UNIX_EPOCH)
            .map_err(|_| JobSpanError::BeforeEpoch)?;
        let timestamp_ns =
            i64::try_from(since_epoch.as_nanos()).map_err(|_| JobSpanError::BeyondNanoseconds)?;
        let host = row.facts.host.map(HostColumns::of).transpose()?;
        let usage = row.kind.unattributed();
        let reconciliation = usage.map(|usage| usage.reconciliation);
        let coverage =
            reconciliation.map(|reconciliation| CoverageKind::of(reconciliation.coverage));
        let span = SpanAddress {
            thread_id: job_span_thread(&row.repo_id, &row.workspace_incarnation, row.job_id),
            span_id: JOB_SPAN_ID,
        };
        let columns = JobColumns {
            repo_id: row.repo_id.as_str().to_owned(),
            workspace_incarnation: row.workspace_incarnation.as_str().to_owned(),
            job_id: row.job_id.get(),
            grant_revision: row.grant_revision,
            w3c_span_id: u64::from_str_radix(row.trace.span_id.as_str(), 16)
                .expect("a SpanId is exactly 16 lowercase hex digits"),
            job_state: row.kind.state().map(|state| state_name(state).to_owned()),
            disk_ws_delta_bytes: row.facts.workspace_delta.map(VolumeUsedBytesDelta::get),
            disk_build_delta_bytes: row.facts.build_delta.map(VolumeUsedBytesDelta::get),
            load1_milli: host.map(|host| host.load1_milli),
            cores: host.map(|host| host.cores),
            charged_memory_current_bytes: row
                .charged_memory
                .map(|charged| charged.current_bytes.get()),
            charged_memory_peak_bytes: row.charged_memory.map(|charged| charged.peak_bytes.get()),
            unattributed_cpu_us: reconciliation.map(|reconciliation| reconciliation.cpu_us.get()),
            unattributed_io_read_bytes: reconciliation
                .and_then(|reconciliation| reconciliation.io_read_bytes)
                .map(|delta| delta.get()),
            unattributed_io_write_bytes: reconciliation
                .and_then(|reconciliation| reconciliation.io_write_bytes)
                .map(|delta| delta.get()),
            unattributed_source: usage.map(|usage| usage.source),
            unattributed_coverage: coverage.map(|(kind, _)| kind),
            unattributed_gap_pid: coverage.and_then(|(_, pid)| pid),
        };
        Ok(Self {
            timestamp_ns,
            date: TelemetryDate::from_unix_seconds(since_epoch.as_secs()),
            trace_id: row.trace.trace_id,
            span,
            kind: row.kind,
            columns,
        })
    }
}

/// One record batch of `rows`, in order: lmao's system columns and the job columns of each row.
fn job_span_batch(rows: &[JobCells]) -> Result<RecordBatch, JobSpanError> {
    let mut system = SystemColumns::with_capacity(rows.len());
    for row in rows {
        system.push(SystemRow {
            timestamp_ns: row.timestamp_ns,
            trace_id: row.trace_id.as_str(),
            span: row.span,
            parent: None,
            entry_type: row.kind.entry_type(),
            message: Some(row.kind.message()),
        })?;
    }
    let arrays =
        job_column_arrays(rows.iter().map(|row| &row.columns)).map_err(TraceSegmentError::from)?;
    Ok(trace_batch(system, arrays)?)
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
        assert_eq!(
            JobSpanEdge::Start { host: None }.entry_type(),
            EntryType::SpanStart
        );
        assert_eq!(JobSpanEdge::Start { host: None }.state(), None);
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

    #[tokio::test]
    async fn a_projected_process_row_seals_on_the_same_writer() {
        use crate::api::dto::{CommandArg, UtcTimestamp};
        use crate::api::process::{JobProcessEvent, JobProcessSample};
        use crate::runtime::process_spans::{
            ProcessSpanProjection, RetainedProcessEvent, read_process_segment,
        };
        use crate::runtime::process_tree::{BirthToken, ProcessIdentity};

        let root = std::env::temp_dir().join(format!("cowshed-span-pub-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let publisher = JobSpanPublisher::start(root.clone());
        let mut projection = ProcessSpanProjection::new("trace", 9);
        let identity = ProcessIdentity {
            pid: 4,
            birth: BirthToken(1),
        };
        let process = JobProcessSample {
            pid: 4,
            ppid: 1,
            program: "/bin/cat".into(),
            argv: vec![CommandArg::from("/bin/cat")].into(),
            truncated: false,
            born_at: UtcTimestamp::new("2026-10-12T00:00:00Z").unwrap(),
            usage: None,
            blocked_on: None,
            exit: None,
        };
        let projected = projection.project(
            RetainedProcessEvent {
                event: &JobProcessEvent::Born { index: 0, process },
                identity: Some(identity),
                parent: None,
            },
            UNIX_EPOCH + std::time::Duration::from_secs(3),
        );
        publisher.record_process([projected]);
        publisher.record_process([Ok(None)]);
        publisher.record_process([Err(ProcessSpanError::MissingIdentity { index: 7 })]);
        let health = publisher.health().await.unwrap();
        assert_eq!((health.recorded, health.failed), (1, 1), "{health:?}");
        let sealed = std::fs::read_dir(root.join("1970-01-01"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("process-")
            })
            .unwrap();
        let decoded = read_process_segment(&sealed).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].message, "process.run");
        assert_eq!(decoded[0].columns.proc_argv.as_deref(), Some("/bin/cat"));
        assert_eq!(decoded[0].parent_span_id, JOB_SPAN_ID);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The seal interval is the only seal rate: every fold batch queued inside one interval seals
    /// as exactly one segment once it ends, with no reader asking, and a batch after it seals as a
    /// second. A burst past [`PROCESS_GATHER_ROWS`] does not seal early: the rows past it are
    /// refused and counted, and the interval's rows still seal once, at its end. A process fold
    /// batch is not a durability event; one seal per batch saturated the drive with full flushes
    /// ([`SEAL_INTERVAL`]).
    #[tokio::test(start_paused = true)]
    async fn fold_batches_seal_once_per_interval_and_never_early_on_a_count() {
        use crate::api::dto::{CommandArg, UtcTimestamp};
        use crate::api::process::{JobProcessEvent, JobProcessSample};
        use crate::runtime::process_spans::{
            ProcessSpanProjection, RetainedProcessEvent, read_process_segment,
        };
        use crate::runtime::process_tree::{BirthToken, ProcessIdentity};

        let root = std::env::temp_dir().join(format!(
            "cowshed-span-interval-{}-{}",
            std::process::id(),
            Uuid::new_v4().simple()
        ));
        std::fs::create_dir(&root).unwrap();
        let publisher = JobSpanPublisher::start(root.clone());
        let mut projection = ProcessSpanProjection::new("trace", 9);
        let mut born = |pid: u32| {
            let process = JobProcessSample {
                pid,
                ppid: 1,
                program: "/bin/cat".into(),
                argv: vec![CommandArg::from("/bin/cat")].into(),
                truncated: false,
                born_at: UtcTimestamp::new("2026-10-12T00:00:00Z").unwrap(),
                usage: None,
                blocked_on: None,
                exit: None,
            };
            projection.project(
                RetainedProcessEvent {
                    event: &JobProcessEvent::Born {
                        index: pid - 1,
                        process,
                    },
                    identity: Some(ProcessIdentity {
                        pid,
                        birth: BirthToken(u64::from(pid)),
                    }),
                    parent: None,
                },
                UNIX_EPOCH + Duration::from_secs(3),
            )
        };
        let segments = |root: &Path| -> Vec<PathBuf> {
            let mut found: Vec<_> = std::fs::read_dir(root.join("1970-01-01"))
                .map(|entries| {
                    entries
                        .map(|entry| entry.unwrap().path())
                        .filter(|path| {
                            path.file_name()
                                .and_then(|name| name.to_str())
                                .is_some_and(|name| name.starts_with("process-"))
                        })
                        .collect()
                })
                .unwrap_or_default();
            found.sort();
            found
        };
        // Lets the writer task take what is queued, without advancing the paused clock.
        let settle = || async {
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
        };

        for pid in 1..=5 {
            publisher.record_process([born(pid)]);
            tokio::time::advance(SEAL_INTERVAL / 10).await;
        }
        settle().await;
        assert!(
            segments(&root).is_empty(),
            "five batches inside the interval wait for it to end"
        );

        tokio::time::advance(SEAL_INTERVAL).await;
        // The seal runs on the blocking pool. Waiting through the runtime's own timer lets it park
        // and drive the writer; the paused clock moves only by the interval already advanced and
        // these short steps, never another whole interval. Real time bounds the wait.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while segments(&root).is_empty() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let first = segments(&root);
        assert_eq!(
            first.len(),
            1,
            "the interval's five batches are one seal; health {:?}",
            publisher.health().await
        );
        assert_eq!(read_process_segment(&first[0]).unwrap().len(), 5);

        publisher.record_process([born(6)]);
        let health = publisher.health().await.unwrap();
        assert_eq!((health.recorded, health.failed), (2, 0), "{health:?}");
        let both = segments(&root);
        assert_eq!(both.len(), 2, "a batch after the interval is a second seal");
        assert_eq!(read_process_segment(&both[1]).unwrap().len(), 1);

        let burst = u32::try_from(PROCESS_GATHER_ROWS).unwrap() + 3;
        publisher.record_process((7..7 + burst).map(&mut born).collect::<Vec<_>>());
        settle().await;
        assert_eq!(
            segments(&root).len(),
            2,
            "a burst past the gather bound waits for the interval like any other batch"
        );
        tokio::time::advance(SEAL_INTERVAL).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while segments(&root).len() < 3 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let all = segments(&root);
        assert_eq!(all.len(), 3, "the burst's interval is one seal");
        assert_eq!(
            read_process_segment(&all[2]).unwrap().len(),
            PROCESS_GATHER_ROWS
        );
        let health = publisher.health().await.unwrap();
        assert_eq!(
            (health.recorded, health.failed),
            (3, 3),
            "the three rows past the bound are refused and counted: {health:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
