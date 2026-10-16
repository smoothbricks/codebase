//! A job's `process.run` spans through the supervisor's production span writer
//! (13_telemetry.md, "Process-tree spans"): the job's process fold emits its events, the live
//! projection projects each with the identity and parent its fold retains, the writer's
//! `record_process` seals each batch's rows as one segment, and the sealed segments are decoded
//! as a reader of lmao's Arrow trace schema sees them.

use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;
use arrow_array::types::{Int32Type, TimestampNanosecondType, UInt32Type, UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use cowshed_core::api::process::{
    BlockerChange, JobProcessDelta, JobProcessEvent, LockHolder, ProcessBlockedOn, ProcessCoverage,
    ProcessCoverageGap, ProcessStorageIo,
};
use cowshed_core::api::resources::{CpuMicros, ResidentBytes, StorageIoBytes};
use cowshed_core::api::{
    CommandArg, ExecCommand, ExitStatus, JobId, JobInfo, JobState, OutputStorage, OutputSummary,
    ProtectedOutput, Sha256Digest, SpanId, StdinInfo, StdinKind, StreamInfo, TraceContext, TraceId,
    UtcTimestamp, WorkspaceIncarnation, WorkspacePath,
};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::job_spans::{JobSpanEdge, JobSpanPublisher, job_span_thread};
use cowshed_core::runtime::process_spans::{
    ProcessSpanProjection, RetainedProcessEvent, process_custom_fields,
};
use cowshed_core::runtime::process_stream::{JobProcessFold, JobProcessObservation};
use cowshed_core::runtime::process_tree::{
    BirthToken, ProcessIdentity, ProcessImage, ProcessObservation, ProcessTreeFold,
};
use cowshed_core::runtime::process_usage::{StartStamp, UsageReading};

#[path = "support/accounting_fixture.rs"]
mod accounting_fixture;
use accounting_fixture::terminal_sample;
#[path = "support/span_schema.rs"]
mod span_schema;
use span_schema::{
    Column, exact_schema, fields, number_cell, sealed_segments, sealed_segments_among_kinds,
    text_cell,
};
#[path = "support/temp_root.rs"]
mod temp_root;
use temp_root::TempRoot;

const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const JOB: u64 = 9;
const MIB: u64 = 1 << 20;

/// The fourteen process columns as 13_telemetry.md's table declares them: written here from the
/// spec, not from the declaration under test. Every one is nullable.
fn spec_process_columns() -> Vec<Column> {
    vec![
        ("proc_pid", "u32", true),
        ("proc_program", "S.category", true),
        ("proc_argv", "S.text", true),
        ("cpu_user_us", "u64", true),
        ("cpu_sys_us", "u64", true),
        ("rss_bytes", "u64", true),
        ("rss_peak_bytes", "u64", true),
        ("io_read_bytes", "u64", true),
        ("io_write_bytes", "u64", true),
        ("exit_code", "i32", true),
        ("exit_signal", "S.enum", true),
        ("blocked_on", "S.enum", true),
        ("blocked_path", "S.category", true),
        ("blocked_holder_pid", "u32", true),
    ]
}

/// The job span's host and volume columns, as 13_telemetry.md declares them. They belong to the job
/// span alone: no process span carries any of them.
const JOB_ONLY_COLUMNS: [Column; 4] = [
    ("disk_ws_delta_bytes", "i64", true),
    ("disk_build_delta_bytes", "i64", true),
    ("load1_milli", "u32", true),
    ("cores", "u16", true),
];

/// One decoded row of a sealed process segment.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Row {
    timestamp_ns: i64,
    trace_id: String,
    thread_id: u64,
    span_id: u32,
    parent_thread_id: Option<u64>,
    parent_span_id: Option<u32>,
    entry_type: String,
    message: String,
    proc_pid: Option<u32>,
    proc_program: Option<String>,
    proc_argv: Option<String>,
    cpu_user_us: Option<u64>,
    cpu_sys_us: Option<u64>,
    rss_bytes: Option<u64>,
    rss_peak_bytes: Option<u64>,
    io_read_bytes: Option<u64>,
    io_write_bytes: Option<u64>,
    exit_code: Option<i32>,
    exit_signal: Option<String>,
    blocked_on: Option<String>,
    blocked_path: Option<String>,
    blocked_holder_pid: Option<u32>,
}

impl Row {
    /// The row kind a reader keys on: its entry type, or its message on an `info` row.
    fn kind(&self) -> &str {
        match self.entry_type.as_str() {
            "info" => &self.message,
            entry_type => entry_type,
        }
    }

    /// Whether the row sets none of the fourteen columns.
    fn columns_unset(&self) -> bool {
        [
            self.cpu_user_us,
            self.cpu_sys_us,
            self.rss_bytes,
            self.rss_peak_bytes,
            self.io_read_bytes,
            self.io_write_bytes,
        ]
        .iter()
        .all(Option::is_none)
            && self.proc_pid.is_none()
            && self.blocked_holder_pid.is_none()
            && self.exit_code.is_none()
            && self.proc_program.is_none()
            && self.proc_argv.is_none()
            && self.exit_signal.is_none()
            && self.blocked_on.is_none()
            && self.blocked_path.is_none()
    }
}

fn decode(batch: &RecordBatch) -> Vec<Row> {
    (0..batch.num_rows())
        .map(|row| {
            let text = |column: &str| text_cell(batch, column, row);
            let u32_at = |column: &str| number_cell::<UInt32Type>(batch, column, row);
            let u64_at = |column: &str| number_cell::<UInt64Type>(batch, column, row);
            Row {
                timestamp_ns: number_cell::<TimestampNanosecondType>(batch, "timestamp", row)
                    .expect("a timestamp"),
                trace_id: text("trace_id").expect("a trace id"),
                thread_id: u64_at("thread_id").expect("a thread"),
                span_id: u32_at("span_id").expect("a span"),
                parent_thread_id: u64_at("parent_thread_id"),
                parent_span_id: u32_at("parent_span_id"),
                entry_type: text("entry_type").expect("an entry type"),
                message: text("message").expect("a message"),
                proc_pid: u32_at("proc_pid"),
                proc_program: text("proc_program"),
                proc_argv: text("proc_argv"),
                cpu_user_us: u64_at("cpu_user_us"),
                cpu_sys_us: u64_at("cpu_sys_us"),
                rss_bytes: u64_at("rss_bytes"),
                rss_peak_bytes: u64_at("rss_peak_bytes"),
                io_read_bytes: u64_at("io_read_bytes"),
                io_write_bytes: u64_at("io_write_bytes"),
                exit_code: number_cell::<Int32Type>(batch, "exit_code", row),
                exit_signal: text("exit_signal"),
                blocked_on: text("blocked_on"),
                blocked_path: text("blocked_path"),
                blocked_holder_pid: u32_at("blocked_holder_pid"),
            }
        })
        .collect()
}

fn id(pid: u32, birth: u64) -> ProcessIdentity {
    ProcessIdentity {
        pid,
        birth: BirthToken(birth),
    }
}

fn image(program: &str, argv: &[&[u8]]) -> ProcessImage {
    ProcessImage {
        program: program.to_owned(),
        argv: argv
            .iter()
            .map(|argument| CommandArg::new(OsString::from_vec(argument.to_vec())))
            .collect(),
        truncated: false,
    }
}

fn utc(second: u32) -> UtcTimestamp {
    UtcTimestamp::new(format!(
        "2026-10-07T00:{:02}:{:02}Z",
        second / 60,
        second % 60
    ))
    .expect("a timestamp")
}

fn reading(at: Instant, cpu_us: u64, resident: u64) -> UsageReading {
    UsageReading {
        started: StartStamp(3),
        at,
        cpu_user: CpuMicros::new(cpu_us).expect("a CPU time"),
        cpu_sys: CpuMicros::new(0).expect("a CPU time"),
        resident: ResidentBytes::new(resident).expect("a resident size"),
        io: ProcessStorageIo::Read {
            read_bytes: StorageIoBytes::new(4096).expect("bytes"),
            write_bytes: StorageIoBytes::new(0).expect("bytes"),
        },
        exited: false,
    }
}

fn stream(path: &str) -> StreamInfo {
    StreamInfo {
        storage: OutputStorage::Captured {
            artifact: ProtectedOutput::File {
                path: WorkspacePath::new(path).expect("workspace path"),
            },
        },
        bytes: 0,
        sha256: Sha256Digest::from_bytes([0; 32]),
        summary: OutputSummary {
            version: 1,
            text: String::new(),
            truncated: false,
        },
    }
}

/// Job `JOB`, running under the adopted trace `TRACE`.
fn running_job() -> JobInfo {
    JobInfo {
        repo_id: RepoId::parse("acme/widget").expect("repo id"),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .expect("incarnation"),
        job_id: JobId::new(JOB).unwrap(),
        state: JobState::Running,
        pid: Some(100),
        grant_revision: 1,
        command: ExecCommand::Argv(vec!["/bin/sh".into()]),
        cwd: None,
        started: utc(0),
        duration_ms: None,
        resources: None,
        exit: None,
        stdout: stream(".cowshed/job/9/out"),
        stderr: stream(".cowshed/job/9/err"),
        trace: TraceContext {
            trace_id: TraceId::new(TRACE).expect("trace id"),
            span_id: SpanId::new("00f067aa0ba902b7").expect("span id"),
        },
        output_limit: None,
        stdin: StdinInfo {
            kind: StdinKind::Empty,
            bytes: 0,
            workspace_path: None,
            complete: true,
        },
        failure: None,
        class: cowshed_gateway_types::JobserverClass::Background,
        scope: cowshed_core::api::ExecScope::Workspace,
        cpu_tokens: None,
        nice: None,
    }
}

/// One job's process fold feeding the live projection and the production writer, the way the
/// observer's trace writer feeds them. Each observation is folded, and every event it emits is
/// projected once, in order, with the identity and parent its fold retains for its life. The
/// batch goes to `record_process` at once, stamped at the observation's time on the wall clock.
struct TracedJob {
    info: JobInfo,
    fold: JobProcessFold,
    projection: ProcessSpanProjection,
    publisher: JobSpanPublisher,
    start: Instant,
    wall: SystemTime,
    batches: usize,
}

impl TracedJob {
    fn new(telemetry: &Path) -> Self {
        let start = Instant::now();
        let publisher = JobSpanPublisher::start(telemetry.to_path_buf());
        let info = running_job();
        Self {
            fold: JobProcessFold::new(info.job_id, start),
            projection: ProcessSpanProjection::new(
                info.trace.trace_id.as_str(),
                job_span_thread(&info.repo_id, &info.workspace_incarnation, info.job_id),
            ),
            info,
            publisher,
            start,
            // 2026-10-07T00:00:00Z.
            wall: UNIX_EPOCH + Duration::from_secs(1_791_331_200),
            batches: 0,
        }
    }

    /// The lmao thread of the job span the process spans hang from.
    fn thread(&self) -> u64 {
        job_span_thread(
            &self.info.repo_id,
            &self.info.workspace_incarnation,
            self.info.job_id,
        )
    }

    fn at(&self, millis: u64) -> Instant {
        self.start + Duration::from_millis(millis)
    }

    /// Fold `observation`, observed `millis` into the job, and record the events it emitted.
    fn observe(&mut self, millis: u64, observation: JobProcessObservation) -> Vec<JobProcessEvent> {
        let mut events = Vec::new();
        self.fold
            .apply(observation, &mut events)
            .expect("a consistent observation");
        let at = self.wall + Duration::from_millis(millis);
        self.record(&events, at);
        events
    }

    /// Project `events`, in the order the fold emitted them, and hand the batch to the writer at
    /// once. A refused event adds no row and is counted by the writer's health; a batch that
    /// yields no row seals nothing and counts as no batch.
    fn record(&mut self, events: &[JobProcessEvent], at: SystemTime) {
        let fold = &self.fold;
        let projection = &mut self.projection;
        let projected: Vec<_> = events
            .iter()
            .map(|event| {
                let (identity, parent) = retained(fold.lives(), event);
                projection.project(
                    RetainedProcessEvent {
                        event,
                        identity,
                        parent,
                    },
                    at,
                )
            })
            .collect();
        if projected.iter().any(|row| matches!(row, Ok(Some(_)))) {
            self.batches += 1;
        }
        self.publisher.record_process(projected);
    }

    fn tree(&mut self, millis: u64, observation: ProcessObservation) {
        let events = self.observe(millis, JobProcessObservation::Tree(observation));
        assert_eq!(
            events
                .iter()
                .filter(|event| !matches!(event, JobProcessEvent::Gap { .. }))
                .count(),
            1,
            "one life transition, with any separately stated coverage gap: {events:?}"
        );
    }

    fn read(&mut self, millis: u64, process: ProcessIdentity, reading: UsageReading) -> usize {
        self.observe(millis, JobProcessObservation::Usage { process, reading })
            .len()
    }

    fn blocked(
        &mut self,
        millis: u64,
        process: ProcessIdentity,
        blocked_on: Option<ProcessBlockedOn>,
    ) {
        let events = self.observe(
            millis,
            JobProcessObservation::Blocker {
                process,
                blocked_on,
            },
        );
        assert_eq!(events.len(), 1, "one blocker change: {events:?}");
    }

    fn tick(&mut self, millis: u64) -> usize {
        let at = self.at(millis);
        self.observe(millis, JobProcessObservation::Tick { at })
            .len()
    }
}

/// The index of the life `event` names, if any: a gap names none.
fn life_of(event: &JobProcessEvent) -> Option<usize> {
    let index = match event {
        JobProcessEvent::Born { index, .. }
        | JobProcessEvent::Exec { index, .. }
        | JobProcessEvent::Heartbeat { index, .. }
        | JobProcessEvent::Exited { index, .. } => *index,
        JobProcessEvent::Changed(delta) => delta.index(),
        JobProcessEvent::Gap { .. } => return None,
    };
    usize::try_from(index).ok()
}

/// The identity and parent the fold retains for the life `event` names, as the live observer
/// hands them to the projection beside each event. Both are absent for a gap, and for a life
/// the fold does not retain.
fn retained(
    lives: &ProcessTreeFold,
    event: &JobProcessEvent,
) -> (Option<ProcessIdentity>, Option<ProcessIdentity>) {
    let life = life_of(event).and_then(|index| lives.node_at(index));
    (
        life.map(|life| life.identity),
        life.and_then(|life| life.parent),
    )
}

/// The kinds of each span's rows, by span id, in the order they were sealed.
fn kinds_by_span(rows: &[Row]) -> Vec<(u32, Vec<String>)> {
    let mut spans: Vec<(u32, Vec<String>)> = Vec::new();
    for row in rows {
        match spans.iter_mut().find(|(span, _)| *span == row.span_id) {
            Some((_, kinds)) => kinds.push(row.kind().to_owned()),
            None => spans.push((row.span_id, vec![row.kind().to_owned()])),
        }
    }
    spans
}

/// A process's record as a reader rebuilds it from its span's rows alone.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Snapshot {
    pid: Option<u32>,
    program: Option<String>,
    argv: Option<String>,
    cpu_user_us: Option<u64>,
    rss_bytes: Option<u64>,
    rss_peak_bytes: Option<u64>,
    busy: bool,
    blocked_on: Option<String>,
    blocked_path: Option<String>,
    blocked_holder_pid: Option<u32>,
    exit_code: Option<i32>,
    exit_signal: Option<String>,
    ended: bool,
}

impl Snapshot {
    /// Apply one row by its kind: blocker rows replace the three blocker columns whole, and on
    /// every other row a null column is unchanged.
    fn apply(&mut self, row: &Row) {
        fn keep<T: Clone>(field: &mut Option<T>, value: &Option<T>) {
            if value.is_some() {
                field.clone_from(value);
            }
        }
        keep(&mut self.pid, &row.proc_pid);
        keep(&mut self.program, &row.proc_program);
        keep(&mut self.argv, &row.proc_argv);
        keep(&mut self.cpu_user_us, &row.cpu_user_us);
        keep(&mut self.rss_bytes, &row.rss_bytes);
        keep(&mut self.rss_peak_bytes, &row.rss_peak_bytes);
        match row.kind() {
            "process.busy" => self.busy = true,
            "process.idle" => self.busy = false,
            "span-start" | "process.blocked" | "process.blocker-cleared" => {
                self.blocked_on.clone_from(&row.blocked_on);
                self.blocked_path.clone_from(&row.blocked_path);
                self.blocked_holder_pid = row.blocked_holder_pid;
            }
            "span-ok" | "span-err" => {
                self.exit_code = row.exit_code;
                self.exit_signal.clone_from(&row.exit_signal);
                self.ended = true;
            }
            _ => {}
        }
    }
}

fn snapshot<'a>(rows: impl IntoIterator<Item = &'a Row>) -> Snapshot {
    let mut snapshot = Snapshot::default();
    for row in rows {
        snapshot.apply(row);
    }
    snapshot
}

/// A traced job whose shell forks two FIFO-held `cat` children yields three `process.run`
/// spans hanging from the job span by the observed ppid edge, every segment carrying exactly the
/// declared fourteen process columns. Each span starts once with its argv display and ends once
/// with its final usage; an idle interval adds only its heartbeats; a busy→idle flip with no
/// counter moved is one idle row; a cleared lock leaves no path or holder behind.
#[tokio::test]
async fn a_job_s_process_tree_is_three_parented_process_run_spans_over_fourteen_columns() {
    let telemetry = TempRoot::new("cowshed-process-spans");
    let mut job = TracedJob::new(&telemetry);
    let shell = id(100, 1);
    let first = id(101, 2);
    let second = id(102, 3);

    job.tree(
        0,
        ProcessObservation::Root {
            process: shell,
            ppid: 1,
            at: utc(0),
            image: image(
                "/bin/sh",
                &[
                    b"sh",
                    b"-c",
                    b"cat \"$1\" & cat \"$2\" & wait",
                    b"fifo-\xff",
                    b"",
                ],
            ),
        },
    );
    for child in [first, second] {
        job.tree(
            0,
            ProcessObservation::Forked {
                process: child,
                parent: shell,
                at: utc(0),
            },
        );
    }
    job.tree(
        0,
        ProcessObservation::Exec {
            process: first,
            image: image("/bin/cat", &[b"cat", b"fifo-a"]),
        },
    );
    job.tree(
        0,
        ProcessObservation::Exec {
            process: second,
            image: image("/bin/cat", &[b"cat", b"fifo-b"]),
        },
    );

    // First reads: each a change.
    for process in [shell, first, second] {
        let at = job.at(0);
        assert_eq!(job.read(0, process, reading(at, 0, MIB)), 1);
    }
    // The first child burns half a second of CPU over one second, then nothing for the next
    // one: busy, then idle with every counter where the busy read left it.
    let at = job.at(1_000);
    assert_eq!(job.read(1_000, first, reading(at, 500_000, MIB)), 1);
    let at = job.at(2_000);
    assert_eq!(job.read(2_000, first, reading(at, 500_000, MIB)), 1);
    // Unchanged samples, and the shell's CPU moving under the busy share: no change, no row.
    for millis in [3_000, 4_000] {
        let at = job.at(millis);
        assert_eq!(job.read(millis, first, reading(at, 500_000, MIB)), 0);
    }
    let at = job.at(5_000);
    assert_eq!(job.read(5_000, shell, reading(at, 1, MIB)), 0);

    // The second child waits on a lock, then on nothing, then is no longer observed.
    let lock = ProcessBlockedOn::Lock {
        path: "/ws/.git/index.lock".to_owned(),
        holder: Some(LockHolder {
            pid: 4242,
            job: None,
            program: None,
        }),
    };
    job.blocked(6_000, second, Some(lock));
    let locked_batch = job.batches;
    job.blocked(7_000, second, Some(ProcessBlockedOn::None));
    job.blocked(8_000, second, None);

    // An idle interval: heartbeats only at the declared minute, one per live process.
    assert_eq!(job.tick(30_000), 0);
    assert_eq!(job.tick(60_000), 3);
    for millis in [70_000, 80_000, 119_999] {
        let at = job.at(millis);
        assert_eq!(job.read(millis, first, reading(at, 500_000, MIB)), 0);
        assert_eq!(job.tick(millis), 0);
    }
    assert_eq!(job.tick(120_000), 3);

    // The first child exits 0 with its counters read after the exit; the second is killed with
    // no read after it; the shell exits 1.
    let mut last = reading(job.at(121_000), 500_000, 0);
    last.exited = true;
    assert_eq!(job.read(121_000, first, last), 0, "withheld until the exit");
    job.tree(
        121_000,
        ProcessObservation::Exited {
            process: first,
            status: ExitStatus::Exited { code: 0 },
            at: utc(121),
        },
    );
    job.tree(
        122_000,
        ProcessObservation::Exited {
            process: second,
            status: ExitStatus::Signaled {
                signal: libc::SIGTERM,
                core_dumped: false,
            },
            at: utc(122),
        },
    );
    let mut last = reading(job.at(123_000), 2, 0);
    last.exited = true;
    job.read(123_000, shell, last);
    job.tree(
        123_000,
        ProcessObservation::Exited {
            process: shell,
            status: ExitStatus::Exited { code: 1 },
            at: utc(123),
        },
    );

    let health = job.publisher.health().await.unwrap();
    assert_eq!(
        (health.recorded, health.failed),
        (u64::try_from(job.batches).unwrap(), 0),
        "one sealed segment per batch that added rows: {health:?}"
    );

    let segments = sealed_segments(&telemetry, "process-");
    assert_eq!(segments.len(), job.batches);
    let writer = &segments[0].0["process-00000000000000000001-".len()..];
    let mut rows = Vec::new();
    let mut through_lock = 0;
    for (order, (name, batch)) in segments.iter().enumerate() {
        assert_eq!(*name, format!("process-{:020}-{writer}", order + 1));
        exact_schema(&batch.schema(), &spec_process_columns())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        rows.extend(decode(batch));
        if order + 1 == locked_batch {
            through_lock = rows.len();
        }
    }

    // Three spans in the job span's thread -- its durable key's, not the workspace-local job id
    // -- and trace: the shell under the job span, both children under the shell.
    let thread = job.thread();
    assert_ne!(thread, JOB);
    for row in &rows {
        assert_eq!(
            (row.trace_id.as_str(), row.thread_id, row.parent_thread_id),
            (TRACE, thread, Some(thread)),
            "{row:?}"
        );
    }
    let parents = |span: u32| {
        rows.iter()
            .filter(|row| row.span_id == span)
            .map(|row| row.parent_span_id)
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(parents(2), [Some(1)].into());
    assert_eq!(parents(3), [Some(2)].into());
    assert_eq!(parents(4), [Some(2)].into());

    // The row-count oracle: exactly these rows, no more.
    let expected = |kinds: &[&str]| kinds.iter().map(|kind| (*kind).to_owned()).collect();
    let row_oracle = vec![
        (
            2,
            expected(&[
                "span-start",
                "process.usage",
                "process.heartbeat",
                "process.heartbeat",
                "span-err",
            ]),
        ),
        (
            3,
            expected(&[
                "span-start",
                "process.exec",
                "process.usage",
                "process.busy",
                "process.idle",
                "process.heartbeat",
                "process.heartbeat",
                "span-ok",
            ]),
        ),
        (
            4,
            expected(&[
                "span-start",
                "process.exec",
                "process.usage",
                "process.blocked",
                "process.blocked",
                "process.blocker-cleared",
                "process.heartbeat",
                "process.heartbeat",
                "span-err",
            ]),
        ),
    ];
    assert_eq!(kinds_by_span(&rows), row_oracle);
    for row in &rows {
        let named = matches!(row.kind(), "span-start" | "span-ok" | "span-err");
        assert_eq!(row.message == "process.run", named, "{row:?}");
    }

    // argv once, on the start row, as its deterministic display; the pid and program there too.
    let start = |span: u32| {
        rows.iter()
            .find(|row| row.span_id == span && row.kind() == "span-start")
            .unwrap()
    };
    assert_eq!(
        start(2).proc_argv.as_deref(),
        Some("sh -c $'cat \"$1\" & cat \"$2\" & wait' $'fifo-\\xff' $''")
    );
    assert_eq!(
        (start(3).proc_pid, start(3).proc_program.as_deref()),
        (Some(101), Some("/bin/sh")),
        "a forked child runs its parent's image until it execs"
    );
    assert_eq!(
        rows.iter().filter(|row| row.proc_argv.is_some()).count(),
        3,
        "argv appears on the three start rows only"
    );

    // One final usage row per span: every counter of the final usage and the exact exit.
    let end = |span: u32| {
        rows.iter()
            .find(|row| row.span_id == span && matches!(row.kind(), "span-ok" | "span-err"))
            .unwrap()
    };
    assert_eq!(
        (
            end(3).cpu_user_us,
            end(3).cpu_sys_us,
            end(3).rss_bytes,
            end(3).rss_peak_bytes,
            end(3).io_read_bytes,
            end(3).io_write_bytes,
            end(3).exit_code,
            end(3).exit_signal.as_deref()
        ),
        (
            Some(500_000),
            Some(0),
            Some(0),
            Some(MIB),
            Some(4096),
            Some(0),
            Some(0),
            None
        )
    );
    assert_eq!(
        (
            end(4).exit_code,
            end(4).exit_signal.as_deref(),
            end(4).cpu_user_us
        ),
        (None, Some("SIGTERM"), Some(0)),
        "killed with no read after the exit: the last usage read"
    );
    assert_eq!((end(2).exit_code, end(2).cpu_user_us), (Some(1), Some(2)));

    // The idle flip moved no counter: one row of the idle kind, every column null.
    let idle = rows
        .iter()
        .filter(|row| row.kind() == "process.idle")
        .collect::<Vec<_>>();
    assert_eq!(idle.len(), 1);
    assert!(idle[0].columns_unset(), "{:?}", idle[0]);
    let busy = rows
        .iter()
        .find(|row| row.kind() == "process.busy")
        .unwrap();
    assert_eq!(busy.cpu_user_us, Some(500_000));
    assert_eq!(
        (busy.rss_bytes, busy.rss_peak_bytes, busy.io_read_bytes),
        (None, None, None),
        "only the counter that moved"
    );
    // The shell's sub-threshold CPU is restated by its first heartbeat, then nothing moves.
    let heartbeats = rows
        .iter()
        .filter(|row| row.span_id == 2 && row.kind() == "process.heartbeat")
        .collect::<Vec<_>>();
    assert_eq!(heartbeats[0].cpu_user_us, Some(1));
    assert!(heartbeats[1].columns_unset(), "{:?}", heartbeats[1]);

    // The lock's path and holder, then its replacement by `none`, then the clear.
    let second_rows = |until: usize| rows[..until].iter().filter(|row| row.span_id == 4);
    let locked = snapshot(second_rows(through_lock));
    assert_eq!(
        (
            locked.blocked_on.as_deref(),
            locked.blocked_path.as_deref(),
            locked.blocked_holder_pid
        ),
        (Some("lock"), Some("/ws/.git/index.lock"), Some(4242))
    );
    let released = snapshot(second_rows(through_lock + 1));
    assert_eq!(
        (
            released.blocked_on.as_deref(),
            released.blocked_path.as_deref(),
            released.blocked_holder_pid
        ),
        (Some("none"), None, None),
        "no stale path or holder"
    );
    let cleared = snapshot(second_rows(through_lock + 2));
    assert_eq!(
        (
            cleared.blocked_on,
            cleared.blocked_path,
            cleared.blocked_holder_pid
        ),
        (None, None, None)
    );

    // Rebuilt from its rows, each span ends at the fold's own final record.
    let tree = job.fold.tree(utc(124));
    assert_eq!(
        tree.coverage,
        ProcessCoverage::Gap {
            reason: ProcessCoverageGap::UnreadFinalUsage { pid: second.pid },
        },
        "the killed child had no identity-fenced read after its exit"
    );
    for (index, record) in tree.processes.iter().enumerate() {
        let span = u32::try_from(index).unwrap() + 2;
        let rebuilt = snapshot(rows.iter().filter(|row| row.span_id == span));
        let usage = record.usage.expect("read usage");
        assert_eq!(
            (
                rebuilt.pid,
                rebuilt.program.as_deref(),
                rebuilt.cpu_user_us,
                rebuilt.rss_bytes,
                rebuilt.rss_peak_bytes,
                rebuilt.busy,
                rebuilt.ended,
            ),
            (
                Some(record.pid),
                Some(record.program.as_str()),
                Some(usage.cpu_user_us.get()),
                Some(usage.rss_bytes.get()),
                Some(usage.rss_peak_bytes.get()),
                usage.busy,
                true,
            ),
            "span {span}"
        );
    }
    // Every row of a batch is stamped at its observation, so a span's rows never go back in time.
    for (span, _) in kinds_by_span(&rows) {
        let times = rows
            .iter()
            .filter(|row| row.span_id == span)
            .map(|row| row.timestamp_ns)
            .collect::<Vec<_>>();
        assert!(times.is_sorted(), "span {span}: {times:?}");
    }

    // Positive controls: the oracles reject what they exist to reject.
    let schema = segments[0].1.schema();
    let with = |field: Field| {
        let mut columns = schema.fields().iter().cloned().collect::<Vec<_>>();
        columns.push(field.into());
        Schema::new(columns)
    };
    assert!(
        exact_schema(
            &with(Field::new("cpu_busy", DataType::Boolean, true)),
            &spec_process_columns()
        )
        .is_err(),
        "a fifteenth column for busy/idle"
    );
    assert!(
        exact_schema(
            &with(Field::new("proc_json", DataType::Utf8, true)),
            &spec_process_columns()
        )
        .is_err(),
        "a JSON payload column"
    );
    // The job span's host and volume columns are its alone: added to any process segment they fail
    // the exact-schema oracle, which every real process segment passed above without them.
    for (name, batch) in &segments {
        let mut columns = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        columns.extend(fields(&JOB_ONLY_COLUMNS).into_iter().map(Into::into));
        assert!(
            exact_schema(&Schema::new(columns), &spec_process_columns()).is_err(),
            "{name} with the job span's columns"
        );
    }
    let mut logged_unchanged = rows.clone();
    let mut unchanged = idle[0].clone();
    unchanged.message = "process.usage".to_owned();
    logged_unchanged.push(unchanged);
    assert_ne!(
        kinds_by_span(&logged_unchanged),
        row_oracle,
        "an unchanged sample logged as a change"
    );
}

/// The declaration under test is the spec's table: the schema the writer derives from it is
/// exactly the fourteen columns, so there is no second schema to keep in step.
#[test]
fn the_declared_process_columns_are_the_spec_s_fourteen() {
    assert_eq!(process_custom_fields().len(), 14);
    assert_eq!(process_custom_fields(), fields(&spec_process_columns()));
}

/// An event the projection cannot place is counted in the writer's health and seals nothing.
#[tokio::test]
async fn an_event_the_projection_refuses_is_counted_and_seals_nothing() {
    let telemetry = TempRoot::new("cowshed-process-spans-refused");
    let mut job = TracedJob::new(&telemetry);
    // The fold retains no life at index 7, so the event names no identity: the projection
    // refuses it and the writer counts the refusal.
    let wall = job.wall;
    job.record(
        &[JobProcessEvent::Changed(JobProcessDelta::Blocker {
            index: 7,
            blocked_on: BlockerChange::Clear,
        })],
        wall,
    );
    let health = job.publisher.health().await.unwrap();
    assert_eq!((health.recorded, health.failed), (0, 1), "{health:?}");
    assert_eq!(
        health.last_failure.as_deref(),
        Some("process event 7 names no retained identity")
    );
    assert!(sealed_segments(&telemetry, "process-").is_empty());
}

/// `schema` with one more column.
fn with_column(schema: &Schema, field: Field) -> Schema {
    let mut columns = schema.fields().iter().cloned().collect::<Vec<_>>();
    columns.push(field.into());
    Schema::new(columns)
}

/// A page-cache-heavy job records its memory in two places that share no column: its cgroup
/// source's charged memory on the job span's end row, and its process's resident memory on the
/// process's own span. The job's one process holds at most 8 MiB resident while its cgroup is
/// charged 96 MiB now and 160 MiB at its peak for the cache it filled; the same job under a source
/// with no charged-memory counter records the same process rows and no charged cell. Every
/// process segment holds exactly the fourteen process columns, and either charged-memory column
/// added to any of them fails the exact-schema oracle.
#[tokio::test]
async fn a_job_s_charged_memory_and_its_process_s_resident_memory_are_recorded_apart() {
    let mut recorded = Vec::new();
    for (label, charged) in [("cgroup", Some((96 * MIB, 160 * MIB))), ("macos", None)] {
        let telemetry = TempRoot::new(&format!("cowshed-process-spans-{label}"));
        let mut job = TracedJob::new(&telemetry);
        let shell = id(100, 1);
        job.publisher
            .record(&job.info, JobSpanEdge::Start { host: None });
        job.tree(
            0,
            ProcessObservation::Root {
                process: shell,
                ppid: 1,
                at: utc(0),
                image: image("/bin/sh", &[b"sh", b"-c", b"fill-the-page-cache"]),
            },
        );
        let at = job.at(0);
        assert_eq!(job.read(0, shell, reading(at, 0, 8 * MIB)), 1);
        let mut last = reading(job.at(1_000), 10_000, 0);
        last.exited = true;
        job.read(1_000, shell, last);
        job.tree(
            1_000,
            ProcessObservation::Exited {
                process: shell,
                status: ExitStatus::Exited { code: 0 },
                at: utc(1),
            },
        );
        let exit = ExitStatus::Exited { code: 0 };
        let ended = JobInfo {
            state: JobState::Exited,
            duration_ms: Some(1_000),
            resources: Some(terminal_sample(job.info.job_id, charged, 0, 8 * MIB)),
            exit: Some(exit.clone()),
            ..job.info.clone()
        };
        job.publisher
            .record(&ended, JobSpanEdge::end(JobState::Exited, Some(&exit)));
        let health = job.publisher.health().await.unwrap();
        let job_rows = if charged.is_some() { 2 } else { 3 };
        assert_eq!(
            (health.recorded, health.failed),
            (u64::try_from(job.batches + job_rows).unwrap(), 0),
            "{label}: process segments, job edges and any unattributed row: {health:?}"
        );

        // The job span: its end row states the source's pair, or nothing.
        let jobs = sealed_segments_among_kinds(&telemetry, "job-");
        assert_eq!(jobs.len(), job_rows);
        let cells = |batch: &RecordBatch| {
            (
                number_cell::<UInt64Type>(batch, "charged_memory_current_bytes", 0),
                number_cell::<UInt64Type>(batch, "charged_memory_peak_bytes", 0),
            )
        };
        for (_, batch) in &jobs[..job_rows - 1] {
            assert_eq!(
                cells(batch),
                (None, None),
                "{label}: only the end row is charged"
            );
        }
        assert_eq!(
            cells(&jobs[job_rows - 1].1),
            charged.map_or((None, None), |(current, peak)| (Some(current), Some(peak))),
            "{label}: the end row"
        );

        // The process spans: exactly the fourteen columns, in the job span's trace and thread.
        let segments = sealed_segments_among_kinds(&telemetry, "process-");
        assert_eq!(segments.len(), job.batches);
        let mut rows = Vec::new();
        for (name, batch) in &segments {
            exact_schema(&batch.schema(), &spec_process_columns())
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            for charged_column in ["charged_memory_current_bytes", "charged_memory_peak_bytes"] {
                let added = with_column(
                    &batch.schema(),
                    Field::new(charged_column, DataType::UInt64, true),
                );
                assert!(
                    exact_schema(&added, &spec_process_columns()).is_err(),
                    "{name} with {charged_column}"
                );
            }
            rows.extend(decode(batch));
        }
        let thread = job.thread();
        assert_eq!(
            number_cell::<UInt64Type>(&jobs[job_rows - 1].1, "thread_id", 0),
            Some(thread)
        );
        assert!(
            rows.iter()
                .all(|row| row.thread_id == thread && row.parent_thread_id == Some(thread)),
            "{label}: {rows:#?}"
        );

        // The process's resident memory is its own: at most 8 MiB, where the cgroup is charged 96.
        assert_eq!(
            rows.iter().filter_map(|row| row.rss_peak_bytes).max(),
            Some(8 * MIB)
        );
        let end = rows
            .iter()
            .find(|row| row.kind() == "span-ok")
            .expect("the process's end");
        assert_eq!(
            (end.rss_bytes, end.rss_peak_bytes),
            (Some(0), Some(8 * MIB))
        );
        recorded.push(rows);
    }
    assert_eq!(
        recorded[0], recorded[1],
        "the source of the job's accounting changes no process row"
    );
}
