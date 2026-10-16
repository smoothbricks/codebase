//! A job's span through the supervisor's production span writer (13_telemetry.md, "Job span
//! segments", "Process-tree spans"): the writer seals each edge of the span as the segment a
//! reader of lmao's trace schema decodes, carrying exactly the columns the declaration names and
//! the host and volume facts of the job's two boundaries.

use std::path::Path;

use arrow_array::RecordBatch;
use arrow_array::types::{Int64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use cowshed_core::api::{
    ChargedMemoryBytes, ChargedMemoryUsage, CpuMicros, CpuMicrosDelta, CpuTotals, ExecCommand,
    ExitStatus, HostLoadSample, JobAccounting, JobId, JobInfo, JobLeafAttribution,
    JobResourceSample, JobState, JobStreamWatermark, JobVolumeUsage, MAX_EXACT_INTEGER,
    OneCoreCpuPercent, OutputStorage, OutputSummary, ProcessCoverage, ProcessCoverageGap,
    ProtectedOutput, ResidentBytes, Sha256Digest, SpanId, StdinInfo, StdinKind, StorageIoBytes,
    StorageIoBytesDelta, StorageIoTotals, StreamBytes, StreamInfo, StreamLines, TraceContext,
    TraceId, UsageReconciliation, UtcTimestamp, VolumeUnavailable, VolumeUsage,
    VolumeUsedBytesDelta, WallMicros, WorkspaceIncarnation, WorkspacePath,
};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::job_spans::{
    JobSpanEdge, JobSpanPublisher, job_fields, job_span_thread,
};

#[path = "support/accounting_fixture.rs"]
mod accounting_fixture;
use accounting_fixture::terminal_sample;
#[path = "support/span_schema.rs"]
mod span_schema;
use span_schema::{Column, exact_schema, fields, number_cell, sealed_segments, text_cell};
#[path = "support/temp_root.rs"]
mod temp_root;
use temp_root::TempRoot;

const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const JOB: u64 = 9;

/// The job span's custom columns as 13_telemetry.md and the job span's first writer declare
/// them: written here, not derived from the declaration under test.
fn spec_job_columns() -> Vec<Column> {
    vec![
        ("repo_id", "utf8", false),
        ("workspace_incarnation", "utf8", false),
        ("job_id", "u64", false),
        ("grant_revision", "u64", false),
        ("w3c_span_id", "u64", false),
        ("job_state", "utf8", true),
        ("disk_ws_delta_bytes", "i64", true),
        ("disk_build_delta_bytes", "i64", true),
        ("load1_milli", "u32", true),
        ("cores", "u16", true),
        ("charged_memory_current_bytes", "u64", true),
        ("charged_memory_peak_bytes", "u64", true),
        ("unattributed_cpu_us", "i64", true),
        ("unattributed_io_read_bytes", "i64", true),
        ("unattributed_io_write_bytes", "i64", true),
        ("unattributed_source", "S.enum", true),
        ("unattributed_coverage", "S.enum", true),
        ("unattributed_gap_pid", "u32", true),
    ]
}

fn utc(second: u32) -> UtcTimestamp {
    UtcTimestamp::new(format!(
        "2026-10-07T00:{:02}:{:02}Z",
        second / 60,
        second % 60
    ))
    .expect("a timestamp")
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

/// Job `JOB`, running under the adopted trace `TRACE`, with no resource sample.
fn running_job() -> JobInfo {
    JobInfo {
        repo_id: RepoId::parse("acme/widget").expect("repo id"),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .expect("incarnation"),
        job_id: JobId::new(JOB).unwrap(),
        state: JobState::Running,
        pid: Some(100),
        grant_revision: 3,
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

/// Seal the job's start and its clean end through a production writer under `telemetry`, and
/// return the two segments it sealed.
async fn sealed_start_and_end(telemetry: &Path, info: &JobInfo) -> Vec<(String, RecordBatch)> {
    let publisher = JobSpanPublisher::start(telemetry.to_path_buf());
    publisher.record(info, JobSpanEdge::Start { host: None });
    publisher.record(
        info,
        JobSpanEdge::end(JobState::Exited, Some(&ExitStatus::Exited { code: 0 })),
    );
    let health = publisher.health().await.expect("the writer answers");
    assert_eq!((health.recorded, health.failed), (2, 0), "{health:?}");
    sealed_segments(telemetry, "job-")
}

/// The declaration is the spec's table: the schema the writer derives from it is exactly these
/// columns, so there is no second schema to keep in step.
#[test]
fn the_declared_job_columns_are_the_spec_s() {
    assert_eq!(job_fields(), fields(&spec_job_columns()));
}

/// Each edge of a job span is one sealed segment holding exactly lmao's system columns and the
/// declared job columns, with the job's durable key, its W3C span id and, on the end row only,
/// its state.
#[tokio::test]
async fn a_sealed_job_span_carries_exactly_the_declared_columns() {
    let telemetry = TempRoot::new("cowshed-job-spans");
    let info = running_job();
    let segments = sealed_start_and_end(&telemetry, &info).await;
    assert_eq!(segments.len(), 2);
    for (name, batch) in &segments {
        exact_schema(&batch.schema(), &spec_job_columns())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(batch.num_rows(), 1, "{name} holds one row");
    }

    let thread = job_span_thread(&info.repo_id, &info.workspace_incarnation, info.job_id);
    let w3c_span_id = u64::from_str_radix(info.trace.span_id.as_str(), 16).expect("hex span id");
    for ((_, batch), (entry_type, job_state)) in segments
        .iter()
        .zip([("span-start", None), ("span-ok", Some("exited"))])
    {
        let text = |column: &str| text_cell(batch, column, 0);
        let number = |column: &str| number_cell::<UInt64Type>(batch, column, 0);
        assert_eq!(text("trace_id").as_deref(), Some(TRACE));
        assert_eq!(number("thread_id"), Some(thread));
        assert_eq!(text("entry_type").as_deref(), Some(entry_type));
        assert_eq!(text("message").as_deref(), Some("cowshed.job"));
        assert_eq!(text("repo_id").as_deref(), Some("acme/widget"));
        assert_eq!(
            text("workspace_incarnation").as_deref(),
            Some(info.workspace_incarnation.as_str())
        );
        assert_eq!(number("job_id"), Some(JOB));
        assert_eq!(number("grant_revision"), Some(3));
        assert_eq!(number("w3c_span_id"), Some(w3c_span_id));
        assert_eq!(text("job_state").as_deref(), job_state);
    }

    // Positive controls: the oracle rejects what it exists to reject.
    let schema = segments[0].1.schema();
    let with = |field: Field| {
        let mut columns = schema.fields().iter().cloned().collect::<Vec<_>>();
        columns.push(field.into());
        Schema::new(columns)
    };
    assert!(
        exact_schema(
            &with(Field::new("disk_ws_delta_bytes", DataType::Int64, true)),
            &spec_job_columns()
        )
        .is_err(),
        "a column the declaration does not name"
    );
    let mut narrower = spec_job_columns();
    narrower.pop();
    assert!(
        exact_schema(&schema, &narrower).is_err(),
        "a column the segment carries and the spec lacks"
    );
    let mut nullable = spec_job_columns();
    nullable[0].2 = true;
    assert!(
        exact_schema(&schema, &nullable).is_err(),
        "a required column read as nullable"
    );
}

/// `info`'s job, running as job `job_id`.
fn job(job_id: u64) -> JobInfo {
    JobInfo {
        job_id: JobId::new(job_id).expect("a job id"),
        ..running_job()
    }
}

/// A terminal resource sample of `job_id` whose hosts and volumes are the ones given; every
/// other reading is quiet.
fn sample(
    job_id: JobId,
    host_start: HostLoadSample,
    host: HostLoadSample,
    volumes: JobVolumeUsage,
) -> JobResourceSample {
    let wall = WallMicros::new(1_000_000).expect("a wall time");
    let zero = CpuMicros::new(0).expect("a CPU time");
    let quiet = JobStreamWatermark {
        bytes: StreamBytes::new(0).expect("bytes"),
        lines: StreamLines::new(0).expect("lines"),
    };
    JobResourceSample {
        job_id,
        sampled_at: utc(1),
        wall_ms: wall.millis(),
        wall_us: wall,
        leader_pid: 100,
        members: Vec::new(),
        leaf: JobLeafAttribution::NoAccountedCpu,
        cpu_user_ms: zero.millis(),
        cpu_sys_ms: zero.millis(),
        cpu_pct: OneCoreCpuPercent::ZERO,
        host_start,
        host,
        rss_bytes: ResidentBytes::ZERO,
        rss_peak_bytes: ResidentBytes::ZERO,
        accounting: JobAccounting::MacOsRusageChildren {
            cpu: CpuTotals {
                user_us: zero,
                sys_us: zero,
            },
            io: None,
            reconciliation: UsageReconciliation {
                cpu_us: CpuMicrosDelta::new(0).expect("a CPU delta"),
                io_read_bytes: None,
                io_write_bytes: None,
                coverage: ProcessCoverage::Complete,
            },
        },
        volumes,
        stdout: quiet,
        stderr: quiet,
    }
}

fn read(bytes: i64) -> VolumeUsage {
    VolumeUsage::Read {
        delta_bytes: VolumeUsedBytesDelta::new(bytes).expect("a volume delta"),
    }
}

fn unavailable(reason: VolumeUnavailable) -> VolumeUsage {
    VolumeUsage::Unavailable { reason }
}

/// What one sealed edge states of the host and the volumes.
#[derive(Debug, Eq, PartialEq)]
struct Facts {
    load1_milli: Option<u32>,
    cores: Option<u16>,
    disk_ws_delta_bytes: Option<i64>,
    disk_build_delta_bytes: Option<i64>,
}

impl Facts {
    const NONE: Self = Self {
        load1_milli: None,
        cores: None,
        disk_ws_delta_bytes: None,
        disk_build_delta_bytes: None,
    };

    fn of(batch: &RecordBatch) -> Self {
        Self {
            load1_milli: number_cell::<UInt32Type>(batch, "load1_milli", 0),
            cores: number_cell::<UInt16Type>(batch, "cores", 0),
            disk_ws_delta_bytes: number_cell::<Int64Type>(batch, "disk_ws_delta_bytes", 0),
            disk_build_delta_bytes: number_cell::<Int64Type>(batch, "disk_build_delta_bytes", 0),
        }
    }
}

/// Two sampled hosts and signed volume deltas reach one job span's start and end rows through the
/// production writer: the start row states the start host, the end row the terminal sample's host
/// and each volume's actual delta. A job with no build volume leaves that column null; adding a
/// volume then states its delta, negative when deletion shrank it; a delta of zero is a value; a
/// volume whose usage was unavailable, and a job with no sample at all, state nothing.
#[tokio::test]
async fn host_and_volume_facts_reach_the_start_and_end_rows_of_each_job_span() {
    let telemetry = TempRoot::new("cowshed-job-span-facts");
    let publisher = JobSpanPublisher::start(telemetry.to_path_buf());
    let start_host = HostLoadSample::new(1.2346, 8).expect("a host");
    let end_host = HostLoadSample::new(2.5, 12).expect("a host");
    let failed = VolumeUnavailable::Failed {
        message: "the stat failed".to_owned(),
    };
    // Each job's volumes, and the disk cells its end row states.
    let cases = [
        (
            JobVolumeUsage {
                workspace: read(4096),
                build: None,
            },
            (Some(4096), None),
        ),
        (
            JobVolumeUsage {
                workspace: read(-8192),
                build: Some(read(-1_048_576)),
            },
            (Some(-8192), Some(-1_048_576)),
        ),
        (
            JobVolumeUsage {
                workspace: read(0),
                build: Some(read(0)),
            },
            (Some(0), Some(0)),
        ),
        (
            JobVolumeUsage {
                workspace: unavailable(VolumeUnavailable::Unconfigured),
                build: Some(unavailable(failed)),
            },
            (None, None),
        ),
    ];
    let end = JobSpanEdge::end(JobState::Exited, Some(&ExitStatus::Exited { code: 0 }));
    for (index, (volumes, _)) in cases.iter().enumerate() {
        let info = JobInfo {
            resources: Some(sample(
                JobId::new(u64::try_from(index).unwrap() + 1).unwrap(),
                start_host,
                end_host,
                volumes.clone(),
            )),
            ..job(u64::try_from(index).unwrap() + 1)
        };
        publisher.record(
            &info,
            JobSpanEdge::Start {
                host: Some(start_host),
            },
        );
        publisher.record(&info, end);
    }
    // No sample and no admission host: nothing is stated, and nothing is zero.
    let unsampled = job(5);
    publisher.record(&unsampled, JobSpanEdge::Start { host: None });
    publisher.record(&unsampled, end);
    let health = publisher.health().await.expect("the writer answers");
    assert_eq!((health.recorded, health.failed), (10, 0), "{health:?}");

    let segments = sealed_segments(&telemetry, "job-");
    assert_eq!(segments.len(), 10);
    for (name, batch) in &segments {
        exact_schema(&batch.schema(), &spec_job_columns())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
    }
    for (index, (_, (workspace, build))) in cases.iter().enumerate() {
        let (start, end) = (&segments[index * 2].1, &segments[index * 2 + 1].1);
        let job_id = u64::try_from(index).unwrap() + 1;
        assert_eq!(number_cell::<UInt64Type>(start, "job_id", 0), Some(job_id));
        assert_eq!(number_cell::<UInt64Type>(end, "job_id", 0), Some(job_id));
        assert_eq!(
            Facts::of(start),
            Facts {
                load1_milli: Some(1235),
                cores: Some(8),
                ..Facts::NONE
            },
            "job {job_id}'s start row states its host, rounded to the nearest milliload, and no volume"
        );
        assert_eq!(
            Facts::of(end),
            Facts {
                load1_milli: Some(2500),
                cores: Some(12),
                disk_ws_delta_bytes: *workspace,
                disk_build_delta_bytes: *build,
            },
            "job {job_id}'s end row"
        );
    }
    assert_eq!(Facts::of(&segments[8].1), Facts::NONE);
    assert_eq!(Facts::of(&segments[9].1), Facts::NONE);
}

/// A load that no `u32` milliload holds is refused by the checked conversion, never truncated:
/// the row it would have described is not sealed, and the refusal is counted with its reason.
#[tokio::test]
async fn a_load_no_milliload_column_holds_is_refused_and_counted() {
    let telemetry = TempRoot::new("cowshed-job-span-overflow");
    let publisher = JobSpanPublisher::start(telemetry.to_path_buf());
    let sound = HostLoadSample::new(1.0, 8).expect("a host");
    // Finite and non-negative, so the API's own unit admits it; 5e9 milliloads exceed `u32`.
    let beyond = HostLoadSample::new(5_000_000.0, 8).expect("a finite load");
    let info = JobInfo {
        resources: Some(sample(
            JobId::new(JOB).unwrap(),
            sound,
            beyond,
            JobVolumeUsage {
                workspace: read(1),
                build: None,
            },
        )),
        ..running_job()
    };
    publisher.record(&info, JobSpanEdge::Start { host: Some(sound) });
    publisher.record(
        &info,
        JobSpanEdge::end(JobState::Exited, Some(&ExitStatus::Exited { code: 0 })),
    );
    let health = publisher.health().await.expect("the writer answers");
    assert_eq!((health.recorded, health.failed), (1, 1), "{health:?}");
    assert!(
        health
            .last_failure
            .as_deref()
            .is_some_and(|failure| failure.contains("exceeds the u32 milliload range")),
        "{health:?}"
    );
    let segments = sealed_segments(&telemetry, "job-");
    assert_eq!(segments.len(), 1, "only the start row was sealed");
    assert_eq!(
        Facts::of(&segments[0].1),
        Facts {
            load1_milli: Some(1000),
            cores: Some(8),
            ..Facts::NONE
        }
    );
}

/// A source's totals: the CPU both the macOS and the Linux source count.
fn totals() -> CpuTotals {
    CpuTotals {
        user_us: CpuMicros::new(1_250_000).unwrap(),
        sys_us: CpuMicros::new(80_000).unwrap(),
    }
}

/// A reconciliation of `cpu_us`, and of `io` as `(read, write)` where the source counts bytes.
fn reconciliation(
    cpu_us: i64,
    io: Option<(i64, i64)>,
    coverage: ProcessCoverage,
) -> UsageReconciliation {
    UsageReconciliation {
        cpu_us: CpuMicrosDelta::new(cpu_us).unwrap(),
        io_read_bytes: io.map(|(read, _)| StorageIoBytesDelta::new(read).unwrap()),
        io_write_bytes: io.map(|(_, write)| StorageIoBytesDelta::new(write).unwrap()),
        coverage,
    }
}

fn gap(reason: ProcessCoverageGap) -> ProcessCoverage {
    ProcessCoverage::Gap { reason }
}

/// The macOS source: leader-own plus reaped-children CPU, and no storage bytes to compare.
fn rusage(reconciliation: UsageReconciliation) -> JobAccounting {
    JobAccounting::MacOsRusageChildren {
        cpu: totals(),
        io: None,
        reconciliation,
    }
}

/// The Linux source: the job's own cgroup, whose storage bytes can be compared with the observed
/// processes' own.
fn cgroup(reconciliation: UsageReconciliation) -> JobAccounting {
    JobAccounting::LinuxCgroupV2 {
        cpu: totals(),
        io: StorageIoTotals {
            read_bytes: StorageIoBytes::new(8192).unwrap(),
            write_bytes: StorageIoBytes::new(4096).unwrap(),
        },
        charged_memory: ChargedMemoryUsage {
            current_bytes: ChargedMemoryBytes::new(1 << 20).unwrap(),
            peak_bytes: ChargedMemoryBytes::new(2 << 20).unwrap(),
        },
        reconciliation,
    }
}

/// Job `job_id`'s terminal sample over `accounting`, its hosts and volumes quiet but present.
fn sampled(job_id: u64, accounting: JobAccounting) -> JobResourceSample {
    let cpu = accounting.cpu();
    JobResourceSample {
        cpu_user_ms: cpu.user_us.millis(),
        cpu_sys_ms: cpu.sys_us.millis(),
        accounting,
        ..sample(
            JobId::new(job_id).expect("a job id"),
            HostLoadSample::new(1.25, 8).expect("a host"),
            HostLoadSample::new(4.5, 16).expect("a host"),
            JobVolumeUsage {
                workspace: read(0),
                build: None,
            },
        )
    }
}

/// Job `job_id`, exited cleanly with the terminal `resources` it was frozen with, if it owned a
/// process.
fn ended_job(job_id: u64, resources: Option<JobResourceSample>) -> JobInfo {
    JobInfo {
        state: JobState::Exited,
        duration_ms: Some(1250),
        resources,
        exit: Some(ExitStatus::Exited { code: 0 }),
        ..job(job_id)
    }
}

/// Seal the end of each of `jobs` through one production writer under `telemetry`, and return the
/// segments it sealed, in the order it sealed them.
async fn sealed_ends(telemetry: &Path, jobs: &[JobInfo]) -> Vec<(String, RecordBatch)> {
    let publisher = JobSpanPublisher::start(telemetry.to_path_buf());
    for job in jobs {
        publisher.record(job, JobSpanEdge::end(job.state, job.exit.as_ref()));
    }
    let health = publisher.health().await.expect("the writer answers");
    assert_eq!(health.failed, 0, "{health:?}");
    let sealed = sealed_segments(telemetry, "job-");
    assert_eq!(
        usize::try_from(health.recorded).unwrap(),
        sealed.len(),
        "{health:?}"
    );
    sealed
}

/// The kind of each sealed row: its lmao entry type and its message, as `<entry type> <message>`.
fn kinds(segments: &[(String, RecordBatch)]) -> Vec<String> {
    segments
        .iter()
        .map(|(_, batch)| {
            format!(
                "{} {}",
                text_cell(batch, "entry_type", 0).expect("an entry type"),
                text_cell(batch, "message", 0).expect("a message"),
            )
        })
        .collect()
}

/// The unattributed-usage cells of a sealed row, as a reader sees them: a null cell is `None`.
#[derive(Debug, Eq, PartialEq)]
struct Unattributed {
    cpu_us: Option<i64>,
    io_read_bytes: Option<i64>,
    io_write_bytes: Option<i64>,
    source: Option<String>,
    coverage: Option<String>,
    gap_pid: Option<u32>,
}

fn unattributed_cells(batch: &RecordBatch) -> Unattributed {
    Unattributed {
        cpu_us: number_cell::<Int64Type>(batch, "unattributed_cpu_us", 0),
        io_read_bytes: number_cell::<Int64Type>(batch, "unattributed_io_read_bytes", 0),
        io_write_bytes: number_cell::<Int64Type>(batch, "unattributed_io_write_bytes", 0),
        source: text_cell(batch, "unattributed_source", 0),
        coverage: text_cell(batch, "unattributed_coverage", 0),
        gap_pid: number_cell::<UInt32Type>(batch, "unattributed_gap_pid", 0),
    }
}

/// The cells of a row that states no unattributed usage.
fn nothing_stated() -> Unattributed {
    Unattributed {
        cpu_us: None,
        io_read_bytes: None,
        io_write_bytes: None,
        source: None,
        coverage: None,
        gap_pid: None,
    }
}

/// The wire kind of the source that counted `accounting`'s totals.
fn wire_source(accounting: &JobAccounting) -> String {
    serde_json::to_value(accounting).expect("accounting serializes")["kind"]
        .as_str()
        .expect("a tagged source")
        .to_owned()
}

/// What a job's terminal sample states: its totals' source by wire kind, and its reconciliation.
fn stated_by(job: &JobInfo) -> (String, UsageReconciliation) {
    let accounting = &job.resources.as_ref().expect("a sampled job").accounting;
    (wire_source(accounting), *accounting.reconciliation())
}

/// The reconciliation a row's cells state, rebuilt through the wire's own record: the source that
/// counted the totals by its wire kind, and the reconciliation with its coverage. `None` for a
/// row that states nothing. A reader holding only the sealed segment recovers the very record the
/// supervisor reconciled, or the cells lost a distinction.
fn restated(cells: &Unattributed) -> Option<(String, UsageReconciliation)> {
    let source = cells.source.clone()?;
    let coverage = match cells.coverage.as_deref()? {
        "complete" => serde_json::json!({"kind": "complete"}),
        kind => {
            let mut reason = serde_json::json!({"kind": kind});
            if let Some(pid) = cells.gap_pid {
                reason["pid"] = serde_json::json!(pid);
            }
            serde_json::json!({"kind": "gap", "reason": reason})
        }
    };
    let mut record = serde_json::json!({"cpuUs": cells.cpu_us?, "coverage": coverage});
    if let Some(bytes) = cells.io_read_bytes {
        record["ioReadBytes"] = serde_json::json!(bytes);
    }
    if let Some(bytes) = cells.io_write_bytes {
        record["ioWriteBytes"] = serde_json::json!(bytes);
    }
    let reconciliation =
        serde_json::from_value(record).expect("cells the wire's reconciliation accepts");
    Some((source, reconciliation))
}

const UNATTRIBUTED_ROW: &str = "info job.unattributed";
const END_ROW: &str = "span-ok cowshed.job";

/// A positive missing attribution, a signed precision and window difference, and a comparison no
/// source could make reach the span as three different rows, and each decodes to what it stated.
#[tokio::test]
async fn a_terminal_reconciliation_reaches_its_span_as_one_row_that_keeps_every_distinction() {
    // The cgroup counted CPU and bytes no observed process did, and events were lost.
    let missing = ended_job(
        1,
        Some(sampled(
            1,
            cgroup(reconciliation(
                1_500_000,
                Some((4096, 2048)),
                gap(ProcessCoverageGap::EventsLost),
            )),
        )),
    );
    // The processes counted more than the totals over a complete tree: the sources' counters and
    // sampling windows disagree, and no process was missed. A zero difference is a count.
    let window = ended_job(
        2,
        Some(sampled(
            2,
            cgroup(reconciliation(
                -1_250,
                Some((0, -512)),
                ProcessCoverage::Complete,
            )),
        )),
    );
    // The macOS source has no storage bytes to compare, and a member exited unread.
    let unavailable = ended_job(
        3,
        Some(sampled(
            3,
            rusage(reconciliation(
                0,
                None,
                gap(ProcessCoverageGap::UnreadFinalUsage { pid: 31 }),
            )),
        )),
    );
    let jobs = [missing, window, unavailable];
    let telemetry = TempRoot::new("cowshed-job-unattributed");
    let segments = sealed_ends(&telemetry, &jobs).await;

    assert_eq!(
        kinds(&segments),
        [UNATTRIBUTED_ROW, END_ROW].repeat(3),
        "each job states its unattributed usage once, before its end"
    );
    let cells = segments
        .iter()
        .map(|(_, batch)| unattributed_cells(batch))
        .collect::<Vec<_>>();
    assert_eq!(
        cells,
        [
            Unattributed {
                cpu_us: Some(1_500_000),
                io_read_bytes: Some(4096),
                io_write_bytes: Some(2048),
                source: Some("linuxCgroupV2".to_owned()),
                coverage: Some("eventsLost".to_owned()),
                gap_pid: None,
            },
            nothing_stated(),
            Unattributed {
                cpu_us: Some(-1_250),
                io_read_bytes: Some(0),
                io_write_bytes: Some(-512),
                source: Some("linuxCgroupV2".to_owned()),
                coverage: Some("complete".to_owned()),
                gap_pid: None,
            },
            nothing_stated(),
            Unattributed {
                cpu_us: Some(0),
                io_read_bytes: None,
                io_write_bytes: None,
                source: Some("macOsRusageChildren".to_owned()),
                coverage: Some("unreadFinalUsage".to_owned()),
                gap_pid: Some(31),
            },
            nothing_stated(),
        ]
    );
    for (job, row) in jobs.iter().zip(cells.chunks(2)) {
        assert_eq!(
            restated(&row[0]),
            Some(stated_by(job)),
            "the row states the very reconciliation the job's terminal sample held"
        );
        assert_eq!(restated(&row[1]), None, "its end row states nothing");
    }

    // The unattributed-usage row states no host or volume fact; the end row states the sample's.
    for row in segments.chunks(2) {
        assert_eq!(Facts::of(&row[0].1), Facts::NONE);
        assert_eq!(
            Facts::of(&row[1].1),
            Facts {
                load1_milli: Some(4500),
                cores: Some(16),
                disk_ws_delta_bytes: Some(0),
                disk_build_delta_bytes: None,
            }
        );
    }

    // The row is one row of the job's span: its address and identity, and no state of its own.
    let thread = job_span_thread(
        &running_job().repo_id,
        &running_job().workspace_incarnation,
        JobId::new(1).unwrap(),
    );
    let (_, row) = &segments[0];
    assert_eq!(number_cell::<UInt64Type>(row, "thread_id", 0), Some(thread));
    assert_eq!(number_cell::<UInt32Type>(row, "span_id", 0), Some(1));
    assert_eq!(number_cell::<UInt32Type>(row, "parent_span_id", 0), None);
    assert_eq!(text_cell(row, "repo_id", 0).as_deref(), Some("acme/widget"));
    assert_eq!(number_cell::<UInt64Type>(row, "job_id", 0), Some(1));
    assert_eq!(number_cell::<UInt64Type>(row, "grant_revision", 0), Some(3));
    assert_eq!(text_cell(row, "job_state", 0), None);
}

/// Each way a tree can be found incomplete is its own value in the span, with the pid its
/// evidence names where it names one.
#[tokio::test]
async fn every_coverage_gap_reaches_the_span_as_its_own_kind_with_its_evidence() {
    let gaps = [
        (ProcessCoverageGap::EventsLost, "eventsLost", None),
        (
            ProcessCoverageGap::UnobservedBirth { pid: 11 },
            "unobservedBirth",
            Some(11),
        ),
        (
            ProcessCoverageGap::UnobservedExit { pid: 12 },
            "unobservedExit",
            Some(12),
        ),
        (
            ProcessCoverageGap::UncountedFork { pid: 13 },
            "uncountedFork",
            Some(13),
        ),
        (
            ProcessCoverageGap::UnreadImage { pid: 14 },
            "unreadImage",
            Some(14),
        ),
        (
            ProcessCoverageGap::UnreadFinalUsage { pid: 15 },
            "unreadFinalUsage",
            Some(15),
        ),
    ];
    let jobs = gaps
        .iter()
        .zip(1..)
        .map(|((reason, _, _), job_id)| {
            let accounting = rusage(reconciliation(0, None, gap(*reason)));
            ended_job(job_id, Some(sampled(job_id, accounting)))
        })
        .collect::<Vec<_>>();
    let telemetry = TempRoot::new("cowshed-job-unattributed-gaps");
    let segments = sealed_ends(&telemetry, &jobs).await;

    assert_eq!(
        kinds(&segments),
        [UNATTRIBUTED_ROW, END_ROW].repeat(gaps.len())
    );
    let stated = segments
        .iter()
        .step_by(2)
        .map(|(_, batch)| unattributed_cells(batch))
        .collect::<Vec<_>>();
    assert_eq!(
        stated
            .iter()
            .map(|cells| (cells.coverage.clone(), cells.gap_pid))
            .collect::<Vec<_>>(),
        gaps.iter()
            .map(|(_, kind, pid)| (Some((*kind).to_owned()), *pid))
            .collect::<Vec<_>>()
    );
    for (job, cells) in jobs.iter().zip(&stated) {
        assert_eq!(restated(cells), Some(stated_by(job)));
    }
}

/// A difference keeps its sign and its whole magnitude, to the largest value every consumer holds
/// exactly: nothing is clamped to zero, saturated or wrapped on the way into the span.
#[tokio::test]
async fn signed_differences_keep_their_sign_and_their_largest_exact_magnitude() {
    let largest = i64::try_from(cowshed_core::api::MAX_EXACT_INTEGER).unwrap();
    let observed = [
        (1, Some((1, -1))),
        (-1, Some((-1, 1))),
        (largest, Some((largest, -largest))),
        (-largest, Some((-largest, largest))),
    ];
    let jobs = observed
        .iter()
        .zip(1..)
        .map(|(&(cpu_us, io), job_id)| {
            let accounting = cgroup(reconciliation(cpu_us, io, ProcessCoverage::Complete));
            ended_job(job_id, Some(sampled(job_id, accounting)))
        })
        .collect::<Vec<_>>();
    let telemetry = TempRoot::new("cowshed-job-unattributed-signed");
    let segments = sealed_ends(&telemetry, &jobs).await;

    let stated = segments
        .iter()
        .step_by(2)
        .map(|(_, batch)| unattributed_cells(batch))
        .collect::<Vec<_>>();
    assert_eq!(
        stated
            .iter()
            .map(|cells| (cells.cpu_us, cells.io_read_bytes, cells.io_write_bytes))
            .collect::<Vec<_>>(),
        observed
            .iter()
            .map(|&(cpu_us, io)| {
                let (read, write) = io.unwrap();
                (Some(cpu_us), Some(read), Some(write))
            })
            .collect::<Vec<_>>()
    );
    for (job, cells) in jobs.iter().zip(&stated) {
        assert_eq!(restated(cells), Some(stated_by(job)));
    }
}

/// A job with no terminal sample has nothing to compare, and a complete tree that agrees in every
/// comparison its source could make has nothing to state: neither writes a row it did not mean.
#[tokio::test]
async fn a_job_with_nothing_unattributed_writes_only_its_end_row() {
    let unsampled = ended_job(1, None);
    let agreed = ended_job(
        2,
        Some(sampled(
            2,
            cgroup(reconciliation(0, Some((0, 0)), ProcessCoverage::Complete)),
        )),
    );
    let telemetry = TempRoot::new("cowshed-job-unattributed-none");
    let segments = sealed_ends(&telemetry, &[unsampled, agreed]).await;
    assert_eq!(kinds(&segments), [END_ROW, END_ROW]);
    for (name, batch) in &segments {
        assert_eq!(unattributed_cells(batch), nothing_stated(), "{name}");
    }
}

/// The row is the reconciliation and nothing more: whichever leaf the sample names, or why it
/// names none, the span states the same differences, source and coverage, and carries no leaf of
/// its own for a baseline to read.
#[tokio::test]
async fn the_row_states_the_reconciliation_whatever_leaf_the_sample_names() {
    let accounting = rusage(reconciliation(
        -40_000,
        None,
        gap(ProcessCoverageGap::UncountedFork { pid: 100 }),
    ));
    let leaves = [
        JobLeafAttribution::Attributed {
            leaf: cowshed_core::api::JobProcessLeaf {
                pid: 100,
                program: "/bin/sh".into(),
                argv: vec!["/bin/sh".into()],
                truncated: false,
            },
            observed_cpu_permille: 950,
        },
        JobLeafAttribution::Unattributed {
            observed_cpu_permille: 300,
            required_permille: 900,
        },
        JobLeafAttribution::NoAccountedCpu,
    ];
    let jobs = leaves
        .into_iter()
        .zip(1..)
        .map(|(leaf, job_id)| {
            let sample = JobResourceSample {
                leaf,
                ..sampled(job_id, accounting)
            };
            ended_job(job_id, Some(sample))
        })
        .collect::<Vec<_>>();
    let telemetry = TempRoot::new("cowshed-job-unattributed-leaf");
    let segments = sealed_ends(&telemetry, &jobs).await;

    assert_eq!(kinds(&segments), [UNATTRIBUTED_ROW, END_ROW].repeat(3));
    let stated = segments
        .iter()
        .step_by(2)
        .map(|(_, batch)| unattributed_cells(batch))
        .collect::<Vec<_>>();
    assert_eq!(stated[0], stated[1]);
    assert_eq!(stated[1], stated[2]);
    assert_eq!(restated(&stated[0]), Some(stated_by(&jobs[0])));
}

/// A row the writer cannot seal is counted, the unattributed-usage row among them: a refused gap
/// is never silently dropped.
#[tokio::test]
async fn a_refused_unattributed_row_is_counted_beside_its_end_row() {
    let root = TempRoot::new("cowshed-job-unattributed-refused");
    let publisher = JobSpanPublisher::start(root.join("absent-telemetry"));
    let job = ended_job(
        1,
        Some(sampled(
            1,
            rusage(reconciliation(
                5,
                None,
                gap(ProcessCoverageGap::UncountedFork { pid: 100 }),
            )),
        )),
    );
    publisher.record(&job, JobSpanEdge::end(job.state, job.exit.as_ref()));
    let health = publisher.health().await.expect("the writer answers");
    assert_eq!((health.recorded, health.failed), (0, 2), "{health:?}");
}

/// The unattributed-usage row adds six fixed columns to the job span and nothing else: a gap
/// column named for a process, a JSON string, a column named for a metric or a sample, a leaf
/// nothing observed, a signed difference stored unsigned and a gap without the column its
/// evidence needs all fail the exact-schema control.
#[tokio::test]
async fn a_gap_carried_outside_the_six_fixed_columns_fails_the_exact_schema_control() {
    let job = ended_job(
        1,
        Some(sampled(
            1,
            rusage(reconciliation(
                5,
                None,
                gap(ProcessCoverageGap::UncountedFork { pid: 100 }),
            )),
        )),
    );
    let telemetry = TempRoot::new("cowshed-job-unattributed-schema");
    let segments = sealed_ends(&telemetry, &[job]).await;
    assert_eq!(kinds(&segments), [UNATTRIBUTED_ROW, END_ROW]);
    for (name, batch) in &segments {
        exact_schema(&batch.schema(), &spec_job_columns())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
    }

    // Positive controls: the oracle rejects what it exists to reject.
    let schema = segments[0].1.schema();
    let with = |field: Field| {
        let mut columns = schema.fields().iter().cloned().collect::<Vec<_>>();
        columns.push(field.into());
        Schema::new(columns)
    };
    for (extra, what) in [
        (
            Field::new("proc_unattributed_cpu_us", DataType::Int64, true),
            "a process-specific gap column",
        ),
        (
            Field::new("unattributed_json", DataType::Utf8, true),
            "a JSON string column",
        ),
        (
            Field::new("unattributed_cpu_us_pid_100", DataType::Int64, true),
            "a column named for a process",
        ),
        (
            Field::new("unattributed_leaf_pid", DataType::UInt32, true),
            "a leaf the observed processes did not earn",
        ),
    ] {
        assert!(
            exact_schema(&with(extra), &spec_job_columns()).is_err(),
            "{what}"
        );
    }
    let mut unsigned = spec_job_columns();
    unsigned
        .iter_mut()
        .find(|column| column.0 == "unattributed_cpu_us")
        .expect("the signed difference's column")
        .1 = "u64";
    assert!(
        exact_schema(&schema, &unsigned).is_err(),
        "a signed difference stored unsigned"
    );
    let mut dropped = spec_job_columns();
    dropped.retain(|column| column.0 != "unattributed_gap_pid");
    assert!(
        exact_schema(&schema, &dropped).is_err(),
        "a gap whose evidence has no column"
    );
}

const MIB: u64 = 1 << 20;

/// Charged memory belongs only to the end row, including when an unattributed row precedes it.
async fn charged_cells(label: &str, info: &JobInfo) -> [(Option<u64>, Option<u64>); 2] {
    let telemetry = TempRoot::new(label);
    let publisher = JobSpanPublisher::start(telemetry.to_path_buf());
    publisher.record(info, JobSpanEdge::Start { host: None });
    publisher.record(info, JobSpanEdge::end(info.state, info.exit.as_ref()));
    let expected_rows = if info.resources.as_ref().is_some_and(|sample| {
        matches!(
            sample.accounting.reconciliation().coverage,
            ProcessCoverage::Gap { .. }
        )
    }) {
        3
    } else {
        2
    };
    let health = publisher.health().await.expect("the writer answers");
    assert_eq!(
        (health.recorded, health.failed),
        (expected_rows, 0),
        "{health:?}"
    );
    let segments = sealed_segments(&telemetry, "job-");
    assert_eq!(segments.len(), usize::try_from(expected_rows).unwrap());
    let cells = |batch: &RecordBatch| {
        (
            number_cell::<UInt64Type>(batch, "charged_memory_current_bytes", 0),
            number_cell::<UInt64Type>(batch, "charged_memory_peak_bytes", 0),
        )
    };
    for (index, (name, batch)) in segments.iter().enumerate() {
        exact_schema(&batch.schema(), &spec_job_columns())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        if index + 1 < segments.len() {
            assert_eq!(
                cells(batch),
                (None, None),
                "only the end row carries charged memory"
            );
        }
    }
    [
        cells(&segments[0].1),
        cells(&segments[segments.len() - 1].1),
    ]
}

/// Cgroup charges and process RSS are distinct, and checked bytes survive without conversion.
#[tokio::test]
async fn a_cgroup_source_s_charged_memory_is_the_end_row_s_alone() {
    let job = JobId::new(JOB).unwrap();
    let sample = terminal_sample(job, Some((96 * MIB, 160 * MIB)), 8 * MIB, 12 * MIB);
    assert_eq!(
        charged_cells("cowshed-job-span-cgroup", &ended_job(JOB, Some(sample))).await,
        [(None, None), (Some(96 * MIB), Some(160 * MIB))]
    );
    let sample = terminal_sample(job, Some((MAX_EXACT_INTEGER - 1, MAX_EXACT_INTEGER)), 0, 0);
    assert_eq!(
        charged_cells("cowshed-job-span-largest", &ended_job(JOB, Some(sample))).await[1],
        (Some(MAX_EXACT_INTEGER - 1), Some(MAX_EXACT_INTEGER))
    );
}

/// A macOS source or absent terminal sample leaves charged cells null, not zero or RSS.
#[tokio::test]
async fn a_source_without_a_charged_memory_counter_leaves_both_cells_absent() {
    let job = JobId::new(JOB).unwrap();
    let absent = [(None, None), (None, None)];
    let macos = terminal_sample(job, None, 64 * MIB, 96 * MIB);
    assert_eq!(
        charged_cells("cowshed-job-span-macos", &ended_job(JOB, Some(macos))).await,
        absent,
        "a macOS rusage record"
    );
    assert_eq!(
        charged_cells("cowshed-job-span-unsampled", &ended_job(JOB, None)).await,
        absent,
        "a job that ended without a sample"
    );
    let cgroup = terminal_sample(job, Some((96 * MIB, 160 * MIB)), 64 * MIB, 96 * MIB);
    assert_eq!(
        charged_cells("cowshed-job-span-control", &ended_job(JOB, Some(cgroup))).await,
        [(None, None), (Some(96 * MIB), Some(160 * MIB))],
        "the same resident memory under a cgroup source"
    );
}
