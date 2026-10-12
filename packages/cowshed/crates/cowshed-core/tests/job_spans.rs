//! A job's span through the supervisor's production span writer (13_telemetry.md, "Job span
//! segments", "Process-tree spans"): the writer seals each edge of the span as the segment a
//! reader of lmao's trace schema decodes, carrying exactly the columns the declaration names and
//! the host and volume facts of the job's two boundaries.

use std::path::Path;

use arrow_array::RecordBatch;
use arrow_array::types::{Int64Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use cowshed_core::api::{
    CpuMicros, CpuMicrosDelta, CpuTotals, ExecCommand, ExitStatus, HostLoadSample, JobAccounting,
    JobId, JobInfo, JobLeafAttribution, JobResourceSample, JobState, JobStreamWatermark,
    JobVolumeUsage, MAX_EXACT_INTEGER, OneCoreCpuPercent, OutputStorage, OutputSummary, ProcessCoverage,
    ProtectedOutput, ResidentBytes, Sha256Digest, SpanId, StdinInfo, StdinKind, StreamBytes,
    StreamInfo, StreamLines, TraceContext, TraceId, UsageReconciliation, UtcTimestamp,
    VolumeUnavailable, VolumeUsage, VolumeUsedBytesDelta, WallMicros, WorkspaceIncarnation,
    WorkspacePath,
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

const MIB: u64 = 1 << 20;

/// `running_job()` as it ends with a clean exit, carrying the terminal sample `resources`.
fn ended_job(resources: Option<JobResourceSample>) -> JobInfo {
    JobInfo {
        state: JobState::Exited,
        duration_ms: Some(1_000),
        resources,
        exit: Some(ExitStatus::Exited { code: 0 }),
        ..running_job()
    }
}

/// The charged-memory cells of `info`'s start row and end row, sealed through a production writer
/// under a root of its own, each segment holding exactly the declared job columns.
async fn charged_cells(label: &str, info: &JobInfo) -> [(Option<u64>, Option<u64>); 2] {
    let telemetry = TempRoot::new(label);
    let segments = sealed_start_and_end(&telemetry, info).await;
    assert_eq!(segments.len(), 2);
    for (name, batch) in &segments {
        exact_schema(&batch.schema(), &spec_job_columns())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
    }
    [0, 1].map(|edge| {
        let batch = &segments[edge].1;
        (
            number_cell::<UInt64Type>(batch, "charged_memory_current_bytes", 0),
            number_cell::<UInt64Type>(batch, "charged_memory_peak_bytes", 0),
        )
    })
}

/// A page-cache-heavy job under a cgroup source states its charged memory on its end row and
/// nowhere else. Its cgroup is charged 96 MiB now and 160 MiB at its peak while its processes
/// hold 8 MiB resident now and 12 MiB at most: the two readings differ, so a cell that took a
/// resident number would show. Both edges are sealed from the same sample, and the start row
/// states none even so.
#[tokio::test]
async fn a_cgroup_source_s_charged_memory_is_the_end_row_s_alone() {
    let job = JobId::new(JOB).unwrap();
    let sample = terminal_sample(job, Some((96 * MIB, 160 * MIB)), 8 * MIB, 12 * MIB);
    assert_eq!(
        charged_cells("cowshed-job-span-cgroup", &ended_job(Some(sample))).await,
        [(None, None), (Some(96 * MIB), Some(160 * MIB))]
    );
    // Bytes, exactly: the largest value the checked unit holds survives whole.
    let sample = terminal_sample(job, Some((MAX_EXACT_INTEGER - 1, MAX_EXACT_INTEGER)), 0, 0);
    assert_eq!(
        charged_cells("cowshed-job-span-largest", &ended_job(Some(sample))).await[1],
        (Some(MAX_EXACT_INTEGER - 1), Some(MAX_EXACT_INTEGER))
    );
}

/// A source with no charged-memory counter leaves both cells absent, never zero and never a
/// resident number: a macOS rusage record, whose sample also counts 64 MiB resident, and a job
/// that ended without a sample. The control is that same resident memory under a cgroup source,
/// which states its pair.
#[tokio::test]
async fn a_source_without_a_charged_memory_counter_leaves_both_cells_absent() {
    let job = JobId::new(JOB).unwrap();
    let absent = [(None, None), (None, None)];
    let macos = terminal_sample(job, None, 64 * MIB, 96 * MIB);
    assert_eq!(
        charged_cells("cowshed-job-span-macos", &ended_job(Some(macos))).await,
        absent,
        "a macOS rusage record"
    );
    assert_eq!(
        charged_cells("cowshed-job-span-unsampled", &ended_job(None)).await,
        absent,
        "a job that ended without a sample"
    );
    let cgroup = terminal_sample(job, Some((96 * MIB, 160 * MIB)), 64 * MIB, 96 * MIB);
    assert_eq!(
        charged_cells("cowshed-job-span-control", &ended_job(Some(cgroup))).await,
        [(None, None), (Some(96 * MIB), Some(160 * MIB))],
        "the same resident memory under a cgroup source"
    );
}
