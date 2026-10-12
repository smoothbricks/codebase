//! A job's span through the supervisor's production span writer (13_telemetry.md, "Job span
//! segments"): the writer seals each edge of the span as the segment a reader of lmao's trace
//! schema decodes, carrying exactly the columns the declaration names.

use std::path::Path;

use arrow_array::RecordBatch;
use arrow_array::types::UInt64Type;
use arrow_schema::{DataType, Field, Schema};
use cowshed_core::api::{
    ExecCommand, ExitStatus, JobId, JobInfo, JobState, OutputStorage, OutputSummary,
    ProtectedOutput, Sha256Digest, SpanId, StdinInfo, StdinKind, StreamInfo, TraceContext, TraceId,
    UtcTimestamp, WorkspaceIncarnation, WorkspacePath,
};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::job_spans::{
    JobSpanEdge, JobSpanPublisher, job_fields, job_span_thread,
};

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
    publisher.record(info, JobSpanEdge::Start);
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
