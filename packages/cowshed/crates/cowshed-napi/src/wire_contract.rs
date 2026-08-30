//! The corpus of JSON the napi exports actually hand JavaScript, and the proof it is current.
//!
//! Every `canonical_json` export in this crate is `serde_json::to_string` of a `cowshed-core`
//! DTO, and `packages/cowshed/src/types.ts` restates those DTOs as typia types so `index.ts` can
//! validate the bytes. Two hand-written statements of one wire drift silently: nothing in either
//! language reads the other. This corpus is the shared witness that makes drift a test failure in
//! whichever language moved.
//!
//! - This module proves the committed file is byte-equal to what core serializes today, through
//!   the same `canonical_json` the exports call. Change a DTO without regenerating and it is red
//!   here.
//! - `packages/cowshed/src/wire-contract.test.ts` proves the TypeScript types accept exactly
//!   these documents and nothing wider. Regenerate the corpus without moving `types.ts` and it is
//!   red there.
//!
//! Every enum variant and every optional field that can appear on the wire must appear in some
//! case: a variant absent from the corpus is a variant the TypeScript side is unverified against.
//! Regenerate with `COWSHED_WIRE_FIXTURES=write cargo test -p cowshed-napi`.

use std::{collections::BTreeMap, ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

use cowshed_core::{
    api::{
        AbandonedWork, BinaryData, CheckpointInfo, CommandArg, DoctorReport, EgressMode,
        EgressRule, ExitStatus, Finding, FindingSeverity, GcCandidate, GcReason, GcReport, GitOid,
        GrantSet, ImageFormat, JobId, JobInfo, JobState, LandReport, LandingCommits,
        OutputLimitInfo, OutputStorage, OutputSummary, PortBlock, ProtectedOutput, PushReport,
        RemoveReport, RepoRule, ResizeResult, Sha256Digest, SimVerb, SpanId, StdinInfo, StdinKind,
        StreamInfo, TraceContext, TraceId, UtcTimestamp, WorkspaceIncarnation, WorkspaceInfo,
        WorkspaceLanding, WorkspaceName, WorkspacePath, WorkspaceRole, WorkspaceState,
    },
    metadata::PORT_BLOCK_SIZE,
    repository::RepoId,
};
use serde::Serialize;
use serde_json::Value;

use super::canonical_json;

/// `include_str!` makes the corpus a compile input of this crate, so editing the file alone
/// cannot leave this assertion unrun behind an up-to-date build.
const GOLDEN: &str = include_str!("../../../src/wire-fixtures.json");
const GOLDEN_PATH: &str = "packages/cowshed/src/wire-fixtures.json";
const WRITE_ENV: &str = "COWSHED_WIRE_FIXTURES";

/// One JSON document exactly as a napi export would resolve it.
fn document<T: Serialize>(kind: &'static str, value: &T) -> Value {
    let json = canonical_json(kind, value)
        .unwrap_or_else(|failure| panic!("fixture {kind} must serialize: {}", failure.message));
    serde_json::from_str(&json).unwrap_or_else(|error| panic!("{kind} emits valid JSON: {error}"))
}

fn repo_id() -> RepoId {
    RepoId::parse("smoothbricks/codebase").expect("fixture repo id is well formed")
}

fn incarnation() -> WorkspaceIncarnation {
    WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef")
        .expect("fixture incarnation is 32 lowercase hex digits")
}

fn workspace_name(value: &str) -> WorkspaceName {
    WorkspaceName::new(value).expect("fixture workspace name is well formed")
}

fn oid(value: &str) -> GitOid {
    GitOid::new(value).expect("fixture git oid is 40 lowercase hex digits")
}

fn timestamp() -> UtcTimestamp {
    UtcTimestamp::new("2026-01-02T03:04:05Z").expect("fixture timestamp is RFC 3339 UTC")
}

fn trace() -> TraceContext {
    TraceContext {
        trace_id: TraceId::new("00112233445566778899aabbccddeeff").expect("fixture trace id"),
        span_id: SpanId::new("0011223344556677").expect("fixture span id"),
    }
}

fn workspace_path(value: &str) -> WorkspacePath {
    WorkspacePath::new(PathBuf::from(value)).expect("fixture workspace path is relative and clean")
}

fn summary(text: &str, truncated: bool) -> OutputSummary {
    OutputSummary {
        version: 1,
        text: text.to_owned(),
        truncated,
    }
}

/// A stream whose bytes are inline in the DTO. `StreamInfo::validate` cross-checks the length and
/// digest against the payload, so those cannot be invented here.
fn inline_stream(text: &str) -> StreamInfo {
    let bytes = text.as_bytes();
    StreamInfo {
        storage: OutputStorage::Captured {
            artifact: ProtectedOutput::Inline {
                data: BinaryData::new(bytes.to_vec()).expect("fixture payload is under the limit"),
            },
        },
        bytes: bytes.len() as u64,
        sha256: Sha256Digest::compute(bytes),
        summary: summary(text, false),
    }
}

/// A stream spilled to the protected job directory. The path is the one `JobInfo::validate`
/// demands for this job and leaf, which is why the job id has to be threaded through.
fn captured_file_stream(job: u64, leaf: &str, bytes: u64) -> StreamInfo {
    StreamInfo {
        storage: OutputStorage::Captured {
            artifact: ProtectedOutput::File {
                path: workspace_path(&format!(".cowshed/job/{job}/{leaf}")),
            },
        },
        bytes,
        sha256: Sha256Digest::compute(b"captured"),
        summary: summary("captured to the protected job directory", true),
    }
}

/// A stream the job redirected into the workspace, with the protected copy beside it.
fn redirect_stream(job: u64, leaf: &str, source: &str, bytes: u64) -> StreamInfo {
    StreamInfo {
        storage: OutputStorage::Redirect {
            source: workspace_path(source),
            artifact: ProtectedOutput::File {
                path: workspace_path(&format!(".cowshed/job/{job}/{leaf}")),
            },
        },
        bytes,
        sha256: Sha256Digest::compute(b"redirected"),
        summary: summary("redirected into the workspace", false),
    }
}

fn empty_stdin() -> StdinInfo {
    StdinInfo {
        kind: StdinKind::Empty,
        bytes: 0,
        workspace_path: None,
        complete: true,
    }
}

fn job_infos() -> BTreeMap<&'static str, Value> {
    // A queued job: no exit, no duration, no output limit, argv that is entirely UTF-8, and the
    // narrowest stdin. This is the shape `listJobs` returns most often.
    let queued = JobInfo {
        repo_id: repo_id(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(1).expect("fixture job id"),
        state: JobState::Queued,
        pid: None,
        grant_revision: 0,
        argv: vec![CommandArg::from("true")],
        cwd: None,
        started: timestamp(),
        duration_ms: None,
        exit: None,
        stdout: inline_stream(""),
        stderr: inline_stream(""),
        trace: trace(),
        output_limit: None,
        stdin: empty_stdin(),
    };

    // A running job with every optional present, a workspace-file stdin, and both stream storage
    // variants that name a workspace path.
    let running = JobInfo {
        repo_id: repo_id(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(2).expect("fixture job id"),
        state: JobState::Running,
        pid: Some(4242),
        grant_revision: 7,
        argv: vec![
            CommandArg::from("cargo"),
            CommandArg::from("test"),
            CommandArg::from("--workspace"),
        ],
        cwd: Some(workspace_path("packages/cowshed")),
        started: timestamp(),
        duration_ms: None,
        exit: None,
        stdout: redirect_stream(2, "out", "build.log", 4096),
        stderr: captured_file_stream(2, "err", 128),
        trace: trace(),
        output_limit: None,
        stdin: StdinInfo {
            kind: StdinKind::WorkspaceFile,
            bytes: 12,
            workspace_path: Some(workspace_path("fixtures/input.txt")),
            complete: true,
        },
    };

    // An exited job with inline stdin. `exit.kind` is `exited` exactly when the state is.
    let exited = JobInfo {
        repo_id: repo_id(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(3).expect("fixture job id"),
        state: JobState::Exited,
        pid: Some(4243),
        grant_revision: 7,
        argv: vec![CommandArg::from("sh"), CommandArg::from("-c")],
        cwd: None,
        started: timestamp(),
        duration_ms: Some(1_234),
        exit: Some(ExitStatus::Exited { code: 0 }),
        stdout: inline_stream("ok\n"),
        stderr: inline_stream(""),
        trace: trace(),
        output_limit: None,
        stdin: StdinInfo {
            kind: StdinKind::Inline,
            bytes: 5,
            workspace_path: None,
            complete: true,
        },
    };

    // A signalled job carrying a non-UTF-8 argument. This is the case a `string[]` argv cannot
    // represent at all, and the reason argv is a tagged union on the wire.
    let signaled = JobInfo {
        repo_id: repo_id(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(4).expect("fixture job id"),
        state: JobState::Signaled,
        pid: Some(4244),
        grant_revision: 9,
        argv: vec![
            CommandArg::from("printf"),
            CommandArg::from("%s"),
            CommandArg::new(OsString::from_vec(vec![0xff, 0xfe, 0x80])),
        ],
        cwd: None,
        started: timestamp(),
        duration_ms: Some(9),
        exit: Some(ExitStatus::Signaled {
            signal: 9,
            core_dumped: true,
        }),
        stdout: inline_stream(""),
        stderr: inline_stream("killed\n"),
        trace: trace(),
        output_limit: None,
        stdin: empty_stdin(),
    };

    // The output-limit state is the only one that carries `outputLimit`, and an incomplete
    // streamed stdin is the only place `complete` is false.
    let output_limit = JobInfo {
        repo_id: repo_id(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(5).expect("fixture job id"),
        state: JobState::OutputLimit,
        pid: Some(4245),
        grant_revision: 9,
        argv: vec![CommandArg::from("yes")],
        cwd: None,
        started: timestamp(),
        duration_ms: Some(50),
        exit: None,
        stdout: captured_file_stream(5, "out", 1_048_576),
        stderr: inline_stream(""),
        trace: trace(),
        output_limit: Some(OutputLimitInfo {
            limit_bytes: 1_048_576,
            crossing_bytes: 1_048_577,
        }),
        stdin: StdinInfo {
            kind: StdinKind::Stream,
            bytes: 64,
            workspace_path: None,
            complete: false,
        },
    };

    // `killed` pairs with a signalled exit; `failed` admits any exit, including none.
    let killed = JobInfo {
        job_id: JobId::new(6).expect("fixture job id"),
        state: JobState::Killed,
        exit: Some(ExitStatus::Signaled {
            signal: 15,
            core_dumped: false,
        }),
        stdout: inline_stream(""),
        stderr: inline_stream(""),
        ..signaled.clone()
    };

    let failed = JobInfo {
        job_id: JobId::new(7).expect("fixture job id"),
        state: JobState::Failed,
        exit: None,
        pid: None,
        stdout: inline_stream(""),
        stderr: inline_stream("spawn refused\n"),
        ..exited.clone()
    };

    let list = vec![queued.clone(), running.clone()];

    BTreeMap::from([
        ("queued", document("job status", &queued)),
        ("running", document("job status", &running)),
        ("exited", document("job status", &exited)),
        ("signaledNonUtf8Argv", document("job status", &signaled)),
        ("outputLimit", document("job status", &output_limit)),
        ("killed", document("job status", &killed)),
        ("failed", document("job status", &failed)),
        ("list", document("job list", &list)),
    ])
}

fn workspace_infos() -> BTreeMap<&'static str, Value> {
    // The main workspace as a bare controller listing reports it: no branch, no base commit, no
    // creation stamp, no landing measurement, and no checkpoints.
    let main = WorkspaceInfo {
        repo_id: repo_id(),
        workspace: workspace_name("main"),
        workspace_incarnation: incarnation(),
        role: WorkspaceRole::Main,
        image_format: ImageFormat::Asif,
        mount: PathBuf::from("/Users/fixture/Dev/codebase"),
        state: WorkspaceState::Attached,
        branch: None,
        base_commit: None,
        created_at: None,
        checkpoints: Vec::new(),
        snapshot_stale: false,
        landing: None,
    };

    let measured = WorkspaceInfo {
        workspace: workspace_name("cs-seam"),
        role: WorkspaceRole::Workspace,
        image_format: ImageFormat::Sparse,
        mount: PathBuf::from("/Users/fixture/Dev/.cowshed/codebase/cs-seam"),
        state: WorkspaceState::Detached,
        branch: Some("cowshed/cs-seam".to_owned()),
        base_commit: Some(oid("0f1e2d3c4b5a69788796a5b4c3d2e1f001234567")),
        created_at: Some(timestamp()),
        checkpoints: vec![
            CheckpointInfo {
                label: "pre-rebase".to_owned(),
                revision: 3,
                pinned: true,
            },
            CheckpointInfo {
                label: "nightly".to_owned(),
                revision: 4,
                pinned: false,
            },
        ],
        snapshot_stale: true,
        landing: Some(WorkspaceLanding {
            dirty_files: Some(2),
            commits: LandingCommits::Measured {
                target_branch: "main".to_owned(),
                target_head: oid("89abcdef0123456789abcdef0123456789abcdef"),
                unlanded: 3,
                landed: 1,
                behind: 5,
            },
        }),
        ..main.clone()
    };

    // "Could not measure" is a different fact from "clean", and has to survive the boundary as
    // its own variant rather than as a zero.
    let indeterminate = WorkspaceInfo {
        workspace: workspace_name("cs-gateway"),
        role: WorkspaceRole::Workspace,
        landing: Some(WorkspaceLanding {
            dirty_files: None,
            commits: LandingCommits::Indeterminate {
                reason: "the target branch does not exist".to_owned(),
            },
        }),
        ..main.clone()
    };

    let list = vec![main.clone(), measured.clone(), indeterminate.clone()];

    BTreeMap::from([
        ("main", document("workspace info", &main)),
        ("landingMeasured", document("workspace info", &measured)),
        (
            "landingIndeterminate",
            document("workspace info", &indeterminate),
        ),
        ("list", document("workspace list", &list)),
    ])
}

fn grant_sets() -> BTreeMap<&'static str, Value> {
    let closed = GrantSet::default();

    let open = GrantSet {
        revision: 12,
        port_block: Some(
            PortBlock::new(51_200, PORT_BLOCK_SIZE).expect("fixture port block is aligned"),
        ),
        read: vec![PathBuf::from("/Users/fixture/.cargo/registry")],
        write: vec![PathBuf::from("/Users/fixture/Library/Caches/sccache")],
        egress: vec![
            EgressRule {
                host: "crates.io".to_owned(),
                ports: Vec::new(),
                mode: EgressMode::Intercept,
                impersonate: None,
            },
            EgressRule {
                host: "github.com".to_owned(),
                ports: vec![22, 443],
                mode: EgressMode::Opaque,
                impersonate: Some("fixture-bot".to_owned()),
            },
        ],
        repos: vec![RepoRule("smoothbricks/*".to_owned())],
        sim: vec![SimVerb::OpenUrl, SimVerb::Install],
    };

    BTreeMap::from([
        ("closed", document("workspace grants", &closed)),
        ("open", document("workspace grants", &open)),
    ])
}

fn reports() -> BTreeMap<&'static str, BTreeMap<&'static str, Value>> {
    let land_first = LandReport {
        landed_head: oid("1111111111111111111111111111111111111111"),
        target_branch: "main".to_owned(),
        previous_target_head: None,
        target_was_checked_out: false,
        retired: false,
    };
    let land_retired = LandReport {
        previous_target_head: Some(oid("2222222222222222222222222222222222222222")),
        target_was_checked_out: true,
        retired: true,
        ..land_first.clone()
    };

    let push_new = PushReport {
        source_head: oid("3333333333333333333333333333333333333333"),
        destination_ref: "refs/heads/cowshed/cs-seam".to_owned(),
        previous_destination_head: None,
    };
    let push_fast_forward = PushReport {
        previous_destination_head: Some(oid("4444444444444444444444444444444444444444")),
        ..push_new.clone()
    };

    // One candidate per `GcReason`: a reason absent here is a reason the TypeScript union is not
    // checked against.
    let gc_dry_run = GcReport {
        examined: 5,
        reclaimed: 0,
        retained_pinned: 1,
        freed_bytes: 0,
        dry_run: true,
        candidates: [
            GcReason::RetiredWorkspace,
            GcReason::OrphanStagingImage,
            GcReason::OrphanStagingMetadata,
            GcReason::ExpiredCheckpoint,
            GcReason::DetachedImageCompaction,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, reason)| GcCandidate {
            identity: Sha256Digest::compute(&[index as u8]),
            path: PathBuf::from(format!("/Users/fixture/Dev/.cowshed/gc/{index}")),
            bytes: 1_024 * (index as u64 + 1),
            reason,
        })
        .collect(),
    };
    let gc_swept = GcReport {
        examined: 5,
        reclaimed: 4,
        retained_pinned: 1,
        freed_bytes: 15_360,
        dry_run: false,
        candidates: Vec::new(),
    };

    let doctor_healthy = DoctorReport {
        healthy: true,
        findings: Vec::new(),
    };
    let doctor_unhealthy = DoctorReport {
        healthy: false,
        findings: vec![
            Finding {
                code: "storage.sparse-fallback".to_owned(),
                severity: FindingSeverity::Info,
                message: "the host fell back to a sparse image".to_owned(),
                hint: "upgrade to a host that supports ASIF".to_owned(),
                path: None,
            },
            Finding {
                code: "gateway.certificate-expiring".to_owned(),
                severity: FindingSeverity::Warning,
                message: "the gateway certificate expires in 6 days".to_owned(),
                hint: "run cowshed gateway renew".to_owned(),
                path: Some(PathBuf::from("/Users/fixture/Library/Application Support")),
            },
            Finding {
                code: "controller.socket-missing".to_owned(),
                severity: FindingSeverity::Error,
                message: "the controller socket is absent".to_owned(),
                hint: "run cowshed setup".to_owned(),
                path: None,
            },
        ],
    };

    let remove_plain = RemoveReport::default();
    let remove_abandoned = RemoveReport {
        abandoned: Some(AbandonedWork {
            head: oid("5555555555555555555555555555555555555555"),
            target_branch: "main".to_owned(),
            target_head: Some(oid("6666666666666666666666666666666666666666")),
            unlanded_commits: 4,
            bundle: PathBuf::from("/Users/fixture/Dev/.cowshed/abandoned/cs-seam.bundle"),
        }),
    };

    let resize = ResizeResult {
        workspace: workspace_name("cs-seam"),
        previous_capacity: "100g".to_owned(),
        capacity: "200g".to_owned(),
    };

    BTreeMap::from([
        (
            "LandReport",
            BTreeMap::from([
                ("firstLanding", document("land report", &land_first)),
                ("retired", document("land report", &land_retired)),
            ]),
        ),
        (
            "PushReport",
            BTreeMap::from([
                ("newBranch", document("push report", &push_new)),
                ("fastForward", document("push report", &push_fast_forward)),
            ]),
        ),
        (
            "GcReport",
            BTreeMap::from([
                ("dryRun", document("GC report", &gc_dry_run)),
                ("swept", document("GC report", &gc_swept)),
            ]),
        ),
        (
            "DoctorReport",
            BTreeMap::from([
                ("healthy", document("doctor report", &doctor_healthy)),
                ("unhealthy", document("doctor report", &doctor_unhealthy)),
            ]),
        ),
        (
            "RemoveReport",
            BTreeMap::from([
                ("imageOnly", document("remove report", &remove_plain)),
                ("abandoned", document("remove report", &remove_abandoned)),
            ]),
        ),
        (
            "ResizeResult",
            BTreeMap::from([("grown", document("resize result", &resize))]),
        ),
    ])
}

fn corpus() -> BTreeMap<&'static str, BTreeMap<&'static str, Value>> {
    let mut corpus = reports();
    corpus.insert("JobInfo", job_infos());
    corpus.insert("WorkspaceInfo", workspace_infos());
    corpus.insert("GrantSet", grant_sets());
    corpus
}

#[test]
fn the_committed_wire_corpus_is_what_core_serializes() {
    let actual = serde_json::to_value(corpus()).expect("the corpus is JSON");
    let mut rendered =
        serde_json::to_string_pretty(&actual).expect("the corpus renders as pretty JSON");
    rendered.push('\n');

    if std::env::var(WRITE_ENV).as_deref() == Ok("write") {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../src/wire-fixtures.json");
        std::fs::write(&path, &rendered).expect("the corpus path is writable");
    }

    assert_eq!(
        rendered, GOLDEN,
        "{GOLDEN_PATH} is not what cowshed-core serializes today. A DTO on the napi seam changed \
         shape. Regenerate with `{WRITE_ENV}=write cargo test -p cowshed-napi \
         the_committed_wire_corpus_is_what_core_serializes`, then run \
         `nx run cowshed:wire-test` so packages/cowshed/src/types.ts moves with it."
    );
}

/// The corpus is only a witness for the variants it contains, so the count of documents is part of
/// the contract: dropping a case has to be a deliberate edit here, not a silent loss of coverage.
#[test]
fn every_seam_type_carries_a_case_per_wire_variant() {
    let corpus = corpus();
    let expected: BTreeMap<&str, usize> = BTreeMap::from([
        ("DoctorReport", 2),
        ("GcReport", 2),
        ("GrantSet", 2),
        ("JobInfo", 8),
        ("LandReport", 2),
        ("PushReport", 2),
        ("RemoveReport", 2),
        ("ResizeResult", 1),
        ("WorkspaceInfo", 4),
    ]);
    let counts: BTreeMap<&str, usize> = corpus
        .iter()
        .map(|(name, cases)| (*name, cases.len()))
        .collect();

    assert_eq!(
        counts, expected,
        "the corpus gained or lost cases; packages/cowshed/src/wire-contract.test.ts enumerates \
         the same table and must move with it"
    );
}

/// Every `JobState` must reach the corpus, because `JobInfo::validate` couples the state to the
/// presence of `exit`, `durationMs`, and `outputLimit`: a state that never serializes here is a
/// combination `types.ts` is unverified against.
#[test]
fn every_job_state_appears_in_the_corpus() {
    let states: Vec<String> = job_infos()
        .values()
        .flat_map(|value| match value {
            Value::Array(items) => items.clone(),
            single => vec![single.clone()],
        })
        .filter_map(|value| {
            value
                .get("state")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();

    for state in [
        JobState::Queued,
        JobState::Running,
        JobState::Exited,
        JobState::Signaled,
        JobState::Killed,
        JobState::OutputLimit,
        JobState::Failed,
    ] {
        let spelling = serde_json::to_value(state).expect("a job state is JSON");
        let spelling = spelling
            .as_str()
            .expect("a job state serializes as a string");
        assert!(
            states.iter().any(|seen| seen == spelling),
            "job state {spelling:?} has no corpus case, so types.ts is unverified for it"
        );
    }
}

/// The tagged argv encoding is the whole point of the corpus: a UTF-8 argument and a byte
/// sequence that is not UTF-8 must produce different tags and both must survive to TypeScript.
#[test]
fn argv_carries_both_command_arg_encodings() {
    let jobs = job_infos();
    let signaled = jobs
        .get("signaledNonUtf8Argv")
        .expect("the non-UTF-8 argv case exists");
    let argv = signaled
        .get("argv")
        .and_then(Value::as_array)
        .expect("argv is an array");

    let encodings: Vec<&str> = argv
        .iter()
        .filter_map(|argument| argument.get("encoding").and_then(Value::as_str))
        .collect();

    assert_eq!(
        encodings,
        vec!["utf8", "utf8", "base64"],
        "argv must serialize as tagged CommandArg objects, with non-UTF-8 bytes as base64"
    );
    assert_eq!(
        argv.last().and_then(|argument| argument.get("data")),
        Some(&Value::String("//6A".to_owned())),
        "the non-UTF-8 argument must be canonical standard base64 of its bytes"
    );
}
