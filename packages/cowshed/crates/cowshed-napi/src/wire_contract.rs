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
//! `nx run cowshed:wire-fixtures` regenerates it and runs ahead of every cargo test target.

use std::{collections::BTreeMap, ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

use cowshed_core::{
    api::{
        AbandonedWork, BinaryData, CarrySide, CheckpointInfo, CommandArg, CpuMicros, CpuTotals,
        DoctorReport, EgressMode, EgressRule, ExitStatus, Finding, FindingSeverity, GcCandidate,
        GcDeferred, GcReason, GcReport, GitOid, GrantSet, HostLoadSample, JobAccounting, JobId,
        JobInfo, JobResourceSample, JobState, JobStreamWatermark, JobVolumeUsage, LandReport,
        LandingCommits, OutputLimitInfo, OutputStorage, OutputSummary, PortBlock, ProtectedOutput,
        PushReport, RebaseBuildVolume, RebaseCarrySkip, RebaseReport, RemoveReport, RepoRule,
        ResidentBytes, ResizeResult, ResizeVolume, Sha256Digest, SimVerb, SpanId, StdinInfo,
        StdinKind, StreamBytes, StreamInfo, StreamLines, TraceContext, TraceId, UtcTimestamp,
        VolumeUnavailable, VolumeUsage, VolumeUsedBytesDelta, WallMicros, WorkspaceIncarnation,
        WorkspaceInfo, WorkspaceLanding, WorkspaceName, WorkspacePath, WorkspaceRole,
        WorkspaceState,
    },
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
        .unwrap_or_else(|failure| panic!("fixture {kind} must serialize: {}", failure.0.message));
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

/// An inline stream's bytes as a sample counts them: each `\n` ends a line, and a trailing line no
/// `\n` ended counts too.
fn watermark(stream: &StreamInfo) -> JobStreamWatermark {
    let OutputStorage::Captured {
        artifact: ProtectedOutput::Inline { data },
    } = &stream.storage
    else {
        panic!("an ended fixture job's streams are inline");
    };
    let bytes = data.as_bytes();
    let ended = bytes.iter().filter(|&&byte| byte == b'\n').count();
    let open = bytes.last().is_some_and(|&byte| byte != b'\n');
    let lines = u64::try_from(ended).expect("fixture line count") + u64::from(open);
    JobStreamWatermark {
        bytes: StreamBytes::new(stream.bytes).expect("fixture stream bytes"),
        lines: StreamLines::new(lines).expect("fixture stream lines"),
    }
}

/// An ended job's terminal sample, taken from the job itself: its leader is the job's process,
/// its wall time the job's duration and its streams the job's output. Nothing of its group is
/// left, and its volumes are `volumes`.
fn ended_sample(job: &JobInfo, volumes: JobVolumeUsage) -> JobResourceSample {
    let wall = job
        .duration_ms
        .and_then(|millis| millis.checked_mul(1_000))
        .expect("an ended fixture job has a duration");
    let wall = WallMicros::new(wall).expect("fixture wall");
    JobResourceSample {
        job_id: job.job_id,
        sampled_at: timestamp(),
        wall_ms: wall.millis(),
        wall_us: wall,
        leader_pid: job.pid.expect("an ended fixture job had a process"),
        members: Vec::new(),
        host_start: HostLoadSample::new(2.0, 10).expect("fixture host"),
        host: HostLoadSample::new(2.5, 10).expect("fixture host"),
        rss_bytes: ResidentBytes::new(0).expect("fixture resident bytes"),
        rss_peak_bytes: ResidentBytes::new(12 << 20).expect("fixture resident bytes"),
        accounting: None,
        volumes,
        stdout: watermark(&job.stdout),
        stderr: watermark(&job.stderr),
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
        command: cowshed_core::api::ExecCommand::Argv(vec![CommandArg::from("true")]),
        cwd: None,
        started: timestamp(),
        duration_ms: None,
        resources: None,
        exit: None,
        stdout: inline_stream(""),
        stderr: inline_stream(""),
        trace: trace(),
        output_limit: None,
        stdin: empty_stdin(),
        failure: None,
    };

    // A running job with every optional present, its latest resource sample among them, a
    // workspace-file stdin, and both stream storage variants that name a workspace path.
    let running = JobInfo {
        repo_id: repo_id(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(2).expect("fixture job id"),
        state: JobState::Running,
        pid: Some(4242),
        grant_revision: 7,
        command: cowshed_core::api::ExecCommand::Argv(vec![
            CommandArg::from("cargo"),
            CommandArg::from("test"),
            CommandArg::from("--workspace"),
        ]),
        cwd: Some(workspace_path("packages/cowshed")),
        started: timestamp(),
        duration_ms: None,
        resources: Some(JobResourceSample {
            job_id: JobId::new(2).expect("fixture job id"),
            sampled_at: timestamp(),
            wall_ms: WallMicros::new(1_250_400).expect("fixture wall").millis(),
            wall_us: WallMicros::new(1_250_400).expect("fixture wall"),
            leader_pid: 4242,
            members: vec![4242, 4243],
            host_start: HostLoadSample::new(1.5, 10).expect("fixture host"),
            host: HostLoadSample::new(6.25, 10).expect("fixture host"),
            rss_bytes: ResidentBytes::new(48 << 20).expect("fixture resident bytes"),
            rss_peak_bytes: ResidentBytes::new(96 << 20).expect("fixture resident bytes"),
            // The macOS source: its CPU in named microseconds, its byte totals unavailable.
            accounting: Some(JobAccounting::MacOsRusageChildren {
                cpu: CpuTotals {
                    user_us: CpuMicros::new(2_180_500).expect("fixture cpu"),
                    sys_us: CpuMicros::new(310_250).expect("fixture cpu"),
                },
                io: None,
            }),
            volumes: JobVolumeUsage {
                workspace: VolumeUsage::Read {
                    delta_bytes: VolumeUsedBytesDelta::new(64 << 20).expect("fixture delta"),
                },
                build: Some(VolumeUsage::Unavailable {
                    reason: VolumeUnavailable::Failed {
                        message: "volume used-byte stat failed: no volume is mounted at the build \
                                  mount"
                            .into(),
                    },
                }),
            },
            stdout: JobStreamWatermark {
                bytes: StreamBytes::new(4096).expect("fixture stream bytes"),
                lines: StreamLines::new(64).expect("fixture stream lines"),
            },
            stderr: JobStreamWatermark {
                bytes: StreamBytes::new(128).expect("fixture stream bytes"),
                lines: StreamLines::new(3).expect("fixture stream lines"),
            },
        }),
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
        failure: None,
    };

    // An exited job with inline stdin.
    let exited = JobInfo {
        repo_id: repo_id(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(3).expect("fixture job id"),
        state: JobState::Exited,
        pid: Some(4243),
        grant_revision: 7,
        command: cowshed_core::api::ExecCommand::Argv(vec![
            CommandArg::from("sh"),
            CommandArg::from("-c"),
        ]),
        cwd: None,
        started: timestamp(),
        duration_ms: Some(1_234),
        resources: None,
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
        failure: None,
    };
    // A supervisor configured with no volume sampled it: its volumes say so.
    let exited = JobInfo {
        resources: Some(ended_sample(
            &exited,
            JobVolumeUsage {
                workspace: VolumeUsage::Unavailable {
                    reason: VolumeUnavailable::Unconfigured,
                },
                build: None,
            },
        )),
        ..exited
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
        command: cowshed_core::api::ExecCommand::Argv(vec![
            CommandArg::from("printf"),
            CommandArg::from("%s"),
            CommandArg::new(OsString::from_vec(vec![0xff, 0xfe, 0x80])),
        ]),
        cwd: None,
        started: timestamp(),
        duration_ms: Some(9),
        resources: None,
        exit: Some(ExitStatus::Signaled {
            signal: 9,
            core_dumped: true,
        }),
        stdout: inline_stream(""),
        stderr: inline_stream("killed\n"),
        trace: trace(),
        output_limit: None,
        stdin: empty_stdin(),
        failure: None,
    };
    // A platform with no used-bytes stat for the workspace volume, and a build volume that shrank.
    let signaled = JobInfo {
        resources: Some(ended_sample(
            &signaled,
            JobVolumeUsage {
                workspace: VolumeUsage::Unavailable {
                    reason: VolumeUnavailable::UnsupportedPlatform,
                },
                build: Some(VolumeUsage::Read {
                    delta_bytes: VolumeUsedBytesDelta::new(-(16 << 20)).expect("fixture delta"),
                }),
            },
        )),
        ..signaled
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
        command: cowshed_core::api::ExecCommand::Argv(vec![CommandArg::from("yes")]),
        cwd: None,
        started: timestamp(),
        duration_ms: Some(50),
        resources: None,
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
        failure: None,
    };

    // A cancelled job may die from a signal or exit normally after handling it.
    let killed = JobInfo {
        job_id: JobId::new(6).expect("fixture job id"),
        state: JobState::Killed,
        resources: None,
        exit: Some(ExitStatus::Signaled {
            signal: 15,
            core_dumped: false,
        }),
        stdout: inline_stream(""),
        stderr: inline_stream(""),
        ..signaled.clone()
    };
    let gracefully_killed = JobInfo {
        exit: Some(ExitStatus::Exited { code: 143 }),
        ..killed.clone()
    };

    let failed = JobInfo {
        job_id: JobId::new(7).expect("fixture job id"),
        state: JobState::Failed,
        exit: None,
        pid: None,
        resources: None,
        stdout: inline_stream(""),
        stderr: inline_stream("spawn refused\n"),
        ..exited.clone()
    };

    // A script job is the other command variant; one that did not parse is the only failure that
    // names its cause and carries an exit status.
    let script_syntax = JobInfo {
        job_id: JobId::new(8).expect("fixture job id"),
        state: JobState::Failed,
        command: cowshed_core::api::ExecCommand::Script(
            cowshed_core::api::ScriptCommand::new(
                vec!["grep -r ".into(), " src | (".into(), String::new()],
                vec![
                    cowshed_core::api::ScriptValue::Word("needle with spaces".into()),
                    cowshed_core::api::ScriptValue::Words(vec!["a".into(), "b c".into()]),
                ],
            )
            .expect("fixture script"),
        ),
        exit: Some(ExitStatus::Exited { code: 2 }),
        pid: None,
        resources: None,
        stdout: inline_stream(""),
        stderr: inline_stream("syntax error: unterminated subshell\n"),
        failure: Some(cowshed_core::api::JobFailure::ScriptSyntax),
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
        (
            "gracefullyKilled",
            document("job status", &gracefully_killed),
        ),
        ("failed", document("job status", &failed)),
        ("scriptSyntax", document("job status", &script_syntax)),
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
        port_block: Some(PortBlock::new(40_960, 128).expect("fixture port block is aligned")),
        // The 64-port block it was relocated from: still reserved to this workspace until it
        // retires, because a background process a finished job left behind may still hold it.
        retained_port_blocks: vec![
            PortBlock::new(41_088, 64).expect("fixture retained port block is aligned"),
        ],
        read: vec![PathBuf::from("/Users/fixture/.cargo/registry")],
        write: vec![PathBuf::from("/Users/fixture/Library/Caches/sccache")],
        deny_write: vec![PathBuf::from(".git/hooks"), PathBuf::from(".git/config")],
        deny: vec![PathBuf::from(".runtime")],
        egress: vec![
            EgressRule {
                host: "crates.io".to_owned(),
                ports: Vec::new(),
                mode: EgressMode::Intercept,
            },
            EgressRule {
                host: "github.com".to_owned(),
                ports: vec![22, 443],
                mode: EgressMode::Opaque,
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
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seeded: false,
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::NoLandingVolume,
            },
        },
    };
    let land_retired = LandReport {
        previous_target_head: Some(oid("2222222222222222222222222222222222222222")),
        target_was_checked_out: true,
        retired: true,
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seeded: true,
            adoption: cowshed_core::api::dto::Adoption::Adopted {
                elapsed_ms: 3,
                carried: cowshed_core::api::dto::NxCarry {
                    entries: 7,
                    bytes: 4096,
                    elapsed_ms: 12,
                    stopped: Some("stage entry 42: No space left on device".to_owned()),
                },
                check: cowshed_core::api::dto::AdoptionCheck {
                    hits: 41,
                    misses: vec![cowshed_core::api::dto::CacheMiss {
                        task: "widget:build".to_owned(),
                        hash: "1234567890".to_owned(),
                        inputs: [("files".to_owned(), vec!["src/a.ts".to_owned()])]
                            .into_iter()
                            .collect(),
                        inputs_digest: Sha256Digest::compute(b"inputs"),
                        inputs_error: None,
                    }],
                    unattributed: vec![cowshed_core::api::dto::UnattributedCheck {
                        check: "cargo test".to_owned(),
                        cache: ".nx/cache".to_owned(),
                        reason: cowshed_core::api::dto::UnattributedRun::BeganBeforeCheck {
                            start: "2026-10-05T00:00:00.000Z".to_owned(),
                        },
                    }],
                    failed: vec![cowshed_core::api::dto::FailedCheck {
                        check: "bun nx run-many -t lint".to_owned(),
                        exit: Some(1),
                    }],
                },
            },
        },
        ..land_first.clone()
    };
    let land_held = LandReport {
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seeded: true,
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::TargetHeld {
                    database: PathBuf::from("/Users/fixture/Dev/widget/.nx/workspace-data/A-v3.db"),
                    holders: vec![cowshed_core::api::dto::DatabaseHolder {
                        pid: 4242,
                        command: "node nx run-many -t build".to_owned(),
                    }],
                },
            },
        },
        ..land_first.clone()
    };
    let land_opening = LandReport {
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seeded: true,
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::TargetOpening {
                    database: PathBuf::from("/Users/fixture/Dev/widget/.nx/workspace-data/A-v3.db"),
                    holders: vec![cowshed_core::api::dto::DatabaseHolder {
                        pid: 4343,
                        command: "node nx build widget".to_owned(),
                    }],
                },
            },
        },
        ..land_first.clone()
    };
    let land_building = LandReport {
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seeded: false,
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::LandingBuilding {
                    lock: PathBuf::from("/Users/fixture/Dev/widget/target/guard/.cargo-lock"),
                    holders: vec![cowshed_core::api::dto::DatabaseHolder {
                        pid: 4747,
                        command: "cargo build --profile guard".to_owned(),
                    }],
                },
            },
        },
        ..land_first.clone()
    };
    let reseed_busy = cowshed_core::api::dto::ReseedResult {
        workspace: WorkspaceName::main(),
        outcome: cowshed_core::api::dto::Reseed::Skipped {
            behind_ms: Some(1200),
            reason: cowshed_core::api::dto::ReseedSkip::VolumeBusy {
                reason: "the source is held by pid 4848 (rust-analyzer); nothing was forced"
                    .to_owned(),
            },
        },
    };
    // One document per `RebaseCarrySkip` arm and per side: an arm absent here is one the
    // TypeScript union is not checked against.
    let rebase_carried = RebaseReport {
        oid: oid("7777777777777777777777777777777777777777"),
        build_volume: RebaseBuildVolume::Carried {
            carried: cowshed_core::api::dto::NxCarry {
                entries: 12,
                bytes: 8192,
                elapsed_ms: 40,
                stopped: None,
            },
        },
    };
    let rebase_skipped = |reason| RebaseReport {
        build_volume: RebaseBuildVolume::Skipped { reason },
        ..rebase_carried.clone()
    };
    let holder = |pid, command: &str| cowshed_core::api::dto::DatabaseHolder {
        pid,
        command: command.to_owned(),
    };
    let database = || PathBuf::from("/Users/fixture/Dev/widget/.nx/workspace-data/A-v3.db");

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
        examined: 7,
        reclaimed: 0,
        retained_pinned: 1,
        retained_active: 1,
        freed_bytes: 0,
        dry_run: true,
        candidates: [
            GcReason::RetiredWorkspace,
            GcReason::OrphanStagingImage,
            GcReason::OrphanStagingMetadata,
            GcReason::OrphanStagingMount,
            GcReason::OrphanMountpoint,
            GcReason::ExpiredCheckpoint,
            GcReason::OrphanSessionImage,
            GcReason::UnlinkedBuildVolume,
            GcReason::SupersededSeed,
            GcReason::UnrecordedBuildVolume,
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
        deferred: Vec::new(),
    };
    let gc_swept = GcReport {
        examined: 7,
        reclaimed: 5,
        retained_pinned: 1,
        retained_active: 0,
        freed_bytes: 21_504,
        dry_run: false,
        candidates: Vec::new(),
        deferred: vec![GcDeferred {
            path: PathBuf::from("/Users/fixture/Dev/.cowshed/gc/6"),
            diagnostic: "the image could not be detached before the deadline".to_owned(),
        }],
    };

    let doctor_healthy = DoctorReport {
        healthy: true,
        findings: Vec::new(),
    };
    let doctor_unhealthy = DoctorReport {
        healthy: false,
        findings: vec![
            Finding {
                code: "no-adopted-checkout".to_owned(),
                severity: FindingSeverity::Info,
                message: "fixture/codebase: recorded in the store but has no adopted checkout path, so the project inventory skips it".to_owned(),
                hint: String::new(),
                path: Some(PathBuf::from("/private/cowshed/store/fixture/codebase")),
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
        volume: ResizeVolume::Workspace,
        previous_capacity: "100g".to_owned(),
        capacity: "200g".to_owned(),
    };
    let resize_build = ResizeResult {
        workspace: WorkspaceName::main(),
        volume: ResizeVolume::Build,
        previous_capacity: "100g".to_owned(),
        capacity: "300g".to_owned(),
    };

    BTreeMap::from([
        (
            "LandReport",
            BTreeMap::from([
                ("firstLanding", document("land report", &land_first)),
                ("retired", document("land report", &land_retired)),
                ("adoptionSkipped", document("land report", &land_held)),
                ("landingBuilding", document("land report", &land_building)),
                (
                    "adoptionSkippedOpening",
                    document("land report", &land_opening),
                ),
            ]),
        ),
        (
            "ReseedResult",
            BTreeMap::from([("volumeBusy", document("reseed result", &reseed_busy))]),
        ),
        (
            "RebaseReport",
            BTreeMap::from([
                ("carried", document("rebase report", &rebase_carried)),
                (
                    "noWorkspaceVolume",
                    document(
                        "rebase report",
                        &rebase_skipped(RebaseCarrySkip::NoWorkspaceVolume),
                    ),
                ),
                (
                    "noTargetVolume",
                    document(
                        "rebase report",
                        &rebase_skipped(RebaseCarrySkip::NoTargetVolume),
                    ),
                ),
                (
                    "workspaceHeld",
                    document(
                        "rebase report",
                        &rebase_skipped(RebaseCarrySkip::Held {
                            side: CarrySide::Workspace,
                            database: database(),
                            holders: vec![holder(4444, "node nx run-many -t test")],
                        }),
                    ),
                ),
                (
                    "targetOpening",
                    document(
                        "rebase report",
                        &rebase_skipped(RebaseCarrySkip::Opening {
                            side: CarrySide::Target,
                            database: database(),
                            holders: vec![holder(4545, "node nx build widget")],
                        }),
                    ),
                ),
                (
                    "daemonStayed",
                    document(
                        "rebase report",
                        &rebase_skipped(RebaseCarrySkip::DaemonStayed {
                            side: CarrySide::Target,
                            daemon: holder(4646, "node nx daemon"),
                        }),
                    ),
                ),
                (
                    "taskHashes",
                    document(
                        "rebase report",
                        &rebase_skipped(RebaseCarrySkip::TaskHashes {
                            state: PathBuf::from("apps/web/.nx"),
                            reason: "node - /Users/fixture/Dev/widget/apps/web ended Exited { code: 1 }: Error: the task graph has a circular dependency".to_owned(),
                        }),
                    ),
                ),
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
            BTreeMap::from([
                ("grown", document("resize result", &resize)),
                ("buildGrown", document("resize result", &resize_build)),
            ]),
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
        // Read at runtime, not through `env!`. The macro bakes the manifest path into the crate's
        // output, which the patched sccache then keeps for that checkout alone, so this crate and
        // everything downstream would miss the compile cache at every new workspace mount path.
        // Cargo exports CARGO_MANIFEST_DIR to the test process, which is the only place
        // this regeneration branch runs.
        let manifest_directory = std::env::var("CARGO_MANIFEST_DIR")
            .expect("cargo exports CARGO_MANIFEST_DIR to the test process");
        let path = PathBuf::from(manifest_directory).join("../../src/wire-fixtures.json");
        std::fs::write(&path, &rendered).expect("the corpus path is writable");
        // The `nx run cowshed:wire-fixtures` leg: GOLDEN is the PREVIOUS file, compiled in, and
        // comparing against it here would fail the very run that brings it current. The next
        // compile includes the written file and the assertion below proves it then.
        return;
    }

    // Compare parsed values, not bytes. The committed file is formatted by biome, which disagrees
    // with serde_json's pretty printer about short arrays; byte-comparing it would force one
    // formatter to yield and neither does. The contract being pinned here is the DOCUMENT — every
    // key, every value, every shape core serializes — and that survives either layout.
    let committed: Value = serde_json::from_str(GOLDEN).expect("the committed corpus is JSON");
    assert_eq!(
        actual, committed,
        "{GOLDEN_PATH} is not what cowshed-core serializes today: a DTO on the napi seam changed \
         shape and the corpus was not regenerated. `nx run cowshed:wire-fixtures` (a build input \
         of every cargo test target) writes it; if this fires, that target's inputs miss the file \
         you changed. Then `nx run cowshed:wire-test` tells whether packages/cowshed/src/types.ts \
         must move with it."
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

/// Each volume answers for itself, read or why not: every `VolumeUsage` and `VolumeUnavailable`
/// variant, and a sample without a build volume, reach the corpus, so `types.ts` is verified
/// against each.
#[test]
fn every_volume_usage_appears_in_the_corpus() {
    let samples: Vec<Value> = job_infos()
        .into_values()
        .filter_map(|value| value.get("resources").cloned())
        .collect();
    let volumes: Vec<&Value> = samples
        .iter()
        .flat_map(|sample| ["workspace", "build"].map(|volume| &sample["volumes"][volume]))
        .collect();
    let kinds = |usage: &Value| {
        (
            usage["kind"].as_str().map(str::to_owned),
            usage["reason"]["kind"].as_str().map(str::to_owned),
        )
    };
    for (kind, reason) in [
        ("read", None),
        ("unavailable", Some("unconfigured")),
        ("unavailable", Some("unsupportedPlatform")),
        ("unavailable", Some("failed")),
    ] {
        let wanted = (Some(kind.to_owned()), reason.map(str::to_owned));
        assert!(
            volumes.iter().any(|usage| kinds(usage) == wanted),
            "no volume in the corpus is {kind} {reason:?}"
        );
    }
    assert!(
        samples
            .iter()
            .any(|sample| sample["volumes"].get("build").is_none()),
        "a sample without a build volume omits it"
    );
}

/// An ended job's sample in the committed corpus is that job's own: its leader is the job's
/// process, its wall time the job's duration and each stream's sampled bytes the job's output, so
/// a regenerated corpus cannot bless a terminal sample no job could have produced.
#[test]
fn each_ended_jobs_sample_in_the_corpus_is_its_own() {
    let corpus: Value = serde_json::from_str(GOLDEN).expect("the committed corpus is JSON");
    let jobs = corpus["JobInfo"]
        .as_object()
        .expect("the corpus holds job documents");
    let ended: Vec<(&String, &Value)> = jobs
        .iter()
        .filter(|(_, job)| !job["resources"].is_null() && !job["durationMs"].is_null())
        .collect();
    assert!(
        ended.len() >= 2,
        "the corpus carries ended jobs' samples: {ended:?}"
    );
    for (name, job) in ended {
        let sample = &job["resources"];
        assert_eq!(sample["jobId"], job["jobId"], "{name}: job id");
        assert_eq!(sample["leaderPid"], job["pid"], "{name}: leader");
        assert_eq!(sample["wallMs"], job["durationMs"], "{name}: wall time");
        for stream in ["stdout", "stderr"] {
            assert_eq!(
                sample[stream]["bytes"], job[stream]["bytes"],
                "{name}: {stream} bytes"
            );
        }
    }
}
