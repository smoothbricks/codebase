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
        AbandonedPreservation, AbandonedWork, BinaryData, BranchLandings, CheckpointInfo,
        CommandArg, CpuMicros, CpuMicrosDelta, CpuTotals, DoctorReport, EgressMode, EgressRule,
        ExecClass, ExecScope, ExitStatus, Finding, FindingSeverity, GcCandidate, GcDeferred,
        GcReason, GcReport, GitOid, GrantSet, HostLoadSample, JobAccounting, JobId, JobInfo,
        JobLeafAttribution, JobProcessLeaf, JobResourceSample, JobState, JobStreamWatermark,
        JobVolumeUsage, LandReport, LandingCommits, OneCoreCpuPercent, OneCoreCpuPermille,
        OutputLimitInfo, OutputStorage, OutputSummary, PortBlock, PreservedRef, ProcessCoverage,
        ProcessCoverageGap, ProtectedOutput, PushReport, RebaseReport, RemoveReport, RepoRule,
        ResidentBytes, ResizeResult, ResizeVolume, Sha256Digest, SimVerb, SpanId, StdinInfo,
        StdinKind, StreamBytes, StreamInfo, StreamLines, TraceContext, TraceId, UnlandedChange,
        UnlandedPath, UnlandedPaths, UnprovenWorkspace, UsageReconciliation, UtcTimestamp,
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
    let leader_pid = job.pid.expect("an ended fixture job had a process");
    let cpu = CpuTotals::ZERO;
    JobResourceSample {
        job_id: job.job_id,
        sampled_at: timestamp(),
        wall_ms: wall.millis(),
        wall_us: wall,
        leader_pid,
        members: Vec::new(),
        leaf: JobLeafAttribution::NoAccountedCpu,
        cpu_user_ms: cpu.user_us.millis(),
        cpu_sys_ms: cpu.sys_us.millis(),
        cpu_pct: OneCoreCpuPercent::ZERO,
        host_start: HostLoadSample::new(2.0, 10).expect("fixture host"),
        host: HostLoadSample::new(2.5, 10).expect("fixture host"),
        rss_bytes: ResidentBytes::new(0).expect("fixture resident bytes"),
        rss_peak_bytes: ResidentBytes::new(12 << 20).expect("fixture resident bytes"),
        accounting: JobAccounting::MacOsRusageChildren {
            cpu,
            io: None,
            reconciliation: UsageReconciliation {
                cpu_us: CpuMicrosDelta::new(0).expect("fixture cpu delta"),
                io_read_bytes: None,
                io_write_bytes: None,
                coverage: ProcessCoverage::Gap {
                    reason: ProcessCoverageGap::UncountedFork { pid: leader_pid },
                },
            },
        },
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
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: None,
        containment: None,
        outlived: Vec::new(),
        outlived_incomplete: false,
    };

    let running_cpu = CpuTotals {
        user_us: CpuMicros::new(2_180_500).expect("fixture cpu"),
        sys_us: CpuMicros::new(310_250).expect("fixture cpu"),
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
            leaf: JobLeafAttribution::Attributed {
                leaf: JobProcessLeaf {
                    pid: 4242,
                    program: "cargo".into(),
                    argv: vec![CommandArg::from("cargo")],
                    truncated: false,
                },
                observed_cpu_permille: 950,
            },
            cpu_user_ms: running_cpu.user_us.millis(),
            cpu_sys_ms: running_cpu.sys_us.millis(),
            cpu_pct: OneCoreCpuPercent::new(87.5).expect("fixture cpu percent"),
            host_start: HostLoadSample::new(1.5, 10).expect("fixture host"),
            host: HostLoadSample::new(6.25, 10).expect("fixture host"),
            rss_bytes: ResidentBytes::new(48 << 20).expect("fixture resident bytes"),
            rss_peak_bytes: ResidentBytes::new(96 << 20).expect("fixture resident bytes"),
            // The macOS source: its CPU in named microseconds, its byte totals unavailable.
            accounting: JobAccounting::MacOsRusageChildren {
                cpu: running_cpu,
                io: None,
                reconciliation: UsageReconciliation {
                    cpu_us: CpuMicrosDelta::new(-12_500).expect("fixture cpu delta"),
                    io_read_bytes: None,
                    io_write_bytes: None,
                    coverage: ProcessCoverage::Gap {
                        reason: ProcessCoverageGap::UncountedFork { pid: 4242 },
                    },
                },
            },
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
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: Some(10),
        containment: None,
        outlived: Vec::new(),
        outlived_incomplete: false,
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
        class: ExecClass::Gate,
        scope: ExecScope::Workspace,
        nice: Some(5),
        containment: None,
        outlived: Vec::new(),
        outlived_incomplete: false,
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
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: Some(10),
        containment: None,
        outlived: Vec::new(),
        outlived_incomplete: false,
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
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: Some(10),
        containment: None,
        outlived: Vec::new(),
        outlived_incomplete: false,
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
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: None,
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
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: None,
        ..exited.clone()
    };

    // A job whose admission failed before its command ran: its jobserver fifo could not be made.
    // It was never observed, and it is the one job that ran as the host gate.
    let cpu_admission_failed = JobInfo {
        job_id: JobId::new(9).expect("fixture job id"),
        state: JobState::Failed,
        exit: None,
        pid: None,
        resources: None,
        stdout: inline_stream(""),
        stderr: inline_stream(
            "cowshed: the job's jobserver fifo could not be made: No space left on device\n",
        ),
        failure: Some(cowshed_core::api::JobFailure::CpuAdmission),
        class: ExecClass::Gate,
        scope: ExecScope::HostGate,
        nice: None,
        ..exited.clone()
    };

    // A job whose owned child could not be placed at its class nice: the child was killed before
    // its command ran, so no nice was observed.
    let priority_failed = JobInfo {
        job_id: JobId::new(10).expect("fixture job id"),
        state: JobState::Failed,
        exit: None,
        pid: Some(4246),
        resources: None,
        stdout: inline_stream(""),
        stderr: inline_stream("cannot place pid 4246 at nice 10\n"),
        failure: Some(cowshed_core::api::JobFailure::Priority),
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: None,
        ..exited.clone()
    };

    // A job whose command was never launched: its program was proven absent before the spawn,
    // so it failed with the not-found wrapper exit, owned no process and was never observed.
    let unlaunched = JobInfo {
        job_id: JobId::new(30).expect("fixture job id"),
        state: JobState::Failed,
        exit: Some(ExitStatus::Exited { code: 102 }),
        pid: None,
        resources: None,
        stdout: inline_stream(""),
        stderr: inline_stream(
            "cowshed: tooling/missing does not exist, so the command did not start\n",
        ),
        failure: Some(cowshed_core::api::JobFailure::Unlaunched),
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: None,
        ..exited.clone()
    };

    // A job its requester waits on: it ran at the supervisor's own nice, above a gate
    // (05_gateway.md "Classes").
    let interactive = JobInfo {
        job_id: JobId::new(11).expect("fixture job id"),
        resources: None,
        class: ExecClass::Interactive,
        scope: ExecScope::Workspace,
        nice: Some(0),
        ..exited.clone()
    };

    // A resident that served its requests and ended: its observed nice stands alone.
    let resident = JobInfo {
        job_id: JobId::new(12).expect("fixture job id"),
        resources: None,
        class: ExecClass::Resident,
        scope: ExecScope::Workspace,
        nice: Some(10),
        ..exited.clone()
    };

    // The same resident at interactive nice: it differs only in the nice it was observed at
    // (05_gateway.md "Classes").
    let interactive_resident = JobInfo {
        job_id: JobId::new(13).expect("fixture job id"),
        class: ExecClass::InteractiveResident,
        nice: Some(0),
        ..resident.clone()
    };

    // A job whose leader has ended while the rest of its process membership is being retired:
    // nonterminal, so it owns its process and states no exit, duration or receipt yet.
    let draining = JobInfo {
        job_id: JobId::new(14).expect("fixture job id"),
        state: JobState::Draining,
        pid: Some(4249),
        grant_revision: 7,
        ..queued.clone()
    };

    // The kernel retired the membership and nothing outlived the leader: a containment, and no
    // survivor list on the wire.
    let retired_kernel = JobInfo {
        job_id: JobId::new(15).expect("fixture job id"),
        resources: None,
        containment: Some(cowshed_core::api::JobContainment::Kernel),
        ..exited.clone()
    };

    // An observer's retirement names what it lacked and each process it ended, one whose position
    // and image it knew and one whose it did not.
    let retired_observed = JobInfo {
        job_id: JobId::new(16).expect("fixture job id"),
        resources: None,
        containment: Some(cowshed_core::api::JobContainment::Observed {
            lost: vec![ProcessCoverageGap::UnreadImage { pid: 4247 }],
        }),
        outlived: vec![
            cowshed_core::api::OutlivedProcess {
                index: Some(2),
                pid: 4247,
                image: Some("/bin/sleep".to_owned()),
                signal: 9,
            },
            cowshed_core::api::OutlivedProcess {
                index: None,
                pid: 4248,
                image: None,
                signal: 9,
            },
        ],
        ..signaled.clone()
    };

    // Empty membership can be proven even when native retirement could not name all members it ended.
    let retired_kernel_incomplete = JobInfo {
        job_id: JobId::new(17).expect("fixture job id"),
        outlived_incomplete: true,
        ..retired_kernel.clone()
    };
    let retired_kernel_incomplete_with_rows = JobInfo {
        job_id: JobId::new(18).expect("fixture job id"),
        containment: Some(cowshed_core::api::JobContainment::Kernel),
        outlived_incomplete: true,
        ..retired_observed.clone()
    };

    // A background job ended at critical host memory pressure, as a cancel ends one: it failed for
    // memory, and its stderr names the pressure level and its resident sum (05_gateway.md "Memory
    // pressure").
    let memory_ended = JobInfo {
        job_id: JobId::new(19).expect("fixture job id"),
        state: JobState::Failed,
        resources: None,
        exit: Some(ExitStatus::Signaled {
            signal: 15,
            core_dumped: false,
        }),
        stdout: inline_stream(""),
        stderr: inline_stream(
            "cowshed: ended at critical memory pressure with 6442450944 resident bytes\n",
        ),
        failure: Some(cowshed_core::api::JobFailure::Memory),
        ..signaled.clone()
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
        (
            "cpuAdmissionFailed",
            document("job status", &cpu_admission_failed),
        ),
        ("priorityFailed", document("job status", &priority_failed)),
        ("memoryEnded", document("job status", &memory_ended)),
        ("unlaunched", document("job status", &unlaunched)),
        ("interactive", document("job status", &interactive)),
        ("resident", document("job status", &resident)),
        (
            "interactiveResident",
            document("job status", &interactive_resident),
        ),
        ("draining", document("job status", &draining)),
        ("retiredKernel", document("job status", &retired_kernel)),
        ("retiredObserved", document("job status", &retired_observed)),
        (
            "retiredKernelIncomplete",
            document("job status", &retired_kernel_incomplete),
        ),
        (
            "retiredKernelIncompleteWithRows",
            document("job status", &retired_kernel_incomplete_with_rows),
        ),
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
                unlanded_paths: UnlandedPaths::Named {
                    paths: vec![
                        UnlandedPath {
                            path: "src/gate.rs".to_owned(),
                            change: UnlandedChange::Missing,
                        },
                        UnlandedPath {
                            path: "assets/logo.png".to_owned(),
                            change: UnlandedChange::Unmergeable {
                                reason: "Cannot merge binary files".to_owned(),
                            },
                        },
                    ],
                },
                preserved_by: None,
            },
            branches: BranchLandings::Measured {
                unlanded: Vec::new(),
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
            branches: BranchLandings::Indeterminate {
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

/// A workspace a listing names but gives no row: its mount does not prove itself the workspace's.
fn unproven_workspaces() -> BTreeMap<&'static str, Value> {
    let unreadable = UnprovenWorkspace {
        workspace: workspace_name("cs-detaching"),
        workspace_incarnation: incarnation(),
        mount: PathBuf::from("/Users/fixture/.cowshed/mnt/smoothbricks/codebase/cs-detaching"),
        reason: "workspace mount identity mismatch at /Users/fixture/.cowshed/mnt/smoothbricks/codebase/cs-detaching: the volume mounted from /dev/disk41s1 carries no readable smoothbricks/codebase/cs-detaching marker for incarnation 0123456789abcdef0123456789abcdef: No such file or directory (os error 2)".to_owned(),
    };
    let list = vec![unreadable.clone()];
    BTreeMap::from([
        (
            "markerUnreadable",
            document("unproven workspace", &unreadable),
        ),
        ("list", document("unproven workspace list", &list)),
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
        loopback_any: true,
    };

    BTreeMap::from([
        ("closed", document("workspace grants", &closed)),
        ("open", document("workspace grants", &open)),
    ])
}

fn reports() -> BTreeMap<&'static str, BTreeMap<&'static str, Value>> {
    let land_first = cowshed_core::api::dto::LandedReport {
        landed_head: oid("1111111111111111111111111111111111111111"),
        target_branch: "main".to_owned(),
        previous_target_head: None,
        target_was_checked_out: false,
        retired: false,
        folded: Vec::new(),
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::Unchanged,
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::NoLandingVolume,
            },
        },
    };
    let fold_retired = cowshed_core::api::dto::FoldedWorkspace {
        workspace: workspace_name("raven"),
        head: oid("3333333333333333333333333333333333333333"),
        outcome: cowshed_core::api::dto::FoldOutcome::Retired,
    };
    let fold_kept = cowshed_core::api::dto::FoldedWorkspace {
        workspace: workspace_name("heron"),
        head: oid("4444444444444444444444444444444444444444"),
        outcome: cowshed_core::api::dto::FoldOutcome::Kept {
            reason: cowshed_core::api::dto::FoldKept::Dirty { files: 2 },
        },
    };
    let fold_refused = cowshed_core::api::dto::FoldedWorkspace {
        workspace: workspace_name("ibis"),
        head: oid("9999999999999999999999999999999999999999"),
        outcome: cowshed_core::api::dto::FoldOutcome::Kept {
            reason: cowshed_core::api::dto::FoldKept::Refused {
                code: cowshed_core::error::ErrorCode::Conflict,
                message: "the workspace's head moved since the fold was planned".to_owned(),
            },
        },
    };
    let land_retired = cowshed_core::api::dto::LandedReport {
        previous_target_head: Some(oid("2222222222222222222222222222222222222222")),
        target_was_checked_out: true,
        retired: true,
        folded: vec![fold_retired, fold_kept, fold_refused],
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::SealBusy {
                reason: "build volume /cache/build/0123.dmg is in use by pid 5940 (workspace job); nothing was forced: close what holds it and run the command again".to_owned(),
            },
            adoption: cowshed_core::api::dto::Adoption::Adopted {
                elapsed_ms: 3,
                check: cowshed_core::api::dto::AdoptionCheck {
                    hits: 41,
                    misses: vec![cowshed_core::api::dto::CacheMiss {
                        task: "widget:build".to_owned(),
                        hash: "1234567890".to_owned(),
                        reason: "no successful local cache result; task was not executed".to_owned(),
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
                        error: "cache-only restoration helper exited 1: unreadable task graph".to_owned(),
                        exit: Some(1),
                    }],
                },
            },
        },
        ..land_first.clone()
    };
    let land_held = cowshed_core::api::dto::LandedReport {
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::Frozen {
                compaction: cowshed_core::api::dto::LandSeedCompaction::Started {
                    pid: 4243,
                    extents: 612_000,
                    log: PathBuf::from("/Users/fixture/.cache/cowshed/build/.compacting/log"),
                },
            },
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
        folded: Vec::new(),
        ..land_first.clone()
    };
    let land_opening = cowshed_core::api::dto::LandedReport {
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::Frozen {
                compaction: cowshed_core::api::dto::LandSeedCompaction::Held {
                    error: "build volume storage failed: the copy could not be written".to_owned(),
                    seconds_ago: 3600,
                },
            },
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
        folded: Vec::new(),
        ..land_first.clone()
    };
    let landing_opening = cowshed_core::api::dto::LandedReport {
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::Unchanged,
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::LandingOpening {
                    database: PathBuf::from("/build/nx/workspace-data/tasks.db"),
                    holders: vec![cowshed_core::api::dto::DatabaseHolder {
                        pid: 4443,
                        command: "node nx build widget".to_owned(),
                    }],
                },
            },
        },
        folded: Vec::new(),
        ..land_first.clone()
    };
    let land_building = cowshed_core::api::dto::LandedReport {
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::Unchanged,
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
        folded: Vec::new(),
        ..land_first.clone()
    };
    let land_unverifiable = cowshed_core::api::dto::LandedReport {
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::Unchanged,
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::LandingDaemonUnverifiable {
                    record: PathBuf::from(
                        "/fixture/Dev/widget/.nx/workspace-data/d/server-process.json",
                    ),
                    reason: "the daemon record is unreadable; no signal was sent".to_owned(),
                },
            },
        },
        folded: Vec::new(),
        ..land_first.clone()
    };
    let land_ungated = cowshed_core::api::dto::LandedReport {
        target_branch: "cowshed/lane".to_owned(),
        target_was_checked_out: true,
        retired: true,
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::Unchanged,
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::Ungated,
            },
        },
        folded: Vec::new(),
        ..land_first.clone()
    };
    let rebased = RebaseReport {
        oid: oid("7777777777777777777777777777777777777777"),
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
        examined: 7,
        reclaimed: 0,
        retained_pinned: 1,
        retained_active: 1,
        released_bytes: 0,
        dry_run: true,
        candidates: [
            GcReason::RetiredWorkspace,
            GcReason::OrphanStagingImage,
            GcReason::OrphanStagingMetadata,
            GcReason::OrphanStagingMount,
            GcReason::OrphanMountpoint,
            GcReason::ExpiredCheckpoint,
            GcReason::OrphanSessionImage,
            GcReason::AbandonedBundle,
            GcReason::UnlinkedBuildVolume,
            GcReason::SupersededSeed,
            GcReason::InterruptedBuildVolume,
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
        released_bytes: 21_504,
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
            branches: Vec::new(),
            preserved: AbandonedPreservation::BundleOnly,
            bundle: PathBuf::from("/Users/fixture/Dev/.cowshed/abandoned/cs-seam.bundle"),
        }),
    };
    let remove_preserved = RemoveReport {
        abandoned: Some(AbandonedWork {
            head: oid("5555555555555555555555555555555555555555"),
            target_branch: "main".to_owned(),
            target_head: Some(oid("6666666666666666666666666666666666666666")),
            unlanded_commits: 4,
            branches: Vec::new(),
            preserved: AbandonedPreservation::Preserved {
                refs: vec![PreservedRef {
                    name: "HEAD".to_owned(),
                    tip: oid("5555555555555555555555555555555555555555"),
                    reference: "refs/cowshed/cs-seam/abandoned/5555555555555555555555555555555555555555/HEAD".to_owned(),
                }],
            },
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
                (
                    "delivered",
                    document(
                        "land report",
                        &LandReport::Delivered(PushReport {
                            source_head: oid("1111111111111111111111111111111111111111"),
                            destination_ref: "refs/cowshed/raven/heads/topic".to_owned(),
                            previous_destination_head: None,
                        }),
                    ),
                ),
                (
                    "firstLanding",
                    document("land report", &LandReport::Landed(land_first)),
                ),
                (
                    "retired",
                    document("land report", &LandReport::Landed(land_retired)),
                ),
                (
                    "adoptionSkipped",
                    document("land report", &LandReport::Landed(land_held)),
                ),
                (
                    "landingBuilding",
                    document("land report", &LandReport::Landed(land_building)),
                ),
                (
                    "landingOpening",
                    document("land report", &LandReport::Landed(landing_opening)),
                ),
                (
                    "landingDaemonUnverifiable",
                    document("land report", &LandReport::Landed(land_unverifiable)),
                ),
                (
                    "adoptionSkippedOpening",
                    document("land report", &LandReport::Landed(land_opening)),
                ),
                (
                    "ungated",
                    document("land report", &LandReport::Landed(land_ungated)),
                ),
                (
                    "preflighted",
                    document(
                        "land report",
                        &LandReport::Preflighted(cowshed_core::api::LandPreflight {
                            workspace_incarnation: incarnation(),
                            source_head: oid("1111111111111111111111111111111111111111"),
                            target_branch: "main".to_owned(),
                            target_head: cowshed_core::api::ExpectedRefHead::Oid(oid(
                                "2222222222222222222222222222222222222222",
                            )),
                        }),
                    ),
                ),
            ]),
        ),
        (
            "RebaseReport",
            BTreeMap::from([("rebased", document("rebase report", &rebased))]),
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
                ("preserved", document("remove report", &remove_preserved)),
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

fn process_observations() -> BTreeMap<&'static str, BTreeMap<&'static str, Value>> {
    use cowshed_core::api::{
        StorageIoBytes,
        process::{
            BlockerChange, JobProcessDelta, JobProcessEvent, JobProcessSample, JobProcessTree,
            LockHolder, ProcessBlockedOn, ProcessCpuWindow, ProcessExit, ProcessIoUnavailable,
            ProcessStorageIo, ProcessUsage,
        },
    };

    let unread = JobProcessSample {
        pid: 4242,
        ppid: 4200,
        program: "cargo".into(),
        argv: vec![CommandArg::from("cargo")].into(),
        truncated: false,
        born_at: timestamp(),
        usage: None,
        blocked_on: None,
        exit: None,
    };
    let usage = ProcessUsage {
        cpu_user_us: CpuMicros::new(12_345).expect("fixture CPU"),
        cpu_sys_us: CpuMicros::new(678).expect("fixture CPU"),
        busy: true,
        cpu_window: Some(ProcessCpuWindow {
            opened_us: WallMicros::new(1_000_000).expect("fixture window"),
            closed_us: WallMicros::new(2_000_000).expect("fixture window"),
            cpu_permille: OneCoreCpuPermille::new(950).expect("fixture share"),
        }),
        rss_bytes: ResidentBytes::new(4096).expect("fixture resident bytes"),
        rss_peak_bytes: ResidentBytes::new(8192).expect("fixture resident bytes"),
        io: ProcessStorageIo::Read {
            read_bytes: StorageIoBytes::new(123).expect("fixture read bytes"),
            write_bytes: StorageIoBytes::new(456).expect("fixture write bytes"),
        },
    };
    let running = JobProcessSample {
        usage: Some(usage),
        blocked_on: Some(ProcessBlockedOn::None),
        ..unread.clone()
    };
    let ended = JobProcessSample {
        usage: Some(ProcessUsage {
            busy: false,
            cpu_window: Some(ProcessCpuWindow {
                opened_us: WallMicros::new(2_000_000).expect("fixture window"),
                closed_us: WallMicros::new(2_500_000).expect("fixture window"),
                cpu_permille: OneCoreCpuPermille::new(4).expect("fixture share"),
            }),
            rss_bytes: ResidentBytes::new(0).expect("exited resident bytes"),
            ..usage
        }),
        exit: Some(ProcessExit {
            status: Some(ExitStatus::Exited { code: 0 }),
            exited_at: timestamp(),
        }),
        ..unread.clone()
    };
    let tree = JobProcessTree {
        job_id: JobId::new(7).expect("fixture job ID"),
        sampled_at: timestamp(),
        processes: vec![unread.clone()],
        coverage: ProcessCoverage::Complete,
    };
    let mut trees = BTreeMap::from([
        ("unread", document("job processes", &tree)),
        (
            "running",
            document(
                "job processes",
                &JobProcessTree {
                    processes: vec![running.clone()],
                    ..tree.clone()
                },
            ),
        ),
        (
            "exited",
            document(
                "job processes",
                &JobProcessTree {
                    processes: vec![ended.clone()],
                    ..tree.clone()
                },
            ),
        ),
        (
            "exitStatusWithheld",
            document(
                "job processes",
                &JobProcessTree {
                    processes: vec![JobProcessSample {
                        exit: Some(ProcessExit {
                            status: None,
                            exited_at: timestamp(),
                        }),
                        ..ended.clone()
                    }],
                    coverage: ProcessCoverage::Gap {
                        reason: ProcessCoverageGap::UnreadExitStatus { pid: 4242 },
                    },
                    ..tree.clone()
                },
            ),
        ),
    ]);
    for (name, reason) in [
        ("ioNotPermitted", ProcessIoUnavailable::NotPermitted),
        ("ioNotAccounted", ProcessIoUnavailable::NotAccounted),
    ] {
        let process = JobProcessSample {
            argv: vec![CommandArg::from(OsString::from_vec(vec![0xff]))].into(),
            usage: Some(ProcessUsage {
                io: ProcessStorageIo::Unavailable { reason },
                ..ended.usage.expect("exited fixture usage")
            }),
            exit: Some(ProcessExit {
                status: Some(ExitStatus::Signaled {
                    signal: 9,
                    core_dumped: false,
                }),
                exited_at: timestamp(),
            }),
            ..ended.clone()
        };
        trees.insert(
            name,
            document(
                "job processes",
                &JobProcessTree {
                    processes: vec![process],
                    ..tree.clone()
                },
            ),
        );
    }
    let mut events = BTreeMap::new();
    for (name, event) in [
        (
            "born",
            JobProcessEvent::Born {
                index: 0,
                process: unread,
            },
        ),
        (
            "exec",
            JobProcessEvent::Exec {
                index: 0,
                process: running.clone(),
            },
        ),
        (
            "heartbeat",
            JobProcessEvent::Heartbeat {
                index: 0,
                process: running,
            },
        ),
        (
            "exited",
            JobProcessEvent::Exited {
                index: 0,
                process: ended,
            },
        ),
        (
            "usage",
            JobProcessEvent::Changed(JobProcessDelta::Usage { index: 0, usage }),
        ),
        (
            "blockerClear",
            JobProcessEvent::Changed(JobProcessDelta::Blocker {
                index: 0,
                blocked_on: BlockerChange::Clear,
            }),
        ),
    ] {
        events.insert(name, document("job process event", &event));
    }
    for (name, blocker) in [
        ("blockerNone", ProcessBlockedOn::None),
        ("blockerSocket", ProcessBlockedOn::Socket),
        ("blockerPipe", ProcessBlockedOn::Pipe),
        ("blockerChild", ProcessBlockedOn::Child),
        ("blockerStdin", ProcessBlockedOn::Stdin),
        ("blockerDisk", ProcessBlockedOn::Disk),
        (
            "lockUnknownHolder",
            ProcessBlockedOn::Lock {
                path: "target/.lock".into(),
                holder: None,
            },
        ),
        (
            "lockHolderPid",
            ProcessBlockedOn::Lock {
                path: "target/.lock".into(),
                holder: Some(LockHolder {
                    pid: 4300,
                    job: None,
                    program: None,
                }),
            },
        ),
        (
            "lockHolder",
            ProcessBlockedOn::Lock {
                path: "target/.lock".into(),
                holder: Some(LockHolder {
                    pid: 4300,
                    job: Some(tree.job_id),
                    program: Some("rustc".into()),
                }),
            },
        ),
    ] {
        events.insert(
            name,
            document(
                "job process event",
                &JobProcessEvent::Changed(JobProcessDelta::Blocker {
                    index: 0,
                    blocked_on: BlockerChange::Set(blocker),
                }),
            ),
        );
    }
    for (name, reason) in [
        ("eventsLost", ProcessCoverageGap::EventsLost),
        (
            "unobservedBirth",
            ProcessCoverageGap::UnobservedBirth { pid: 4243 },
        ),
        (
            "unobservedExit",
            ProcessCoverageGap::UnobservedExit { pid: 4243 },
        ),
        (
            "uncountedFork",
            ProcessCoverageGap::UncountedFork { pid: 4242 },
        ),
        ("unreadImage", ProcessCoverageGap::UnreadImage { pid: 4242 }),
        (
            "unreadFinalUsage",
            ProcessCoverageGap::UnreadFinalUsage { pid: 4242 },
        ),
        (
            "unreadExitStatus",
            ProcessCoverageGap::UnreadExitStatus { pid: 4242 },
        ),
    ] {
        trees.insert(
            name,
            document(
                "job processes",
                &JobProcessTree {
                    coverage: ProcessCoverage::Gap { reason },
                    ..tree.clone()
                },
            ),
        );
        events.insert(
            name,
            document("job process event", &JobProcessEvent::Gap { reason }),
        );
    }
    BTreeMap::from([("JobProcessTree", trees), ("JobProcessEvent", events)])
}

fn corpus() -> BTreeMap<&'static str, BTreeMap<&'static str, Value>> {
    let mut corpus = reports();
    corpus.extend(process_observations());
    corpus.insert("JobInfo", job_infos());
    corpus.insert("WorkspaceInfo", workspace_infos());
    corpus.insert("UnprovenWorkspace", unproven_workspaces());
    corpus.insert("GrantSet", grant_sets());
    corpus
}

#[test]
fn the_committed_wire_corpus_is_what_core_serializes() {
    let actual = serde_json::to_value(corpus()).expect("the corpus is JSON");
    let mut rendered =
        serde_json::to_string_pretty(&actual).expect("the corpus renders as pretty JSON");
    rendered.push('\n');

    if let Some(destination) = std::env::var_os(WRITE_ENV) {
        // `scripts/wire-fixtures.sh` names the destination: a staging file it formats and renames
        // over the committed corpus, so no reader sees this unformatted render.
        std::fs::write(&destination, &rendered).expect("the render destination is writable");
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
        JobState::Draining,
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

/// Every scheduling variant and optional observation a job can carry is in the corpus: a value
/// absent from it is one the TypeScript side is unverified against, and a zero nice is a real
/// observation, not an omission.
#[test]
fn every_scheduling_fact_appears_in_the_corpus() {
    let documents: Vec<Value> = job_infos()
        .values()
        .flat_map(|value| match value {
            Value::Array(items) => items.clone(),
            single => vec![single.clone()],
        })
        .collect();
    for (field, spellings) in [
        (
            "class",
            [
                "background",
                "interactive",
                "gate",
                "resident",
                "interactive-resident",
            ]
            .as_slice(),
        ),
        ("scope", ["workspace", "hostGate"].as_slice()),
        (
            "failure",
            ["scriptSyntax", "cpuAdmission", "priority", "memory"].as_slice(),
        ),
    ] {
        for spelling in spellings {
            assert!(
                documents
                    .iter()
                    .any(|document| document.get(field).and_then(Value::as_str) == Some(spelling)),
                "no job carries {field} {spelling:?}, so types.ts is unverified for it"
            );
        }
    }
    assert!(
        documents
            .iter()
            .any(|document| document.get("nice").is_some()),
        "no job carries nice"
    );
    assert!(
        documents
            .iter()
            .any(|document| document.get("nice").is_none()),
        "no job omits nice"
    );
    assert!(
        documents.iter().any(|document| document["nice"] == 0),
        "a gate's observed nice zero is a real observation"
    );
}

/// Every retirement fact a job can carry is in the corpus: both containments, a survivor whose
/// index and image were observed and one whose were not, and a job that states no receipt at all.
/// A value absent from it is one the TypeScript side is unverified against.
#[test]
fn every_retirement_fact_appears_in_the_corpus() {
    let documents: Vec<Value> = job_infos()
        .values()
        .flat_map(|value| match value {
            Value::Array(items) => items.clone(),
            single => vec![single.clone()],
        })
        .collect();
    for kind in ["kernel", "observed"] {
        assert!(
            documents
                .iter()
                .any(|document| document["containment"]["kind"] == kind),
            "no job states a {kind} containment, so types.ts is unverified for it"
        );
    }
    let survivors: Vec<&Value> = documents
        .iter()
        .flat_map(|document| document["outlived"].as_array().into_iter().flatten())
        .collect();
    for field in ["index", "image"] {
        assert!(
            survivors
                .iter()
                .any(|survivor| survivor.get(field).is_some()),
            "no survivor names its {field}"
        );
        assert!(
            survivors
                .iter()
                .any(|survivor| survivor.get(field).is_none()),
            "no survivor omits its {field}"
        );
    }
    assert!(
        documents.iter().any(|document| {
            document.get("containment").is_none() && document.get("outlived").is_none()
        }),
        "no job states no receipt"
    );
}
