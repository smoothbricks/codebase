use cowshed_core::api::*;
use cowshed_core::{CowshedError, ErrorCode};
use cowshed_os::test_backstop::CommandBackstop as _;
use serde_json::json;
use std::ffi::OsString;
use std::fs;
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::process::Command;

#[path = "support/temp_root.rs"]
mod temp_root;
use temp_root::TempRoot;

fn repo() -> cowshed_core::repository::RepoId {
    cowshed_core::repository::RepoId::parse("acme/widget").expect("repo id")
}

fn workspace() -> WorkspaceName {
    WorkspaceName::new("raven").expect("workspace")
}

fn incarnation() -> WorkspaceIncarnation {
    WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").expect("incarnation")
}

fn oid(digit: char) -> GitOid {
    GitOid::new(digit.to_string().repeat(40)).expect("oid")
}

fn timestamp() -> UtcTimestamp {
    UtcTimestamp::new("2026-07-11T12:34:56Z").expect("timestamp")
}

fn trace() -> TraceContext {
    TraceContext {
        trace_id: TraceId::new("4bf92f3577b34da6a3ce929d0e0e4736").expect("trace id"),
        span_id: SpanId::new("00f067aa0ba902b7").expect("span id"),
    }
}

fn stream(path: &str, bytes: u64, text: &str) -> StreamInfo {
    StreamInfo {
        storage: OutputStorage::Captured {
            artifact: ProtectedOutput::File {
                path: WorkspacePath::new(path).expect("workspace path"),
            },
        },
        bytes,
        sha256: Sha256Digest::from_bytes([0; 32]),
        summary: OutputSummary {
            version: 1,
            text: text.into(),
            truncated: false,
        },
    }
}

fn finished_job(cwd: Option<WorkspacePath>) -> JobInfo {
    JobInfo {
        repo_id: repo(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(7).unwrap(),
        state: JobState::Signaled,
        pid: Some(4242),
        grant_revision: 9,
        command: cowshed_core::api::ExecCommand::Argv(vec!["bun".into(), "test".into()]),
        cwd,
        started: timestamp(),
        duration_ms: Some(1250),
        resources: Some(JobResourceSample {
            job_id: JobId::new(7).unwrap(),
            sampled_at: timestamp(),
            wall_ms: WallMicros::new(1_249_731).unwrap().millis(),
            wall_us: WallMicros::new(1_249_731).unwrap(),
            leader_pid: 4242,
            members: vec![4242, 4243],
            leaf: JobLeafAttribution::Attributed {
                leaf: JobProcessLeaf {
                    pid: 4242,
                    program: "bun".into(),
                    argv: vec!["bun".into(), "test".into()],
                    truncated: false,
                },
                observed_cpu_permille: 950,
            },
            cpu_user_ms: CpuMicros::new(1_100_000).unwrap().millis(),
            cpu_sys_ms: CpuMicros::new(90_000).unwrap().millis(),
            cpu_pct: OneCoreCpuPercent::new(87.5).unwrap(),
            host_start: HostLoadSample::new(1.25, 8).unwrap(),
            host: HostLoadSample::new(4.5, 16).unwrap(),
            rss_bytes: ResidentBytes::new(48 << 20).unwrap(),
            rss_peak_bytes: ResidentBytes::new(96 << 20).unwrap(),
            accounting: JobAccounting::MacOsRusageChildren {
                cpu: CpuTotals {
                    user_us: CpuMicros::new(1_100_000).unwrap(),
                    sys_us: CpuMicros::new(90_000).unwrap(),
                },
                io: None,
                reconciliation: UsageReconciliation {
                    cpu_us: CpuMicrosDelta::new(59_500).unwrap(),
                    io_read_bytes: None,
                    io_write_bytes: None,
                    coverage: ProcessCoverage::Gap {
                        reason: ProcessCoverageGap::UncountedFork { pid: 4242 },
                    },
                },
            },
            volumes: JobVolumeUsage {
                workspace: VolumeUsage::Read {
                    delta_bytes: VolumeUsedBytesDelta::new(-4096).unwrap(),
                },
                build: None,
            },
            stdout: JobStreamWatermark {
                bytes: StreamBytes::new(3).unwrap(),
                lines: StreamLines::new(1).unwrap(),
            },
            stderr: JobStreamWatermark {
                bytes: StreamBytes::new(0).unwrap(),
                lines: StreamLines::new(0).unwrap(),
            },
        }),
        exit: Some(ExitStatus::Signaled {
            signal: 15,
            core_dumped: false,
        }),
        stdout: stream(".cowshed/job/7/out", 3, "ok\n"),
        stderr: stream(".cowshed/job/7/err", 0, ""),
        trace: trace(),
        output_limit: None,
        stdin: StdinInfo {
            kind: StdinKind::WorkspaceFile,
            bytes: 12,
            workspace_path: Some(WorkspacePath::new("fixtures/input.bin").unwrap()),
            complete: true,
        },
        failure: None,
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: Some(10),
        containment: None,
        outlived: Vec::new(),
        outlived_incomplete: false,
    }
}

#[test]
fn workspace_snapshot_accessors_are_public_and_consuming_projections_are_owned() {
    let _: for<'a> fn(&'a WorkspaceRef) -> &'a WorkspaceInfo = WorkspaceRef::info;
    let _: for<'a> fn(&'a WorkspaceRef) -> &'a GrantSet = WorkspaceRef::grants;
    let _: for<'a> fn(&'a WorkspaceRef) -> (&'a WorkspaceInfo, &'a GrantSet) =
        WorkspaceRef::snapshot;
    let _: fn(WorkspaceRef) -> WorkspaceInfo = WorkspaceRef::into_info;
    let _: fn(WorkspaceRef) -> (WorkspaceInfo, GrantSet) = WorkspaceRef::into_snapshot;
}

#[test]
fn job_id_and_path_domain_types_reject_unsafe_values() {
    assert!(JobId::new(0).is_err());
    assert_eq!(JobId::new(MAX_JOB_ID).expect("max id").get(), MAX_JOB_ID);
    assert!(JobId::new(MAX_JOB_ID + 1).is_err());

    for path in [
        "",
        ".",
        "/absolute",
        "../escape",
        "a/../escape",
        "a//b",
        "a\\b",
    ] {
        assert!(WorkspacePath::new(path).is_err(), "accepted {path:?}");
    }
    assert_eq!(
        WorkspacePath::new(".cowshed/job/7/out")
            .expect("spool path")
            .as_path(),
        std::path::Path::new(".cowshed/job/7/out")
    );

    assert!(GitOid::new("A".repeat(40)).is_err());
    assert!(GitOid::new("a".repeat(39)).is_err());
    assert!(GitOid::new("a".repeat(40)).is_ok());
    assert!(GitOid::new("b".repeat(64)).is_ok());
    for valid in [
        "2024-02-29T23:59:59.123Z",
        "2016-12-31T23:59:60Z",
        "2016-12-31T18:59:60-05:00",
        "2017-01-01T00:59:60+01:00",
        "2026-07-11T12:34:56+05:30",
        "2026-07-11T12:34:56-00:00",
    ] {
        assert!(UtcTimestamp::new(valid).is_ok(), "rejected {valid}");
    }
    for invalid in [
        "2023-02-29T00:00:00Z",
        "2026-13-01T00:00:00Z",
        "2026-01-01T24:00:00Z",
        "2026-01-01T00:00:61Z",
        "2016-12-31T23:59:60-05:00",
        "2016-12-31T22:59:60Z",
        "2026-12-31T23:59:60Z",
        "2026-01-01 00:00:00Z",
        "2026-01-01T00:00:00+24:00",
    ] {
        assert!(UtcTimestamp::new(invalid).is_err(), "accepted {invalid}");
    }
    assert!(TraceId::new("0".repeat(32)).is_err());
    assert!(SpanId::new("0".repeat(16)).is_err());
}

#[cfg(unix)]
#[test]
fn command_args_are_canonical_lossless_and_strictly_bounded() {
    for byte in 1_u8..=u8::MAX {
        let bytes = vec![byte];
        let argument = CommandArg::from(OsString::from_vec(bytes.clone()));
        let value = serde_json::to_value(&argument).expect("serialize argument");
        assert_eq!(
            value["encoding"],
            if byte.is_ascii() { "utf8" } else { "base64" }
        );
        let decoded: CommandArg = serde_json::from_value(value).expect("decode argument");
        assert_eq!(decoded.as_os_str().as_bytes(), bytes);
    }

    let common = CommandArg::from("bun");
    assert_eq!(
        serde_json::to_value(common).unwrap(),
        json!({"encoding":"utf8","data":"bun"})
    );

    for invalid in [
        json!({"encoding":"base64","data":"%%%"}),
        json!({"encoding":"base64","data":"YQ=="}),
        json!({"encoding":"base64","data":"_w=="}),
        json!({"encoding":"wat","data":"x"}),
        json!({"encoding":"utf8","data":"x","extra":true}),
        json!({"encoding":"utf8","data":"\u{0}"}),
        json!({"encoding":"base64","data":"AA=="}),
    ] {
        assert!(
            serde_json::from_value::<CommandArg>(invalid).is_err(),
            "accepted non-canonical or unsafe argument"
        );
    }

    for invalid in [
        json!({"encoding":"base64","data":"YQ=="}),
        json!({"encoding":"base64","data":"/x=="}),
        json!({"encoding":"base64","data":"%%%"}),
    ] {
        assert!(
            serde_json::from_value::<BinaryData>(invalid).is_err(),
            "accepted non-canonical binary data"
        );
    }

    let oversize = json!({
        "encoding": "utf8",
        "data": "x".repeat(MAX_COMMAND_ARG_BYTES + 1),
    });
    assert!(serde_json::from_value::<CommandArg>(oversize).is_err());

    let maximum = CommandArg::from("x".repeat(MAX_COMMAND_ARG_BYTES));
    let mut argv = vec![maximum; MAX_ARGV_BYTES / MAX_COMMAND_ARG_BYTES];
    validate_command_argv(&argv).expect("exact total argv bound");
    argv.push(CommandArg::from("x"));
    assert_eq!(
        validate_command_argv(&argv),
        Err(DtoError::CommandArgvTooLarge)
    );
}

#[cfg(unix)]
#[test]
fn command_arg_serializer_reuses_the_public_projection() {
    let utf8 = CommandArg::from("bun");
    let raw = CommandArg::from(OsString::from_vec(vec![0xff, 0xfe]));
    for argument in [utf8, raw] {
        let through_serialize = serde_json::to_vec(&argument).expect("serialize argument");
        let through_projection = argument
            .with_wire(|proxy| serde_json::to_vec(proxy))
            .expect("projection")
            .expect("serialize projection");
        assert_eq!(through_serialize, through_projection);
    }

    let utf8 = CommandArg::from("bun");
    utf8.with_wire(|proxy| {
        assert_eq!(proxy.encoding, TaggedBytesEncoding::Utf8);
        assert_eq!(proxy.data, "bun");
    })
    .expect("utf8 projection");
    assert_eq!(
        serde_json::to_vec(&CommandArg::from("bun")).expect("serialize"),
        br#"{"encoding":"utf8","data":"bun"}"#
    );

    let raw = CommandArg::from(OsString::from_vec(vec![0xff, 0xfe]));
    raw.with_wire(|proxy| {
        assert_eq!(proxy.encoding, TaggedBytesEncoding::Base64);
        assert_eq!(proxy.data, "//4=");
    })
    .expect("base64 projection");

    let nul = CommandArg::from(OsString::from_vec(vec![b'x', 0]));
    let mut called = false;
    assert_eq!(
        nul.with_wire(|_| {
            called = true;
        }),
        Err(DtoError::CommandArgumentContainsNul)
    );
    assert!(!called, "a refused argument must not lend a projection");
}

#[test]
fn workspace_info_attached_and_detached_shapes_are_frozen() {
    let attached = WorkspaceInfo {
        repo_id: repo(),
        workspace: workspace(),
        workspace_incarnation: incarnation(),
        role: WorkspaceRole::Workspace,
        mount: PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/raven"),
        state: WorkspaceState::Attached,
        branch: Some("raven".into()),
        base_commit: Some(oid('a')),
        created_at: Some(timestamp()),
        snapshot_stale: false,
        // A landing measurement is opt-in, so the frozen shape is the one without it. The
        // populated shape is pinned separately below.
        landing: None,
    };
    assert_eq!(
        serde_json::to_value(&attached).expect("attached JSON"),
        json!({
            "repoId": "acme/widget",
            "workspace": "raven",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "role": "workspace",
            "mount": "/Users/tester/.cowshed/mnt/acme/widget/raven",
            "state": "attached",
            "branch": "raven",
            "baseCommit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "createdAt": "2026-07-11T12:34:56Z",
            "snapshotStale": false
        })
    );

    let detached = WorkspaceInfo {
        branch: None,
        base_commit: None,
        created_at: None,
        state: WorkspaceState::Detached,
        snapshot_stale: true,
        ..attached
    };
    let detached_json = serde_json::to_value(detached).expect("detached JSON");
    assert_eq!(detached_json["state"], "detached");
    assert_eq!(detached_json["snapshotStale"], true);
    for absent in ["branch", "baseCommit", "createdAt", "landing"] {
        assert!(detached_json.get(absent).is_none(), "unexpected {absent}");
    }
}

/// Scripts are the main consumer of the landing fields, so the shape they read is frozen here.
///
/// The discriminated `state` is the load-bearing part: a script must be able to tell "measured, and
/// nothing is unlanded" from "nobody could tell", and a shape where the counts were merely absent
/// would let a `// unlanded == 0` test pass on an unanswered question.
#[test]
fn workspace_landing_shapes_are_frozen_and_discriminated() {
    let measured = WorkspaceLanding {
        dirty_files: Some(2),
        commits: LandingCommits::Measured {
            target_branch: "main".into(),
            target_head: oid('b'),
            unlanded: 4,
            landed: 541,
            behind: 673,
            unlanded_paths: UnlandedPaths::Named {
                paths: vec![
                    UnlandedPath {
                        path: "src/gate.rs".into(),
                        change: UnlandedChange::Conflict,
                    },
                    UnlandedPath {
                        path: "assets/logo.png".into(),
                        change: UnlandedChange::Unmergeable {
                            reason: "Cannot merge binary files".into(),
                        },
                    },
                ],
            },
            preserved_by: None,
        },
        branches: BranchLandings::Measured {
            unlanded: vec![BranchLanding {
                branch: "side".into(),
                tip: oid('c'),
                commits: LandingCommits::Measured {
                    target_branch: "main".into(),
                    target_head: oid('b'),
                    unlanded: 2,
                    landed: 0,
                    behind: 673,
                    unlanded_paths: UnlandedPaths::Named {
                        paths: vec![UnlandedPath {
                            path: "side.txt".into(),
                            change: UnlandedChange::Added,
                        }],
                    },
                    preserved_by: None,
                },
            }],
        },
    };
    assert_eq!(
        serde_json::to_value(&measured).expect("measured JSON"),
        json!({
            "dirtyFiles": 2,
            "commits": {
                "state": "measured",
                "targetBranch": "main",
                "targetHead": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "unlanded": 4,
                "landed": 541,
                "behind": 673,
                "unlandedPaths": { "kind": "named", "paths": [
                    { "path": "src/gate.rs", "change": { "kind": "conflict" } },
                    {
                        "path": "assets/logo.png",
                        "change": { "kind": "unmergeable", "reason": "Cannot merge binary files" }
                    }
                ] }
            },
            "branches": {
                "state": "measured",
                "unlanded": [{
                    "branch": "side",
                    "tip": "cccccccccccccccccccccccccccccccccccccccc",
                    "commits": {
                        "state": "measured",
                        "targetBranch": "main",
                        "targetHead": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "unlanded": 2,
                        "landed": 0,
                        "behind": 673,
                        "unlandedPaths": {
                            "kind": "named",
                            "paths": [{ "path": "side.txt", "change": { "kind": "added" } }]
                        }
                    }
                }]
            }
        })
    );
    assert!(measured.commits.target_head().is_some());
    assert!(!measured.commits.fully_landed());

    let indeterminate = WorkspaceLanding {
        dirty_files: None,
        commits: LandingCommits::Indeterminate {
            reason: "main's repository has no main branch".into(),
        },
        branches: BranchLandings::Indeterminate {
            reason: "main's repository has no main branch".into(),
        },
    };
    assert_eq!(
        serde_json::to_value(&indeterminate).expect("indeterminate JSON"),
        json!({
            "commits": {
                "state": "indeterminate",
                "reason": "main's repository has no main branch"
            },
            "branches": {
                "state": "indeterminate",
                "reason": "main's repository has no main branch"
            }
        })
    );
    assert!(
        !indeterminate.commits.fully_landed(),
        "an unanswered question must never satisfy the landed predicate"
    );
    assert!(indeterminate.commits.target_head().is_none());

    // Nothing unlanded, at the head or on any other branch, is the one shape that authorizes a
    // no-flag removal; a landed head beside an unlanded branch does not.
    let landed_head = |branches| WorkspaceLanding {
        dirty_files: Some(0),
        commits: LandingCommits::Measured {
            target_branch: "main".into(),
            target_head: oid('b'),
            unlanded: 0,
            landed: 1,
            behind: 1,
            unlanded_paths: UnlandedPaths::Named { paths: Vec::new() },
            preserved_by: None,
        },
        branches,
    };
    assert!(
        landed_head(BranchLandings::Measured {
            unlanded: Vec::new()
        })
        .fully_landed()
    );
    let BranchLandings::Measured { unlanded } = measured.branches.clone() else {
        unreachable!("measured above")
    };
    assert!(!landed_head(BranchLandings::Measured { unlanded }).fully_landed());
    assert!(!landed_head(indeterminate.branches.clone()).fully_landed());
}

/// A head the target does not hold and a push ref keeps names that ref, beside the counts that
/// still say what the target lacks: kept is not held, and only the kept head and the held head
/// satisfy the landed predicate. A record from a build before the field reads as no ref at all.
#[test]
fn a_head_a_push_ref_keeps_names_its_ref_and_is_never_read_as_held_by_the_target() {
    let kept = LandingCommits::Measured {
        target_branch: "main".into(),
        target_head: oid('b'),
        unlanded: 7,
        landed: 0,
        behind: 2,
        unlanded_paths: UnlandedPaths::Named { paths: Vec::new() },
        preserved_by: Some(PushRef {
            reference: "refs/cowshed/raven/heads/cowshed/raven".into(),
            tip: oid('d'),
        }),
    };
    let wire = json!({
        "state": "measured",
        "targetBranch": "main",
        "targetHead": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "unlanded": 7,
        "landed": 0,
        "behind": 2,
        "unlandedPaths": { "kind": "named", "paths": [] },
        "preservedBy": {
            "reference": "refs/cowshed/raven/heads/cowshed/raven",
            "tip": "dddddddddddddddddddddddddddddddddddddddd"
        }
    });
    assert_eq!(serde_json::to_value(&kept).expect("kept JSON"), wire);
    assert_eq!(
        serde_json::from_value::<LandingCommits>(wire).expect("kept record"),
        kept
    );
    assert!(kept.fully_landed(), "the ref keeps the head");
    assert!(!kept.held_by_target(), "main lacks seven commits");
    assert_eq!(
        kept.preserved_by().map(|pushed| pushed.reference.as_str()),
        Some("refs/cowshed/raven/heads/cowshed/raven")
    );

    let before_the_field = serde_json::from_value::<LandingCommits>(json!({
        "state": "measured",
        "targetBranch": "main",
        "targetHead": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "unlanded": 7,
        "landed": 0,
        "behind": 2
    }))
    .expect("a record written before preservedBy existed");
    assert_eq!(before_the_field.preserved_by(), None);
    assert!(!before_the_field.fully_landed());

    let unanswered = LandingCommits::Indeterminate {
        reason: "main's repository has no main branch".into(),
    };
    assert_eq!(unanswered.preserved_by(), None);
    assert!(!unanswered.fully_landed() && !unanswered.held_by_target());
}

#[test]
fn doctor_gc_and_empty_results_have_exact_shapes() {
    let doctor = DoctorReport {
        healthy: false,
        findings: vec![Finding {
            code: "format-mismatch".into(),
            severity: FindingSeverity::Error,
            message: "detached format disagrees with extension".into(),
            hint: "cowshed doctor --fix-format raven".into(),
            path: Some(PathBuf::from("/store/raven.dmg.grants.json")),
        }],
    };
    assert_eq!(
        serde_json::to_value(doctor).expect("doctor JSON"),
        json!({
            "healthy": false,
            "findings": [{
                "code": "format-mismatch",
                "severity": "error",
                "message": "detached format disagrees with extension",
                "hint": "cowshed doctor --fix-format raven",
                "path": "/store/raven.dmg.grants.json"
            }]
        })
    );

    let gc_identity = Sha256Digest::compute(b"orphan-session-image");
    let gc = GcReport {
        examined: 9,
        reclaimed: 3,
        retained_active: 0,
        released_bytes: 4096,
        dry_run: true,
        candidates: vec![GcCandidate {
            identity: gc_identity,
            path: PathBuf::from("/store/sessions/raven/old.dmg"),
            bytes: 4096,
            reason: GcReason::OrphanSessionImage,
        }],
        deferred: Vec::new(),
    };
    assert_eq!(
        serde_json::to_value(gc).expect("gc JSON"),
        json!({
            "examined":9,
            "reclaimed":3,
            "retainedActive":0,
            "releasedBytes":4096,
            "dryRun":true,
            "candidates":[{
                "identity":gc_identity.to_string(),
                "path":"/store/sessions/raven/old.dmg",
                "bytes":4096,
                "reason":"orphanSessionImage"
            }],
            "deferred": []
        })
    );
    assert_eq!(serde_json::to_value(EmptyResult {}).unwrap(), json!({}));
}

#[test]
fn nested_job_info_shape_is_byte_safe_and_frozen() {
    let info = finished_job(Some(WorkspacePath::new("packages/app").unwrap()));
    let value = serde_json::to_value(&info).expect("job JSON");
    assert_eq!(
        value,
        json!({
            "repoId": "acme/widget",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "jobId": 7,
            "state": "signaled",
            "pid": 4242,
            "grantRevision": 9,
            "argv": [
                {"encoding":"utf8","data":"bun"},
                {"encoding":"utf8","data":"test"}
            ],
            "cwd": "packages/app",
            "started": "2026-07-11T12:34:56Z",
            "durationMs": 1250,
            "resources": {"jobId":7,"sampledAt":"2026-07-11T12:34:56Z","wallMs":1249,"wallUs":1_249_731,"leaderPid":4242,"members":[4242,4243],"leaf":{"kind":"attributed","leaf":{"pid":4242,"program":"bun","argv":[{"encoding":"utf8","data":"bun"},{"encoding":"utf8","data":"test"}],"truncated":false},"observedCpuPermille":950},"cpuUserMs":1100,"cpuSysMs":90,"cpuPct":87.5,"hostStart":{"load1":1.25,"cores":8},"host":{"load1":4.5,"cores":16},"rssBytes":50_331_648,"rssPeakBytes":100_663_296,"accounting":{"kind":"macOsRusageChildren","cpu":{"userUs":1_100_000,"sysUs":90_000},"reconciliation":{"cpuUs":59_500,"coverage":{"kind":"gap","reason":{"kind":"uncountedFork","pid":4242}}}},"volumes":{"workspace":{"kind":"read","deltaBytes":-4096}},"stdout":{"bytes":3,"lines":1},"stderr":{"bytes":0,"lines":0}},
            "exit": {"kind":"signaled","signal":15,"coreDumped":false},
            "stdout": {"storage":{"kind":"captured","artifact":{"kind":"file","path":".cowshed/job/7/out"}},"bytes":3,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","summary":{"version":1,"text":"ok\n","truncated":false}},
            "stderr": {"storage":{"kind":"captured","artifact":{"kind":"file","path":".cowshed/job/7/err"}},"bytes":0,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","summary":{"version":1,"text":"","truncated":false}},
            "trace": {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7"},
            "class": "background",
            "scope": "workspace",
            "nice": 10,
            "stdin": {"kind":"workspaceFile","bytes":12,"workspacePath":"fixtures/input.bin","complete":true}
        })
    );
    info.validate().expect("job invariants");
    let encoded = serde_json::to_string(&value).unwrap();
    assert!(!encoded.contains("rawBytes"));
    assert!(!encoded.contains("stdoutBytes"));
    assert_eq!(serde_json::from_value::<JobInfo>(value).unwrap(), info);

    let mut queued = info.clone();
    queued.state = JobState::Queued;
    queued.duration_ms = None;
    queued.exit = None;
    assert!(
        queued.validate().is_err(),
        "a queued job owns no process to sample"
    );
    let mut foreign = info.clone();
    foreign.resources.as_mut().unwrap().job_id = JobId::new(8).unwrap();
    assert!(foreign.validate().is_err(), "a sample of another job");
    let mut disagreeing = info.clone();
    disagreeing.resources.as_mut().unwrap().wall_us = WallMicros::new(2_000_000).unwrap();
    assert!(
        disagreeing.validate().is_err(),
        "wallMs must project wallUs"
    );
    let mut past_peak = info.clone();
    past_peak.resources.as_mut().unwrap().rss_bytes = ResidentBytes::new(97 << 20).unwrap();
    assert!(
        past_peak.validate().is_err(),
        "rssPeakBytes includes the sample's own rssBytes"
    );
    let mut impossible = info;
    impossible.resources.as_mut().unwrap().stderr.lines = StreamLines::new(1).unwrap();
    assert!(
        impossible.validate().is_err(),
        "an empty stream has no line"
    );
}

#[test]
fn root_job_info_requires_explicit_null_cwd() {
    let info = finished_job(None);
    let value = json!({
        "repoId": "acme/widget",
        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
        "jobId": 7,
        "state": "signaled",
        "pid": 4242,
        "grantRevision": 9,
        "argv": [
            {"encoding":"utf8","data":"bun"},
            {"encoding":"utf8","data":"test"}
        ],
        "cwd": null,
        "started": "2026-07-11T12:34:56Z",
        "durationMs": 1250,
        "resources": {"jobId":7,"sampledAt":"2026-07-11T12:34:56Z","wallMs":1249,"wallUs":1_249_731,"leaderPid":4242,"members":[4242,4243],"leaf":{"kind":"attributed","leaf":{"pid":4242,"program":"bun","argv":[{"encoding":"utf8","data":"bun"},{"encoding":"utf8","data":"test"}],"truncated":false},"observedCpuPermille":950},"cpuUserMs":1100,"cpuSysMs":90,"cpuPct":87.5,"hostStart":{"load1":1.25,"cores":8},"host":{"load1":4.5,"cores":16},"rssBytes":50_331_648,"rssPeakBytes":100_663_296,"accounting":{"kind":"macOsRusageChildren","cpu":{"userUs":1_100_000,"sysUs":90_000},"reconciliation":{"cpuUs":59_500,"coverage":{"kind":"gap","reason":{"kind":"uncountedFork","pid":4242}}}},"volumes":{"workspace":{"kind":"read","deltaBytes":-4096}},"stdout":{"bytes":3,"lines":1},"stderr":{"bytes":0,"lines":0}},
        "exit": {"kind":"signaled","signal":15,"coreDumped":false},
        "stdout": {"storage":{"kind":"captured","artifact":{"kind":"file","path":".cowshed/job/7/out"}},"bytes":3,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","summary":{"version":1,"text":"ok\n","truncated":false}},
        "stderr": {"storage":{"kind":"captured","artifact":{"kind":"file","path":".cowshed/job/7/err"}},"bytes":0,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","summary":{"version":1,"text":"","truncated":false}},
        "trace": {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7"},
        "class": "background",
        "scope": "workspace",
        "nice": 10,
        "stdin": {"kind":"workspaceFile","bytes":12,"workspacePath":"fixtures/input.bin","complete":true}
    });

    assert_eq!(serde_json::to_value(&info).expect("root job JSON"), value);
    assert_eq!(
        serde_json::from_value::<JobInfo>(value.clone()).expect("root job"),
        info
    );

    let mut omitted = value.clone();
    omitted.as_object_mut().unwrap().remove("cwd");
    assert!(serde_json::from_value::<JobInfo>(omitted).is_err());

    for invalid in [".", "/absolute", "../escape", "a/../escape"] {
        let mut invalid_value = value.clone();
        invalid_value["cwd"] = json!(invalid);
        assert!(
            serde_json::from_value::<JobInfo>(invalid_value).is_err(),
            "accepted invalid cwd {invalid:?}"
        );
    }
}

#[test]
fn output_limit_is_an_explicit_terminal_projection() {
    let mut value = json!({
        "repoId": "acme/widget",
        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
        "jobId": 8,
        "state": "outputLimit",
        "grantRevision": 9,
        "argv": [{"encoding":"utf8","data":"cat"}],
        "cwd": "packages/app",
        "started": "2026-07-11T12:34:56Z",
        "durationMs": 50,
        "stdout": {"storage":{"kind":"captured","artifact":{"kind":"file","path":".cowshed/job/8/out"}},"bytes":1024,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","summary":{"version":1,"text":"bounded","truncated":true}},
        "stderr": {"storage":{"kind":"captured","artifact":{"kind":"file","path":".cowshed/job/8/err"}},"bytes":1,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","summary":{"version":1,"text":"","truncated":false}},
        "trace": {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7"},
        "outputLimit": {"limitBytes":1024,"crossingBytes":1025},
        "class": "background",
        "scope": "workspace",
        "stdin": {"kind":"empty","bytes":0,"complete":true}
    });
    let info: JobInfo = serde_json::from_value(value.clone()).expect("output limit job");
    info.validate().expect("valid output-limit projection");
    assert_eq!(info.state, JobState::OutputLimit);
    assert_eq!(info.output_limit.as_ref().unwrap().crossing_bytes, 1025);
    let mut invalid = info.clone();
    invalid.output_limit = None;
    assert!(serde_json::to_value(invalid).is_err());
    value.as_object_mut().unwrap().remove("outputLimit");
    assert!(serde_json::from_value::<JobInfo>(value).is_err());
}

/// A job killed before its command existed never ran: it is projected killed with no pid and no
/// exit status. A killed job that owned a process keeps the status it ended with; one that omits
/// it is refused, so the two facts never share a projection.
#[test]
fn a_job_killed_before_it_ran_has_no_exit_and_one_that_ran_keeps_its_own() {
    let mut never_ran = finished_job(None);
    never_ran.state = JobState::Killed;
    never_ran.pid = None;
    never_ran.exit = None;
    never_ran.resources = None;
    never_ran.nice = None;
    let value = serde_json::to_value(&never_ran).expect("a job killed while queued is projected");
    assert!(value.get("exit").is_none() && value.get("pid").is_none());
    assert_eq!(
        serde_json::from_value::<JobInfo>(value).expect("and decoded"),
        never_ran
    );

    let mut ran = finished_job(None);
    ran.state = JobState::Killed;
    ran.exit = None;
    assert!(
        ran.validate().is_err(),
        "a killed job that owned a process keeps its status"
    );
}

#[cfg(unix)]
#[test]
fn exec_record_enforces_terminal_invariants_both_directions() {
    let value = json!({
        "repoId": "acme/widget",
        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
        "jobId": 9,
        "state": "exited",
        "argv": [{"encoding":"base64","data":"/w=="}],
        "cwd": "packages/app",
        "envHash": 17,
        "grantRevision": 9,
        "trace": {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7"},
        "started": "2026-07-11T12:34:56Z",
        "durationMs": 3,
        "exit": {"kind":"exited","code":0},
        "stdout": {"storage":{"kind":"captured","artifact":{"kind":"file","path":".cowshed/job/9/out"}},"bytes":0,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","summary":{"version":1,"text":"","truncated":false}},
        "stderr": {"storage":{"kind":"captured","artifact":{"kind":"file","path":".cowshed/job/9/err"}},"bytes":0,"sha256":"0000000000000000000000000000000000000000000000000000000000000000","summary":{"version":1,"text":"","truncated":false}},
        "stdin": {"kind":"empty","bytes":0,"complete":true}
    });
    let record: ExecRecord = serde_json::from_value(value.clone()).unwrap();
    record.validate().unwrap();
    assert_eq!(record.argv[0].as_os_str().as_bytes(), &[0xff]);
    assert_eq!(serde_json::to_value(&record).unwrap(), value);

    for exit in [
        json!({"kind":"exited","code":0}),
        json!({"kind":"exited","code":143}),
        json!({"kind":"signaled","signal":15,"coreDumped":false}),
    ] {
        let mut cancelled = value.clone();
        cancelled["state"] = json!("killed");
        cancelled["exit"] = exit;
        let record: ExecRecord = serde_json::from_value(cancelled.clone())
            .expect("requested cancellation preserves the actual process exit");
        assert_eq!(serde_json::to_value(record).unwrap(), cancelled);
    }
    let mut missing_exit = value.clone();
    missing_exit["state"] = json!("killed");
    missing_exit.as_object_mut().unwrap().remove("exit");
    assert!(serde_json::from_value::<ExecRecord>(missing_exit).is_err());

    let mut invalid_record = record;
    invalid_record.state = JobState::Running;
    assert!(serde_json::to_value(invalid_record).is_err());

    let mut invalid_value = value;
    invalid_value["state"] = json!("signaled");
    assert!(serde_json::from_value::<ExecRecord>(invalid_value).is_err());
}

#[test]
fn grant_platform_union_omits_linux_port_block() {
    let linux = GrantSet::closed_baseline(None).unwrap();
    let linux_json = serde_json::to_value(&linux).unwrap();
    assert!(linux_json.get("portBlock").is_none());

    let macos = GrantSet::closed_baseline(Some(PortBlock::new(40960, 16).unwrap())).unwrap();
    assert_eq!(
        serde_json::to_value(macos).unwrap()["portBlock"],
        json!({"base":40960,"size":16})
    );
    let block: PortBlock =
        serde_json::from_value(json!({"base":40960,"size":16})).expect("valid port block");
    assert_eq!((block.base(), block.size()), (40960, 16));
    for invalid in [
        json!({"base":40960,"size":0}),
        json!({"base":40960,"size":15}),
        json!({"base":65530,"size":16}),
        json!({"base":40960,"size":16,"extra":true}),
    ] {
        assert!(serde_json::from_value::<PortBlock>(invalid).is_err());
    }
}

#[test]
fn revision_and_expected_head_unions_are_exact_objects() {
    assert_eq!(
        serde_json::to_value(RevisionTarget::Branch(BranchName::new("main").unwrap())).unwrap(),
        json!({"branch":"main"})
    );
    assert_eq!(
        serde_json::to_value(RevisionTarget::Ref(GitRef::new("refs/tags/v1").unwrap(),)).unwrap(),
        json!({"ref":"refs/tags/v1"})
    );
    for invalid in [
        json!({"branch":"-topic"}),
        json!({"branch":"topic","ref":"refs/heads/topic"}),
        json!({"ref":"heads/topic"}),
        json!({"ref":"refs/heads/bad..name"}),
        json!("main"),
    ] {
        assert!(serde_json::from_value::<RevisionTarget>(invalid).is_err());
    }
    assert_eq!(
        serde_json::to_value(ExpectedRefHead::Missing).unwrap(),
        json!({"missing":true})
    );
    assert_eq!(
        serde_json::to_value(ExpectedRefHead::Oid(oid('b'))).unwrap(),
        json!({"oid":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"})
    );
    for invalid in [
        json!({"missing":false}),
        json!({"missing":true,"oid":"a"}),
        json!("missing"),
    ] {
        assert!(serde_json::from_value::<ExpectedRefHead>(invalid).is_err());
    }
}
#[test]
fn cli_revision_parser_has_exact_precedence_and_no_rev_expression_fallback() {
    let oid40 = "a".repeat(40);
    let oid64 = "b".repeat(64);
    assert_eq!(
        RevisionTarget::parse_cli(&oid40).unwrap(),
        RevisionTarget::Oid(GitOid::new(oid40).unwrap())
    );
    assert_eq!(
        RevisionTarget::parse_cli(&oid64).unwrap(),
        RevisionTarget::Oid(GitOid::new(oid64).unwrap())
    );
    assert_eq!(
        RevisionTarget::parse_cli("refs/heads/topic").unwrap(),
        RevisionTarget::Ref(GitRef::new("refs/heads/topic").unwrap())
    );
    assert_eq!(
        RevisionTarget::parse_cli("main").unwrap(),
        RevisionTarget::Branch(BranchName::new("main").unwrap())
    );
    assert_eq!(
        RevisionTarget::parse_cli("A".repeat(40)).unwrap(),
        RevisionTarget::Branch(BranchName::new("A".repeat(40)).unwrap())
    );

    for invalid in [
        "",
        "-topic",
        "HEAD~1",
        "main^{tree}",
        "topic..next",
        "refs/heads/bad..name",
        "refs/heads/topic.lock",
    ] {
        assert!(
            RevisionTarget::parse_cli(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
}

#[test]
fn all_lifecycle_options_use_camel_case_and_omit_only_optionals() {
    assert_eq!(
        serde_json::to_value(AdoptOptions {
            path: None,
            repo_id: Some(repo()),
            capacity: Some("100g".into()),
            quarantine: true,
            re_adopt: true,
        })
        .unwrap(),
        json!({"repoId":"acme/widget","capacity":"100g","quarantine":true,"reAdopt":true})
    );
    assert_eq!(
        serde_json::to_value(AdoptOptions::default()).unwrap(),
        json!({"quarantine":false,"reAdopt":false})
    );
    assert_eq!(
        serde_json::to_value(ForkOptions {
            revision: Some(RevisionTarget::Oid(oid('a'))),
            clean: true,
            browse: false,
            slot: Some(2),
            register: true,
            git_worktree: true,
            markdown_only: false,
        })
        .unwrap(),
        json!({
            "ref":{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "clean":true,
            "browse":false,
            "slot":2,
            "register":true,
            "gitWorktree":true
        })
    );
    assert_eq!(
        serde_json::to_value(AttachOptions {
            browse: true,
            observed_path: Some(PathBuf::from("/Users/tester/Dev/widget")),
            aggregate: false,
        })
        .unwrap(),
        json!({"browse":true,"observedPath":"/Users/tester/Dev/widget","aggregate":false})
    );
    assert_eq!(
        serde_json::to_value(RemoveOptions {
            force: false,
            restore: true,
            abandon: false,
        })
        .unwrap(),
        json!({"force":false,"restore":true,"abandon":false})
    );
    // An ordinary removal reports an empty object: `abandoned` appears only when commits died.
    assert_eq!(
        serde_json::to_value(RemoveReport::default()).unwrap(),
        json!({})
    );
    assert_eq!(
        serde_json::to_value(RemoveReport {
            abandoned: Some(AbandonedWork {
                head: GitOid::new("4".repeat(40)).unwrap(),
                target_branch: "main".into(),
                target_head: Some(GitOid::new("1".repeat(40)).unwrap()),
                unlanded_commits: 3,
                branches: Vec::new(),
                preserved: AbandonedPreservation::BundleOnly,
                bundle: PathBuf::from("/store/acme/widget/sessions/.trash/raven-tip.bundle"),
            }),
        })
        .unwrap(),
        json!({"abandoned":{
            "head":"4444444444444444444444444444444444444444",
            "targetBranch":"main",
            "targetHead":"1111111111111111111111111111111111111111",
            "unlandedCommits":3,
            "bundle":"/store/acme/widget/sessions/.trash/raven-tip.bundle"
        }})
    );
    assert_eq!(
        serde_json::to_value(GcOptions {
            dry_run: true,
            abandoned: false,
        })
        .unwrap(),
        json!({"dryRun":true,"abandoned":false})
    );
    assert_eq!(
        serde_json::from_value::<GcOptions>(json!({"abandoned":true})).unwrap(),
        GcOptions {
            dry_run: false,
            abandoned: true,
        }
    );

    assert_eq!(
        serde_json::from_value::<AttachOptions>(json!({})).unwrap(),
        AttachOptions::default()
    );
    assert_eq!(
        serde_json::from_value::<ForkOptions>(json!({})).unwrap(),
        ForkOptions::default()
    );
    let land_default = serde_json::from_value::<LandOptions>(json!({})).unwrap();
    assert!(land_default.retire);
    assert!(!land_default.push_only);
    assert_eq!(
        serde_json::to_value(land_default).unwrap(),
        json!({"retire":true,"pushOnly":false})
    );
    // Gated is the default and travels as nothing; only a lane fold names its mode.
    let ungated = serde_json::from_value::<LandOptions>(json!({"mode":"ungated"})).unwrap();
    assert_eq!(ungated.mode, cowshed_core::api::dto::LandMode::Ungated);
    assert_eq!(
        serde_json::to_value(ungated).unwrap(),
        json!({"retire":true,"pushOnly":false,"mode":"ungated"})
    );
    let grant_delta = serde_json::from_value::<GrantDelta>(json!({})).unwrap();
    assert_eq!(grant_delta, GrantDelta::default());
    assert_eq!(serde_json::to_value(grant_delta).unwrap(), json!({}));
    // `servicePorts` is the minimum service-port count, the gateway port excluded; absent means no
    // port request, and it travels as a bare JSON number in camelCase like every other field.
    let ports_delta = GrantDelta {
        service_ports: Some(80),
        ..GrantDelta::default()
    };
    assert_eq!(
        serde_json::to_value(&ports_delta).unwrap(),
        json!({"servicePorts":80})
    );
    assert_eq!(
        serde_json::from_value::<GrantDelta>(json!({"servicePorts":80})).unwrap(),
        ports_delta
    );
    for refused in [
        json!({"service_ports":80}),
        json!({"servicePorts":-1}),
        json!({"servicePorts":65536}),
        json!({"servicePorts":"80"}),
    ] {
        assert!(
            serde_json::from_value::<GrantDelta>(refused.clone()).is_err(),
            "{refused}"
        );
    }

    let rebase = RebaseOptions {
        onto: Some(RevisionTarget::Branch(BranchName::new("main").unwrap())),
        expected_workspace_incarnation: Some(incarnation()),
        expected_source_head: Some(oid('a')),
        expected_onto_head: None,
    };
    let rebase_json = serde_json::to_value(&rebase).unwrap();
    assert_eq!(
        rebase_json["expectedWorkspaceIncarnation"],
        incarnation().as_str()
    );
    assert!(rebase_json.get("expectedOntoHead").is_none());
    // A rebase is Git alone: it names no job class and no fold mode, and refuses either field.
    assert_eq!(
        serde_json::from_value::<RebaseOptions>(rebase_json).unwrap(),
        rebase
    );
    for refused in [json!({"gate":true}), json!({"mode":"ungated"})] {
        assert!(
            serde_json::from_value::<RebaseOptions>(refused.clone()).is_err(),
            "{refused}"
        );
    }

    let push = PushOptions {
        branch: None,
        expected_workspace_incarnation: Some(incarnation()),
        expected_source_head: Some(oid('a')),
        expected_destination_head: Some(ExpectedRefHead::Missing),
    };
    assert_eq!(
        serde_json::to_value(push).unwrap()["expectedDestinationHead"],
        json!({"missing":true})
    );
}

#[test]
fn seed_capture_skip_shapes_preserve_the_named_writer_and_refusal() {
    use cowshed_core::api::dto::{AdoptionSkip, DatabaseHolder};
    let building = AdoptionSkip::LandingBuilding {
        lock: PathBuf::from("/build/target/guard/.cargo-lock"),
        holders: vec![DatabaseHolder {
            pid: 42,
            command: "cargo build --profile guard".into(),
        }],
    };
    let wire = json!({
        "kind": "landingBuilding",
        "lock": "/build/target/guard/.cargo-lock",
        "holders": [{"pid": 42, "command": "cargo build --profile guard"}]
    });
    assert_eq!(serde_json::to_value(&building).unwrap(), wire);
    assert_eq!(
        serde_json::from_value::<AdoptionSkip>(wire).unwrap(),
        building
    );
    assert_eq!(building.name(), "landingBuilding");
    let opening = AdoptionSkip::LandingOpening {
        database: PathBuf::from("/build/nx/workspace-data/tasks.db"),
        holders: vec![DatabaseHolder {
            pid: 43,
            command: "node nx build widget".into(),
        }],
    };
    let wire = json!({
        "kind": "landingOpening",
        "database": "/build/nx/workspace-data/tasks.db",
        "holders": [{"pid": 43, "command": "node nx build widget"}]
    });
    assert_eq!(serde_json::to_value(&opening).unwrap(), wire);
    assert_eq!(
        serde_json::from_value::<AdoptionSkip>(wire).unwrap(),
        opening
    );
    assert_eq!(opening.name(), "landingOpening");
}

#[test]
fn seed_compaction_reports_only_observed_foreground_facts() {
    use cowshed_core::api::dto::{LandSeedCompaction, SeedCompactionReport};
    assert_eq!(
        serde_json::to_value(LandSeedCompaction::NotAdvised).unwrap(),
        json!({"kind": "notAdvised"})
    );
    assert_eq!(
        serde_json::to_value(LandSeedCompaction::Started {
            pid: 42,
            log: "/fixture/compaction.log".into(),
        })
        .unwrap(),
        json!({"kind": "started", "pid": 42, "log": "/fixture/compaction.log"})
    );
    for obsolete in [
        json!({"kind": "recent", "secondsAgo": 1}),
        json!({"kind": "below", "extents": 499999}),
        json!({"kind": "noSeed"}),
        json!({"kind": "started", "pid": 42, "log": "/fixture/compaction.log", "extents": 500000}),
    ] {
        assert!(
            serde_json::from_value::<LandSeedCompaction>(obsolete.clone()).is_err(),
            "obsolete foreground report: {obsolete}"
        );
    }
    assert!(
        serde_json::from_value::<SeedCompactionReport>(json!({"kind": "recent", "secondsAgo": 1}))
            .is_err()
    );
    assert_eq!(
        serde_json::to_value(SeedCompactionReport::Below { extents: 499999 }).unwrap(),
        json!({"kind": "below", "extents": 499999})
    );
    assert_eq!(
        serde_json::to_value(SeedCompactionReport::NoSeed).unwrap(),
        json!({"kind": "noSeed"})
    );
}

#[test]
fn reports_gateway_and_audit_shapes_are_frozen() {
    let push = PushReport {
        source_head: oid('a'),
        destination_ref: "refs/cowshed/raven/heads/topic".into(),
        previous_destination_head: None,
    };
    assert_eq!(
        serde_json::to_value(push).unwrap(),
        json!({
            "sourceHead":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "destinationRef":"refs/cowshed/raven/heads/topic"
        })
    );
    let land = LandReport::Landed(cowshed_core::api::dto::LandedReport {
        landed_head: oid('b'),
        target_branch: "main".into(),
        previous_target_head: Some(oid('a')),
        target_was_checked_out: true,
        retired: true,
        folded: Vec::new(),
        build_volume: cowshed_core::api::dto::LandBuildVolume {
            seed: cowshed_core::api::dto::LandSeed::Frozen {
                compaction: cowshed_core::api::dto::LandSeedCompaction::Failed {
                    error: "cannot open the seed compaction log at /Users/fixture/.cache/cowshed/build/.compacting/log: Permission denied (os error 13)".to_owned(),
                },
            },
            adoption: cowshed_core::api::dto::Adoption::Skipped {
                reason: cowshed_core::api::dto::AdoptionSkip::TargetHeld {
                    database: PathBuf::from("/m/.nx/workspace-data/A-v3.db"),
                    holders: vec![cowshed_core::api::dto::DatabaseHolder {
                        pid: 42,
                        command: "node nx run-many".into(),
                    }],
                },
            },
        },
    });
    let land = serde_json::to_value(land).unwrap();
    assert_eq!(land["kind"], "landed");
    assert_eq!(land["targetWasCheckedOut"], true);
    assert_eq!(
        land["buildVolume"],
        json!({"seed":{"kind":"frozen","compaction":{"kind":"failed","error":"cannot open the seed compaction log at /Users/fixture/.cache/cowshed/build/.compacting/log: Permission denied (os error 13)"}},
            "adoption":{"kind":"skipped","reason":{"kind":"targetHeld",
            "database":"/m/.nx/workspace-data/A-v3.db","holders":[{"pid":42,"command":"node nx run-many"}]}}})
    );

    let delivered = LandReport::Delivered(PushReport {
        source_head: oid('b'),
        destination_ref: "refs/cowshed/raven/heads/topic".into(),
        previous_destination_head: None,
    });
    let wire = json!({
        "kind": "delivered",
        "sourceHead": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "destinationRef": "refs/cowshed/raven/heads/topic"
    });
    assert_eq!(serde_json::to_value(&delivered).unwrap(), wire);
    assert_eq!(
        serde_json::from_value::<LandReport>(wire).unwrap(),
        delivered
    );
    assert!(
        serde_json::from_value::<LandReport>(json!({
            "kind": "delivered",
            "sourceHead": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "destinationRef": "refs/cowshed/raven/heads/topic",
            "retired": true
        }))
        .is_err(),
        "a delivery cannot pretend it retired a workspace"
    );

    // A passed preflight answers the land's three pins; a missing target branch is `missing`.
    let preflighted = LandReport::Preflighted(LandPreflight {
        workspace_incarnation: incarnation(),
        source_head: oid('b'),
        target_branch: "main".into(),
        target_head: ExpectedRefHead::Missing,
    });
    let wire = json!({
        "kind": "preflighted",
        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
        "sourceHead": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "targetBranch": "main",
        "targetHead": {"missing": true}
    });
    assert_eq!(serde_json::to_value(&preflighted).unwrap(), wire);
    assert_eq!(
        serde_json::from_value::<LandReport>(wire).unwrap(),
        preflighted
    );
    // Only a preflight names it; an ordinary land's options travel as before.
    assert_eq!(
        serde_json::to_value(LandOptions {
            preflight: true,
            ..LandOptions::default()
        })
        .unwrap(),
        json!({"retire":true,"pushOnly":false,"preflight":true})
    );

    let status = GatewayStatus {
        installed: true,
        running: true,
        socket: PathBuf::from("/store/gateway.sock"),
        cli_version: "1.4.0".into(),
        daemon_version: Some("1.3.0".into()),
        active_workspaces: 2,
        drain_cause: None,
        healing: None,
        recovering: None,
        stale_daemon: None,
        pending_install: false,
        land_turns: Vec::new(),
        land_turns_error: None,
        scheduling: None,
    };
    assert_eq!(
        serde_json::to_value(&status).unwrap(),
        json!({"installed":true,"running":true,"socket":"/store/gateway.sock","cliVersion":"1.4.0","daemonVersion":"1.3.0","activeWorkspaces":2})
    );
    // An install that holds the slot says so; a free slot says nothing.
    let pending = GatewayStatus {
        pending_install: true,
        ..status.clone()
    };
    assert_eq!(
        serde_json::to_value(&pending).unwrap()["pendingInstall"],
        json!(true)
    );
    assert!(
        serde_json::to_value(&status)
            .unwrap()
            .get("pendingInstall")
            .is_none()
    );
    assert_eq!(
        serde_json::from_value::<GatewayStatus>(serde_json::to_value(&pending).unwrap()).unwrap(),
        pending
    );
    // A daemon still recovering the supervisors from before it started says how many, never 0.
    let recovering = GatewayStatus {
        recovering: Some(cowshed_gateway_types::SupervisorRecovery {
            supervisors: std::num::NonZeroUsize::new(3).expect("three"),
        }),
        ..status.clone()
    };
    assert_eq!(
        serde_json::to_value(recovering).unwrap()["recovering"],
        json!({"supervisors":3})
    );
    assert!(
        serde_json::from_value::<GatewayStatus>(json!({
            "installed":true,"running":true,"socket":"/s","cliVersion":"1.4.0",
            "activeWorkspaces":0,"recovering":{"supervisors":0}
        }))
        .is_err(),
        "a recovery with nothing left to recover is no recovery"
    );
    // A daemon still in its startup pass says where it is: how many projects it still mounts,
    // never 0, or that it is restoring sessions.
    let mounting = GatewayStatus {
        healing: Some(cowshed_gateway_types::StartupHeal::Mounting {
            projects: std::num::NonZeroUsize::new(2).expect("two"),
        }),
        ..status.clone()
    };
    assert_eq!(
        serde_json::to_value(mounting).unwrap()["healing"],
        json!({"mounting":{"projects":2}})
    );
    let restoring = GatewayStatus {
        healing: Some(cowshed_gateway_types::StartupHeal::RestoringSessions),
        ..status
    };
    assert_eq!(
        serde_json::to_value(restoring).unwrap()["healing"],
        json!("restoringSessions")
    );
    assert!(
        serde_json::from_value::<GatewayStatus>(json!({
            "installed":true,"running":true,"socket":"/s","cliVersion":"1.4.0",
            "activeWorkspaces":0,"healing":{"mounting":{"projects":0}}
        }))
        .is_err(),
        "a pass with no project left to mount is restoring sessions, not mounting none"
    );
    let unmeasured: GatewayStatus = serde_json::from_value(json!({
        "installed":false,
        "running":false,
        "socket":"/store/gateway.sock",
        "cliVersion":"1.4.0",
        "activeWorkspaces":0
    }))
    .expect("status without invented cache counters");
    assert_eq!(
        serde_json::to_value(unmeasured).unwrap(),
        json!({"installed":false,"running":false,"socket":"/store/gateway.sock","cliVersion":"1.4.0","activeWorkspaces":0})
    );
    // Nothing in the gateway control protocol reports cache occupancy, so a counter here could
    // only ever be the constant 0 the CLI used to send — a number that says "empty" when the fact
    // is "never measured". `deny_unknown_fields` is what keeps it from coming back.
    for invented in [
        json!({"installed":false,"running":false,"socket":"/s","cliVersion":"1.4.0","activeWorkspaces":0,"cacheEntries":0}),
        json!({"installed":false,"running":false,"socket":"/s","cliVersion":"1.4.0","activeWorkspaces":0,"cacheBytes":0}),
    ] {
        assert!(
            serde_json::from_value::<GatewayStatus>(invented).is_err(),
            "an unmeasured cache counter is not a gateway status field"
        );
    }

    let audit = AuditEvent {
        timestamp: timestamp(),
        repo_id: repo(),
        workspace_incarnation: incarnation(),
        workspace: workspace(),
        action: "egress".into(),
        decision: AuditDecision::Denied,
        reason: Some("host not granted".into()),
        trace: trace(),
    };
    assert_eq!(serde_json::to_value(audit).unwrap()["decision"], "denied");
}

#[test]
fn json_envelope_has_exact_discriminated_success_and_failure_shapes() {
    let success = JsonEnvelope::success(MountResult {
        workspace: workspace(),
        mount: PathBuf::from("/mnt/raven"),
        base_commit: Some(oid('a')),
    });
    assert_eq!(
        serde_json::to_value(&success).unwrap(),
        json!({"ok":true,"result":{"workspace":"raven","mount":"/mnt/raven","baseCommit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}})
    );
    assert_eq!(
        serde_json::to_value(JsonEnvelope::success(EmptyResult {})).unwrap(),
        json!({"ok":true,"result":{}})
    );

    let failure: JsonEnvelope<EmptyResult> = JsonEnvelope::failure(CowshedError::new(
        ErrorCode::Conflict,
        "workspace raven already exists",
        "cowshed ls",
    ));
    assert_eq!(
        serde_json::to_value(&failure).unwrap(),
        json!({
            "ok":false,
            "error":{"code":"conflict","message":"workspace raven already exists","hint":"cowshed ls"}
        })
    );

    for invalid in [
        json!({"ok":true,"error":{"code":"conflict","message":"x","hint":"y"}}),
        json!({"ok":false,"result":{}}),
        json!({"ok":true,"result":{},"cmd":"new"}),
        json!({"ok":"true","result":{}}),
    ] {
        assert!(serde_json::from_value::<JsonEnvelope<EmptyResult>>(invalid).is_err());
    }
}

/// Each capability probe case: a crate name, a snippet that must not compile, and what its errors
/// must name.
const CAPABILITY_CASES: [(&str, &str, &[&str]); 9] = [
    (
        "project_authority",
        "use cowshed_core::Project;\nfn deny(value: &Project) { value.attach(); value.exec(); value.grant(); value.gc(); }\n",
        &["attach", "exec", "grant", "gc"],
    ),
    (
        "worker_authority",
        "use cowshed_core::WorkspaceHandle;\nfn deny(value: &WorkspaceHandle) { value.grant(); value.revoke(); value.destroy(); value.rebase(); value.land(); value.gc(); value.repo_mirror(); value.detach(); value.workspace(\"other\"); }\n",
        &[
            "grant",
            "revoke",
            "destroy",
            "rebase",
            "land",
            "gc",
            "repo_mirror",
            "detach",
            "workspace",
        ],
    ),
    (
        "token_traits",
        "use cowshed_core::CoordinatorToken;\nfn must_clone<T: Clone>() {} fn must_serialize<T: serde::Serialize>() {}\nfn deny() { must_clone::<CoordinatorToken>(); must_serialize::<CoordinatorToken>(); }\n",
        &["Clone", "Serialize"],
    ),
    (
        "private_construction",
        "use cowshed_core::{CoordinatorToken,Cowshed,Project,WorkspaceHandle};\nfn deny() { let _ = Cowshed {}; let _ = Project {}; let _ = WorkspaceHandle {}; let _ = CoordinatorToken {}; }\n",
        &[
            "Cowshed",
            "Project",
            "WorkspaceHandle",
            "CoordinatorToken",
            "private",
        ],
    ),
    (
        "port_block_construction",
        "use cowshed_core::api::PortBlock;\nfn deny() { let _ = PortBlock { base: 40960, size: 16 }; }\n",
        &["PortBlock", "private"],
    ),
    (
        "null_success",
        "use cowshed_core::api::JsonEnvelope;\nfn deny() { let _ = JsonEnvelope::success(()); }\n",
        &["success", "Sealed"],
    ),
    (
        "worker_operation_authority",
        "use cowshed_core::WorkspaceHandle;\nuse cowshed_core::api::call::{Arguments, call};\nuse cowshed_core::api::operations::{CoordinatorDestroy, WorkspaceAttach};\npub async fn deny(value: &WorkspaceHandle) { let _ = call::<CoordinatorDestroy, _>(value, Arguments::new()).await; let _ = call::<WorkspaceAttach, _>(value, Arguments::new()).await; }\n",
        &[
            "Serves<cowshed_core::api::operations::CoordinatorDestroy>` is not implemented for `WorkspaceHandle`",
            "Serves<cowshed_core::api::operations::WorkspaceAttach>` is not implemented for `WorkspaceHandle`",
        ],
    ),
    (
        "workspace_ref_operation_authority",
        "use cowshed_core::WorkspaceRef;\nuse cowshed_core::api::call::{Arguments, call_upload};\nuse cowshed_core::api::operations::WorkerExec;\npub async fn deny(value: &WorkspaceRef) { let _ = call_upload::<WorkerExec, _>(value, Arguments::new(), None).await; }\n",
        &[
            "Serves<cowshed_core::api::operations::WorkerExec>` is not implemented for `WorkspaceRef`",
        ],
    ),
    (
        "forged_operation",
        "use cowshed_core::JobHandle;\nuse cowshed_core::api::call::Serves;\nuse cowshed_core::api::operations::{JobRequest, Lane, Operation, OperationRequest, Scope};\npub enum Forged {}\nimpl Operation for Forged { const METHOD: &'static str = \"coordinator.destroy\"; const SCOPE: Scope = Scope::Worker; const LANE: Lane = Lane::Json; type Request = JobRequest; type Result = (); fn request(request: JobRequest) -> OperationRequest { OperationRequest::JobKill(request) } }\nimpl Serves<Forged> for JobHandle { const BOUND: &'static [&'static str] = &[]; }\n",
        &["`Forged: operations::sealed::Sealed` is not satisfied"],
    ),
];

/// Writes the capability probe workspace, one crate per case holding `source(name)`, and checks it
/// with cargo in the OUTER target directory.
///
/// Compiled by cargo, not by a hand-assembled rustc command line.
///
/// This test used to run rustc itself against the newest `libcowshed_core-*.rlib` in
/// `<target>/debug/deps`, to avoid a path-dependency `cargo check` rebuilding cowshed-core. Two
/// toolchain changes made that technique impossible rather than merely fragile:
///
/// * cargo no longer writes package rlibs into `<target>/<profile>/deps`. They live in
///   `<target>/<profile>/build/<pkg>/<hash>/out/`, and dependencies are found through one
///   `-L dependency=` per package rather than a single shared directory.
/// * rustc now emits rlibs carrying only a metadata *stub*, with the full metadata in a sibling
///   `.rmeta`. `--extern name=<rlib>` fails with "only metadata stub found", and reproducing
///   cargo's rlib/rmeta pairing by hand does not resolve either.
///
/// Reconstructing cargo's dependency search path here would have to be redone at every layout
/// change, and while it was broken this gate passed nothing and proved nothing. Asking cargo
/// cannot go stale.
///
/// The check shares the OUTER target directory. A probe-private one costs a full cold dependency
/// check of cowshed-core's whole tree — measured at 48s on an idle M5 Max, ~110s of CPU, and 927MB
/// of duplicated artifacts. Sharing reuses the build scripts and proc macros `cargo test
/// --workspace --no-run` already built, and keeps what one probe check compiled for the next.
///
/// The outer cargo owning this directory is not a conflict. nextest builds through cargo and then
/// runs the test binaries itself, releasing the build lock before execution, so a `cargo check`
/// against the shared directory while this suite is running does not block — measured at 0.401s
/// mid-run. Fingerprints keep the probe's artifacts distinct from the outer build's rather than
/// evicting them, so neither invalidates the other.
///
/// If a stale-artifact failure ever reappears here, this is what it looks like: a poisoned rlib
/// left in the legacy `deps` directory, written by sccache with 0640 permissions on a cache hit,
/// compiled by a rustc that is no longer the one on PATH.
///
/// Every case is its own crate, so each is checked in a rustc session of its own: rustc skips its
/// privacy pass once type checking failed, so cases sharing a crate would hide each other's
/// later-phase errors. The crates are members of ONE probe workspace checked by ONE cargo with
/// `--keep-going`; a cargo per case paid resolution and the fingerprint walk of cowshed-core's
/// whole tree once per case. The outer lockfile pins the probe to the versions the outer build
/// compiled, so resolution is a lookup.
///
/// The probe's manifests, lockfile and shared target are the same whatever `source` returns, so
/// dependencies fingerprint the same for the warm-up and the test. Only the case crates live in a
/// fresh run-owned directory: an archived test binary must never clear its producer's fixture.
fn check_capability_probe(source: impl Fn(&str) -> &'static str) -> std::process::Output {
    let probe = TempRoot::new_in(&std::env::temp_dir(), "cowshed-capability-probe");
    let manifest_dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR")
            .expect("cargo sets CARGO_MANIFEST_DIR for the tests it runs"),
    );
    let lockfile = manifest_dir
        .ancestors()
        .map(|directory| directory.join("Cargo.lock"))
        .find(|lockfile| lockfile.is_file())
        .expect("the workspace lockfile");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    // Ask Cargo in the executing checkout, not the checkout that compiled this test binary.
    // Metadata honors its runtime target configuration without building any dependencies.
    let metadata = Command::new(&cargo)
        .args([
            "metadata",
            "--no-deps",
            "--offline",
            "--format-version",
            "1",
        ])
        .current_dir(&manifest_dir)
        .output_bounded()
        .unwrap_or_else(|error| panic!("cargo metadata in {}: {error}", manifest_dir.display()));
    assert!(
        metadata.status.success(),
        "cargo metadata in {}: {}",
        manifest_dir.display(),
        String::from_utf8_lossy(&metadata.stderr)
    );
    let metadata: serde_json::Value =
        serde_json::from_slice(&metadata.stdout).expect("cargo metadata JSON");
    let target = PathBuf::from(
        metadata["target_directory"]
            .as_str()
            .expect("cargo metadata target_directory"),
    );
    // The owned scratch root has no repository config ancestors. Forward the files a Cargo
    // started in cowshed-core's runtime directory reads, shallowest first: a later `--config`
    // wins, as a deeper file does. This preserves checksum freshness, linker and `[env]` in
    // the one Cargo environment (16_build_volumes.md, "One Cargo environment on every path").
    let mut workspace_configs: Vec<PathBuf> = manifest_dir
        .ancestors()
        .map(|directory| directory.join(".cargo").join("config.toml"))
        .filter(|config| config.is_file())
        .collect();
    workspace_configs.reverse();
    // The probe's own `[workspace]` detaches it from the outer workspace, whose members it must
    // not join and whose lints and profiles it must not inherit — each snippet is meant to fail on
    // visibility, and nothing else. `serde` is here because one snippet names a serde trait to
    // prove a sealed token does not implement it; without it that snippet would fail on an
    // unresolved crate and prove nothing.
    let members: Vec<String> = CAPABILITY_CASES
        .iter()
        .map(|(name, ..)| format!("{name:?}"))
        .collect();
    fs::write(
        probe.join("Cargo.toml"),
        format!(
            "[workspace]\nresolver = \"3\"\nmembers = [{}]\n",
            members.join(", ")
        ),
    )
    .expect("probe workspace manifest");
    fs::copy(&lockfile, probe.join("Cargo.lock")).expect("probe lockfile");
    for (name, ..) in CAPABILITY_CASES {
        let crate_dir = probe.join(name);
        fs::create_dir_all(crate_dir.join("src")).expect("probe crate directory");
        fs::write(
            crate_dir.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\
                 publish = false\n\n[lib]\npath = \"src/lib.rs\"\n\n\
                 [dependencies]\ncowshed-core = {{ path = {manifest_dir:?} }}\n\
                 serde = {{ version = \"1\", features = [\"derive\"] }}\n"
            ),
        )
        .expect("probe manifest");
        fs::write(crate_dir.join("src/lib.rs"), source(name)).expect("probe source");
    }
    Command::new(&cargo)
        .args(
            workspace_configs
                .iter()
                .flat_map(|config| [std::ffi::OsStr::new("--config"), config.as_os_str()]),
        )
        .arg("check")
        .arg("--workspace")
        .arg("--keep-going")
        .arg("--offline")
        .arg("--message-format=short")
        .arg("--target-dir")
        .arg(&target)
        .current_dir(&*probe)
        .output_bounded()
        .unwrap_or_else(|error| panic!("cargo check: {error}"))
}

/// Exercise our Cargo CLI wiring without rebuilding Cargo's dependency tree. A child test process
/// relocates the manifest and scratch parent at runtime, just as an archived nextest binary does.
#[cfg(unix)]
#[test]
fn capability_probe_owns_runtime_scratch_and_reuses_runtime_target() {
    const FIXTURE: &str = "COWSHED_CAPABILITY_PROBE_FIXTURE";
    if std::env::var_os(FIXTURE).is_some() {
        for _ in 0..2 {
            let output = check_capability_probe(|_| "");
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    }

    use std::os::unix::fs::PermissionsExt as _;
    let fixture = TempRoot::new_in(&std::env::temp_dir(), "cowshed-capability-relocation");
    let consumer = fixture.join("consumer");
    let manifest = consumer.join("crates/probe-owner");
    let scratch = fixture.join("scratch");
    let target = consumer.join("configured-target");
    let producer = fixture.join("producer-target/tmp/capability-probe");
    let configs = [
        consumer.join(".cargo/config.toml"),
        manifest.join(".cargo/config.toml"),
    ];
    for directory in [&scratch, &producer] {
        fs::create_dir_all(directory).unwrap();
    }
    for config in &configs {
        fs::create_dir_all(config.parent().unwrap()).unwrap();
        fs::write(config, "# runtime checkout config\n").unwrap();
    }
    fs::write(consumer.join("Cargo.lock"), "# runtime checkout lock\n").unwrap();
    fs::write(producer.join("keep"), "producer-owned bytes").unwrap();
    fs::write(
        fixture.join("metadata.json"),
        serde_json::to_vec(&json!({ "target_directory": target })).unwrap(),
    )
    .unwrap();
    let cargo = fixture.join("cargo");
    fs::write(
        &cargo,
        r#"#!/bin/sh
set -eu
fixture="$COWSHED_CAPABILITY_PROBE_FIXTURE"
if [ "$1" = metadata ]; then
    printf '%s\n' "$PWD" >> "$fixture/metadata-cwds"
    cat "$fixture/metadata.json"
else
    printf '%s\n' "$PWD" >> "$fixture/probe-roots"
    printf '%s\0' "$@" >> "$fixture/check-args"
    cp Cargo.lock "$fixture/probe-lock"
    cp private_construction/Cargo.toml "$fixture/probe-manifest"
fi
"#,
    )
    .unwrap();
    fs::set_permissions(&cargo, fs::Permissions::from_mode(0o700)).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "capability_probe_owns_runtime_scratch_and_reuses_runtime_target",
            "--nocapture",
        ])
        .env(FIXTURE, &*fixture)
        .env("CARGO", &cargo)
        .env("CARGO_MANIFEST_DIR", &manifest)
        .env("CARGO_TARGET_TMPDIR", producer.parent().unwrap())
        .env("TMPDIR", &scratch)
        .output_bounded()
        .expect("run relocated capability probe");
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let roots = fs::read_to_string(fixture.join("probe-roots")).unwrap();
    let roots: Vec<PathBuf> = roots.lines().map(PathBuf::from).collect();
    assert_eq!(roots.len(), 2);
    assert_ne!(roots[0], roots[1]);
    for root in roots {
        assert_eq!(root.parent(), Some(scratch.as_path()));
        assert!(!root.exists(), "run-owned root {} survived", root.display());
    }
    let metadata_cwds = fs::read_to_string(fixture.join("metadata-cwds")).unwrap();
    assert_eq!(
        metadata_cwds.lines().collect::<Vec<_>>(),
        vec![manifest.to_str().unwrap(); 2]
    );
    let mut expected: Vec<OsString> = configs
        .iter()
        .flat_map(|config| [OsString::from("--config"), config.as_os_str().to_owned()])
        .collect();
    expected.extend(
        [
            "check",
            "--workspace",
            "--keep-going",
            "--offline",
            "--message-format=short",
            "--target-dir",
        ]
        .map(OsString::from),
    );
    expected.push(target.into_os_string());
    let arguments = fs::read(fixture.join("check-args")).unwrap();
    let arguments: Vec<OsString> = arguments
        .split_inclusive(|byte| *byte == 0)
        .map(|argument| OsString::from_vec(argument[..argument.len() - 1].to_vec()))
        .collect();
    assert_eq!(arguments.len(), expected.len() * 2);
    for invocation in arguments.chunks(expected.len()) {
        assert_eq!(invocation, expected);
    }
    assert_eq!(
        fs::read(fixture.join("probe-lock")).unwrap(),
        b"# runtime checkout lock\n"
    );
    assert!(
        fs::read_to_string(fixture.join("probe-manifest"))
            .unwrap()
            .contains(&format!("path = {manifest:?}"))
    );
    assert_eq!(
        fs::read(producer.join("keep")).unwrap(),
        b"producer-owned bytes"
    );
}

/// The capability probe's build step: everything the probe checks except its snippets, which is
/// cowshed-core's whole dependency tree as the probe resolves it. On a cold CI runner that check
/// takes longer than the bounded window of the test below (measured over 100s on a hosted arm64
/// macOS runner, where it timed the release candidate out), and none of it is under test. So
/// each exceptional shard runs this first as its bounded-exec fixture segment, in a build
/// budget, and the test's window covers the case crates alone. Ignored, because it
/// proves nothing a test should: every crate is empty and must check.
#[test]
#[ignore = "build step for the capability probe; the exceptional shards run it as their fixture segment"]
fn capability_probe_dependencies_check() {
    let output = check_capability_probe(|_| "");
    assert!(
        output.status.success(),
        "the capability probe's dependencies did not check:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn lesser_capabilities_fail_to_compile_with_coordinator_authority() {
    let output = check_capability_probe(|name| {
        CAPABILITY_CASES
            .iter()
            .find(|(case, ..)| *case == name)
            .map(|(_, source, _)| *source)
            .expect("a capability case")
    });
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "every capability probe unexpectedly compiled:\n{diagnostic}"
    );
    for (name, _, expected) in CAPABILITY_CASES {
        // `--message-format=short` puts each diagnostic on one line headed by its file, relative
        // to the probe workspace.
        let file = format!("{name}/src/lib.rs:");
        let errors: Vec<&str> = diagnostic
            .lines()
            .filter(|line| line.starts_with(&file) && line.contains(": error"))
            .collect();
        assert!(!errors.is_empty(), "{name} compiled:\n{diagnostic}");
        let errors = errors.join("\n");
        for expected in expected {
            assert!(
                errors.contains(expected),
                "{name} did not fail on {expected}:\n{errors}"
            );
        }
    }
}

/// The scheduling facts of a job are explicit on its JSON projection: the class and scope it was
/// admitted under always, and the nice its command was observed at only once there is
/// something to say: never null, never a fabricated zero.
#[test]
fn job_info_reports_admitted_scheduling_and_observations_exactly() {
    let info = finished_job(None);
    let value = serde_json::to_value(&info).expect("job JSON");
    assert_eq!(value["class"], json!("background"));
    assert_eq!(value["scope"], json!("workspace"));
    assert_eq!(value["nice"], json!(10));

    let mut unobserved = info.clone();
    unobserved.nice = None;
    let value = serde_json::to_value(&unobserved).expect("unobserved job JSON");
    assert!(value.get("nice").is_none());
    assert_eq!(
        serde_json::from_value::<JobInfo>(value).expect("decode"),
        unobserved
    );

    // A gate's observed nice is what the supervisor set: zero is a real observation.
    let mut gate = info;
    gate.class = ExecClass::Gate;
    gate.nice = Some(0);
    let value = serde_json::to_value(&gate).expect("gate job JSON");
    assert_eq!(value["class"], json!("gate"));
    assert_eq!(value["nice"], json!(0));
    assert_eq!(
        serde_json::from_value::<JobInfo>(value).expect("decode"),
        gate
    );
}

/// A job's class and scope are required on the wire, each refuses an unknown spelling, a host
/// job may be of any class, and a queued job has observed no nice.
#[test]
fn job_info_requires_its_class_and_scope_and_refuses_impossible_scheduling() {
    let info = finished_job(None);
    let value = serde_json::to_value(&info).expect("job JSON");
    for required in ["class", "scope"] {
        let mut omitted = value.clone();
        omitted.as_object_mut().unwrap().remove(required);
        assert!(
            serde_json::from_value::<JobInfo>(omitted).is_err(),
            "{required} is required"
        );
    }
    for (field, invalid) in [
        ("class", json!("foreground")),
        ("scope", json!("host")),
        ("nice", json!("10")),
    ] {
        let mut changed = value.clone();
        changed[field] = invalid;
        assert!(
            serde_json::from_value::<JobInfo>(changed).is_err(),
            "{field} refuses its invalid spelling"
        );
    }

    let mut host = info.clone();
    host.scope = ExecScope::HostGate;
    host.validate()
        .expect("background work runs at the host scope");
    host.class = ExecClass::Gate;
    host.validate().expect("a gate runs at the host scope");

    let mut queued = info;
    queued.state = JobState::Queued;
    queued.duration_ms = None;
    queued.exit = None;
    queued.resources = None;
    queued.pid = None;
    assert!(
        queued.validate().is_err(),
        "a queued job owns no command to observe a nice for"
    );
    queued.nice = None;
    queued
        .validate()
        .expect("a queued job validates without an observed nice");
}

/// A sealed record keeps the scheduling facts after the supervisor that ran the job is gone, and
/// the two CPU failures a job can end with have their exact wire names.
#[test]
fn sealed_job_and_cpu_failures_keep_their_exact_wire_spellings() {
    let sealed = SealedJob {
        job_id: JobId::new(7).unwrap(),
        state: JobState::Failed,
        exit: None,
        failure: Some(JobFailure::Priority),
        duration_ms: Some(3),
        resources: None,
        output_limit: None,
        stdout: stream(".cowshed/job/7/out", 0, ""),
        stderr: stream(".cowshed/job/7/err", 0, ""),
        class: ExecClass::Gate,
        scope: ExecScope::HostGate,
        nice: None,
        containment: None,
        outlived: Vec::new(),
        outlived_incomplete: false,
    };
    let value = serde_json::to_value(&sealed).expect("sealed JSON");
    assert_eq!(value["failure"], json!("priority"));
    assert_eq!(value["class"], json!("gate"));
    assert_eq!(value["scope"], json!("hostGate"));
    assert!(value.get("nice").is_none());
    assert_eq!(
        serde_json::from_value::<SealedJob>(value).expect("decode"),
        sealed
    );

    assert_eq!(
        serde_json::to_value(JobFailure::CpuAdmission).unwrap(),
        json!("cpuAdmission")
    );
    assert_eq!(
        serde_json::to_value(JobFailure::Priority).unwrap(),
        json!("priority")
    );
    assert_eq!(
        serde_json::to_value(JobFailure::Unlaunched).unwrap(),
        json!("unlaunched")
    );
    assert_eq!(
        serde_json::to_value(ExecScope::default()).unwrap(),
        json!("workspace")
    );
}

/// What retiring a job's process membership found is stated exactly on the wire, and only where
/// the job can have one: absent, never `kernel` and never an empty list, for a job that retired
/// nothing; a receipt the job cannot have is refused in both directions; and `draining` is a state
/// no job ends in.
#[test]
fn a_job_states_its_retirement_receipt_exactly_and_only_where_it_can_have_one() {
    let observed = JobContainment::Observed {
        lost: vec![ProcessCoverageGap::UncountedFork { pid: 4242 }],
    };
    let survivors = vec![
        OutlivedProcess {
            index: Some(1),
            pid: 4243,
            image: Some("/bin/sleep".into()),
            signal: 9,
        },
        OutlivedProcess {
            index: None,
            pid: 4244,
            image: None,
            signal: 9,
        },
    ];

    // No receipt is absent on the wire, and an absent one decodes as none, never as `kernel`.
    let mut info = finished_job(None);
    let bare = serde_json::to_value(&info).expect("job JSON");
    assert!(bare.get("containment").is_none());
    assert!(bare.get("outlived").is_none());
    let read = serde_json::from_value::<JobInfo>(bare).expect("a job without a receipt decodes");
    assert_eq!((read.containment, read.outlived), (None, Vec::new()));

    info.containment = Some(observed.clone());
    info.outlived = survivors.clone();
    let value = serde_json::to_value(&info).expect("retired job JSON");
    assert_eq!(
        value["containment"],
        json!({"kind": "observed", "lost": [{"kind": "uncountedFork", "pid": 4242}]})
    );
    assert_eq!(
        value["outlived"],
        json!([
            {"index": 1, "pid": 4243, "image": "/bin/sleep", "signal": 9},
            {"pid": 4244, "signal": 9},
        ]),
        "an index or image nobody observed is absent: unknown, never zero or empty"
    );
    assert_eq!(
        serde_json::from_value::<JobInfo>(value).expect("decode"),
        info
    );

    // A receipt that found nothing alive states its containment and no survivors.
    info.containment = Some(JobContainment::Kernel);
    info.outlived = Vec::new();
    let value = serde_json::to_value(&info).expect("kernel job JSON");
    assert_eq!(value["containment"], json!({"kind": "kernel"}));
    assert!(value.get("outlived").is_none());
    assert_eq!(
        serde_json::from_value::<JobInfo>(value).expect("decode"),
        info
    );

    // Draining is no end: the leader is gone and its membership is being retired.
    for state in [JobState::Queued, JobState::Running, JobState::Draining] {
        assert!(!state.is_terminal(), "{state:?} is no end");
    }
    for state in [
        JobState::Exited,
        JobState::Signaled,
        JobState::Killed,
        JobState::OutputLimit,
        JobState::Failed,
    ] {
        assert!(state.is_terminal(), "{state:?} is an end");
    }
    assert_eq!(
        serde_json::to_value(JobState::Draining).unwrap(),
        json!("draining")
    );
    let mut draining = finished_job(None);
    draining.state = JobState::Draining;
    draining.duration_ms = None;
    draining.exit = None;
    draining
        .validate()
        .expect("a draining job owns a process and has no end");
    draining.containment = Some(JobContainment::Kernel);
    draining
        .validate()
        .expect("a draining job may already hold its proof");
    let mut ended = draining.clone();
    ended.exit = Some(ExitStatus::Exited { code: 0 });
    assert!(ended.validate().is_err(), "a draining job has no exit yet");
    let mut ended = draining;
    ended.duration_ms = Some(1);
    assert!(ended.validate().is_err(), "a draining job has no duration");

    // A job that has retired nothing, or never owned a process, has no receipt to state.
    for state in [JobState::Queued, JobState::Running] {
        let mut early = finished_job(None);
        early.state = state;
        early.duration_ms = None;
        early.exit = None;
        early.resources = None;
        early.nice = None;
        early
            .validate()
            .unwrap_or_else(|error| panic!("a {state:?} job: {error}"));
        early.containment = Some(JobContainment::Kernel);
        assert!(
            early.validate().is_err(),
            "a {state:?} job has retired nothing"
        );
    }
    let mut refused = finished_job(None);
    refused.state = JobState::Failed;
    refused.pid = None;
    refused.exit = None;
    refused.resources = None;
    refused.nice = None;
    refused
        .validate()
        .expect("a start refused before its command ran states no receipt");
    refused.containment = Some(JobContainment::Kernel);
    assert!(
        refused.validate().is_err(),
        "a job that never owned a process retired no membership"
    );

    // Survivors need the containment that found them, and name an image or none.
    let mut orphaned = finished_job(None);
    orphaned.outlived = survivors.clone();
    assert!(orphaned.validate().is_err());
    let mut wire = serde_json::to_value(finished_job(None)).expect("job JSON");
    wire["outlived"] = json!([{"pid": 4243, "signal": 9}]);
    assert!(
        serde_json::from_value::<JobInfo>(wire).is_err(),
        "survivors without their containment are refused on decode too"
    );
    let mut unnamed = finished_job(None);
    unnamed.containment = Some(JobContainment::Kernel);
    unnamed.outlived = vec![OutlivedProcess {
        image: Some(String::new()),
        ..survivors[0].clone()
    }];
    assert!(
        unnamed.validate().is_err(),
        "an empty image stands for no image, which is absent"
    );

    // The sealed projection carries the same two facts, absent when there is no receipt.
    let sealed = SealedJob {
        job_id: JobId::new(7).unwrap(),
        state: JobState::Signaled,
        exit: Some(ExitStatus::Signaled {
            signal: 9,
            core_dumped: false,
        }),
        failure: None,
        duration_ms: Some(3),
        resources: None,
        output_limit: None,
        stdout: stream(".cowshed/job/7/out", 0, ""),
        stderr: stream(".cowshed/job/7/err", 0, ""),
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: None,
        containment: Some(observed.clone()),
        outlived: survivors.clone(),
        outlived_incomplete: false,
    };
    let value = serde_json::to_value(&sealed).expect("sealed JSON");
    assert_eq!(value["containment"]["kind"], json!("observed"));
    assert_eq!(value["outlived"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        serde_json::from_value::<SealedJob>(value).expect("decode"),
        sealed
    );
    let unreceipted = SealedJob {
        containment: None,
        outlived: Vec::new(),
        outlived_incomplete: false,
        ..sealed
    };
    let value = serde_json::to_value(&unreceipted).expect("sealed JSON");
    assert!(value.get("containment").is_none());
    assert!(value.get("outlived").is_none());
    assert_eq!(
        serde_json::from_value::<SealedJob>(value).expect("decode"),
        unreceipted
    );

    // The retirement itself is one value: its containment is tagged, and an observed one names
    // what it lacks even when that is nothing.
    let retirement = JobProcessRetirement {
        containment: observed,
        outlived: survivors,
        outlived_incomplete: false,
    };
    let value = serde_json::to_value(&retirement).expect("retirement JSON");
    assert_eq!(value["containment"]["kind"], json!("observed"));
    assert_eq!(
        serde_json::from_value::<JobProcessRetirement>(value).expect("decode"),
        retirement
    );
    assert!(
        serde_json::from_value::<JobContainment>(json!({"kind": "observed"})).is_err(),
        "an observed containment states its lost list, empty or not"
    );
}

/// An empty survivor list with incomplete metadata is a real receipt, not evidence of no kills.
/// The marker defaults to false and only true crosses the public wire.
#[test]
fn incomplete_retirement_metadata_round_trips_and_refuses_impossible_job_receipts() {
    let mut info = finished_job(None);
    info.containment = Some(JobContainment::Kernel);
    info.outlived_incomplete = true;
    let value = serde_json::to_value(&info).unwrap();
    assert_eq!(value["outlivedIncomplete"], json!(true));
    assert!(value.get("outlived").is_none());
    assert_eq!(
        serde_json::from_value::<JobInfo>(value.clone()).unwrap(),
        info
    );

    info.outlived_incomplete = false;
    let complete = serde_json::to_value(&info).unwrap();
    assert!(complete.get("outlivedIncomplete").is_none());
    assert!(
        !serde_json::from_value::<JobInfo>(complete)
            .unwrap()
            .outlived_incomplete
    );

    for state in [JobState::Queued, JobState::Running] {
        let mut early = info.clone();
        early.state = state;
        early.duration_ms = None;
        early.exit = None;
        early.resources = None;
        early.nice = None;
        early.outlived_incomplete = true;
        assert!(early.validate().is_err());
        assert!(serde_json::to_value(&early).is_err());
        let mut wire = value.clone();
        wire["state"] = serde_json::to_value(state).unwrap();
        wire.as_object_mut().unwrap().remove("durationMs");
        wire.as_object_mut().unwrap().remove("exit");
        wire.as_object_mut().unwrap().remove("resources");
        wire.as_object_mut().unwrap().remove("nice");
        assert!(serde_json::from_value::<JobInfo>(wire).is_err());
    }
    let mut draining = info.clone();
    draining.state = JobState::Draining;
    draining.duration_ms = None;
    draining.exit = None;
    draining.outlived_incomplete = true;
    let wire = serde_json::to_value(&draining).unwrap();
    assert_eq!(serde_json::from_value::<JobInfo>(wire).unwrap(), draining);

    // A refused/never-born job cannot acquire a receipt just by claiming metadata was missed.
    let mut never_born = info.clone();
    never_born.state = JobState::Failed;
    never_born.pid = None;
    never_born.exit = None;
    never_born.resources = None;
    never_born.nice = None;
    never_born.containment = None;
    never_born.validate().unwrap();
    let never_born_wire = serde_json::to_value(&never_born).unwrap();
    for containment in [None, Some(JobContainment::Kernel)] {
        never_born.containment = containment.clone();
        never_born.outlived_incomplete = true;
        assert!(never_born.validate().is_err());
        assert!(serde_json::to_value(&never_born).is_err());
        let mut wire = never_born_wire.clone();
        wire["outlivedIncomplete"] = json!(true);
        if let Some(containment) = containment {
            wire["containment"] = serde_json::to_value(containment).unwrap();
        }
        assert!(serde_json::from_value::<JobInfo>(wire).is_err());
    }
    info.containment = None;
    info.outlived_incomplete = true;
    assert!(info.validate().is_err());
    assert!(serde_json::to_value(info).is_err());
    let mut orphaned = value.clone();
    orphaned.as_object_mut().unwrap().remove("containment");
    assert!(serde_json::from_value::<JobInfo>(orphaned).is_err());
    for bad_marker in [json!(null), json!("true"), json!(1)] {
        let mut malformed = value.clone();
        malformed["outlivedIncomplete"] = bad_marker;
        assert!(serde_json::from_value::<JobInfo>(malformed).is_err());
    }
}

#[test]
fn sealed_receipts_preserve_incomplete_metadata_and_refuse_orphaned_markers() {
    let sealed = SealedJob {
        job_id: JobId::new(7).unwrap(),
        state: JobState::Signaled,
        exit: Some(ExitStatus::Signaled {
            signal: 9,
            core_dumped: false,
        }),
        failure: None,
        duration_ms: Some(3),
        resources: None,
        output_limit: None,
        stdout: stream(".cowshed/job/7/out", 0, ""),
        stderr: stream(".cowshed/job/7/err", 0, ""),
        class: ExecClass::Background,
        scope: ExecScope::Workspace,
        nice: None,
        containment: Some(JobContainment::Kernel),
        outlived: Vec::new(),
        outlived_incomplete: true,
    };
    let value = serde_json::to_value(&sealed).unwrap();
    assert_eq!(value["outlivedIncomplete"], json!(true));
    assert!(value.get("outlived").is_none());
    assert_eq!(
        serde_json::from_value::<SealedJob>(value.clone()).unwrap(),
        sealed
    );
    let complete = SealedJob {
        outlived_incomplete: false,
        ..sealed.clone()
    };
    let wire = serde_json::to_value(&complete).unwrap();
    assert!(wire.get("outlivedIncomplete").is_none());
    assert_eq!(serde_json::from_value::<SealedJob>(wire).unwrap(), complete);
    for malformed in [
        SealedJob {
            containment: None,
            ..sealed.clone()
        },
        SealedJob {
            state: JobState::Running,
            ..sealed.clone()
        },
        SealedJob {
            state: JobState::Draining,
            ..sealed.clone()
        },
    ] {
        assert!(malformed.validate().is_err());
        assert!(serde_json::to_value(malformed).is_err());
    }
    let mut orphaned = value.clone();
    orphaned.as_object_mut().unwrap().remove("containment");
    assert!(serde_json::from_value::<SealedJob>(orphaned).is_err());
    for state in [JobState::Queued, JobState::Running, JobState::Draining] {
        let mut early = value.clone();
        early["state"] = serde_json::to_value(state).unwrap();
        assert!(serde_json::from_value::<SealedJob>(early).is_err());
    }
    let mut unknown = value;
    unknown["inventedProof"] = json!(true);
    assert!(serde_json::from_value::<SealedJob>(unknown).is_err());
}

#[test]
fn subtree_dtos_keep_opaque_handles_and_closed_admission_and_retirement_outcomes() {
    let handle = JobSubtreeHandle {
        job_id: JobId::new(7).unwrap(),
        index: 2,
        incarnation: "0123456789abcdef0123456789abcdef".into(),
    };
    let handle_wire = json!({
        "jobId": 7, "index": 2, "incarnation": "0123456789abcdef0123456789abcdef",
    });
    assert_eq!(serde_json::to_value(&handle).unwrap(), handle_wire);
    let admission = JobSubtreeAdmission::Admitted {
        handle: handle.clone(),
    };
    let admitted = json!({"kind": "admitted", "handle": handle_wire});
    assert_eq!(serde_json::to_value(&admission).unwrap(), admitted);
    assert_eq!(
        serde_json::from_value::<JobSubtreeAdmission>(admitted.clone()).unwrap(),
        admission
    );
    for (reason, spelling) in [
        (JobSubtreeRefusal::FoldUnavailable, "foldUnavailable"),
        (JobSubtreeRefusal::NotMember, "notMember"),
        (JobSubtreeRefusal::ForeignHandle, "foreignHandle"),
        (JobSubtreeRefusal::JobEnded, "jobEnded"),
        (JobSubtreeRefusal::SpentHandle, "spentHandle"),
    ] {
        let expected = json!({"kind": "refused", "reason": spelling});
        let admission = JobSubtreeAdmission::Refused { reason };
        let retirement = JobSubtreeRetirement::Refused { reason };
        assert_eq!(serde_json::to_value(&admission).unwrap(), expected);
        assert_eq!(
            serde_json::from_value::<JobSubtreeAdmission>(expected.clone()).unwrap(),
            admission
        );
        assert_eq!(serde_json::to_value(&retirement).unwrap(), expected);
        assert_eq!(
            serde_json::from_value::<JobSubtreeRetirement>(expected).unwrap(),
            retirement
        );
    }
    let retirement = JobProcessRetirement {
        containment: JobContainment::Kernel,
        outlived: Vec::new(),
        outlived_incomplete: true,
    };
    let receipt =
        json!({"containment": {"kind": "kernel"}, "outlived": [], "outlivedIncomplete": true});
    assert_eq!(serde_json::to_value(&retirement).unwrap(), receipt);
    assert_eq!(
        serde_json::from_value::<JobProcessRetirement>(receipt.clone()).unwrap(),
        retirement
    );
    let retired = JobSubtreeRetirement::Retired {
        retirement: retirement.clone(),
    };
    let retired_wire = json!({"kind": "retired", "retirement": receipt});
    assert_eq!(serde_json::to_value(&retired).unwrap(), retired_wire);
    assert_eq!(
        serde_json::from_value::<JobSubtreeRetirement>(retired_wire.clone()).unwrap(),
        retired
    );
    let complete = JobProcessRetirement {
        outlived_incomplete: false,
        ..retirement
    };
    let wire = serde_json::to_value(&complete).unwrap();
    assert!(wire.get("outlivedIncomplete").is_none());
    assert_eq!(
        serde_json::from_value::<JobProcessRetirement>(wire).unwrap(),
        complete
    );

    let mut unknown_handle = handle_wire;
    unknown_handle["pid"] = json!(4242);
    assert!(serde_json::from_value::<JobSubtreeHandle>(unknown_handle.clone()).is_err());
    assert!(
        serde_json::from_value::<JobSubtreeAdmission>(
            json!({"kind": "admitted", "handle": unknown_handle})
        )
        .is_err()
    );
    for malformed in [
        json!({"kind": "admitted"}),
        json!({"kind": "refused"}),
        json!({"kind": "refused", "reason": "unknown"}),
        json!({"kind": "refused", "reason": "jobEnded", "handle": handle}),
        json!({"kind": "admitted", "handle": admitted["handle"], "reason": "notMember"}),
    ] {
        assert!(serde_json::from_value::<JobSubtreeAdmission>(malformed).is_err());
    }
    for malformed in [
        json!({"kind": "retired"}),
        json!({"kind": "retired", "retirement": {"outlived": [], "outlivedIncomplete": true}}),
        json!({"kind": "retired", "retirement": {"containment": {"kind": "kernel"}}}),
        json!({"kind": "retired", "retirement": retired_wire["retirement"], "reason": "jobEnded"}),
        json!({"kind": "refused", "reason": "unknown"}),
        json!({"kind": "refused", "reason": "foreignHandle", "retirement": retired_wire["retirement"]}),
    ] {
        assert!(serde_json::from_value::<JobSubtreeRetirement>(malformed).is_err());
    }
    let mut unknown_receipt = retired_wire["retirement"].clone();
    unknown_receipt["inventedProof"] = json!(true);
    assert!(serde_json::from_value::<JobProcessRetirement>(unknown_receipt).is_err());
}

/// Admission records in every scope contain only public tool and identity names
/// (07_api.md "Keyed admission and restart attachment"). This projection is not an
/// execution allowlist; the locale family is closed to its category names.
#[test]
fn admission_records_contain_only_the_public_environment_names() {
    for name in HOST_GATE_ENV_NAMES.iter().copied().chain([
        "LC_ALL",
        "LC_CTYPE",
        "LC_MESSAGES",
        "LC_NUMERIC_2",
    ]) {
        assert!(host_gate_env_name_allowed(name), "{name}");
    }
    for name in [
        "CI",
        "GITHUB_ACTIONS",
        "NX_DAEMON",
        "NODE_OPTIONS",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "NIX_SSL_CERT_FILE",
        "SDKROOT",
        "GITHUB_TOKEN",
        "NPM_TOKEN",
        "AWS_SECRET_ACCESS_KEY",
        "MAKEFLAGS",
        "CARGO_MAKEFLAGS",
        "MFLAGS",
        "BOUNDED_EXEC_CPU_TOKENS",
        "LD_PRELOAD",
        "DYLD_INSERT_LIBRARIES",
        "BASH_ENV",
        "ENV",
        "GIT_DIR",
        "path",
        "Home",
        "PATH ",
        "PATH=/usr/bin",
        "RUSTUP",
        "LC",
        "LC_",
        "LC_a",
        "LC_ALL=C",
        "LCALL",
        "",
    ] {
        assert!(!host_gate_env_name_allowed(name), "{name:?}");
    }
}

/// A host gate runs in no session; this request validator has no environment-name policy.
/// Other scopes remain free to borrow a session.
#[test]
fn a_host_gate_request_runs_outside_every_session() {
    validate_host_gate_request(ExecScope::HostGate, false).expect("a host gate without a session");
    validate_host_gate_request(ExecScope::Workspace, false).expect("an ordinary one-shot");
    validate_host_gate_request(ExecScope::Workspace, true).expect("an ordinary session exec");

    let error = validate_host_gate_request(ExecScope::HostGate, true)
        .expect_err("a host gate runs in no session");
    assert_eq!(error.code, ErrorCode::Usage);
    assert!(error.message.contains("session"), "{}", error.message);
}

/// 07_api.md, "Reader projection": a newer controller's result carries members this build does not
/// declare, at the top and inside tagged unions, newtype arms, wire proxies and lists. The client
/// reads the declared projection; its own strict decode, a request and an unknown variant refuse.
#[test]
fn a_peer_result_reads_as_its_declared_projection_and_a_request_stays_strict() {
    use cowshed_core::api::operations::{
        JobProcessEvents, JobStatus, OperationRequest, WorkerListJobs, decode_result, read_result,
    };

    let info = finished_job(Some(WorkspacePath::new("packages/app").unwrap()));
    let mut newer = serde_json::to_value(&info).expect("job JSON");
    newer["futureField"] = json!({"nested": true});
    newer["argv"][0]["futureKey"] = json!(1);
    newer["stdout"]["futureKey"] = json!(1);
    newer["stdout"]["storage"]["futureKey"] = json!(1);
    newer["stdout"]["storage"]["artifact"]["futureKey"] = json!(1);
    newer["exit"]["futureKey"] = json!(1);
    newer["resources"]["leaf"]["futureKey"] = json!(1);
    newer["resources"]["accounting"]["reconciliation"]["coverage"]["reason"]["futureKey"] =
        json!(1);

    let strict =
        decode_result::<JobStatus>(newer.clone()).expect_err("this build's own decode is strict");
    assert!(
        strict.message.contains("unknown field `future"),
        "{strict:?}"
    );
    assert_eq!(
        read_result::<JobStatus>(newer.clone()).expect("projected"),
        info
    );
    assert_eq!(
        read_result::<WorkerListJobs>(json!([newer.clone()])).expect("projected list"),
        vec![info.clone()]
    );

    let mut unknown_variant = newer.clone();
    unknown_variant["state"] = json!("hibernating");
    let refused =
        read_result::<JobStatus>(unknown_variant).expect_err("an unknown variant is not projected");
    assert!(
        refused.message.contains("unknown variant `hibernating`"),
        "{refused:?}"
    );
    let mut missing = newer;
    missing.as_object_mut().unwrap().remove("jobId");
    let refused = read_result::<JobStatus>(missing).expect_err("a missing field is not projected");
    assert!(
        refused.message.contains("missing field `jobId`"),
        "{refused:?}"
    );

    // An internally tagged newtype arm holding an untagged union keeps its tag and its own arm.
    let change = JobProcessEvent::Changed(JobProcessDelta::Blocker {
        index: 1,
        blocked_on: BlockerChange::Clear,
    });
    let mut newer_change = serde_json::to_value(&change).expect("event JSON");
    newer_change["futureKey"] = json!(1);
    assert_eq!(
        read_result::<JobProcessEvents>(newer_change).expect("projected event"),
        change
    );

    // Members of two untagged arms at once select neither: the strict decode refuses them rather
    // than dropping one arm's member.
    let usage = json!({"kind": "changed", "index": 0, "usage": {"busy": true, "cpuSysUs": 678, "cpuUserUs": 12345, "cpuWindow": {"closedUs": 2000000, "cpuPermille": 950, "openedUs": 1000000}, "io": {"kind": "read", "readBytes": 123, "writeBytes": 456}, "rssBytes": 4096, "rssPeakBytes": 8192}});
    read_result::<JobProcessEvents>(usage.clone()).expect("a usage change");
    let mut mixed = usage;
    mixed["blockedOn"] = json!("clear");
    read_result::<JobProcessEvents>(mixed).expect_err("two arms' members are not projected");
    // An externally tagged value is one member; a second beside it is no variant to project.
    let set = json!({"kind": "changed", "index": 0, "blockedOn": {"set": {"kind": "pipe"}}});
    read_result::<JobProcessEvents>(set.clone()).expect("a blocker change");
    let mut wrapped = set;
    wrapped["blockedOn"]["futureKey"] = json!(1);
    read_result::<JobProcessEvents>(wrapped).expect_err("an external tag with a second member");

    let request = json!({
        "repoId": "acme/widget",
        "workspace": "raven",
        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
        "jobId": 7,
    });
    assert!(OperationRequest::decode("job.status", &request).is_ok());
    let mut newer_request = request;
    newer_request["futureField"] = json!(1);
    let refused =
        OperationRequest::decode("job.status", &newer_request).expect_err("a request is strict");
    assert!(
        refused.message.contains("unknown field `futureField`"),
        "{refused:?}"
    );
}
