use cowshed_core::api::*;
use cowshed_core::{CowshedError, ErrorCode};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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
        path: WorkspacePath::new(path).expect("workspace path"),
        bytes,
        summary: OutputSummary {
            version: 1,
            text: text.into(),
            truncated: false,
        },
    }
}

#[test]
fn job_id_and_path_domain_types_reject_unsafe_values() {
    assert!(JobId::new(0).is_err());
    assert_eq!(JobId::new(MAX_JOB_ID).expect("max id").get(), MAX_JOB_ID);
    assert!(JobId::new(MAX_JOB_ID + 1).is_err());

    for path in ["", "/absolute", "../escape", "a/../escape", "a//b", "a\\b"] {
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
        "2026-12-31T23:59:60Z",
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
        "2026-01-01 00:00:00Z",
        "2026-01-01T00:00:00+24:00",
    ] {
        assert!(UtcTimestamp::new(invalid).is_err(), "accepted {invalid}");
    }
    assert!(TraceId::new("0".repeat(32)).is_err());
    assert!(SpanId::new("0".repeat(16)).is_err());
}

#[test]
fn workspace_info_attached_and_detached_shapes_are_frozen() {
    let attached = WorkspaceInfo {
        repo_id: repo(),
        workspace: workspace(),
        workspace_incarnation: incarnation(),
        role: WorkspaceRole::Workspace,
        image_format: ImageFormat::Asif,
        mount: PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/raven"),
        state: WorkspaceState::Attached,
        branch: Some("raven".into()),
        base_commit: Some(oid('a')),
        created_at: Some(timestamp()),
        snapshot_stale: false,
    };
    assert_eq!(
        serde_json::to_value(&attached).expect("attached JSON"),
        json!({
            "repoId": "acme/widget",
            "workspace": "raven",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "role": "workspace",
            "imageFormat": "asif",
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
    for absent in ["branch", "baseCommit", "createdAt"] {
        assert!(detached_json.get(absent).is_none(), "unexpected {absent}");
    }
}

#[test]
fn ensure_doctor_gc_and_empty_results_have_exact_shapes() {
    let ensure = EnsureReport {
        workspace: workspace(),
        mount: PathBuf::from("/mnt/raven"),
        action: EnsureAction::Healed,
    };
    assert_eq!(
        serde_json::to_value(ensure).expect("ensure JSON"),
        json!({"workspace":"raven","mount":"/mnt/raven","action":"healed"})
    );

    let doctor = DoctorReport {
        healthy: false,
        findings: vec![Finding {
            code: "format-mismatch".into(),
            severity: FindingSeverity::Error,
            message: "detached format disagrees with extension".into(),
            hint: "cowshed doctor --fix-format raven".into(),
            path: Some(PathBuf::from("/store/raven.asif.grants.json")),
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
                "path": "/store/raven.asif.grants.json"
            }]
        })
    );

    let gc = GcReport {
        examined: 9,
        reclaimed: 3,
        retained_pinned: 2,
        freed_bytes: 4096,
        dry_run: true,
    };
    assert_eq!(
        serde_json::to_value(gc).expect("gc JSON"),
        json!({"examined":9,"reclaimed":3,"retainedPinned":2,"freedBytes":4096,"dryRun":true})
    );
    assert_eq!(serde_json::to_value(EmptyResult {}).unwrap(), json!({}));
}

#[test]
fn nested_job_info_shape_is_byte_safe_and_frozen() {
    let info = JobInfo {
        repo_id: repo(),
        workspace_incarnation: incarnation(),
        job_id: JobId::new(7).unwrap(),
        state: JobState::Signaled,
        pid: Some(4242),
        grant_revision: 9,
        argv: vec!["bun".into(), "test".into()],
        cwd: WorkspacePath::new("packages/app").unwrap(),
        started: timestamp(),
        duration_ms: Some(1250),
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
    };
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
            "argv": ["bun", "test"],
            "cwd": "packages/app",
            "started": "2026-07-11T12:34:56Z",
            "durationMs": 1250,
            "exit": {"kind":"signaled","signal":15,"coreDumped":false},
            "stdout": {"path":".cowshed/job/7/out","bytes":3,"summary":{"version":1,"text":"ok\n","truncated":false}},
            "stderr": {"path":".cowshed/job/7/err","bytes":0,"summary":{"version":1,"text":"","truncated":false}},
            "trace": {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7"},
            "stdin": {"kind":"workspaceFile","bytes":12,"workspacePath":"fixtures/input.bin","complete":true}
        })
    );
    info.validate().expect("job invariants");
    let encoded = serde_json::to_string(&value).unwrap();
    assert!(!encoded.contains("rawBytes"));
    assert!(!encoded.contains("stdoutBytes"));
    assert_eq!(serde_json::from_value::<JobInfo>(value).unwrap(), info);
}

#[test]
fn output_limit_is_an_explicit_terminal_projection() {
    let mut value = json!({
        "repoId": "acme/widget",
        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
        "jobId": 8,
        "state": "outputLimit",
        "grantRevision": 9,
        "argv": ["cat"],
        "cwd": "packages/app",
        "started": "2026-07-11T12:34:56Z",
        "durationMs": 50,
        "stdout": {"path":".cowshed/job/8/out","bytes":1024,"summary":{"version":1,"text":"bounded","truncated":true}},
        "stderr": {"path":".cowshed/job/8/err","bytes":1,"summary":{"version":1,"text":"","truncated":false}},
        "trace": {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7"},
        "outputLimit": {"limitBytes":1024,"crossingBytes":1025},
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
fn all_lifecycle_options_use_camel_case_and_omit_only_optionals() {
    assert_eq!(
        serde_json::to_value(AdoptOptions {
            path: None,
            capacity: Some("100g".into()),
            quarantine: true,
            image_format: Some(ImageFormat::Sparse),
        })
        .unwrap(),
        json!({"capacity":"100g","quarantine":true,"imageFormat":"sparse"})
    );
    assert_eq!(
        serde_json::to_value(CreateOptions {
            revision: Some(RevisionTarget::Oid(oid('a'))),
            from_workspace: Some(workspace()),
            browse: false,
            slot: Some(2),
        })
        .unwrap(),
        json!({
            "revision":{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "fromWorkspace":"raven",
            "browse":false,
            "slot":2
        })
    );
    assert_eq!(
        serde_json::to_value(AttachOptions { browse: true }).unwrap(),
        json!({"browse":true})
    );
    assert_eq!(
        serde_json::to_value(RemoveOptions { force: false }).unwrap(),
        json!({"force":false})
    );
    assert_eq!(
        serde_json::to_value(GcOptions { dry_run: true }).unwrap(),
        json!({"dryRun":true})
    );

    assert_eq!(
        serde_json::from_value::<AttachOptions>(json!({})).unwrap(),
        AttachOptions::default()
    );
    assert_eq!(
        serde_json::from_value::<CreateOptions>(json!({})).unwrap(),
        CreateOptions::default()
    );
    let land_default = serde_json::from_value::<LandOptions>(json!({})).unwrap();
    assert!(land_default.retire);
    assert!(!land_default.push_only);
    assert_eq!(
        serde_json::to_value(land_default).unwrap(),
        json!({"retire":true,"pushOnly":false})
    );
    let grant_delta = serde_json::from_value::<GrantDelta>(json!({})).unwrap();
    assert_eq!(grant_delta, GrantDelta::default());
    assert_eq!(serde_json::to_value(grant_delta).unwrap(), json!({}));

    let rebase = RebaseOptions {
        onto: Some(RevisionTarget::Branch(BranchName::new("main").unwrap())),
        fresh: true,
        expected_workspace_incarnation: Some(incarnation()),
        expected_source_head: Some(oid('a')),
        expected_onto_head: None,
    };
    let rebase_json = serde_json::to_value(rebase).unwrap();
    assert_eq!(
        rebase_json["expectedWorkspaceIncarnation"],
        incarnation().as_str()
    );
    assert!(rebase_json.get("expectedOntoHead").is_none());

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
    let land = LandReport {
        landed_head: oid('b'),
        target_branch: "main".into(),
        previous_target_head: Some(oid('a')),
        target_was_checked_out: true,
        retired: true,
    };
    assert_eq!(
        serde_json::to_value(land).unwrap()["targetWasCheckedOut"],
        true
    );

    let status = GatewayStatus {
        running: true,
        socket: PathBuf::from("/store/gateway.sock"),
        cache_entries: 7,
        cache_bytes: 8192,
        active_workspaces: 2,
    };
    assert_eq!(
        serde_json::to_value(status).unwrap(),
        json!({"running":true,"socket":"/store/gateway.sock","cacheEntries":7,"cacheBytes":8192,"activeWorkspaces":2})
    );

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

#[test]
fn lesser_capabilities_fail_to_compile_with_coordinator_authority() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "cowshed-capability-compile-fail-{}-{nonce}",
        std::process::id()
    ));
    let bins = root.join("src/bin");
    fs::create_dir_all(&bins).expect("compile-fail fixture directory");
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    fs::write(
        root.join("Cargo.toml"),
        format!(
            "[package]\nname = \"cowshed-capability-negative\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[dependencies]\ncowshed-core = {{ path = {:?} }}\nserde = \"1\"\n",
            manifest_dir
        ),
    )
    .expect("compile-fail manifest");
    let cases: [(&str, &str, &[&str]); 5] = [
        (
            "project_authority",
            "use cowshed_core::Project;\nfn deny(value: &Project) { value.attach(); value.exec(); value.grant(); value.gc(); }\nfn main() {}\n",
            &["attach", "exec", "grant", "gc"],
        ),
        (
            "worker_authority",
            "use cowshed_core::WorkspaceHandle;\nfn deny(value: &WorkspaceHandle) { value.grant(); value.revoke(); value.restore(); value.destroy(); value.rebase(); value.land(); value.gc(); value.repo_mirror(); value.detach(); value.workspace(\"other\"); }\nfn main() {}\n",
            &[
                "grant",
                "revoke",
                "restore",
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
            "use cowshed_core::CoordinatorToken;\nfn must_clone<T: Clone>() {} fn must_serialize<T: serde::Serialize>() {}\nfn main() { must_clone::<CoordinatorToken>(); must_serialize::<CoordinatorToken>(); }\n",
            &["Clone", "Serialize"],
        ),
        (
            "private_construction",
            "use cowshed_core::{CoordinatorToken,Cowshed,Project,WorkspaceHandle};\nfn main() { let _ = Cowshed {}; let _ = Project {}; let _ = WorkspaceHandle {}; let _ = CoordinatorToken {}; }\n",
            &["Cowshed", "Project", "WorkspaceHandle", "CoordinatorToken"],
        ),
        (
            "null_success",
            "use cowshed_core::api::JsonEnvelope;\nfn main() { let _ = JsonEnvelope::success(()); }\n",
            &["ResultBody"],
        ),
    ];
    for (name, source, expected) in cases {
        fs::write(bins.join(format!("{name}.rs")), source).expect("compile-fail source");
        let output = Command::new(env!("CARGO"))
            .args(["check", "--quiet", "--offline", "--bin", name])
            .current_dir(&root)
            .env("CARGO_TARGET_DIR", root.join("target"))
            .output()
            .expect("run cargo check");
        assert!(!output.status.success(), "{name} unexpectedly compiled");
        let stderr = String::from_utf8_lossy(&output.stderr);
        for expected in expected {
            assert!(
                stderr.contains(expected),
                "{name} did not fail on {expected}:\n{stderr}"
            );
        }
    }
    fs::remove_dir_all(root).expect("remove compile-fail fixtures");
}
