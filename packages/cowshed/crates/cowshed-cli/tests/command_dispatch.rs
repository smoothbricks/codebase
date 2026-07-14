use async_trait::async_trait;
use cowshed_cli::args::parse_args;
use cowshed_cli::output::Output;
use cowshed_cli::runtime::{
    CliService, ExecCommand, ExecPresentation, ExecResult, dispatch, dispatch_and_shutdown,
};
use cowshed_core::api::*;
use cowshed_core::metadata::{
    ImageFormat, PortBlock, WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use cowshed_core::repository::RepoId;
use cowshed_core::{CowshedError, ErrorCode, Result};
use std::collections::HashSet;
use std::ffi::OsString;
use std::io::Write;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};

struct FakeService {
    events: Vec<String>,
    argv: Vec<Vec<u8>>,
    stdin: Vec<u8>,
    presentation: Option<ExecPresentation>,
    child_exit: ExitStatus,
    fail_list: Option<CowshedError>,
    fail_push: Option<CowshedError>,
    push_options: Option<PushOptions>,
    rebase_options: Option<RebaseOptions>,
    land_options: Option<LandOptions>,
    ensure_report: Option<EnsureReport>,
    ensure_error: Option<CowshedError>,
    ensure_path: Option<PathBuf>,
    gc_candidates: Vec<GcCandidate>,
    shutdowns: Option<Arc<AtomicUsize>>,
    shutdown_error: Option<CowshedError>,
}

impl Default for FakeService {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            argv: Vec::new(),
            stdin: Vec::new(),
            presentation: None,
            child_exit: ExitStatus::Exited { code: 0 },
            fail_list: None,
            fail_push: None,
            push_options: None,
            rebase_options: None,
            land_options: None,
            ensure_report: None,
            ensure_error: None,
            ensure_path: None,
            gc_candidates: Vec::new(),
            shutdowns: None,
            shutdown_error: None,
        }
    }
}

#[async_trait]
impl CliService for FakeService {
    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceInfo> {
        self.events.push(format!("adopt:{:?}", options.path));
        Ok(workspace("main", WorkspaceState::Attached))
    }

    async fn create(&mut self, name: &str, options: CreateOptions) -> Result<WorkspaceInfo> {
        self.events.push(format!("new:{name}:{}", options.browse));
        Ok(workspace(name, WorkspaceState::Attached))
    }

    async fn fork(&mut self, source: &str, destination: &str) -> Result<WorkspaceInfo> {
        self.events.push(format!("fork:{source}:{destination}"));
        Ok(workspace(destination, WorkspaceState::Attached))
    }

    async fn checkpoint(&mut self, name: &str, options: CheckpointOptions) -> Result<String> {
        self.events.push(format!(
            "checkpoint:{name}:{:?}:{}",
            options.label, options.keep
        ));
        Ok(options
            .label
            .unwrap_or_else(|| "2026-07-14T00-00-00Z".into()))
    }

    async fn restore(&mut self, name: &str, label: &str) -> Result<WorkspaceInfo> {
        self.events.push(format!("restore:{name}:{label}"));
        Ok(workspace(name, WorkspaceState::Attached))
    }

    async fn ensure_current(&mut self, path: PathBuf) -> Result<EnsureReport> {
        self.events.push(format!("ensure:{}", path.display()));
        self.ensure_path = Some(path);
        if let Some(error) = self.ensure_error.take() {
            return Err(error);
        }
        Ok(self.ensure_report.take().unwrap_or_else(|| EnsureReport {
            workspace: WorkspaceName::new("raven").unwrap(),
            mount: PathBuf::from("/mnt/raven"),
            action: EnsureAction::AlreadyMounted,
            go_env: PathBuf::from("/mnt/raven/.cowshed/cache/go/env"),
            workspace_token: PathBuf::from("/mnt/raven/.cowshed/token"),
            port_block: None,
        }))
    }

    async fn list(&mut self) -> Result<Vec<WorkspaceInfo>> {
        self.events.push("ls".into());
        if let Some(error) = self.fail_list.take() {
            return Err(error);
        }
        Ok(vec![
            workspace("zebra", WorkspaceState::Detached),
            workspace("main", WorkspaceState::Attached),
        ])
    }

    async fn path(&mut self, name: &str, no_attach: bool) -> Result<WorkspaceInfo> {
        self.events.push(format!("path:{name}:{no_attach}"));
        Ok(workspace(name, WorkspaceState::Attached))
    }

    async fn remove(&mut self, name: &str, options: RemoveOptions) -> Result<()> {
        self.events
            .push(format!("rm:{name}:{}:{}", options.force, options.restore));
        Ok(())
    }

    async fn attach(&mut self, name: &str, options: AttachOptions) -> Result<()> {
        self.events
            .push(format!("attach:{name}:{}", options.browse));
        Ok(())
    }

    async fn detach(&mut self, name: &str) -> Result<()> {
        self.events.push(format!("detach:{name}"));
        Ok(())
    }

    async fn doctor(&mut self) -> Result<DoctorReport> {
        self.events.push("doctor".into());
        Ok(DoctorReport {
            healthy: true,
            findings: Vec::new(),
        })
    }

    async fn gc(&mut self, options: GcOptions) -> Result<GcReport> {
        self.events.push(format!("gc:{}", options.dry_run));
        let candidate_bytes = self
            .gc_candidates
            .iter()
            .map(|candidate| candidate.bytes)
            .sum();
        Ok(GcReport {
            examined: 9,
            reclaimed: u64::from(!options.dry_run) * 3,
            retained_pinned: 2,
            freed_bytes: if options.dry_run {
                candidate_bytes
            } else {
                4096
            },
            dry_run: options.dry_run,
            candidates: self.gc_candidates.clone(),
        })
    }

    async fn push(&mut self, name: &str, options: PushOptions) -> Result<PushReport> {
        self.push_options = Some(options.clone());
        self.events.push(format!("push:{name}:{options:?}"));
        if let Some(error) = self.fail_push.take() {
            return Err(error);
        }
        Ok(PushReport {
            source_head: GitOid::new("2".repeat(40)).unwrap(),
            destination_ref: format!(
                "refs/cowshed/{name}/heads/{}",
                options.branch.as_deref().unwrap_or(name)
            ),
            previous_destination_head: None,
        })
    }

    async fn rebase(&mut self, name: &str, options: RebaseOptions) -> Result<GitOid> {
        self.rebase_options = Some(options.clone());
        self.events.push(format!("rebase:{name}:{options:?}"));
        Ok(GitOid::new("3".repeat(40)).unwrap())
    }

    async fn land(&mut self, name: &str, options: LandOptions) -> Result<LandReport> {
        self.land_options = Some(options.clone());
        self.events.push(format!("land:{name}:{options:?}"));
        Ok(LandReport {
            landed_head: GitOid::new("4".repeat(40)).unwrap(),
            target_branch: options.target_branch.unwrap_or_else(|| "main".into()),
            previous_target_head: Some(GitOid::new("1".repeat(40)).unwrap()),
            target_was_checked_out: true,
            retired: options.retire,
        })
    }
    async fn exec(
        &mut self,
        command: ExecCommand,
        presentation: ExecPresentation,
        stdout: &mut (dyn Write + Send),
        stderr: &mut (dyn Write + Send),
    ) -> Result<ExecResult> {
        self.events.push(format!("exec:{}", command.workspace));
        self.presentation = Some(presentation);
        self.argv = command
            .request
            .argv
            .iter()
            .map(|arg| arg.as_os_str().as_bytes().to_vec())
            .collect();
        match command.request.stdin {
            StdinSource::Empty => {}
            StdinSource::Inline(bytes) => self.stdin.extend_from_slice(&bytes),
            StdinSource::WorkspaceFile(path) => self
                .stdin
                .extend_from_slice(path.as_path().as_os_str().as_bytes()),
            StdinSource::Stream(mut reader) => {
                reader.read_to_end(&mut self.stdin).await.map_err(|error| {
                    CowshedError::environment_missing(error.to_string(), "retry stdin")
                })?;
            }
        }
        if presentation == ExecPresentation::Raw {
            stdout.write_all(b"out\xff").unwrap();
            stderr.write_all(b"err\0").unwrap();
        }
        Ok(ExecResult {
            info: job_info(command.request.argv, self.child_exit.clone()),
            backgrounded: command.background,
        })
    }

    async fn shutdown(mut self) -> Result<()> {
        if let Some(shutdowns) = self.shutdowns {
            shutdowns.fetch_add(1, Ordering::SeqCst);
        }
        match self.shutdown_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn workspace(name: &str, state: WorkspaceState) -> WorkspaceInfo {
    WorkspaceInfo {
        repo_id: RepoId::parse("acme/widget").unwrap(),
        workspace: WorkspaceName::new(name).unwrap(),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .unwrap(),
        role: if name == "main" {
            WorkspaceRole::Main
        } else {
            WorkspaceRole::Workspace
        },
        image_format: ImageFormat::Asif,
        mount: PathBuf::from(format!("/mnt/{name}")),
        state,
        branch: Some(if name == "main" {
            "main".into()
        } else {
            format!("cowshed/{name}")
        }),
        base_commit: Some(GitOid::new("1".repeat(40)).unwrap()),
        created_at: Some(UtcTimestamp::new("2026-07-14T00:00:00Z").unwrap()),
        checkpoints: Vec::new(),
        snapshot_stale: false,
    }
}

fn stream(path: &str, bytes: &[u8]) -> StreamInfo {
    StreamInfo {
        storage: OutputStorage::Captured {
            artifact: ProtectedOutput::File {
                path: WorkspacePath::new(path).unwrap(),
            },
        },
        bytes: bytes.len() as u64,
        sha256: Sha256Digest::from_bytes([0; 32]),
        summary: OutputSummary {
            version: 1,
            text: String::from_utf8_lossy(bytes).into_owned(),
            truncated: false,
        },
    }
}

fn job_info(argv: Vec<CommandArg>, exit: ExitStatus) -> JobInfo {
    let state = match exit {
        ExitStatus::Exited { .. } => JobState::Exited,
        ExitStatus::Signaled { .. } => JobState::Signaled,
    };
    JobInfo {
        repo_id: RepoId::parse("acme/widget").unwrap(),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .unwrap(),
        job_id: JobId::new(7).unwrap(),
        state,
        pid: Some(42),
        grant_revision: 1,
        argv,
        cwd: None,
        started: UtcTimestamp::new("2026-07-14T00:00:00Z").unwrap(),
        duration_ms: Some(1),
        exit: Some(exit),
        stdout: stream(".cowshed/job/7/out", b"out"),
        stderr: stream(".cowshed/job/7/err", b"err"),
        trace: TraceContext {
            trace_id: TraceId::new("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            span_id: SpanId::new("00f067aa0ba902b7").unwrap(),
        },
        output_limit: None,
        stdin: StdinInfo {
            kind: StdinKind::Empty,
            bytes: 0,
            workspace_path: None,
            complete: true,
        },
    }
}

async fn run(
    service: &mut FakeService,
    args: impl IntoIterator<Item = impl Into<OsString>>,
) -> (i32, Vec<u8>, Vec<u8>) {
    let cli = parse_args(args).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), cli.global.quiet);
    let exit = dispatch(service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap();
    let (stdout, stderr) = output.into_inner();
    (exit.code, stdout, stderr)
}

#[tokio::test]
async fn all_nine_parser_commands_dispatch_and_obey_machine_output_contracts() {
    let mut service = FakeService::default();

    let (_, stdout, stderr) = run(&mut service, ["adopt", "/repo"]).await;
    assert_eq!(stdout, b"/mnt/main\n");
    assert_eq!(stderr, b"next: cowshed new <name>\n");

    let (_, stdout, stderr) = run(&mut service, ["new", "raven", "--browse"]).await;
    assert_eq!(stdout, b"/mnt/raven\n");
    assert_eq!(stderr, b"next: cowshed exec raven -- <cmd>\n");

    let (_, stdout, stderr) = run(&mut service, ["ls"]).await;
    assert_eq!(
        stdout,
        b"main\tmounted\tmain\t/mnt/main\nzebra\tdetached\tcowshed/zebra\t\n"
    );
    assert!(stderr.is_empty());

    let (_, stdout, _) = run(&mut service, ["path", "raven"]).await;
    assert_eq!(stdout, b"/mnt/raven\n");

    let (code, stdout, stderr) = run(&mut service, ["exec", "raven", "--", "true"]).await;
    assert_eq!(code, 0);
    assert_eq!(stdout, b"out\xff");
    assert_eq!(stderr, b"err\0");

    let (_, stdout, _) = run(&mut service, ["rm", "raven", "--force"]).await;
    assert!(stdout.is_empty());
    let (_, stdout, _) = run(&mut service, ["attach", "raven", "--browse"]).await;
    assert!(stdout.is_empty());
    let (_, stdout, _) = run(&mut service, ["detach", "raven"]).await;
    assert!(stdout.is_empty());
    let (_, stdout, _) = run(&mut service, ["doctor"]).await;
    assert_eq!(stdout, b"healthy\n");

    assert_eq!(
        service.events,
        [
            "adopt:Some(\"/repo\")",
            "new:raven:true",
            "ls",
            "path:raven:false",
            "exec:raven",
            "rm:raven:true:false",
            "attach:raven:true",
            "detach:raven",
            "doctor",
        ]
    );
}

#[tokio::test]
async fn lifecycle_commands_delegate_exact_options_and_keep_stdout_machine_only() {
    let mut service = FakeService::default();
    let incarnation = "0198f2c0b7e34dc795f17b238b331c80";
    let source = "1111111111111111111111111111111111111111";
    let destination = "2222222222222222222222222222222222222222";

    let (_, stdout, stderr) = run(&mut service, ["fork", "raven", "falcon"]).await;
    assert_eq!(stdout, b"/mnt/falcon\n");
    assert_eq!(stderr, b"next: cowshed exec falcon -- <cmd>\n");

    let (_, stdout, stderr) = run(
        &mut service,
        ["checkpoint", "raven", "pre-land", "--keep", "--json"],
    )
    .await;
    assert_eq!(
        stdout,
        b"{\"ok\":true,\"result\":{\"label\":\"pre-land\"}}\n"
    );
    assert!(stderr.is_empty());

    let (_, stdout, stderr) = run(&mut service, ["restore", "raven", "pre-land", "--json"]).await;
    assert_eq!(
        stdout,
        format!(
            "{{\"ok\":true,\"result\":{{\"workspace\":\"raven\",\"mount\":\"/mnt/raven\",\"baseCommit\":\"{}\"}}}}\n",
            "1".repeat(40)
        )
        .as_bytes()
    );
    assert_eq!(stderr, b"next: cowshed exec raven -- git status\n");

    let token_path =
        std::env::temp_dir().join(format!("cowshed-cli-envrc-{}-token", std::process::id()));
    std::fs::write(&token_path, b"tok'en").unwrap();
    service.ensure_report = Some(EnsureReport {
        workspace: WorkspaceName::new("raven").unwrap(),
        mount: PathBuf::from("/mnt/raven"),
        action: EnsureAction::AlreadyMounted,
        go_env: PathBuf::from("/mnt/raven/nested dir/it's/go env"),
        workspace_token: token_path.clone(),
        port_block: Some(PortBlock::new(40960, 16).unwrap()),
    });
    let (_, stdout, stderr) = run(&mut service, ["ensure", "--envrc"]).await;
    assert_eq!(
        stdout,
        b"export GOENV='/mnt/raven/nested dir/it'\\''s/go env'\nexport COWSHED_WORKSPACE_TOKEN='tok'\\''en'\nexport COWSHED_PORT_BASE='40960'\n"
    );
    assert!(stderr.is_empty());
    assert_eq!(service.ensure_path, Some(std::env::current_dir().unwrap()));
    std::fs::remove_file(token_path).unwrap();

    let (_, stdout, stderr) = run(&mut service, ["gc", "--dry-run"]).await;
    assert_eq!(stdout, b"0\n");
    assert_eq!(
        stderr,
        b"cowshed: dry run examined 9 objects; 0 candidates, 0 bytes reclaimable\n"
    );

    let (_, stdout, stderr) = run(
        &mut service,
        [
            "push",
            "raven",
            "--branch",
            "release",
            "--expected-workspace-incarnation",
            incarnation,
            "--expected-source-head",
            source,
            "--expected-destination-head",
            destination,
            "--json",
        ],
    )
    .await;
    assert_eq!(
        stdout,
        format!(
            "{{\"ok\":true,\"result\":{{\"sourceHead\":\"{}\",\"destinationRef\":\"refs/cowshed/raven/heads/release\"}}}}\n",
            "2".repeat(40)
        )
        .as_bytes()
    );
    assert!(stderr.is_empty());
    assert_eq!(
        service.push_options,
        Some(PushOptions {
            branch: Some("release".into()),
            expected_workspace_incarnation: Some(WorkspaceIncarnation::new(incarnation).unwrap()),
            expected_source_head: Some(GitOid::new(source).unwrap()),
            expected_destination_head: Some(ExpectedRefHead::Oid(
                GitOid::new(destination).unwrap()
            )),
        })
    );

    let (_, stdout, stderr) = run(
        &mut service,
        [
            "rebase",
            "raven",
            "--onto",
            "refs/heads/release",
            "--fresh",
            "--expected-workspace-incarnation",
            incarnation,
            "--expected-source-head",
            source,
            "--expected-onto-head",
            destination,
        ],
    )
    .await;
    assert_eq!(stdout, format!("{}\n", "3".repeat(40)).as_bytes());
    assert!(stderr.is_empty());
    assert_eq!(
        service.rebase_options,
        Some(RebaseOptions {
            onto: Some(RevisionTarget::Ref(
                GitRef::new("refs/heads/release").unwrap()
            )),
            fresh: true,
            expected_workspace_incarnation: Some(WorkspaceIncarnation::new(incarnation).unwrap()),
            expected_source_head: Some(GitOid::new(source).unwrap()),
            expected_onto_head: Some(GitOid::new(destination).unwrap()),
        })
    );

    let (_, stdout, stderr) = run(
        &mut service,
        [
            "land",
            "raven",
            "--target",
            "release",
            "--check",
            "cargo test",
            "--check",
            "cargo clippy",
            "--no-retire",
            "--push-only",
            "--expected-workspace-incarnation",
            incarnation,
            "--expected-source-head",
            source,
            "--expected-target-head",
            "missing",
            "--json",
        ],
    )
    .await;
    assert_eq!(
        stdout,
        format!(
            "{{\"ok\":true,\"result\":{{\"landedHead\":\"{}\",\"targetBranch\":\"release\",\"previousTargetHead\":\"{}\",\"targetWasCheckedOut\":true,\"retired\":false}}}}\n",
            "4".repeat(40),
            "1".repeat(40)
        )
        .as_bytes()
    );
    assert!(stderr.is_empty());
    assert_eq!(
        service.land_options,
        Some(LandOptions {
            target_branch: Some("release".into()),
            check: Some(vec!["cargo test".into(), "cargo clippy".into()]),
            retire: false,
            push_only: true,
            expected_workspace_incarnation: Some(WorkspaceIncarnation::new(incarnation).unwrap()),
            expected_source_head: Some(GitOid::new(source).unwrap()),
            expected_target_head: Some(ExpectedRefHead::Missing),
        })
    );

    let (_, stdout, stderr) = run(&mut service, ["rm", "main", "--restore", "--json"]).await;
    assert_eq!(stdout, b"{\"ok\":true,\"result\":{}}\n");
    assert_eq!(stderr, b"next: cowshed gc\n");
    assert!(
        service
            .events
            .iter()
            .any(|event| event == "rm:main:false:true")
    );
}

#[tokio::test]
async fn ensure_uses_nested_invocation_cwd_and_reports_detached_healing() {
    const CHILD: &str = "COWSHED_CLI_NESTED_CWD_TEST";
    if std::env::var_os(CHILD).is_some() {
        let mut service = FakeService {
            ensure_report: Some(EnsureReport {
                workspace: WorkspaceName::new("raven").unwrap(),
                mount: PathBuf::from("/mnt/raven"),
                action: EnsureAction::Attached,
                go_env: PathBuf::from("/mnt/raven/.cowshed/cache/go/env"),
                workspace_token: PathBuf::from("/mnt/raven/.cowshed/token"),
                port_block: None,
            }),
            ..FakeService::default()
        };
        let (_, stdout, stderr) = run(&mut service, ["ensure", "--json"]).await;
        assert_eq!(
            stdout,
            b"{\"ok\":true,\"result\":{\"workspace\":\"raven\",\"mount\":\"/mnt/raven\",\"action\":\"attached\",\"goEnv\":\"/mnt/raven/.cowshed/cache/go/env\",\"workspaceToken\":\"/mnt/raven/.cowshed/token\"}}\n"
        );
        assert_eq!(stderr, b"cowshed: workspace raven is ready (attached)\n");
        assert_eq!(service.ensure_path, Some(std::env::current_dir().unwrap()));
        return;
    }

    let root = std::env::temp_dir().join(format!("cowshed-cli-nested-cwd-{}", std::process::id()));
    let nested = root.join("workspace/deep/path");
    std::fs::create_dir_all(&nested).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("ensure_uses_nested_invocation_cwd_and_reports_detached_healing")
        .arg("--nocapture")
        .env(CHILD, "1")
        .current_dir(&nested)
        .status()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn ensure_resolution_and_token_errors_emit_no_partial_machine_output() {
    let mut resolution_failure = FakeService {
        ensure_error: Some(CowshedError::not_found(
            "the current directory is not a cowshed workspace",
            "cd into a workspace",
        )),
        ..FakeService::default()
    };
    let cli = parse_args(["ensure", "--json"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(
        &mut resolution_failure,
        cli,
        tokio::io::empty(),
        &mut output,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    assert!(output.into_inner().0.is_empty());

    let mut token_failure = FakeService {
        ensure_report: Some(EnsureReport {
            workspace: WorkspaceName::new("raven").unwrap(),
            mount: PathBuf::from("/mnt/raven"),
            action: EnsureAction::AlreadyMounted,
            go_env: PathBuf::from("/mnt/raven/.cowshed/cache/go/env"),
            workspace_token: PathBuf::from("/definitely/missing/cowshed/token"),
            port_block: None,
        }),
        ..FakeService::default()
    };
    let cli = parse_args(["ensure", "--envrc"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(&mut token_failure, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Integrity);
    let (stdout, stderr) = output.into_inner();
    assert!(stdout.is_empty());
    assert!(stderr.is_empty());
}

#[tokio::test]
async fn gc_dry_run_zero_and_unicode_candidates_keep_streams_separate() {
    let mut empty = FakeService::default();
    let (_, stdout, stderr) = run(&mut empty, ["gc", "--dry-run"]).await;
    assert_eq!(stdout, b"0\n");
    assert_eq!(
        stderr,
        b"cowshed: dry run examined 9 objects; 0 candidates, 0 bytes reclaimable\n"
    );

    let candidate = GcCandidate {
        identity: Sha256Digest::from_bytes([0xab; 32]),
        path: PathBuf::from("/tmp/回收 space/checkpoint"),
        bytes: 1234,
        reason: GcReason::ExpiredCheckpoint,
    };
    let mut populated = FakeService {
        gc_candidates: vec![candidate],
        ..FakeService::default()
    };
    let (_, stdout, stderr) = run(&mut populated, ["gc", "--dry-run"]).await;
    assert_eq!(stdout, b"1234\n");
    assert_eq!(
        stderr,
        b"cowshed: would reclaim /tmp/\xe5\x9b\x9e\xe6\x94\xb6 space/checkpoint (1234 bytes; reason: expiredCheckpoint)\ncowshed: dry run examined 9 objects; 1 candidate, 1234 bytes reclaimable\n"
    );

    let (_, stdout, stderr) = run(&mut populated, ["gc", "--dry-run", "--json"]).await;
    assert_eq!(
        stdout,
        format!(
            "{{\"ok\":true,\"result\":{{\"examined\":9,\"reclaimed\":0,\"retainedPinned\":2,\"freedBytes\":1234,\"dryRun\":true,\"candidates\":[{{\"identity\":\"{}\",\"path\":\"/tmp/回收 space/checkpoint\",\"bytes\":1234,\"reason\":\"expiredCheckpoint\"}}]}}}}\n",
            "ab".repeat(32)
        )
        .as_bytes()
    );
    assert_eq!(
        stderr,
        b"cowshed: would reclaim /tmp/\xe5\x9b\x9e\xe6\x94\xb6 space/checkpoint (1234 bytes; reason: expiredCheckpoint)\ncowshed: dry run examined 9 objects; 1 candidate, 1234 bytes reclaimable\n"
    );
}

#[tokio::test]
async fn lifecycle_conflicts_and_non_utf8_revisions_fail_without_partial_output() {
    let mut service = FakeService {
        fail_push: Some(CowshedError::conflict(
            "push destination head is stale",
            "refresh and retry",
        )),
        ..FakeService::default()
    };
    let cli = parse_args(["push", "raven", "--json"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(&mut service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    let (stdout, stderr) = output.into_inner();
    assert!(stdout.is_empty());
    assert!(stderr.is_empty());

    let opaque = OsString::from_vec(vec![b'm', 0x80, b'a', b'i', b'n']);
    let cli = parse_args(vec![
        OsString::from("rebase"),
        OsString::from("raven"),
        OsString::from("--onto"),
        opaque,
    ])
    .unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(&mut service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Usage);
    assert!(error.message.contains("valid UTF-8"));
    assert!(service.rebase_options.is_none());
    assert!(output.into_inner().0.is_empty());
}

#[tokio::test]
async fn exec_preserves_non_utf8_argv_and_maps_child_exit_and_signal() {
    let opaque = OsString::from_vec(vec![b'a', 0x80, b'z']);
    let mut service = FakeService {
        child_exit: ExitStatus::Exited { code: 23 },
        ..FakeService::default()
    };
    let cli = parse_args(vec![
        OsString::from("exec"),
        OsString::from("raven"),
        OsString::from("--"),
        opaque.clone(),
    ])
    .unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let exit = dispatch(&mut service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap();
    assert_eq!(exit.code, 23);
    assert_eq!(service.argv, vec![opaque.as_bytes()]);

    service.child_exit = ExitStatus::Signaled {
        signal: 9,
        core_dumped: false,
    };
    let (code, _, _) = run(&mut service, ["exec", "raven", "--", "sleep"]).await;
    assert_eq!(code, 137);
}

#[tokio::test]
async fn stdin_sources_are_exclusive_exact_and_streams_apply_backpressure() {
    let mut inline = FakeService::default();
    run(
        &mut inline,
        ["exec", "raven", "--stdin-base64", "AP+A", "--", "cat"],
    )
    .await;
    assert_eq!(inline.stdin, vec![0, 0xff, 0x80]);

    let mut file = FakeService::default();
    run(
        &mut file,
        ["exec", "raven", "--stdin-file", "fixtures/in", "--", "cat"],
    )
    .await;
    assert_eq!(file.stdin, b"fixtures/in");

    let mut streamed = FakeService::default();
    let cli = parse_args(["exec", "raven", "--stdin", "--", "cat"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let (mut writer, reader) = tokio::io::duplex(1);
    let payload = vec![0x5a; 256 * 1024];
    let expected = payload.clone();
    let producer = tokio::spawn(async move {
        tokio::io::AsyncWriteExt::write_all(&mut writer, &payload)
            .await
            .unwrap();
    });
    dispatch(&mut streamed, cli, reader, &mut output)
        .await
        .unwrap();
    producer.await.unwrap();
    assert_eq!(streamed.stdin, expected);
}

#[tokio::test]
async fn json_exec_emits_only_bounded_job_info_and_never_raw_streams() {
    let mut service = FakeService::default();
    let (code, stdout, stderr) = run(
        &mut service,
        ["--json", "exec", "raven", "--", "printf", "bytes"],
    )
    .await;
    assert_eq!(code, 0);
    assert!(stderr.is_empty());
    assert_eq!(service.presentation, Some(ExecPresentation::Control));
    assert!(!stdout.windows(4).any(|window| window == b"out\xff"));
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["ok"], true);
    assert_eq!(value["result"]["jobId"], 7);
}

#[tokio::test]
async fn not_adopted_failure_keeps_typed_hint_and_writes_no_machine_output() {
    let mut service = FakeService {
        fail_list: Some(CowshedError::not_found(
            "project has not been adopted",
            "cowshed adopt",
        )),
        ..FakeService::default()
    };
    let cli = parse_args(["ls"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(&mut service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    assert_eq!(error.hint, "cowshed adopt");
    let (stdout, stderr) = output.into_inner();
    assert!(stdout.is_empty());
    assert!(stderr.is_empty());

    let unknown = parse_args(["unknown"]).unwrap_err();
    assert!(unknown.hint.contains("cowshed"));
}

#[tokio::test]
async fn service_teardown_runs_after_dispatch_failure_and_preserves_primary_error() {
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let service = FakeService {
        fail_list: Some(CowshedError::not_found(
            "project has not been adopted",
            "cowshed adopt",
        )),
        shutdowns: Some(Arc::clone(&shutdowns)),
        shutdown_error: Some(CowshedError::internal("shutdown failed")),
        ..FakeService::default()
    };
    let cli = parse_args(["ls"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);

    let error = dispatch_and_shutdown(service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap_err();

    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(error.code, ErrorCode::NotFound);
    assert!(error.message.contains("project has not been adopted"));
    assert!(error.message.contains("shutdown failed"));
}

struct CreateRequest {
    name: String,
    reply: oneshot::Sender<Result<WorkspaceInfo>>,
}

struct SerializedCreateService {
    sender: mpsc::Sender<CreateRequest>,
}

#[async_trait]
impl CliService for SerializedCreateService {
    async fn create(&mut self, name: &str, _options: CreateOptions) -> Result<WorkspaceInfo> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(CreateRequest {
                name: name.into(),
                reply,
            })
            .await
            .unwrap();
        response.await.unwrap()
    }

    async fn adopt(&mut self, _: AdoptOptions) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn fork(&mut self, _: &str, _: &str) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn checkpoint(&mut self, _: &str, _: CheckpointOptions) -> Result<String> {
        unreachable!()
    }
    async fn restore(&mut self, _: &str, _: &str) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn ensure_current(&mut self, _: PathBuf) -> Result<EnsureReport> {
        unreachable!()
    }
    async fn list(&mut self) -> Result<Vec<WorkspaceInfo>> {
        unreachable!()
    }
    async fn path(&mut self, _: &str, _: bool) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn remove(&mut self, _: &str, _: RemoveOptions) -> Result<()> {
        unreachable!()
    }
    async fn attach(&mut self, _: &str, _: AttachOptions) -> Result<()> {
        unreachable!()
    }
    async fn detach(&mut self, _: &str) -> Result<()> {
        unreachable!()
    }
    async fn doctor(&mut self) -> Result<DoctorReport> {
        unreachable!()
    }
    async fn gc(&mut self, _: GcOptions) -> Result<GcReport> {
        unreachable!()
    }
    async fn push(&mut self, _: &str, _: PushOptions) -> Result<PushReport> {
        unreachable!()
    }
    async fn rebase(&mut self, _: &str, _: RebaseOptions) -> Result<GitOid> {
        unreachable!()
    }
    async fn land(&mut self, _: &str, _: LandOptions) -> Result<LandReport> {
        unreachable!()
    }
    async fn exec(
        &mut self,
        _: ExecCommand,
        _: ExecPresentation,
        _: &mut (dyn Write + Send),
        _: &mut (dyn Write + Send),
    ) -> Result<ExecResult> {
        unreachable!()
    }

    async fn shutdown(self) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_invocations_serialize_same_name_create() {
    let (sender, mut receiver) = mpsc::channel::<CreateRequest>(8);
    let actor = tokio::spawn(async move {
        let mut names = HashSet::new();
        while let Some(request) = receiver.recv().await {
            let result = if names.insert(request.name.clone()) {
                Ok(workspace(&request.name, WorkspaceState::Attached))
            } else {
                Err(CowshedError::conflict(
                    format!("workspace {} already exists", request.name),
                    "choose a different workspace name",
                ))
            };
            let _ = request.reply.send(result);
        }
    });
    let invoke = |sender| async move {
        let mut service = SerializedCreateService { sender };
        let cli = parse_args(["new", "raven"]).unwrap();
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        dispatch(&mut service, cli, tokio::io::empty(), &mut output).await
    };
    let (first, second) = tokio::join!(invoke(sender.clone()), invoke(sender.clone()));
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let conflict = first.err().or_else(|| second.err()).unwrap();
    assert_eq!(conflict.code, ErrorCode::Conflict);
    drop(sender);
    actor.await.unwrap();
}
