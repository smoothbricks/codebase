use async_trait::async_trait;
use cowshed_cli::args::parse_args;
use cowshed_cli::output::Output;
use cowshed_cli::runtime::{
    CliService, ExecCommand, ExecPresentation, ExecResult, ProjectWorkspaces, dispatch,
    dispatch_and_shutdown,
};
use cowshed_core::api::*;
use cowshed_core::metadata::{
    ImageFormat, WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
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
    listed_workspaces: Option<Vec<WorkspaceInfo>>,
    listed_projects: Vec<ProjectWorkspaces>,
    other_adopted_projects: usize,
    fail_push: Option<CowshedError>,
    fail_reconcile_gateway: Option<CowshedError>,
    adopt_options: Option<AdoptOptions>,
    push_options: Option<PushOptions>,
    rebase_options: Option<RebaseOptions>,
    land_options: Option<LandOptions>,
    workspace_at_error: Option<CowshedError>,
    /// What `workspace_at` reports for the invocation cwd: `None` means the command was not run
    /// inside any mounted workspace, which is what makes the refusal path testable.
    cwd_workspace: Option<String>,
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
            listed_workspaces: None,
            listed_projects: Vec::new(),
            other_adopted_projects: 0,
            fail_push: None,
            fail_reconcile_gateway: None,
            cwd_workspace: None,
            adopt_options: None,
            push_options: None,
            rebase_options: None,
            land_options: None,
            workspace_at_error: None,
            gc_candidates: Vec::new(),
            shutdowns: None,
            shutdown_error: None,
        }
    }
}

#[async_trait]
impl CliService for FakeService {
    async fn reconcile_gateway(&mut self) -> Result<()> {
        match self.fail_reconcile_gateway.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceInfo> {
        self.adopt_options = Some(options.clone());
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

    async fn rename(&mut self, source: &str, destination: &str) -> Result<WorkspaceInfo> {
        self.events.push(format!("rename:{source}:{destination}"));
        Ok(workspace(destination, WorkspaceState::Attached))
    }

    async fn move_checkout(&mut self, destination: &std::path::Path) -> Result<WorkspaceInfo> {
        self.events
            .push(format!("move-checkout:{}", destination.display()));
        Ok(workspace("main", WorkspaceState::Attached))
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

    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceInfo> {
        self.events.push(format!("workspace-at:{}", path.display()));
        if let Some(error) = self.workspace_at_error.take() {
            return Err(error);
        }
        match self.cwd_workspace.clone() {
            Some(name) => Ok(workspace(&name, WorkspaceState::Attached)),
            None => Err(CowshedError::not_found(
                "no mounted workspace contains the invocation directory",
                "run cowshed from inside a mounted workspace",
            )),
        }
    }

    async fn list(&mut self) -> Result<Vec<WorkspaceInfo>> {
        self.events.push("ls".into());
        if let Some(error) = self.fail_list.take() {
            return Err(error);
        }
        Ok(self.listed_workspaces.take().unwrap_or_else(|| {
            vec![
                workspace("zebra", WorkspaceState::Detached),
                workspace("main", WorkspaceState::Attached),
            ]
        }))
    }

    async fn list_all(&mut self) -> Result<Vec<ProjectWorkspaces>> {
        self.events.push("ls-all".into());
        Ok(std::mem::take(&mut self.listed_projects))
    }

    async fn other_adopted_project_count(&mut self) -> Result<usize> {
        self.events.push("project-count".into());
        Ok(self.other_adopted_projects)
    }

    async fn path(&mut self, name: &str, no_attach: bool) -> Result<WorkspaceInfo> {
        self.events.push(format!("path:{name}:{no_attach}"));
        Ok(workspace(name, WorkspaceState::Attached))
    }

    async fn remove(&mut self, name: &str, options: RemoveOptions) -> Result<RemoveReport> {
        self.events.push(format!(
            "rm:{name}:{}:{}:{}",
            options.force, options.restore, options.abandon
        ));
        // Only an authorized abandonment has anything to report, and the dispatcher has to print
        // exactly that.
        Ok(RemoveReport {
            abandoned: options.abandon.then(|| AbandonedWork {
                head: GitOid::new("4".repeat(40)).expect("fixed head"),
                target_branch: "main".to_owned(),
                target_head: Some(GitOid::new("1".repeat(40)).expect("fixed tip")),
                unlanded_commits: 3,
                bundle: PathBuf::from(format!(
                    "/store/acme/widget/sessions/.trash/{name}-{}.bundle",
                    "4".repeat(40)
                )),
            }),
        })
    }

    async fn attach(&mut self, name: &str, options: AttachOptions) -> Result<WorkspaceInfo> {
        self.events
            .push(format!("attach:{name}:{}", options.browse));
        Ok(workspace(name, WorkspaceState::Attached))
    }

    async fn detach(&mut self, name: &str) -> Result<()> {
        self.events.push(format!("detach:{name}"));
        Ok(())
    }

    async fn resize(&mut self, name: &str, capacity: &str) -> Result<ResizeResult> {
        self.events.push(format!("resize:{name}:{capacity}"));
        Ok(ResizeResult {
            workspace: WorkspaceName::new(name).unwrap(),
            previous_capacity: "100g".to_owned(),
            capacity: capacity.to_owned(),
        })
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
    workspace_for("acme/widget", name, state)
}

fn workspace_for(repo_id: &str, name: &str, state: WorkspaceState) -> WorkspaceInfo {
    WorkspaceInfo {
        repo_id: RepoId::parse(repo_id).unwrap(),
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
    assert_eq!(
        stderr,
        b"cowshed: created main.asif for acme/widget (capacity 100g, asif)\nnext: cowshed new <name>\n"
    );

    let (_, stdout, stderr) = run(&mut service, ["new", "raven", "--browse"]).await;
    assert_eq!(stdout, b"/mnt/raven\n");
    assert_eq!(stderr, b"next: cowshed exec raven -- <cmd>\n");

    let (_, stdout, stderr) = run(&mut service, ["ls"]).await;
    assert_eq!(
        stdout,
        b"main   mounted   main           /mnt/main\nzebra  detached  cowshed/zebra\n"
    );
    assert!(stderr.is_empty());

    let (_, stdout, _) = run(&mut service, ["path", "raven"]).await;
    assert_eq!(stdout, b"/mnt/raven\n");

    // `path --slot` answers from the mount paths the listing already carries: a slot's mountpoint
    // leaf *is* the slot, so there is no second record to consult or keep in step.
    service.listed_workspaces = Some(vec![
        workspace("main", WorkspaceState::Attached),
        WorkspaceInfo {
            mount: PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/slot@2"),
            ..workspace("raven", WorkspaceState::Attached)
        },
    ]);
    let (_, stdout, _) = run(&mut service, ["path", "--slot", "2"]).await;
    assert_eq!(stdout, b"/mnt/raven\n");
    let cli = parse_args(["path", "--slot", "5"]).unwrap();
    let mut sink = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(&mut service, cli, tokio::io::empty(), &mut sink)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    assert_eq!(error.message, "no workspace occupies slot 5");
    assert!(sink.into_inner().0.is_empty());

    let (code, stdout, stderr) = run(&mut service, ["exec", "raven", "--", "true"]).await;
    assert_eq!(code, 0);
    assert_eq!(stdout, b"out\xff");
    assert_eq!(stderr, b"err\0");

    let (_, stdout, _) = run(&mut service, ["rm", "raven", "--force"]).await;
    assert!(stdout.is_empty());
    let (_, stdout, _) = run(&mut service, ["attach", "raven", "--browse"]).await;
    assert_eq!(stdout, b"/mnt/raven\n");
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
            // `--slot` resolves through the listing, then paths the tenant it found.
            "ls",
            "path:raven:false",
            // A slot with no tenant stops at the listing.
            "ls",
            "exec:raven",
            "rm:raven:true:false:false",
            "attach:raven:true",
            "detach:raven",
            "doctor",
        ]
    );
}

#[tokio::test]
async fn list_all_groups_every_project_in_stable_tsv_and_json_shapes() {
    let projects = || {
        vec![
            ProjectWorkspaces {
                repo_id: RepoId::parse("zeta/tool").unwrap(),
                workspaces: vec![workspace_for(
                    "zeta/tool",
                    "warp-ceiling",
                    WorkspaceState::Detached,
                )],
            },
            ProjectWorkspaces {
                repo_id: RepoId::parse("alpha/widget").unwrap(),
                workspaces: vec![
                    workspace_for("alpha/widget", "zebra", WorkspaceState::Attached),
                    workspace_for("alpha/widget", "main", WorkspaceState::Attached),
                ],
            },
        ]
    };
    let mut service = FakeService {
        listed_projects: projects(),
        ..FakeService::default()
    };

    let (_, stdout, stderr) = run(&mut service, ["ls", "--all"]).await;
    assert_eq!(
        stdout,
        b"alpha/widget  main          mounted   main                  /mnt/main\n\
          alpha/widget  zebra         mounted   cowshed/zebra         /mnt/zebra\n\
          zeta/tool     warp-ceiling  detached  cowshed/warp-ceiling\n"
    );
    assert!(stderr.is_empty());
    assert_eq!(service.events, ["ls-all"]);

    let mut service = FakeService {
        listed_projects: projects(),
        ..FakeService::default()
    };
    let (_, stdout, stderr) = run(&mut service, ["ls", "--all", "--json"]).await;
    assert!(stderr.is_empty());
    let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
    assert_eq!(value["ok"], true);
    assert_eq!(value["result"][0]["repoId"], "alpha/widget");
    assert_eq!(value["result"][0]["workspaces"][0]["workspace"], "main");
    assert_eq!(value["result"][1]["repoId"], "zeta/tool");
    assert_eq!(
        value["result"][1]["workspaces"][0]["workspace"],
        "warp-ceiling"
    );
}

#[tokio::test]
async fn empty_project_list_names_other_projects_and_guides_to_list_all() {
    let mut service = FakeService {
        listed_workspaces: Some(Vec::new()),
        other_adopted_projects: 2,
        ..FakeService::default()
    };

    let (_, stdout, stderr) = run(&mut service, ["ls"]).await;
    assert!(stdout.is_empty());
    assert_eq!(
        stderr,
        b"cowshed: current repository has no workspaces or is not adopted; 2 other adopted \
          projects exist; run cowshed ls --all\n"
    );
    assert_eq!(service.events, ["ls", "project-count"]);
}

#[tokio::test]
async fn adopt_delegates_explicit_identity_and_quarantine_with_exact_output() {
    let mut service = FakeService::default();
    let (_, stdout, stderr) = run(
        &mut service,
        [
            "adopt",
            "/repo",
            "--capacity",
            "100g",
            "--repo-id",
            "local/widget",
            "--quarantine",
            "--json",
        ],
    )
    .await;
    assert_eq!(
        stdout,
        format!(
            "{{\"ok\":true,\"result\":{{\"workspace\":\"main\",\"mount\":\"/mnt/main\",\"baseCommit\":\"{}\"}}}}\n",
            "1".repeat(40)
        )
        .as_bytes()
    );
    assert_eq!(
        stderr,
        b"cowshed: created main.asif for acme/widget (capacity 100g, asif)\nnext: cowshed new <name>\n"
    );
    assert_eq!(
        service.adopt_options,
        Some(AdoptOptions {
            path: Some(PathBuf::from("/repo")),
            repo_id: Some(RepoId::parse("local/widget").unwrap()),
            capacity: Some("100g".into()),
            quarantine: true,
            image_format: None,
        })
    );

    let cli = parse_args(["adopt", "--repo-id", "not-a-repo", "--json"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(&mut service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Usage);
    assert!(error.message.contains("repository identity"));
    assert!(output.into_inner().0.is_empty());
}

/// Adoption is the durable state change; a gateway that is not ready is a
/// separate recoverable condition. Reporting it as the command's failure reads
/// as "adoption failed" and invites a destructive retry.
#[tokio::test]
async fn adopt_reports_the_mount_even_when_the_gateway_is_not_ready() {
    let mut service = FakeService {
        fail_reconcile_gateway: Some(CowshedError::environment_missing(
            "cowshed gateway is not available",
            "cowshed gateway start",
        )),
        ..Default::default()
    };

    let (exit, stdout, stderr) = run(&mut service, ["adopt", "/repo"]).await;

    assert_eq!(exit, 0, "adoption succeeded");
    assert_eq!(
        stdout, b"/mnt/main\n",
        "the mount is still the machine answer"
    );
    let stderr = String::from_utf8(stderr).unwrap();
    assert!(
        stderr.contains("adopted; the gateway is not ready yet"),
        "{stderr}"
    );
    assert!(stderr.contains("next: cowshed gateway start"), "{stderr}");
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

    // mv reports the destination's mount, like every verb that lands a workspace somewhere.
    let (_, stdout, stderr) = run(&mut service, ["mv", "raven", "kestrel"]).await;
    assert_eq!(stdout, b"/mnt/kestrel\n");
    assert_eq!(stderr, b"next: cowshed exec kestrel -- <cmd>\n");
    assert!(service.events.contains(&"rename:raven:kestrel".to_owned()));

    // `mv main` is the other grammar behind the same verb: the destination is a path, the checkout
    // moves rather than a workspace being renamed, and the hint takes the user to the new place.
    let (_, stdout, stderr) = run(&mut service, ["mv", "main", "/Users/dev/moved"]).await;
    assert_eq!(stdout, b"/mnt/main\n");
    assert_eq!(stderr, b"next: cd /Users/dev/moved\n");
    assert!(
        service
            .events
            .contains(&"move-checkout:/Users/dev/moved".to_owned())
    );
    assert!(
        !service
            .events
            .iter()
            .any(|event| event.starts_with("rename:main")),
        "moving the checkout must never route through the workspace rename"
    );

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


    let (_, stdout, stderr) = run(&mut service, ["gc", "--dry-run"]).await;
    assert_eq!(stdout, b"0\n");
    assert_eq!(
        stderr,
        b"cowshed: dry run examined 9 objects; 0 candidates, 0 bytes deletable\n"
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
            .any(|event| event == "rm:main:false:true:false")
    );
}

/// An authorized abandonment is not a silent one: passing the flag buys the deletion, not silence.
/// Both the tip that died and the bundle that is now its only copy have to reach the operator.
#[tokio::test]
async fn abandoning_removal_prints_what_it_destroyed_and_where_the_bundle_went() {
    let mut service = FakeService::default();
    let (_, stdout, stderr) = run(&mut service, ["rm", "raven", "--abandon"]).await;
    assert!(stdout.is_empty(), "human mode keeps stdout free of prose");
    let stderr = String::from_utf8(stderr).expect("utf-8 stderr");
    assert_eq!(
        stderr,
        format!(
            "cowshed: abandoned 3 commits at {tip} that main (at {main}) did not contain\n\
             cowshed: bundled to /store/acme/widget/sessions/.trash/raven-{tip}.bundle\n\
             next: cowshed gc\n",
            tip = "4".repeat(40),
            main = "1".repeat(40)
        )
    );
    assert!(
        service
            .events
            .iter()
            .any(|event| event == "rm:raven:false:false:true")
    );

    // An ordinary removal has nothing to announce, and says nothing.
    let mut service = FakeService::default();
    let (_, _, stderr) = run(&mut service, ["rm", "raven"]).await;
    assert_eq!(stderr, b"next: cowshed gc\n");

    // The machine-readable form carries the same facts, so a harness never has to scrape prose.
    let mut service = FakeService::default();
    let (_, stdout, _) = run(&mut service, ["rm", "raven", "--abandon", "--json"]).await;
    let envelope: serde_json::Value =
        serde_json::from_slice(&stdout).expect("machine-readable envelope");
    assert_eq!(
        envelope["result"]["abandoned"]["unlandedCommits"],
        serde_json::json!(3)
    );
    assert_eq!(
        envelope["result"]["abandoned"]["head"],
        serde_json::json!("4".repeat(40))
    );
}

#[tokio::test]
async fn attach_one_workspace_prints_its_mount() {
    let mut service = FakeService::default();
    let (_, stdout, stderr) = run(&mut service, ["attach", "raven", "--json"]).await;
    assert_eq!(
        stdout,
        format!(
            "{{\"ok\":true,\"result\":{{\"workspace\":\"raven\",\"mount\":\"/mnt/raven\",\"baseCommit\":\"{}\"}}}}\n",
            "1".repeat(40)
        )
        .as_bytes()
    );
    assert!(stderr.is_empty());
    assert_eq!(service.events, ["attach:raven:false"]);
}

#[tokio::test]
async fn attach_without_name_reattaches_the_project_detached_sessions() {
    let mut service = FakeService {
        cwd_workspace: Some("main".into()),
        listed_workspaces: Some(vec![
            workspace("main", WorkspaceState::Attached),
            workspace("raven", WorkspaceState::Detached),
            workspace("fox", WorkspaceState::Attached),
        ]),
        ..FakeService::default()
    };
    let (_, stdout, stderr) = run(&mut service, ["attach"]).await;
    assert_eq!(stdout, b"/mnt/raven\n");
    assert!(stderr.is_empty());
    assert!(
        service
            .events
            .iter()
            .any(|event| event.starts_with("workspace-at:"))
    );
    assert!(service.events.iter().any(|event| event == "ls"));
    assert!(
        service
            .events
            .iter()
            .any(|event| event == "attach:raven:false")
    );
    assert!(
        !service
            .events
            .iter()
            .any(|event| event.starts_with("attach:main:") || event.starts_with("attach:fox:"))
    );
}

#[tokio::test]
async fn attach_all_reattaches_every_detached_session_store_wide() {
    let mut service = FakeService {
        listed_projects: vec![
            ProjectWorkspaces {
                repo_id: RepoId::parse("zeta/tool").unwrap(),
                workspaces: vec![
                    workspace_for("zeta/tool", "main", WorkspaceState::Attached),
                    workspace_for("zeta/tool", "warp", WorkspaceState::Detached),
                ],
            },
            ProjectWorkspaces {
                repo_id: RepoId::parse("alpha/widget").unwrap(),
                workspaces: vec![
                    workspace_for("alpha/widget", "main", WorkspaceState::Attached),
                    workspace_for("alpha/widget", "raven", WorkspaceState::Detached),
                    workspace_for("alpha/widget", "fox", WorkspaceState::Attached),
                ],
            },
        ],
        ..FakeService::default()
    };
    let (_, stdout, stderr) = run(&mut service, ["attach", "--all"]).await;
    assert_eq!(stdout, b"/mnt/warp\n/mnt/raven\n");
    assert!(stderr.is_empty());
    assert_eq!(
        service.events,
        ["ls-all", "attach:warp:false", "attach:raven:false"]
    );
}

#[tokio::test]
async fn attach_reports_no_workspace_when_the_scope_has_no_detached_session() {
    let mut service = FakeService {
        cwd_workspace: Some("main".into()),
        listed_workspaces: Some(vec![
            workspace("main", WorkspaceState::Attached),
            workspace("fox", WorkspaceState::Attached),
        ]),
        ..FakeService::default()
    };
    let cli = parse_args(["attach", "--json"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(&mut service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    assert_eq!(error.message, "no detached session workspace found");
    assert!(output.into_inner().0.is_empty());
}

#[tokio::test]
async fn attach_refuses_an_ambiguous_project_without_partial_output() {
    let mut service = FakeService {
        workspace_at_error: Some(CowshedError::conflict(
            "/tmp/overlap is contained in multiple active workspace mounts",
            "repair overlapping workspace mounts and retry",
        )),
        listed_workspaces: Some(vec![workspace("raven", WorkspaceState::Detached)]),
        ..FakeService::default()
    };
    let cli = parse_args(["attach", "--json"]).unwrap();
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    let error = dispatch(&mut service, cli, tokio::io::empty(), &mut output)
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Conflict);
    assert!(error.message.contains("multiple active workspace mounts"));
    assert!(output.into_inner().0.is_empty());
    assert!(
        !service
            .events
            .iter()
            .any(|event| event.starts_with("attach:"))
    );
}


#[tokio::test]
async fn gc_dry_run_zero_and_unicode_candidates_keep_streams_separate() {
    let mut empty = FakeService::default();
    let (_, stdout, stderr) = run(&mut empty, ["gc", "--dry-run"]).await;
    assert_eq!(stdout, b"0\n");
    assert_eq!(
        stderr,
        b"cowshed: dry run examined 9 objects; 0 candidates, 0 bytes deletable\n"
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
        b"cowshed: would delete /tmp/\xe5\x9b\x9e\xe6\x94\xb6 space/checkpoint (1234 bytes; reason: expired checkpoint)\ncowshed: dry run examined 9 objects; 1 candidate, 1234 bytes deletable\n"
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
        b"cowshed: would delete /tmp/\xe5\x9b\x9e\xe6\x94\xb6 space/checkpoint (1234 bytes; reason: expired checkpoint)\ncowshed: dry run examined 9 objects; 1 candidate, 1234 bytes deletable\n"
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

    async fn rename(&mut self, _: &str, _: &str) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn move_checkout(&mut self, _: &std::path::Path) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn checkpoint(&mut self, _: &str, _: CheckpointOptions) -> Result<String> {
        unreachable!()
    }
    async fn restore(&mut self, _: &str, _: &str) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn workspace_at(&mut self, _: PathBuf) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn list(&mut self) -> Result<Vec<WorkspaceInfo>> {
        unreachable!()
    }
    async fn path(&mut self, _: &str, _: bool) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn remove(&mut self, _: &str, _: RemoveOptions) -> Result<RemoveReport> {
        unreachable!()
    }
    async fn attach(&mut self, _: &str, _: AttachOptions) -> Result<WorkspaceInfo> {
        unreachable!()
    }
    async fn detach(&mut self, _: &str) -> Result<()> {
        unreachable!()
    }
    async fn resize(&mut self, _: &str, _: &str) -> Result<ResizeResult> {
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

#[test]
fn skill_install_parses_repeated_harnesses_once_and_validates_names() {
    use cowshed_cli::args::{Command, SkillCommand};

    let parsed = parse_args([
        "skill",
        "install",
        "--harness",
        "cursor",
        "--harness",
        "cursor",
    ])
    .expect("cursor is a known harness");
    match parsed.command {
        Command::Skill(args) => {
            assert_eq!(args.action, SkillCommand::Install);
            assert_eq!(args.harnesses, ["cursor"], "a repeated harness is deduped");
        }
        other => panic!("{other:?}"),
    }
    assert!(parsed.global.project.is_none());

    let project = parse_args([
        "skill",
        "install",
        "--harness",
        "github-copilot",
        "--project",
        "/repo",
    ])
    .expect("github-copilot is a known harness");
    match project.command {
        Command::Skill(args) => assert_eq!(args.harnesses, ["github-copilot"]),
        other => panic!("{other:?}"),
    }
    assert_eq!(project.global.project, Some(PathBuf::from("/repo")));

    // Names come from the generated snapshot, which uses upstream's spelling.
    assert!(
        parse_args(["skill", "install", "--harness", "copilot"]).is_err(),
        "the harness is spelled github-copilot"
    );
    assert!(parse_args(["skill", "install", "--harness", "nonesuch"]).is_err());
}

#[test]
fn skill_requires_a_known_action_and_takes_no_positional_arguments() {
    assert!(parse_args(["skill"]).is_err());
    assert!(parse_args(["skill", "uninstall"]).is_err());
    assert!(parse_args(["skill", "install", "extra"]).is_err());
    assert!(parse_args(["skill", "install", "--nonesuch"]).is_err());
}

/// Spec 06_cli.md rule 4: every hinted command exists in the parser.
///
/// Scans `hint(` / `with_hint(` and CowshedError constructors that take a hint.
/// Each complete `"cowshed …"` literal is tokenized (placeholders dropped) and
/// fed to `parse_args`, so `cowshed setup --nonesuch` fails just like an
/// unknown verb. CommandSpec `about` prose is not a hint call site.
#[test]
fn every_next_hint_verb_in_source_is_a_registered_command() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut source = String::new();
    collect_rust_source(&root, &mut source);
    let mut hints = HashSet::new();
    for site in [
        ".hint(",
        "with_hint(",
        "CowshedError::new(",
        "CowshedError::usage(",
        "CowshedError::not_found(",
        "CowshedError::conflict(",
        "CowshedError::environment_missing(",
        "::usage(",
        "::not_found(",
        "::conflict(",
        "::environment_missing(",
    ] {
        let mut rest = source.as_str();
        while let Some(start) = rest.find(site) {
            let window = rest[start..].get(..512).unwrap_or(&rest[start..]);
            rest = &rest[start + site.len()..];
            let mut quoted = window;
            while let Some(quote) = quoted.find("\"cowshed ") {
                let body = &quoted[quote + 1..];
                let Some(end) = body.find('"') else {
                    break;
                };
                let hint = body[..end].trim_end_matches('\\');
                if !hint.is_empty() {
                    hints.insert(hint.to_owned());
                }
                quoted = &body[end + 1..];
            }
        }
    }

    assert!(
        !hints.is_empty(),
        "expected at least one `cowshed …` hint string in src/"
    );
    for hint in hints {
        for part in hint.split(';') {
            let argv: Vec<&str> = part
                .split_whitespace()
                .filter(|token| *token != "cowshed")
                .filter(|token| !is_hint_placeholder(token))
                .collect();
            if argv.is_empty() || matches!(argv[0], "--help" | "--project" | "help") {
                continue;
            }
            let parsed = parse_args(argv.iter().copied());
            let unknown = parsed.as_ref().err().is_some_and(|error| {
                error.message.starts_with("unknown command")
                    || error.message.starts_with("unknown flag")
            });
            assert!(
                !unknown,
                "hint `{hint}` does not parse: {parsed:?}"
            );
        }
    }
}

fn is_hint_placeholder(token: &str) -> bool {
    token.starts_with('<')
        || token.starts_with('[')
        || token.contains("{}")
        || token == "{}"
}


fn collect_rust_source(path: &std::path::Path, out: &mut String) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("read src") {
            collect_rust_source(&entry.expect("dirent").path(), out);
        }
        return;
    }
    if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
        out.push_str(&std::fs::read_to_string(path).expect("read rust source"));
        out.push('\n');
    }
}


