use async_trait::async_trait;
use cowshed_cli::args::parse_args;
use cowshed_cli::output::Output;
use cowshed_cli::runtime::{
    CliService, ExecCommand, ExecPresentation, ExecResult, dispatch, dispatch_and_shutdown,
};
use cowshed_core::api::*;
use cowshed_core::metadata::{ImageFormat, WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
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
        self.events.push(format!("rm:{name}:{}", options.force));
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
            "rm:raven:true",
            "attach:raven:true",
            "detach:raven",
            "doctor",
        ]
    );
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
