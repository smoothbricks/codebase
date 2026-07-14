use crate::args::{AdoptArgs, Cli, Command, ExecArgs, StdinSource as CliStdinSource};
use crate::output::Output;
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use cowshed_core::api::server::{ConnectionAuthority, serve_controller_connection};
use cowshed_core::api::{
    AdoptOptions, AttachOptions, CommandArg, Coordinator, CreateOptions, DoctorReport, EmptyResult,
    ExecRequest, ExitStatus, JobInfo, JobStream, MountResult, OutputPublication, PublicationPolicy,
    RemoveOptions, RevisionTarget, RunSandboxMode, StdinSource as CoreStdinSource, WorkspaceInfo,
    WorkspacePath, WorkspaceState, validate_command_argv,
};
use cowshed_core::git::GitRepository;
use cowshed_core::metadata::WorkspaceName;
use cowshed_core::runtime::ProjectRuntime;
use cowshed_core::{CowshedError, ErrorCode, Result};
use std::collections::HashMap;
use std::io::{self, Write};
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::task::JoinHandle;

const DEFAULT_FOREGROUND_TIMEOUT: Duration = Duration::from_secs(120);

pub struct ExecCommand {
    pub workspace: String,
    pub request: ExecRequest,
    pub session: Option<String>,
    pub background: bool,
    pub timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecPresentation {
    Raw,
    Control,
}

pub struct ExecResult {
    pub info: JobInfo,
    pub backgrounded: bool,
}

#[async_trait]
pub trait CliService: Send {
    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceInfo>;
    async fn create(&mut self, name: &str, options: CreateOptions) -> Result<WorkspaceInfo>;
    async fn list(&mut self) -> Result<Vec<WorkspaceInfo>>;
    async fn path(&mut self, workspace: &str, no_attach: bool) -> Result<WorkspaceInfo>;
    async fn remove(&mut self, workspace: &str, options: RemoveOptions) -> Result<()>;
    async fn attach(&mut self, workspace: &str, options: AttachOptions) -> Result<()>;
    async fn detach(&mut self, workspace: &str) -> Result<()>;
    async fn doctor(&mut self) -> Result<DoctorReport>;
    async fn exec(
        &mut self,
        command: ExecCommand,
        presentation: ExecPresentation,
        stdout: &mut (dyn Write + Send),
        stderr: &mut (dyn Write + Send),
    ) -> Result<ExecResult>;
    async fn shutdown(self) -> Result<()>
    where
        Self: Sized;
}

pub struct ActorBridge {
    coordinator: Option<Coordinator>,
    connection: Option<JoinHandle<Result<()>>>,
    runtime: Option<ProjectRuntime>,
}

impl ActorBridge {
    pub async fn open(project_root: &Path) -> Result<Self> {
        let runtime = ProjectRuntime::open(project_root).await?;
        let (client, server) = match std::os::unix::net::UnixStream::pair() {
            Ok(pair) => pair,
            Err(error) => {
                let primary = CowshedError::environment_missing(
                    format!("could not create the in-process controller socket: {error}"),
                    "check the per-process file descriptor limit",
                );
                return Err(merge_primary(primary, runtime.shutdown().await.err()));
            }
        };
        let authority = ConnectionAuthority::Coordinator {
            repo_id: runtime.descriptor().repo_id.clone(),
        };
        let descriptor: OwnedFd = server.into();
        let connection = tokio::spawn(serve_controller_connection(
            descriptor,
            authority,
            runtime.router(),
        ));
        let client_descriptor: OwnedFd = client.into();

        let (cowshed, token) = match cowshed_core::Cowshed::connect(client_descriptor).await {
            Ok(connection) => connection,
            Err(primary) => {
                return Err(cleanup_open_failure(primary, connection, runtime).await);
            }
        };
        let project = match cowshed.open(project_root).await {
            Ok(project) => project,
            Err(primary) => {
                drop(token);
                drop(cowshed);
                return Err(cleanup_open_failure(primary, connection, runtime).await);
            }
        };
        let coordinator = match cowshed.coordinator(&project, token) {
            Ok(coordinator) => coordinator,
            Err(primary) => {
                drop(project);
                drop(cowshed);
                return Err(cleanup_open_failure(primary, connection, runtime).await);
            }
        };
        drop(project);
        drop(cowshed);
        Ok(Self {
            coordinator: Some(coordinator),
            connection: Some(connection),
            runtime: Some(runtime),
        })
    }

    fn coordinator(&self) -> Result<&Coordinator> {
        self.coordinator.as_ref().ok_or_else(|| {
            CowshedError::internal("the CLI controller bridge has already been shut down")
        })
    }

    pub async fn shutdown(mut self) -> Result<()> {
        drop(self.coordinator.take());
        let connection_error = match self.connection.take() {
            Some(connection) => join_connection(connection).await.err(),
            None => None,
        };
        let runtime_error = match self.runtime.take() {
            Some(runtime) => runtime.shutdown().await.err(),
            None => None,
        };
        combine_teardown(connection_error, runtime_error)
    }
}

#[async_trait]
impl CliService for ActorBridge {
    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceInfo> {
        Ok(self.coordinator()?.adopt(options).await?.into_info())
    }

    async fn create(&mut self, name: &str, options: CreateOptions) -> Result<WorkspaceInfo> {
        Ok(self.coordinator()?.create(name, options).await?.into_info())
    }

    async fn list(&mut self) -> Result<Vec<WorkspaceInfo>> {
        Ok(self
            .coordinator()?
            .project()
            .list()
            .await?
            .into_iter()
            .map(|workspace| workspace.into_info())
            .collect())
    }

    async fn path(&mut self, workspace: &str, no_attach: bool) -> Result<WorkspaceInfo> {
        let snapshot = self.coordinator()?.project().workspace(workspace).await?;
        if no_attach {
            return Ok(snapshot.into_info());
        }
        snapshot.attach(AttachOptions::default()).await?;
        snapshot.refresh_info().await
    }

    async fn remove(&mut self, workspace: &str, options: RemoveOptions) -> Result<()> {
        self.coordinator()?.destroy(workspace, options).await
    }

    async fn attach(&mut self, workspace: &str, options: AttachOptions) -> Result<()> {
        self.coordinator()?
            .project()
            .workspace(workspace)
            .await?
            .attach(options)
            .await
    }

    async fn detach(&mut self, workspace: &str) -> Result<()> {
        self.coordinator()?.detach(workspace).await.map(|_| ())
    }

    async fn doctor(&mut self) -> Result<DoctorReport> {
        self.coordinator()?.doctor().await
    }

    async fn exec(
        &mut self,
        command: ExecCommand,
        presentation: ExecPresentation,
        stdout: &mut (dyn Write + Send),
        stderr: &mut (dyn Write + Send),
    ) -> Result<ExecResult> {
        let worker = self.coordinator()?.worker(&command.workspace).await?;
        let job = if let Some(session_name) = command.session.as_deref() {
            worker
                .shell(Some(session_name))
                .await?
                .run(command.request)
                .await?
        } else {
            worker.exec(command.request).await?
        };

        if command.background {
            let info = job.status().await?;
            job.detach().await?;
            return Ok(ExecResult {
                info,
                backgrounded: true,
            });
        }

        match presentation {
            ExecPresentation::Control => {
                let wait = job.wait();
                tokio::pin!(wait);
                tokio::select! {
                    info = &mut wait => Ok(ExecResult { info: info?, backgrounded: false }),
                    () = tokio::time::sleep(command.timeout) => {
                        let info = job.status().await?;
                        job.detach().await?;
                        Ok(ExecResult { info, backgrounded: true })
                    }
                }
            }
            ExecPresentation::Raw => {
                let stdout_stream = job.logs(JobStream::Stdout, true).await?;
                let stderr_stream = job.logs(JobStream::Stderr, true).await?;
                let foreground = async {
                    let (info, stdout_result, stderr_result) = tokio::join!(
                        job.wait(),
                        pump_stream(stdout_stream, stdout),
                        pump_stream(stderr_stream, stderr),
                    );
                    stdout_result?;
                    stderr_result?;
                    info
                };
                tokio::pin!(foreground);
                tokio::select! {
                    info = &mut foreground => Ok(ExecResult { info: info?, backgrounded: false }),
                    () = tokio::time::sleep(command.timeout) => {
                        let info = job.status().await?;
                        job.detach().await?;
                        Ok(ExecResult { info, backgrounded: true })
                    }
                }
            }
        }
    }

    async fn shutdown(self) -> Result<()> {
        ActorBridge::shutdown(self).await
    }
}

async fn pump_stream(
    mut stream: cowshed_core::RawByteStream,
    writer: &mut (dyn Write + Send),
) -> Result<()> {
    while let Some(chunk) = stream.next().await {
        writer.write_all(&chunk?).map_err(output_error)?;
    }
    writer.flush().map_err(output_error)
}

fn output_error(error: io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("could not write child output: {error}"),
        "check that the output consumer is still connected",
    )
}

async fn cleanup_open_failure(
    primary: CowshedError,
    connection: JoinHandle<Result<()>>,
    runtime: ProjectRuntime,
) -> CowshedError {
    let connection_error = join_connection(connection).await.err();
    let runtime_error = runtime.shutdown().await.err();
    merge_primary(
        primary,
        combine_teardown(connection_error, runtime_error).err(),
    )
}

async fn join_connection(connection: JoinHandle<Result<()>>) -> Result<()> {
    match connection.await {
        Ok(result) => result,
        Err(error) => Err(CowshedError::internal(format!(
            "controller connection actor did not join: {error}"
        ))),
    }
}

fn combine_teardown(first: Option<CowshedError>, second: Option<CowshedError>) -> Result<()> {
    match (first, second) {
        (None, None) => Ok(()),
        (Some(error), None) | (None, Some(error)) => Err(error),
        (Some(first), Some(second)) => Err(CowshedError::new(
            ErrorCode::Internal,
            format!(
                "controller connection teardown failed: {}; project runtime shutdown also failed: {}",
                first.message, second.message
            ),
            format!("{}; {}", first.hint, second.hint),
        )),
    }
}

pub fn merge_primary(primary: CowshedError, teardown: Option<CowshedError>) -> CowshedError {
    match teardown {
        None => primary,
        Some(teardown) => CowshedError::new(
            primary.code,
            format!(
                "{}; controller teardown also failed: {}",
                primary.message, teardown.message
            ),
            format!("{}; teardown: {}", primary.hint, teardown.hint),
        ),
    }
}

pub async fn resolve_project_root(cli: &Cli) -> Result<PathBuf> {
    let candidate = cli.global.project.as_deref().or(match &cli.command {
        Command::Adopt(args) => args.path.as_deref(),
        _ => None,
    });
    let start = match candidate {
        Some(path) => path.to_path_buf(),
        None => std::env::current_dir().map_err(|error| {
            CowshedError::environment_missing(
                format!("could not determine the current directory: {error}"),
                "use --project <git-root>",
            )
        })?,
    };
    Ok(GitRepository::discover(start).await?.root().to_path_buf())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatchExit {
    pub code: i32,
}

pub async fn dispatch<S, R, W, E>(
    service: &mut S,
    cli: Cli,
    stdin: R,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    S: CliService,
    R: AsyncRead + Send + 'static,
    W: Write + Send,
    E: Write + Send,
{
    let json = cli.global.json;
    match cli.command {
        Command::Adopt(args) => {
            let options = adopt_options(args)?;
            let info = service.adopt(options).await?;
            emit_mount(output, json, &info)?;
            output.hint("cowshed new <name>").map_err(output_error)?;
            Ok(success())
        }
        Command::New(args) => {
            let options =
                CreateOptions {
                    revision: args.reference.map(os_revision).transpose()?,
                    from_workspace: args.from.map(WorkspaceName::new).transpose().map_err(
                        |error| usage(error.to_string(), "use a valid source workspace name"),
                    )?,
                    browse: args.browse,
                    slot: args.slot,
                };
            let info = service.create(&args.name, options).await?;
            emit_mount(output, json, &info)?;
            output
                .hint(&format!("cowshed exec {} -- <cmd>", args.name))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::List => {
            let mut workspaces = service.list().await?;
            workspaces.sort_by(|left, right| left.workspace.cmp(&right.workspace));
            if json {
                output.success(workspaces).map_err(output_error)?;
            } else {
                emit_workspace_tsv(output, &workspaces)?;
            }
            Ok(success())
        }
        Command::Path(args) => {
            let info = service.path(&args.workspace, args.no_attach).await?;
            if args.no_attach && info.state == WorkspaceState::Detached {
                output
                    .guidance("workspace is detached; returning its configured mount path")
                    .map_err(output_error)?;
            }
            emit_mount(output, json, &info)?;
            Ok(success())
        }
        Command::Exec(args) => {
            let command = exec_command(args, stdin)?;
            let presentation = if json {
                ExecPresentation::Control
            } else {
                ExecPresentation::Raw
            };
            let (stdout, stderr) = output.writers_mut();
            let result = service.exec(command, presentation, stdout, stderr).await?;
            if json {
                output.success(result.info).map_err(output_error)?;
                Ok(success())
            } else if result.backgrounded {
                output
                    .bare_line(result.info.job_id.get().to_string().as_bytes())
                    .map_err(output_error)?;
                Ok(success())
            } else {
                Ok(DispatchExit {
                    code: child_exit_code(&result.info)?,
                })
            }
        }
        Command::Remove(args) => {
            service
                .remove(
                    &args.workspace,
                    RemoveOptions {
                        force: args.force,
                        restore: false,
                    },
                )
                .await?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            }
            output.hint("cowshed gc").map_err(output_error)?;
            Ok(success())
        }
        Command::Attach(args) => {
            service
                .attach(
                    &args.workspace,
                    AttachOptions {
                        browse: args.browse,
                    },
                )
                .await?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Detach(args) => {
            service.detach(&args.workspace).await?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Doctor => {
            let report = service.doctor().await?;
            let healthy = report.healthy;
            if json {
                output.success(report).map_err(output_error)?;
            } else {
                emit_doctor(output, &report)?;
            }
            Ok(DispatchExit {
                code: if healthy { 0 } else { 5 },
            })
        }
    }
}

fn success() -> DispatchExit {
    DispatchExit { code: 0 }
}

fn adopt_options(args: AdoptArgs) -> Result<AdoptOptions> {
    Ok(AdoptOptions {
        path: args.path,
        repo_id: None,
        capacity: args.capacity.map(os_utf8).transpose()?,
        quarantine: false,
        image_format: None,
    })
}

fn os_revision(value: std::ffi::OsString) -> Result<RevisionTarget> {
    RevisionTarget::parse_cli(os_utf8(value)?).map_err(|error| {
        usage(
            format!("invalid revision: {error}"),
            "use a branch, full ref, or full object id",
        )
    })
}

fn os_utf8(value: std::ffi::OsString) -> Result<String> {
    value.into_string().map_err(|_| {
        usage(
            "this option requires valid UTF-8",
            "use UTF-8 for control options; child argv may contain arbitrary Unix bytes",
        )
    })
}

fn exec_command<R: AsyncRead + Send + 'static>(args: ExecArgs, stdin: R) -> Result<ExecCommand> {
    let argv: Vec<CommandArg> = args.argv.into_iter().map(CommandArg::from).collect();
    validate_command_argv(&argv).map_err(|error| {
        usage(
            format!("invalid child argv: {error}"),
            "remove NUL bytes and keep argv within the documented size limits",
        )
    })?;
    let stdin = match args.stdin {
        None => CoreStdinSource::Empty,
        Some(CliStdinSource::Stream) => CoreStdinSource::Stream(Box::pin(stdin)),
        Some(CliStdinSource::WorkspaceFile(path)) => {
            CoreStdinSource::WorkspaceFile(workspace_path(path, "stdin file")?)
        }
        Some(CliStdinSource::InlineBase64(data)) => {
            let text = os_utf8(data)?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(text.as_bytes())
                .map_err(|error| {
                    usage(
                        format!("invalid --stdin-base64 data: {error}"),
                        "use canonical base64 input",
                    )
                })?;
            CoreStdinSource::Inline(Bytes::from(decoded))
        }
    };
    let policy = if args.replace_output {
        PublicationPolicy::Replace
    } else {
        PublicationPolicy::CreateNew
    };
    let publication = |path: PathBuf, label| {
        Ok(OutputPublication {
            path: workspace_path(path, label)?,
            policy,
        })
    };
    let timeout = args
        .timeout
        .map(parse_duration)
        .transpose()?
        .unwrap_or(DEFAULT_FOREGROUND_TIMEOUT);
    Ok(ExecCommand {
        workspace: args.workspace,
        request: ExecRequest {
            argv,
            cwd: args
                .cwd
                .map(|path| workspace_path(path, "cwd"))
                .transpose()?,
            mode: if args.read_only {
                RunSandboxMode::ReadOnly
            } else {
                RunSandboxMode::ReadWrite
            },
            env: HashMap::new(),
            trace: None,
            stdin,
            stdout_copy: args
                .stdout_copy
                .map(|path| publication(path, "stdout copy"))
                .transpose()?,
            stderr_copy: args
                .stderr_copy
                .map(|path| publication(path, "stderr copy"))
                .transpose()?,
        },
        session: args.session,
        background: args.background,
        timeout,
    })
}

fn workspace_path(path: PathBuf, label: &str) -> Result<WorkspacePath> {
    WorkspacePath::new(path).map_err(|error| {
        usage(
            format!("invalid {label} path: {error}"),
            "use a normalized workspace-relative path",
        )
    })
}

fn parse_duration(value: std::ffi::OsString) -> Result<Duration> {
    let value = os_utf8(value)?;
    let (digits, multiplier) = if let Some(digits) = value.strip_suffix("ms") {
        (digits, 1_u64)
    } else if let Some(digits) = value.strip_suffix('s') {
        (digits, 1_000)
    } else if let Some(digits) = value.strip_suffix('m') {
        (digits, 60_000)
    } else if let Some(digits) = value.strip_suffix('h') {
        (digits, 3_600_000)
    } else {
        return Err(usage(
            "timeout must end in ms, s, m, or h",
            "for example: --timeout 500ms or --timeout 2m",
        ));
    };
    let count = digits.parse::<u64>().map_err(|_| {
        usage(
            "timeout must be a non-negative integer duration",
            "for example: --timeout 500ms or --timeout 2m",
        )
    })?;
    let millis = count.checked_mul(multiplier).ok_or_else(|| {
        usage(
            "timeout is too large",
            "choose a timeout that fits in 64-bit milliseconds",
        )
    })?;
    Ok(Duration::from_millis(millis))
}

fn emit_mount<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    info: &WorkspaceInfo,
) -> Result<()> {
    if json {
        output
            .success(MountResult {
                workspace: info.workspace.clone(),
                mount: info.mount.clone(),
                base_commit: info.base_commit.clone(),
            })
            .map_err(output_error)
    } else {
        output
            .bare_line(info.mount.as_os_str().as_bytes())
            .map_err(output_error)
    }
}

fn emit_workspace_tsv<W: Write, E: Write>(
    output: &mut Output<W, E>,
    workspaces: &[WorkspaceInfo],
) -> Result<()> {
    for workspace in workspaces {
        output
            .bare(workspace.workspace.as_str().as_bytes())
            .and_then(|()| output.bare(b"\t"))
            .and_then(|()| {
                output.bare(match workspace.state {
                    WorkspaceState::Attached => b"mounted",
                    WorkspaceState::Detached => b"detached",
                })
            })
            .and_then(|()| output.bare(b"\t"))
            .and_then(|()| output.bare(workspace.branch.as_deref().unwrap_or("").as_bytes()))
            .and_then(|()| output.bare(b"\t"))
            .and_then(|()| {
                if workspace.state == WorkspaceState::Attached {
                    output.bare(workspace.mount.as_os_str().as_bytes())
                } else {
                    Ok(())
                }
            })
            .and_then(|()| output.bare(b"\n"))
            .map_err(output_error)?;
    }
    Ok(())
}

fn emit_doctor<W: Write, E: Write>(output: &mut Output<W, E>, report: &DoctorReport) -> Result<()> {
    output
        .bare_line(if report.healthy {
            b"healthy"
        } else {
            b"unhealthy"
        })
        .map_err(output_error)?;
    for finding in &report.findings {
        output
            .guidance(&format!("[{}] {}", finding.code, finding.message))
            .map_err(output_error)?;
        if !finding.hint.is_empty() {
            output.hint(&finding.hint).map_err(output_error)?;
        }
    }
    Ok(())
}

fn child_exit_code(info: &JobInfo) -> Result<i32> {
    match info.exit {
        Some(ExitStatus::Exited { code }) => Ok(code),
        Some(ExitStatus::Signaled { signal, .. }) => Ok(128_i32.saturating_add(signal)),
        None => Err(CowshedError::internal(format!(
            "terminal job {} has no child exit status",
            info.job_id.get()
        ))),
    }
}

fn usage(message: impl Into<String>, hint: impl Into<String>) -> CowshedError {
    CowshedError::usage(message, hint)
}

pub async fn dispatch_and_shutdown<S, R, W, E>(
    mut service: S,
    cli: Cli,
    stdin: R,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    S: CliService,
    R: AsyncRead + Send + 'static,
    W: Write + Send,
    E: Write + Send,
{
    let primary = dispatch(&mut service, cli, stdin, output).await;
    let teardown = service.shutdown().await.err();
    match primary {
        Ok(exit) => match teardown {
            None => Ok(exit),
            Some(error) => Err(error),
        },
        Err(primary) => Err(merge_primary(primary, teardown)),
    }
}

pub async fn run_bridge_command<R, W, E>(
    cli: Cli,
    stdin: R,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    R: AsyncRead + Send + 'static,
    W: Write + Send,
    E: Write + Send,
{
    let root = resolve_project_root(&cli).await?;
    let bridge = ActorBridge::open(&root).await?;
    dispatch_and_shutdown(bridge, cli, stdin, output).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_are_exact_and_checked() {
        assert_eq!(
            parse_duration("500ms".into()).unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_duration("2m".into()).unwrap(),
            Duration::from_secs(120)
        );
        assert!(parse_duration("1.5s".into()).is_err());
        assert!(parse_duration("9d".into()).is_err());
    }

    #[test]
    fn teardown_error_preserves_primary_taxonomy_and_both_messages() {
        let primary = CowshedError::not_found("missing", "cowshed adopt");
        let teardown = CowshedError::internal("shutdown failed");
        let merged = merge_primary(primary, Some(teardown));
        assert_eq!(merged.code, ErrorCode::NotFound);
        assert!(merged.message.contains("missing"));
        assert!(merged.message.contains("shutdown failed"));
    }

    #[tokio::test]
    async fn real_unix_controller_connection_shuts_down_every_actor() {
        use cowshed_core::api::server::RouterHandle;
        use cowshed_core::repository::RepoId;
        use std::num::NonZeroUsize;

        let repo_id = RepoId::parse("acme/widget").unwrap();
        let (router, mut receiver) = RouterHandle::channel(NonZeroUsize::new(4).unwrap());
        let router_actor = tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                let (_, reply) = command.into_parts();
                let _ = reply.send(Err(CowshedError::internal(
                    "handshake-only test routed an unexpected request",
                )));
            }
        });
        let (client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let connection = tokio::spawn(serve_controller_connection(
            server.into(),
            ConnectionAuthority::Coordinator { repo_id },
            router.clone(),
        ));
        let (cowshed, token) = cowshed_core::Cowshed::connect(client.into()).await.unwrap();
        drop(token);
        drop(cowshed);
        tokio::time::timeout(Duration::from_secs(1), connection)
            .await
            .expect("controller connection actor leaked")
            .unwrap()
            .unwrap();
        drop(router);
        tokio::time::timeout(Duration::from_secs(1), router_actor)
            .await
            .expect("router actor leaked")
            .unwrap();
    }
}
