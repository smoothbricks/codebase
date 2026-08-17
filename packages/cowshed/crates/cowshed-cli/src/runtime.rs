use crate::args::{
    AdoptArgs, Cli, Command, ExecArgs, MoveDestination, StdinSource as CliStdinSource,
};
use crate::gateway_service;
use crate::output::Output;
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
pub use cowshed_core::api::ProjectWorkspaces;
use cowshed_core::api::server::{ConnectionAuthority, serve_controller_connection};
use cowshed_core::api::{
    AdoptOptions, AttachOptions, BranchName, CheckpointOptions, CheckpointResult, CommandArg,
    Coordinator, CreateOptions, DoctorReport, EmptyResult, EnsureAction, EnsureReport, ExecRequest,
    ExitStatus, ExpectedRefHead, GcOptions, GcReason, GcReport, GitOid, JobInfo, JobStream,
    LandOptions, LandReport, MountResult, OutputPublication, PublicationPolicy, PushOptions,
    PushReport, RebaseOptions, RemoveOptions, RemoveReport, ResizeResult, RevisionResult,
    RevisionTarget, RunSandboxMode, StdinSource as CoreStdinSource, WorkspaceInfo, WorkspacePath,
    WorkspaceState, validate_command_argv,
};
use cowshed_core::git::GitRepository;
use cowshed_core::metadata::{ImageCapacity, SlotId, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::ProjectRuntime;
use cowshed_core::storage::apfs::DEFAULT_IMAGE_CAPACITY;
use cowshed_core::{
    AdoptedProject, CowshedError, ErrorCode, NativeGatewayInventory, Result,
    validate_existing_host_storage,
};
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
    async fn fork(&mut self, source: &str, destination: &str) -> Result<WorkspaceInfo>;
    async fn rename(&mut self, source: &str, destination: &str) -> Result<WorkspaceInfo>;
    async fn move_checkout(&mut self, destination: &Path) -> Result<WorkspaceInfo>;
    async fn checkpoint(&mut self, workspace: &str, options: CheckpointOptions) -> Result<String>;
    async fn restore(&mut self, workspace: &str, label: &str) -> Result<WorkspaceInfo>;
    async fn ensure_current(&mut self, path: PathBuf) -> Result<EnsureReport>;
    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceInfo>;
    async fn list(&mut self) -> Result<Vec<WorkspaceInfo>>;
    async fn list_all(&mut self) -> Result<Vec<ProjectWorkspaces>> {
        Err(CowshedError::internal(
            "CLI service does not support store-wide workspace discovery",
        ))
    }
    async fn other_adopted_project_count(&mut self) -> Result<usize> {
        Ok(0)
    }
    async fn path(&mut self, workspace: &str, no_attach: bool) -> Result<WorkspaceInfo>;
    async fn remove(&mut self, workspace: &str, options: RemoveOptions) -> Result<RemoveReport>;
    async fn attach(&mut self, workspace: &str, options: AttachOptions) -> Result<()>;
    async fn detach(&mut self, workspace: &str) -> Result<()>;
    async fn resize(&mut self, workspace: &str, capacity: &str) -> Result<ResizeResult>;
    async fn doctor(&mut self) -> Result<DoctorReport>;
    async fn gc(&mut self, options: GcOptions) -> Result<GcReport>;
    async fn push(&mut self, workspace: &str, options: PushOptions) -> Result<PushReport>;
    async fn rebase(&mut self, workspace: &str, options: RebaseOptions) -> Result<GitOid>;
    async fn land(&mut self, workspace: &str, options: LandOptions) -> Result<LandReport>;
    async fn exec(
        &mut self,
        command: ExecCommand,
        presentation: ExecPresentation,
        stdout: &mut (dyn Write + Send),
        stderr: &mut (dyn Write + Send),
    ) -> Result<ExecResult>;
    async fn reconcile_gateway(&mut self) -> Result<()> {
        Ok(())
    }
    async fn shutdown(self) -> Result<()>
    where
        Self: Sized;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeOpenMode {
    Provision,
    ExistingOnly,
}

fn runtime_open_mode(command: &Command) -> RuntimeOpenMode {
    match command {
        Command::Adopt(_) => RuntimeOpenMode::Provision,
        Command::New(_)
        | Command::Fork(_)
        | Command::Move(_)
        | Command::Checkpoint(_)
        | Command::Restore(_)
        | Command::Ensure(_)
        | Command::List(_)
        | Command::Path(_)
        | Command::Exec(_)
        | Command::Remove(_)
        | Command::Attach(_)
        | Command::Detach(_)
        | Command::Resize(_)
        | Command::Gc(_)
        | Command::Push(_)
        | Command::Rebase(_)
        | Command::Land(_)
        | Command::Gateway(_)
        | Command::Sccache(_)
        | Command::Skill(_)
        | Command::Doctor => RuntimeOpenMode::ExistingOnly,
    }
}

fn runtime_open_repo_id(command: &Command) -> Result<Option<RepoId>> {
    match command {
        Command::Adopt(args) => args.repo_id.as_deref().map(os_repo_id_ref).transpose(),
        _ => Ok(None),
    }
}

pub struct ActorBridge {
    coordinator: Option<Coordinator>,
    connection: Option<JoinHandle<Result<()>>>,
    runtime: Option<ProjectRuntime>,
}

impl ActorBridge {
    pub async fn open_for_adopt(
        project_root: &Path,
        requested_repo_id: Option<RepoId>,
    ) -> Result<Self> {
        let runtime = ProjectRuntime::open_for_adopt(project_root, requested_repo_id).await?;
        Self::from_runtime(project_root, runtime).await
    }

    pub async fn open_existing(project_root: &Path) -> Result<Self> {
        let runtime = ProjectRuntime::open_existing(project_root).await?;
        Self::from_runtime(project_root, runtime).await
    }

    async fn from_runtime(project_root: &Path, runtime: ProjectRuntime) -> Result<Self> {
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

    fn repo_id(&self) -> Result<&RepoId> {
        self.runtime
            .as_ref()
            .map(|runtime| &runtime.descriptor().repo_id)
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })
    }

    async fn adopted_projects(&self) -> Result<Vec<AdoptedProject>> {
        let home = gateway_service::canonical_home()?;
        let storage = validate_existing_host_storage(&home).await?;
        NativeGatewayInventory::new(storage)
            .adopted_projects()
            .await
            .map_err(project_inventory_error)
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

async fn list_adopted_project(project: &AdoptedProject) -> Result<Vec<WorkspaceInfo>> {
    let mut bridge = ActorBridge::open_existing(&project.project_root).await?;
    let primary = bridge.list().await;
    let teardown = bridge.shutdown().await.err();
    match primary {
        Ok(workspaces) => match teardown {
            Some(error) => Err(error),
            None => Ok(workspaces),
        },
        Err(primary) => Err(merge_primary(primary, teardown)),
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

    async fn fork(&mut self, source: &str, destination: &str) -> Result<WorkspaceInfo> {
        Ok(self
            .coordinator()?
            .fork(source, destination)
            .await?
            .into_info())
    }

    async fn rename(&mut self, source: &str, destination: &str) -> Result<WorkspaceInfo> {
        Ok(self
            .coordinator()?
            .rename(source, destination)
            .await?
            .into_info())
    }

    async fn move_checkout(&mut self, destination: &Path) -> Result<WorkspaceInfo> {
        Ok(self
            .coordinator()?
            .move_checkout(destination)
            .await?
            .into_info())
    }

    async fn checkpoint(&mut self, workspace: &str, options: CheckpointOptions) -> Result<String> {
        self.coordinator()?
            .worker(workspace)
            .await?
            .checkpoint(options)
            .await
    }

    async fn restore(&mut self, workspace: &str, label: &str) -> Result<WorkspaceInfo> {
        self.coordinator()?.restore(workspace, label).await?;
        self.coordinator()?
            .project()
            .workspace(workspace)
            .await?
            .refresh_info()
            .await
    }

    async fn ensure_current(&mut self, path: PathBuf) -> Result<EnsureReport> {
        self.coordinator()?
            .project()
            .workspace_at(path.clone())
            .await?
            .ensure_observed_at(Some(&path))
            .await
    }

    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceInfo> {
        self.coordinator()?
            .project()
            .workspace_at(path)
            .await?
            .refresh_info()
            .await
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

    async fn list_all(&mut self) -> Result<Vec<ProjectWorkspaces>> {
        let current_repo = self.repo_id()?.clone();
        let projects = self.adopted_projects().await?;
        let mut grouped = Vec::with_capacity(projects.len());
        for project in projects {
            let workspaces = if project.repo_id == current_repo {
                self.list().await?
            } else {
                list_adopted_project(&project).await?
            };
            grouped.push(ProjectWorkspaces {
                repo_id: project.repo_id,
                workspaces,
            });
        }
        Ok(grouped)
    }

    async fn other_adopted_project_count(&mut self) -> Result<usize> {
        let current_repo = self.repo_id()?.clone();
        Ok(self
            .adopted_projects()
            .await?
            .into_iter()
            .filter(|project| project.repo_id != current_repo)
            .count())
    }

    async fn path(&mut self, workspace: &str, no_attach: bool) -> Result<WorkspaceInfo> {
        let snapshot = self.coordinator()?.project().workspace(workspace).await?;
        if no_attach {
            return Ok(snapshot.into_info());
        }
        snapshot.attach(AttachOptions::default()).await?;
        snapshot.refresh_info().await
    }

    async fn remove(&mut self, workspace: &str, options: RemoveOptions) -> Result<RemoveReport> {
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

    async fn resize(&mut self, workspace: &str, capacity: &str) -> Result<ResizeResult> {
        self.coordinator()?.resize(workspace, capacity).await
    }

    async fn doctor(&mut self) -> Result<DoctorReport> {
        self.coordinator()?.doctor().await
    }

    async fn gc(&mut self, options: GcOptions) -> Result<GcReport> {
        self.coordinator()?.gc(options).await
    }

    async fn push(&mut self, workspace: &str, options: PushOptions) -> Result<PushReport> {
        self.coordinator()?
            .worker(workspace)
            .await?
            .push(options)
            .await
    }

    async fn rebase(&mut self, workspace: &str, options: RebaseOptions) -> Result<GitOid> {
        self.coordinator()?.rebase(workspace, options).await
    }

    async fn land(&mut self, workspace: &str, options: LandOptions) -> Result<LandReport> {
        self.coordinator()?.land(workspace, options).await
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

    async fn reconcile_gateway(&mut self) -> Result<()> {
        let repo_id = self.coordinator()?.project().repo_id().clone();
        gateway_service::reconcile_native_project(&repo_id)
            .await
            .map(|_| ())
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

fn project_inventory_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("project inventory failed: {error}"),
        "cowshed doctor --json",
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
    if requires_gateway_before_dispatch(&cli.command) {
        service.reconcile_gateway().await?;
    }
    let json = cli.global.json;
    match cli.command {
        Command::Adopt(args) => {
            let options = adopt_options(args)?;
            // The capacity is resolved here rather than left implicit, so the line that reports
            // the adoption states the size the image was actually minted at.
            let capacity = options
                .capacity
                .clone()
                .unwrap_or_else(|| DEFAULT_IMAGE_CAPACITY.to_string());
            let info = service.adopt(options).await?;
            // Adoption is the durable state change and it has already happened:
            // the tree is copied and the image is mounted. A gateway that is not
            // installed or running is a separate, recoverable condition, and
            // reporting it as the command's failure reads as "adoption failed"
            // and invites a destructive retry. Report the mount, then the
            // gateway, and let `doctor` be the health verdict.
            let gateway = service.reconcile_gateway().await;
            emit_mount(output, json, &info)?;
            output
                .guidance(&format!("image capacity {capacity}"))
                .map_err(output_error)?;
            match gateway {
                Ok(()) => output.hint("cowshed new <name>").map_err(output_error)?,
                Err(error) => {
                    output
                        .guidance(&format!(
                            "adopted; the gateway is not ready yet: {}",
                            error.message
                        ))
                        .map_err(output_error)?;
                    output.hint(&error.hint).map_err(output_error)?;
                }
            }
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
                    register: args.register,
                    git_worktree: args.git_worktree,
                };
            let info = service.create(&args.name, options).await?;
            service.reconcile_gateway().await?;
            emit_mount(output, json, &info)?;
            output
                .hint(&format!("cowshed exec {} -- <cmd>", args.name))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::Move(args) => {
            let info = match &args.destination {
                MoveDestination::Workspace(name) => service.rename(&args.source, name).await?,
                MoveDestination::Checkout(path) => service.move_checkout(path).await?,
            };
            service.reconcile_gateway().await?;
            emit_mount(output, json, &info)?;
            let hint = match &args.destination {
                MoveDestination::Workspace(name) => format!("cowshed exec {name} -- <cmd>"),
                MoveDestination::Checkout(path) => format!("cd {}", path.display()),
            };
            output.hint(&hint).map_err(output_error)?;
            Ok(success())
        }
        Command::Fork(args) => {
            let info = service.fork(&args.source, &args.destination).await?;
            service.reconcile_gateway().await?;
            emit_mount(output, json, &info)?;
            output
                .hint(&format!("cowshed exec {} -- <cmd>", args.destination))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::Checkpoint(args) => {
            let workspace = resolve_workspace(service, args.workspace, "checkpoint").await?;
            let label = service
                .checkpoint(
                    &workspace,
                    CheckpointOptions {
                        label: args.label.map(os_utf8).transpose()?,
                        keep: args.keep,
                    },
                )
                .await?;
            if json {
                output
                    .success(CheckpointResult { label })
                    .map_err(output_error)?;
            } else {
                output.bare_line(label.as_bytes()).map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Restore(args) => {
            let label = os_utf8(args.label)?;
            let info = service.restore(&args.workspace, &label).await?;
            service.reconcile_gateway().await?;
            emit_mount(output, json, &info)?;
            output
                .hint(&format!("cowshed exec {} -- git status", args.workspace))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::Ensure(args) => {
            let report = service.ensure_current(invocation_cwd()?).await?;
            service.reconcile_gateway().await?;
            if json {
                output.success(report.clone()).map_err(output_error)?;
            } else if args.envrc {
                emit_envrc(output, &report).await?;
            }
            if report.action != EnsureAction::AlreadyMounted {
                output
                    .guidance(&format!(
                        "workspace {} is ready ({})",
                        report.workspace,
                        ensure_action(report.action)
                    ))
                    .map_err(output_error)?;
            }
            Ok(success())
        }
        Command::List(args) => {
            if args.all {
                let mut projects = service.list_all().await?;
                projects.sort_by(|left, right| left.repo_id.cmp(&right.repo_id));
                for project in &mut projects {
                    project
                        .workspaces
                        .sort_by(|left, right| left.workspace.cmp(&right.workspace));
                }
                if json {
                    output.success(projects).map_err(output_error)?;
                } else {
                    emit_project_workspaces_table(output, &projects)?;
                }
            } else {
                let mut workspaces = service.list().await?;
                workspaces.sort_by(|left, right| left.workspace.cmp(&right.workspace));
                let empty = workspaces.is_empty();
                if json {
                    output.success(workspaces).map_err(output_error)?;
                } else {
                    emit_workspace_table(output, &workspaces)?;
                }
                if empty {
                    let others = service.other_adopted_project_count().await?;
                    let noun = if others == 1 {
                        "project exists"
                    } else {
                        "projects exist"
                    };
                    output
                        .guidance(&format!(
                            "current repository has no workspaces or is not adopted; {others} \
                             other adopted {noun}; run cowshed ls --all"
                        ))
                        .map_err(output_error)?;
                }
            }
            Ok(success())
        }
        Command::Path(args) => {
            let workspace = match args.slot {
                Some(slot) => slot_tenant(service, slot).await?,
                None => resolve_workspace(service, args.workspace, "path").await?,
            };
            let info = service.path(&workspace, args.no_attach).await?;
            if !args.no_attach {
                service.reconcile_gateway().await?;
            }
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
            let report = service
                .remove(
                    &args.workspace,
                    RemoveOptions {
                        force: args.force,
                        restore: args.restore,
                        abandon: args.abandon,
                    },
                )
                .await?;
            service.reconcile_gateway().await?;
            if json {
                output.success(report.clone()).map_err(output_error)?;
            }
            // An authorized abandonment still says out loud what it destroyed and where the only
            // remaining copy went. The refusal it replaced would have said the same thing; passing
            // the flag buys the deletion, not silence.
            if let Some(abandoned) = report.abandoned.as_ref() {
                output
                    .guidance(&format!(
                        "abandoned {} commit{} at {} that {} ({}) did not contain",
                        abandoned.unlanded_commits,
                        if abandoned.unlanded_commits == 1 {
                            ""
                        } else {
                            "s"
                        },
                        abandoned.head,
                        abandoned.target_branch,
                        match abandoned.target_head.as_ref() {
                            Some(head) => format!("at {head}"),
                            None => "no such branch".to_owned(),
                        }
                    ))
                    .map_err(output_error)?;
                output
                    .guidance(&format!("bundled to {}", abandoned.bundle.display()))
                    .map_err(output_error)?;
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
            service.reconcile_gateway().await?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Detach(args) => {
            service.detach(&args.workspace).await?;
            service.reconcile_gateway().await?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Resize(args) => {
            let capacity = os_capacity(args.capacity)?;
            let result = service.resize(&args.workspace, &capacity).await?;
            // Resize leaves the workspace mounted exactly as it found it, but the mount it comes
            // back on is a new attachment, so the gateway's view has to be refreshed.
            service.reconcile_gateway().await?;
            if json {
                output.success(result.clone()).map_err(output_error)?;
            } else {
                output
                    .bare_line(result.capacity.as_bytes())
                    .map_err(output_error)?;
            }
            output
                .guidance(&format!(
                    "workspace {} grew from {} to {}",
                    result.workspace, result.previous_capacity, result.capacity
                ))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::Gc(args) => {
            let report = service
                .gc(GcOptions {
                    dry_run: args.dry_run,
                })
                .await?;
            if json {
                output.success(report.clone()).map_err(output_error)?;
            } else {
                output
                    .bare_line(report.freed_bytes.to_string().as_bytes())
                    .map_err(output_error)?;
            }
            if report.dry_run {
                emit_gc_candidates(output, &report)?;
                let candidate_noun = if report.candidates.len() == 1 {
                    "candidate"
                } else {
                    "candidates"
                };
                output
                    .guidance(&format!(
                        "dry run examined {} objects; {} {}, {} bytes reclaimable",
                        report.examined,
                        report.candidates.len(),
                        candidate_noun,
                        report.freed_bytes
                    ))
                    .map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Push(args) => {
            let workspace = resolve_workspace(service, args.workspace, "push").await?;
            let options = PushOptions {
                branch: args.branch.map(os_branch).transpose()?,
                expected_workspace_incarnation: args
                    .expected_workspace_incarnation
                    .map(os_incarnation)
                    .transpose()?,
                expected_source_head: args.expected_source_head.map(os_git_oid).transpose()?,
                expected_destination_head: args
                    .expected_destination_head
                    .map(os_expected_ref)
                    .transpose()?,
            };
            let report = service.push(&workspace, options).await?;
            if json {
                output.success(report.clone()).map_err(output_error)?;
            } else {
                emit_push(output, &report)?;
            }
            Ok(success())
        }
        Command::Rebase(args) => {
            let workspace = resolve_workspace(service, args.workspace, "rebase").await?;
            let options = RebaseOptions {
                onto: args.onto.map(os_revision).transpose()?,
                fresh: args.fresh,
                expected_workspace_incarnation: args
                    .expected_workspace_incarnation
                    .map(os_incarnation)
                    .transpose()?,
                expected_source_head: args.expected_source_head.map(os_git_oid).transpose()?,
                expected_onto_head: args.expected_onto_head.map(os_git_oid).transpose()?,
            };
            let oid = service.rebase(&workspace, options).await?;
            if json {
                output
                    .success(RevisionResult { oid: oid.clone() })
                    .map_err(output_error)?;
            } else {
                output
                    .bare_line(oid.as_str().as_bytes())
                    .map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Land(args) => {
            let reconcile_gateway = args.retire;
            let options = LandOptions {
                target_branch: args.target.map(os_branch).transpose()?,
                check: (!args.checks.is_empty())
                    .then(|| {
                        args.checks
                            .into_iter()
                            .map(os_utf8)
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?,
                retire: args.retire,
                push_only: args.push_only,
                expected_workspace_incarnation: args
                    .expected_workspace_incarnation
                    .map(os_incarnation)
                    .transpose()?,
                expected_source_head: args.expected_source_head.map(os_git_oid).transpose()?,
                expected_target_head: args.expected_target_head.map(os_expected_ref).transpose()?,
            };
            let report = service.land(&args.workspace, options).await?;
            if reconcile_gateway {
                service.reconcile_gateway().await?;
            }
            if json {
                output.success(report.clone()).map_err(output_error)?;
            } else {
                emit_land(output, &report)?;
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
        Command::Gateway(_) => Err(CowshedError::internal(
            "gateway commands must be dispatched by the host service entrypoint",
        )),
        Command::Sccache(_) => Err(CowshedError::internal(
            "sccache commands must be dispatched by the host service entrypoint",
        )),
        Command::Skill(_) => Err(CowshedError::internal(
            "skill commands must be dispatched before the runtime bridge",
        )),
    }
}

fn requires_gateway_before_dispatch(command: &Command) -> bool {
    matches!(
        command,
        Command::Exec(_) | Command::Ensure(_) | Command::Doctor
    )
}

fn success() -> DispatchExit {
    DispatchExit { code: 0 }
}

fn adopt_options(args: AdoptArgs) -> Result<AdoptOptions> {
    Ok(AdoptOptions {
        path: args.path,
        repo_id: args.repo_id.map(os_repo_id).transpose()?,
        capacity: args.capacity.map(os_capacity).transpose()?,
        quarantine: args.quarantine,
        image_format: None,
    })
}

/// Validate and normalise a capacity at the CLI boundary, so a malformed size is refused before
/// any host state is touched and the size the CLI reports is the one the image is sized to.
fn os_capacity(value: std::ffi::OsString) -> Result<String> {
    let text = os_utf8(value)?;
    ImageCapacity::parse(&text)
        .map(|capacity| capacity.to_string())
        .map_err(|error| {
            usage(
                error.to_string(),
                "use a capacity such as 100g, 200g, or 1t",
            )
        })
}

fn os_repo_id(value: std::ffi::OsString) -> Result<RepoId> {
    os_repo_id_ref(&value)
}

fn os_repo_id_ref(value: &std::ffi::OsStr) -> Result<RepoId> {
    let value = value.to_str().ok_or_else(|| {
        usage(
            "this option requires valid UTF-8",
            "use UTF-8 for control options; child argv may contain arbitrary Unix bytes",
        )
    })?;
    RepoId::parse(value).map_err(|error| {
        usage(
            format!("invalid repository identity: {error}"),
            "use an explicit owner/repository identity",
        )
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

fn os_branch(value: std::ffi::OsString) -> Result<String> {
    let value = os_utf8(value)?;
    BranchName::new(value)
        .map(|branch| branch.as_str().to_owned())
        .map_err(|error| usage(error.to_string(), "use a valid local branch name"))
}

fn os_git_oid(value: std::ffi::OsString) -> Result<GitOid> {
    GitOid::new(os_utf8(value)?).map_err(|error| {
        usage(
            error.to_string(),
            "use a full lowercase 40- or 64-hex object id",
        )
    })
}

fn os_incarnation(value: std::ffi::OsString) -> Result<WorkspaceIncarnation> {
    WorkspaceIncarnation::new(os_utf8(value)?).map_err(|error| {
        usage(
            error.to_string(),
            "use the current 32-character lowercase workspace incarnation",
        )
    })
}

fn os_expected_ref(value: std::ffi::OsString) -> Result<ExpectedRefHead> {
    if value == std::ffi::OsStr::new("missing") {
        Ok(ExpectedRefHead::Missing)
    } else {
        os_git_oid(value).map(ExpectedRefHead::Oid)
    }
}

/// Resolve a `<ws>` argument, falling back to the workspace the command was run inside.
///
/// An explicit argument always wins — inference never overrides what the caller named. Otherwise
/// the cwd is resolved the way `ensure` already resolves it: containment in exactly one currently
/// mounted workspace, which is authoritative because mount identity is keyed off the in-image
/// marker, and which already refuses an ambiguous match rather than picking one.
///
/// Only verbs that act on a workspace *in place* get this. Verbs that retire, replace, rename, or
/// unmount one require it to be named, so that losing the workspace you are standing in is always
/// something you asked for by name rather than something the cwd decided for you.
async fn resolve_workspace(
    service: &mut dyn CliService,
    workspace: Option<String>,
    verb: &str,
) -> Result<String> {
    if let Some(workspace) = workspace {
        return Ok(workspace);
    }
    let cwd = invocation_cwd()?;
    match service.workspace_at(cwd).await {
        Ok(info) => Ok(info.workspace.to_string()),
        Err(error) if error.code == ErrorCode::NotFound => Err(usage(
            format!("{verb} requires a workspace"),
            format!("name one — cowshed {verb} <ws> — or run it inside a mounted workspace"),
        )),
        Err(error) => Err(error),
    }
}

fn invocation_cwd() -> Result<PathBuf> {
    std::env::current_dir().map_err(|error| {
        CowshedError::environment_missing(
            format!("could not determine the invocation directory: {error}"),
            "run cowshed from an accessible workspace directory",
        )
    })
}

/// Which workspace currently occupies a build slot.
///
/// Resolved from the mount path rather than a separate index: a slot's mountpoint leaf *is* the
/// slot, so the listing the CLI already has is authoritative and there is no second record to keep
/// in step.
async fn slot_tenant(service: &mut dyn CliService, slot: u32) -> Result<String> {
    let wanted = SlotId::new(slot).map_err(|error| {
        usage(
            error.to_string(),
            "choose a slot within the project's range",
        )
    })?;
    let workspaces = service.list().await?;
    workspaces
        .into_iter()
        .find(|info| SlotId::from_mount_path(&info.mount) == Some(wanted))
        .map(|info| info.workspace.to_string())
        .ok_or_else(|| {
            CowshedError::not_found(
                format!("no workspace occupies slot {slot}"),
                format!("give the slot a tenant: cowshed new <name> --slot {slot}"),
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

async fn emit_envrc<W: Write, E: Write>(
    output: &mut Output<W, E>,
    report: &EnsureReport,
) -> Result<()> {
    let token = tokio::fs::read(&report.workspace_token)
        .await
        .map_err(|error| {
            CowshedError::integrity(
                format!(
                    "could not read workspace token {}: {error}",
                    report.workspace_token.display()
                ),
                "run cowshed ensure again; if this persists, run cowshed doctor",
            )
        })?;
    emit_shell_export(output, b"GOENV", report.go_env.as_os_str().as_bytes())?;
    // Host-level endpoint, identical for every workspace: rustc-wrapper clients
    // in IDE terminals and other cowshed-unspawned processes must reach the
    // host-owned sccache daemon instead of spawning a private server with
    // default cache paths.
    emit_shell_export(
        output,
        b"SCCACHE_SERVER_UDS",
        report.sccache_server_uds.as_os_str().as_bytes(),
    )?;
    emit_shell_export(
        output,
        b"SCCACHE_DIR",
        report.sccache_dir.as_os_str().as_bytes(),
    )?;
    // Routing Rust through sccache is a slot-path privilege, not a default. Cargo's `-C metadata`
    // and sccache's own key both carry the absolute build path, so only a workspace mounted at a
    // slot's stable path can hit another generation's entries; a name-mounted workspace would pay
    // the wrapper's cost for a cache it can never share. The same trade forces incremental off:
    // incremental compilation is per-unit local state sccache cannot cache, and cargo silently
    // wins the race, so a slot tenant chooses the shared cache over local incrementality.
    if SlotId::from_mount_path(&report.mount).is_some() {
        emit_shell_export(output, b"RUSTC_WRAPPER", b"sccache")?;
        emit_shell_export(output, b"CARGO_INCREMENTAL", b"0")?;
    }
    emit_shell_export(output, b"COWSHED_WORKSPACE_TOKEN", &token)?;
    if let Some(port_block) = report.port_block {
        emit_shell_export(
            output,
            b"COWSHED_PORT_BASE",
            port_block.base().to_string().as_bytes(),
        )?;
    }
    Ok(())
}

fn emit_shell_export<W: Write, E: Write>(
    output: &mut Output<W, E>,
    name: &[u8],
    value: &[u8],
) -> Result<()> {
    output
        .bare(b"export ")
        .and_then(|()| output.bare(name))
        .and_then(|()| output.bare(b"='"))
        .map_err(output_error)?;
    let mut first = true;
    for part in value.split(|byte| *byte == b'\'') {
        if !first {
            output.bare(b"'\\''").map_err(output_error)?;
        }
        first = false;
        output.bare(part).map_err(output_error)?;
    }
    output.bare(b"'\n").map_err(output_error)
}

const fn ensure_action(action: EnsureAction) -> &'static str {
    match action {
        EnsureAction::AlreadyMounted => "already mounted",
        EnsureAction::Attached => "attached",
        EnsureAction::Healed => "healed",
    }
}

fn emit_gc_candidates<W: Write, E: Write>(
    output: &mut Output<W, E>,
    report: &GcReport,
) -> Result<()> {
    for candidate in &report.candidates {
        output
            .guidance(&format!(
                "would reclaim {} ({} bytes; reason: {})",
                candidate.path.display(),
                candidate.bytes,
                gc_reason(candidate.reason)
            ))
            .map_err(output_error)?;
    }
    Ok(())
}

const fn gc_reason(reason: GcReason) -> &'static str {
    match reason {
        GcReason::RetiredWorkspace => "retiredWorkspace",
        GcReason::OrphanStagingImage => "orphanStagingImage",
        GcReason::OrphanStagingMetadata => "orphanStagingMetadata",
        GcReason::ExpiredCheckpoint => "expiredCheckpoint",
        GcReason::DetachedImageCompaction => "detachedImageCompaction",
    }
}

fn emit_push<W: Write, E: Write>(output: &mut Output<W, E>, report: &PushReport) -> Result<()> {
    output
        .bare(report.destination_ref.as_bytes())
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| output.bare(report.source_head.as_str().as_bytes()))
        .and_then(|()| output.bare(b"\n"))
        .map_err(output_error)
}

fn emit_land<W: Write, E: Write>(output: &mut Output<W, E>, report: &LandReport) -> Result<()> {
    output
        .bare(report.target_branch.as_bytes())
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| output.bare(report.landed_head.as_str().as_bytes()))
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| {
            output.bare(if report.target_was_checked_out {
                b"true"
            } else {
                b"false"
            })
        })
        .and_then(|()| output.bare(b"\n"))
        .map_err(output_error)
}

fn emit_workspace_table<W: Write, E: Write>(
    output: &mut Output<W, E>,
    workspaces: &[WorkspaceInfo],
) -> Result<()> {
    let rows: Vec<Vec<String>> = workspaces
        .iter()
        .map(|workspace| workspace_row(None, workspace))
        .collect();
    emit_aligned_rows(output, &rows)
}

fn emit_project_workspaces_table<W: Write, E: Write>(
    output: &mut Output<W, E>,
    projects: &[ProjectWorkspaces],
) -> Result<()> {
    let rows: Vec<Vec<String>> = projects
        .iter()
        .flat_map(|project| {
            project
                .workspaces
                .iter()
                .map(|workspace| workspace_row(Some(&project.repo_id), workspace))
        })
        .collect();
    emit_aligned_rows(output, &rows)
}

fn workspace_row(repo_id: Option<&RepoId>, workspace: &WorkspaceInfo) -> Vec<String> {
    let mut row = Vec::with_capacity(5);
    if let Some(repo_id) = repo_id {
        row.push(repo_id.as_str().to_owned());
    }
    row.push(workspace.workspace.as_str().to_owned());
    row.push(
        match workspace.state {
            WorkspaceState::Attached => "mounted",
            WorkspaceState::Detached => "detached",
        }
        .to_owned(),
    );
    row.push(workspace.branch.clone().unwrap_or_default());
    row.push(if workspace.state == WorkspaceState::Attached {
        workspace.mount.display().to_string()
    } else {
        String::new()
    });
    row
}

/// Human list output: columns padded with spaces to equal width (two-space
/// gutter), so `ls` reads as a table. The final column — and any trailing
/// empty cell — is never padded, keeping lines free of trailing whitespace.
fn emit_aligned_rows<W: Write, E: Write>(
    output: &mut Output<W, E>,
    rows: &[Vec<String>],
) -> Result<()> {
    let columns = rows.first().map(Vec::len).unwrap_or(0);
    let mut widths = vec![0usize; columns];
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }
    for row in rows {
        let mut line = String::new();
        let last_filled = row.iter().rposition(|cell| !cell.is_empty()).unwrap_or(0);
        for (index, cell) in row.iter().enumerate().take(last_filled + 1) {
            if index > 0 {
                line.push_str("  ");
            }
            line.push_str(cell);
            if index < last_filled {
                for _ in cell.chars().count()..widths[index] {
                    line.push(' ');
                }
            }
        }
        line.push('\n');
        output.bare(line.as_bytes()).map_err(output_error)?;
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
    let mode = runtime_open_mode(&cli.command);
    let requested_repo_id = runtime_open_repo_id(&cli.command)?;
    let root = resolve_project_root(&cli).await?;
    let bridge = match mode {
        RuntimeOpenMode::Provision => ActorBridge::open_for_adopt(&root, requested_repo_id).await?,
        RuntimeOpenMode::ExistingOnly => ActorBridge::open_existing(&root).await?,
    };
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
    fn only_adopt_receives_provisioning_authority() {
        let cases = [
            (vec!["adopt", "/repo"], RuntimeOpenMode::Provision),
            (vec!["new", "raven"], RuntimeOpenMode::ExistingOnly),
            (
                vec!["fork", "raven", "falcon"],
                RuntimeOpenMode::ExistingOnly,
            ),
            (vec!["checkpoint", "raven"], RuntimeOpenMode::ExistingOnly),
            (
                vec!["restore", "raven", "stable"],
                RuntimeOpenMode::ExistingOnly,
            ),
            (vec!["ensure"], RuntimeOpenMode::ExistingOnly),
            (vec!["ls"], RuntimeOpenMode::ExistingOnly),
            (vec!["path", "raven"], RuntimeOpenMode::ExistingOnly),
            (
                vec!["exec", "raven", "--", "true"],
                RuntimeOpenMode::ExistingOnly,
            ),
            (vec!["rm", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["attach", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["detach", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["gc"], RuntimeOpenMode::ExistingOnly),
            (vec!["push", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["rebase", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["land", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["doctor"], RuntimeOpenMode::ExistingOnly),
        ];

        for (arguments, expected) in cases {
            let parsed = crate::args::parse_args(arguments).unwrap();
            assert_eq!(runtime_open_mode(&parsed.command), expected);
        }
    }

    #[test]
    fn adopt_runtime_open_receives_the_parsed_repository_identity() {
        let parsed = crate::args::parse_args(["adopt", "/repo", "--repo-id", "acme/widget"])
            .expect("adopt arguments");
        assert_eq!(
            runtime_open_repo_id(&parsed.command).expect("runtime open identity"),
            Some(RepoId::parse("acme/widget").expect("repository identity"))
        );

        let parsed = crate::args::parse_args(["adopt", "/repo"]).expect("adopt arguments");
        assert_eq!(
            runtime_open_repo_id(&parsed.command).expect("optional runtime open identity"),
            None
        );
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
