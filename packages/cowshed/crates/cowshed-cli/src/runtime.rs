use crate::args::{
    AdoptArgs, Cli, Command, ExecArgs, MoveDestination, ProjectDiscovery,
    StdinSource as CliStdinSource,
};
use crate::gateway_service;
use crate::output::Output;
use crate::probe;
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use cowshed_core::apfs::{ApfsCaseSensitivity, SystemCommandRunner};
pub use cowshed_core::api::ProjectWorkspaces;
use cowshed_core::api::server::{ConnectionAuthority, serve_controller_connection};
use cowshed_core::api::{
    AdoptOptions, AttachOptions, BranchName, CheckpointInfo, CheckpointOptions, CheckpointResult,
    CommandArg, Coordinator, CreateOptions, DoctorReport, EmptyResult, ExecRequest, ExitStatus,
    ExpectedRefHead, Finding, FindingSeverity, GatewayStatus, GcOptions, GcReason, GcReport,
    GitOid, JobInfo, JobStream, LandOptions, LandReport, LandingCommits, MountResult,
    OutputPublication, PublicationPolicy, PushOptions, PushReport, RebaseOptions, RemoveOptions,
    RemoveReport, ResizeResult, RevisionResult, RevisionTarget, RunSandboxMode, SccacheStatus,
    StdinSource as CoreStdinSource, UtcTimestamp, WorkspaceInfo, WorkspaceLanding, WorkspacePath,
    WorkspaceState, validate_command_argv,
};
use cowshed_core::git::GitRepository;
use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, ImageCapacity, ImageFormat, SlotId, WorkspaceIncarnation,
    WorkspaceName, WorkspaceRole,
};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::ProjectRuntime;
use cowshed_core::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_core::storage::apfs::{ApfsSubstrate, ApfsSubstrateConfig, DEFAULT_IMAGE_CAPACITY};
use cowshed_core::storage::bootstrap::{
    CACHES_ROOT, CanonicalRoots, HostAction, HostSetupPlan, HostSetupReport, STORE_ROOT,
    ValidatedHostStorage, execute_host_setup, plan_host_setup,
};
use cowshed_core::storage::host_config::{RETIRED_LAYOUT_HINT, retired_layout_paths};
use cowshed_core::storage::lifecycle::{DerivedWorkspace, MountIntent, MountState, Pin, Substrate};
use cowshed_core::storage::{StorageLayout, discover_session_images};
use cowshed_core::{
    AdoptedProject, CowshedError, ErrorCode, NativeGatewayInventory, Result, UnreachableMain,
    validate_existing_host_storage,
};
use std::collections::HashMap;
use std::fs;
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
    async fn attach(&mut self, workspace: &str, options: AttachOptions) -> Result<WorkspaceInfo>;
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
        | Command::Setup(_)
        | Command::Gateway(_)
        | Command::Sccache(_)
        | Command::Skill(_)
        | Command::Version
        | Command::Help(_)
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

    fn git_root(&self) -> Result<&Path> {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.descriptor().git_root.as_path())
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })
    }

    fn store_root(&self) -> Result<&Path> {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.descriptor().store_root.as_path())
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })
    }

    async fn adopted_projects(&self) -> Result<Vec<AdoptedProject>> {
        adopted_projects().await
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

async fn adopted_projects() -> Result<Vec<AdoptedProject>> {
    let home = gateway_service::canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    NativeGatewayInventory::new(storage)
        .adopted_projects()
        .await
        .map_err(project_inventory_error)
}

/// Which adopted projects have no mounted main, read straight from the host store.
///
/// Host-scoped rather than project-scoped on purpose: mains are always-mounted across every
/// adopted project, so the answer must not depend on where `doctor` was run from.
async fn unmounted_mains() -> Result<Vec<UnreachableMain>> {
    let home = gateway_service::canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    NativeGatewayInventory::new(storage)
        .unmounted_mains()
        .await
        .map_err(project_inventory_error)
}

async fn list_all_adopted_projects() -> Result<Vec<ProjectWorkspaces>> {
    let home = gateway_service::canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    NativeGatewayInventory::new(storage)
        .all_projects()
        .await
        .map_err(project_inventory_error)
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
        list_all_adopted_projects().await
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

    async fn attach(&mut self, workspace: &str, options: AttachOptions) -> Result<WorkspaceInfo> {
        let snapshot = self.coordinator()?.project().workspace(workspace).await?;
        snapshot.attach(options).await?;
        snapshot.refresh_info().await
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
    let root = GitRepository::discover(start)
        .await
        .map(|repository| repository.root().to_path_buf());
    match (&cli.command, root) {
        (Command::Adopt(_), result) => result,
        (_, Err(error)) => Err(project_context_error(error)),
        (_, Ok(root)) => Ok(root),
    }
}

fn project_context_error(error: CowshedError) -> CowshedError {
    CowshedError::new(
        error.code,
        error.message,
        "cowshed ls; cowshed --project <git-root> <command>",
    )
}

fn optional_project_unavailable(error: &CowshedError) -> bool {
    matches!(
        error.code,
        ErrorCode::EnvironmentMissing | ErrorCode::NotFound
    )
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
                .guidance(&format!(
                    "created {}.{} for {} (capacity {}, {})",
                    info.workspace,
                    info.image_format.extension(),
                    info.repo_id,
                    capacity,
                    match info.image_format {
                        ImageFormat::Asif => "asif",
                        ImageFormat::Sparse => "sparse",
                    }
                ))
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
        Command::List(args) => {
            if args.all {
                let mut projects = service.list_all().await?;
                projects.sort_by(|left, right| left.repo_id.cmp(&right.repo_id));
                for project in &mut projects {
                    project
                        .workspaces
                        .sort_by(|left, right| left.workspace.cmp(&right.workspace));
                }
                emit_project_listing(output, json, args, projects).await?;
            } else {
                let mut workspaces = service.list().await?;
                workspaces.sort_by(|left, right| left.workspace.cmp(&right.workspace));
                let empty = workspaces.is_empty();
                emit_workspace_listing(output, json, args, None, workspaces).await?;
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
            let options = AttachOptions {
                browse: args.browse,
                observed_path: if args.all {
                    None
                } else {
                    Some(invocation_cwd()?)
                },
            };
            let infos = if let Some(workspace) = args.workspace {
                vec![service.attach(&workspace, options).await?]
            } else {
                attach_scoped_sessions(service, args.all, options).await?
            };
            service.reconcile_gateway().await?;
            emit_attached(output, json, &infos)?;
            Ok(success())
        }
        Command::Detach(args) => {
            if args.all {
                detach_scoped_sessions(service).await?;
            } else {
                let workspace = args.workspace.ok_or_else(|| {
                    usage(
                        "detach requires a workspace",
                        "cowshed detach <ws>; cowshed detach --all",
                    )
                })?;
                service.detach(&workspace).await?;
            }
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
                        "dry run examined {} objects; {} {}, {} bytes deletable",
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
        Command::Setup(_) => Err(CowshedError::internal(
            "setup must be dispatched by the host service entrypoint",
        )),
        Command::Sccache(_) => Err(CowshedError::internal(
            "sccache commands must be dispatched by the host service entrypoint",
        )),
        Command::Skill(_) => Err(CowshedError::internal(
            "skill commands must be dispatched before the runtime bridge",
        )),
        Command::Help(_) => Err(CowshedError::internal(
            "help is answered before the runtime bridge",
        )),
        Command::Version => Err(CowshedError::internal(
            "version is answered before the runtime bridge",
        )),
    }
}

fn requires_gateway_before_dispatch(command: &Command) -> bool {
    matches!(command, Command::Exec(_))
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
            "find it in `cowshed ls --json` (workspaceIncarnation)",
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
/// the cwd is resolved by containment in exactly one currently mounted workspace, which is
/// authoritative because mount identity is keyed off the in-image marker, and which already
/// refuses an ambiguous match rather than picking one.
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

fn emit_attached<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    infos: &[WorkspaceInfo],
) -> Result<()> {
    match infos {
        [] => Err(no_detached_sessions()),
        [info] => emit_mount(output, json, info),
        many => {
            if json {
                output.success(many.to_vec()).map_err(output_error)
            } else {
                for info in many {
                    output
                        .bare_line(info.mount.as_os_str().as_bytes())
                        .map_err(output_error)?;
                }
                Ok(())
            }
        }
    }
}

fn is_detached_session(info: &WorkspaceInfo) -> bool {
    info.role != WorkspaceRole::Main && info.state == WorkspaceState::Detached
}

fn no_detached_sessions() -> CowshedError {
    CowshedError::not_found(
        "no detached session workspace found",
        "cowshed ls; cowshed attach <ws>; cowshed attach --all",
    )
}
async fn attach_scoped_sessions(
    service: &mut dyn CliService,
    all: bool,
    options: AttachOptions,
) -> Result<Vec<WorkspaceInfo>> {
    if !all {
        match service.workspace_at(invocation_cwd()?).await {
            Ok(_) => {}
            Err(error) if error.code == ErrorCode::Conflict => {
                return Err(CowshedError::conflict(
                    error.message,
                    "name one workspace or repair overlapping mounts",
                ));
            }
            Err(error) if error.code == ErrorCode::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    let candidates = if all {
        service
            .list_all()
            .await?
            .into_iter()
            .flat_map(|project| project.workspaces)
            .collect()
    } else {
        service.list().await?
    };
    let targets: Vec<WorkspaceInfo> = candidates.into_iter().filter(is_detached_session).collect();
    if targets.is_empty() {
        return Err(no_detached_sessions());
    }
    let mut attached = Vec::with_capacity(targets.len());
    for target in targets {
        attached.push(
            service
                .attach(target.workspace.as_str(), options.clone())
                .await?,
        );
    }
    Ok(attached)
}

async fn attach_store_wide(browse: bool) -> Result<Vec<WorkspaceInfo>> {
    let home = gateway_service::canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let projects = NativeGatewayInventory::new(storage.clone())
        .adopted_projects()
        .await
        .map_err(project_inventory_error)?;
    let mut attached = Vec::new();
    for project in &projects {
        attached.extend(attach_project_sessions_from_store(&storage, project, browse).await?);
    }
    if attached.is_empty() {
        return Err(no_detached_sessions());
    }
    Ok(attached)
}

/// Attach a project's detached sessions from store metadata and APFS facts alone.
///
/// Store-wide attach must not open `project.project_root`: a stale or missing checkout remote
/// does not invalidate the image sidecar, repository binding, or canonical mount layout needed to
/// mount a session. Main is intentionally excluded because it is an always-mounted host invariant,
/// not a user-detachable session.
async fn attach_project_sessions_from_store(
    storage: &ValidatedHostStorage,
    project: &AdoptedProject,
    browse: bool,
) -> Result<Vec<WorkspaceInfo>> {
    let layout = StorageLayout::new(storage.store(), &project.repo_id)
        .map_err(attach_store_storage_error)?;
    let checkout_layout = layout
        .checkout_layout()
        .map_err(attach_store_storage_error)?;
    let config = ApfsSubstrateConfig::new(
        storage.store(),
        storage.caches(),
        &project.project_root,
        checkout_layout,
        ApfsCaseSensitivity::Sensitive,
    );
    let host = MacOsApfsExecutionHost::new(SystemCommandRunner, config.clone())
        .map_err(attach_store_storage_error)?;
    let substrate = ApfsSubstrate::new(config, host);
    let targets = substrate
        .list(&project.repo_id)
        .await
        .map_err(attach_store_storage_error)?
        .into_iter()
        .filter(|derived| {
            derived.workspace.role() != WorkspaceRole::Main
                && derived.mount_state == MountState::Detached
        })
        .collect::<Vec<_>>();
    let mut attached = Vec::with_capacity(targets.len());
    for target in targets {
        let mut info = store_workspace_info(&layout, &target)?;
        info.mount = substrate
            .ensure_mounted(&target.workspace, MountIntent { browse })
            .await
            .map_err(attach_store_storage_error)?;
        info.state = WorkspaceState::Attached;
        attached.push(info);
    }
    Ok(attached)
}

fn store_workspace_info(
    layout: &StorageLayout,
    derived: &DerivedWorkspace,
) -> Result<WorkspaceInfo> {
    let image = layout
        .session_image(derived.workspace.name(), derived.workspace.format())
        .map_err(attach_store_storage_error)?;
    let metadata = DetachedWorkspaceMetadata::read_for_image(image.image())
        .map_err(attach_store_storage_error)?;
    let snapshot = metadata.info_snapshot.as_ref();
    let base_commit = snapshot
        .map(|snapshot| GitOid::new(snapshot.base_commit.clone()))
        .transpose()
        .map_err(attach_store_storage_error)?;
    let created_at = snapshot
        .map(|snapshot| UtcTimestamp::new(snapshot.created_at.clone()))
        .transpose()
        .map_err(attach_store_storage_error)?;
    Ok(WorkspaceInfo {
        repo_id: derived.workspace.repo().clone(),
        workspace: derived.workspace.name().clone(),
        workspace_incarnation: derived.workspace.incarnation().clone(),
        role: derived.workspace.role(),
        image_format: derived.workspace.format(),
        mount: layout
            .workspace_mount(derived.workspace.name())
            .map_err(attach_store_storage_error)?,
        state: WorkspaceState::Detached,
        branch: snapshot.and_then(|snapshot| snapshot.branch.clone()),
        base_commit,
        created_at,
        checkpoints: derived
            .checkpoints
            .iter()
            .map(|checkpoint| CheckpointInfo {
                label: checkpoint.label.to_string(),
                revision: checkpoint.revision.get(),
                pinned: checkpoint.pin == Pin::Pinned,
            })
            .collect(),
        snapshot_stale: snapshot.is_some_and(|snapshot| snapshot.stale),
        landing: None,
    })
}

fn attach_store_storage_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("store-wide attach could not read or mount a session: {error}"),
        "cowshed doctor --json",
    )
}

fn is_attached_session(info: &WorkspaceInfo) -> bool {
    info.role != WorkspaceRole::Main && info.state == WorkspaceState::Attached
}

fn no_attached_sessions() -> CowshedError {
    CowshedError::not_found(
        "no attached session workspace found",
        "cowshed ls; cowshed detach <ws>; cowshed detach --all",
    )
}

async fn detach_scoped_sessions(service: &mut dyn CliService) -> Result<()> {
    let targets: Vec<WorkspaceInfo> = service
        .list_all()
        .await?
        .into_iter()
        .flat_map(|project| project.workspaces)
        .filter(is_attached_session)
        .collect();
    if targets.is_empty() {
        return Err(no_attached_sessions());
    }
    for target in targets {
        service.detach(target.workspace.as_str()).await?;
    }
    Ok(())
}

async fn detach_store_wide() -> Result<()> {
    let home = gateway_service::canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let projects = NativeGatewayInventory::new(storage.clone())
        .adopted_projects()
        .await
        .map_err(project_inventory_error)?;
    let mut detached = 0usize;
    for project in &projects {
        detached += detach_project_sessions_from_store(&storage, project).await?;
    }
    if detached == 0 {
        return Err(no_attached_sessions());
    }
    Ok(())
}

/// Detach a project's mounted sessions from store metadata and kernel mount facts alone.
///
/// In particular, this does not open `project.project_root` as a Git repository. Store-wide
/// detach must keep working when an adopted checkout's recorded remote name is stale or absent:
/// the image sidecar and repository binding already provide every identity unmount needs.
async fn detach_project_sessions_from_store(
    storage: &ValidatedHostStorage,
    project: &AdoptedProject,
) -> Result<usize> {
    let layout = StorageLayout::new(storage.store(), &project.repo_id)
        .map_err(detach_store_storage_error)?;
    let checkout_layout = layout
        .checkout_layout()
        .map_err(detach_store_storage_error)?;
    let config = ApfsSubstrateConfig::new(
        storage.store(),
        storage.caches(),
        &project.project_root,
        checkout_layout,
        ApfsCaseSensitivity::Sensitive,
    );
    let host = MacOsApfsExecutionHost::new(SystemCommandRunner, config.clone())
        .map_err(detach_store_storage_error)?;
    let substrate = ApfsSubstrate::new(config, host);
    let targets = substrate
        .list(&project.repo_id)
        .await
        .map_err(detach_store_storage_error)?
        .into_iter()
        .filter(|derived| {
            !derived.workspace.name().is_main()
                && matches!(derived.mount_state, MountState::Mounted { .. })
        })
        .map(|derived| derived.workspace)
        .collect::<Vec<_>>();
    for target in &targets {
        substrate
            .unmount(target)
            .await
            .map_err(detach_store_storage_error)?;
    }
    Ok(targets.len())
}

fn detach_store_storage_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("store-wide detach could not read or unmount a session: {error}"),
        "cowshed doctor --json",
    )
}

/// Resolve a session workspace's owning checkout from the store readdir.
///
/// Identity is the image sidecar's `infoSnapshot.projectRoot` (the same project root the
/// in-image marker records). Remotes and cwd git discovery are not consulted.
pub(crate) fn resolve_session_project_root(store: &Path, workspace: &str) -> Result<PathBuf> {
    let wanted = WorkspaceName::session(workspace)
        .map_err(|error| usage(error.to_string(), "cowshed detach <ws>; cowshed ls --all"))?;
    let mut found: Vec<(RepoId, PathBuf)> = Vec::new();
    let owners = fs::read_dir(store).map_err(|error| {
        CowshedError::environment_missing(
            format!("could not read cowshed store {}: {error}", store.display()),
            "cowshed setup",
        )
    })?;
    for owner in owners {
        let owner = owner.map_err(|error| {
            CowshedError::environment_missing(
                format!("could not read cowshed store {}: {error}", store.display()),
                "cowshed setup",
            )
        })?;
        if !dirent_is_plain_dir(&owner) {
            continue;
        }
        let repos = fs::read_dir(owner.path()).map_err(|error| {
            CowshedError::environment_missing(
                format!("could not read {}: {error}", owner.path().display()),
                "cowshed setup",
            )
        })?;
        for repo in repos {
            let repo = repo.map_err(|error| {
                CowshedError::environment_missing(
                    format!("could not read {}: {error}", owner.path().display()),
                    "cowshed setup",
                )
            })?;
            if !dirent_is_plain_dir(&repo) {
                continue;
            }
            let sessions = repo.path().join("sessions");
            let Ok(entries) = fs::read_dir(&sessions) else {
                continue;
            };
            let images = discover_session_images(
                entries.filter_map(|entry| entry.ok().map(|entry| entry.path())),
            )
            .map_err(|error| {
                CowshedError::conflict(
                    error.to_string(),
                    "cowshed --project <git-root> detach <ws>",
                )
            })?;
            for image in images {
                if image.workspace() != &wanted {
                    continue;
                }
                let metadata =
                    DetachedWorkspaceMetadata::read_for_image(image.path()).map_err(|error| {
                        CowshedError::integrity(
                            format!(
                                "workspace {wanted} sidecar at {} is unreadable: {error}",
                                image.path().display()
                            ),
                            "cowshed doctor",
                        )
                    })?;
                let snapshot = metadata.require_info_snapshot().map_err(|error| {
                    CowshedError::integrity(
                        format!(
                            "workspace {wanted} sidecar at {} has no project identity: {error}",
                            image.path().display()
                        ),
                        "cowshed doctor",
                    )
                })?;
                found.push((metadata.repo_id.clone(), snapshot.project_root.clone()));
            }
        }
    }
    match found.as_slice() {
        [] => Err(CowshedError::not_found(
            format!("no session workspace {workspace} in the store"),
            "cowshed ls --all; cowshed --project <git-root> detach <ws>",
        )),
        [(_, root)] => Ok(root.clone()),
        many => {
            let projects = many
                .iter()
                .map(|(repo_id, _)| repo_id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Err(CowshedError::conflict(
                format!("workspace {workspace} exists in more than one project ({projects})"),
                "cowshed --project <git-root> detach <ws>",
            ))
        }
    }
}

fn dirent_is_plain_dir(entry: &fs::DirEntry) -> bool {
    let name = entry.file_name();
    let Some(name) = name.to_str() else {
        return false;
    };
    if name.starts_with('.') {
        return false;
    }
    entry.file_type().is_ok_and(|kind| kind.is_dir())
}

fn report_new_git_identity<W: Write, E: Write>(
    bridge: &ActorBridge,
    name: &str,
    output: &mut Output<W, E>,
) -> Result<()> {
    let (candidate, mount_root) = identity_probe_target(bridge, Some(name))?;
    let gaps = probe::probe_git_identity(bridge.git_root()?, &candidate)?;
    for gap in &gaps {
        output
            .guidance(&gap.message(&mount_root))
            .map_err(output_error)?;
    }
    if !gaps.is_empty() {
        output
            .hint("cowshed setup --mount-root <dir>")
            .map_err(output_error)?;
    }
    Ok(())
}

fn git_identity_findings(bridge: &ActorBridge) -> Result<Vec<Finding>> {
    let (candidate, mount_root) = identity_probe_target(bridge, None)?;
    let gaps = probe::probe_git_identity(bridge.git_root()?, &candidate)?;
    Ok(gaps.iter().map(|gap| gap.finding(&mount_root)).collect())
}

fn identity_probe_target(
    bridge: &ActorBridge,
    workspace: Option<&str>,
) -> Result<(PathBuf, PathBuf)> {
    let layout = StorageLayout::new(bridge.store_root()?, bridge.repo_id()?).map_err(|error| {
        CowshedError::environment_missing(error.to_string(), "cowshed setup --mount-root <dir>")
    })?;
    let mount_root = layout.project().host_mount_root.clone();
    let candidate = match workspace {
        Some(name) => {
            let workspace = WorkspaceName::session(name)
                .map_err(|error| usage(error.to_string(), "use a valid workspace name"))?;
            layout.workspace_mount(&workspace).map_err(|error| {
                CowshedError::environment_missing(
                    error.to_string(),
                    "cowshed setup --mount-root <dir>",
                )
            })?
        }
        None => unused_identity_candidate(&layout)?,
    };
    Ok((candidate, mount_root))
}

fn unused_identity_candidate(layout: &StorageLayout) -> Result<PathBuf> {
    for index in 0..8 {
        let name = if index == 0 {
            "identity-probe".to_owned()
        } else {
            format!("identity-probe-{index}")
        };
        let workspace = WorkspaceName::session(name)
            .map_err(|error| CowshedError::internal(error.to_string()))?;
        let path = layout.workspace_mount(&workspace).map_err(|error| {
            CowshedError::environment_missing(error.to_string(), "cowshed setup --mount-root <dir>")
        })?;
        if !path.exists() {
            return Ok(path);
        }
    }
    Err(CowshedError::environment_missing(
        "could not allocate a throwaway path for the git-identity probe",
        "cowshed setup --mount-root <dir>",
    ))
}

async fn resolve_detach_root(cli: &Cli) -> Result<PathBuf> {
    if cli.global.project.is_some() {
        return resolve_project_root(cli).await;
    }
    let Command::Detach(args) = &cli.command else {
        return Err(CowshedError::internal(
            "detach project resolution ran for a non-detach command",
        ));
    };
    let workspace = args.workspace.as_deref().ok_or_else(|| {
        usage(
            "detach requires a workspace",
            "cowshed detach <ws>; cowshed detach --all",
        )
    })?;
    let home = gateway_service::canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    resolve_session_project_root(storage.store(), workspace)
}

fn emit_gc_candidates<W: Write, E: Write>(
    output: &mut Output<W, E>,
    report: &GcReport,
) -> Result<()> {
    for candidate in &report.candidates {
        output
            .guidance(&format!(
                "would delete {} ({} bytes; reason: {})",
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
        GcReason::RetiredWorkspace => "workspace was retired",
        GcReason::OrphanStagingImage => "orphaned staging image",
        GcReason::OrphanStagingMetadata => "orphaned staging metadata",
        GcReason::ExpiredCheckpoint => "expired checkpoint",
        GcReason::DetachedImageCompaction => "detached image compaction",
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

/// The branch `ls` measures against, which is the same constant `land` merges into and `rm` gates
/// on. Sharing it is the point: a listing that reported against a different branch than the gate
/// enforces would be a listing that lies about what `rm` will do.
const LANDING_TARGET_BRANCH: &str = cowshed_core::runtime::project::DEFAULT_LANDING_BRANCH;

/// The landing measurement is opt-in, and `--landed` opts in by implication: a filter on a fact
/// nobody measured would filter on nothing.
const fn landing_requested(args: crate::args::ListArgs) -> bool {
    args.landing || args.landed
}

async fn emit_project_listing<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    args: crate::args::ListArgs,
    mut projects: Vec<ProjectWorkspaces>,
) -> Result<()> {
    if landing_requested(args) {
        for project in &mut projects {
            annotate_landing(&mut project.workspaces).await;
        }
    }
    if args.landed {
        for project in &mut projects {
            let withheld = retain_fully_landed(&mut project.workspaces);
            emit_withheld(output, Some(&project.repo_id), &withheld)?;
        }
    }
    if json {
        return output.success(projects).map_err(output_error);
    }
    if args.landing {
        let rows = std::iter::once(landing_header(true))
            .chain(projects.iter().flat_map(|project| {
                project
                    .workspaces
                    .iter()
                    .map(|workspace| landing_row(Some(&project.repo_id), workspace))
            }))
            .collect::<Vec<_>>();
        return emit_aligned_rows(output, &rows);
    }
    if args.landed {
        return emit_bare_names(
            output,
            projects.iter().flat_map(|project| &project.workspaces),
        );
    }
    emit_project_workspaces_table(output, &projects)
}

async fn emit_workspace_listing<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    args: crate::args::ListArgs,
    repo_id: Option<&RepoId>,
    mut workspaces: Vec<WorkspaceInfo>,
) -> Result<()> {
    if landing_requested(args) {
        annotate_landing(&mut workspaces).await;
    }
    if args.landed {
        let withheld = retain_fully_landed(&mut workspaces);
        emit_withheld(output, repo_id, &withheld)?;
    }
    if json {
        return output.success(workspaces).map_err(output_error);
    }
    if args.landing {
        let rows = std::iter::once(landing_header(repo_id.is_some()))
            .chain(
                workspaces
                    .iter()
                    .map(|workspace| landing_row(repo_id, workspace)),
            )
            .collect::<Vec<_>>();
        return emit_aligned_rows(output, &rows);
    }
    if args.landed {
        return emit_bare_names(output, workspaces.iter());
    }
    emit_workspace_table(output, &workspaces)
}

/// Measure every session workspace against the project's own main workspace.
///
/// Main is left unmeasured because main *is* the target: comparing it to itself answers nothing, and
/// a zero-unlanded row for main would make it look retirable to anything reading the column.
///
/// One task per workspace, because the measurement is process-bound rather than CPU-bound and the
/// serial version costs the sum of every workspace's history walk — which on a project whose main
/// has been rewritten is the slowest thing `ls` could be asked to do.
async fn annotate_landing(workspaces: &mut [WorkspaceInfo]) {
    let Some(main) = workspaces
        .iter()
        .find(|workspace| workspace.role == WorkspaceRole::Main)
    else {
        let reason = String::from(
            "this project records no main workspace, so there is no branch to compare against",
        );
        for workspace in workspaces.iter_mut() {
            workspace.landing = Some(indeterminate_landing(reason.clone()));
        }
        return;
    };
    let target = std::sync::Arc::new(
        cowshed_core::landing::resolve_target(&main.mount, LANDING_TARGET_BRANCH).await,
    );
    let mut tasks = tokio::task::JoinSet::new();
    for (index, workspace) in workspaces.iter().enumerate() {
        if workspace.role == WorkspaceRole::Main {
            continue;
        }
        if workspace.state == WorkspaceState::Detached {
            continue;
        }
        let target = std::sync::Arc::clone(&target);
        let mount = workspace.mount.clone();
        tasks.spawn(async move {
            (
                index,
                cowshed_core::landing::measure(&target, &mount, "HEAD").await,
            )
        });
    }
    for workspace in workspaces.iter_mut() {
        if workspace.role != WorkspaceRole::Main && workspace.state == WorkspaceState::Detached {
            workspace.landing = Some(indeterminate_landing(String::from(
                "workspace is detached, so its repository is not mounted to be read",
            )));
        }
    }
    while let Some(finished) = tasks.join_next().await {
        match finished {
            Ok((index, landing)) => workspaces[index].landing = Some(landing),
            // A panicked measurement is still an unanswered question, and the row it belongs to is
            // no longer identifiable — so every row left unmeasured says so below.
            Err(_) => continue,
        }
    }
    for workspace in workspaces.iter_mut() {
        if workspace.role != WorkspaceRole::Main && workspace.landing.is_none() {
            workspace.landing = Some(indeterminate_landing(String::from(
                "the measurement did not complete",
            )));
        }
    }
}

fn indeterminate_landing(reason: String) -> WorkspaceLanding {
    WorkspaceLanding {
        dirty_files: None,
        commits: LandingCommits::Indeterminate { reason },
    }
}

/// Keep only the workspaces `--landed` may name, and report which of the rest went unanswered.
///
/// The output of `--landed` is meant to be piped into `rm`, so the bar for staying in it is a
/// measurement that says nothing is unlanded — never an absent measurement, and never main. Rows
/// dropped for having real unlanded work need no explanation; a row dropped because the question
/// could not be answered does, or the silence reads as "nothing here".
fn retain_fully_landed(workspaces: &mut Vec<WorkspaceInfo>) -> Vec<(String, String)> {
    let mut withheld = Vec::new();
    workspaces.retain(|workspace| {
        if workspace.role == WorkspaceRole::Main {
            return false;
        }
        match workspace.landing.as_ref().map(|landing| &landing.commits) {
            Some(LandingCommits::Indeterminate { reason }) => {
                withheld.push((workspace.workspace.as_str().to_owned(), reason.clone()));
                false
            }
            Some(commits) => commits.fully_landed(),
            None => {
                withheld.push((
                    workspace.workspace.as_str().to_owned(),
                    String::from("no landing measurement was taken"),
                ));
                false
            }
        }
    });
    withheld
}

fn emit_withheld<W: Write, E: Write>(
    output: &mut Output<W, E>,
    repo_id: Option<&RepoId>,
    withheld: &[(String, String)],
) -> Result<()> {
    for (workspace, reason) in withheld {
        let scope = match repo_id {
            Some(repo_id) => format!("{}/{workspace}", repo_id.as_str()),
            None => workspace.clone(),
        };
        output
            .guidance(&format!(
                "{scope} withheld: cannot determine whether its work is landed — {reason}"
            ))
            .map_err(output_error)?;
    }
    Ok(())
}

fn emit_bare_names<'a, W: Write, E: Write>(
    output: &mut Output<W, E>,
    workspaces: impl Iterator<Item = &'a WorkspaceInfo>,
) -> Result<()> {
    for workspace in workspaces {
        output
            .bare_line(workspace.workspace.as_str().as_bytes())
            .map_err(output_error)?;
    }
    Ok(())
}

/// The one place `--landing` prints a header, because four bare integers per row are unreadable
/// without one. `--landed`, whose output is consumed by `rm`, prints names and never a header.
fn landing_header(with_repo: bool) -> Vec<String> {
    let mut row = Vec::with_capacity(9);
    if with_repo {
        row.push(String::from("REPOSITORY"));
    }
    for column in [
        "WORKSPACE",
        "STATE",
        "UNLANDED",
        "LANDED",
        "BEHIND",
        "DIRTY",
        "BRANCH",
        "MOUNT",
    ] {
        row.push(String::from(column));
    }
    row
}

fn landing_row(repo_id: Option<&RepoId>, workspace: &WorkspaceInfo) -> Vec<String> {
    let mut row = workspace_row(repo_id, workspace);
    // `workspace_row` ends with branch then mount; the counts belong before them, where a reader
    // finds them without crossing a mountpoint.
    let tail = row.split_off(row.len() - 2);
    row.extend(landing_cells(workspace));
    row.extend(tail);
    row
}

/// `-` where nothing was measured by design, `?` where the answer could not be had. Keeping those
/// two apart is the whole point: one is main, the other is a workspace nobody can vouch for.
fn landing_cells(workspace: &WorkspaceInfo) -> [String; 4] {
    let unknown = || String::from("?");
    let Some(landing) = workspace.landing.as_ref() else {
        return std::array::from_fn(|_| String::from("-"));
    };
    let dirty = landing
        .dirty_files
        .map_or_else(unknown, |count| count.to_string());
    match &landing.commits {
        LandingCommits::Measured {
            unlanded,
            landed,
            behind,
            ..
        } => [
            unlanded.to_string(),
            landed.to_string(),
            behind.to_string(),
            dirty,
        ],
        LandingCommits::Indeterminate { .. } => {
            [unknown(), unknown(), unknown(), dirty]
        }
    }
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
    let mut hints = Vec::new();
    for finding in &report.findings {
        let severity = match finding.severity {
            FindingSeverity::Info => "info",
            FindingSeverity::Warning => "warning",
            FindingSeverity::Error => "error",
        };
        output
            .guidance(&format!(
                "[{severity} {}] {}",
                finding.code, finding.message
            ))
            .map_err(output_error)?;
        if !finding.hint.is_empty() && !hints.contains(&finding.hint.as_str()) {
            hints.push(finding.hint.as_str());
        }
    }
    for hint in hints {
        output.hint(hint).map_err(output_error)?;
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

#[async_trait]
trait AdoptHostSetup: Send {
    async fn plan(&mut self) -> Result<HostSetupPlan>;
    async fn execute(&mut self) -> Result<HostSetupReport>;
}

struct NativeAdoptHostSetup {
    home: PathBuf,
}

impl NativeAdoptHostSetup {
    fn for_canonical_home() -> Result<Self> {
        Ok(Self {
            home: gateway_service::canonical_home()?,
        })
    }
}

#[async_trait]
impl AdoptHostSetup for NativeAdoptHostSetup {
    async fn plan(&mut self) -> Result<HostSetupPlan> {
        plan_host_setup(&self.home).await
    }

    async fn execute(&mut self) -> Result<HostSetupReport> {
        execute_host_setup(&self.home).await
    }
}

async fn prepare_adopt_host_storage<S, W, E>(
    setup: &mut S,
    output: &mut Output<W, E>,
) -> Result<HostSetupReport>
where
    S: AdoptHostSetup,
    W: Write,
    E: Write,
{
    let plan = setup.plan().await?;
    if plan.requires_authorization {
        output
            .announce(
                "adopt will request administrator authorization once to set up cowshed host storage",
            )
            .map_err(output_error)?;
        for action in &plan.actions {
            output
                .announce(&host_action_evidence(action))
                .map_err(output_error)?;
        }
    }
    let report = setup.execute().await?;
    if let Some(error) = report.failure() {
        return Err(error.clone());
    }
    Ok(report)
}

fn host_action_evidence(action: &HostAction) -> String {
    match action {
        HostAction::CreateVolume {
            name,
            container,
            mount_at,
        } => format!(
            "create {name} in APFS container {container} and mount it at {}",
            mount_at.display()
        ),
        HostAction::MountExisting {
            name,
            uuid,
            size_bytes,
            mount_at,
        } => format!(
            "mount existing {name} ({uuid}, {size_bytes} bytes) at {}",
            mount_at.display()
        ),
        HostAction::RepairMounted {
            name,
            uuid,
            size_bytes,
            mounted_at,
            mount_at,
        } => format!(
            "repair {name} ({uuid}, {size_bytes} bytes) mounted at {}; canonical mount is {}",
            mounted_at.display(),
            mount_at.display()
        ),
        HostAction::EncryptVolume {
            name,
            uuid,
            size_bytes,
        } => format!(
            "FileVault-encrypt existing {name} ({uuid}, {size_bytes} bytes) in place and store its passphrase in System.keychain"
        ),
        HostAction::PinFstab { uuid, mount_at } => {
            format!("pin volume {uuid} at {} in /etc/fstab", mount_at.display())
        }
        HostAction::InstallMountService { label } => {
            format!(
                "install system LaunchDaemon {label} to unlock and mount cowshed volumes before login"
            )
        }
        HostAction::ReclaimStubs { paths } => format!(
            "reclaim mountpoint stubs: {}",
            paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Per-volume findings, reporting the mountpoint that was *observed* and carrying the one that is
/// *expected*.
///
/// The distinction is the whole point of the finding. A volume mounted somewhere other than its
/// canonical root is the case an operator has to see, and rendering the canonical root in the
/// message tells them their bytes are at a path they are not — so every message names
/// `mounted_at`, the place the volume actually is, and `path` carries the canonical root, which is
/// what a repair works towards.
///
/// The expected roots come from [`STORE_ROOT`] and [`CACHES_ROOT`] rather than from literals here:
/// the actions in the plan carry core's canonical mountpoint, and a second copy of it in the CLI
/// is a copy that can disagree with the volume the planner actually looked at.
fn host_storage_findings(plan: &HostSetupPlan) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (name, expected) in [
        ("cowshed.store", Path::new(STORE_ROOT)),
        ("cowshed.caches", Path::new(CACHES_ROOT)),
    ] {
        let action = plan.actions.iter().find(|action| {
            matches!(
                action,
                HostAction::CreateVolume { name: action_name, .. }
                    | HostAction::MountExisting { name: action_name, .. }
                    | HostAction::RepairMounted { name: action_name, .. }
                    if action_name == name
            )
        });
        let finding = match action {
            // No action for this volume means `MountedValid`, which is only reached for a volume
            // mounted at its canonical root — so here, and only here, observed and expected are
            // the same path by construction rather than by assumption.
            None => Finding {
                code: "host-volume".into(),
                severity: FindingSeverity::Info,
                message: format!(
                    "{name}: present, mounted at {}, marker valid",
                    expected.display()
                ),
                hint: String::new(),
                path: Some(expected.to_owned()),
            },
            Some(HostAction::CreateVolume {
                container,
                mount_at,
                ..
            }) => Finding {
                code: "host-volume-absent".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: absent from APFS container {container}; expected mount {}; marker absent",
                    mount_at.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(mount_at.clone()),
            },
            Some(HostAction::MountExisting {
                uuid,
                size_bytes,
                mount_at,
                ..
            }) => Finding {
                code: "mount".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: present ({uuid}, {size_bytes} bytes), not mounted; expected {}; marker unavailable while detached",
                    mount_at.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(mount_at.clone()),
            },
            Some(HostAction::RepairMounted {
                uuid,
                size_bytes,
                mounted_at,
                mount_at,
                ..
            }) if mounted_at == mount_at => Finding {
                code: "marker".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: present ({uuid}, {size_bytes} bytes), mounted at {}; marker missing or invalid",
                    mounted_at.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(mount_at.clone()),
            },
            Some(HostAction::RepairMounted {
                uuid,
                size_bytes,
                mounted_at,
                mount_at,
                ..
            }) => Finding {
                code: "mount".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: present ({uuid}, {size_bytes} bytes), mounted at {}; expected {}; marker will be validated after remount",
                    mounted_at.display(),
                    mount_at.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(mount_at.clone()),
            },
            Some(
                HostAction::EncryptVolume { .. }
                | HostAction::PinFstab { .. }
                | HostAction::ReclaimStubs { .. }
                | HostAction::InstallMountService { .. },
            ) => {
                unreachable!("volume lookup only selects volume mount actions")
            }
        };
        findings.push(finding);
    }
    for action in &plan.actions {
        match action {
            HostAction::EncryptVolume {
                name,
                uuid,
                size_bytes,
            } => findings.push(Finding {
                code: "host-filevault".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: FileVault encryption or its System.keychain passphrase is not configured ({uuid}, {size_bytes} bytes)"
                ),
                hint: "cowshed setup".into(),
                path: None,
            }),
            HostAction::PinFstab { uuid, mount_at } => findings.push(Finding {
                code: "host-fstab".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "volume {uuid} should mount at {} but /etc/fstab has no canonical pin",
                    mount_at.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(mount_at.clone()),
            }),
            HostAction::ReclaimStubs { paths } => findings.push(Finding {
                code: "mount-stubs".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "reclaimable mountpoint stubs: {}",
                    paths
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                hint: "cowshed setup".into(),
                path: paths.first().cloned(),
            }),
            HostAction::InstallMountService { label } => findings.push(Finding {
                code: "host-mount-service".into(),
                severity: FindingSeverity::Error,
                message: format!("system LaunchDaemon {label} is missing, outdated, or not loaded"),
                hint: "cowshed setup".into(),
                path: Some(PathBuf::from(
                    "/Library/LaunchDaemons/dev.cowshed.storage.plist",
                )),
            }),
            HostAction::CreateVolume { .. }
            | HostAction::MountExisting { .. }
            | HostAction::RepairMounted { .. } => {}
        }
    }
    findings
}

fn gateway_findings(status: &GatewayStatus) -> Vec<Finding> {
    let mut findings = vec![if status.running {
        Finding {
            code: "gateway".into(),
            severity: FindingSeverity::Info,
            message: format!(
                "gateway: launchd loaded; control socket answers at {}; cli version {}; daemon version {}",
                status.socket.display(),
                status.cli_version,
                status.daemon_version.as_deref().unwrap_or("unavailable")
            ),
            hint: String::new(),
            path: Some(status.socket.clone()),
        }
    } else {
        Finding {
            code: "gateway-down".into(),
            severity: FindingSeverity::Error,
            message: format!(
                "gateway: launchd {}; control socket does not answer at {}; cli version {}; daemon version unavailable",
                if status.installed {
                    "loaded"
                } else {
                    "not loaded"
                },
                status.socket.display(),
                status.cli_version
            ),
            hint: if status.installed {
                "cowshed gateway stop && cowshed gateway start".into()
            } else {
                "cowshed gateway start".into()
            },
            path: Some(status.socket.clone()),
        }
    }];
    if let Some(daemon_version) = status.daemon_version.as_deref()
        && daemon_version != status.cli_version
    {
        findings.push(Finding {
            code: "gateway-version-skew".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "gateway version skew: cli {}; daemon {}",
                status.cli_version, daemon_version
            ),
            hint: "cowshed gateway stop && cowshed gateway start".into(),
            path: Some(status.socket.clone()),
        });
    }
    findings
}

/// An unmounted main is critical, not informational.
///
/// Mains are always-mounted (02_workspaces.md): the gateway mounts every one across every adopted
/// project before it serves, so this is the user's own checkout missing from their shell, editor,
/// and Finder — not a workspace they chose to detach. Both paths are named because they answer
/// different questions: the mountpoint is the directory that looks wrong, the image is the volume
/// that belongs there. The remedy is the gateway's startup pass, which is what mounts mains.
fn main_mount_finding(main: &UnreachableMain) -> Finding {
    Finding {
        code: "main-not-mounted".into(),
        severity: FindingSeverity::Error,
        message: format!(
            "{}: main is not mounted at {} (image {}): {}",
            main.repo_id,
            main.mountpoint.display(),
            main.image.display(),
            main.reason
        ),
        hint: "cowshed gateway start".into(),
        path: Some(main.mountpoint.clone()),
    }
}

fn retired_mount_layout_findings(store_root: &Path) -> Vec<Finding> {
    match retired_layout_paths(store_root) {
        Ok(records) => records
            .into_iter()
            .map(|record| Finding {
                code: "retired-mount-layout".into(),
                severity: FindingSeverity::Error,
                message: record.doctor_message(),
                hint: RETIRED_LAYOUT_HINT.into(),
                path: Some(record.metadata_path),
            })
            .collect(),
        Err(error) => vec![Finding {
            code: "retired-mount-layout-scan".into(),
            severity: FindingSeverity::Error,
            message: format!(
                "could not inspect detached metadata for retired mount paths: {error}"
            ),
            hint: "cowshed doctor --json".into(),
            path: Some(store_root.to_path_buf()),
        }],
    }
}

fn sccache_finding(status: &SccacheStatus) -> Finding {
    let (severity, hint) = if status.running {
        (FindingSeverity::Info, String::new())
    } else if status.installed {
        (
            FindingSeverity::Warning,
            "cowshed sccache stop && cowshed sccache start".into(),
        )
    } else {
        (FindingSeverity::Info, "cowshed sccache start".into())
    };
    Finding {
        code: if status.running {
            "sccache"
        } else {
            "sccache-down"
        }
        .into(),
        severity,
        message: format!(
            "sccache: launchd {}; socket {} at {}{}",
            if status.installed {
                "loaded"
            } else {
                "not loaded"
            },
            if status.running {
                "answers"
            } else {
                "does not answer"
            },
            status.socket.display(),
            status
                .stats
                .as_ref()
                .map_or_else(String::new, |stats| format!(
                    "; {} compile requests, {} executed, {} configured base directories",
                    stats.compile_requests,
                    stats.requests_executed,
                    stats.base_directories.len()
                ))
        ),
        hint,
        path: Some(status.socket.clone()),
    }
}

struct HostDiagnosis {
    storage_ready: bool,
    findings: Vec<Finding>,
}

async fn diagnose_host() -> Result<HostDiagnosis> {
    let home = gateway_service::canonical_home()?;
    let plan = plan_host_setup(&home).await;
    let mut diagnosis = match plan {
        Ok(plan) => HostDiagnosis {
            storage_ready: plan.actions.is_empty(),
            findings: host_storage_findings(&plan),
        },
        Err(error) => HostDiagnosis {
            storage_ready: false,
            findings: vec![Finding {
                code: "host-storage".into(),
                severity: FindingSeverity::Error,
                message: error.message,
                hint: "cowshed setup".into(),
                path: None,
            }],
        },
    };
    if diagnosis.storage_ready {
        diagnosis.findings.extend(retired_mount_layout_findings(
            CanonicalRoots::global().store(),
        ));
    }
    match gateway_service::service_status().await {
        Ok(status) => diagnosis.findings.extend(gateway_findings(&status)),
        Err(error) => diagnosis.findings.push(Finding {
            code: "gateway-status".into(),
            severity: FindingSeverity::Error,
            message: error.message,
            hint: "cowshed gateway start".into(),
            path: None,
        }),
    }
    match crate::sccache_service::service_status().await {
        Ok(status) => diagnosis.findings.push(sccache_finding(&status)),
        Err(error) => diagnosis.findings.push(Finding {
            code: "sccache-status".into(),
            severity: FindingSeverity::Warning,
            message: error.message,
            hint: "cowshed sccache status".into(),
            path: None,
        }),
    }
    if diagnosis.storage_ready {
        match adopted_projects().await {
            Ok(projects) => diagnosis.findings.push(Finding {
                code: "workspace-inventory".into(),
                severity: FindingSeverity::Info,
                message: format!("{} adopted project(s) recorded", projects.len()),
                hint: if projects.is_empty() {
                    "cowshed adopt <git-root>".into()
                } else {
                    String::new()
                },
                path: None,
            }),
            Err(error) => diagnosis.findings.push(Finding {
                code: "workspace-inventory".into(),
                severity: FindingSeverity::Error,
                message: error.message,
                hint: error.hint,
                path: None,
            }),
        }
        // Every adopted project, not whichever one the cwd sits in: mains are always-mounted, and
        // the same host state has to yield the same verdict from any directory (06_cli.md rule 4).
        match unmounted_mains().await {
            Ok(mains) => diagnosis
                .findings
                .extend(mains.iter().map(main_mount_finding)),
            Err(error) => diagnosis.findings.push(Finding {
                code: "main-mounts".into(),
                severity: FindingSeverity::Error,
                message: error.message,
                hint: error.hint,
                path: None,
            }),
        }
    }
    Ok(diagnosis)
}

fn doctor_report(findings: Vec<Finding>) -> DoctorReport {
    DoctorReport {
        healthy: !findings
            .iter()
            .any(|finding| finding.severity == FindingSeverity::Error),
        findings,
    }
}

fn emit_project_checks_skipped<W: Write, E: Write>(
    output: &mut Output<W, E>,
    error: Option<&CowshedError>,
) -> Result<()> {
    let message = error.map_or_else(
        || "project checks skipped: no adopted checkout at cwd".to_owned(),
        |error| format!("project checks skipped: {}", error.message),
    );
    output.note(&message).map_err(output_error)
}

fn emit_doctor_report<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    report: DoctorReport,
) -> Result<DispatchExit> {
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

async fn run_doctor_command<W, E>(cli: Cli, output: &mut Output<W, E>) -> Result<DispatchExit>
where
    W: Write + Send,
    E: Write + Send,
{
    let project_root = resolve_project_root(&cli).await;
    let mut diagnosis = diagnose_host().await?;
    if diagnosis.storage_ready {
        match project_root {
            Ok(root) => match ActorBridge::open_existing(&root).await {
                Ok(mut bridge) => {
                    let identity = git_identity_findings(&bridge);
                    let project = bridge.doctor().await;
                    let teardown = bridge.shutdown().await.err();
                    match identity {
                        Ok(findings) => diagnosis.findings.extend(findings),
                        Err(error) => diagnosis.findings.push(Finding {
                            code: "git-identity".into(),
                            severity: FindingSeverity::Warning,
                            message: error.message,
                            hint: error.hint,
                            path: Some(root.clone()),
                        }),
                    }
                    match project {
                        Ok(report) => diagnosis.findings.extend(report.findings),
                        Err(error) => diagnosis.findings.push(Finding {
                            code: "project-doctor".into(),
                            severity: FindingSeverity::Error,
                            message: error.message,
                            hint: error.hint,
                            path: Some(root),
                        }),
                    }
                    if let Some(error) = teardown {
                        diagnosis.findings.push(Finding {
                            code: "project-shutdown".into(),
                            severity: FindingSeverity::Error,
                            message: error.message,
                            hint: error.hint,
                            path: None,
                        });
                    }
                }
                // Opening cwd is enrichment only: stale identity or remote-name bindings belong to
                // that checkout, while the host storage, services, and store-wide inventory above
                // remain authoritative. Reporting an open failure as a host error made the same
                // machine healthy or unhealthy solely according to which clone invoked doctor.
                Err(error) => emit_project_checks_skipped(output, Some(&error))?,
            },
            Err(_) => emit_project_checks_skipped(output, None)?,
        }
    }
    emit_doctor_report(output, cli.global.json, doctor_report(diagnosis.findings))
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
    if matches!(&cli.command, Command::Doctor) {
        return run_doctor_command(cli, output).await;
    }
    if let Command::Detach(args) = &cli.command
        && !args.all
    {
        let root = resolve_detach_root(&cli).await?;
        let bridge = ActorBridge::open_existing(&root).await?;
        return dispatch_and_shutdown(bridge, cli, stdin, output).await;
    }
    let discovery = cli.command.project_discovery();
    if discovery == ProjectDiscovery::NotUsed {
        return dispatch_host_command(cli, output, false).await;
    }
    let root = match resolve_project_root(&cli).await {
        Ok(root) => root,
        Err(_) if discovery == ProjectDiscovery::Optional => {
            return dispatch_host_command(cli, output, true).await;
        }
        Err(error) => return Err(error),
    };
    let mode = runtime_open_mode(&cli.command);
    let requested_repo_id = runtime_open_repo_id(&cli.command)?;
    if mode == RuntimeOpenMode::Provision {
        let mut setup = NativeAdoptHostSetup::for_canonical_home()?;
        let _ = prepare_adopt_host_storage(&mut setup, output).await?;
    }
    let bridge = match mode {
        RuntimeOpenMode::Provision => ActorBridge::open_for_adopt(&root, requested_repo_id).await,
        RuntimeOpenMode::ExistingOnly => ActorBridge::open_existing(&root).await,
    };
    let bridge = match bridge {
        Ok(bridge) => bridge,
        Err(error)
            if discovery == ProjectDiscovery::Optional && optional_project_unavailable(&error) =>
        {
            return dispatch_host_command(cli, output, true).await;
        }
        Err(error) => return Err(error),
    };
    if let Command::New(args) = &cli.command
        && let Err(error) = report_new_git_identity(&bridge, &args.name, output)
    {
        output
            .guidance(&format!("git identity probe skipped: {}", error.message))
            .map_err(output_error)?;
    }
    dispatch_and_shutdown(bridge, cli, stdin, output).await
}

pub async fn run_host_command<W, E>(cli: Cli, output: &mut Output<W, E>) -> Result<DispatchExit>
where
    W: Write + Send,
    E: Write + Send,
{
    dispatch_host_command(cli, output, false).await
}

async fn dispatch_host_command<W, E>(
    cli: Cli,
    output: &mut Output<W, E>,
    project_checks_skipped: bool,
) -> Result<DispatchExit>
where
    W: Write + Send,
    E: Write + Send,
{
    match cli.command {
        Command::Attach(args) if args.all && args.workspace.is_none() => {
            let infos = attach_store_wide(args.browse).await?;
            emit_attached(output, cli.global.json, &infos)?;
            Ok(success())
        }
        Command::Detach(args) if args.all && args.workspace.is_none() => {
            detach_store_wide().await?;
            if cli.global.json {
                output.success(EmptyResult {}).map_err(output_error)?;
            }
            Ok(success())
        }
        Command::List(args) => {
            let mut projects = list_all_adopted_projects().await?;
            projects.sort_by(|left, right| left.repo_id.cmp(&right.repo_id));
            for project in &mut projects {
                project
                    .workspaces
                    .sort_by(|left, right| left.workspace.cmp(&right.workspace));
            }
            emit_project_listing(output, cli.global.json, args, projects).await?;
            Ok(success())
        }
        Command::Doctor => {
            let diagnosis = diagnose_host().await?;
            if project_checks_skipped {
                emit_project_checks_skipped(output, None)?;
            }
            emit_doctor_report(output, cli.global.json, doctor_report(diagnosis.findings))
        }
        _ => Err(CowshedError::internal(
            "command without project context was not dispatched by its host service",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("writer lock")
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct RefusingAdoptSetup {
        plan: HostSetupPlan,
        stderr: SharedWriter,
        saw_announcement_before_execute: Arc<AtomicBool>,
    }

    #[async_trait]
    impl AdoptHostSetup for RefusingAdoptSetup {
        async fn plan(&mut self) -> Result<HostSetupPlan> {
            Ok(self.plan.clone())
        }

        async fn execute(&mut self) -> Result<HostSetupReport> {
            let announced = String::from_utf8(self.stderr.0.lock().expect("writer lock").clone())
                .expect("utf8 announcement")
                .contains("will request administrator authorization");
            self.saw_announcement_before_execute
                .store(announced, Ordering::SeqCst);
            Err(CowshedError::sandbox_denied(
                "authorization declined",
                "cowshed setup",
            ))
        }
    }

    #[tokio::test]
    async fn adopt_announces_an_escalating_plan_before_authorization() {
        let stderr = SharedWriter::default();
        let observed = Arc::new(AtomicBool::new(false));
        let mut setup = RefusingAdoptSetup {
            plan: HostSetupPlan {
                actions: vec![HostAction::PinFstab {
                    uuid: "1111-2222".into(),
                    mount_at: PathBuf::from("/private/cowshed/store"),
                }],
                requires_authorization: true,
                non_destructive: true,
            },
            stderr: stderr.clone(),
            saw_announcement_before_execute: Arc::clone(&observed),
        };
        let mut output = Output::new(Vec::new(), stderr, false);

        let error = prepare_adopt_host_storage(&mut setup, &mut output)
            .await
            .expect_err("authorization refusal");

        assert_eq!(error.code, ErrorCode::SandboxDenied);
        assert!(observed.load(Ordering::SeqCst));
    }

    #[test]
    fn setup_required_becomes_per_volume_findings_with_evidence() {
        let plan = HostSetupPlan {
            actions: vec![
                HostAction::ReclaimStubs {
                    paths: vec![
                        PathBuf::from("/private/cowshed/store/gateway.stderr.log"),
                        PathBuf::from("/private/cowshed/store/stale.sock"),
                    ],
                },
                HostAction::RepairMounted {
                    name: "cowshed.store".into(),
                    uuid: "STORE-UUID".into(),
                    size_bytes: 4096,
                    mounted_at: PathBuf::from("/Volumes/cowshed.store"),
                    mount_at: PathBuf::from("/private/cowshed/store"),
                },
                HostAction::MountExisting {
                    name: "cowshed.caches".into(),
                    uuid: "CACHE-UUID".into(),
                    size_bytes: 8192,
                    mount_at: PathBuf::from("/private/cowshed/caches"),
                },
                HostAction::InstallMountService {
                    label: "dev.cowshed.storage".into(),
                },
            ],
            requires_authorization: true,
            non_destructive: true,
        };

        let findings = host_storage_findings(&plan);

        assert!(findings.iter().any(|finding| {
            finding.code == "mount"
                && finding.message.contains("STORE-UUID")
                && finding.message.contains("/Volumes/cowshed.store")
                && finding.message.contains("/private/cowshed/store")
                && finding.hint == "cowshed setup"
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "mount"
                && finding.message.contains("CACHE-UUID")
                && finding
                    .message
                    .contains("marker unavailable while detached")
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "mount-stubs"
                && finding.message.contains("gateway.stderr.log")
                && finding.message.contains("stale.sock")
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "host-mount-service"
                && finding.message.contains("dev.cowshed.storage")
                && finding.hint == "cowshed setup"
        }));
        assert!(findings.iter().all(|finding| !finding.message.is_empty()));
    }

    /// A mis-mounted volume is the case the distinction exists for: the message has to name where
    /// the bytes actually are, because that is what looks wrong, while the JSON `path` carries the
    /// canonical root a repair works towards. Rendering the canonical root as the observed
    /// mountpoint would tell an operator their store is somewhere it is not.
    #[test]
    fn a_mis_mounted_volume_reports_the_observed_mountpoint_and_expects_the_canonical_root() {
        let observed = PathBuf::from("/Volumes/cowshed.store");
        let plan = HostSetupPlan {
            actions: vec![HostAction::RepairMounted {
                name: "cowshed.store".into(),
                uuid: "STORE-UUID".into(),
                size_bytes: 4096,
                mounted_at: observed.clone(),
                mount_at: PathBuf::from(STORE_ROOT),
            }],
            requires_authorization: true,
            non_destructive: true,
        };

        let findings = host_storage_findings(&plan);
        let store = findings
            .iter()
            .find(|finding| finding.message.starts_with("cowshed.store:"))
            .expect("a finding for the store");

        assert_eq!(store.code, "mount");
        assert_eq!(store.path.as_deref(), Some(Path::new(STORE_ROOT)));
        assert!(
            store
                .message
                .contains(&format!("mounted at {}", observed.display())),
            "{}",
            store.message
        );
        assert!(
            store.message.contains(&format!("expected {STORE_ROOT}")),
            "{}",
            store.message
        );
    }

    /// The expected roots are core's, not a second copy in the CLI: a copy can disagree with the
    /// volume the planner actually looked at, and then the finding describes a host nobody has.
    #[test]
    fn expected_volume_roots_come_from_the_canonical_constants() {
        let findings = host_storage_findings(&HostSetupPlan {
            actions: Vec::new(),
            requires_authorization: false,
            non_destructive: true,
        });

        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.path.clone().expect("a path per volume"))
                .collect::<Vec<_>>(),
            vec![PathBuf::from(STORE_ROOT), PathBuf::from(CACHES_ROOT)]
        );
        assert_eq!(
            CanonicalRoots::global().store(),
            Path::new(STORE_ROOT),
            "the canonical roots and the doctor's expected roots must be one definition"
        );
        for finding in &findings {
            assert_eq!(finding.code, "host-volume");
            let expected = finding.path.as_deref().expect("a path per volume");
            assert!(
                finding
                    .message
                    .contains(&format!("mounted at {}", expected.display())),
                "{}",
                finding.message
            );
        }
    }

    #[test]
    fn setup_required_report_emits_json_findings_and_environment_exit() {
        let plan = HostSetupPlan {
            actions: vec![HostAction::CreateVolume {
                name: "cowshed.store".into(),
                container: "disk3".into(),
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            requires_authorization: true,
            non_destructive: false,
        };
        let mut output = Output::new(Vec::new(), Vec::new(), false);

        let exit = emit_doctor_report(
            &mut output,
            true,
            doctor_report(host_storage_findings(&plan)),
        )
        .expect("doctor emits");

        assert_eq!(exit.code, 5);
        let (stdout, stderr) = output.into_inner();
        assert!(stderr.is_empty());
        let envelope: serde_json::Value =
            serde_json::from_slice(&stdout).expect("doctor JSON envelope");
        assert_eq!(envelope["ok"], true);
        assert_eq!(envelope["result"]["healthy"], false);
        assert!(
            envelope["result"]["findings"]
                .as_array()
                .expect("findings")
                .iter()
                .any(|finding| finding["code"] == "host-volume-absent"
                    && finding["hint"] == "cowshed setup"
                    && finding["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("disk3")))
        );
    }

    #[test]
    fn gateway_version_skew_is_a_warning_with_a_restart_hint() {
        let status = GatewayStatus {
            installed: true,
            running: true,
            socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
            cli_version: "2.0.0".into(),
            daemon_version: Some("1.9.0".into()),
            cache_entries: 0,
            cache_bytes: 0,
            active_workspaces: 0,
        };

        let findings = gateway_findings(&status);
        let skew = findings
            .iter()
            .find(|finding| finding.code == "gateway-version-skew")
            .expect("version skew finding");
        assert_eq!(skew.severity, FindingSeverity::Warning);
        assert_eq!(skew.hint, "cowshed gateway stop && cowshed gateway start");
        assert!(skew.message.contains("cli 2.0.0"));
        assert!(skew.message.contains("daemon 1.9.0"));
    }

    #[test]
    fn service_findings_name_launchd_socket_and_recovery() {
        let gateway = gateway_findings(&GatewayStatus {
            installed: true,
            running: false,
            socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
            cli_version: "2.0.0".into(),
            daemon_version: None,
            cache_entries: 0,
            cache_bytes: 0,
            active_workspaces: 0,
        });
        assert_eq!(gateway[0].code, "gateway-down");
        assert_eq!(gateway[0].severity, FindingSeverity::Error);
        assert!(gateway[0].message.contains("launchd loaded"));
        assert!(gateway[0].message.contains("gateway.sock"));
        assert_eq!(
            gateway[0].hint,
            "cowshed gateway stop && cowshed gateway start"
        );

        let sccache = sccache_finding(&SccacheStatus {
            installed: true,
            running: false,
            socket: PathBuf::from("/private/cowshed/store/sccache.sock"),
            stats: None,
        });
        assert_eq!(sccache.code, "sccache-down");
        assert_eq!(sccache.severity, FindingSeverity::Warning);
        assert!(sccache.message.contains("launchd loaded"));
        assert!(sccache.message.contains("sccache.sock"));
        assert_eq!(
            sccache.hint,
            "cowshed sccache stop && cowshed sccache start"
        );
    }

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
            (vec!["attach", "--all"], RuntimeOpenMode::ExistingOnly),
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
    fn optional_context_falls_back_only_when_an_adopted_checkout_is_absent() {
        for code in [ErrorCode::EnvironmentMissing, ErrorCode::NotFound] {
            assert!(optional_project_unavailable(&CowshedError::new(
                code, "missing", "old hint"
            )));
        }
        for code in [
            ErrorCode::Internal,
            ErrorCode::Usage,
            ErrorCode::Conflict,
            ErrorCode::SandboxDenied,
            ErrorCode::Integrity,
        ] {
            assert!(!optional_project_unavailable(&CowshedError::new(
                code,
                "real failure",
                "repair it"
            )));
        }
    }

    #[test]
    fn repository_binding_mismatch_skips_project_checks_without_changing_host_verdict() {
        let mismatch = CowshedError::conflict(
            "repository binding remote codebase does not match Git configuration",
            "restore the recorded remote before opening cowshed",
        );
        let mut output = Output::new(Vec::new(), Vec::new(), false);

        emit_project_checks_skipped(&mut output, Some(&mismatch)).expect("skip note");
        let report = doctor_report(Vec::new());

        assert!(report.healthy);
        assert!(
            report
                .findings
                .iter()
                .all(|finding| finding.code != "project-open"),
            "optional project-open failures do not become host findings"
        );
        let (_, stderr) = output.into_inner();
        assert_eq!(
            stderr,
            b"project checks skipped: repository binding remote codebase does not match Git configuration\n"
        );
    }

    #[test]
    fn project_discovery_failure_points_to_inventory_and_explicit_context() {
        let original = CowshedError::environment_missing(
            "/tmp is not inside a standalone git repository",
            "cowshed adopt <git-root>",
        );
        let mapped = project_context_error(original);
        assert_eq!(
            mapped.hint,
            "cowshed ls; cowshed --project <git-root> <command>"
        );
        assert_eq!(mapped.code, ErrorCode::EnvironmentMissing);
        assert_eq!(
            mapped.message,
            "/tmp is not inside a standalone git repository"
        );
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

    /// An unmounted main is a critical finding: it names both paths, carries a remedy, and makes
    /// the report unhealthy. Mains are always-mounted (02_workspaces.md), so `healthy: true` over
    /// a checkout the user cannot open would be the one verdict doctor must never give.
    #[test]
    fn an_unmounted_main_is_a_critical_doctor_finding() {
        use cowshed_core::repository::RepoId;

        let main = UnreachableMain {
            repo_id: RepoId::parse("acme/widget").expect("repo"),
            image: PathBuf::from("/private/cowshed/store/acme/widget/main.asif"),
            mountpoint: PathBuf::from("/Users/dev/src/widget"),
            reason: String::from("main's volume is not mounted"),
        };

        let finding = main_mount_finding(&main);

        assert_eq!(finding.code, "main-not-mounted");
        assert_eq!(finding.severity, FindingSeverity::Error);
        assert_eq!(
            finding.message,
            "acme/widget: main is not mounted at /Users/dev/src/widget \
             (image /private/cowshed/store/acme/widget/main.asif): main's volume is not mounted"
        );
        assert_eq!(finding.hint, "cowshed gateway start");
        assert_eq!(finding.path.as_deref(), Some(main.mountpoint.as_path()));
        let report = doctor_report(vec![finding]);
        assert!(
            !report.healthy,
            "a project whose checkout is not mounted is not a healthy host"
        );
    }

    #[test]
    fn retired_mount_metadata_is_a_critical_host_doctor_finding() {
        use cowshed_core::storage::host_config::{
            execute_mount_root_change, plan_mount_root_change,
        };

        let root = std::env::temp_dir().join(format!(
            "cowshed-retired-layout-doctor-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let store = root.join("store");
        let project = bind_test_repository(&store, "acme/widget");
        let metadata = project.join("sessions/raven.asif.grants.json");
        std::fs::create_dir_all(metadata.parent().unwrap()).unwrap();
        let plan = plan_mount_root_change(&store, &root.join("configured-mount-root"), []).unwrap();
        execute_mount_root_change(&plan).unwrap();
        let recorded = store.join("mnt/acme/widget/raven");
        std::fs::write(
            &metadata,
            serde_json::to_vec(&serde_json::json!({ "write": [recorded] })).unwrap(),
        )
        .unwrap();

        let findings = retired_mount_layout_findings(&store);
        assert_eq!(findings.len(), 1);
        let finding = &findings[0];
        assert_eq!(finding.code, "retired-mount-layout");
        assert_eq!(finding.severity, FindingSeverity::Error);
        assert!(
            finding
                .message
                .contains("recorded under retired layout, run cowshed setup --mount-root <dir>")
        );
        assert_eq!(finding.hint, RETIRED_LAYOUT_HINT);
        assert_eq!(finding.path.as_deref(), Some(metadata.as_path()));
        assert!(!doctor_report(findings).healthy);

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn store_readdir_resolves_a_session_in_another_project_without_cwd() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-detach-store-readdir-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = root.join("store");
        let other_checkout = PathBuf::from("/other/project");
        write_session_image(&store, "acme", "widget", "fox", Path::new("/cwd/project"));
        write_session_image(&store, "zeta", "tool", "raven", &other_checkout);

        let resolved = resolve_session_project_root(&store, "raven").expect("found raven");
        assert_eq!(resolved, other_checkout);

        let missing = resolve_session_project_root(&store, "absent").unwrap_err();
        assert_eq!(missing.code, ErrorCode::NotFound);

        write_session_image(&store, "acme", "widget", "raven", Path::new("/cwd/project"));
        let conflict = resolve_session_project_root(&store, "raven").unwrap_err();
        assert_eq!(conflict.code, ErrorCode::Conflict);
        assert!(conflict.message.contains("zeta/tool"));
        assert!(conflict.message.contains("acme/widget"));

        std::fs::remove_dir_all(root).unwrap();
    }

    fn bind_test_repository(store: &Path, repo_id: &str) -> PathBuf {
        let repo_id =
            cowshed_core::repository::RepoId::parse(repo_id).expect("repository identity");
        let paths = cowshed_core::storage::StorageLayout::new(store, &repo_id)
            .expect("project paths")
            .project()
            .clone();
        std::fs::create_dir_all(&paths.project_root).expect("project root");
        let binding = cowshed_core::repository::RepositoryBinding::new(vec![
            cowshed_core::repository::BoundIdentity {
                repo_id,
                remote_name: None,
                remote_url: None,
                primary: true,
            },
        ])
        .expect("repository binding");
        cowshed_core::metadata::write_json(&paths.repository_binding, &binding)
            .expect("binding file");
        paths.project_root
    }

    fn write_session_image(
        store: &Path,
        owner: &str,
        repo: &str,
        workspace: &str,
        project_root: &Path,
    ) {
        let sessions = store.join(owner).join(repo).join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let image = sessions.join(format!("{workspace}.asif"));
        std::fs::write(&image, b"image").unwrap();
        let mut sidecar = serde_json::json!({
            "version": 1,
            "repoId": format!("{owner}/{repo}"),
            "workspace": workspace,
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "publicationState": "active",
            "imageFormat": "asif",
            "platform": "macos",
            "updatedAt": "2026-07-11T12:34:56Z",
            "revision": 1,
            "portBlock": { "base": 40976, "size": 16 },
            "infoSnapshot": {
                "projectRoot": project_root,
                "role": "workspace",
                "baseCommit": "8f31c2d",
                "createdAt": "2026-07-11T12:00:00Z",
                "capturedAt": "2026-07-11T12:34:00Z",
                "stale": false
            }
        });
        if !cfg!(target_os = "macos") {
            sidecar["platform"] = serde_json::json!("linux");
            sidecar.as_object_mut().unwrap().remove("portBlock");
        }
        std::fs::write(
            format!("{}.grants.json", image.display()),
            serde_json::to_vec(&sidecar).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn doctor_prints_status_then_findings_then_unique_hints() {
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        emit_doctor(
            &mut output,
            &DoctorReport {
                healthy: false,
                findings: vec![
                    Finding {
                        code: "mount".into(),
                        severity: FindingSeverity::Error,
                        message: "cowshed.store: present, not mounted".into(),
                        hint: "cowshed setup".into(),
                        path: None,
                    },
                    Finding {
                        code: "mount".into(),
                        severity: FindingSeverity::Error,
                        message: "cowshed.caches: present, not mounted".into(),
                        hint: "cowshed setup".into(),
                        path: None,
                    },
                    Finding {
                        code: "gateway-down".into(),
                        severity: FindingSeverity::Error,
                        message: "gateway: launchd loaded; control socket does not answer".into(),
                        hint: "cowshed gateway stop && cowshed gateway start".into(),
                        path: None,
                    },
                ],
            },
        )
        .unwrap();
        let (stdout, stderr) = output.into_inner();
        assert_eq!(stdout, b"unhealthy\n");
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "cowshed: [error mount] cowshed.store: present, not mounted\n\
             cowshed: [error mount] cowshed.caches: present, not mounted\n\
             cowshed: [error gateway-down] gateway: launchd loaded; control socket does not answer\n\
             next: cowshed setup\n\
             next: cowshed gateway stop && cowshed gateway start\n"
        );
    }
}
