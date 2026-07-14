use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use url::Url;

use crate::api::dto::{
    AdoptOptions, AttachOptions, CheckpointOptions, CheckpointQuota, CheckpointResult, CommandArg,
    CreateOptions, DoctorReport, EmptyResult, ExecRequest, GcOptions, GcReport, GitOid, GrantDelta,
    GrantSet, JobId, JobInfo, LandOptions, LandReport, MirrorInfo, PushOptions, PushReport,
    RebaseOptions, RemoveOptions, RevisionResult, RunSandboxMode, StdinSource,
    WorkspaceIncarnation, WorkspaceInfo, validate_command_argv,
};
use crate::api::server::{
    ConnectionAuthority, RouterCommand, RouterHandle, RouterRequest, RouterResponse,
};
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::WorkspaceName;
use crate::repository::{RepoId, RepositoryBinding};

const ROUTER_CAPACITY: usize = 64;
const MAX_LOG_CHUNK_BYTES: usize = 64 * 1024;

/// Immutable facts returned by one authoritative substrate enumeration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceSnapshot {
    pub info: WorkspaceInfo,
    pub grants: GrantSet,
    pub lifecycle_revision: u64,
    pub topology_revision: u64,
}

/// Project identity fixed for the lifetime of one controller actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectDescriptor {
    pub repo_id: RepoId,
    pub binding: RepositoryBinding,
    pub git_root: PathBuf,
    pub store_root: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeJobStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeLogChunk {
    pub bytes: Bytes,
    pub next_offset: u64,
    pub eof: bool,
}

/// Actor-owned platform seam. Implementations must reread authoritative repository, storage,
/// metadata, mount, gateway, and supervisor facts inside every effectful method before mutation.
///
/// The seam exists for deterministic lifecycle/failpoint tests; production uses one of
/// [`ProjectRuntime::open_for_adopt`] or [`ProjectRuntime::open_existing`]. It deliberately
/// requires `&mut self`, preventing a backend from being shared behind a lock or mutated outside
/// the project actor.
#[async_trait]
pub trait ProjectRuntimeHost: Send + 'static {
    fn descriptor(&self) -> &ProjectDescriptor;

    async fn recover(&mut self) -> Result<()>;
    async fn snapshots(&mut self) -> Result<Vec<WorkspaceSnapshot>>;
    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceSnapshot>;

    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceSnapshot>;
    async fn create(
        &mut self,
        workspace: WorkspaceName,
        options: CreateOptions,
    ) -> Result<WorkspaceSnapshot>;
    async fn fork(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot>;
    async fn ensure(&mut self, workspace: WorkspaceName) -> Result<crate::api::dto::EnsureReport>;
    async fn attach(&mut self, workspace: WorkspaceName, options: AttachOptions) -> Result<()>;
    async fn detach(&mut self, workspace: WorkspaceName) -> Result<()>;
    async fn checkpoint(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: Option<WorkspaceIncarnation>,
        options: CheckpointOptions,
    ) -> Result<CheckpointResult>;
    async fn restore(&mut self, workspace: WorkspaceName, label: String) -> Result<()>;
    async fn remove(&mut self, workspace: WorkspaceName, options: RemoveOptions) -> Result<()>;
    async fn gc(&mut self, options: GcOptions) -> Result<GcReport>;
    async fn grant(
        &mut self,
        workspace: WorkspaceName,
        delta: GrantDelta,
        revoke: bool,
    ) -> Result<GrantSet>;
    async fn assign_slot(&mut self, workspace: WorkspaceName, slot: u32) -> Result<()>;
    async fn set_checkpoint_quota(
        &mut self,
        workspace: WorkspaceName,
        quota: CheckpointQuota,
    ) -> Result<()>;
    async fn rebase(&mut self, workspace: WorkspaceName, options: RebaseOptions) -> Result<GitOid>;
    async fn land(&mut self, workspace: WorkspaceName, options: LandOptions) -> Result<LandReport>;
    async fn push(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: WorkspaceIncarnation,
        options: PushOptions,
    ) -> Result<PushReport>;
    async fn repo_mirror(&mut self, workspace: WorkspaceName, url: Url) -> Result<MirrorInfo>;
    async fn doctor(&mut self) -> Result<DoctorReport>;

    async fn open_worker(&mut self, workspace: WorkspaceName) -> Result<WorkspaceSnapshot>;
    async fn open_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        name: Option<String>,
    ) -> Result<()>;
    async fn close_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        name: Option<String>,
    ) -> Result<()>;
    async fn exec(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        session: Option<String>,
        request: ExecRequest,
    ) -> Result<JobId>;
    async fn stdin_write(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        bytes: Bytes,
    ) -> Result<()>;
    async fn stdin_close(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()>;
    async fn list_jobs(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    ) -> Result<Vec<JobInfo>>;
    async fn job_info(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobInfo>;
    async fn wait_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobInfo>;
    async fn kill_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()>;
    async fn detach_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()>;
    async fn read_log(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        stream: RuntimeJobStream,
        offset: u64,
        follow: bool,
    ) -> Result<RuntimeLogChunk>;
}

/// Cloneable ingress plus ownership of the single Tokio actor task.
pub struct ProjectRuntime {
    descriptor: ProjectDescriptor,
    router: RouterHandle,
    actor: JoinHandle<()>,
}

impl ProjectRuntime {
    /// Opens the production runtime with foreground provisioning authority.
    ///
    /// Only the parsed `cowshed adopt` command may call this entrypoint.
    pub async fn open_for_adopt(
        project_root: impl AsRef<Path>,
        requested_repo_id: Option<RepoId>,
    ) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::Provision,
            requested_repo_id,
        )
        .await
    }

    /// Opens the production runtime without storage provisioning authority.
    ///
    /// Ordinary commands and background services must use this entrypoint. Missing or incorrectly
    /// mounted storage fails closed without creating or mounting anything.
    pub async fn open_existing(project_root: impl AsRef<Path>) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly,
            None,
        )
        .await
    }

    async fn open_native(
        project_root: &Path,
        mode: crate::storage::bootstrap::native::NativeBootstrapMode,
        requested_repo_id: Option<RepoId>,
    ) -> Result<Self> {
        #[cfg(target_os = "macos")]
        {
            let host =
                NativeProjectRuntimeHost::open(project_root, mode, requested_repo_id.as_ref())
                    .await?;
            Self::start(host).await
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (project_root, mode, requested_repo_id);
            Err(CowshedError::environment_missing(
                "the native cowshed project runtime requires macOS APFS",
                "run the controller on macOS or use an injected test host",
            ))
        }
    }

    /// Starts a runtime from an injected host after strict recovery completes.
    pub async fn start(mut host: impl ProjectRuntimeHost) -> Result<Self> {
        host.recover().await?;
        let descriptor = host.descriptor().clone();
        let capacity = NonZeroUsize::new(ROUTER_CAPACITY)
            .ok_or_else(|| CowshedError::internal("project router capacity is zero"))?;
        let (router, receiver) = RouterHandle::channel(capacity);
        let actor = tokio::spawn(ProjectActor::new(Box::new(host), receiver).run());
        Ok(Self {
            descriptor,
            router,
            actor,
        })
    }

    pub fn descriptor(&self) -> &ProjectDescriptor {
        &self.descriptor
    }

    pub fn router(&self) -> RouterHandle {
        self.router.clone()
    }

    pub async fn shutdown(self) -> Result<()> {
        drop(self.router);
        self.actor
            .await
            .map_err(|error| CowshedError::internal(format!("project actor failed: {error}")))
    }
}

struct ProjectActor {
    host: Box<dyn ProjectRuntimeHost>,
    receiver: mpsc::Receiver<RouterCommand>,
}

impl ProjectActor {
    fn new(host: Box<dyn ProjectRuntimeHost>, receiver: mpsc::Receiver<RouterCommand>) -> Self {
        Self { host, receiver }
    }

    async fn run(mut self) {
        while let Some(command) = self.receiver.recv().await {
            let (request, reply) = command.into_parts();
            let response = self.route(request).await;
            let _ = reply.send(response);
        }
    }

    async fn route(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        self.validate_connection_authority(request.authority())?;
        match request.method() {
            "project.open" => self.project_open(request).await,
            "project.workspace" => self.project_workspace(request).await,
            "project.workspaceAt" => self.project_workspace_at(request).await,
            "project.list" => self.project_list(request).await,
            "workspace.info" => self.workspace_info(request).await,
            "workspace.ensure" => self.workspace_ensure(request).await,
            "workspace.attach" => self.workspace_attach(request).await,
            "workspace.grants" => self.workspace_grants(request).await,
            "coordinator.adopt" => self.coordinator_adopt(request).await,
            "coordinator.create" => self.coordinator_create(request).await,
            "coordinator.fork" => self.coordinator_fork(request).await,
            "coordinator.grant" => self.coordinator_grant(request, false).await,
            "coordinator.revoke" => self.coordinator_grant(request, true).await,
            "coordinator.rebase" => self.coordinator_rebase(request).await,
            "coordinator.land" => self.coordinator_land(request).await,
            "coordinator.restore" => self.coordinator_restore(request).await,
            "coordinator.detach" => self.coordinator_detach(request).await,
            "coordinator.assignSlot" => self.coordinator_assign_slot(request).await,
            "coordinator.destroy" => self.coordinator_destroy(request).await,
            "coordinator.gc" => self.coordinator_gc(request).await,
            "coordinator.repoMirror" => self.coordinator_repo_mirror(request).await,
            "coordinator.setCheckpointQuota" => self.coordinator_checkpoint_quota(request).await,
            "coordinator.doctor" => self.coordinator_doctor(request).await,
            "coordinator.worker" => self.coordinator_worker(request).await,
            "worker.exec" => self.worker_exec(request).await,
            "worker.stdinChunk" | "job.attachWrite" => self.worker_stdin_chunk(request).await,
            "worker.stdinClose" => self.worker_stdin_close(request).await,
            "worker.shell" => self.worker_shell(request).await,
            "worker.listJobs" => self.worker_list_jobs(request).await,
            "worker.job" | "job.status" => self.worker_job_info(request).await,
            "worker.checkpoint" => self.worker_checkpoint(request).await,
            "worker.push" => self.worker_push(request).await,
            "job.logs" => self.job_logs(request).await,
            "job.detach" => self.job_detach(request).await,
            "job.wait" => self.job_wait(request).await,
            "job.kill" => self.job_kill(request).await,
            "session.close" => self.session_close(request).await,
            method => Err(CowshedError::usage(
                format!("unknown controller method {method}"),
                "upgrade the client and controller together",
            )),
        }
    }

    fn validate_connection_authority(&self, authority: &ConnectionAuthority) -> Result<()> {
        if authority.repo_id() != &self.host.descriptor().repo_id {
            return Err(CowshedError::conflict(
                "connection repository authority does not match this project runtime",
                "reopen the project through its bound controller descriptor",
            ));
        }
        Ok(())
    }

    async fn project_open(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: ProjectOpenParams = decode_params(request.params(), request.method())?;
        let requested = canonical_input_path(&params.path)?;
        if requested != self.host.descriptor().git_root {
            return Err(CowshedError::conflict(
                format!(
                    "project path {} does not match bound git root {}",
                    requested.display(),
                    self.host.descriptor().git_root.display()
                ),
                "reopen the controller for the requested repository",
            ));
        }
        let descriptor = self.host.descriptor();
        json_response(json!({
            "repoId": descriptor.repo_id,
            "binding": descriptor.binding,
            "gitRoot": descriptor.git_root,
            "storeRoot": descriptor.store_root,
        }))
    }

    async fn project_workspace(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshots = self.host.snapshots().await?;
        let snapshot = find_workspace(&snapshots, &params.workspace)?;
        self.validate_worker_snapshot(request.authority(), snapshot)?;
        workspace_response(snapshot)
    }

    async fn project_workspace_at(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceAtParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let path = canonical_input_path(&params.path)?;
        let snapshot = self.host.workspace_at(path).await?;
        self.validate_worker_snapshot(request.authority(), &snapshot)?;
        workspace_response(&snapshot)
    }

    async fn project_list(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: RepoParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshots = self.host.snapshots().await?;
        let wires: Vec<_> = snapshots
            .iter()
            .map(workspace_wire)
            .collect::<Result<Vec<_>>>()?;
        json_response(wires)
    }

    async fn workspace_info(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshots = self.host.snapshots().await?;
        let snapshot = find_workspace(&snapshots, &params.workspace)?;
        self.validate_worker_snapshot(request.authority(), snapshot)?;
        json_response(&snapshot.info)
    }

    async fn workspace_ensure(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params)
            .await?;
        let result = self.host.ensure(params.workspace).await?;
        json_response(result)
    }

    async fn workspace_attach(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceOptionsParams<AttachOptions> =
            decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        self.host.attach(params.workspace, params.options).await?;
        json_response(EmptyResult {})
    }

    async fn workspace_grants(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: WorkspaceParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshots = self.host.snapshots().await?;
        let snapshot = find_workspace(&snapshots, &params.workspace)?;
        self.validate_worker_snapshot(request.authority(), snapshot)?;
        json_response(&snapshot.grants)
    }

    async fn coordinator_adopt(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: OptionsParams<AdoptOptions> =
            decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        if params
            .options
            .repo_id
            .as_ref()
            .is_some_and(|repo_id| repo_id != &self.host.descriptor().repo_id)
        {
            return Err(CowshedError::conflict(
                "adopt repository identity differs from the provisional project binding",
                "retry with the repository identity selected while opening the project",
            ));
        }
        let snapshot = self.host.adopt(params.options).await?;
        workspace_response(&snapshot)
    }

    async fn coordinator_create(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceOptionsParams<CreateOptions> =
            decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshot = self.host.create(params.workspace, params.options).await?;
        workspace_response(&snapshot)
    }

    async fn coordinator_fork(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: ForkParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshot = self.host.fork(params.source, params.destination).await?;
        workspace_response(&snapshot)
    }

    async fn coordinator_grant(
        &mut self,
        request: RouterRequest,
        revoke: bool,
    ) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: GrantParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let grants = self
            .host
            .grant(params.workspace, params.delta, revoke)
            .await?;
        json_response(grants)
    }

    async fn coordinator_rebase(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceOptionsParams<RebaseOptions> =
            decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let oid = self.host.rebase(params.workspace, params.options).await?;
        json_response(RevisionResult { oid })
    }

    async fn coordinator_land(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceOptionsParams<LandOptions> =
            decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let report = self.host.land(params.workspace, params.options).await?;
        json_response(report)
    }

    async fn coordinator_restore(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: RestoreParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        self.host.restore(params.workspace, params.label).await?;
        json_response(EmptyResult {})
    }

    async fn coordinator_detach(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        self.host.detach(params.workspace).await?;
        json_response(EmptyResult {})
    }

    async fn coordinator_assign_slot(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: SlotParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        self.host.assign_slot(params.workspace, params.slot).await?;
        json_response(EmptyResult {})
    }

    async fn coordinator_destroy(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceOptionsParams<RemoveOptions> =
            decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        self.host.remove(params.workspace, params.options).await?;
        json_response(EmptyResult {})
    }

    async fn coordinator_gc(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: OptionsParams<GcOptions> = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        json_response(self.host.gc(params.options).await?)
    }

    async fn coordinator_repo_mirror(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: MirrorParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let url = Url::parse(&params.url).map_err(|error| {
            CowshedError::usage(
                format!("invalid repository mirror URL: {error}"),
                "use an absolute supported repository URL",
            )
        })?;
        json_response(self.host.repo_mirror(params.workspace, url).await?)
    }

    async fn coordinator_checkpoint_quota(
        &mut self,
        request: RouterRequest,
    ) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: QuotaParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        self.host
            .set_checkpoint_quota(params.workspace, params.quota)
            .await?;
        json_response(EmptyResult {})
    }

    async fn coordinator_doctor(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: RepoParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        json_response(self.host.doctor().await?)
    }

    async fn coordinator_worker(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: WorkspaceParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshot = self.host.open_worker(params.workspace).await?;
        workspace_response(&snapshot)
    }

    async fn worker_exec(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let (scope, session, exec) = decode_exec_request(&request)?;
        self.require_scoped_workspace(request.authority(), &scope.workspace_params())
            .await?;
        let id = self
            .host
            .exec(scope.workspace, scope.workspace_incarnation, session, exec)
            .await?;
        json_response(id)
    }

    async fn worker_stdin_chunk(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: JobParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        let bytes = request.upload().cloned().ok_or_else(|| {
            CowshedError::usage(
                "stdin chunk is missing binary data",
                "retry the stdin write",
            )
        })?;
        self.host
            .stdin_write(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
                bytes,
            )
            .await?;
        json_response(EmptyResult {})
    }

    async fn worker_stdin_close(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: JobParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        self.host
            .stdin_close(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
            )
            .await?;
        json_response(EmptyResult {})
    }

    async fn worker_shell(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: SessionParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        self.host
            .open_session(
                params.workspace,
                params.workspace_incarnation,
                params.session,
            )
            .await?;
        json_response(EmptyResult {})
    }

    async fn worker_list_jobs(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: WorkerScope = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        json_response(
            self.host
                .list_jobs(params.workspace, params.workspace_incarnation)
                .await?,
        )
    }

    async fn worker_job_info(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: JobParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        json_response(
            self.host
                .job_info(
                    params.workspace,
                    params.workspace_incarnation,
                    params.job_id,
                )
                .await?,
        )
    }

    async fn worker_checkpoint(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: WorkerCheckpointParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        json_response(
            self.host
                .checkpoint(
                    params.workspace,
                    Some(params.workspace_incarnation),
                    params.options,
                )
                .await?,
        )
    }

    async fn worker_push(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: WorkerPushParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        json_response(
            self.host
                .push(
                    params.workspace,
                    params.workspace_incarnation,
                    params.options,
                )
                .await?,
        )
    }

    async fn job_logs(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: LogsParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        let stream = match params.stream {
            JobStreamWire::Stdout => RuntimeJobStream::Stdout,
            JobStreamWire::Stderr => RuntimeJobStream::Stderr,
        };
        let chunk = self
            .host
            .read_log(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
                stream,
                params.offset,
                params.follow,
            )
            .await?;
        if chunk.bytes.len() > MAX_LOG_CHUNK_BYTES {
            return Err(CowshedError::internal(
                "supervisor returned a log chunk larger than the transport frame",
            ));
        }
        RouterResponse::binary(
            json!({ "eof": chunk.eof, "nextOffset": chunk.next_offset }),
            chunk.bytes,
        )
    }

    async fn job_detach(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: JobParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        self.host
            .detach_job(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
            )
            .await?;
        json_response(EmptyResult {})
    }

    async fn job_wait(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: JobParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        json_response(
            self.host
                .wait_job(
                    params.workspace,
                    params.workspace_incarnation,
                    params.job_id,
                )
                .await?,
        )
    }

    async fn job_kill(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: JobParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        self.host
            .kill_job(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
            )
            .await?;
        json_response(EmptyResult {})
    }

    async fn session_close(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        let params: SessionParams = decode_params(request.params(), request.method())?;
        self.require_scoped_workspace(request.authority(), &params.workspace_params())
            .await?;
        self.host
            .close_session(
                params.workspace,
                params.workspace_incarnation,
                params.session,
            )
            .await?;
        json_response(EmptyResult {})
    }

    fn require_repo(&self, repo_id: &RepoId) -> Result<()> {
        if repo_id != &self.host.descriptor().repo_id {
            return Err(CowshedError::conflict(
                "request repository does not match the project binding",
                "reopen the project and retry with its bound repository identity",
            ));
        }
        Ok(())
    }

    async fn require_scoped_workspace(
        &mut self,
        authority: &ConnectionAuthority,
        params: &WorkspaceParams,
    ) -> Result<()> {
        self.require_repo(&params.repo_id)?;
        let snapshots = self.host.snapshots().await?;
        let snapshot = find_workspace(&snapshots, &params.workspace)?;
        self.validate_worker_snapshot(authority, snapshot)
    }

    fn validate_worker_snapshot(
        &self,
        authority: &ConnectionAuthority,
        snapshot: &WorkspaceSnapshot,
    ) -> Result<()> {
        if let ConnectionAuthority::Worker {
            workspace,
            workspace_incarnation,
            ..
        } = authority
            && (workspace != &snapshot.info.workspace
                || workspace_incarnation != &snapshot.info.workspace_incarnation)
        {
            return Err(CowshedError::conflict(
                "workspace capability is stale or belongs to another workspace incarnation",
                "reacquire a worker handle from the coordinator",
            ));
        }
        Ok(())
    }
}

fn require_coordinator(authority: &ConnectionAuthority) -> Result<()> {
    if matches!(authority, ConnectionAuthority::Coordinator { .. }) {
        Ok(())
    } else {
        Err(CowshedError::new(
            ErrorCode::SandboxDenied,
            "workspace capability cannot perform coordinator operation",
            "request coordinator authority from the controller owner",
        ))
    }
}

fn find_workspace<'a>(
    snapshots: &'a [WorkspaceSnapshot],
    workspace: &WorkspaceName,
) -> Result<&'a WorkspaceSnapshot> {
    snapshots
        .iter()
        .find(|snapshot| &snapshot.info.workspace == workspace)
        .ok_or_else(|| {
            CowshedError::not_found(
                format!("workspace {workspace} does not exist"),
                "list workspaces and retry with a published name",
            )
        })
}

fn workspace_wire(snapshot: &WorkspaceSnapshot) -> Result<Value> {
    serde_json::to_value(json!({ "info": snapshot.info, "grants": snapshot.grants }))
        .map_err(|error| CowshedError::internal(format!("serialize workspace snapshot: {error}")))
}

fn workspace_response(snapshot: &WorkspaceSnapshot) -> Result<RouterResponse> {
    json_response(workspace_wire(snapshot)?)
}

fn json_response(value: impl Serialize) -> Result<RouterResponse> {
    serde_json::to_value(value)
        .map(RouterResponse::json)
        .map_err(|error| CowshedError::internal(format!("serialize router response: {error}")))
}

fn decode_params<T: DeserializeOwned>(params: &Value, method: &str) -> Result<T> {
    serde_json::from_value(params.clone()).map_err(|error| {
        CowshedError::usage(
            format!("invalid {method} parameters: {error}"),
            "upgrade the client and controller together",
        )
    })
}

fn canonical_input_path(path: &str) -> Result<PathBuf> {
    let path = PathBuf::from(path);
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(CowshedError::usage(
            "project path must be absolute and lexically normalized",
            "retry with the discovered git repository root",
        ));
    }
    Ok(path)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProjectOpenParams {
    path: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RepoParams {
    repo_id: RepoId,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkspaceParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkspaceAtParams {
    repo_id: RepoId,
    path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OptionsParams<T> {
    repo_id: RepoId,
    options: T,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkspaceOptionsParams<T> {
    repo_id: RepoId,
    workspace: WorkspaceName,
    options: T,
}

impl<T> WorkspaceOptionsParams<T> {
    fn workspace_params(&self) -> WorkspaceParams {
        WorkspaceParams {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForkParams {
    repo_id: RepoId,
    source: WorkspaceName,
    destination: WorkspaceName,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RestoreParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    label: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    delta: GrantDelta,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SlotParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    slot: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QuotaParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    quota: CheckpointQuota,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MirrorParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    url: String,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkerScope {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
}

impl WorkerScope {
    fn workspace_params(&self) -> WorkspaceParams {
        WorkspaceParams {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    session: Option<String>,
}

impl SessionParams {
    fn workspace_params(&self) -> WorkspaceParams {
        WorkspaceParams {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JobParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
}

impl JobParams {
    fn workspace_params(&self) -> WorkspaceParams {
        WorkspaceParams {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkerCheckpointParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    options: CheckpointOptions,
}

impl WorkerCheckpointParams {
    fn workspace_params(&self) -> WorkspaceParams {
        WorkspaceParams {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkerPushParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    options: PushOptions,
}

impl WorkerPushParams {
    fn workspace_params(&self) -> WorkspaceParams {
        WorkspaceParams {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum JobStreamWire {
    Stdout,
    Stderr,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LogsParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    job_id: JobId,
    stream: JobStreamWire,
    follow: bool,
    offset: u64,
}

impl LogsParams {
    fn workspace_params(&self) -> WorkspaceParams {
        WorkspaceParams {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecWire {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    session: Option<String>,
    argv: Vec<CommandArg>,
    cwd: Option<crate::api::dto::WorkspacePath>,
    mode: ExecModeWire,
    env: std::collections::HashMap<String, String>,
    trace: Option<crate::api::dto::TraceContext>,
    stdin: StdinWire,
    stdout_copy: Option<crate::api::dto::OutputPublication>,
    stderr_copy: Option<crate::api::dto::OutputPublication>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
enum ExecModeWire {
    ReadWrite,
    ReadOnly,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum StdinWire {
    Empty,
    Inline,
    Stream,
    WorkspaceFile {
        workspace_path: crate::api::dto::WorkspacePath,
    },
}

fn decode_exec_request(
    request: &RouterRequest,
) -> Result<(WorkerScope, Option<String>, ExecRequest)> {
    let wire: ExecWire = decode_params(request.params(), request.method())?;
    validate_command_argv(&wire.argv).map_err(|error| {
        CowshedError::usage(error.to_string(), "provide a valid bounded command argv")
    })?;
    let stdin = match wire.stdin {
        StdinWire::Empty => {
            if request.upload().is_some() {
                return Err(CowshedError::usage(
                    "empty stdin request unexpectedly included binary data",
                    "retry without an upload frame",
                ));
            }
            StdinSource::Empty
        }
        StdinWire::Inline => StdinSource::Inline(request.upload().cloned().ok_or_else(|| {
            CowshedError::usage(
                "inline stdin request is missing binary data",
                "retry with the declared upload frame",
            )
        })?),
        StdinWire::Stream => {
            if request.upload().is_some() {
                return Err(CowshedError::usage(
                    "stream stdin admission unexpectedly included binary data",
                    "send stream chunks after job admission",
                ));
            }
            return Err(CowshedError::usage(
                "stream stdin requires the controller streaming channel",
                "retry through WorkspaceHandle::exec",
            ));
        }
        StdinWire::WorkspaceFile { workspace_path } => {
            if request.upload().is_some() {
                return Err(CowshedError::usage(
                    "workspace-file stdin unexpectedly included binary data",
                    "retry without an upload frame",
                ));
            }
            StdinSource::WorkspaceFile(workspace_path)
        }
    };
    let mode = match wire.mode {
        ExecModeWire::ReadWrite => RunSandboxMode::ReadWrite,
        ExecModeWire::ReadOnly => RunSandboxMode::ReadOnly,
    };
    let scope = WorkerScope {
        repo_id: wire.repo_id,
        workspace: wire.workspace,
        workspace_incarnation: wire.workspace_incarnation,
    };
    Ok((
        scope,
        wire.session,
        ExecRequest {
            argv: wire.argv,
            cwd: wire.cwd,
            mode,
            env: wire.env,
            trace: wire.trace,
            stdin,
            stdout_copy: wire.stdout_copy,
            stderr_copy: wire.stderr_copy,
        },
    ))
}

#[cfg(target_os = "macos")]
type NativeSubstrate = crate::storage::apfs::ApfsSubstrate<
    crate::storage::apfs::native::MacOsApfsExecutionHost<crate::apfs::SystemCommandRunner>,
>;

#[cfg(target_os = "macos")]
struct NativeProjectRuntimeHost {
    descriptor: ProjectDescriptor,
    git: crate::git::GitRepository,
    layout: crate::storage::StorageLayout,
    substrate: NativeSubstrate,
    commitments: super::supervisor::CommitmentPublisherHandle,
    supervisors:
        std::collections::BTreeMap<WorkspaceName, super::supervisor::WorkspaceSupervisorHandle>,
    sessions: std::collections::BTreeMap<
        (WorkspaceName, Option<String>),
        super::supervisor::SessionToken,
    >,
    home: PathBuf,
    telemetry_root: PathBuf,
}

#[cfg(target_os = "macos")]
impl NativeProjectRuntimeHost {
    async fn open(
        project_root: &Path,
        bootstrap_mode: crate::storage::bootstrap::native::NativeBootstrapMode,
        requested_repo_id: Option<&RepoId>,
    ) -> Result<Self> {
        use crate::storage::apfs::ApfsExecutionHost;
        use crate::storage::lifecycle::Substrate;

        let git = crate::git::GitRepository::discover(project_root).await?;
        git.ensure_adoptable().await?;
        let git_root = git.root().to_path_buf();
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .ok_or_else(|| {
                CowshedError::environment_missing(
                    "HOME is missing or is not absolute",
                    "launch the controller with a canonical HOME",
                )
            })?;
        let binding_repo_id = if matches!(
            &bootstrap_mode,
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly
        ) {
            repo_id_from_workspace_marker(&git_root).await?
        } else {
            requested_repo_id.cloned()
        };
        let candidate = binding_from_git(&git, binding_repo_id.as_ref()).await?;
        let repo_id = candidate
            .primary()
            .map_err(native_integrity_error)?
            .repo_id
            .clone();
        let bootstrap = crate::storage::bootstrap::native::bootstrap_system_storage(
            &git_root,
            &home,
            bootstrap_mode,
        )
        .await
        .map_err(native_environment_error)?;
        if !matches!(
            bootstrap.substrate(),
            crate::storage::bootstrap::SelectedSubstrate::Apfs { .. }
        ) {
            return Err(CowshedError::environment_missing(
                "the macOS runtime requires the APFS image substrate",
                "remove the unsupported substrate override and retry",
            ));
        }
        let layout = crate::storage::StorageLayout::new(bootstrap.roots().store(), &repo_id)
            .map_err(native_integrity_error)?;
        let binding = load_or_validate_binding(&layout, candidate, &git).await?;
        let config = crate::storage::apfs::ApfsSubstrateConfig::new(
            bootstrap.roots().store(),
            bootstrap.roots().caches(),
            &git_root,
            crate::apfs::ApfsCaseSensitivity::Sensitive,
        );
        let host = crate::storage::apfs::native::MacOsApfsExecutionHost::new(
            crate::apfs::SystemCommandRunner,
            config.clone(),
        )
        .map_err(native_storage_error)?;
        let recovery_config = config.clone();
        let recovery_repo = repo_id.clone();
        let (host, facts, pending) = crate::storage::lifecycle::dispatch_blocking(move || {
            host.recover_pending(&recovery_config, &[])?;
            let facts = host.list(&recovery_repo)?;
            let pending = host.pending_publications(&recovery_repo)?;
            Ok::<_, crate::storage::apfs::ApfsStorageError>((host, facts, pending))
        })
        .await
        .map_err(|error| CowshedError::internal(format!("APFS recovery task failed: {error}")))?
        .map_err(native_storage_error)?;
        let retired_project_root = layout.project().project_root.clone();
        let retired_repo = repo_id.clone();
        let retired = crate::storage::lifecycle::dispatch_blocking(move || {
            native_retired_refs(&retired_project_root, &retired_repo)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("retired workspace recovery task failed: {error}"))
        })??;
        let known_incarnations = facts
            .iter()
            .map(|fact| fact.workspace.incarnation().clone())
            .chain(
                pending
                    .iter()
                    .map(|fact| fact.workspace.incarnation().clone()),
            )
            .chain(
                retired
                    .iter()
                    .map(|fact| fact.workspace().incarnation().clone()),
            )
            .collect::<Vec<_>>();
        let telemetry_root = bootstrap.roots().store().join("telemetry");
        let mut commitments = super::supervisor::CommitmentPublisher::open(
            &telemetry_root,
            repo_id.clone(),
            known_incarnations,
            ROUTER_CAPACITY,
        )?;
        for retirement in &retired {
            commitments
                .ensure_workspace_retired(
                    repo_id.clone(),
                    retirement.workspace().incarnation().clone(),
                )
                .await?;
        }
        for fact in &facts {
            commitments
                .ensure_workspace_introduced(repo_id.clone(), fact.workspace.incarnation().clone())
                .await?;
        }
        for publication in &pending {
            use super::supervisor::{CommitmentDraft, CommitmentSink};

            commitments
                .publish(CommitmentDraft::Restore {
                    repo_id: repo_id.clone(),
                    source_checkpoint: publication.source_checkpoint.clone(),
                    source_incarnation: publication.source_incarnation.clone(),
                    destination_incarnation: publication.workspace.incarnation().clone(),
                })
                .await?;
            host.activate_restored_metadata(&publication.image)
                .map_err(native_storage_error)?;
        }
        let substrate = crate::storage::apfs::ApfsSubstrate::new(config, host);
        for retirement in retired {
            // The retirement commitment is durable before any best-effort trash reclamation.
            let _ = substrate.reclaim(retirement).await;
        }
        let descriptor = ProjectDescriptor {
            repo_id,
            binding,
            git_root,
            store_root: bootstrap.roots().store().to_path_buf(),
        };
        Ok(Self {
            descriptor,
            git,
            layout,
            substrate,
            commitments,
            supervisors: std::collections::BTreeMap::new(),
            sessions: std::collections::BTreeMap::new(),
            home,
            telemetry_root,
        })
    }

    async fn validate_binding(&self) -> Result<()> {
        let remotes = self.git.remotes().await?;
        validate_binding_against_remotes(&self.descriptor.binding, &remotes)
    }

    async fn authoritative(&self) -> Result<Vec<NativeWorkspace>> {
        use crate::storage::lifecycle::Substrate;

        let derived = self
            .substrate
            .list(&self.descriptor.repo_id)
            .await
            .map_err(native_storage_error)?;
        let layout = self.layout.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            derived
                .into_iter()
                .map(|derived| {
                    let image = if derived.workspace.name().is_main() {
                        layout
                            .main_image(derived.workspace.format())?
                            .image()
                            .to_path_buf()
                    } else {
                        layout
                            .session_image(derived.workspace.name(), derived.workspace.format())?
                            .image()
                            .to_path_buf()
                    };
                    let metadata =
                        crate::metadata::DetachedWorkspaceMetadata::read_for_image(&image)
                            .map_err(|error| {
                                crate::storage::apfs::ApfsStorageError::Host(error.to_string())
                            })?;
                    if metadata.repo_id != *derived.workspace.repo()
                        || metadata.workspace != *derived.workspace.name()
                        || metadata.workspace_incarnation != *derived.workspace.incarnation()
                    {
                        return Err(crate::storage::apfs::ApfsStorageError::MarkerMismatch(
                            format!("detached metadata disagrees with {}", image.display()),
                        ));
                    }
                    Ok(NativeWorkspace {
                        derived,
                        metadata,
                        image,
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .await
        .map_err(|error| CowshedError::internal(format!("metadata read task failed: {error}")))?
        .map_err(native_storage_error)
    }

    async fn current(&self, name: &WorkspaceName) -> Result<NativeWorkspace> {
        self.authoritative()
            .await?
            .into_iter()
            .find(|workspace| workspace.derived.workspace.name() == name)
            .ok_or_else(|| {
                CowshedError::not_found(
                    format!("workspace {name} does not exist"),
                    "list published workspaces and retry",
                )
            })
    }

    async fn pending_metadata(
        &self,
    ) -> Result<Vec<(PathBuf, crate::metadata::DetachedWorkspaceMetadata)>> {
        let main_images = [
            self.layout
                .main_image(crate::metadata::ImageFormat::Asif)
                .map_err(native_integrity_error)?
                .image()
                .to_path_buf(),
            self.layout
                .main_image(crate::metadata::ImageFormat::Sparse)
                .map_err(native_integrity_error)?
                .image()
                .to_path_buf(),
        ];
        let sessions = self.layout.project().sessions.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let mut images = main_images
                .into_iter()
                .filter(|image| image.exists())
                .collect::<Vec<_>>();
            let entries = match std::fs::read_dir(&sessions) {
                Ok(entries) => entries
                    .map(|entry| {
                        entry.map(|entry| entry.path()).map_err(|error| {
                            CowshedError::environment_missing(
                                format!(
                                    "cannot enumerate session metadata in {}: {error}",
                                    sessions.display()
                                ),
                                "check controller storage permissions",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "cannot enumerate session metadata in {}: {error}",
                            sessions.display()
                        ),
                        "check controller storage permissions",
                    ));
                }
            };
            images.extend(
                crate::storage::discover_session_images(entries)
                    .map_err(native_integrity_error)?
                    .into_iter()
                    .map(|image| image.path().to_path_buf()),
            );
            let mut pending = Vec::new();
            for image in images {
                let metadata = crate::metadata::DetachedWorkspaceMetadata::read_for_image(&image)
                    .map_err(native_integrity_error)?;
                if metadata.publication_state == crate::metadata::PublicationState::PendingFence {
                    pending.push((image, metadata));
                }
            }
            Ok(pending)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("pending metadata task failed: {error}")))?
    }

    fn snapshot(&self, workspace: &NativeWorkspace) -> Result<WorkspaceSnapshot> {
        let info_snapshot = workspace.metadata.info_snapshot.as_ref();
        let base_commit = info_snapshot
            .map(|info| GitOid::new(info.base_commit.clone()))
            .transpose()
            .map_err(native_integrity_error)?;
        let created_at = info_snapshot
            .map(|info| crate::api::dto::UtcTimestamp::new(info.created_at.clone()))
            .transpose()
            .map_err(native_integrity_error)?;
        Ok(WorkspaceSnapshot {
            info: WorkspaceInfo {
                repo_id: self.descriptor.repo_id.clone(),
                workspace: workspace.derived.workspace.name().clone(),
                workspace_incarnation: workspace.derived.workspace.incarnation().clone(),
                role: workspace.derived.workspace.role(),
                image_format: workspace.derived.workspace.format(),
                mount: self
                    .layout
                    .workspace_mount(workspace.derived.workspace.name())
                    .map_err(native_integrity_error)?,
                state: match workspace.derived.mount_state {
                    crate::storage::lifecycle::MountState::Detached => {
                        crate::api::dto::WorkspaceState::Detached
                    }
                    crate::storage::lifecycle::MountState::Mounted { .. } => {
                        crate::api::dto::WorkspaceState::Attached
                    }
                },
                branch: info_snapshot.and_then(|info| info.branch.clone()),
                base_commit,
                created_at,
                checkpoints: workspace
                    .derived
                    .checkpoints
                    .iter()
                    .map(|checkpoint| crate::api::dto::CheckpointInfo {
                        label: checkpoint.label.to_string(),
                        revision: checkpoint.revision.get(),
                        pinned: matches!(checkpoint.pin, crate::storage::lifecycle::Pin::Pinned),
                    })
                    .collect(),
                snapshot_stale: info_snapshot.is_some_and(|info| info.stale),
            },
            grants: workspace.metadata.grants.clone(),
            lifecycle_revision: workspace.derived.workspace.revision().get(),
            topology_revision: workspace.derived.workspace.topology_revision().get(),
        })
    }

    fn ensure_report(
        &self,
        workspace: &NativeWorkspace,
        mount: PathBuf,
        action: crate::api::dto::EnsureAction,
    ) -> crate::api::dto::EnsureReport {
        crate::api::dto::EnsureReport {
            workspace: workspace.derived.workspace.name().clone(),
            go_env: mount.join(".cowshed/cache/go/env"),
            workspace_token: mount.join(crate::workspace_credentials::WORKSPACE_TOKEN_PATH),
            port_block: workspace.metadata.grants.port_block,
            mount,
            action,
        }
    }

    async fn checkpoint_quota(&self, workspace: &WorkspaceName) -> Result<Option<CheckpointQuota>> {
        let path = self.layout.project().policy.clone();
        let workspace = workspace.to_string();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let policy: std::collections::BTreeMap<String, CheckpointQuota> =
                match crate::metadata::read_json(&path) {
                    Ok(policy) => policy,
                    Err(crate::metadata::MetadataError::Io { source, .. })
                        if source.kind() == std::io::ErrorKind::NotFound =>
                    {
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                };
            Ok(policy.get(&workspace).copied())
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkpoint quota task failed: {error}")))?
        .map_err(native_integrity_error)
    }

    async fn enforce_checkpoint_quota(&self, workspace: &NativeWorkspace) -> Result<()> {
        use crate::storage::lifecycle::Substrate;

        let Some(quota) = self
            .checkpoint_quota(workspace.derived.workspace.name())
            .await?
        else {
            return Ok(());
        };
        let stats = self
            .substrate
            .stats(&workspace.derived.workspace)
            .await
            .map_err(native_storage_error)?;
        if stats.pinned_checkpoint_bytes > stats.checkpoint_bytes {
            return Err(CowshedError::integrity(
                "pinned checkpoint bytes exceed total checkpoint bytes",
                "run cowshed doctor --json",
            ));
        }
        let projected_count = stats.checkpoint_count.checked_add(1).ok_or_else(|| {
            CowshedError::integrity("checkpoint count overflow", "run cowshed gc")
        })?;
        let projected_bytes = stats
            .checkpoint_bytes
            .checked_add(stats.allocated_bytes)
            .ok_or_else(|| {
                CowshedError::integrity("checkpoint byte accounting overflow", "run cowshed gc")
            })?;
        if projected_count > u64::from(quota.max_count) || projected_bytes > quota.max_bytes {
            return Err(CowshedError::conflict(
                format!(
                    "checkpoint quota exceeded for {}: projected {projected_count} checkpoints and {projected_bytes} bytes, limit {} checkpoints and {} bytes",
                    workspace.derived.workspace.name(),
                    quota.max_count,
                    quota.max_bytes
                ),
                "remove or unpin checkpoints, raise the workspace quota, or run cowshed gc",
            ));
        }
        Ok(())
    }

    async fn operation_identity(
        &self,
        grants: GrantSet,
    ) -> Result<crate::storage::lifecycle::OperationIdentity> {
        Ok(crate::storage::lifecycle::OperationIdentity {
            project_root: self.descriptor.git_root.clone(),
            base_commit: self.git.head_oid().await?,
            created_at: utc_timestamp().await?,
            created_trace: uuid::Uuid::new_v4().simple().to_string(),
            grants,
        })
    }

    async fn fresh_grants(&self) -> Result<GrantSet> {
        let used = self
            .authoritative()
            .await?
            .into_iter()
            .filter_map(|workspace| {
                workspace
                    .metadata
                    .grants
                    .port_block
                    .map(|block| block.base())
            })
            .collect::<std::collections::BTreeSet<_>>();
        for base in (49_152_u16..=65_520_u16).step_by(usize::from(crate::metadata::PORT_BLOCK_SIZE))
        {
            if !used.contains(&base) {
                return GrantSet::closed_baseline(Some(
                    crate::metadata::PortBlock::new(base, crate::metadata::PORT_BLOCK_SIZE)
                        .map_err(native_integrity_error)?,
                ))
                .map_err(native_integrity_error);
            }
        }
        Err(CowshedError::conflict(
            "no macOS workspace port block remains",
            "remove an unused workspace or reassign its slot",
        ))
    }

    async fn snapshot_named(&self, name: &WorkspaceName) -> Result<WorkspaceSnapshot> {
        let current = self.current(name).await?;
        self.snapshot(&current)
    }

    fn require_exact_incarnation(
        workspace: &NativeWorkspace,
        expected: &WorkspaceIncarnation,
    ) -> Result<()> {
        if workspace.derived.workspace.incarnation() != expected {
            return Err(CowshedError::conflict(
                "workspace incarnation is stale",
                "reacquire the worker handle and retry",
            ));
        }
        Ok(())
    }

    async fn ensure_supervisor(
        &mut self,
        name: &WorkspaceName,
    ) -> Result<super::supervisor::WorkspaceSupervisorHandle> {
        use crate::storage::lifecycle::{MountIntent, Substrate};

        self.validate_binding().await?;
        let current = self.current(name).await?;
        let mount = self
            .substrate
            .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
            .await
            .map_err(native_storage_error)?;
        if let Some(handle) = self.supervisors.get(name)
            && handle.snapshot().workspace_incarnation == *current.derived.workspace.incarnation()
            && handle.snapshot().grant_revision == current.metadata.grants.revision
        {
            return Ok(handle.clone());
        }
        if let Some(old) = self.supervisors.remove(name) {
            old.quiesce().await?;
            old.retire().await?;
            self.sessions.retain(|(workspace, _), _| workspace != name);
        }
        let port_block = current.metadata.grants.port_block.ok_or_else(|| {
            CowshedError::integrity(
                "macOS workspace metadata has no port block",
                "cowshed doctor --json",
            )
        })?;
        let sandbox = crate::sandbox::SandboxConfig {
            home: self.home.clone(),
            workspace_mount: mount.clone(),
            exec_temp_dir: self
                .layout
                .project()
                .quarantine
                .join(current.derived.workspace.incarnation().as_str()),
            port_block,
            mode: crate::sandbox::RunSandboxMode::ReadWrite,
            grants: crate::sandbox::SandboxGrants {
                read: current.metadata.grants.read.clone(),
                write: current.metadata.grants.write.clone(),
                egress: current
                    .metadata
                    .grants
                    .egress
                    .iter()
                    .map(|rule| crate::sandbox::EgressGrant {
                        host: rule.host.clone(),
                        ports: rule.ports.clone(),
                    })
                    .collect(),
            },
            allowed_unix_sockets: Vec::new(),
            additional_denies: vec![
                self.layout.project().project_root.clone(),
                self.telemetry_root.clone(),
            ],
        };
        let admitted_historical_incarnations = self
            .commitments
            .admitted_lifecycle_incarnations(self.descriptor.repo_id.clone())
            .await?;
        let config = super::supervisor::WorkspaceSupervisorConfig {
            authority: super::supervisor::WorkspaceAuthoritySnapshot {
                repo_id: self.descriptor.repo_id.clone(),
                workspace: name.clone(),
                workspace_incarnation: current.derived.workspace.incarnation().clone(),
                grant_revision: current.metadata.grants.revision,
                lifecycle_revision: current.derived.workspace.revision().get(),
            },
            workspace_root: mount,
            default_cwd: None,
            sandbox,
            artifacts: crate::storage::job_artifact::ArtifactConfig {
                admitted_historical_incarnations,
                ..crate::storage::job_artifact::ArtifactConfig::default()
            },
            term_grace: std::time::Duration::from_secs(2),
            actor_capacity: ROUTER_CAPACITY,
            event_capacity: ROUTER_CAPACITY,
        };
        let handle =
            super::supervisor::WorkspaceSupervisor::start(config, self.commitments.clone())?;
        self.supervisors.insert(name.clone(), handle.clone());
        Ok(handle)
    }

    async fn stop_supervisor(&mut self, name: &WorkspaceName) -> Result<()> {
        if let Some(handle) = self.supervisors.remove(name) {
            handle.quiesce().await?;
            handle.retire().await?;
        }
        self.sessions.retain(|(workspace, _), _| workspace != name);
        Ok(())
    }

    fn session(
        &self,
        workspace: &WorkspaceName,
        name: &Option<String>,
    ) -> Option<&super::supervisor::SessionToken> {
        self.sessions.get(&(workspace.clone(), name.clone()))
    }
}

#[cfg(target_os = "macos")]
struct NativeWorkspace {
    derived: crate::storage::lifecycle::DerivedWorkspace,
    metadata: crate::metadata::DetachedWorkspaceMetadata,
    image: PathBuf,
}

#[cfg(target_os = "macos")]
#[async_trait]
impl ProjectRuntimeHost for NativeProjectRuntimeHost {
    fn descriptor(&self) -> &ProjectDescriptor {
        &self.descriptor
    }

    async fn recover(&mut self) -> Result<()> {
        self.validate_binding().await?;
        let attached = self
            .authoritative()
            .await?
            .into_iter()
            .filter(|workspace| {
                matches!(
                    workspace.derived.mount_state,
                    crate::storage::lifecycle::MountState::Mounted { .. }
                )
            })
            .map(|workspace| workspace.derived.workspace.name().clone())
            .collect::<Vec<_>>();
        for workspace in attached {
            self.ensure_supervisor(&workspace).await?;
        }
        Ok(())
    }

    async fn snapshots(&mut self) -> Result<Vec<WorkspaceSnapshot>> {
        self.validate_binding().await?;
        self.authoritative()
            .await?
            .iter()
            .map(|workspace| self.snapshot(workspace))
            .collect()
    }

    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceSnapshot> {
        use crate::storage::lifecycle::LifecyclePlanner;

        self.validate_binding().await?;
        if !self.authoritative().await?.is_empty() {
            return Err(CowshedError::conflict(
                "repository is already adopted",
                "list the existing main workspace",
            ));
        }
        if options
            .repo_id
            .as_ref()
            .is_some_and(|repo| repo != &self.descriptor.repo_id)
        {
            return Err(CowshedError::conflict(
                "adopt repository identity differs from the bound remote",
                "retry with the bound repository identity",
            ));
        }
        enforce_adopt_secret_policy(
            self.descriptor.git_root.clone(),
            self.layout.project().waivers.clone(),
            self.layout.project().quarantine.clone(),
            options.quarantine,
        )
        .await?;

        let mut grants = self.fresh_grants().await?;
        grants.revision = 0;
        let identity = self.operation_identity(grants).await?;
        let format = options
            .image_format
            .unwrap_or(crate::metadata::ImageFormat::Asif);
        let pre_cowshed = pre_cowshed_path(&self.descriptor.git_root)?;
        let plan = self
            .substrate
            .plan_adopt(crate::storage::lifecycle::AdoptRequest {
                repo: self.descriptor.repo_id.clone(),
                format,
                topology_revision: crate::storage::lifecycle::Revision::new(0),
                source_checkout: self.descriptor.git_root.clone(),
                pre_cowshed_checkout: pre_cowshed,
                identity,
            })
            .map_err(native_integrity_error)?;
        let binding = self.descriptor.binding.clone();
        let binding_path = self.layout.project().repository_binding.clone();
        let receipt = self
            .substrate
            .execute_adopt_staged(plan, move |_stage| async move {
                crate::storage::lifecycle::dispatch_blocking(move || {
                    crate::metadata::write_json(&binding_path, &binding)
                })
                .await
                .map_err(|error| CowshedError::internal(error.to_string()))?
                .map_err(native_integrity_error)
            })
            .await
            .map_err(native_staged_error)?;
        self.commitments
            .ensure_workspace_introduced(
                self.descriptor.repo_id.clone(),
                receipt.workspace.incarnation().clone(),
            )
            .await?;
        let name = receipt.workspace.name().clone();
        self.ensure_supervisor(&name).await?;
        self.snapshot_named(&name).await
    }

    async fn create(
        &mut self,
        workspace: WorkspaceName,
        options: CreateOptions,
    ) -> Result<WorkspaceSnapshot> {
        use crate::storage::lifecycle::LifecyclePlanner;

        self.validate_binding().await?;
        if self
            .authoritative()
            .await?
            .iter()
            .any(|current| current.derived.workspace.name() == &workspace)
        {
            return Err(CowshedError::conflict(
                format!("workspace {workspace} already exists"),
                "choose another workspace name",
            ));
        }
        let source_name = options
            .from_workspace
            .clone()
            .unwrap_or_else(|| WorkspaceName::new("main").expect("fixed main"));
        let source = self.current(&source_name).await?;
        let identity = self.operation_identity(self.fresh_grants().await?).await?;
        let plan = self
            .substrate
            .plan_create(
                &source.derived.workspace,
                crate::storage::lifecycle::Destination {
                    repo: self.descriptor.repo_id.clone(),
                    name: workspace.clone(),
                    topology_revision: source.derived.workspace.topology_revision(),
                    identity,
                },
            )
            .map_err(native_integrity_error)?;
        let host_root = self.descriptor.git_root.clone();
        let start = options.revision.as_ref().map(revision_target);
        let destination = workspace.clone();
        let receipt = self
            .substrate
            .execute_create_staged(plan, move |stage| async move {
                crate::git::GitRepository::from_root(&stage.mount_point)
                    .prepare_workspace(&destination.to_string(), &host_root, start.as_deref())
                    .await
            })
            .await
            .map_err(native_staged_error)?;
        self.commitments
            .ensure_workspace_introduced(
                self.descriptor.repo_id.clone(),
                receipt.workspace.incarnation().clone(),
            )
            .await?;
        self.ensure_supervisor(&workspace).await?;
        self.snapshot_named(&workspace).await
    }

    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceSnapshot> {
        self.validate_binding().await?;
        let workspaces = self.authoritative().await?;
        let active_mounts = workspaces
            .iter()
            .enumerate()
            .filter(|(_, workspace)| {
                matches!(
                    workspace.derived.mount_state,
                    crate::storage::lifecycle::MountState::Mounted { .. }
                )
            })
            .map(|(index, workspace)| {
                self.layout
                    .workspace_mount(workspace.derived.workspace.name())
                    .map(|mount| (index, mount))
                    .map_err(native_integrity_error)
            })
            .collect::<Result<Vec<_>>>()?;
        let requested = path.clone();
        let matching = crate::storage::lifecycle::dispatch_blocking(move || {
            let requested = std::fs::canonicalize(&requested).map_err(|error| {
                CowshedError::not_found(
                    format!(
                        "workspace path {} is not accessible: {error}",
                        requested.display()
                    ),
                    "retry from inside an attached workspace",
                )
            })?;
            let mut matching = Vec::new();
            for (index, mount) in active_mounts {
                let mount = std::fs::canonicalize(&mount).map_err(|error| {
                    CowshedError::integrity(
                        format!(
                            "authoritatively mounted workspace path {} is not accessible: {error}",
                            mount.display()
                        ),
                        "run cowshed doctor --json",
                    )
                })?;
                if requested.starts_with(&mount) {
                    matching.push(index);
                }
            }
            Ok::<_, CowshedError>(matching)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("workspace path task failed: {error}"))
        })??;
        match matching.as_slice() {
            [index] => self.snapshot(&workspaces[*index]),
            [] => Err(CowshedError::not_found(
                format!(
                    "{} is not contained in an active workspace mount for project {}",
                    path.display(),
                    self.descriptor.repo_id
                ),
                "retry from inside an attached workspace",
            )),
            _ => Err(CowshedError::conflict(
                format!(
                    "{} is contained in multiple active workspace mounts",
                    path.display()
                ),
                "repair overlapping workspace mounts and retry",
            )),
        }
    }

    async fn fork(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot> {
        use super::supervisor::{CommitmentDraft, CommitmentSink};
        use crate::storage::lifecycle::LifecyclePlanner;

        self.validate_binding().await?;
        let source_fact = self.current(&source).await?;
        if self
            .authoritative()
            .await?
            .iter()
            .any(|current| current.derived.workspace.name() == &destination)
        {
            return Err(CowshedError::conflict(
                format!("workspace {destination} already exists"),
                "choose another workspace name",
            ));
        }
        let identity = self.operation_identity(self.fresh_grants().await?).await?;
        let plan = self
            .substrate
            .plan_fork(
                &source_fact.derived.workspace,
                crate::storage::lifecycle::Destination {
                    repo: self.descriptor.repo_id.clone(),
                    name: destination.clone(),
                    topology_revision: source_fact.derived.workspace.topology_revision(),
                    identity,
                },
            )
            .map_err(native_integrity_error)?;
        let receipt = self
            .substrate
            .execute_fork_staged(plan, |_stage| async { Ok::<_, CowshedError>(()) })
            .await
            .map_err(native_staged_error)?;
        self.commitments
            .publish(CommitmentDraft::Fork {
                repo_id: self.descriptor.repo_id.clone(),
                source_incarnation: source_fact.derived.workspace.incarnation().clone(),
                destination_incarnation: receipt.workspace.incarnation().clone(),
            })
            .await?;
        self.ensure_supervisor(&destination).await?;
        self.snapshot_named(&destination).await
    }

    async fn ensure(&mut self, workspace: WorkspaceName) -> Result<crate::api::dto::EnsureReport> {
        let before = self.current(&workspace).await?;
        let already = matches!(
            before.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        );
        self.ensure_supervisor(&workspace).await?;
        let current = self.current(&workspace).await?;
        let mount = self
            .layout
            .workspace_mount(&workspace)
            .map_err(native_integrity_error)?;
        Ok(self.ensure_report(
            &current,
            mount,
            if already {
                crate::api::dto::EnsureAction::AlreadyMounted
            } else {
                crate::api::dto::EnsureAction::Attached
            },
        ))
    }

    async fn attach(&mut self, workspace: WorkspaceName, options: AttachOptions) -> Result<()> {
        use crate::storage::lifecycle::{MountIntent, Substrate};
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        self.substrate
            .ensure_mounted(
                &current.derived.workspace,
                MountIntent {
                    browse: options.browse,
                },
            )
            .await
            .map_err(native_storage_error)?;
        self.ensure_supervisor(&workspace).await.map(|_| ())
    }

    async fn detach(&mut self, workspace: WorkspaceName) -> Result<()> {
        use crate::storage::lifecycle::Substrate;
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        self.stop_supervisor(&workspace).await?;
        self.substrate
            .unmount(&current.derived.workspace)
            .await
            .map_err(native_storage_error)
    }

    async fn checkpoint(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: Option<WorkspaceIncarnation>,
        options: CheckpointOptions,
    ) -> Result<CheckpointResult> {
        use crate::storage::lifecycle::LifecyclePlanner;

        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        if let Some(expected) = expected_incarnation.as_ref() {
            Self::require_exact_incarnation(&current, expected)?;
        }
        let explicitly_labeled = options.label.is_some();
        let label = crate::storage::CheckpointLabel::new(options.label.unwrap_or_else(|| {
            format!(
                "checkpoint-{}",
                current.derived.workspace.revision().get() + 1
            )
        }))
        .map_err(native_integrity_error)?;
        self.enforce_checkpoint_quota(&current).await?;
        let handle = self.ensure_supervisor(&workspace).await?;
        let barrier_id = u64::try_from(current.derived.checkpoints.len())
            .map_err(|_| CowshedError::internal("checkpoint count overflow"))?
            + 1;
        let barrier = handle
            .checkpoint_barrier(label.to_string(), barrier_id)
            .await?;
        let plan = self
            .substrate
            .plan_checkpoint(
                &current.derived.workspace,
                label.clone(),
                if options.keep || explicitly_labeled {
                    crate::storage::lifecycle::Pin::Pinned
                } else {
                    crate::storage::lifecycle::Pin::Automatic
                },
            )
            .map_err(native_integrity_error)?;
        self.substrate
            .execute_checkpoint_staged(plan, move |stage| async move {
                if stage.checkpoint.label().as_str() != barrier.checkpoint_id {
                    return Err(CowshedError::integrity(
                        "supervisor checkpoint barrier identity changed",
                        "cowshed doctor --json",
                    ));
                }
                Ok(())
            })
            .await
            .map_err(native_staged_error)?;
        Ok(CheckpointResult {
            label: label.to_string(),
        })
    }

    async fn restore(&mut self, workspace: WorkspaceName, label: String) -> Result<()> {
        use super::supervisor::{CommitmentDraft, CommitmentSink};
        use crate::storage::lifecycle::LifecyclePlanner;

        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        let label = crate::storage::CheckpointLabel::new(label).map_err(native_integrity_error)?;
        let checkpoint = current
            .derived
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.label == label)
            .cloned()
            .ok_or_else(|| {
                CowshedError::not_found(
                    format!("checkpoint {label} does not exist"),
                    "list workspace checkpoints and retry",
                )
            })?;
        self.stop_supervisor(&workspace).await?;
        let identity = self
            .operation_identity(current.metadata.grants.clone())
            .await?;
        let checkpoint_ref = crate::storage::lifecycle::CheckpointRef::new(
            current.derived.workspace.clone(),
            checkpoint.label.clone(),
            checkpoint.revision,
            matches!(checkpoint.pin, crate::storage::lifecycle::Pin::Pinned),
        );
        let plan = self
            .substrate
            .plan_restore(
                &current.derived.workspace,
                &checkpoint_ref,
                crate::storage::lifecycle::RestoreMode::Replace,
                identity,
            )
            .map_err(native_integrity_error)?;
        let mut commitments = self.commitments.clone();
        let result = self
            .substrate
            .execute_restore_staged(
                plan,
                |_stage| async { Ok::<_, CowshedError>(()) },
                move |fence| async move {
                    commitments
                        .publish(CommitmentDraft::Restore {
                            repo_id: fence.pending.workspace.repo().clone(),
                            source_checkpoint: fence.pending.source_checkpoint,
                            source_incarnation: fence.pending.source_incarnation,
                            destination_incarnation: fence.pending.workspace.incarnation().clone(),
                        })
                        .await
                        .map(|_| ())
                },
            )
            .await;
        match result {
            Ok(_) => {
                self.ensure_supervisor(&workspace).await?;
                Ok(())
            }
            Err(error) => Err(native_restore_error(error)),
        }
    }

    async fn remove(&mut self, workspace: WorkspaceName, _options: RemoveOptions) -> Result<()> {
        use crate::storage::lifecycle::{LifecyclePlanner, Substrate};

        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        self.stop_supervisor(&workspace).await?;
        let plan = self
            .substrate
            .plan_retire(&current.derived.workspace)
            .map_err(native_integrity_error)?;
        let mut commitments = self.commitments.clone();
        let repo_id = self.descriptor.repo_id.clone();
        let retired = self
            .substrate
            .execute_retire_staged(plan, move |retired| async move {
                commitments
                    .ensure_workspace_retired(repo_id, retired.workspace().incarnation().clone())
                    .await
                    .map(|_| ())
            })
            .await
            .map_err(native_retire_error)?;
        let substrate = self.substrate.clone();
        std::mem::drop(tokio::spawn(async move {
            // Retirement removed the canonical image from discovery. Reclamation is deliberately
            // best-effort here: an interrupted task leaves trash for the next idempotent gc pass.
            let _ = substrate.reclaim(retired).await;
        }));
        Ok(())
    }

    async fn gc(&mut self, options: GcOptions) -> Result<GcReport> {
        use crate::storage::lifecycle::{StorageGcReason, Substrate};

        self.validate_binding().await?;
        let plan = self
            .substrate
            .preview_gc(&self.descriptor.repo_id)
            .await
            .map_err(native_storage_error)?;
        let candidates = plan
            .candidates()
            .iter()
            .map(|candidate| crate::api::dto::GcCandidate {
                identity: crate::api::dto::Sha256Digest::from_bytes(candidate.identity()),
                path: candidate.path().to_owned(),
                bytes: candidate.bytes(),
                reason: match candidate.reason() {
                    StorageGcReason::RetiredWorkspace => {
                        crate::api::dto::GcReason::RetiredWorkspace
                    }
                    StorageGcReason::OrphanStagingImage => {
                        crate::api::dto::GcReason::OrphanStagingImage
                    }
                    StorageGcReason::OrphanStagingMetadata => {
                        crate::api::dto::GcReason::OrphanStagingMetadata
                    }
                    StorageGcReason::ExpiredCheckpoint => {
                        crate::api::dto::GcReason::ExpiredCheckpoint
                    }
                    StorageGcReason::DetachedImageCompaction => {
                        crate::api::dto::GcReason::DetachedImageCompaction
                    }
                },
            })
            .collect::<Vec<_>>();
        if options.dry_run {
            let freed_bytes = candidates
                .iter()
                .try_fold(0_u64, |sum, candidate| sum.checked_add(candidate.bytes))
                .ok_or_else(|| CowshedError::internal("GC candidate byte accounting overflow"))?;
            return Ok(GcReport {
                examined: u64::try_from(plan.examined())
                    .map_err(|_| CowshedError::internal("GC count overflow"))?,
                reclaimed: 0,
                retained_pinned: u64::try_from(plan.retained_pinned())
                    .map_err(|_| CowshedError::internal("GC count overflow"))?,
                freed_bytes,
                dry_run: true,
                candidates,
            });
        }
        let report = self
            .substrate
            .execute_gc(plan)
            .await
            .map_err(native_storage_error)?;
        Ok(GcReport {
            examined: u64::try_from(report.examined)
                .map_err(|_| CowshedError::internal("GC count overflow"))?,
            reclaimed: u64::try_from(report.reclaimed)
                .map_err(|_| CowshedError::internal("GC count overflow"))?,
            retained_pinned: u64::try_from(report.retained_pinned)
                .map_err(|_| CowshedError::internal("GC count overflow"))?,
            freed_bytes: report.freed_bytes,
            dry_run: false,
            candidates,
        })
    }

    async fn grant(
        &mut self,
        workspace: WorkspaceName,
        delta: GrantDelta,
        revoke: bool,
    ) -> Result<GrantSet> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        if delta
            .expected_revision
            .is_some_and(|revision| revision != current.metadata.grants.revision)
        {
            return Err(CowshedError::conflict(
                "grant revision is stale",
                "refresh grants and retry",
            ));
        }
        let mut metadata = current.metadata.clone();
        apply_grant_delta(&mut metadata.grants, delta, revoke);
        metadata.grants.revision = metadata
            .grants
            .revision
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("grant revision overflow"))?;
        let image = current.image;
        let published = metadata.grants.clone();
        crate::storage::lifecycle::dispatch_blocking(move || metadata.write_for_image(&image))
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))?
            .map_err(native_integrity_error)?;
        if let Some(handle) = self.supervisors.remove(&workspace) {
            let current = self.current(&workspace).await?;
            let config =
                supervisor_sandbox(&self.home, &self.layout, &self.telemetry_root, &current)?;
            let replacement = handle
                .advance_authority(
                    published.revision,
                    current.derived.workspace.revision().get(),
                    config,
                )
                .await?;
            self.supervisors.insert(workspace, replacement);
        }
        Ok(published)
    }

    async fn assign_slot(&mut self, workspace: WorkspaceName, slot: u32) -> Result<()> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        let base = u16::try_from(
            slot.checked_mul(u32::from(crate::metadata::PORT_BLOCK_SIZE))
                .ok_or_else(|| {
                    CowshedError::usage("slot overflows port space", "choose a smaller slot")
                })?,
        )
        .map_err(|_| CowshedError::usage("slot overflows port space", "choose a smaller slot"))?;
        let mut metadata = current.metadata;
        metadata.grants.port_block = Some(
            crate::metadata::PortBlock::new(base, crate::metadata::PORT_BLOCK_SIZE)
                .map_err(|error| CowshedError::usage(error.to_string(), "choose another slot"))?,
        );
        metadata.grants.revision = metadata
            .grants
            .revision
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("grant revision overflow"))?;
        let image = current.image;
        crate::storage::lifecycle::dispatch_blocking(move || metadata.write_for_image(&image))
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))?
            .map_err(native_integrity_error)?;
        if self.supervisors.contains_key(&workspace) {
            self.stop_supervisor(&workspace).await?;
            self.ensure_supervisor(&workspace).await?;
        }
        Ok(())
    }

    async fn set_checkpoint_quota(
        &mut self,
        workspace: WorkspaceName,
        quota: CheckpointQuota,
    ) -> Result<()> {
        self.validate_binding().await?;
        self.current(&workspace).await?;
        let path = self.layout.project().policy.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let mut policy: std::collections::BTreeMap<String, CheckpointQuota> =
                match crate::metadata::read_json(&path) {
                    Ok(policy) => policy,
                    Err(crate::metadata::MetadataError::Io { source, .. })
                        if source.kind() == std::io::ErrorKind::NotFound =>
                    {
                        std::collections::BTreeMap::new()
                    }
                    Err(error) => return Err(error),
                };
            policy.insert(workspace.to_string(), quota);
            crate::metadata::write_json(&path, &policy)
        })
        .await
        .map_err(|error| CowshedError::internal(error.to_string()))?
        .map_err(native_integrity_error)
    }

    async fn rebase(&mut self, workspace: WorkspaceName, options: RebaseOptions) -> Result<GitOid> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        if let Some(expected) = options.expected_workspace_incarnation.as_ref() {
            Self::require_exact_incarnation(&current, expected)?;
        }
        let root = current_snapshot_mount(self, &current)?;
        let source_head = git_oid(&root).await?;
        if options
            .expected_source_head
            .as_ref()
            .is_some_and(|expected| expected != &source_head)
        {
            return Err(CowshedError::conflict(
                "workspace source head is stale",
                "refresh the workspace revision and retry rebase",
            ));
        }
        let onto = options
            .onto
            .as_ref()
            .map(revision_target)
            .unwrap_or_else(|| "host/main".to_owned());
        let onto_head = git_revision_oid(&root, &onto).await?;
        if options
            .expected_onto_head
            .as_ref()
            .is_some_and(|expected| expected != &onto_head)
        {
            return Err(CowshedError::conflict(
                "rebase destination head is stale",
                "refresh the destination revision and retry rebase",
            ));
        }
        run_git(&root, ["rebase", onto.as_str()]).await?;
        git_oid(&root).await
    }

    async fn land(&mut self, workspace: WorkspaceName, options: LandOptions) -> Result<LandReport> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        if let Some(expected) = options.expected_workspace_incarnation.as_ref() {
            Self::require_exact_incarnation(&current, expected)?;
        }
        let source_head = git_oid(&current_snapshot_mount(self, &current)?).await?;
        if options
            .expected_source_head
            .as_ref()
            .is_some_and(|expected| expected != &source_head)
        {
            return Err(CowshedError::conflict(
                "workspace source head is stale",
                "refresh the workspace revision and retry land",
            ));
        }
        let target_branch = options
            .target_branch
            .clone()
            .unwrap_or_else(|| "main".into());
        let target_ref = format!("refs/heads/{target_branch}");
        let previous = git_optional_ref_oid(&self.descriptor.git_root, &target_ref).await?;
        require_expected_ref(
            options.expected_target_head.as_ref(),
            previous.as_ref(),
            "land target",
        )?;
        let retire = options.retire;
        let handle = self.ensure_supervisor(&workspace).await?;
        for check in options.check.unwrap_or_default() {
            let job_id = handle
                .exec(
                    None,
                    ExecRequest {
                        argv: vec!["/bin/sh".into(), "-c".into(), check.clone().into()],
                        cwd: None,
                        mode: RunSandboxMode::ReadWrite,
                        env: std::collections::HashMap::new(),
                        trace: None,
                        stdin: StdinSource::Empty,
                        stdout_copy: None,
                        stderr_copy: None,
                    },
                )
                .await?;
            let info = handle.wait(job_id).await?;
            if !matches!(
                info.exit,
                Some(crate::api::dto::ExitStatus::Exited { code: 0 })
            ) {
                return Err(CowshedError::conflict(
                    format!("land check failed: {check}"),
                    "fix the workspace and retry land",
                ));
            }
        }
        run_git(
            &self.descriptor.git_root,
            ["merge", "--ff-only", source_head.as_str()],
        )
        .await?;
        if retire {
            self.remove(workspace, RemoveOptions::default()).await?;
        }
        Ok(LandReport {
            landed_head: source_head,
            target_branch,
            previous_target_head: previous,
            target_was_checked_out: true,
            retired: retire,
        })
    }

    async fn push(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: WorkspaceIncarnation,
        options: PushOptions,
    ) -> Result<PushReport> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &expected_incarnation)?;
        let root = current_snapshot_mount(self, &current)?;
        let source_head = git_oid(&root).await?;
        if options
            .expected_source_head
            .as_ref()
            .is_some_and(|expected| expected != &source_head)
        {
            return Err(CowshedError::conflict(
                "workspace source head is stale",
                "refresh the workspace revision and retry push",
            ));
        }
        let branch = options.branch.unwrap_or_else(|| workspace.to_string());
        let destination_ref = format!("refs/heads/{branch}");
        let previous_destination_head = git_remote_ref_oid(&root, "host", &destination_ref).await?;
        require_expected_ref(
            options.expected_destination_head.as_ref(),
            previous_destination_head.as_ref(),
            "push destination",
        )?;
        run_git(&root, ["push", "host", &format!("HEAD:{destination_ref}")]).await?;
        Ok(PushReport {
            source_head,
            destination_ref,
            previous_destination_head,
        })
    }

    async fn repo_mirror(&mut self, workspace: WorkspaceName, url: Url) -> Result<MirrorInfo> {
        use crate::storage::lifecycle::Substrate;
        self.validate_binding().await?;
        self.current(&workspace).await?;
        let root = self
            .substrate
            .caches_root()
            .await
            .map_err(native_storage_error)?
            .join("mirrors")
            .join(
                crate::repository::encode_component(url.as_str())
                    .map_err(native_integrity_error)?,
            );
        if tokio::fs::try_exists(&root).await.map_err(|error| {
            CowshedError::environment_missing(error.to_string(), "check cache permissions")
        })? {
            run_git(&root, ["remote", "update", "--prune"]).await?;
        } else {
            let parent = root
                .parent()
                .ok_or_else(|| CowshedError::internal("mirror root has no parent"))?
                .to_path_buf();
            tokio::fs::create_dir_all(&parent).await.map_err(|error| {
                CowshedError::environment_missing(error.to_string(), "check cache permissions")
            })?;
            let output = tokio::process::Command::new("/usr/bin/git")
                .arg("clone")
                .arg("--mirror")
                .arg(url.as_str())
                .arg(&root)
                .output()
                .await
                .map_err(|error| {
                    CowshedError::environment_missing(error.to_string(), "install git")
                })?;
            require_git_success("clone mirror", &output)?;
        }
        Ok(MirrorInfo {
            url: url.to_string(),
            mirror: root,
        })
    }

    async fn doctor(&mut self) -> Result<DoctorReport> {
        let mut findings = Vec::new();
        if let Err(error) = self.validate_binding().await {
            findings.push(native_finding(
                "binding",
                crate::api::dto::FindingSeverity::Error,
                error,
            ));
        }
        match self.pending_metadata().await {
            Ok(pending) => {
                for (image, metadata) in pending {
                    findings.push(crate::api::dto::Finding {
                        code: "pending-publication".into(),
                        severity: crate::api::dto::FindingSeverity::Warning,
                        message: format!(
                            "workspace {} is pending its restore fence",
                            metadata.workspace
                        ),
                        hint: "retry restore after repairing commitment or gateway evidence".into(),
                        path: Some(image),
                    });
                }
            }
            Err(error) => findings.push(native_finding(
                "pending-integrity",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
        }
        match self.authoritative().await {
            Ok(workspaces) => {
                for workspace in workspaces {
                    if matches!(
                        workspace.derived.mount_state,
                        crate::storage::lifecycle::MountState::Detached
                    ) && self
                        .supervisors
                        .contains_key(workspace.derived.workspace.name())
                    {
                        findings.push(crate::api::dto::Finding {
                            code: "mount-supervisor".into(),
                            severity: crate::api::dto::FindingSeverity::Error,
                            message: "detached workspace still has a supervisor".into(),
                            hint: "cowshed detach and reattach the workspace".into(),
                            path: Some(workspace.image),
                        });
                    }
                }
            }
            Err(error) => findings.push(native_finding(
                "metadata-integrity",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
        }
        Ok(DoctorReport {
            healthy: !findings
                .iter()
                .any(|finding| finding.severity == crate::api::dto::FindingSeverity::Error),
            findings,
        })
    }

    async fn open_worker(&mut self, workspace: WorkspaceName) -> Result<WorkspaceSnapshot> {
        self.ensure_supervisor(&workspace).await?;
        self.snapshot_named(&workspace).await
    }

    async fn open_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        name: Option<String>,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let handle = self.ensure_supervisor(&workspace).await?;
        let token = handle.open_session(name.clone()).await?;
        if let Some(previous) = self.sessions.insert((workspace, name), token) {
            handle.close_session(previous).await?;
        }
        Ok(())
    }

    async fn close_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        name: Option<String>,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let token = self
            .sessions
            .remove(&(workspace.clone(), name))
            .ok_or_else(|| {
                CowshedError::not_found(
                    "session does not exist",
                    "open the session before closing it",
                )
            })?;
        self.ensure_supervisor(&workspace)
            .await?
            .close_session(token)
            .await
    }

    async fn exec(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        session: Option<String>,
        request: ExecRequest,
    ) -> Result<JobId> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let token = self.session(&workspace, &session).cloned();
        self.ensure_supervisor(&workspace)
            .await?
            .exec(token.as_ref(), request)
            .await
    }

    async fn stdin_write(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        bytes: Bytes,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace)
            .await?
            .stdin_write(job, bytes)
            .await
    }

    async fn stdin_close(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace)
            .await?
            .stdin_close(job)
            .await
    }

    async fn list_jobs(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    ) -> Result<Vec<JobInfo>> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.list().await
    }

    async fn job_info(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobInfo> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.info(job).await
    }

    async fn wait_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobInfo> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.wait(job).await
    }

    async fn kill_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.kill(job).await
    }

    async fn detach_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.info(job).await?;
        Ok(())
    }

    async fn read_log(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        stream: RuntimeJobStream,
        offset: u64,
        follow: bool,
    ) -> Result<RuntimeLogChunk> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let stream = match stream {
            RuntimeJobStream::Stdout => super::supervisor::OutputStream::Stdout,
            RuntimeJobStream::Stderr => super::supervisor::OutputStream::Stderr,
        };
        let chunk = self
            .ensure_supervisor(&workspace)
            .await?
            .log_read(job, stream, offset, follow)
            .await?;
        Ok(RuntimeLogChunk {
            bytes: chunk.bytes,
            next_offset: chunk.next_offset,
            eof: chunk.eof,
        })
    }
}

#[cfg(target_os = "macos")]
async fn binding_from_git(
    git: &crate::git::GitRepository,
    requested_repo_id: Option<&RepoId>,
) -> Result<RepositoryBinding> {
    let remotes = git.remotes().await?;
    binding_from_remotes(&remotes, requested_repo_id)
}

fn binding_from_remotes(
    remotes: &[crate::git::RemoteUrl],
    requested_repo_id: Option<&RepoId>,
) -> Result<RepositoryBinding> {
    if remotes.is_empty() {
        let repo_id = requested_repo_id.cloned().ok_or_else(|| {
            CowshedError::environment_missing(
                "repository has no remote from which to derive its identity",
                "retry adoption with --repo-id owner/repo",
            )
        })?;
        return RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id,
            remote_name: None,
            remote_url: None,
            primary: true,
        }])
        .map_err(binding_integrity_error);
    }

    let mut candidates = Vec::with_capacity(remotes.len());
    for remote in remotes {
        let repo_id = crate::repository::normalize_remote_url(&remote.url).map_err(|error| {
            CowshedError::usage(
                format!(
                    "Git remote {} has an invalid repository URL: {error}",
                    remote.name
                ),
                format!("fix or remove Git remote {}", remote.name),
            )
        })?;
        candidates.push((remote, repo_id));
    }

    let available = candidates
        .iter()
        .map(|(_, repo_id)| repo_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let selected_repo_id = if let Some(requested_repo_id) = requested_repo_id {
        if !available.contains(requested_repo_id) {
            return Err(CowshedError::conflict(
                format!(
                    "explicit repository identity {requested_repo_id} does not match any Git remote"
                ),
                format!(
                    "retry with --repo-id matching one of: {}",
                    available
                        .iter()
                        .map(RepoId::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        requested_repo_id.clone()
    } else {
        if available.len() != 1 {
            return Err(CowshedError::conflict(
                "Git remotes resolve to multiple repository identities",
                format!(
                    "retry with --repo-id selecting one of: {}",
                    available
                        .iter()
                        .map(RepoId::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        available
            .first()
            .cloned()
            .ok_or_else(|| CowshedError::internal("repository candidate set is empty"))?
    };

    let selected = candidates
        .iter()
        .filter(|(_, repo_id)| repo_id == &selected_repo_id)
        .min_by(|(left, _), (right, _)| {
            (left.name != "origin", &left.name, &left.url).cmp(&(
                right.name != "origin",
                &right.name,
                &right.url,
            ))
        })
        .map(|(remote, _)| *remote)
        .ok_or_else(|| CowshedError::internal("selected repository candidate is missing"))?;

    RepositoryBinding::new(vec![crate::repository::BoundIdentity {
        repo_id: selected_repo_id,
        remote_name: Some(selected.name.clone()),
        remote_url: Some(persistable_remote_url(&selected.url)),
        primary: true,
    }])
    .map_err(binding_integrity_error)
}

fn persistable_remote_url(value: &str) -> String {
    let suffix = value
        .char_indices()
        .find_map(|(index, character)| matches!(character, '?' | '#').then_some(index));
    let without_suffix = suffix.map_or(value, |index| &value[..index]);
    if let Some((scheme, remainder)) = without_suffix.split_once("://") {
        let (authority, path) = remainder
            .split_once('/')
            .expect("normalized remote URL has a repository path");
        let authority = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        format!("{scheme}://{authority}/{path}")
    } else {
        let (authority, path) = without_suffix
            .split_once(':')
            .expect("normalized SCP-like remote has a repository path");
        let authority = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        format!("{authority}:{path}")
    }
}

fn binding_integrity_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(error.to_string(), "repair the repository binding")
}

#[cfg(target_os = "macos")]
async fn repo_id_from_workspace_marker(project_root: &Path) -> Result<Option<RepoId>> {
    let marker_path = project_root.join(crate::storage::WORKSPACE_MARKER_PATH);
    let marker = crate::storage::lifecycle::dispatch_blocking(move || {
        match crate::metadata::WorkspaceMarker::read_from(&marker_path) {
            Ok(marker) => Ok(Some(marker)),
            Err(crate::metadata::MetadataError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|error| CowshedError::internal(format!("workspace marker task failed: {error}")))?
    .map_err(native_integrity_error)?;
    let Some(marker) = marker else {
        return Ok(None);
    };
    if marker.project_root != project_root
        || !marker.workspace.is_main()
        || marker.role != crate::metadata::WorkspaceRole::Main
    {
        return Err(CowshedError::conflict(
            "main workspace marker does not match this project root",
            "reopen the canonical main workspace or repair its marker",
        ));
    }
    Ok(Some(marker.repo_id))
}

#[cfg(target_os = "macos")]
fn validate_binding_against_remotes(
    binding: &RepositoryBinding,
    remotes: &[crate::git::RemoteUrl],
) -> Result<()> {
    binding.validate().map_err(native_integrity_error)?;
    for identity in &binding.identities {
        if let (Some(name), Some(url)) = (&identity.remote_name, &identity.remote_url)
            && !remotes
                .iter()
                .any(|remote| &remote.name == name && persistable_remote_url(&remote.url) == *url)
        {
            return Err(CowshedError::conflict(
                format!("repository binding remote {name} does not match Git configuration"),
                "restore the recorded remote before opening cowshed",
            ));
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
async fn load_or_validate_binding(
    layout: &crate::storage::StorageLayout,
    candidate: RepositoryBinding,
    git: &crate::git::GitRepository,
) -> Result<RepositoryBinding> {
    let candidate_repo_id = candidate
        .primary()
        .map_err(native_integrity_error)?
        .repo_id
        .clone();
    let path = layout.project().repository_binding.clone();
    let loaded = crate::storage::lifecycle::dispatch_blocking(move || {
        match crate::metadata::read_json::<RepositoryBinding>(&path) {
            Ok(binding) => Ok(Some(binding)),
            Err(crate::metadata::MetadataError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|error| CowshedError::internal(error.to_string()))?
    .map_err(native_integrity_error)?;
    let binding = loaded.unwrap_or(candidate);
    if binding.primary().map_err(native_integrity_error)?.repo_id != candidate_repo_id {
        return Err(CowshedError::conflict(
            "persisted repository identity differs from the opened storage layout",
            "repair the repository binding before opening cowshed",
        ));
    }
    let remotes = git.remotes().await?;
    validate_binding_against_remotes(&binding, &remotes)?;
    Ok(binding)
}

#[cfg(target_os = "macos")]
async fn enforce_adopt_secret_policy(
    root: PathBuf,
    waivers_path: PathBuf,
    quarantine_root: PathBuf,
    quarantine: bool,
) -> Result<()> {
    crate::storage::lifecycle::dispatch_blocking(move || {
        let waivers =
            match crate::metadata::read_json::<Vec<crate::secrets::SecretWaiver>>(&waivers_path) {
                Ok(waivers) => waivers,
                Err(crate::metadata::MetadataError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    Vec::new()
                }
                Err(error) => return Err(native_integrity_error(error)),
            };
        let scan = crate::secrets::scan_tree(&root, &waivers).map_err(secret_scan_error)?;
        if scan.findings.is_empty() {
            return Ok(());
        }
        if !quarantine {
            return Err(secret_findings_error(&scan.findings));
        }
        quarantine_secret_files(&root, &quarantine_root, &scan.findings)?;
        let remaining = crate::secrets::scan_tree(&root, &waivers).map_err(secret_scan_error)?;
        if remaining.findings.is_empty() {
            Ok(())
        } else {
            Err(secret_findings_error(&remaining.findings))
        }
    })
    .await
    .map_err(|error| CowshedError::internal(format!("secret scan task failed: {error}")))?
}

#[cfg(target_os = "macos")]
fn secret_scan_error(error: crate::secrets::SecretScanError) -> CowshedError {
    match error {
        crate::secrets::SecretScanError::InvalidWaiver { .. }
        | crate::secrets::SecretScanError::DuplicateWaiver { .. } => CowshedError::integrity(
            error.to_string(),
            "repair the controller-owned waivers file",
        ),
        crate::secrets::SecretScanError::InvalidRoot { .. }
        | crate::secrets::SecretScanError::Walk { .. }
        | crate::secrets::SecretScanError::Read { .. } => CowshedError::environment_missing(
            error.to_string(),
            "make the complete repository tree readable and retry adopt",
        ),
    }
}

#[cfg(target_os = "macos")]
fn secret_findings_error(findings: &[crate::secrets::SecretFinding]) -> CowshedError {
    let paths = findings
        .iter()
        .map(|finding| finding.path.display().to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ");
    CowshedError::conflict(
        format!("repository contains secrets in: {paths}"),
        "remove the files, add reasoned controller waivers, or retry adopt with quarantine",
    )
}

#[cfg(target_os = "macos")]
fn quarantine_secret_files(
    root: &Path,
    quarantine_root: &Path,
    findings: &[crate::secrets::SecretFinding],
) -> Result<()> {
    let paths = findings
        .iter()
        .map(|finding| finding.path.clone())
        .collect::<std::collections::BTreeSet<_>>();
    secure_quarantine_directory(quarantine_root, Path::new(""))?;
    for relative in paths {
        let source = root.join(&relative);
        let source_metadata = match std::fs::symlink_metadata(&source) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(quarantine_io_error("inspect secret source", &source, error)),
        };
        if !source_metadata.is_file() || source_metadata.file_type().is_symlink() {
            return Err(CowshedError::conflict(
                format!(
                    "secret source {} changed after the full-tree scan",
                    relative.display()
                ),
                "stop repository writers and retry adopt",
            ));
        }
        let parent = relative.parent().unwrap_or_else(|| Path::new(""));
        let destination_parent = secure_quarantine_directory(quarantine_root, parent)?;
        let file_name = relative.file_name().ok_or_else(|| {
            CowshedError::integrity(
                format!("secret finding has no file name: {}", relative.display()),
                "run cowshed doctor --json",
            )
        })?;
        let destination = destination_parent.join(file_name);
        if destination.exists() {
            if files_equal(&source, &destination)? {
                std::fs::set_permissions(
                    &destination,
                    std::os::unix::fs::PermissionsExt::from_mode(0o600),
                )
                .map_err(|error| {
                    quarantine_io_error("secure quarantined secret", &destination, error)
                })?;
                std::fs::remove_file(&source).map_err(|error| {
                    quarantine_io_error("remove quarantined source", &source, error)
                })?;
                sync_parent(&source)?;
                continue;
            }
            return Err(CowshedError::conflict(
                format!(
                    "quarantine destination {} already contains different bytes",
                    destination.display()
                ),
                "move the existing quarantine artifact aside and retry adopt",
            ));
        }
        let temporary =
            destination_parent.join(format!(".cowshed-quarantine-{}.tmp", uuid::Uuid::new_v4()));
        if let Err(error) = std::fs::copy(&source, &temporary) {
            return Err(quarantine_io_error(
                "copy secret into quarantine",
                &temporary,
                error,
            ));
        }
        let prepared = (|| {
            std::fs::set_permissions(
                &temporary,
                std::os::unix::fs::PermissionsExt::from_mode(0o600),
            )
            .map_err(|error| quarantine_io_error("secure quarantined secret", &temporary, error))?;
            std::fs::File::open(&temporary)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    quarantine_io_error("sync quarantined secret", &temporary, error)
                })?;
            if !files_equal(&source, &temporary)? {
                return Err(CowshedError::conflict(
                    format!(
                        "secret source {} changed while it was quarantined",
                        relative.display()
                    ),
                    "stop repository writers and retry adopt",
                ));
            }
            std::fs::rename(&temporary, &destination).map_err(|error| {
                quarantine_io_error("publish quarantined secret", &destination, error)
            })?;
            sync_parent(&destination)?;
            std::fs::remove_file(&source).map_err(|error| {
                quarantine_io_error("remove quarantined source", &source, error)
            })?;
            sync_parent(&source)
        })();
        if prepared.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        prepared?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn secure_quarantine_directory(root: &Path, relative: &Path) -> Result<PathBuf> {
    let mut current = root.to_path_buf();
    for component in std::iter::once(None).chain(relative.components().map(Some)) {
        if let Some(component) = component {
            let std::path::Component::Normal(component) = component else {
                return Err(CowshedError::integrity(
                    format!(
                        "secret quarantine path escapes its root: {}",
                        relative.display()
                    ),
                    "run cowshed doctor --json",
                ));
            };
            current.push(component);
        }
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(CowshedError::integrity(
                    format!(
                        "secret quarantine directory is not a real directory: {}",
                        current.display()
                    ),
                    "repair the controller-owned quarantine tree",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&current).map_err(|error| {
                    quarantine_io_error("create secret quarantine directory", &current, error)
                })?;
            }
            Err(error) => {
                return Err(quarantine_io_error(
                    "inspect secret quarantine directory",
                    &current,
                    error,
                ));
            }
        }
        std::fs::set_permissions(
            &current,
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .map_err(|error| {
            quarantine_io_error("secure secret quarantine directory", &current, error)
        })?;
    }
    Ok(current)
}

#[cfg(target_os = "macos")]
fn files_equal(left: &Path, right: &Path) -> Result<bool> {
    use std::io::Read;

    let mut left = std::io::BufReader::new(
        std::fs::File::open(left)
            .map_err(|error| quarantine_io_error("open secret source", left, error))?,
    );
    let mut right = std::io::BufReader::new(
        std::fs::File::open(right)
            .map_err(|error| quarantine_io_error("open quarantined secret", right, error))?,
    );
    let mut left_buffer = [0_u8; 16 * 1024];
    let mut right_buffer = [0_u8; 16 * 1024];
    loop {
        let left_read = left
            .read(&mut left_buffer)
            .map_err(|error| CowshedError::environment_missing(error.to_string(), "retry adopt"))?;
        let right_read = right
            .read(&mut right_buffer)
            .map_err(|error| CowshedError::environment_missing(error.to_string(), "retry adopt"))?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

#[cfg(target_os = "macos")]
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        CowshedError::integrity(
            format!("path has no parent: {}", path.display()),
            "run cowshed doctor --json",
        )
    })?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| quarantine_io_error("sync directory", parent, error))
}

#[cfg(target_os = "macos")]
fn quarantine_io_error(operation: &str, path: &Path, error: std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("{operation} at {} failed: {error}", path.display()),
        "check repository and controller storage permissions, then retry adopt",
    )
}

#[cfg(target_os = "macos")]
fn pre_cowshed_path(root: &Path) -> Result<PathBuf> {
    if root.file_name().is_none() {
        return Err(CowshedError::usage(
            "repository root has no final component",
            "move the repository to a supported path",
        ));
    }
    let mut path = root.as_os_str().to_owned();
    path.push(".pre-cowshed");
    Ok(PathBuf::from(path))
}

#[cfg(all(test, target_os = "macos"))]
mod pre_cowshed_tests {
    use std::ffi::OsString;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    use super::*;

    #[test]
    fn handoff_suffix_preserves_the_exact_repository_path_bytes() {
        assert_eq!(
            pre_cowshed_path(Path::new("/tmp/widget")).expect("UTF-8 root"),
            Path::new("/tmp/widget.pre-cowshed")
        );

        let mut root = PathBuf::from("/tmp");
        root.push(OsString::from_vec(vec![b'w', 0x80, b's']));
        let mut expected = root.as_os_str().as_bytes().to_vec();
        expected.extend_from_slice(b".pre-cowshed");
        assert_eq!(
            pre_cowshed_path(&root)
                .expect("opaque Unix root")
                .as_os_str()
                .as_bytes(),
            expected
        );
    }
}

#[cfg(target_os = "macos")]
fn revision_target(target: &crate::api::dto::RevisionTarget) -> String {
    match target {
        crate::api::dto::RevisionTarget::Branch(branch) => branch.as_str().to_owned(),
        crate::api::dto::RevisionTarget::Ref(reference) => reference.as_str().to_owned(),
        crate::api::dto::RevisionTarget::Oid(oid) => oid.as_str().to_owned(),
    }
}

#[cfg(target_os = "macos")]
async fn utc_timestamp() -> Result<String> {
    let output = tokio::process::Command::new("/bin/date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .await
        .map_err(|error| {
            CowshedError::environment_missing(error.to_string(), "restore /bin/date")
        })?;
    if !output.status.success() {
        return Err(CowshedError::environment_missing(
            "cannot read the system UTC clock",
            "repair /bin/date and retry",
        ));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim_end().to_owned())
        .map_err(|error| CowshedError::integrity(error.to_string(), "repair /bin/date"))
}

#[cfg(target_os = "macos")]
fn current_snapshot_mount(
    host: &NativeProjectRuntimeHost,
    workspace: &NativeWorkspace,
) -> Result<PathBuf> {
    host.layout
        .workspace_mount(workspace.derived.workspace.name())
        .map_err(native_integrity_error)
}

#[cfg(target_os = "macos")]
async fn run_git<const N: usize>(root: &Path, args: [&str; N]) -> Result<()> {
    let output = tokio::process::Command::new("/usr/bin/git")
        .args(args)
        .current_dir(root)
        .output()
        .await
        .map_err(|error| {
            CowshedError::environment_missing(error.to_string(), "restore /usr/bin/git")
        })?;
    require_git_success("git operation", &output)
}

#[cfg(target_os = "macos")]
async fn git_oid(root: &Path) -> Result<GitOid> {
    git_revision_oid(root, "HEAD").await
}

#[cfg(target_os = "macos")]
async fn git_revision_oid(root: &Path, revision: &str) -> Result<GitOid> {
    let output = tokio::process::Command::new("/usr/bin/git")
        .args(["rev-parse", "--verify", revision])
        .current_dir(root)
        .output()
        .await
        .map_err(|error| {
            CowshedError::environment_missing(error.to_string(), "restore /usr/bin/git")
        })?;
    require_git_success("resolve git revision", &output)?;
    let value = String::from_utf8(output.stdout)
        .map_err(|error| CowshedError::integrity(error.to_string(), "repair the git repository"))?;
    GitOid::new(value.trim_end()).map_err(native_integrity_error)
}

#[cfg(target_os = "macos")]
async fn git_optional_ref_oid(root: &Path, reference: &str) -> Result<Option<GitOid>> {
    let output = tokio::process::Command::new("/usr/bin/git")
        .args(["show-ref", "--verify", "--hash", reference])
        .current_dir(root)
        .output()
        .await
        .map_err(|error| {
            CowshedError::environment_missing(error.to_string(), "restore /usr/bin/git")
        })?;
    if output.status.code() == Some(1) {
        return Ok(None);
    }
    require_git_success("resolve git reference", &output)?;
    let value = String::from_utf8(output.stdout)
        .map_err(|error| CowshedError::integrity(error.to_string(), "repair the git repository"))?;
    GitOid::new(value.trim_end())
        .map(Some)
        .map_err(native_integrity_error)
}

#[cfg(target_os = "macos")]
async fn git_remote_ref_oid(root: &Path, remote: &str, reference: &str) -> Result<Option<GitOid>> {
    let output = tokio::process::Command::new("/usr/bin/git")
        .args(["ls-remote", "--refs", remote, reference])
        .current_dir(root)
        .output()
        .await
        .map_err(|error| {
            CowshedError::environment_missing(error.to_string(), "restore /usr/bin/git")
        })?;
    require_git_success("resolve remote git reference", &output)?;
    if output.stdout.is_empty() {
        return Ok(None);
    }
    let value = String::from_utf8(output.stdout)
        .map_err(|error| CowshedError::integrity(error.to_string(), "repair the git remote"))?;
    let oid = value.split_whitespace().next().ok_or_else(|| {
        CowshedError::integrity(
            "remote reference response is empty",
            "repair the git remote",
        )
    })?;
    GitOid::new(oid).map(Some).map_err(native_integrity_error)
}

#[cfg(target_os = "macos")]
fn require_expected_ref(
    expected: Option<&crate::api::dto::ExpectedRefHead>,
    actual: Option<&GitOid>,
    dimension: &str,
) -> Result<()> {
    let matches = match (expected, actual) {
        (None, _) => true,
        (Some(crate::api::dto::ExpectedRefHead::Missing), None) => true,
        (Some(crate::api::dto::ExpectedRefHead::Oid(expected)), Some(actual)) => expected == actual,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(CowshedError::conflict(
            format!("{dimension} revision is stale"),
            "refresh repository revisions and retry",
        ))
    }
}

#[cfg(target_os = "macos")]
fn require_git_success(operation: &str, output: &std::process::Output) -> Result<()> {
    if output.status.success() {
        Ok(())
    } else {
        Err(CowshedError::conflict(
            format!(
                "{operation} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
            "resolve the git conflict and retry",
        ))
    }
}

#[cfg(target_os = "macos")]
fn apply_grant_delta(grants: &mut GrantSet, delta: GrantDelta, revoke: bool) {
    update_set(&mut grants.read, delta.read, revoke);
    update_set(&mut grants.write, delta.write, revoke);
    update_set(&mut grants.egress, delta.egress, revoke);
    update_set(&mut grants.repos, delta.repos, revoke);
    update_set(&mut grants.sim, delta.sim, revoke);
}

#[cfg(target_os = "macos")]
fn update_set<T: PartialEq>(current: &mut Vec<T>, delta: Vec<T>, revoke: bool) {
    if revoke {
        current.retain(|value| !delta.contains(value));
    } else {
        for value in delta {
            if !current.contains(&value) {
                current.push(value);
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn supervisor_sandbox(
    home: &Path,
    layout: &crate::storage::StorageLayout,
    telemetry_root: &Path,
    current: &NativeWorkspace,
) -> Result<crate::sandbox::SandboxConfig> {
    let mount = layout
        .workspace_mount(current.derived.workspace.name())
        .map_err(native_integrity_error)?;
    Ok(crate::sandbox::SandboxConfig {
        home: home.to_path_buf(),
        workspace_mount: mount,
        exec_temp_dir: layout
            .project()
            .quarantine
            .join(current.derived.workspace.incarnation().as_str()),
        port_block: current.metadata.grants.port_block.ok_or_else(|| {
            CowshedError::integrity("workspace has no port block", "cowshed doctor --json")
        })?,
        mode: crate::sandbox::RunSandboxMode::ReadWrite,
        grants: crate::sandbox::SandboxGrants {
            read: current.metadata.grants.read.clone(),
            write: current.metadata.grants.write.clone(),
            egress: current
                .metadata
                .grants
                .egress
                .iter()
                .map(|rule| crate::sandbox::EgressGrant {
                    host: rule.host.clone(),
                    ports: rule.ports.clone(),
                })
                .collect(),
        },
        allowed_unix_sockets: Vec::new(),
        additional_denies: vec![
            layout.project().project_root.clone(),
            telemetry_root.to_path_buf(),
        ],
    })
}

#[cfg(target_os = "macos")]
fn native_retired_refs(
    project_root: &Path,
    repo_id: &RepoId,
) -> Result<Vec<crate::storage::lifecycle::RetiredRef>> {
    use crate::metadata::{
        DetachedWorkspaceMetadata, ImageFormat, PublicationState, WorkspaceRole,
    };
    use crate::storage::lifecycle::{LifecycleWorkspace, RetiredRef, Revision};

    let trash = project_root.join("sessions").join(".trash");
    let entries = match std::fs::read_dir(&trash) {
        Ok(entries) => entries
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                CowshedError::integrity(
                    format!("cannot enumerate retired workspace trash: {error}"),
                    "cowshed doctor --json",
                )
            })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(CowshedError::integrity(
                format!("cannot enumerate retired workspace trash: {error}"),
                "cowshed doctor --json",
            ));
        }
    };
    let mut images = entries
        .into_iter()
        .filter_map(|entry| {
            ImageFormat::from_image_path(&entry.path())
                .ok()
                .map(|format| (entry, format))
        })
        .collect::<Vec<_>>();
    images.sort_by_key(|(entry, _)| entry.file_name());

    let mut retired = Vec::new();
    retired
        .try_reserve(images.len())
        .map_err(|_| CowshedError::internal("cannot reserve retired workspace recovery facts"))?;
    for (entry, format) in images {
        let file_type = entry.file_type().map_err(|error| {
            CowshedError::integrity(
                format!("cannot inspect retired workspace image: {error}"),
                "cowshed doctor --json",
            )
        })?;
        if !file_type.is_file() {
            return Err(CowshedError::integrity(
                format!(
                    "retired workspace image is not a regular file: {}",
                    entry.path().display()
                ),
                "cowshed doctor --json",
            ));
        }
        let metadata = DetachedWorkspaceMetadata::read_for_image(&entry.path())
            .map_err(native_integrity_error)?;
        if metadata.repo_id != *repo_id
            || metadata.workspace.is_main()
            || metadata.image_format != format
            || metadata.publication_state != PublicationState::Active
        {
            return Err(CowshedError::integrity(
                format!(
                    "retired workspace metadata identity mismatch: {}",
                    entry.path().display()
                ),
                "cowshed doctor --json",
            ));
        }
        let expected = trash.join(format!(
            "{}-{}.{}",
            metadata.workspace.as_str(),
            metadata.workspace_incarnation.as_str(),
            format.extension()
        ));
        if entry.path() != expected {
            return Err(CowshedError::integrity(
                format!(
                    "retired workspace path disagrees with metadata identity: {}",
                    entry.path().display()
                ),
                "cowshed doctor --json",
            ));
        }
        let revision = Revision::new(metadata.grants.revision);
        let workspace = LifecycleWorkspace::new(
            metadata.repo_id,
            metadata.workspace,
            metadata.workspace_incarnation,
            revision,
            revision,
            WorkspaceRole::Workspace,
            format,
        )
        .map_err(native_integrity_error)?;
        let resulting_revision = revision
            .get()
            .checked_add(1)
            .map(Revision::new)
            .ok_or_else(|| {
                CowshedError::integrity(
                    "retired workspace revision overflow",
                    "cowshed doctor --json",
                )
            })?;
        retired.push(RetiredRef::new(workspace, resulting_revision));
    }
    Ok(retired)
}

#[cfg(all(test, target_os = "macos"))]
mod retired_recovery_tests {
    use super::*;
    use crate::metadata::{
        DetachedWorkspaceMetadata, GrantSet, ImageFormat, METADATA_VERSION, Platform, PortBlock,
        PublicationState,
    };

    #[test]
    fn retired_trash_is_a_verified_restart_baseline_fact() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-retired-recovery-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let project_root = root.join("acme/widget");
        let trash = project_root.join("sessions/.trash");
        std::fs::create_dir_all(&trash).unwrap();
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
        let image = trash.join(format!("raven-{}.asif", incarnation.as_str()));
        std::fs::write(&image, b"retired image").unwrap();
        let mut grants =
            GrantSet::closed_baseline(Some(PortBlock::new(49_152, 16).unwrap())).unwrap();
        grants.revision = 4;
        DetachedWorkspaceMetadata {
            version: METADATA_VERSION,
            repo_id: repo_id.clone(),
            workspace: WorkspaceName::new("raven").unwrap(),
            workspace_incarnation: incarnation.clone(),
            image_format: ImageFormat::Asif,
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-14T00:00:00Z".into(),
            grants,
            info_snapshot: None,
        }
        .write_for_image(&image)
        .unwrap();

        let retired = native_retired_refs(&project_root, &repo_id).unwrap();
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].workspace().incarnation(), &incarnation);
        assert_eq!(
            retired[0].resulting_revision(),
            crate::storage::lifecycle::Revision::new(5)
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(target_os = "macos")]
fn native_staged_error(
    error: crate::storage::apfs::StagedExecutionError<CowshedError>,
) -> CowshedError {
    match error {
        crate::storage::apfs::StagedExecutionError::Storage(error) => native_storage_error(error),
        crate::storage::apfs::StagedExecutionError::Initializer(error) => error,
        crate::storage::apfs::StagedExecutionError::InitializerCleanup {
            initializer,
            cleanup,
        } => CowshedError::integrity(
            format!("{initializer}; cleanup also failed: {cleanup}"),
            "cowshed doctor --json",
        ),
    }
}

#[cfg(target_os = "macos")]
fn native_retire_error(
    error: crate::storage::apfs::RetireExecutionError<CowshedError>,
) -> CowshedError {
    match error {
        crate::storage::apfs::RetireExecutionError::Storage(error) => native_storage_error(error),
        crate::storage::apfs::RetireExecutionError::Fence { source, .. } => source,
    }
}

#[cfg(target_os = "macos")]
fn native_restore_error(
    error: crate::storage::apfs::RestoreExecutionError<CowshedError, CowshedError>,
) -> CowshedError {
    match error {
        crate::storage::apfs::RestoreExecutionError::Storage(error)
        | crate::storage::apfs::RestoreExecutionError::Activation { source: error, .. } => {
            native_storage_error(error)
        }
        crate::storage::apfs::RestoreExecutionError::Prepare(error)
        | crate::storage::apfs::RestoreExecutionError::Fence { source: error, .. } => error,
        crate::storage::apfs::RestoreExecutionError::PrepareCleanup { prepare, cleanup } => {
            CowshedError::integrity(
                format!("{prepare}; cleanup also failed: {cleanup}"),
                "cowshed doctor --json",
            )
        }
    }
}

#[cfg(target_os = "macos")]
fn native_storage_error(error: crate::storage::apfs::ApfsStorageError) -> CowshedError {
    match error {
        crate::storage::apfs::ApfsStorageError::Conflict(error) => {
            CowshedError::conflict(error.to_string(), "refresh workspace state and retry")
        }
        crate::storage::apfs::ApfsStorageError::GcPlanStale => CowshedError::conflict(
            "garbage-collection plan became stale",
            "preview garbage collection again and retry",
        ),
        crate::storage::apfs::ApfsStorageError::PendingPublication(path) => CowshedError::conflict(
            format!("restore publication is pending at {}", path.display()),
            "repair commitment/gateway evidence and retry restore",
        ),
        crate::storage::apfs::ApfsStorageError::MarkerMismatch(message)
        | crate::storage::apfs::ApfsStorageError::Host(message) => {
            CowshedError::integrity(message, "cowshed doctor --json")
        }
        other => {
            CowshedError::environment_missing(other.to_string(), "repair APFS storage and retry")
        }
    }
}

#[cfg(target_os = "macos")]
fn native_environment_error(
    error: crate::storage::bootstrap::native::NativeBootstrapError,
) -> CowshedError {
    match error {
        crate::storage::bootstrap::native::NativeBootstrapError::StorageSetupRequired {
            actions,
            hint,
        } => CowshedError::environment_missing(
            format!("cowshed storage setup is required: {}", actions.join("; ")),
            hint,
        ),
        error => {
            CowshedError::environment_missing(error.to_string(), "repair host storage and retry")
        }
    }
}

#[cfg(target_os = "macos")]
fn native_integrity_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(error.to_string(), "cowshed doctor --json")
}

#[cfg(target_os = "macos")]
fn native_finding(
    code: &str,
    severity: crate::api::dto::FindingSeverity,
    error: CowshedError,
) -> crate::api::dto::Finding {
    crate::api::dto::Finding {
        code: code.into(),
        severity,
        message: error.message,
        hint: error.hint,
        path: None,
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;

    fn repo_id(value: &str) -> RepoId {
        RepoId::parse(value).expect("valid repository identity")
    }

    fn remote(name: &str, url: &str) -> crate::git::RemoteUrl {
        crate::git::RemoteUrl {
            name: name.to_owned(),
            url: url.to_owned(),
        }
    }

    #[test]
    fn local_only_binding_requires_and_preserves_explicit_identity() {
        let requested = repo_id("acme/widget");
        let binding = binding_from_remotes(&[], Some(&requested)).expect("local-only binding");
        assert_eq!(
            binding.primary().expect("primary"),
            &crate::repository::BoundIdentity {
                repo_id: requested,
                remote_name: None,
                remote_url: None,
                primary: true,
            }
        );

        let error = binding_from_remotes(&[], None).expect_err("missing identity must fail");
        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        assert!(error.hint.contains("--repo-id"));
    }

    #[test]
    fn explicit_identity_must_match_a_normalized_remote_candidate() {
        let remotes = [remote(
            "origin",
            "https://user:secret@example.com/Acme/Widget.git?token=secret#fragment",
        )];
        let requested = repo_id("acme/widget");
        let binding =
            binding_from_remotes(&remotes, Some(&requested)).expect("matching explicit identity");
        let primary = binding.primary().expect("primary");
        assert_eq!(primary.repo_id, requested);
        assert_eq!(primary.remote_name.as_deref(), Some("origin"));
        assert_eq!(
            primary.remote_url.as_deref(),
            Some("https://example.com/Acme/Widget.git")
        );

        let error = binding_from_remotes(&remotes, Some(&repo_id("other/repo")))
            .expect_err("mismatching explicit identity must fail");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.message.contains("does not match any Git remote"));
    }

    #[test]
    fn distinct_remote_candidates_require_explicit_selection() {
        let remotes = [
            remote("origin", "https://example.com/acme/widget.git"),
            remote("upstream", "ssh://git@example.com/upstream/widget.git"),
        ];
        let error =
            binding_from_remotes(&remotes, None).expect_err("ambiguous identities must fail");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.hint.contains("--repo-id"));
        assert!(error.hint.contains("acme/widget"));
        assert!(error.hint.contains("upstream/widget"));
    }

    #[test]
    fn duplicate_same_identity_remotes_are_unambiguous_and_prefer_origin() {
        let remotes = [
            remote("backup", "ssh://git@mirror.example/acme/widget.git"),
            remote("origin", "https://example.com/acme/widget.git"),
            remote("upstream", "git://example.net/acme/widget.git"),
        ];
        let binding = binding_from_remotes(&remotes, None).expect("one normalized identity");
        let primary = binding.primary().expect("primary");
        assert_eq!(primary.repo_id, repo_id("acme/widget"));
        assert_eq!(primary.remote_name.as_deref(), Some("origin"));
        assert_eq!(
            primary.remote_url.as_deref(),
            Some("https://example.com/acme/widget.git")
        );
    }
}
