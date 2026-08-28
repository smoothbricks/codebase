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

#[cfg(target_os = "macos")]
use crate::api::dto::AbandonedWork;
#[cfg(target_os = "macos")]
use crate::api::dto::LandingCommits;
use crate::api::dto::{
    AdoptOptions, AttachOptions, CheckpointOptions, CheckpointQuota, CheckpointResult, CommandArg,
    CreateOptions, DoctorReport, EmptyResult, ExecRequest, GcOptions, GcReport, GitOid, GrantDelta,
    GrantSet, JobId, JobInfo, LandOptions, LandReport, MirrorInfo, PushOptions, PushReport,
    RebaseOptions, RemoveOptions, RemoveReport, RevisionResult, RunSandboxMode, StdinSource,
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

/// The branch a workspace's commits are expected to reach.
///
/// One constant for two questions that must never disagree: where `land` merges by default, and
/// which branch `rm` requires to contain a workspace's head before destroying its object store.
/// If they diverged, `land` would satisfy a check `rm` does not make.
pub const DEFAULT_LANDING_BRANCH: &str = "main";

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
    async fn rename(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot>;
    /// Move the project's checkout to a new path, keeping every record of where it lives in step.
    async fn move_checkout(&mut self, destination: PathBuf) -> Result<WorkspaceSnapshot>;
    async fn attach(&mut self, workspace: WorkspaceName, options: AttachOptions) -> Result<()>;
    async fn detach(&mut self, workspace: WorkspaceName) -> Result<()>;
    async fn resize(
        &mut self,
        workspace: WorkspaceName,
        capacity: String,
    ) -> Result<crate::api::dto::ResizeResult>;
    async fn checkpoint(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: Option<WorkspaceIncarnation>,
        options: CheckpointOptions,
    ) -> Result<CheckpointResult>;
    async fn restore(&mut self, workspace: WorkspaceName, label: String) -> Result<()>;
    async fn remove(
        &mut self,
        workspace: WorkspaceName,
        options: RemoveOptions,
    ) -> Result<RemoveReport>;
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

fn continuity_from_environment() -> Result<crate::storage::audit::ContinuityAudit> {
    crate::storage::audit::ContinuityAudit::from_environment().map_err(|error| {
        CowshedError::usage(
            error.to_string(),
            "unset COWSHED_CONTINUITY_AUDIT or set it to arrow|off",
        )
    })
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
            continuity_from_environment()?,
        )
        .await
    }

    /// Opens the production runtime without storage provisioning authority.
    ///
    /// Ordinary commands and background services must use this entrypoint. Missing or incorrectly
    /// mounted storage fails closed without creating or mounting anything. The audit sink is the
    /// standalone default (`COWSHED_CONTINUITY_AUDIT`, §[`crate::storage::audit`]).
    pub async fn open_existing(project_root: impl AsRef<Path>) -> Result<Self> {
        Self::open_existing_with_audit(project_root, continuity_from_environment()?).await
    }

    /// [`Self::open_existing`] with the host's own audit sink — the entrypoint a supervising
    /// runtime uses to route controller audit records into its durable log instead of Arrow files.
    pub async fn open_existing_with_audit(
        project_root: impl AsRef<Path>,
        continuity: crate::storage::audit::ContinuityAudit,
    ) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly,
            None,
            continuity,
        )
        .await
    }

    async fn open_native(
        project_root: &Path,
        mode: crate::storage::bootstrap::native::NativeBootstrapMode,
        requested_repo_id: Option<RepoId>,
        continuity: crate::storage::audit::ContinuityAudit,
    ) -> Result<Self> {
        #[cfg(target_os = "macos")]
        {
            let host = NativeProjectRuntimeHost::open(
                project_root,
                mode,
                requested_repo_id.as_ref(),
                continuity,
            )
            .await?;
            Self::start(host).await
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (project_root, mode, requested_repo_id, continuity);
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
            "workspace.attach" => self.workspace_attach(request).await,
            "workspace.grants" => self.workspace_grants(request).await,
            "coordinator.adopt" => self.coordinator_adopt(request).await,
            "coordinator.create" => self.coordinator_create(request).await,
            "coordinator.fork" => self.coordinator_fork(request).await,
            "coordinator.rename" => self.coordinator_rename(request).await,
            "coordinator.moveCheckout" => self.coordinator_move_checkout(request).await,
            "coordinator.grant" => self.coordinator_grant(request, false).await,
            "coordinator.revoke" => self.coordinator_grant(request, true).await,
            "coordinator.rebase" => self.coordinator_rebase(request).await,
            "coordinator.land" => self.coordinator_land(request).await,
            "coordinator.restore" => self.coordinator_restore(request).await,
            "coordinator.resize" => self.coordinator_resize(request).await,
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
        // The caller names where it is; the descriptor names the project. They are the same
        // directory when the caller stands in main's checkout, and different ones whenever it
        // stands in a workspace mount — which is the arrangement `cowshed rebase` and friends
        // support by inferring their workspace from the cwd. String identity therefore refused the
        // very callers the inference exists for, so the question asked is the one that matters:
        // does the requested path belong to this project? A path resolving to the bound root does,
        // and so does a workspace mount whose marker records that root.
        let bound = self.host.descriptor().git_root.clone();
        let belongs = names_one_root(&requested, &bound)
            || marker_project_root(&requested)
                .await?
                .is_some_and(|root| names_one_root(&root, &bound));
        if !belongs {
            return Err(CowshedError::conflict(
                format!(
                    "project path {} does not belong to the bound project at {}",
                    requested.display(),
                    bound.display()
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

    async fn coordinator_rename(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: ForkParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshot = self.host.rename(params.source, params.destination).await?;
        workspace_response(&snapshot)
    }

    async fn coordinator_move_checkout(
        &mut self,
        request: RouterRequest,
    ) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: MoveCheckoutParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let snapshot = self.host.move_checkout(params.destination).await?;
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

    async fn coordinator_resize(&mut self, request: RouterRequest) -> Result<RouterResponse> {
        require_coordinator(request.authority())?;
        let params: ResizeParams = decode_params(request.params(), request.method())?;
        self.require_repo(&params.repo_id)?;
        let result = self.host.resize(params.workspace, params.capacity).await?;
        json_response(result)
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
        let report = self.host.remove(params.workspace, params.options).await?;
        json_response(report)
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
struct MoveCheckoutParams {
    repo_id: RepoId,
    destination: PathBuf,
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
struct ResizeParams {
    repo_id: RepoId,
    workspace: WorkspaceName,
    capacity: String,
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
    substrate_config: crate::storage::apfs::ApfsSubstrateConfig,
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
    lifecycle_intents_path: PathBuf,
    lifecycle_intents: crate::storage::recovery::LifecycleIntentJournal,
}

#[cfg(target_os = "macos")]
struct PortGrantReservation {
    grants: GrantSet,
    marker: PathBuf,
}

#[cfg(target_os = "macos")]
impl Drop for PortGrantReservation {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.marker);
    }
}

#[cfg(target_os = "macos")]
fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 does not deliver a signal; it only asks the kernel whether the PID exists.
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(target_os = "macos")]
fn claim_port_block(staging: &Path, base: u16) -> std::io::Result<Option<PathBuf>> {
    use std::os::unix::fs::symlink;

    std::fs::create_dir_all(staging)?;
    let marker = staging.join(format!("port-{base}.reservation"));
    let owner = std::process::id().to_string();
    for _ in 0..2 {
        match symlink(&owner, &marker) {
            Ok(()) => return Ok(Some(marker)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = std::fs::read_link(&marker)?;
                let existing = existing
                    .to_str()
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid port reservation marker {}", marker.display()),
                        )
                    })?;
                if process_is_alive(existing) {
                    return Ok(None);
                }
                std::fs::remove_file(&marker)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

#[cfg(target_os = "macos")]
fn remove_terminal_storage_tree(path: &Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect terminal project storage {}: {error}",
                    path.display()
                ),
                "check controller storage permissions and retry",
            ));
        }
    };
    if metadata.file_type().is_dir() {
        let entries = std::fs::read_dir(path).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot enumerate terminal project storage {}: {error}",
                    path.display()
                ),
                "check controller storage permissions and retry",
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot read terminal project storage {}: {error}",
                        path.display()
                    ),
                    "check controller storage permissions and retry",
                )
            })?;
            remove_terminal_storage_tree(&entry.path())?;
        }
        std::fs::remove_dir(path).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot remove terminal project directory {}: {error}",
                    path.display()
                ),
                "check controller storage permissions and retry",
            )
        })
    } else if metadata.file_type().is_file()
        && metadata.len() == 0
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".lock"))
    {
        std::fs::remove_file(path).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot remove terminal project lock {}: {error}",
                    path.display()
                ),
                "check controller storage permissions and retry",
            )
        })
    } else {
        Err(CowshedError::integrity(
            format!(
                "terminal project storage contains an unexpected retained artifact: {}",
                path.display()
            ),
            "run cowshed doctor --json before removing the project binding",
        ))
    }
}

#[cfg(target_os = "macos")]
fn clean_terminal_project_storage(project_root: &Path, binding: &Path) -> Result<()> {
    for name in [".staging", "checkpoints", "sessions"] {
        remove_terminal_storage_tree(&project_root.join(name))?;
    }
    for entry in std::fs::read_dir(project_root).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot enumerate terminal project root {}: {error}",
                project_root.display()
            ),
            "check controller storage permissions and retry",
        )
    })? {
        let path = entry
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot read terminal project root {}: {error}",
                        project_root.display()
                    ),
                    "check controller storage permissions and retry",
                )
            })?
            .path();
        if path != binding
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".lock"))
        {
            remove_terminal_storage_tree(&path)?;
        }
    }
    Ok(())
}

#[cfg(any(test, target_os = "macos"))]
fn verified_recovery_facts<'a>(
    facts: &'a [crate::storage::lifecycle::StorageFact],
    pending: &[crate::storage::apfs::PendingPublicationFact],
) -> Vec<&'a crate::storage::lifecycle::StorageFact> {
    let pending_destinations = pending
        .iter()
        .map(|fact| fact.destination_incarnation.clone())
        .collect::<std::collections::BTreeSet<_>>();
    facts
        .iter()
        .filter(|fact| !pending_destinations.contains(fact.workspace.incarnation()))
        .collect()
}

#[cfg(target_os = "macos")]
impl NativeProjectRuntimeHost {
    async fn open(
        project_root: &Path,
        bootstrap_mode: crate::storage::bootstrap::native::NativeBootstrapMode,
        requested_repo_id: Option<&RepoId>,
        continuity: crate::storage::audit::ContinuityAudit,
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
        let existing_only = matches!(
            &bootstrap_mode,
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly
        );
        let origin = if existing_only {
            workspace_origin_from_marker(&git_root).await?
        } else {
            None
        };
        // A session marker identifies the project from store authority. Its recorded checkout may
        // be a missing direct mount, so resolving that identity must happen before replacing the
        // invocation repository handle with one rooted at the recorded checkout.
        let session_project = if existing_only {
            project_binding_from_workspace_origin(
                bootstrap.roots().store(),
                &git_root,
                origin.as_ref(),
            )
            .await?
        } else {
            None
        };
        let mut binding_repo_id = if existing_only {
            origin.as_ref().map(|origin| origin.repo_id.clone())
        } else {
            requested_repo_id.cloned()
        };
        // Where the caller stands and where the project is checked out are two different facts,
        // and only the first is what Git discovery reports. A coordinator verb invoked from inside
        // a session workspace discovers that workspace's mount — a standalone repository in its own
        // right — while everything downstream (the binding's remotes, the substrate's checkout
        // path, the recorded-project-root agreement every enumeration checks) means the project's
        // checkout. Every marker records that checkout, so when the two differ the marker is the
        // authority and the invocation root is left behind here.
        let (git_root, git) = match origin.as_ref() {
            Some(origin) if !names_one_root(&origin.project_root, &git_root) => {
                let root = origin.project_root.clone();
                let git = crate::git::GitRepository::from_root(&root);
                (root, git)
            }
            _ => (git_root, git),
        };
        if existing_only && binding_repo_id.is_none() {
            let storage = crate::storage::bootstrap::ValidatedHostStorage::new(
                bootstrap.home().to_owned(),
                bootstrap.roots().clone(),
            );
            binding_repo_id = crate::gateway_inventory::NativeGatewayInventory::new(storage)
                .repository_for_project_root(&git_root)
                .await
                .map_err(native_integrity_error)?;
        }
        let (repo_id, layout, binding) = if let Some((repo_id, layout, binding)) = session_project {
            (repo_id, layout, binding)
        } else {
            let candidate = binding_from_git(&git, binding_repo_id.as_ref()).await?;
            let repo_id = candidate
                .primary()
                .map_err(native_integrity_error)?
                .repo_id
                .clone();
            let layout = crate::storage::StorageLayout::new(bootstrap.roots().store(), &repo_id)
                .map_err(native_integrity_error)?;
            let binding = load_or_validate_binding(&layout, candidate, &git).await?;
            (repo_id, layout, binding)
        };
        // Every resolver that answers "where does main mount" reads this one value, so it is
        // resolved once here, from durable project state, and never inferred per call site.
        let checkout_layout = layout.checkout_layout().map_err(native_integrity_error)?;
        let config = crate::storage::apfs::ApfsSubstrateConfig::new(
            bootstrap.roots().store(),
            bootstrap.roots().caches(),
            &git_root,
            checkout_layout,
            crate::apfs::ApfsCaseSensitivity::Sensitive,
        );
        let host = crate::storage::apfs::native::MacOsApfsExecutionHost::new(
            crate::apfs::SystemCommandRunner,
            config.clone(),
        )
        .map_err(native_storage_error)?;
        let lifecycle_intents_path = layout
            .project()
            .project_root
            .join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE);
        let recovery_intents_path = lifecycle_intents_path.clone();
        let recovery_config = config.clone();
        let recovery_repo = repo_id.clone();
        let (host, facts, pending, lifecycle_intents) =
            crate::storage::lifecycle::dispatch_blocking(move || {
                let lifecycle_intents =
                    crate::storage::recovery::LifecycleIntentJournal::load(&recovery_intents_path)?;
                host.recover_pending(&recovery_config, &[])
                    .map_err(native_storage_error)?;
                let facts = host.list(&recovery_repo).map_err(native_storage_error)?;
                let pending = host
                    .pending_publications(&recovery_repo)
                    .map_err(native_storage_error)?;
                Ok::<_, CowshedError>((host, facts, pending, lifecycle_intents))
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("APFS recovery task failed: {error}"))
            })??;
        let retired_project_root = layout.project().project_root.clone();
        let retired_repo = repo_id.clone();
        let retired = crate::storage::lifecycle::dispatch_blocking(move || {
            native_retired_refs(&retired_project_root, &retired_repo)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("retired workspace recovery task failed: {error}"))
        })??;
        let verified_facts = verified_recovery_facts(&facts, &pending);
        if let Some(origin) = origin.as_ref() {
            validate_workspace_origin_against_inventory(origin, &verified_facts)?;
        }
        // Authority is the inventory itself: an incarnation that is both an active storage fact
        // and a retired (trashed) one is a host-side integrity fault, found here in one pass —
        // no log replay has anything to add to what the images say.
        {
            let retired_incarnations = retired
                .iter()
                .map(|fact| fact.workspace().incarnation())
                .collect::<std::collections::BTreeSet<_>>();
            if let Some(conflict) = verified_facts
                .iter()
                .find(|fact| retired_incarnations.contains(fact.workspace.incarnation()))
            {
                return Err(CowshedError::integrity(
                    format!(
                        "active storage fact references a retired workspace incarnation {}",
                        conflict.workspace.incarnation()
                    ),
                    "cowshed doctor --json",
                ));
            }
        }
        let telemetry_root = bootstrap.roots().store().join("telemetry");
        let mut commitments = super::supervisor::CommitmentPublisher::open(
            &telemetry_root,
            continuity,
            ROUTER_CAPACITY,
        )?;
        for publication in &pending {
            use super::supervisor::{CommitmentDraft, CommitmentSink};

            commitments
                .record(CommitmentDraft::Restore {
                    repo_id: repo_id.clone(),
                    source_checkpoint: publication.source_checkpoint.clone(),
                    source_incarnation: publication.source_incarnation.clone(),
                    replaced_incarnation: publication.replaced_incarnation.clone(),
                    destination_incarnation: publication.destination_incarnation.clone(),
                })
                .await?;
            host.activate_restored_metadata(&publication.image)
                .map_err(native_storage_error)?;
        }
        let substrate = crate::storage::apfs::ApfsSubstrate::new(config.clone(), host);
        for retirement in retired {
            // Trash reclamation is best effort; the retirement is already a fact of the inventory.
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
            substrate_config: config,
            substrate,
            commitments,
            supervisors: std::collections::BTreeMap::new(),
            sessions: std::collections::BTreeMap::new(),
            home,
            telemetry_root,
            lifecycle_intents_path,
            lifecycle_intents,
        })
    }
    async fn replace_lifecycle_intents(
        &mut self,
        next: crate::storage::recovery::LifecycleIntentJournal,
    ) -> Result<()> {
        let path = self.lifecycle_intents_path.clone();
        self.lifecycle_intents = crate::storage::lifecycle::dispatch_blocking(move || {
            next.persist(&path)?;
            Ok::<_, CowshedError>(next)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("lifecycle intent persistence task failed: {error}"))
        })??;
        Ok(())
    }

    async fn begin_lifecycle_intent(
        &mut self,
        operation: crate::storage::recovery::LifecycleIntent,
    ) -> Result<()> {
        let mut next = self.lifecycle_intents.clone();
        next.begin(operation);
        self.replace_lifecycle_intents(next).await
    }
    async fn mark_lifecycle_intent_mutating(&mut self, workspace: &WorkspaceName) -> Result<()> {
        let mut next = self.lifecycle_intents.clone();
        next.mark_mutating(workspace)?;
        self.replace_lifecycle_intents(next).await
    }
    async fn discard_prepared_retire_intent(&mut self, workspace: &WorkspaceName) -> Result<()> {
        let mut next = self.lifecycle_intents.clone();
        if !next.discard_prepared_retirement(workspace) {
            return Err(CowshedError::internal(format!(
                "prepared retirement for {workspace} disappeared during recovery"
            )));
        }
        self.replace_lifecycle_intents(next).await
    }

    async fn complete_lifecycle_intent(
        &mut self,
        workspace: &WorkspaceName,
        completion: crate::storage::recovery::LifecycleIntentCompletion,
    ) -> Result<()> {
        let mut next = self.lifecycle_intents.clone();
        next.complete(workspace, completion)?;
        self.replace_lifecycle_intents(next).await
    }

    fn completed_workspace_intent(
        &self,
        operation: &crate::storage::recovery::LifecycleIntent,
    ) -> Option<&WorkspaceIncarnation> {
        let record = self.lifecycle_intents.get(operation.target())?;
        if record.operation != *operation {
            return None;
        }
        match record.completion.as_ref()? {
            crate::storage::recovery::LifecycleIntentCompletion::Workspace(incarnation) => {
                Some(incarnation)
            }
            crate::storage::recovery::LifecycleIntentCompletion::Retire(_) => None,
        }
    }

    fn completed_retire_intent(
        &self,
        operation: &crate::storage::recovery::LifecycleIntent,
    ) -> Option<&RemoveReport> {
        let record = self.lifecycle_intents.get(operation.target())?;
        if record.operation != *operation {
            return None;
        }
        match record.completion.as_ref()? {
            crate::storage::recovery::LifecycleIntentCompletion::Retire(report) => Some(report),
            crate::storage::recovery::LifecycleIntentCompletion::Workspace(_) => None,
        }
    }

    /// Finishes create/fork/adopt work and authorized retire mutations a crash left pending, then
    /// records the exact result so a later start does not repeat it. A prepared retirement has not
    /// mutated anything and is discarded: it may be the residue of a safety refusal, not durable
    /// authorization to delete on every later command. Reports whether recovery mutated images or
    /// mounts, so a caller can discard an inventory read only when necessary.
    async fn recover_lifecycle_intents(&mut self) -> Result<bool> {
        use crate::storage::recovery::{
            LifecycleIntent, LifecycleIntentCompletion, LifecycleIntentPhase,
        };

        let pending = self
            .lifecycle_intents
            .records()
            .filter(|(_, record)| record.completion.is_none())
            .map(|(_, record)| record.clone())
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(false);
        }
        async {
            for record in pending {
                let phase = record.phase;
                match record.operation {
                    LifecycleIntent::Adopt { options } => match self.current(&main_name()).await {
                        Ok(current) => {
                            self.complete_lifecycle_intent(
                                current.derived.workspace.name(),
                                LifecycleIntentCompletion::Workspace(
                                    current.derived.workspace.incarnation().clone(),
                                ),
                            )
                            .await?;
                        }
                        Err(error) if error.code == ErrorCode::NotFound => {
                            self.adopt(options).await?;
                        }
                        Err(error) => return Err(error),
                    },
                    LifecycleIntent::Create { workspace, options } => {
                        match self.current(&workspace).await {
                            Ok(current) => {
                                self.complete_lifecycle_intent(
                                    &workspace,
                                    LifecycleIntentCompletion::Workspace(
                                        current.derived.workspace.incarnation().clone(),
                                    ),
                                )
                                .await?;
                            }
                            Err(error) if error.code == ErrorCode::NotFound => {
                                self.create(workspace, options).await?;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    LifecycleIntent::Fork {
                        source,
                        destination,
                    } => match self.current(&destination).await {
                        Ok(current) => {
                            self.complete_lifecycle_intent(
                                &destination,
                                LifecycleIntentCompletion::Workspace(
                                    current.derived.workspace.incarnation().clone(),
                                ),
                            )
                            .await?;
                        }
                        Err(error) if error.code == ErrorCode::NotFound => {
                            self.fork(source, destination).await?;
                        }
                        Err(error) => return Err(error),
                    },
                    LifecycleIntent::Retire {
                        workspace,
                        options: _,
                    } if phase == LifecycleIntentPhase::Prepared => {
                        match self.current(&workspace).await {
                            // Existing authoritative state proves retirement never published.
                            // Discarding this request prevents a refusal from becoming a deferred
                            // deletion on every later command.
                            Ok(_) => {
                                self.discard_prepared_retire_intent(&workspace).await?;
                            }
                            // Absence is the publication fence: an older process crossed it but
                            // died before recording the result, so retain idempotent completion.
                            Err(error) if error.code == ErrorCode::NotFound => {
                                self.complete_lifecycle_intent(
                                    &workspace,
                                    LifecycleIntentCompletion::Retire(RemoveReport::default()),
                                )
                                .await?;
                            }
                            // An unreadable target is not evidence either way. Fail closed rather
                            // than throwing away the only recovery record.
                            Err(error) => return Err(error),
                        }
                    }
                    LifecycleIntent::Retire { workspace, options } => {
                        match self.current(&workspace).await {
                            Ok(_) => {
                                self.remove(workspace, options).await?;
                            }
                            Err(error) if error.code == ErrorCode::NotFound => {
                                self.complete_lifecycle_intent(
                                    &workspace,
                                    LifecycleIntentCompletion::Retire(RemoveReport::default()),
                                )
                                .await?;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            Ok(())
        }
        .await?;
        Ok(true)
    }

    async fn validate_binding(&self) -> Result<()> {
        let remotes = self.git.remotes().await?;
        validate_binding_against_remotes(&self.descriptor.binding, &remotes)
    }

    async fn authoritative(&self) -> Result<Vec<NativeWorkspace>> {
        self.authoritative_with_project_root_validation(ProjectRootValidation::Strict)
            .await
    }

    async fn authoritative_allowing_detached_main_relocation(
        &self,
    ) -> Result<Vec<NativeWorkspace>> {
        self.authoritative_with_project_root_validation(
            ProjectRootValidation::AllowDetachedMainRelocation,
        )
        .await
    }

    async fn authoritative_with_project_root_validation(
        &self,
        root_validation: ProjectRootValidation,
    ) -> Result<Vec<NativeWorkspace>> {
        use crate::storage::lifecycle::Substrate;

        let derived = self
            .substrate
            .list(&self.descriptor.repo_id)
            .await
            .map_err(native_storage_error)?;
        let layout = self.layout.clone();
        let project_root = self.descriptor.git_root.clone();
        let checkout_layout = self.substrate_config.checkout_layout;
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
                    validate_workspace_controller_root(
                        &derived,
                        &metadata,
                        &project_root,
                        checkout_layout,
                        root_validation,
                    )?;
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

    fn workspace_mount_path(&self, workspace: &WorkspaceName) -> Result<PathBuf> {
        // Main's mount follows the project's checkout layout; every other workspace mounts under
        // `mnt/` in both layouts.
        if workspace.is_main() && self.substrate_config.checkout_layout.mounts_at_checkout() {
            return Ok(self.substrate_config.checkout_path.clone());
        }
        self.layout
            .workspace_mount(workspace)
            .map_err(native_integrity_error)
    }

    /// Give `workspace` the stable mount path of `slot`.
    ///
    /// Recorded before the workspace's first mount, because the record is what every mount path
    /// derivation reads: binding a workspace that is already mounted would leave the live volume at
    /// one path and the whole controller looking at another.
    async fn bind_slot(
        &self,
        workspace: &WorkspaceName,
        slot: crate::metadata::SlotId,
    ) -> Result<()> {
        let layout = self.layout.clone();
        let workspace = workspace.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let mut bindings = layout.slot_bindings()?;
            bindings.bind(slot, workspace)?;
            layout.record_slot_bindings(&bindings)?;
            Ok::<_, crate::storage::StorageLayoutError>(())
        })
        .await
        .map_err(|error| CowshedError::internal(format!("slot binding task failed: {error}")))?
        .map_err(slot_binding_error)
    }

    /// Vacate whatever slot `workspace` held, reporting it. Idempotent: an unbound workspace is
    /// already in the desired state.
    async fn release_slot(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Option<crate::metadata::SlotId>> {
        let layout = self.layout.clone();
        let workspace = workspace.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let mut bindings = layout.slot_bindings()?;
            let released = bindings.release(&workspace);
            if released.is_some() {
                layout.record_slot_bindings(&bindings)?;
            }
            Ok::<_, crate::storage::StorageLayoutError>(released)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("slot release task failed: {error}")))?
        .map_err(native_integrity_error)
    }

    /// Where this project's durable record of the checkout path lives.
    ///
    /// The marker is read through main's mount, so this is only usable while main is mounted; the
    /// sidecar half is store-side and always reachable.
    fn checkout_record(
        &self,
        workspace: &NativeWorkspace,
    ) -> Result<crate::checkout::CheckoutRecord> {
        Ok(crate::checkout::CheckoutRecord {
            mount_point: self.workspace_mount_path(&main_name())?,
            image: self
                .layout
                .main_image(workspace.derived.workspace.format())
                .map_err(native_integrity_error)?
                .image()
                .to_path_buf(),
        })
    }

    /// Rebuild the substrate around a checkout that now lives somewhere else.
    ///
    /// `ApfsSubstrate` and `MacOsApfsExecutionHost` each capture the substrate config by value, and
    /// the substrate shares its copy with every clone it has handed out, so there is no in-place
    /// mutation that could not be observed half-applied. The whole triple — config, execution host,
    /// substrate — is therefore rebuilt and swapped at once. `&mut self` is what makes that safe:
    /// the project actor owns the runtime host exclusively, so the swap cannot race a concurrent
    /// operation, and the move transaction performs it at the one moment nothing is mounted.
    ///
    /// The Git repository handle and the descriptor's project root move with it. They are the same
    /// fact spelled three ways, and leaving any one behind would send the next operation to the old
    /// path.
    fn rebind_checkout(
        &mut self,
        checkout_path: &Path,
        checkout_layout: crate::metadata::CheckoutLayout,
    ) -> Result<()> {
        let config = self
            .substrate_config
            .rebind_checkout(checkout_path, checkout_layout);
        let host = crate::storage::apfs::native::MacOsApfsExecutionHost::new(
            crate::apfs::SystemCommandRunner,
            config.clone(),
        )
        .map_err(native_storage_error)?;
        self.substrate = crate::storage::apfs::ApfsSubstrate::new(config.clone(), host);
        self.substrate_config = config;
        self.descriptor.git_root = checkout_path.to_owned();
        self.git = crate::git::GitRepository::from_root(checkout_path);
        Ok(())
    }

    /// Bring every workspace's own record of the project into line with where the project is now.
    ///
    /// A workspace records the project in four independent places, and a relocation invalidates all
    /// four at once:
    ///
    /// * its **marker** (`.cowshed/workspace.json`) and its **detached sidecar**, both of which
    ///   name `projectRoot`. Rewriting only main's pair — which is all this used to do — is what
    ///   left every session of a relocated project naming a directory that had stopped being a
    ///   repository, and
    ///   what made `doctor` report "workspace marker identity does not match" with no remedy in
    ///   sight;
    /// * its **`main` remote**, or its **linked-worktree registration** when it is a git-worktree
    ///   workspace;
    /// * its **merge drivers**, whose absolute program paths die with the old checkout and take
    ///   every rebase in the project with them.
    ///
    /// Under direct mount main's mount *is* the checkout, so moving the checkout moves the URL
    /// every workspace fetches from and the gitdir every git-worktree workspace points at; under
    /// the symlink layout the mount never moves and the remote repair is the idempotent re-run
    /// `configure_main_remote` is built for. One code path covers both because the layout is
    /// exactly the thing `workspace_mount_path` already answers.
    ///
    /// A detached workspace has no reachable marker and no reachable config, so only its sidecar
    /// moves; `attach` finishes the pair. Every workspace is attempted before any failure is
    /// raised: a project half-repaired by a refusal in the middle is worse than one fully
    /// repaired except for the workspace that genuinely could not be written, and re-running the
    /// same verb is the remedy either way.
    async fn repair_workspace_records(&mut self, project_root: &Path) -> Result<()> {
        let main_mount = self.workspace_mount_path(&main_name())?;
        let mut failures = Vec::new();
        for workspace in self.authoritative().await? {
            let name = workspace.derived.workspace.name().clone();
            if let Err(error) = self
                .repair_one_workspace_record(&workspace, &name, &main_mount, project_root)
                .await
            {
                failures.push(format!("{name}: {}", error.message));
            }
        }
        if failures.is_empty() {
            return Ok(());
        }
        Err(CowshedError::integrity(
            format!(
                "could not repair every workspace's record of {}: {}",
                project_root.display(),
                failures.join("; ")
            ),
            "cowshed doctor --json",
        ))
    }

    async fn repair_one_workspace_record(
        &self,
        workspace: &NativeWorkspace,
        name: &WorkspaceName,
        main_mount: &Path,
        project_root: &Path,
    ) -> Result<()> {
        let record = crate::checkout::CheckoutRecord {
            mount_point: self.workspace_mount_path(name)?,
            image: workspace.image.clone(),
        };
        let mounted = matches!(
            workspace.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        );
        let rewrite_root = project_root.to_owned();
        crate::storage::lifecycle::dispatch_blocking(move || {
            if mounted {
                record.rewrite_project_root(&rewrite_root).map(|_| ())
            } else {
                record
                    .rewrite_detached_project_root(&rewrite_root)
                    .map(|_| ())
            }
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkout record task failed: {error}")))?
        .map_err(native_integrity_error)?;
        if !mounted {
            return Ok(());
        }
        let mount = self.workspace_mount_path(name)?;
        crate::git::GitRepository::from_root(&mount)
            .repair_merge_drivers()
            .await?;
        if name.is_main() {
            return Ok(());
        }
        if is_git_worktree(&workspace.metadata) {
            return repair_git_worktree_link(main_mount, &mount).await;
        }
        crate::git::GitRepository::from_root(&mount)
            .configure_main_remote(main_mount)
            .await
            .map(|_| ())
    }

    /// Converge the recorded checkout path onto where the checkout is actually observed.
    ///
    /// `mv` is the sanctioned front door for moving a checkout; this is the safety net under it. A
    /// user who moves a symlinked checkout by hand — or who reaches the project through a second
    /// alias — has broken nothing, because both spellings still resolve to main's volume. The
    /// record simply falls behind, and every later answer that quotes it (`doctor`, the gateway
    /// inventory's project-root lookup, a cold open from the checkout directory) quotes a path the
    /// user no longer uses.
    ///
    /// Convergence fires only when all of these hold, which together mean "the same checkout, spelt
    /// differently" and nothing else:
    ///
    /// - `observed` sits inside main's mount, so the caller really is in this project;
    /// - the checkout root above it resolves to main's mount, so it is a checkout and not some
    ///   deeper directory;
    /// - that root lies outside cowshed's own storage, so the mount path can never be mistaken for
    ///   the user's checkout under the symlink layout;
    /// - it differs from the record, so an agreeing record is never rewritten.
    ///
    /// The observed layout is recorded alongside the path: a checkout that is a symlink is the
    /// symlink layout by construction, and one that is the mountpoint is direct mount. Recording
    /// the observation rather than the previous belief is what keeps `mount_point()` — which reads
    /// the layout to decide whether main mounts at the checkout — answering truthfully afterwards.
    async fn converge_checkout_record(&mut self, observed: &Path) -> Result<()> {
        let main = main_name();
        let current = self.current(&main).await?;
        if !matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        ) {
            return Ok(());
        }
        let mount_point = self.workspace_mount_path(&main)?;
        let record = self.checkout_record(&current)?;
        let store_root = self.descriptor.store_root.clone();
        let observed = observed.to_owned();
        let probe_mount = mount_point.clone();
        let Some((checkout, layout)) = crate::storage::lifecycle::dispatch_blocking(move || {
            let checkout = crate::checkout::observed_checkout(&observed, &probe_mount)?;
            if checkout.starts_with(&store_root) {
                return None;
            }
            let layout = crate::checkout::observed_layout(&checkout);
            Some((checkout, layout))
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("observed checkout task failed: {error}"))
        })?
        else {
            return Ok(());
        };
        let converge_record = record.clone();
        let converge_checkout = checkout.clone();
        let changed = crate::storage::lifecycle::dispatch_blocking(move || {
            converge_record.rewrite_project_root(&converge_checkout)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkout record task failed: {error}")))?
        .map_err(native_integrity_error)?;
        if !changed && layout == self.substrate_config.checkout_layout {
            return Ok(());
        }
        let layout_record = self.layout.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            layout_record.record_checkout_layout(layout)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("layout record task failed: {error}")))?
        .map_err(native_integrity_error)?;
        self.rebind_checkout(&checkout, layout)?;
        // A hand-moved checkout invalidates every workspace's record of the project, not just
        // main's, so the convergence that repairs main's has to repair theirs in the same breath.
        // Reached only when the record actually changed: the guard above returns early otherwise,
        // so an agreeing project pays nothing for this.
        self.repair_workspace_records(&checkout).await
    }

    /// Refuse a checkout destination that cannot be moved onto, before anything is mutated.
    async fn validate_move_destination(
        &self,
        source: &Path,
        destination: &Path,
        retired_main_targets: &[PathBuf],
    ) -> Result<MoveDestination> {
        if !destination.is_absolute()
            || destination
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(CowshedError::usage(
                format!(
                    "{} is not an absolute, resolved path",
                    destination.display()
                ),
                "pass an absolute destination path with no `..` segments",
            ));
        }
        if destination == source {
            return Err(CowshedError::usage(
                format!("the checkout is already at {}", source.display()),
                "nothing to move; run cowshed doctor to see whether a workspace's records lag",
            ));
        }
        if destination.starts_with(&self.descriptor.store_root)
            || self.descriptor.store_root.starts_with(destination)
            || destination.starts_with(source)
        {
            return Err(CowshedError::usage(
                format!(
                    "{} overlaps cowshed storage or the current checkout",
                    destination.display()
                ),
                "choose a destination outside /private/cowshed/store and outside the current checkout",
            ));
        }
        let destination = destination.to_owned();
        let retired_main_targets = retired_main_targets.to_vec();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let state = classify_move_destination(&destination, &retired_main_targets)?;
            let parent = destination.parent().ok_or_else(|| {
                CowshedError::usage(
                    format!("{} has no parent directory", destination.display()),
                    "choose a destination inside an existing directory",
                )
            })?;
            if !parent.is_dir() {
                return Err(CowshedError::not_found(
                    format!("{} is not an existing directory", parent.display()),
                    "create the parent directory first",
                ));
            }
            Ok(state)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("checkout destination task failed: {error}"))
        })?
    }

    /// Detach main, rename its mountpoint directory, and leave the substrate rebound to the new
    /// path with nothing mounted.
    ///
    /// Split out because it is the half that has an inverse: if the rename fails, main is put back
    /// where it was and remounted, so a failed move is indistinguishable from one never attempted.
    async fn move_direct_mount(
        &mut self,
        current: &NativeWorkspace,
        source: &Path,
        destination: &Path,
    ) -> Result<()> {
        use crate::storage::lifecycle::{MountIntent, Substrate};

        self.stop_supervisor(&main_name()).await?;
        self.substrate
            .unmount(&current.derived.workspace)
            .await
            .map_err(native_storage_error)?;
        let rename_source = source.to_owned();
        let rename_destination = destination.to_owned();
        let renamed = crate::storage::lifecycle::dispatch_blocking(move || {
            std::fs::rename(&rename_source, &rename_destination).map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot move the checkout mountpoint to {}: {error}",
                        rename_destination.display()
                    ),
                    "choose a destination on the same filesystem as the current checkout",
                )
            })
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkout rename task failed: {error}")))?;
        if let Err(error) = renamed {
            // Nothing moved; put main back exactly where the caller found it.
            let _ = self
                .substrate
                .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                .await;
            let _ = self.ensure_supervisor(&main_name()).await;
            return Err(error);
        }
        Ok(())
    }

    /// Add `workspace`'s mount as a remote in main's repository, for `cowshed new --register`.
    ///
    /// Opt-in because it is host-side state that accumulates one entry per workspace: a
    /// coordinator minting and retiring all day would silt up the user's config.
    async fn register_workspace_in_main(&self, workspace: &WorkspaceName) -> Result<()> {
        let main_mount = self.workspace_mount_path(&main_name())?;
        let workspace_mount = self.workspace_mount_path(workspace)?;
        crate::git::GitRepository::from_root(&main_mount)
            .register_workspace_remote(workspace.as_str(), &workspace_mount)
            .await
    }

    /// Drop the host-side state `workspace` put in main's repository: its reverse remote, and its
    /// linked-worktree registration if it is a git-worktree workspace.
    ///
    /// Main being unmounted is not a failure: there is nothing to clean up in a repository that is
    /// not there, and `gc` re-runs this from the same revalidated retirement metadata that
    /// authorizes the rest of cleanup.
    async fn unregister_workspace_in_main(
        &self,
        workspace: &WorkspaceName,
        git_worktree: bool,
    ) -> Result<()> {
        if workspace.is_main() {
            return Ok(());
        }
        let main_mount = self.workspace_mount_path(&main_name())?;
        if !main_mount.join(".git").exists() {
            return Ok(());
        }
        let main = crate::git::GitRepository::from_root(&main_mount);
        main.unregister_workspace_remote(workspace.as_str()).await?;
        if git_worktree {
            main.unregister_linked_worktree(workspace.as_str()).await?;
        }
        Ok(())
    }

    /// Refuse a git-worktree operation while main is not mounted.
    ///
    /// The gitdir lives outside the workspace's volume, so with main detached the workspace has
    /// files and no repository: `git status` fails and everything built on it fails with it.
    /// Handing back a mount whose git is broken is worse than saying which command fixes it.
    async fn require_main_mounted_for_git_worktree(
        &mut self,
        workspace: &WorkspaceName,
    ) -> Result<()> {
        let main = self.current(&main_name()).await?;
        if matches!(
            main.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        ) {
            return Err(CowshedError::conflict(
                format!(
                    "git-worktree workspace {workspace} needs main mounted: its repository lives in main"
                ),
                "cowshed attach main",
            ));
        }
        Ok(())
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
                mount: self.workspace_mount_path(workspace.derived.workspace.name())?,
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
                landing: None,
            },
            grants: workspace.metadata.grants.clone(),
            lifecycle_revision: workspace.derived.workspace.revision().get(),
            topology_revision: workspace.derived.workspace.topology_revision().get(),
        })
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
        branch: Option<String>,
        forked_from: Option<WorkspaceName>,
        git_worktree: bool,
    ) -> Result<crate::storage::lifecycle::OperationIdentity> {
        Ok(crate::storage::lifecycle::OperationIdentity {
            project_root: self.descriptor.git_root.clone(),
            base_commit: self.git.head_oid().await?,
            created_at: utc_timestamp().await?,
            branch,
            forked_from,
            created_trace: uuid::Uuid::new_v4().simple().to_string(),
            grants,
            git_worktree,
        })
    }

    async fn removal_git_fence(
        &self,
        workspace: &NativeWorkspace,
    ) -> Result<NativeRemovalGitFence> {
        let root = current_snapshot_mount(self, workspace)?;
        let git = crate::git::GitRepository::from_root(root);
        let head = GitOid::new(git.head_oid().await?).map_err(native_integrity_error)?;
        Ok(NativeRemovalGitFence {
            incarnation: workspace.derived.workspace.incarnation().clone(),
            head,
            dirty: git.is_dirty().await?,
            in_progress: git.in_progress_operation().await?,
        })
    }

    /// Every gate a removal must pass, in the order that puts the cheapest refusal first.
    ///
    /// Answers `Some` when an authorized abandonment has commits to bundle. Run twice per removal —
    /// once before the supervisor stops and again on the revalidated fence — because both halves of
    /// the answer can move underneath a removal: the workspace can pick up work, and main can land
    /// or rewind it.
    async fn require_removal_safe(
        &self,
        workspace: &WorkspaceName,
        options: RemoveOptions,
        fence: &NativeRemovalGitFence,
    ) -> Result<Option<NativeLandedState>> {
        if workspace.is_main() {
            // Main's removal has no landed gate: main *is* the branch a session has to reach, and
            // its own preservation proof is the retained checkout or a remote ref, which the
            // `--restore` path checks. What remains here is transient state — and unlike a session,
            // main's is not overridable, because there is no fork of it to fall back on.
            Self::require_session_state_clean(workspace, fence)?;
            return Ok(None);
        }
        if !options.force {
            Self::require_session_state_clean(workspace, fence)?;
        }
        self.require_session_landed(workspace, fence, options.abandon)
            .await
    }

    /// Stop the supervisor, then prove the workspace is still the one that was checked.
    ///
    /// The gap between a safety decision and the deletion it authorizes is where a workspace can
    /// pick up a commit, so both the incarnation and the head are re-read *after* the only thing
    /// that could still be writing to the volume has stopped.
    async fn revalidated_removal_fence(
        &mut self,
        workspace: &WorkspaceName,
        initial: &NativeRemovalGitFence,
    ) -> Result<(NativeWorkspace, NativeRemovalGitFence)> {
        self.stop_supervisor(workspace).await?;
        let current = self.current(workspace).await?;
        Self::require_exact_incarnation(&current, &initial.incarnation)?;
        let fence = self.removal_git_fence(&current).await?;
        if fence.head != initial.head {
            return Err(removal_head_moved_refusal(
                workspace,
                &initial.head,
                &fence.head,
            ));
        }
        Ok((current, fence))
    }

    /// Drop the workspace's host-side registrations, then retire its image.
    ///
    /// Host-side state goes before the image does. A remote naming a trashed mount is a broken
    /// fetch in the user's own checkout — the one piece of this teardown that lives where they can
    /// see it.
    async fn finish_retirement(&mut self, current: NativeWorkspace) -> Result<()> {
        let workspace = current.derived.workspace.name().clone();
        let git_worktree = is_git_worktree(&current.metadata);
        self.unregister_workspace_in_main(&workspace, git_worktree)
            .await?;
        self.retire_workspace(current).await
    }

    /// Refuse a session removal whose workspace is in transient Git state.
    ///
    /// Transient means recoverable-by-hand: uncommitted edits, a half-finished merge. This is the
    /// class `--force` overrides, and the hint names only remedies that lose nothing — naming the
    /// override here is what taught coordinator scripts to reach for it by reflex.
    fn require_session_state_clean(
        workspace: &WorkspaceName,
        fence: &NativeRemovalGitFence,
    ) -> Result<()> {
        if let Some(operation) = fence.in_progress.as_deref() {
            return Err(removal_in_progress_refusal(workspace, operation));
        }
        if fence.dirty {
            return Err(removal_dirty_refusal(workspace));
        }
        Ok(())
    }

    /// Where a session's commits stand relative to the branch that has to hold them.
    ///
    /// The target tip is read out of *main's own repository* — the object store that survives this
    /// workspace — and never out of a `refs/remotes/*` cache inside the workspace, which is a
    /// clone-time snapshot that has been observed hundreds of commits stale. The comparison then
    /// runs inside the workspace with main's object store attached read-only, so main's commits are
    /// visible without fetching and without writing anything anywhere.
    ///
    /// Containment is by patch identity, not only by ancestry. That is the correction this gate
    /// needed: a workspace whose work reached main by squash-merge or a history rewrite is not an
    /// ancestor of anything, and demanding `--abandon` to retire it taught callers to pass a
    /// commit-destroying flag for a safe operation.
    async fn landed_state(
        &self,
        workspace: &WorkspaceName,
        head: &GitOid,
    ) -> Result<NativeLandedState> {
        let main_mount = self.workspace_mount_path(&main_name())?;
        let target = crate::landing::resolve_target(&main_mount, DEFAULT_LANDING_BRANCH).await;
        let mount = self.workspace_mount_path(workspace)?;
        Ok(NativeLandedState {
            branch: DEFAULT_LANDING_BRANCH.to_owned(),
            commits: crate::landing::measure_commits(&target, &mount, head.as_str()).await,
        })
    }

    /// Refuse a session removal that would destroy commits `main` does not contain.
    ///
    /// `--abandon` is the only authorization: `--force` covers transient state and deliberately
    /// stops there, so a script that carries `--force` to get past a stuck workspace cannot also
    /// delete work with no other home. Answers `Some` when the caller authorized an abandonment
    /// and there is genuinely something to abandon, so the caller can bundle it before deleting.
    async fn require_session_landed(
        &self,
        workspace: &WorkspaceName,
        fence: &NativeRemovalGitFence,
        abandon: bool,
    ) -> Result<Option<NativeLandedState>> {
        let landed = self.landed_state(workspace, &fence.head).await?;
        removal_landed_decision(workspace, &fence.head, landed, abandon)
    }

    /// Write the unlanded commits beside the image that is about to be trashed.
    ///
    /// Belt for a deliberate abandonment: the refs die with the image, so a bundle in the same
    /// trash directory as the retired sidecars is the only thing that keeps the commits fetchable
    /// afterwards. Written from the workspace's own repository, whose local `main` is main's tip at
    /// mint time and therefore a prerequisite main's repository still holds.
    async fn bundle_abandoned_work(
        &self,
        workspace: &NativeWorkspace,
        fence: &NativeRemovalGitFence,
        landed: NativeLandedState,
    ) -> Result<AbandonedWork> {
        let mount = current_snapshot_mount(self, workspace)?;
        let git = crate::git::GitRepository::from_root(&mount);
        // What was destroyed is the authoritative count, not the workspace's own view of
        // `main..HEAD`: its local `main` is a clone-time snapshot, so against a rewritten upstream
        // it reports hundreds of commits where four were actually lost. Without a measurement there
        // is nothing better than that local view, and over-counting is the safe direction to err in
        // a report about loss.
        let unlanded_commits = match &landed.commits {
            LandingCommits::Measured { unlanded, .. } => *unlanded,
            LandingCommits::Indeterminate { .. } => {
                git.commits_ahead(Some(landed.branch.as_str()), "HEAD")
                    .await?
            }
        };
        let trash = self
            .layout
            .project()
            .sessions
            .join(crate::storage::recovery::TRASH_NAMESPACE);
        let bundle = trash.join(format!(
            "{}-{}.bundle",
            workspace.derived.workspace.name().as_str(),
            fence.head
        ));
        let directory = trash.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            std::fs::create_dir_all(&directory).map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot create the retirement trash directory {}: {error}",
                        directory.display()
                    ),
                    "repair the cowshed store and retry",
                )
            })
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("trash directory task failed: {error}"))
        })??;
        // The bundle range stays the workspace's own `main..HEAD`, deliberately: its local `main`
        // is an ancestor of main's own, so main's repository holds the prerequisite this thin
        // bundle names, and a stale local `main` only makes the bundle carry more than it must.
        // Naming the authoritative tip instead would need main's object store attached, which is
        // one more thing that can fail on the path that exists to lose nothing.
        //
        // `HEAD`, not the fence oid, and the difference is load-bearing: `git bundle create` names
        // the bundle's contents after the *refs* in its rev range, so a range whose tip is a raw
        // oid produces a bundle with no refs — which git rejects as empty. The fence has already
        // proved HEAD is exactly `fence.head`, so the ref spelling is the same commit.
        git.bundle_commits(&bundle, Some(landed.branch.as_str()), "HEAD")
            .await?;
        Ok(AbandonedWork {
            head: fence.head.clone(),
            target_branch: landed.branch,
            target_head: landed.commits.target_head().cloned(),
            unlanded_commits,
            bundle,
        })
    }

    async fn require_main_restore_safe(
        &self,
        workspace: &NativeWorkspace,
        pre_cowshed_checkout: &Path,
    ) -> Result<()> {
        let fence = self.removal_git_fence(workspace).await?;
        if fence.dirty || fence.in_progress.is_some() {
            return Err(CowshedError::conflict(
                "main has uncommitted or in-progress Git work",
                "commit or discard the changes, then retry",
            ));
        }
        let current_git =
            crate::git::GitRepository::from_root(current_snapshot_mount(self, workspace)?);
        let retained_git = crate::git::GitRepository::discover(pre_cowshed_checkout)
            .await
            .map_err(|_| {
                CowshedError::conflict(
                    "retained pre-cowshed checkout cannot prove main commit preservation",
                    "restore the exact retained checkout, or push main to its remote, then retry",
                )
            })?;
        let preserved_locally = retained_git
            .commit_is_preserved(fence.head.as_str())
            .await?;
        let preserved_remotely = current_git
            .commit_is_remote_preserved(fence.head.as_str())
            .await?;
        if !preserved_locally && !preserved_remotely {
            return Err(CowshedError::conflict(
                format!(
                    "main head {} is not preserved by the retained checkout or a remote ref",
                    fence.head
                ),
                "push main to its remote so its commits survive, then retry",
            ));
        }
        Ok(())
    }

    async fn verify_checkout_identity(&self, path: &Path, description: &str) -> Result<()> {
        let path_metadata = tokio::fs::symlink_metadata(path).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} is not the exact retained checkout directory"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        let main_mount =
            self.workspace_mount_path(&WorkspaceName::new("main").expect("fixed main"))?;
        let resolved =
            resolve_checkout_identity_path(path, &path_metadata, &main_mount, description).await?;
        let path = resolved.as_path();
        let git = crate::git::GitRepository::discover(path)
            .await
            .map_err(|_| {
                CowshedError::conflict(
                    format!("{description} is not the retained standalone Git checkout"),
                    "restore the exact .pre-cowshed tree or move the collision aside",
                )
            })?;
        let candidate_root = tokio::fs::canonicalize(path).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} cannot be resolved as an exact checkout root"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        let discovered_root = tokio::fs::canonicalize(git.root()).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} has no resolvable Git root"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        if candidate_root != discovered_root {
            return Err(CowshedError::conflict(
                format!("{description} is nested inside another checkout"),
                "restore the exact .pre-cowshed checkout root and retry",
            ));
        }
        let binding = binding_from_git(&git, Some(&self.descriptor.repo_id)).await?;
        if binding.primary().map_err(native_integrity_error)?.repo_id != self.descriptor.repo_id {
            return Err(CowshedError::conflict(
                format!("{description} belongs to a different repository"),
                "move the unrelated path aside and retry",
            ));
        }
        Ok(())
    }

    async fn remove_project_binding_after_restore(&self) -> Result<()> {
        let path = self.layout.project().repository_binding.clone();
        let expected = self.descriptor.binding.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let metadata = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(error) => {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "cannot inspect repository binding {}: {error}",
                            path.display()
                        ),
                        "check controller storage permissions and retry",
                    ));
                }
            };
            if !metadata.file_type().is_file() {
                return Err(CowshedError::integrity(
                    format!(
                        "repository binding is not a regular file: {}",
                        path.display()
                    ),
                    "move the collision aside and retry",
                ));
            }
            let actual = crate::metadata::read_json::<RepositoryBinding>(&path)
                .map_err(native_integrity_error)?;
            if actual != expected {
                return Err(CowshedError::integrity(
                    "repository binding changed during adoption rollback",
                    "restore the exact binding and retry",
                ));
            }
            let parent = path
                .parent()
                .ok_or_else(|| CowshedError::internal("repository binding has no parent"))?;
            clean_terminal_project_storage(parent, &path)?;
            std::fs::remove_file(&path).map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot remove repository binding {}: {error}",
                        path.display()
                    ),
                    "check controller storage permissions and retry",
                )
            })?;
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    CowshedError::environment_missing(
                        format!(
                            "cannot sync repository binding directory {}: {error}",
                            parent.display()
                        ),
                        "check controller storage permissions and retry",
                    )
                })?;
            match std::fs::remove_dir(parent) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => Ok(()),
                Err(error) => Err(CowshedError::environment_missing(
                    format!(
                        "cannot remove terminal project directory {}: {error}",
                        parent.display()
                    ),
                    "check controller storage permissions and retry",
                )),
            }
        })
        .await
        .map_err(|error| CowshedError::internal(format!("binding cleanup task failed: {error}")))?
    }

    async fn adopt_rollback_state(
        &self,
        workspace: &NativeWorkspace,
        pre_cowshed_checkout: &Path,
    ) -> Result<NativeAdoptRollbackState> {
        let source = &self.descriptor.git_root;
        let pre_exists = tokio::fs::symlink_metadata(pre_cowshed_checkout)
            .await
            .map(|_| true)
            .or_else(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(CowshedError::environment_missing(
                        format!(
                            "cannot inspect retained checkout {}: {error}",
                            pre_cowshed_checkout.display()
                        ),
                        "check parent-directory permissions and retry",
                    ))
                }
            })?;
        let detached = matches!(
            workspace.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        );
        if !pre_exists {
            if !detached {
                return Err(CowshedError::conflict(
                    format!(
                        "retained checkout {} is missing",
                        pre_cowshed_checkout.display()
                    ),
                    "restore the exact .pre-cowshed tree before retrying adoption rollback",
                ));
            }
            self.verify_checkout_identity(source, "restored project checkout")
                .await?;
            return Ok(NativeAdoptRollbackState::Complete);
        }

        match self
            .verify_checkout_identity(pre_cowshed_checkout, "retained .pre-cowshed checkout")
            .await
        {
            Ok(()) if detached => {
                if self
                    .verify_checkout_identity(source, "canonical project path")
                    .await
                    .is_ok()
                {
                    return Err(CowshedError::conflict(
                        "canonical project path contains a checkout while the retained checkout still exists",
                        "move the unrelated canonical-path checkout aside and retry",
                    ));
                }
                Ok(NativeAdoptRollbackState::Retained)
            }
            Ok(()) => Ok(NativeAdoptRollbackState::Retained),
            Err(retained_error) if detached => {
                if self
                    .verify_checkout_identity(source, "restored project checkout")
                    .await
                    .is_ok()
                {
                    Ok(NativeAdoptRollbackState::Swapped)
                } else {
                    Err(retained_error)
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn fresh_grants(&self) -> Result<PortGrantReservation> {
        let roots = crate::storage::bootstrap::CanonicalRoots::global();
        // NativeProjectRuntimeHost is created only after ExistingOnly/Provision bootstrap has
        // validated these exact roots; preserve that capability while enumerating every repo.
        let storage =
            crate::storage::bootstrap::ValidatedHostStorage::new(self.home.clone(), roots);
        let reservation_root = storage.store().join(".staging");
        let used = crate::gateway_inventory::NativeGatewayInventory::new(storage)
            .all_reserved_port_bases()
            .await
            .map_err(native_integrity_error)?;
        for base in (crate::metadata::MACOS_PORT_BLOCK_MIN
            ..=crate::metadata::MACOS_PORT_BLOCK_LAST_BASE)
            .step_by(usize::from(crate::metadata::PORT_BLOCK_SIZE))
        {
            if used.contains(&base) {
                continue;
            }
            let Some(marker) = claim_port_block(&reservation_root, base).map_err(|error| {
                CowshedError::internal(format!(
                    "claim macOS port block {base} at {}: {error}",
                    reservation_root.display()
                ))
            })?
            else {
                continue;
            };
            let grants = GrantSet::closed_baseline(Some(
                crate::metadata::PortBlock::new(base, crate::metadata::PORT_BLOCK_SIZE)
                    .map_err(native_integrity_error)?,
            ))
            .map_err(native_integrity_error)?;
            return Ok(PortGrantReservation { grants, marker });
        }
        Err(CowshedError::conflict(
            "no macOS workspace port block remains",
            "remove an unused workspace",
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

    async fn advance_gateway_revision(&self, workspace: &NativeWorkspace) -> Result<()> {
        let mut metadata = workspace.metadata.clone();
        metadata.grants.revision = metadata
            .grants
            .revision
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("gateway session revision overflow"))?;
        let image = workspace.image.clone();
        crate::storage::lifecycle::dispatch_blocking(move || metadata.write_for_image(&image))
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))?
            .map_err(native_integrity_error)
    }

    async fn ensure_supervisor(
        &mut self,
        name: &WorkspaceName,
    ) -> Result<super::supervisor::WorkspaceSupervisorHandle> {
        self.validate_binding().await?;
        let current = self.current(name).await?;
        self.ensure_supervisor_for(current).await
    }

    /// Start or reuse the supervisor for a workspace whose current state the caller already
    /// read under a validated binding.
    async fn ensure_supervisor_for(
        &mut self,
        mut current: NativeWorkspace,
    ) -> Result<super::supervisor::WorkspaceSupervisorHandle> {
        use crate::storage::lifecycle::{MountIntent, Substrate};

        let name = current.derived.workspace.name().clone();
        let name = &name;
        // Every verb that runs work in a workspace arrives here, so this is where the
        // git-worktree precondition belongs: exec, sessions, and `path`'s implicit attach all get
        // the same refusal rather than a mount whose git is broken.
        if is_git_worktree(&current.metadata) {
            self.require_main_mounted_for_git_worktree(name).await?;
            current = self.current(name).await?;
        }
        let was_detached = matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        );
        let mount = self
            .substrate
            .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
            .await
            .map_err(native_storage_error)?;
        if was_detached {
            self.advance_gateway_revision(&current).await?;
            current = self.current(name).await?;
        }
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
        crate::git::GitRepository::from_root(&mount)
            .ensure_cowshed_excludes()
            .await?;
        let port_block = current.metadata.grants.port_block.ok_or_else(|| {
            CowshedError::integrity(
                "macOS workspace metadata has no port block",
                "cowshed doctor --json",
            )
        })?;
        let sandbox = crate::sandbox::SandboxConfig {
            home: self.home.clone(),
            mount_root: self.layout.project().host_mount_root.clone(),
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
            // Multi-user Nix is a requirement, and evaluation inside a workspace is the point of
            // a workspace: without the daemon the client cannot build or substitute at all. The
            // sccache server socket rides along so rustc-wrapper clients reach the host-owned
            // daemon instead of spawning a wrong-boundary server in-sandbox.
            allowed_unix_sockets: crate::sandbox::nix_daemon_socket()
                .into_iter()
                .chain([crate::sandbox::sccache_server_socket()])
                .collect(),
            additional_denies: vec![
                self.layout.project().project_root.clone(),
                self.telemetry_root.clone(),
            ],
            git_worktree_repository: git_worktree_repository(
                &current.metadata,
                self.workspace_mount_path(&main_name())?,
            ),
        };
        let historical_incarnations = workspace_lineage(
            &mount,
            current.derived.workspace.incarnation(),
            crate::storage::job_artifact::ArtifactConfig::default().retained_recovery_budget_bytes,
        )?;
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
                historical_incarnations,
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

    async fn retire_workspace(&mut self, current: NativeWorkspace) -> Result<()> {
        use super::supervisor::CommitmentSink;
        use crate::storage::lifecycle::{LifecyclePlanner, Substrate};

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
                    .record(super::supervisor::CommitmentDraft::WorkspaceRetired {
                        repo_id,
                        workspace_incarnation: retired.workspace().incarnation().clone(),
                    })
                    .await
            })
            .await
            .map_err(native_retire_error)?;
        // The volume is detached, so the slot is free for its next tenant. Released before
        // reclamation rather than after: reclamation only ever removes an empty mountpoint the
        // *name* derives, and a slot mountpoint is meant to outlive its tenants — the next
        // workspace to take the slot mounts at exactly the same absolute path, which is the whole
        // point of a slot.
        self.release_slot(current.derived.workspace.name()).await?;
        let substrate = self.substrate.clone();
        std::mem::drop(tokio::spawn(async move {
            // Retirement removed the canonical image from discovery. Reclamation is deliberately
            // best-effort here: an interrupted task leaves trash for the next idempotent gc pass.
            let _ = substrate.reclaim(retired).await;
        }));
        Ok(())
    }

    async fn retire_restored_main(&mut self, current: NativeWorkspace) -> Result<()> {
        use super::supervisor::CommitmentSink;
        use crate::storage::lifecycle::Substrate;

        let mut commitments = self.commitments.clone();
        let repo_id = self.descriptor.repo_id.clone();
        let retired = self
            .substrate
            .execute_restored_main_retirement(
                &current.derived.workspace,
                move |retired| async move {
                    commitments
                        .record(super::supervisor::CommitmentDraft::WorkspaceRetired {
                            repo_id,
                            workspace_incarnation: retired.workspace().incarnation().clone(),
                        })
                        .await
                },
            )
            .await
            .map_err(native_retire_error)?;
        self.substrate
            .reclaim(retired)
            .await
            .map_err(native_storage_error)?;
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
#[derive(Clone, Debug, Eq, PartialEq)]
enum MoveDestination {
    Vacant,
    ReplaceDanglingLegacySymlink { target: PathBuf },
}

#[cfg(target_os = "macos")]
fn occupied_move_destination(destination: &Path) -> CowshedError {
    CowshedError::conflict(
        format!("{} already exists", destination.display()),
        "remove the occupant or choose another destination",
    )
}

#[cfg(target_os = "macos")]
fn canonical_lexical_absolute(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::RootDir => normalized.push(Path::new("/")),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            std::path::Component::Normal(component) => normalized.push(component),
            std::path::Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

/// Exact mount roots emitted by direct-main layouts that cowshed has retired.
///
/// These are derived from the validated primary binding, never from the link. Comparing only
/// lexically normalized raw link text keeps an arbitrary filesystem alias or user-supplied path
/// from acquiring migration authority.
#[cfg(target_os = "macos")]
fn known_retired_main_targets(
    current_main_mount: &Path,
    invoking_home: &Path,
    binding: &RepositoryBinding,
) -> Result<Vec<PathBuf>> {
    let repo_id = &binding.primary().map_err(native_integrity_error)?.repo_id;
    let candidates = [
        current_main_mount.to_owned(),
        invoking_home
            .join(".cowshed/mnt")
            .join(repo_id.owner())
            .join(repo_id.repo())
            .join("main"),
        Path::new("/private/cowshed/store/mnt")
            .join(repo_id.owner())
            .join(repo_id.repo())
            .join("main"),
    ];
    let mut targets = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let normalized = canonical_lexical_absolute(&candidate).ok_or_else(|| {
            native_integrity_error(format!(
                "retired main target is not an absolute lexical path: {}",
                candidate.display()
            ))
        })?;
        if !targets.contains(&normalized) {
            targets.push(normalized);
        }
    }
    Ok(targets)
}

#[cfg(target_os = "macos")]
fn classify_move_destination(
    destination: &Path,
    retired_main_targets: &[PathBuf],
) -> Result<MoveDestination> {
    let metadata = match std::fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MoveDestination::Vacant);
        }
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect checkout destination {}: {error}",
                    destination.display()
                ),
                "check the destination parent permissions and retry",
            ));
        }
    };
    if !metadata.file_type().is_symlink() {
        return Err(occupied_move_destination(destination));
    }
    let target = std::fs::read_link(destination)
        .ok()
        .and_then(|target| canonical_lexical_absolute(&target))
        .and_then(|target| retired_main_targets.contains(&target).then_some(target));
    let is_dangling = std::fs::metadata(destination)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound);
    let Some(target) = target.filter(|_| is_dangling) else {
        return Err(occupied_move_destination(destination));
    };
    Ok(MoveDestination::ReplaceDanglingLegacySymlink { target })
}

#[cfg(target_os = "macos")]
fn swap_checkout_paths(left: &Path, right: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let left = CString::new(left.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let right = CString::new(right.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    const RENAME_SWAP: u32 = 0x0000_0002;
    let result = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            left.as_ptr(),
            libc::AT_FDCWD,
            right.as_ptr(),
            RENAME_SWAP,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn replace_legacy_destination(
    mountpoint: &Path,
    destination: &Path,
    retired_main_mount: &PathBuf,
) -> Result<()> {
    match classify_move_destination(destination, std::slice::from_ref(retired_main_mount))? {
        MoveDestination::ReplaceDanglingLegacySymlink { .. } => {}
        MoveDestination::Vacant => return Err(occupied_move_destination(destination)),
    }
    swap_checkout_paths(mountpoint, destination).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot replace the retired checkout link at {}: {error}",
                destination.display()
            ),
            "choose a destination on the same writable filesystem",
        )
    })?;
    if matches!(
        classify_move_destination(mountpoint, std::slice::from_ref(retired_main_mount)),
        Ok(MoveDestination::ReplaceDanglingLegacySymlink { .. })
    ) {
        return Ok(());
    }
    let rollback = swap_checkout_paths(mountpoint, destination);
    Err(CowshedError::conflict(
        format!(
            "{} changed while the retired checkout link was being replaced{}",
            destination.display(),
            rollback
                .err()
                .map(|error| format!("; rollback failed: {error}"))
                .unwrap_or_default()
        ),
        "inspect both checkout paths and retry",
    ))
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectRootValidation {
    Strict,
    AllowDetachedMainRelocation,
}

#[cfg(target_os = "macos")]
fn validate_workspace_controller_root(
    derived: &crate::storage::lifecycle::DerivedWorkspace,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
    project_root: &Path,
    checkout_layout: crate::metadata::CheckoutLayout,
    validation: ProjectRootValidation,
) -> std::result::Result<(), crate::storage::apfs::ApfsStorageError> {
    if !derived.workspace.name().is_main() {
        return Ok(());
    }
    let permits_retired_root = matches!(
        validation,
        ProjectRootValidation::AllowDetachedMainRelocation
    ) && checkout_layout.mounts_at_checkout()
        && matches!(
            derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        );
    if !permits_retired_root
        && let Some(info) = metadata.info_snapshot.as_ref()
        && !names_one_root(&info.project_root, project_root)
    {
        return Err(crate::storage::apfs::ApfsStorageError::MarkerMismatch(
            format!(
                "persisted project root {} disagrees with controller root {}",
                info.project_root.display(),
                project_root.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeRemovalGitFence {
    incarnation: WorkspaceIncarnation,
    head: GitOid,
    dirty: bool,
    in_progress: Option<String>,
}

/// Whether the branch that outlives a workspace already holds the workspace's work.
#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeLandedState {
    branch: String,
    /// The measurement, or the reason there is none. A missing measurement is never landed: there
    /// is no shape here in which the absence of an answer can be read as a permissive one.
    commits: LandingCommits,
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeAdoptRollbackState {
    Retained,
    Swapped,
    Complete,
}

#[cfg(target_os = "macos")]
#[async_trait]
impl ProjectRuntimeHost for NativeProjectRuntimeHost {
    fn descriptor(&self) -> &ProjectDescriptor {
        &self.descriptor
    }

    async fn recover(&mut self) -> Result<()> {
        // One inventory read for the whole recovery. A detached direct-mounted main has no Git
        // repository at its recorded checkout by definition; its session marker and persisted
        // binding were validated during open, so querying remotes there would prevent the move
        // operation that repairs it. Every other state retains the ordinary live-Git check.
        let authoritative = self
            .authoritative_allowing_detached_main_relocation()
            .await?;
        let detached_direct_main = self.substrate_config.checkout_layout.mounts_at_checkout()
            && authoritative.iter().any(|workspace| {
                workspace.derived.workspace.name().is_main()
                    && matches!(
                        workspace.derived.mount_state,
                        crate::storage::lifecycle::MountState::Detached
                    )
            });
        if !detached_direct_main {
            self.validate_binding().await?;
        }
        // Intent recovery finishes interrupted create/fork/remove work by creating and destroying
        // images, so the read above describes the host from before those mutations. Re-read only
        // when recovery actually acted: with no pending intent — every start that did not follow a
        // crash — the first read still describes the host, so open stays linear in workspace count.
        let authoritative = if self.recover_lifecycle_intents().await? {
            self.authoritative_allowing_detached_main_relocation()
                .await?
        } else {
            authoritative
        };
        let attached = authoritative
            .into_iter()
            .filter(|workspace| {
                matches!(
                    workspace.derived.mount_state,
                    crate::storage::lifecycle::MountState::Mounted { .. }
                )
            })
            .collect::<Vec<_>>();
        for workspace in attached {
            self.ensure_supervisor_for(workspace).await?;
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
        use super::supervisor::CommitmentSink;
        use crate::storage::lifecycle::LifecyclePlanner;
        self.validate_binding().await?;
        let intent = crate::storage::recovery::LifecycleIntent::Adopt {
            options: options.clone(),
        };
        if let Some(expected) = self.completed_workspace_intent(&intent).cloned() {
            let current = self.current(&main_name()).await?;
            Self::require_exact_incarnation(&current, &expected)?;
            return self.snapshot(&current);
        }

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
        let format = options
            .image_format
            .unwrap_or(crate::metadata::ImageFormat::Asif);
        // Capacity is fixed for the image's lifetime at creation; `cowshed resize` is what moves
        // it afterwards. An unset option means the project default rather than "no capacity".
        let capacity = match options.capacity.as_deref() {
            Some(requested) => parse_capacity(requested)?,
            None => self.substrate_config.capacity,
        };
        let pre_cowshed = pre_cowshed_path(&self.descriptor.git_root)?;
        self.begin_lifecycle_intent(intent).await?;

        let reservation = self.fresh_grants().await?;
        let mut grants = reservation.grants.clone();
        grants.revision = 0;
        let identity = self
            .operation_identity(grants, self.git.current_branch().await?, None, false)
            .await?;
        let plan = self
            .substrate
            .plan_adopt(crate::storage::lifecycle::AdoptRequest {
                repo: self.descriptor.repo_id.clone(),
                format,
                capacity,
                topology_revision: crate::storage::lifecycle::Revision::new(0),
                source_checkout: self.descriptor.git_root.clone(),
                pre_cowshed_checkout: pre_cowshed,
                identity,
            })
            .map_err(native_integrity_error)?;
        let binding = self.descriptor.binding.clone();
        let binding_path = self.layout.project().repository_binding.clone();
        // The layout is recorded in the same staged step as the binding, before publication takes
        // the checkout path over. Every resolver reads it back from here; leaving it to be
        // inferred later would mean inferring it from a tree publication is midway through
        // rearranging.
        let layout_record = self.layout.clone();
        let checkout_layout = self.substrate_config.checkout_layout;
        let receipt = self
            .substrate
            .execute_adopt_staged(plan, move |stage| async move {
                crate::git::GitRepository::from_root(&stage.mount_point)
                    .ensure_workspace_environment_wiring()
                    .await?;
                crate::storage::lifecycle::dispatch_blocking(move || {
                    layout_record.record_checkout_layout(checkout_layout)?;
                    crate::metadata::write_json(&binding_path, &binding)
                })
                .await
                .map_err(|error| CowshedError::internal(error.to_string()))?
                .map_err(native_integrity_error)
            })
            .await
            .map_err(native_staged_error)?;
        self.commitments
            .record(super::supervisor::CommitmentDraft::WorkspaceIntroduced {
                repo_id: self.descriptor.repo_id.clone(),
                workspace_incarnation: receipt.workspace.incarnation().clone(),
            })
            .await?;
        self.complete_lifecycle_intent(
            receipt.workspace.name(),
            crate::storage::recovery::LifecycleIntentCompletion::Workspace(
                receipt.workspace.incarnation().clone(),
            ),
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
        use super::supervisor::CommitmentSink;
        use crate::storage::lifecycle::LifecyclePlanner;
        self.validate_binding().await?;
        let intent = crate::storage::recovery::LifecycleIntent::Create {
            workspace: workspace.clone(),
            options: options.clone(),
        };
        if let Some(expected) = self.completed_workspace_intent(&intent).cloned() {
            let current = self.current(&workspace).await?;
            Self::require_exact_incarnation(&current, &expected)?;
            return self.snapshot(&current);
        }

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
        // Git-worktree-ness is inherited, not just requested. A clone of a git-worktree workspace
        // carries no repository of its own — only a pointer file naming the *source's*
        // registration — so it has to be re-registered as one whatever the caller asked for.
        let git_worktree = options.git_worktree || is_git_worktree(&source.metadata);
        if git_worktree {
            self.require_main_mounted_for_git_worktree(&workspace)
                .await?;
        }
        if options.register && git_worktree {
            return Err(CowshedError::usage(
                "--register has nothing to fetch on a git-worktree workspace: main already holds its branch",
                format!("cowshed new {workspace} --git-worktree"),
            ));
        }
        // The slot is recorded before anything derives a mount path, because the record is what
        // decides where this workspace mounts. A failure after this point has to give the slot
        // back: a binding without a workspace would keep the next tenant out for good.
        let slot = options
            .slot
            .map(crate::metadata::SlotId::new)
            .transpose()
            .map_err(|error| {
                CowshedError::usage(
                    error.to_string(),
                    "choose a slot within the project's range",
                )
            })?;
        self.begin_lifecycle_intent(intent).await?;
        if let Some(slot) = slot {
            self.bind_slot(&workspace, slot).await?;
        }
        let created = async {
            let reservation = self.fresh_grants().await?;
            let identity = self
                .operation_identity(
                    reservation.grants.clone(),
                    Some(format!("cowshed/{workspace}")),
                    None,
                    git_worktree,
                )
                .await?;
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
            // Main's canonical mount, never `descriptor.git_root`: under the symlink layout the
            // recorded checkout is a symlink outside this workspace's read grants that dangles as
            // soon as the checkout moves, and only the canonical mount is maintained by
            // `cowshed mv`.
            let main_mount = self.workspace_mount_path(&main_name())?;
            let start = options.revision.as_ref().map(revision_target);
            let destination = workspace.clone();
            let receipt = self
                .substrate
                .execute_create_staged(plan, move |stage| async move {
                    let repository = crate::git::GitRepository::from_root(&stage.mount_point);
                    repository.ensure_workspace_environment_wiring().await?;
                    if git_worktree {
                        repository
                            .adopt_as_linked_worktree(
                                &destination.to_string(),
                                &main_mount,
                                start.as_deref(),
                            )
                            .await
                    } else {
                        repository
                            .prepare_workspace(
                                &destination.to_string(),
                                &main_mount,
                                start.as_deref(),
                            )
                            .await
                            .map(|_| ())
                    }
                })
                .await
                .map_err(native_staged_error)?;
            if options.register {
                self.register_workspace_in_main(&workspace).await?;
            }
            self.commitments
                .record(super::supervisor::CommitmentDraft::WorkspaceIntroduced {
                    repo_id: self.descriptor.repo_id.clone(),
                    workspace_incarnation: receipt.workspace.incarnation().clone(),
                })
                .await?;
            self.complete_lifecycle_intent(
                &workspace,
                crate::storage::recovery::LifecycleIntentCompletion::Workspace(
                    receipt.workspace.incarnation().clone(),
                ),
            )
            .await?;
            self.ensure_supervisor(&workspace).await?;
            self.snapshot_named(&workspace).await
        }
        .await;
        if created.is_err() && slot.is_some() {
            let _ = self.release_slot(&workspace).await;
        }
        created
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
                self.workspace_mount_path(workspace.derived.workspace.name())
                    .map(|mount| (index, mount))
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
        let intent = crate::storage::recovery::LifecycleIntent::Fork {
            source: source.clone(),
            destination: destination.clone(),
        };
        if let Some(expected) = self.completed_workspace_intent(&intent).cloned() {
            let current = self.current(&destination).await?;
            Self::require_exact_incarnation(&current, &expected)?;
            return self.snapshot(&current);
        }

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
        // A fork of a git-worktree workspace is one too, and has to be: the cloned image carries
        // a pointer file naming the *source's* registration, so the destination is re-registered
        // under its own id rather than left as a second claim on one worktree.
        let source_is_git_worktree = is_git_worktree(&source_fact.metadata);
        if source_is_git_worktree {
            self.require_main_mounted_for_git_worktree(&destination)
                .await?;
        }
        self.begin_lifecycle_intent(intent).await?;
        let reservation = self.fresh_grants().await?;
        let identity = self
            .operation_identity(
                reservation.grants.clone(),
                Some(format!("cowshed/{destination}")),
                Some(source.clone()),
                source_is_git_worktree,
            )
            .await?;
        let main_mount = self.workspace_mount_path(&main_name())?;
        let forked = destination.clone();
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
            .execute_fork_staged(plan, move |stage| async move {
                let repository = crate::git::GitRepository::from_root(&stage.mount_point);
                repository.ensure_workspace_environment_wiring().await?;
                if !source_is_git_worktree {
                    return Ok(());
                }
                repository
                    .adopt_as_linked_worktree(&forked.to_string(), &main_mount, None)
                    .await
            })
            .await
            .map_err(native_staged_error)?;
        self.commitments
            .record(CommitmentDraft::Fork {
                repo_id: self.descriptor.repo_id.clone(),
                source_incarnation: source_fact.derived.workspace.incarnation().clone(),
                destination_incarnation: receipt.workspace.incarnation().clone(),
            })
            .await?;
        self.complete_lifecycle_intent(
            &destination,
            crate::storage::recovery::LifecycleIntentCompletion::Workspace(
                receipt.workspace.incarnation().clone(),
            ),
        )
        .await?;
        self.ensure_supervisor(&destination).await?;
        self.snapshot_named(&destination).await
    }

    /// Renaming is a lifecycle operation for the same reason removal is: the name decides the
    /// image path, the volume label, the marker, and the mount point, and a workspace cannot be
    /// renamed out from under its own mount.
    ///
    /// It is composed rather than open-coded. A fork to the destination already clones the image,
    /// mints a fresh incarnation, relabels the volume, rewrites the marker, and publishes the
    /// result under one crash-safe transaction with its commitment; retiring the source is the
    /// other half. Same-volume `clonefile` makes the copy free, so the composition costs an
    /// incarnation and nothing else — and it inherits both transactions' recovery instead of
    /// needing its own.
    async fn rename(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot> {
        if source.is_main() || destination.is_main() {
            return Err(CowshedError::usage(
                "main cannot be renamed; its name is fixed by the project layout",
                "move the project checkout instead: cowshed mv main <path>",
            ));
        }
        if source == destination {
            return Err(CowshedError::usage(
                format!("workspace {source} already has that name"),
                "choose a different destination name",
            ));
        }
        self.validate_binding().await?;
        let current = self.current(&source).await?;
        if self
            .authoritative()
            .await?
            .iter()
            .any(|existing| existing.derived.workspace.name() == &destination)
        {
            return Err(CowshedError::conflict(
                format!("workspace {destination} already exists"),
                "choose another destination name, or remove the occupant first",
            ));
        }

        // Fence before either half runs. The source is about to be retired, so uncommitted or
        // in-progress work has to be refused here rather than discovered halfway through.
        let fence = self.removal_git_fence(&current).await?;
        if fence.dirty || fence.in_progress.is_some() {
            return Err(CowshedError::conflict(
                format!("workspace {source} has uncommitted or in-progress Git work"),
                format!("commit or stash the work, then retry: cowshed mv {source} {destination}"),
            ));
        }

        self.fork(source.clone(), destination.clone()).await?;
        // The source's commits are not being discarded, they are being republished under the
        // destination, whose image is a copy of this one — so the landed-ancestry gate that guards
        // a real removal would refuse a rename that loses nothing. This retires the source directly
        // instead of laundering that through a removal override: the fork is the preservation, and
        // the fence above is what makes the retirement safe.
        let retirement = async {
            let (retiring, _) = self.revalidated_removal_fence(&source, &fence).await?;
            self.finish_retirement(retiring).await
        }
        .await;
        if let Err(error) = retirement {
            // The destination now holds the work; leaving the source usable is the recoverable
            // half of a half-done rename.
            let _ = self.ensure_supervisor(&source).await;
            return Err(error);
        }
        self.snapshot_named(&destination).await
    }

    /// Move the project's checkout to `destination`, the `main` half of `cowshed mv`.
    ///
    /// The two layouts are genuinely different operations, not one operation with a branch:
    ///
    /// **Symlink.** Main stays mounted at `mnt/<owner>/<repo>/main` throughout — the checkout path
    /// is only a symlink into it, and nothing about the mount depends on where that symlink sits.
    /// So there is no unmount, and gaplessness costs nothing: the destination link is created
    /// before the source link is removed, and the tree is reachable by at least one name at every
    /// instant.
    ///
    /// **Direct mount.** The checkout path *is* the mountpoint. A mounted main is detached, its
    /// stub directory is renamed, the substrate is rebound, and the image is re-attached. A main
    /// that was already detached is recovered from its image and detached sidecar instead: the old
    /// path need not exist, and no Git command is sent there. In either case the destination fact
    /// is durable before the final mount, so a crash can only leave a forward-recoverable detach.
    ///
    /// The durable record is rewritten **before** the tree moves, in both layouts. Under direct
    /// mount the source path stops existing the instant the rename lands, so a record still naming
    /// it would be unrecoverable — nothing left to resolve — whereas a record naming the
    /// destination becomes true the moment the rename completes, and `attach` converges the rest.
    /// Recording ahead of the move is what makes the crash window recoverable in the forward
    /// direction instead of the dead one.
    async fn move_checkout(&mut self, destination: PathBuf) -> Result<WorkspaceSnapshot> {
        use crate::storage::lifecycle::{MountIntent, Substrate};

        let main = main_name();
        let source = self.substrate_config.checkout_path.clone();
        let layout = self.substrate_config.checkout_layout;
        let current = self
            .authoritative_allowing_detached_main_relocation()
            .await?
            .into_iter()
            .find(|workspace| workspace.derived.workspace.name() == &main)
            .ok_or_else(|| {
                CowshedError::not_found(
                    "workspace main does not exist",
                    "list published workspaces and retry",
                )
            })?;
        let detached_direct = layout.mounts_at_checkout()
            && matches!(
                current.derived.mount_state,
                crate::storage::lifecycle::MountState::Detached
            );
        // A detached direct mount has no repository at the recorded checkout path. Its persisted
        // binding was validated while opening from the session marker; querying Git here would
        // turn the exact recovery state into "cannot change to <old path>".
        if !detached_direct {
            self.validate_binding().await?;
        }
        let retired_main_targets = if detached_direct {
            let current_main_mount = self
                .layout
                .workspace_mount(&main)
                .map_err(native_integrity_error)?;
            known_retired_main_targets(&current_main_mount, &self.home, &self.descriptor.binding)?
        } else {
            Vec::new()
        };
        let destination_state = self
            .validate_move_destination(&source, &destination, &retired_main_targets)
            .await?;

        let mount_point = self.workspace_mount_path(&main)?;
        if !detached_direct && !crate::checkout::resolves_to(&source, &mount_point) {
            return Err(CowshedError::conflict(
                format!(
                    "the recorded checkout {} does not resolve to main's mount {}",
                    source.display(),
                    mount_point.display()
                ),
                "cowshed doctor --json",
            ));
        }
        let record = self.checkout_record(&current)?;

        if detached_direct {
            let prepare_record = record.clone();
            let prepare_layout = self.layout.clone();
            let prepare_source = source.clone();
            let prepare_destination = destination.clone();
            let prepare_destination_state = destination_state.clone();
            crate::storage::lifecycle::dispatch_blocking(move || {
                prepare_detached_checkout_relocation(
                    &prepare_record,
                    &prepare_layout,
                    layout,
                    &prepare_source,
                    &prepare_destination,
                    &prepare_destination_state,
                )
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("detached checkout move task failed: {error}"))
            })??;

            self.rebind_checkout(&destination, layout)?;
            let current = self.current(&main).await?;
            self.substrate
                .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                .await
                .map_err(native_storage_error)?;
            self.advance_gateway_revision(&current).await?;
            let current = self.current(&main).await?;
            let mounted_record = self.checkout_record(&current)?;
            let mounted_destination = destination.clone();
            crate::storage::lifecycle::dispatch_blocking(move || {
                mounted_record.rewrite_project_root(&mounted_destination)
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("checkout record task failed: {error}"))
            })?
            .map_err(native_integrity_error)?;
            self.repair_workspace_records(&destination).await?;
            self.ensure_supervisor(&main).await?;
            return self.snapshot_named(&main).await;
        }

        // The record moves first; see the method comment for why this direction is the recoverable
        // one. It is also the only step that can fail for a reason the filesystem cannot undo, so
        // failing here costs nothing but a refusal.
        let rewrite_record = record.clone();
        let rewrite_destination = destination.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            rewrite_record.rewrite_project_root(&rewrite_destination)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkout record task failed: {error}")))?
        .map_err(native_integrity_error)?;

        let moved = if layout.mounts_at_checkout() {
            self.move_direct_mount(&current, &source, &destination)
                .await
        } else {
            let move_source = source.clone();
            let move_destination = destination.clone();
            let target = mount_point.clone();
            crate::storage::lifecycle::dispatch_blocking(move || {
                crate::checkout::relink_checkout(&move_source, &move_destination, &target).map_err(
                    |error| {
                        CowshedError::environment_missing(
                            format!(
                                "cannot link {} to main's mount: {error}",
                                move_destination.display()
                            ),
                            "choose a destination on a writable filesystem",
                        )
                    },
                )
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("checkout link task failed: {error}"))
            })?
        };
        if let Err(error) = moved {
            // The tree never moved, so the only thing to undo is the record.
            let rollback = record.clone();
            let rollback_source = source.clone();
            let _ = crate::storage::lifecycle::dispatch_blocking(move || {
                rollback.rewrite_project_root(&rollback_source)
            })
            .await;
            return Err(error);
        }

        self.rebind_checkout(&destination, layout)?;
        if layout.mounts_at_checkout() {
            let current = self.current(&main).await?;
            // Past the rename there is no way back worth taking: the record and the tree both name
            // the destination, so a failure to re-attach here is a detached project at the right
            // path, which `cowshed attach` mounts. Rolling back would move the tree a second time
            // to reach a state that is strictly further from where the user asked to be.
            self.substrate
                .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                .await
                .map_err(native_storage_error)?;
        }
        self.repair_workspace_records(&destination).await?;
        self.ensure_supervisor(&main).await?;
        self.snapshot_named(&main).await
    }

    async fn attach(&mut self, workspace: WorkspaceName, options: AttachOptions) -> Result<()> {
        use crate::storage::lifecycle::{MountIntent, Substrate};
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        let was_detached = matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        );
        if is_git_worktree(&current.metadata) {
            self.require_main_mounted_for_git_worktree(&workspace)
                .await?;
        }
        self.substrate
            .ensure_mounted(
                &current.derived.workspace,
                MountIntent {
                    browse: options.browse,
                },
            )
            .await
            .map_err(native_storage_error)?;
        if was_detached {
            self.advance_gateway_revision(&current).await?;
        }
        self.ensure_supervisor(&workspace).await?;
        if let Some(observed) = options.observed_path {
            self.converge_checkout_record(&observed).await?;
        }
        // A workspace that was detached while the project moved still records main's old mount as
        // its `projectRoot`, still fetches from the old path, and still runs merge drivers spelt
        // against it. Attachment is the reconciliation front door and is what `doctor` sends the
        // operator to, so the whole record is repaired here rather than the remote alone — a hint
        // that names a command which cannot fix the condition it names is worse than no hint.
        let attached = self.current(&workspace).await?;
        let main_mount = self.workspace_mount_path(&main_name())?;
        let project_root = self.descriptor.git_root.clone();
        self.repair_one_workspace_record(&attached, &workspace, &main_mount, &project_root)
            .await?;
        Ok(())
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

    /// Grow a workspace's image, restoring the mount state the verb found it in.
    ///
    /// The supervisor is stopped first for the same reason `detach` stops it: the image has to
    /// leave the kernel for the resize, and a supervisor holding the mount would either keep it
    /// busy or come back pointed at a volume that went away underneath it.
    async fn resize(
        &mut self,
        workspace: WorkspaceName,
        capacity: String,
    ) -> Result<crate::api::dto::ResizeResult> {
        use crate::storage::lifecycle::Substrate;
        self.validate_binding().await?;
        let requested = parse_capacity(&capacity)?;
        let current = self.current(&workspace).await?;
        let was_mounted = matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        );
        self.stop_supervisor(&workspace).await?;
        let outcome = self
            .substrate
            .resize(&current.derived.workspace, requested)
            .await
            .map_err(native_storage_error)?;
        if was_mounted {
            self.ensure_supervisor(&workspace).await?;
        }
        Ok(crate::api::dto::ResizeResult {
            workspace,
            previous_capacity: outcome.previous.to_string(),
            capacity: outcome.capacity.to_string(),
        })
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
        require_checkpointable(&workspace, &current.metadata, "checkpoint")?;
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
        require_checkpointable(&workspace, &current.metadata, "restore")?;
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
        let info = current
            .metadata
            .require_info_snapshot()
            .map_err(native_integrity_error)?;
        let identity = self
            .operation_identity(
                current.metadata.grants.clone(),
                info.branch.clone(),
                info.forked_from.clone(),
                info.git_worktree,
            )
            .await?;
        self.stop_supervisor(&workspace).await?;
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
                |stage| async move {
                    if let crate::storage::apfs::RestoreStage::Replace(stage) = stage {
                        crate::git::GitRepository::from_root(&stage.mount_point)
                            .ensure_workspace_environment_wiring()
                            .await?;
                    }
                    Ok::<_, CowshedError>(())
                },
                move |fence| async move {
                    commitments
                        .record(CommitmentDraft::Restore {
                            repo_id: fence.pending.workspace.repo().clone(),
                            source_checkpoint: fence.pending.source_checkpoint,
                            source_incarnation: fence.pending.source_incarnation,
                            replaced_incarnation: fence.pending.replaced_incarnation,
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

    async fn remove(
        &mut self,
        workspace: WorkspaceName,
        options: RemoveOptions,
    ) -> Result<RemoveReport> {
        use crate::storage::lifecycle::{MountIntent, MountState, Substrate};

        if options.restore && options.force {
            return Err(CowshedError::usage(
                "--force and --restore select conflicting main removal modes",
                "choose exactly one main removal mode",
            ));
        }
        if options.restore && !workspace.is_main() {
            return Err(CowshedError::usage(
                "--restore is only valid for the adopted main workspace",
                "remove a session without --restore",
            ));
        }
        // `--abandon` authorizes destroying commits the project's main branch does not contain.
        // Main *is* that branch, so on main the flag has nothing to authorize and accepting it
        // would let a script carry one spelling for both and lose main to a typo.
        if options.abandon && workspace.is_main() {
            return Err(CowshedError::usage(
                "--abandon applies to session workspaces, whose commits main can contain",
                "recover the pre-adoption checkout instead: cowshed rm main --restore",
            ));
        }
        if workspace.is_main() && !options.restore && !options.force {
            return Err(main_removal_mode_refusal());
        }
        self.validate_binding().await?;
        let intent = crate::storage::recovery::LifecycleIntent::Retire {
            workspace: workspace.clone(),
            options,
        };
        let was_pending = self
            .lifecycle_intents
            .get(&workspace)
            .is_some_and(|record| record.operation == intent && record.completion.is_none());
        if let Some(report) = self.completed_retire_intent(&intent).cloned()
            && self
                .current(&workspace)
                .await
                .is_err_and(|error| error.code == ErrorCode::NotFound)
        {
            return Ok(report);
        }

        let mut current = match self.current(&workspace).await {
            Err(error) if error.code == ErrorCode::NotFound && was_pending => {
                let report = RemoveReport::default();
                self.complete_lifecycle_intent(
                    &workspace,
                    crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
                )
                .await?;
                return Ok(report);
            }
            Ok(current) => current,
            Err(error) if options.restore && error.code == ErrorCode::NotFound => {
                let pre_cowshed = pre_cowshed_path(&self.descriptor.git_root)?;
                let restored = tokio::fs::symlink_metadata(&pre_cowshed).await.is_err()
                    && self
                        .verify_checkout_identity(
                            &self.descriptor.git_root,
                            "restored project checkout",
                        )
                        .await
                        .is_ok();
                if restored {
                    if !was_pending {
                        self.begin_lifecycle_intent(intent.clone()).await?;
                    }
                    self.mark_lifecycle_intent_mutating(&workspace).await?;
                    self.remove_project_binding_after_restore().await?;
                    let report = RemoveReport::default();
                    self.complete_lifecycle_intent(
                        &workspace,
                        crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
                    )
                    .await?;
                    return Ok(report);
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };

        if options.restore {
            let pre_cowshed = pre_cowshed_path(&self.descriptor.git_root)?;
            let initial_rollback_state = self.adopt_rollback_state(&current, &pre_cowshed).await?;
            if !options.force && initial_rollback_state != NativeAdoptRollbackState::Complete {
                self.verify_checkout_identity(&pre_cowshed, "retained pre-cowshed checkout")
                    .await?;
                let initially_detached =
                    matches!(current.derived.mount_state, MountState::Detached);
                if initially_detached {
                    self.substrate
                        .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                        .await
                        .map_err(native_storage_error)?;
                    current = self.current(&workspace).await?;
                }
                if let Err(error) = self.require_main_restore_safe(&current, &pre_cowshed).await {
                    if initially_detached {
                        self.substrate
                            .unmount(&current.derived.workspace)
                            .await
                            .map_err(native_storage_error)?;
                    }
                    return Err(error);
                }
            }
            if !was_pending {
                self.begin_lifecycle_intent(intent.clone()).await?;
            }
            self.mark_lifecycle_intent_mutating(&workspace).await?;
            let incarnation = current.derived.workspace.incarnation().clone();
            self.stop_supervisor(&workspace).await?;
            let current = self.current(&workspace).await?;
            Self::require_exact_incarnation(&current, &incarnation)?;
            let rollback_state = self.adopt_rollback_state(&current, &pre_cowshed).await?;
            if rollback_state != NativeAdoptRollbackState::Complete {
                self.substrate
                    .restore_adopted_checkout(&current.derived.workspace, &pre_cowshed)
                    .await
                    .map_err(native_storage_error)?;
            }
            let current = self.current(&workspace).await?;
            Self::require_exact_incarnation(&current, &incarnation)?;
            self.verify_checkout_identity(&self.descriptor.git_root, "restored project checkout")
                .await?;
            if tokio::fs::symlink_metadata(&pre_cowshed).await.is_ok() {
                return Err(CowshedError::integrity(
                    "pre-cowshed path remains after atomic checkout restoration",
                    "retry adoption rollback before removing project state",
                ));
            }
            self.retire_restored_main(current).await?;
            self.remove_project_binding_after_restore().await?;
            let report = RemoveReport::default();
            self.complete_lifecycle_intent(
                &workspace,
                crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
            )
            .await?;
            return Ok(report);
        }

        let initially_detached = matches!(current.derived.mount_state, MountState::Detached);
        if initially_detached {
            self.substrate
                .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                .await
                .map_err(native_storage_error)?;
        }
        // The landed proof lives in main's repository, so main has to be readable for the whole
        // removal — including the revalidation after the supervisor stops. A project whose main is
        // detached still gets the proof: main is mounted for the duration and put back as found,
        // because the answer to "would this destroy work" must not depend on mount posture.
        let main_initially_detached = !workspace.is_main() && {
            let main = self.current(&main_name()).await?;
            let detached = matches!(main.derived.mount_state, MountState::Detached);
            if detached {
                self.substrate
                    .ensure_mounted(&main.derived.workspace, MountIntent { browse: false })
                    .await
                    .map_err(native_storage_error)?;
            }
            detached
        };

        let removal = async {
            let current = self.current(&workspace).await?;
            let initial_fence = self.removal_git_fence(&current).await?;
            self.require_removal_safe(&workspace, options, &initial_fence)
                .await?;
            let (current, final_fence) = self
                .revalidated_removal_fence(&workspace, &initial_fence)
                .await?;
            let abandoning = self
                .require_removal_safe(&workspace, options, &final_fence)
                .await?;
            if !was_pending {
                self.begin_lifecycle_intent(intent).await?;
            }
            self.mark_lifecycle_intent_mutating(&workspace).await?;
            // The bundle goes in before anything is destroyed: a belt written after the image is
            // gone would have nothing left to read.
            let abandoned = match abandoning {
                Some(landed) => Some(
                    self.bundle_abandoned_work(&current, &final_fence, landed)
                        .await?,
                ),
                None => None,
            };
            self.finish_retirement(current).await?;
            Ok(RemoveReport { abandoned })
        }
        .await;

        if main_initially_detached {
            let main = self.current(&main_name()).await?;
            self.stop_supervisor(&main_name()).await?;
            self.substrate
                .unmount(&main.derived.workspace)
                .await
                .map_err(native_storage_error)?;
        }
        let report = match removal {
            Ok(report) => report,
            Err(primary) => {
                let cleanup = match self.current(&workspace).await {
                    Ok(current) if initially_detached => self
                        .substrate
                        .unmount(&current.derived.workspace)
                        .await
                        .map_err(native_storage_error),
                    Ok(_) => self.ensure_supervisor(&workspace).await.map(|_| ()),
                    Err(_) => Ok(()),
                };
                return match cleanup {
                    Ok(()) => Err(primary),
                    Err(cleanup) => Err(CowshedError::internal(format!(
                        "workspace removal failed: {primary}; state restoration also failed: {cleanup}"
                    ))),
                };
            }
        };
        self.complete_lifecycle_intent(
            &workspace,
            crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
        )
        .await?;
        Ok(report)
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
        // Host-side state goes before the image does here too, for the same reason retirement
        // orders it that way: an image `gc` has already deleted leaves no authority to clean up
        // what it left behind in main. The authority is the retired image's own revalidated
        // sidecar — never the observation that a registered worktree's path is missing, which is
        // also what a merely detached workspace looks like.
        for candidate in plan.candidates() {
            if !matches!(candidate.reason(), StorageGcReason::RetiredWorkspace) {
                continue;
            }
            let Ok(metadata) =
                crate::metadata::DetachedWorkspaceMetadata::read_for_image(candidate.path())
            else {
                continue;
            };
            if metadata.info_snapshot.is_some_and(|info| info.git_worktree) {
                self.unregister_workspace_in_main(&metadata.workspace, true)
                    .await?;
            }
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
            let mount = self.workspace_mount_path(&workspace)?;
            let main_mount = self.workspace_mount_path(&main_name())?;
            let config = supervisor_sandbox(
                &self.home,
                &self.layout,
                &self.telemetry_root,
                &current,
                mount,
                main_mount,
            )?;
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
        // The default destination follows the remote's name, which is `main` — and `cowshed-main`
        // in a workspace where something else already held that name. A git-worktree workspace has
        // no remote at all and needs none: main's `main` branch is already in its ref namespace,
        // so the default destination is that branch itself.
        let main_mount = self.workspace_mount_path(&main_name())?;
        let mut fetch_remote = None;
        let git_worktree = is_git_worktree(&current.metadata);
        let default_onto = if git_worktree {
            "main".to_owned()
        } else {
            let main_remote = crate::git::GitRepository::from_root(&root)
                .configure_main_remote(&main_mount)
                .await?;
            fetch_remote = Some(main_remote.remote_name().to_owned());
            format!("{}/main", main_remote.remote_name())
        };
        let onto = options
            .onto
            .as_ref()
            .map(revision_target)
            .unwrap_or(default_onto);
        // Refresh the remote-tracking refs first: rebasing onto `main/main` resolves a ref that
        // only a fetch creates, and a stale one silently replays onto yesterday's base. A
        // git-worktree workspace reads main's branches directly out of the shared ref namespace,
        // so there is nothing to refresh and no remote to refresh it from.
        if let Some(remote) = fetch_remote {
            run_git(&root, ["fetch", "--no-tags", remote.as_str()]).await?;
        }
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
            .unwrap_or_else(|| DEFAULT_LANDING_BRANCH.to_owned());
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
            let exit_code = match info.exit {
                Some(crate::api::dto::ExitStatus::Exited { code }) => Some(code),
                _ => None,
            };
            if exit_code != Some(0) {
                // The check's own words are the diagnosis. Read them back through the
                // supervisor's bounded log so a failing check can never report as a bare
                // category, then decide whose fault this is: the workspace's, or an
                // environment that refused to run it at all.
                let stderr = read_job_stderr_tail(&handle, job_id).await;
                if let Some(denial) = sandbox_denial_in(&stderr) {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "land check `{check}` exited {exit} inside the sandbox: {denial}",
                            exit = exit_code
                                .map(|code| code.to_string())
                                .unwrap_or_else(|| "killed".into()),
                            denial = denial,
                        ),
                        format!(
                            "the sandbox refused this command (workspace grants do not cover it), not the code: run `cowshed exec {ws} -- {check}` unsandboxed-equivalent or `cowshed grant {ws} --read <path>`",
                            ws = workspace,
                        ),
                    ));
                }
                return Err(CowshedError::conflict(
                    format!(
                        "land check `{check}` failed with exit {exit}: {stderr}",
                        exit = exit_code
                            .map(|code| code.to_string())
                            .unwrap_or_else(|| "killed".into()),
                    ),
                    "fix the workspace and retry land",
                ));
            }
        }
        // Bring the workspace's objects into the host before merging them. A workspace is a
        // standalone repository — nothing has ever replicated its commits — so a bare `merge`
        // against a validated source head resolves to an object the host has never seen and fails
        // with git's own "not something we can merge". The fetch is the hand-back, and it is
        // pull-based like every other direction cowshed moves work.
        //
        // Land has no branch-name contract with the workspace: it fetches whatever branch the
        // workspace has checked out, whether an agent named it `cowshed/<ws>`, `wt/<ws>`, or
        // anything else. Only the resolved head is load-bearing.
        let source_mount = current_snapshot_mount(self, &current)?;
        let source_branch = crate::git::GitRepository::from_root(&source_mount)
            .current_branch()
            .await?
            .ok_or_else(|| {
                CowshedError::conflict(
                    format!("workspace {workspace} has no checked-out branch to land"),
                    "check out a branch in the workspace and retry land",
                )
            })?;
        let preservation_ref = format!("refs/cowshed/{workspace}/heads/{source_branch}");
        run_git(
            &self.descriptor.git_root,
            [
                "fetch",
                "--no-tags",
                source_mount.to_str().ok_or_else(|| {
                    CowshedError::internal("workspace mount path is not valid UTF-8")
                })?,
                &format!("+refs/heads/{source_branch}:{preservation_ref}"),
            ],
        )
        .await?;
        // The fetch is also the revalidation: if the workspace advanced between the check and now,
        // what arrived is not what was validated, and landing it would land unchecked work.
        let fetched = git_revision_oid(&self.descriptor.git_root, &preservation_ref).await?;
        if fetched != source_head {
            return Err(CowshedError::conflict(
                format!(
                    "workspace {workspace} advanced from {source_head} to {fetched} during land"
                ),
                "re-run the check against the new head and retry land",
            ));
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
        use crate::storage::lifecycle::{StorageGcReason, Substrate};

        let mut findings = Vec::new();
        if let Err(error) = self.validate_binding().await {
            findings.push(native_finding(
                "binding",
                crate::api::dto::FindingSeverity::Error,
                error,
            ));
        }
        let gateway_socket = crate::gateway_sessions::control_socket_path();
        let gateway_status =
            match cowshed_gateway::GatewayControlClient::new(gateway_socket.clone()) {
                Ok(client) => client.status().await.map_err(|error| error.to_string()),
                Err(error) => Err(error.to_string()),
            };
        if let Err(error) = gateway_status {
            findings.push(crate::api::dto::Finding {
                code: "gateway-down".into(),
                severity: crate::api::dto::FindingSeverity::Error,
                message: format!(
                    "gateway control socket does not answer at {}: {error}",
                    gateway_socket.display()
                ),
                hint: "cowshed gateway start".into(),
                path: Some(gateway_socket),
            });
        }
        match self.commitments.health().await {
            Ok(health) if health.failed > 0 => findings.push(crate::api::dto::Finding {
                code: "audit-sink".into(),
                severity: crate::api::dto::FindingSeverity::Warning,
                message: format!(
                    "the {} audit sink refused {} of {} records; last: {}",
                    health.sink,
                    health.failed,
                    health.failed.saturating_add(health.recorded),
                    health.last_failure.as_deref().unwrap_or("(no message)")
                ),
                hint: "verify telemetry storage, or set COWSHED_CONTINUITY_AUDIT=off — the audit trail gates nothing"
                    .into(),
                path: Some(self.telemetry_root.clone()),
            }),
            Ok(_) => {}
            Err(error) => findings.push(native_finding(
                "audit-sink",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
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
                        hint: "retry restore after repairing the image or gateway evidence".into(),
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
                    let workspace_name = workspace.derived.workspace.name().clone();
                    let expected_mount = match self.workspace_mount_path(&workspace_name) {
                        Ok(path) => path,
                        Err(error) => {
                            findings.push(native_finding(
                                "mount",
                                crate::api::dto::FindingSeverity::Error,
                                error,
                            ));
                            continue;
                        }
                    };
                    match &workspace.derived.mount_state {
                        crate::storage::lifecycle::MountState::Detached => {
                            findings.push(crate::api::dto::Finding {
                                code: "mount".into(),
                                severity: crate::api::dto::FindingSeverity::Info,
                                message: format!(
                                    "workspace {workspace_name} is detached; expected mount {}",
                                    expected_mount.display()
                                ),
                                hint: format!("cowshed attach {workspace_name}"),
                                path: Some(expected_mount),
                            });
                            if self.supervisors.contains_key(&workspace_name) {
                                findings.push(crate::api::dto::Finding {
                                    code: "mount-supervisor".into(),
                                    severity: crate::api::dto::FindingSeverity::Error,
                                    message: format!(
                                        "detached workspace {workspace_name} still has a supervisor"
                                    ),
                                    hint: format!(
                                        "cowshed detach {workspace_name} && cowshed attach {workspace_name}"
                                    ),
                                    path: Some(workspace.image),
                                });
                            }
                        }
                        crate::storage::lifecycle::MountState::Mounted { .. } => {
                            let marker_path =
                                expected_mount.join(crate::storage::WORKSPACE_MARKER_PATH);
                            let expected_repo = self.descriptor.repo_id.clone();
                            let expected_workspace = workspace_name.clone();
                            let expected_incarnation =
                                workspace.derived.workspace.incarnation().clone();
                            let expected_project_root = self.descriptor.git_root.clone();
                            let checked_marker_path = marker_path.clone();
                            let marker = crate::storage::lifecycle::dispatch_blocking(move || {
                                let marker = crate::metadata::WorkspaceMarker::read_from(
                                    &checked_marker_path,
                                )
                                .map_err(|error| error.to_string())?;
                                if marker.repo_id != expected_repo
                                    || marker.workspace != expected_workspace
                                    || marker.workspace_incarnation != expected_incarnation
                                    || !names_one_root(
                                        &marker.project_root,
                                        &expected_project_root,
                                    )
                                {
                                    return Err(format!(
                                        "workspace marker identity does not match {expected_workspace}"
                                    ));
                                }
                                Ok(())
                            })
                            .await;
                            match marker {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => findings.push(crate::api::dto::Finding {
                                    code: "marker".into(),
                                    severity: crate::api::dto::FindingSeverity::Error,
                                    message: format!(
                                        "workspace {workspace_name} marker is invalid: {error}"
                                    ),
                                    hint: format!(
                                        "cowshed detach {workspace_name} && cowshed attach {workspace_name}"
                                    ),
                                    path: Some(marker_path),
                                }),
                                Err(error) => findings.push(crate::api::dto::Finding {
                                    code: "marker".into(),
                                    severity: crate::api::dto::FindingSeverity::Error,
                                    message: format!(
                                        "could not validate workspace {workspace_name} marker: {error}"
                                    ),
                                    hint: "cowshed doctor --json".into(),
                                    path: Some(marker_path),
                                }),
                            }
                        }
                    }
                }
            }
            Err(error) => findings.push(native_finding(
                "marker",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
        }
        // A blocked collection sends the operator here, so this reads gc's own preview rather than a
        // second scan that could disagree with the command that forwarded to it. A preview that
        // cannot be taken at all is itself the finding — reporting a healthy host while `gc` refuses
        // leaves the operator with no next move.
        match self.substrate.preview_gc(&self.descriptor.repo_id).await {
            Ok(plan) => {
                let stranded = plan
                    .candidates()
                    .iter()
                    .filter(|candidate| {
                        matches!(candidate.reason(), StorageGcReason::RetiredWorkspace)
                    })
                    .collect::<Vec<_>>();
                if let Some(first) = stranded.first() {
                    let bytes = stranded
                        .iter()
                        .try_fold(0_u64, |sum, candidate| sum.checked_add(candidate.bytes()))
                        .ok_or_else(|| {
                            CowshedError::internal("GC candidate byte accounting overflow")
                        })?;
                    findings.push(crate::api::dto::Finding {
                        code: "retired-trash".into(),
                        severity: crate::api::dto::FindingSeverity::Warning,
                        message: format!(
                            "sessions/.trash holds {} retired workspace {} totalling {bytes} bytes",
                            stranded.len(),
                            if stranded.len() == 1 {
                                "entry"
                            } else {
                                "entries"
                            }
                        ),
                        hint: "cowshed gc".into(),
                        path: Some(first.path().to_owned()),
                    });
                }
            }
            Err(error) => findings.push(native_finding(
                "gc-preview",
                crate::api::dto::FindingSeverity::Error,
                native_storage_error(error),
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

#[cfg(any(target_os = "macos", test))]
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

    // A remote that yields no owner/repo identity is not an error: local-path
    // mirrors and backup remotes are ordinary Git and carry no identity to
    // derive. They are skipped as identity candidates, and only reported if
    // nothing else identifies the repository — one unusable remote must not
    // brick read-only commands in an otherwise well-formed checkout.
    let mut candidates = Vec::with_capacity(remotes.len());
    let mut unusable = Vec::new();
    for remote in remotes {
        match crate::repository::normalize_remote_url(&remote.url) {
            Ok(repo_id) => candidates.push((remote, repo_id)),
            Err(error) => unusable.push(format!("{} ({error})", remote.name)),
        }
    }

    if candidates.is_empty() {
        let repo_id = requested_repo_id.cloned().ok_or_else(|| {
            CowshedError::environment_missing(
                format!(
                    "no Git remote yields a repository identity; skipped: {}",
                    unusable.join(", ")
                ),
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

    let remote_url = persistable_remote_url(&selected.url)
        .ok_or_else(|| CowshedError::internal("normalized repository URL cannot be persisted"))?;
    RepositoryBinding::new(vec![crate::repository::BoundIdentity {
        repo_id: selected_repo_id,
        remote_name: Some(selected.name.clone()),
        remote_url: Some(remote_url),
        primary: true,
    }])
    .map_err(binding_integrity_error)
}

#[cfg(any(target_os = "macos", test))]
fn persistable_remote_url(value: &str) -> Option<String> {
    let suffix = value
        .char_indices()
        .find_map(|(index, character)| matches!(character, '?' | '#').then_some(index));
    let without_suffix = suffix.map_or(value, |index| &value[..index]);
    if let Some((scheme, remainder)) = without_suffix.split_once("://") {
        let (authority, path) = remainder.split_once('/')?;
        let authority = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        Some(format!("{scheme}://{authority}/{path}"))
    } else {
        let (authority, path) = without_suffix.split_once(':')?;
        let authority = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        Some(format!("{authority}:{path}"))
    }
}

#[cfg(any(target_os = "macos", test))]
fn binding_integrity_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(error.to_string(), "repair the repository binding")
}

/// The project checkout a path's workspace marker records, if it carries one.
///
/// Portable because the router needs it on every target: a marker is plain metadata, and the
/// question "which project does this directory belong to" has nothing platform-specific in it.
async fn marker_project_root(path: &Path) -> Result<Option<PathBuf>> {
    let marker_path = path.join(crate::storage::WORKSPACE_MARKER_PATH);
    let marker = crate::storage::lifecycle::dispatch_blocking(move || {
        match crate::metadata::WorkspaceMarker::read_from(&marker_path) {
            Ok(marker) => Ok(Some(marker)),
            Err(_) => Ok::<_, CowshedError>(None),
        }
    })
    .await
    .map_err(|error| CowshedError::internal(format!("workspace marker task failed: {error}")))??;
    Ok(marker.map(|marker| marker.project_root))
}

#[cfg(target_os = "macos")]
fn prepare_detached_checkout_relocation(
    record: &crate::checkout::CheckoutRecord,
    storage_layout: &crate::storage::StorageLayout,
    checkout_layout: crate::metadata::CheckoutLayout,
    source: &Path,
    destination: &Path,
    destination_state: &MoveDestination,
) -> Result<()> {
    let replacement_target = match destination_state {
        MoveDestination::Vacant => None,
        MoveDestination::ReplaceDanglingLegacySymlink { target } => Some(target),
    };
    let mut displaced_legacy_link = None;
    let source_existed = match std::fs::symlink_metadata(source) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            if let Some(retired_main_mount) = replacement_target {
                replace_legacy_destination(source, destination, retired_main_mount)?;
                displaced_legacy_link = Some(source.to_owned());
            } else {
                std::fs::rename(source, destination).map_err(|error| {
                    CowshedError::environment_missing(
                        format!(
                            "cannot move the detached checkout mountpoint to {}: {error}",
                            destination.display()
                        ),
                        "choose a destination on the same writable filesystem",
                    )
                })?;
            }
            true
        }
        Ok(_) => {
            return Err(CowshedError::conflict(
                format!(
                    "the detached checkout path {} is not a directory",
                    source.display()
                ),
                "remove the occupant or run cowshed doctor --json",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(retired_main_mount) = replacement_target {
                let parent = destination
                    .parent()
                    .expect("validated checkout destination has a parent");
                let leaf = destination
                    .file_name()
                    .expect("validated checkout destination has a file name")
                    .to_string_lossy();
                let staging = parent.join(format!(
                    ".{leaf}.cowshed-relocate-{}",
                    uuid::Uuid::new_v4().simple()
                ));
                std::fs::create_dir(&staging).map_err(|error| {
                    CowshedError::environment_missing(
                        format!(
                            "cannot stage checkout mountpoint beside {}: {error}",
                            destination.display()
                        ),
                        "choose a destination in a writable directory",
                    )
                })?;
                if let Err(error) =
                    replace_legacy_destination(&staging, destination, retired_main_mount)
                {
                    let _ = std::fs::remove_dir(&staging);
                    return Err(error);
                }
                displaced_legacy_link = Some(staging);
            } else {
                std::fs::create_dir(destination).map_err(|error| {
                    CowshedError::environment_missing(
                        format!(
                            "cannot create checkout mountpoint {}: {error}",
                            destination.display()
                        ),
                        "choose a destination in a writable directory",
                    )
                })?;
            }
            false
        }
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect detached checkout path {}: {error}",
                    source.display()
                ),
                "check the old checkout parent permissions and retry",
            ));
        }
    };
    let recorded = storage_layout
        .record_checkout_layout(checkout_layout)
        .map_err(native_integrity_error)
        .and_then(|()| {
            record
                .rewrite_detached_project_root(destination)
                .map_err(native_integrity_error)
                .map(|_| ())
        });
    if let Err(error) = recorded {
        if let Some(displaced) = displaced_legacy_link.as_ref() {
            let _ = swap_checkout_paths(destination, displaced);
            if !source_existed {
                let _ = std::fs::remove_dir(displaced);
            }
        } else if source_existed {
            let _ = std::fs::rename(destination, source);
        } else {
            let _ = std::fs::remove_dir(destination);
        }
        return Err(error);
    }
    if let Some(displaced) = displaced_legacy_link {
        std::fs::remove_file(&displaced).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot remove retired checkout link {}: {error}",
                    displaced.display()
                ),
                "check the checkout parent permissions and retry",
            )
        })?;
    }
    Ok(())
}

/// What a workspace marker tells an opening controller about the project it belongs to.
#[cfg(target_os = "macos")]
struct WorkspaceOrigin {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    /// The project's checkout path, which every marker records — main's own for main, and main's
    /// for a session, since a session is a clone of the project rather than a project of its own.
    project_root: PathBuf,
}

#[cfg(target_os = "macos")]
async fn workspace_origin_from_marker(project_root: &Path) -> Result<Option<WorkspaceOrigin>> {
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
    // The marker's job here is to name the repository, and every workspace's marker names it. A
    // coordinator verb is routinely invoked from inside a session workspace — `cowshed rebase`
    // infers its workspace from the cwd precisely so it can be — and that directory is a session
    // mount carrying a session marker.
    //
    // The recorded project root cannot be compared against the invocation root for a session:
    // every marker records the project's checkout path, which for a session is main's checkout and
    // therefore a different directory than the mount the caller is standing in. Comparing them
    // rejected every workspace cwd, and no amount of repairing main's marker could satisfy it,
    // because main's marker was never the file being read.
    //
    // What remains checkable is real incoherence: a marker whose role and workspace name disagree
    // is corrupt either way, and main's marker must still name the root it sits in, since for main
    // — and only for main — the recorded project root and the invocation root are the same
    // directory.
    let claims_main = marker.workspace.is_main();
    if claims_main != (marker.role == crate::metadata::WorkspaceRole::Main) {
        return Err(CowshedError::conflict(
            format!(
                "workspace marker at {} names {} with role {:?}",
                project_root.display(),
                marker.workspace,
                marker.role
            ),
            "repair the workspace marker, or reopen from the canonical main checkout",
        ));
    }
    if claims_main && !names_one_root(&marker.project_root, project_root) {
        return Err(CowshedError::conflict(
            format!(
                "main workspace marker records {} but was read at {}",
                marker.project_root.display(),
                project_root.display()
            ),
            "cowshed attach",
        ));
    }
    Ok(Some(WorkspaceOrigin {
        repo_id: marker.repo_id,
        workspace: marker.workspace,
        workspace_incarnation: marker.workspace_incarnation,
        project_root: marker.project_root,
    }))
}

#[cfg(target_os = "macos")]
fn validate_workspace_origin_against_inventory(
    origin: &WorkspaceOrigin,
    facts: &[&crate::storage::lifecycle::StorageFact],
) -> Result<()> {
    if facts.iter().any(|fact| {
        fact.workspace.repo() == &origin.repo_id
            && fact.workspace.name() == &origin.workspace
            && fact.workspace.incarnation() == &origin.workspace_incarnation
    }) {
        return Ok(());
    }
    Err(CowshedError::conflict(
        format!(
            "workspace marker identity {}/{}/{} differs from active storage inventory",
            origin.repo_id, origin.workspace, origin.workspace_incarnation
        ),
        "reopen from a workspace whose marker matches active storage",
    ))
}

/// Do a recorded project root and an observed one name the same directory?
///
/// Recorded metadata holds the adopted checkout path, while the controller and Git report the
/// physical root — since main mounts under `mnt/<owner>/<repo>/main` and the checkout is a symlink
/// into it, those two strings legitimately differ for the same workspace. They still describe one
/// directory, so a disagreement is only real when the paths do not resolve to the same place.
fn names_one_root(recorded: &Path, observed: &Path) -> bool {
    if recorded == observed {
        return true;
    }
    let (Ok(recorded), Ok(observed)) = (
        std::fs::canonicalize(recorded),
        std::fs::canonicalize(observed),
    ) else {
        return false;
    };
    recorded == observed
}

#[cfg(target_os = "macos")]
fn validate_binding_against_remotes(
    binding: &RepositoryBinding,
    remotes: &[crate::git::RemoteUrl],
) -> Result<()> {
    binding.validate().map_err(native_integrity_error)?;
    for identity in &binding.identities {
        if let (Some(name), Some(url)) = (&identity.remote_name, &identity.remote_url)
            && !remotes.iter().any(|remote| {
                &remote.name == name
                    && persistable_remote_url(&remote.url).as_deref() == Some(url.as_str())
            })
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
async fn read_persisted_binding(
    layout: &crate::storage::StorageLayout,
) -> Result<Option<RepositoryBinding>> {
    let path = layout.project().repository_binding.clone();
    crate::storage::lifecycle::dispatch_blocking(move || {
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
    .map_err(native_integrity_error)
}

/// Resolve a session invocation through the store binding, without opening the recorded checkout.
///
/// A session's marker names main's checkout, not the session repository the caller is standing
/// in. When those roots differ, the marker identity and persisted binding are the complete project
/// authority. In particular, the recorded checkout may be a missing direct mount; no Git command
/// may be aimed at it merely to learn an identity the store already records.
#[cfg(target_os = "macos")]
async fn project_binding_from_workspace_origin(
    store_root: &Path,
    invocation_root: &Path,
    origin: Option<&WorkspaceOrigin>,
) -> Result<Option<(RepoId, crate::storage::StorageLayout, RepositoryBinding)>> {
    let Some(origin) = origin else {
        return Ok(None);
    };
    if names_one_root(&origin.project_root, invocation_root) {
        return Ok(None);
    }
    let repo_id = origin.repo_id.clone();
    let layout =
        crate::storage::StorageLayout::new(store_root, &repo_id).map_err(native_integrity_error)?;
    let binding = read_persisted_binding(&layout).await?.ok_or_else(|| {
        CowshedError::integrity(
            format!("adopted project {repo_id} has no persisted repository binding"),
            "cowshed doctor --json",
        )
    })?;
    binding.validate().map_err(native_integrity_error)?;
    if binding.primary().map_err(native_integrity_error)?.repo_id != repo_id {
        return Err(CowshedError::conflict(
            "workspace marker identity differs from the persisted repository binding",
            "repair the repository binding before opening cowshed",
        ));
    }
    Ok(Some((repo_id, layout, binding)))
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
    let loaded = read_persisted_binding(layout).await?;
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
        let waivers = match crate::metadata::read_json::<Vec<crate::secrets::SecretWaiver>>(
            &waivers_path,
        ) {
            Ok(waivers) => waivers,
            Err(crate::metadata::MetadataError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Vec::new()
            }
            Err(error @ crate::metadata::MetadataError::Json { .. }) => {
                return Err(CowshedError::integrity(
                    error.to_string(),
                    format!(
                        "repair the waivers file first (or delete it to start without waivers): {}",
                        crate::secrets::waiver_guidance(&waivers_path),
                    ),
                ));
            }
            Err(error) => return Err(native_integrity_error(error)),
        };
        let scan = crate::secrets::scan_tree(&root, &waivers)
            .map_err(|error| secret_scan_error(&waivers_path, error))?;
        if scan.findings.is_empty() {
            return Ok(());
        }
        if !quarantine {
            return Err(secret_findings_error(&scan.findings, &waivers_path));
        }
        quarantine_secret_files(&root, &quarantine_root, &scan.findings)?;
        let remaining = crate::secrets::scan_tree(&root, &waivers)
            .map_err(|error| secret_scan_error(&waivers_path, error))?;
        if remaining.findings.is_empty() {
            Ok(())
        } else {
            Err(secret_findings_error(&remaining.findings, &waivers_path))
        }
    })
    .await
    .map_err(|error| CowshedError::internal(format!("secret scan task failed: {error}")))?
}

#[cfg(target_os = "macos")]
fn secret_scan_error(waivers_path: &Path, error: crate::secrets::SecretScanError) -> CowshedError {
    match error {
        crate::secrets::SecretScanError::InvalidWaiver { .. }
        | crate::secrets::SecretScanError::DuplicateWaiver { .. } => CowshedError::integrity(
            error.to_string(),
            format!(
                "repair the controller-owned waivers file first: {}",
                crate::secrets::waiver_guidance(waivers_path),
            ),
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
fn secret_findings_error(
    findings: &[crate::secrets::SecretFinding],
    waivers_path: &Path,
) -> CowshedError {
    let paths = findings
        .iter()
        .map(|finding| finding.path.display().to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ");
    CowshedError::conflict(
        format!("repository contains secrets in: {paths}"),
        format!(
            "remove the files, or waive a false positive: {}; otherwise retry adopt with --quarantine",
            crate::secrets::waiver_guidance(waivers_path),
        ),
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

#[cfg(all(test, target_os = "macos"))]
mod removal_refusal_tests {
    use super::*;

    fn workspace() -> WorkspaceName {
        WorkspaceName::new("raven").expect("fixed workspace name")
    }

    fn oid(fill: char) -> GitOid {
        GitOid::new(fill.to_string().repeat(40)).expect("fixed oid")
    }

    fn fence(dirty: bool, in_progress: Option<&str>) -> NativeRemovalGitFence {
        NativeRemovalGitFence {
            incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .expect("fixed incarnation"),
            head: oid('4'),
            dirty,
            in_progress: in_progress.map(str::to_owned),
        }
    }

    fn state(commits: LandingCommits) -> NativeLandedState {
        NativeLandedState {
            branch: DEFAULT_LANDING_BRANCH.to_owned(),
            commits,
        }
    }

    fn measured(unlanded: u64, landed: u64) -> NativeLandedState {
        state(LandingCommits::Measured {
            target_branch: DEFAULT_LANDING_BRANCH.to_owned(),
            target_head: oid('1'),
            unlanded,
            landed,
            behind: 0,
        })
    }

    fn indeterminate() -> NativeLandedState {
        state(LandingCommits::Indeterminate {
            reason: String::from("main's repository has no main branch"),
        })
    }

    /// Every refusal a removal can answer with, so the sweep below cannot miss one.
    fn every_removal_refusal() -> Vec<CowshedError> {
        vec![
            removal_in_progress_refusal(&workspace(), "rebase-merge"),
            removal_dirty_refusal(&workspace()),
            removal_unlanded_refusal(&workspace(), &oid('4'), &measured(3, 1)),
            removal_unlanded_refusal(&workspace(), &oid('4'), &indeterminate()),
            removal_head_moved_refusal(&workspace(), &oid('1'), &oid('4')),
            main_removal_mode_refusal(),
            NativeProjectRuntimeHost::require_session_state_clean(&workspace(), &fence(true, None))
                .expect_err("dirty is refused"),
            NativeProjectRuntimeHost::require_session_state_clean(
                &workspace(),
                &fence(false, Some("MERGE_HEAD")),
            )
            .expect_err("an in-progress operation is refused"),
        ]
    }

    /// The regression that produced this gate: a refusal that prescribed `--force` taught
    /// coordinator scripts to retry with it, and the retry destroyed unlanded work. A refusal may
    /// never name the flag that overrides it — the flags are documented in `cowshed rm`'s usage
    /// text, where a human reads options deliberately.
    #[test]
    fn no_removal_refusal_prescribes_the_flag_that_overrides_it() {
        for refusal in every_removal_refusal() {
            for (field, value) in [("message", &refusal.message), ("hint", &refusal.hint)] {
                for flag in ["--force", "--abandon"] {
                    assert!(
                        !value.contains(flag),
                        "removal refusal {field} prescribes {flag}: {value}"
                    );
                }
            }
            assert_eq!(refusal.code, ErrorCode::Conflict);
        }
    }

    /// The refusal has to be actionable on its own: it names the head that is at risk, how much of
    /// it is unheld, the branch that does not hold it, and where that branch stands.
    #[test]
    fn the_unlanded_refusal_names_the_head_the_count_the_branch_and_the_tip() {
        let refusal = removal_unlanded_refusal(&workspace(), &oid('4'), &measured(3, 1));
        assert_eq!(
            refusal.message,
            format!(
                "workspace raven head {} carries 3 commits that main does not hold, by ancestry or \
                 by patch equivalence (main is at {})",
                oid('4'),
                oid('1')
            )
        );
        assert_eq!(refusal.hint, "land the workspace: cowshed land raven");

        // One commit reads as one commit. A gate that says "1 commits" is a gate nobody trusts.
        assert!(
            removal_unlanded_refusal(&workspace(), &oid('4'), &measured(1, 0))
                .message
                .contains("carries 1 commit that main"),
        );

        // No measurement names the unanswered question rather than printing an absence, because the
        // caller is being refused for a missing proof and not for work they can see.
        let refusal = removal_unlanded_refusal(&workspace(), &oid('4'), &indeterminate());
        assert_eq!(
            refusal.message,
            format!(
                "workspace raven head {} cannot be proven to be in main: main's repository has no \
                 main branch",
                oid('4')
            )
        );
    }

    /// The half of the gate this change exists to fix. Patch equivalence satisfies "landed", so a
    /// workspace whose work reached main by squash-merge or a history rewrite retires with no flag
    /// at all — while genuinely unheld work still requires the one flag that authorizes losing it.
    #[test]
    fn the_landed_gate_accepts_patch_equivalence_and_still_refuses_unheld_work() {
        // Landed by patch identity alone: nothing ahead is unheld, though one commit is not an
        // ancestor of main. No flag, and nothing to bundle.
        assert_eq!(
            removal_landed_decision(&workspace(), &oid('4'), measured(0, 1), false)
                .expect("patch-equivalent work needs no authorization"),
            None
        );

        // Nothing ahead at all — landed by ancestry — is the same answer by the same rule.
        assert_eq!(
            removal_landed_decision(&workspace(), &oid('4'), measured(0, 0), false)
                .expect("a workspace with nothing ahead needs no authorization"),
            None
        );

        // Partly landed is not landed, and the refusal stands without `--abandon`.
        let refused = removal_landed_decision(&workspace(), &oid('4'), measured(2, 1), false)
            .expect_err("unheld commits must be refused");
        assert_eq!(refused.code, ErrorCode::Conflict);

        // `--abandon` is what turns that refusal into an authorized loss, and it answers with the
        // state to bundle rather than with a bare go-ahead.
        assert_eq!(
            removal_landed_decision(&workspace(), &oid('4'), measured(2, 1), true)
                .expect("--abandon authorizes the loss"),
            Some(measured(2, 1))
        );

        // An unanswered question is refused exactly as unheld work is, and `--abandon` is still the
        // only way past it — so a stale or unreadable target can never authorize a deletion.
        assert!(
            removal_landed_decision(&workspace(), &oid('4'), indeterminate(), false).is_err(),
            "an indeterminate verdict must never read as landed"
        );
        assert_eq!(
            removal_landed_decision(&workspace(), &oid('4'), indeterminate(), true)
                .expect("--abandon authorizes a loss it cannot measure"),
            Some(indeterminate())
        );
    }

    /// `--force` and `--abandon` authorize different losses, and the dispatcher must not let either
    /// stand in for the other. Only the transient half is overridable by `--force`.
    #[test]
    fn force_overrides_transient_state_and_nothing_else() {
        let clean = fence(false, None);
        NativeProjectRuntimeHost::require_session_state_clean(&workspace(), &clean)
            .expect("a clean workspace passes the transient gate");
        assert!(
            NativeProjectRuntimeHost::require_session_state_clean(&workspace(), &fence(true, None))
                .is_err(),
            "dirt is exactly what the transient gate is for"
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
    host.workspace_mount_path(workspace.derived.workspace.name())
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

/// Resolve `reference`, answering `None` when this repository simply does not have it.
///
/// `rev-parse --verify --quiet` is the spelling that distinguishes those two outcomes: `show-ref
/// --verify` is *fatal* (exit 128) on an absent ref, which would turn "the target branch does not
/// exist yet" into an internal error instead of the `None` every caller here is written for.
#[cfg(target_os = "macos")]
async fn git_optional_ref_oid(root: &Path, reference: &str) -> Result<Option<GitOid>> {
    let output = tokio::process::Command::new("/usr/bin/git")
        .args(["rev-parse", "--verify", "--quiet", reference])
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
/// Turn a failed git invocation into an error whose `next:` names what actually went wrong.
///
/// A single catch-all hint is worse than no hint: it covers several unrelated failures, so a
/// reader cannot tell which situation they are in — a dirty worktree that blocks a merge, a
/// branch pair that can no longer fast-forward, a real conflict, or something else entirely —
/// and one of those recourses is wrong for every other cause. Git's own diagnosis is always
/// quoted verbatim; what this classification adds is cowshed's recourse for the cause git
/// names, in cowshed's vocabulary (`cowshed rebase`, not git's merge menu).
fn require_git_success(operation: &str, output: &std::process::Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let message = format!("{operation} failed: {stderr}");
    // Each marker below is git naming a distinct situation; each gets the recourse for that
    // situation and no other. Anything unclassified keeps git's words plus the generic
    // inspect-state fallback rather than guessing.
    Err(
        if stderr.contains("CONFLICT")
            || stderr.contains("Automatic merge failed")
            || stderr.contains("could not apply")
            || stderr.contains("needs merge")
        {
            CowshedError::conflict(message, "resolve the git conflict and retry")
        } else if stderr.contains("would be overwritten by merge")
            || stderr.contains("would be overwritten by checkout")
            || stderr.contains("untracked working tree files would be overwritten")
            || stderr.contains("cannot rebase: Your index contains uncommitted changes")
        {
            CowshedError::conflict(
                message,
                "the target tree has uncommitted work: commit or discard it there, then retry",
            )
        } else if stderr.contains("Not possible to fast-forward")
            || stderr.contains("Diverging branches")
        {
            CowshedError::conflict(
                message,
                "the workspace base is behind the target: rebase first (cowshed rebase <ws>), then retry land",
            )
        } else {
            CowshedError::conflict(message, "inspect the repository state: git status")
        },
    )
}

#[cfg(target_os = "macos")]
/// Read back the bounded tail of a finished job's stderr for diagnostic purposes.
///
/// Best effort by design: a check whose output cannot be read still fails with its exit
/// status; the tail only sharpens the message. The log API bounds each read, so a chatty
/// check cannot balloon this error.
async fn read_job_stderr_tail(
    handle: &crate::runtime::supervisor::WorkspaceSupervisorHandle,
    job_id: JobId,
) -> String {
    use crate::runtime::supervisor::OutputStream;
    let mut collected = Vec::new();
    let mut offset = 0_u64;
    while let Ok(chunk) = handle
        .log_read(job_id, OutputStream::Stderr, offset, false)
        .await
    {
        collected.extend_from_slice(&chunk.bytes);
        offset = chunk.next_offset;
        if chunk.eof || collected.len() >= DIAGNOSTIC_STDERR_LIMIT {
            break;
        }
    }
    if collected.len() > DIAGNOSTIC_STDERR_LIMIT {
        collected.drain(..collected.len() - DIAGNOSTIC_STDERR_LIMIT);
    }
    String::from_utf8_lossy(&collected).trim().to_owned()
}

#[cfg(target_os = "macos")]
/// The bound on child stderr kept in a land-check/exec diagnostic.
const DIAGNOSTIC_STDERR_LIMIT: usize = 2048;

#[cfg(target_os = "macos")]
/// Recognize a Seatbelt-class denial in a child's own stderr, quoted verbatim in the
/// resulting diagnostic.
///
/// The kernel surfaces a sandboxed denial to the child as EPERM ("Operation not permitted");
/// the same command outside the sandbox succeeds. That signature is what distinguishes "the
/// environment refused this" from "the workspace's code failed" — the misreporting that sends
/// callers to fix working code. Matching the child's words keeps this honest: no signature,
/// no environment claim.
fn sandbox_denial_in(stderr: &str) -> Option<String> {
    const DENIAL_MARKERS: [&str; 3] = [
        "Operation not permitted",
        "operation not permitted",
        "Permission denied",
    ];
    DENIAL_MARKERS.iter().find_map(|marker| {
        stderr
            .lines()
            .find(|line| line.contains(marker))
            .map(str::to_owned)
    })
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
    mount: PathBuf,
    main_mount: PathBuf,
) -> Result<crate::sandbox::SandboxConfig> {
    Ok(crate::sandbox::SandboxConfig {
        home: home.to_path_buf(),
        mount_root: layout.project().host_mount_root.clone(),
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
        // The supervisor is the trusted tier of the same workspace; it gets the same daemon reach
        // as the children it launches, or an in-workspace evaluation would depend on which tier ran
        // it. The sccache server socket rides along for the same reason.
        allowed_unix_sockets: crate::sandbox::nix_daemon_socket()
            .into_iter()
            .chain([crate::sandbox::sccache_server_socket()])
            .collect(),
        additional_denies: vec![
            layout.project().project_root.clone(),
            telemetry_root.to_path_buf(),
        ],
        git_worktree_repository: git_worktree_repository(&current.metadata, main_mount),
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
        let role = if metadata.workspace.is_main() {
            WorkspaceRole::Main
        } else {
            WorkspaceRole::Workspace
        };
        let revision = Revision::new(metadata.grants.revision);
        let workspace = LifecycleWorkspace::new(
            metadata.repo_id,
            metadata.workspace,
            metadata.workspace_incarnation,
            revision,
            revision,
            role,
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

    #[test]
    fn retired_main_trash_preserves_main_role_for_restart_reclamation() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-retired-main-recovery-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let project_root = root.join("acme/widget");
        let trash = project_root.join("sessions/.trash");
        std::fs::create_dir_all(&trash).unwrap();
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let incarnation = WorkspaceIncarnation::new("2198f2c0b7e34dc795f17b238b331c80").unwrap();
        let image = trash.join(format!("main-{}.sparseimage", incarnation.as_str()));
        std::fs::write(&image, b"retired main image").unwrap();
        let mut grants =
            GrantSet::closed_baseline(Some(PortBlock::new(49_168, 16).unwrap())).unwrap();
        grants.revision = 8;
        DetachedWorkspaceMetadata {
            version: METADATA_VERSION,
            repo_id: repo_id.clone(),
            workspace: WorkspaceName::new("main").unwrap(),
            workspace_incarnation: incarnation.clone(),
            image_format: ImageFormat::Sparse,
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
            retired[0].workspace().role(),
            crate::metadata::WorkspaceRole::Main
        );
        assert_eq!(
            retired[0].resulting_revision(),
            crate::storage::lifecycle::Revision::new(9)
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
        crate::storage::apfs::RestoreExecutionError::Storage(error) => native_storage_error(error),
        crate::storage::apfs::RestoreExecutionError::Activation { source: error, .. } => {
            native_storage_error(*error)
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

/// Read a capacity the way both image verbs spell it, refusing anything the tools would have to
/// round or reinterpret before the caller's workspace is touched.
#[cfg(target_os = "macos")]
fn parse_capacity(value: &str) -> Result<crate::metadata::ImageCapacity> {
    crate::metadata::ImageCapacity::parse(value).map_err(|error| {
        CowshedError::usage(
            error.to_string(),
            "use a capacity such as 100g, 200g, or 1t",
        )
    })
}

/// The ancestor incarnations a workspace's records may carry, read from the marker the image
/// itself holds (the clone source's lineage plus the source, written when the incarnation was
/// minted). A marker from before lineage was recorded is healed once: its ancestors are exactly
/// the foreign origins already in its records — every one was admitted by the controller that
/// wrote it — so the marker is rewritten with them and is strict from then on.
#[cfg(target_os = "macos")]
fn workspace_lineage(
    mount: &Path,
    current: &WorkspaceIncarnation,
    retained_recovery_budget_bytes: usize,
) -> Result<std::collections::BTreeSet<WorkspaceIncarnation>> {
    let marker_path = mount.join(crate::storage::WORKSPACE_MARKER_PATH);
    let mut marker = crate::metadata::WorkspaceMarker::read_from(&marker_path)
        .map_err(|error| CowshedError::integrity(error.to_string(), "cowshed doctor --json"))?;
    if marker.workspace_incarnation != *current {
        return Err(CowshedError::integrity(
            format!(
                "workspace marker names incarnation {} but the inventory says {current}",
                marker.workspace_incarnation
            ),
            "cowshed doctor --json",
        ));
    }
    if marker.lineage.is_none() {
        let recorded = crate::storage::job_artifact::recorded_historical_incarnations(
            mount,
            current,
            retained_recovery_budget_bytes,
        )
        .map_err(|error| CowshedError::integrity(error.to_string(), "cowshed doctor --json"))?;
        marker.lineage = Some(recorded.into_iter().collect());
        marker
            .validate()
            .map_err(|error| CowshedError::integrity(error.to_string(), "cowshed doctor --json"))?;
        // Persisting the healed marker is an optimization — the next open recomputes the same
        // lineage from the same records — so a write failure (a full disk, a read-only mount)
        // must not take the workspace down with it.
        let _ = crate::metadata::write_json(&marker_path, &marker);
    }
    Ok(marker.lineage.unwrap_or_default().into_iter().collect())
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
            "repair the image or gateway evidence and retry restore",
        ),
        // Asking to shrink, or to resize to the size it already is, is a mistake in the request,
        // not a broken host: report it as usage so the caller is told to name a larger capacity.
        error @ crate::storage::apfs::ApfsStorageError::CapacityNotGrowing { .. } => {
            CowshedError::usage(
                error.to_string(),
                "cowshed resize <workspace> <capacity larger than the current one>",
            )
        }
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
fn main_name() -> WorkspaceName {
    WorkspaceName::new("main").expect("fixed main workspace name is valid")
}

/// Every refusal the removal path can answer with, in one place.
///
/// They are free functions rather than inline `CowshedError::conflict` calls for one reason: the
/// invariant that *no removal refusal may name the flag that overrides it* is only enforceable if
/// the refusals can be enumerated and swept. Tonight's incident was a coordinator script that
/// learned `--force` from a refusal that prescribed it, so the hints here name safe remedies only —
/// land it, commit it, finish the merge — and the destructive flag is documented where a human
/// reads options deliberately, in `cowshed rm`'s usage text.
#[cfg(target_os = "macos")]
fn removal_in_progress_refusal(workspace: &WorkspaceName, operation: &str) -> CowshedError {
    CowshedError::conflict(
        format!("workspace {workspace} has an in-progress {operation} Git operation"),
        "finish or abort the Git operation, then retry",
    )
}

#[cfg(target_os = "macos")]
fn removal_dirty_refusal(workspace: &WorkspaceName) -> CowshedError {
    CowshedError::conflict(
        format!("workspace {workspace} has uncommitted Git work"),
        format!("commit the work and land it: cowshed land {workspace}"),
    )
}

/// The one place that decides whether a removal may destroy a session's object store.
///
/// Pure, and separated from the measurement on purpose: this is the decision an incident turned
/// into a gate, and a decision worth testing directly is worth being able to test without a
/// substrate. `Some` means the caller authorized an abandonment and there is genuinely something to
/// bundle before deleting.
#[cfg(target_os = "macos")]
fn removal_landed_decision(
    workspace: &WorkspaceName,
    head: &GitOid,
    landed: NativeLandedState,
    abandon: bool,
) -> Result<Option<NativeLandedState>> {
    if landed.commits.fully_landed() {
        return Ok(None);
    }
    if abandon {
        return Ok(Some(landed));
    }
    Err(removal_unlanded_refusal(workspace, head, &landed))
}

/// The gate the incident turned on: these commits exist nowhere but the image about to be deleted.
#[cfg(target_os = "macos")]
fn removal_unlanded_refusal(
    workspace: &WorkspaceName,
    head: &GitOid,
    landed: &NativeLandedState,
) -> CowshedError {
    let branch = &landed.branch;
    CowshedError::conflict(
        match &landed.commits {
            LandingCommits::Measured {
                target_head,
                unlanded,
                ..
            } => format!(
                "workspace {workspace} head {head} carries {unlanded} commit{} that {branch} does \
                 not hold, by ancestry or by patch equivalence ({branch} is at {target_head})",
                if *unlanded == 1 { "" } else { "s" }
            ),
            // Saying which question went unanswered is the whole value of this branch: the caller
            // is being refused for a missing proof, not for work they can see.
            LandingCommits::Indeterminate { reason } => format!(
                "workspace {workspace} head {head} cannot be proven to be in {branch}: {reason}"
            ),
        },
        format!("land the workspace: cowshed land {workspace}"),
    )
}

/// A refused slot binding is the caller's problem, not the store's: the slot is taken, or the
/// workspace already has one. Only genuine record damage becomes an integrity error.
#[cfg(target_os = "macos")]
fn slot_binding_error(error: crate::storage::StorageLayoutError) -> CowshedError {
    use crate::metadata::MetadataError;
    use crate::storage::StorageLayoutError;

    match &error {
        StorageLayoutError::Metadata(
            MetadataError::SlotAlreadyBound { .. }
            | MetadataError::WorkspaceAlreadySlotted { .. }
            | MetadataError::MainIsNotSlottable
            | MetadataError::SlotOutOfRange(_),
        ) => CowshedError::conflict(error.to_string(), "choose a free slot: cowshed ls"),
        _ => native_integrity_error(error),
    }
}

#[cfg(target_os = "macos")]
fn removal_head_moved_refusal(
    workspace: &WorkspaceName,
    from: &GitOid,
    to: &GitOid,
) -> CowshedError {
    CowshedError::conflict(
        format!("workspace {workspace} HEAD changed from {from} to {to} during removal"),
        "review the new HEAD and retry removal",
    )
}

/// Removing main without `--restore` throws the project's warm image away for good, so the gate
/// points at the mode that recovers the pre-adoption checkout instead.
#[cfg(target_os = "macos")]
fn main_removal_mode_refusal() -> CowshedError {
    CowshedError::conflict(
        "removing main without --restore destroys this project's warm main image",
        "recover the pre-adoption checkout instead: cowshed rm main --restore",
    )
}

/// The repository a git-worktree workspace's sandbox must reach into, if it is one.
///
/// Narrowed to `.git`: the workspace needs main's object store and its own administrative
/// directory, and nothing in main's working tree.
#[cfg(target_os = "macos")]
fn git_worktree_repository(
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
    main_mount: PathBuf,
) -> Option<PathBuf> {
    is_git_worktree(metadata).then(|| main_mount.join(".git"))
}

/// Re-aim a git-worktree workspace's registration at main's current mount, both directions.
///
/// `git worktree repair`, run from main, is the primitive for exactly this: it rewrites the
/// pointer file in the worktree and the `gitdir` file in main's administrative directory from
/// whichever of the two is still intact, which is what makes it correct whether main moved or the
/// workspace did.
#[cfg(target_os = "macos")]
async fn repair_git_worktree_link(main_mount: &Path, mount: &Path) -> Result<()> {
    crate::git::GitRepository::from_root(main_mount)
        .repair_linked_worktree(mount)
        .await
}

/// Refuse `checkpoint` and `restore` on a git-worktree workspace.
///
/// Its image is not self-contained: the tree is here and the history is in main. A checkpoint
/// clone would capture half a repository, and restoring one would resurrect a registration for a
/// worktree id main has since pruned, quietly claiming a branch another workspace may now hold.
/// The refusal lands before any quota read or barrier, because nothing about the workspace's size
/// or its supervisor changes the answer, and the hints name the two honest substitutes.
#[cfg(target_os = "macos")]
fn require_checkpointable(
    name: &WorkspaceName,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
    verb: &str,
) -> Result<()> {
    if !is_git_worktree(metadata) {
        return Ok(());
    }
    Err(CowshedError::conflict(
        format!(
            "git-worktree workspace {name} cannot {verb}: its history lives in main, not in its image"
        ),
        format!(
            "commit and cowshed land {name}, or cowshed new <name> for a checkpointable workspace"
        ),
    ))
}

/// Whether this workspace is a registered linked worktree of main's repository.
///
/// Read from the store-side sidecar, so it answers while the workspace is detached — which is
/// exactly when retirement and `gc` need it. A sidecar too old to carry the field describes a
/// workspace minted before the mode existed, and those are standalone.
#[cfg(target_os = "macos")]
fn is_git_worktree(metadata: &crate::metadata::DetachedWorkspaceMetadata) -> bool {
    metadata
        .info_snapshot
        .as_ref()
        .is_some_and(|info| info.git_worktree)
}

/// Decide which path a checkout-identity check should actually inspect.
///
/// Exactly one symlink is legitimate at a checkout path: the one adoption plants there, aimed at
/// main's own mount. It is accepted only when it resolves to precisely that mount, and the
/// identity checks then run against the resolved target. Every other symlink is an unrelated path
/// standing where the checkout belongs, and stays a conflict — the check narrows, never inverts.
#[cfg(target_os = "macos")]
async fn resolve_checkout_identity_path(
    path: &Path,
    path_metadata: &std::fs::Metadata,
    main_mount: &Path,
    description: &str,
) -> Result<PathBuf> {
    if path_metadata.file_type().is_symlink() {
        let target = tokio::fs::canonicalize(path).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} is a symlink that does not resolve"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        let canonical_main = tokio::fs::canonicalize(main_mount).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} cannot be compared against main's mount"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        if target != canonical_main {
            return Err(CowshedError::conflict(
                format!("{description} is a symlink to something other than main's mount"),
                "move the unrelated symlink aside and retry",
            ));
        }
        return Ok(target);
    }
    if path_metadata.file_type().is_dir() {
        return Ok(path.to_owned());
    }
    Err(CowshedError::conflict(
        format!("{description} is not the exact retained checkout directory"),
        "restore the exact .pre-cowshed tree or move the collision aside",
    ))
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

/// The git-worktree decisions are all read off one store-side fact, so they are tested off one
/// too: a sidecar. Nothing here needs a mount, which is the point — every one of these answers has
/// to be available while the workspace is detached.
#[cfg(all(test, target_os = "macos"))]
mod git_worktree_tests {
    use super::*;
    use crate::metadata::{
        DetachedWorkspaceMetadata, GrantSet, ImageFormat, METADATA_VERSION, Platform, PortBlock,
        PublicationState, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceRole,
    };

    fn sidecar(git_worktree: bool) -> DetachedWorkspaceMetadata {
        DetachedWorkspaceMetadata {
            version: METADATA_VERSION,
            repo_id: RepoId::parse("acme/widget").expect("repo identity"),
            workspace: WorkspaceName::new("raven").expect("workspace name"),
            workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .expect("incarnation"),
            image_format: ImageFormat::Asif,
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-13T00:00:00Z".to_owned(),
            grants: GrantSet::closed_baseline(Some(
                PortBlock::new(40_960, 16).expect("port block"),
            ))
            .expect("grants"),
            info_snapshot: Some(WorkspaceInfoSnapshot {
                project_root: PathBuf::from("/project"),
                role: WorkspaceRole::Workspace,
                base_commit: "0123456789abcdef".to_owned(),
                branch: Some("cowshed/raven".to_owned()),
                created_at: "2026-07-13T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-13T00:00:00Z".to_owned(),
                stale: false,
                git_worktree,
            }),
        }
    }

    #[test]
    fn checkpoint_and_restore_refuse_a_git_worktree_workspace_and_name_both_substitutes() {
        let name = WorkspaceName::new("raven").expect("workspace name");
        require_checkpointable(&name, &sidecar(false), "checkpoint")
            .expect("a standalone workspace checkpoints");
        require_checkpointable(&name, &sidecar(false), "restore")
            .expect("a standalone workspace restores");

        for verb in ["checkpoint", "restore"] {
            let error = require_checkpointable(&name, &sidecar(true), verb)
                .expect_err("a git-worktree workspace must refuse");
            // Exit 4: the workspace is fine, the operation is the thing that cannot be done.
            assert_eq!(error.exit_code(), 4);
            assert!(error.message.contains(verb));
            // Both honest substitutes: keep the work, or mint something checkpointable.
            assert!(error.hint.contains("cowshed land raven"), "{}", error.hint);
            assert!(error.hint.contains("cowshed new"), "{}", error.hint);
        }
    }

    #[test]
    fn only_a_git_worktree_workspace_reaches_into_mains_repository() {
        let main_mount = PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/main");
        assert_eq!(
            git_worktree_repository(&sidecar(true), main_mount.clone()),
            Some(main_mount.join(".git"))
        );
        assert_eq!(git_worktree_repository(&sidecar(false), main_mount), None);
    }

    /// A sidecar written before the mode existed describes a standalone workspace, and must read
    /// as one rather than failing closed: the absent field is an answer, not a gap.
    #[test]
    fn a_sidecar_without_the_field_is_a_standalone_workspace() {
        let mut wire = serde_json::to_value(sidecar(false)).expect("encode sidecar");
        wire["infoSnapshot"]
            .as_object_mut()
            .expect("info snapshot object")
            .remove("gitWorktree");
        let decoded: DetachedWorkspaceMetadata =
            serde_json::from_value(wire).expect("decode legacy sidecar");
        assert!(!is_git_worktree(&decoded));
    }
}

#[cfg(all(test, target_os = "macos"))]
mod workspace_origin_tests {
    use super::*;
    use crate::metadata::{
        DetachedWorkspaceMetadata, GrantSet, ImageFormat, METADATA_VERSION, Platform, PortBlock,
        PublicationState, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceMarker,
        WorkspaceRole,
    };
    use crate::storage::lifecycle::{
        DerivedWorkspace, LifecycleWorkspace, MountState, Revision, StorageFact,
    };

    fn temp_directory(test: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "cowshed-origin-{test}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("temp directory");
        path
    }

    fn write_marker(root: &Path, workspace: &str, role: WorkspaceRole, project_root: &Path) {
        let path = root.join(crate::storage::WORKSPACE_MARKER_PATH);
        std::fs::create_dir_all(path.parent().expect("marker parent")).expect("marker directory");
        crate::metadata::write_json(
            &path,
            &WorkspaceMarker {
                version: METADATA_VERSION,
                repo_id: RepoId::parse("acme/widget").expect("repo"),
                project_root: project_root.to_owned(),
                workspace: WorkspaceName::new(workspace).expect("workspace"),
                workspace_incarnation: WorkspaceIncarnation::new(
                    "00000000000000000000000000000001",
                )
                .expect("incarnation"),
                role,
                image_format: ImageFormat::Asif,
                base_commit: "0123456789abcdef".to_owned(),
                created_at: "2026-07-13T00:00:00Z".to_owned(),
                forked_from: None,
                created_trace: "fixture".to_owned(),
                lineage: Some(Vec::new()),
            },
        )
        .expect("write marker");
    }

    fn incarnation(value: &str) -> WorkspaceIncarnation {
        WorkspaceIncarnation::new(value).expect("incarnation")
    }

    fn lifecycle_workspace(
        workspace: &str,
        incarnation: WorkspaceIncarnation,
        role: WorkspaceRole,
    ) -> LifecycleWorkspace {
        LifecycleWorkspace::new(
            RepoId::parse("acme/widget").expect("repo"),
            WorkspaceName::new(workspace).expect("workspace"),
            incarnation,
            Revision::new(1),
            Revision::new(1),
            role,
            ImageFormat::Asif,
        )
        .expect("lifecycle workspace")
    }

    fn main_metadata(
        project_root: &Path,
        incarnation: WorkspaceIncarnation,
    ) -> DetachedWorkspaceMetadata {
        DetachedWorkspaceMetadata {
            version: METADATA_VERSION,
            repo_id: RepoId::parse("acme/widget").expect("repo"),
            workspace: WorkspaceName::new("main").expect("main"),
            workspace_incarnation: incarnation,
            image_format: ImageFormat::Asif,
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-13T00:00:00Z".to_owned(),
            grants: GrantSet::closed_baseline(Some(
                PortBlock::new(49_152, 16).expect("port block"),
            ))
            .expect("grants"),
            info_snapshot: Some(WorkspaceInfoSnapshot {
                project_root: project_root.to_owned(),
                role: WorkspaceRole::Main,
                base_commit: "0123456789abcdef".to_owned(),
                branch: None,
                created_at: "2026-07-13T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-13T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            }),
        }
    }

    /// A coordinator verb invoked from inside a session workspace must open the project, not be
    /// refused. The session's marker records main's checkout — a different directory than the mount
    /// the caller stands in — and that difference is the normal case, not a mismatch.
    #[tokio::test]
    async fn a_session_mount_names_its_project_and_reports_mains_checkout() {
        let temp = temp_directory("session");
        let checkout = temp.join("checkout");
        let mount = temp.join("mnt/task");
        std::fs::create_dir_all(&checkout).expect("checkout");
        std::fs::create_dir_all(&mount).expect("mount");
        write_marker(&mount, "task", WorkspaceRole::Workspace, &checkout);

        let origin = workspace_origin_from_marker(&mount)
            .await
            .expect("a session marker identifies its project")
            .expect("marker present");
        assert_eq!(origin.repo_id, RepoId::parse("acme/widget").expect("repo"));
        assert_eq!(
            origin.project_root, checkout,
            "the project checkout comes from the marker, never from the invocation directory"
        );

        std::fs::remove_dir_all(&temp).ok();
    }

    #[tokio::test]
    async fn a_session_resolves_a_missing_main_from_the_store_without_opening_old_git() {
        let temp = temp_directory("stale-main-binding");
        let store = temp.join("store");
        let missing_checkout = temp.join("missing-main");
        let session = temp.join("mnt/slot@1");
        std::fs::create_dir_all(&session).expect("session mount");
        write_marker(
            &session,
            "task",
            WorkspaceRole::Workspace,
            &missing_checkout,
        );
        let origin = workspace_origin_from_marker(&session)
            .await
            .expect("session marker")
            .expect("origin");
        let repo_id = RepoId::parse("acme/widget").expect("repo");
        let layout = crate::storage::StorageLayout::new(&store, &repo_id).expect("layout");
        std::fs::create_dir_all(&layout.project().project_root).expect("project store");
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id.clone(),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://example.test/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        crate::metadata::write_json(&layout.project().repository_binding, &binding)
            .expect("persist binding");

        let (resolved_repo, _, resolved_binding) =
            project_binding_from_workspace_origin(&store, &session, Some(&origin))
                .await
                .expect("store binding resolves a detached main")
                .expect("session roots differ");
        assert_eq!(resolved_repo, repo_id);
        assert_eq!(resolved_binding, binding);
        assert!(
            !missing_checkout.exists(),
            "the old checkout path remains absent; resolving identity never opens it"
        );

        std::fs::remove_dir_all(&temp).ok();
    }

    #[tokio::test]
    async fn detached_main_relocation_accepts_retired_roots_after_session_identity_validation() {
        let temp = temp_directory("retired-roots");
        let persisted_old_root = temp.join("historical-a");
        let retired_controller_root = temp.join("historical-b");
        let session = temp.join("mnt/slot@1");
        let destination = temp.join("destination-d");
        let store = temp.join("store");
        std::fs::create_dir_all(&session).expect("session mount");
        write_marker(
            &session,
            "task",
            WorkspaceRole::Workspace,
            &retired_controller_root,
        );

        let origin = workspace_origin_from_marker(&session)
            .await
            .expect("session marker")
            .expect("origin");
        let repo_id = RepoId::parse("acme/widget").expect("repo");
        let layout =
            crate::storage::StorageLayout::with_mount_root(&store, temp.join("mnt"), &repo_id)
                .expect("layout");
        std::fs::create_dir_all(&layout.project().project_root).expect("project store");
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id.clone(),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://example.test/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        let mismatched_binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: RepoId::parse("other/widget").expect("other repo"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://example.test/other/widget.git".to_owned()),
            primary: true,
        }])
        .expect("mismatched binding");
        crate::metadata::write_json(&layout.project().repository_binding, &mismatched_binding)
            .expect("persist mismatched binding");
        project_binding_from_workspace_origin(&store, &session, Some(&origin))
            .await
            .expect_err("marker and binding repository identities must agree");
        crate::metadata::write_json(&layout.project().repository_binding, &binding)
            .expect("persist binding");
        project_binding_from_workspace_origin(&store, &session, Some(&origin))
            .await
            .expect("store binding")
            .expect("session invocation");

        let session_fact = StorageFact {
            workspace: lifecycle_workspace(
                "task",
                incarnation("00000000000000000000000000000001"),
                WorkspaceRole::Workspace,
            ),
            volume_key: "disk-session".to_owned(),
        };
        let stale_session_fact = StorageFact {
            workspace: lifecycle_workspace(
                "task",
                incarnation("00000000000000000000000000000009"),
                WorkspaceRole::Workspace,
            ),
            volume_key: "disk-stale-session".to_owned(),
        };
        validate_workspace_origin_against_inventory(&origin, &[&stale_session_fact])
            .expect_err("marker and active storage incarnations must agree");
        validate_workspace_origin_against_inventory(&origin, &[&session_fact])
            .expect("marker repo, workspace, and incarnation match storage");

        let session_incarnation = incarnation("00000000000000000000000000000001");
        let session_workspace = DerivedWorkspace {
            workspace: lifecycle_workspace(
                "task",
                session_incarnation.clone(),
                WorkspaceRole::Workspace,
            ),
            mount_state: MountState::Mounted { mount_id: 7 },
            checkpoints: Vec::new(),
        };
        let mut session_metadata = main_metadata(&persisted_old_root, session_incarnation);
        session_metadata.workspace = WorkspaceName::new("task").expect("task");
        session_metadata
            .info_snapshot
            .as_mut()
            .expect("snapshot")
            .role = WorkspaceRole::Workspace;
        validate_workspace_controller_root(
            &session_workspace,
            &session_metadata,
            &retired_controller_root,
            crate::metadata::CheckoutLayout::DirectMount,
            ProjectRootValidation::Strict,
        )
        .expect("session sidecars do not claim the controller checkout");

        let main_incarnation = incarnation("00000000000000000000000000000002");
        let main = DerivedWorkspace {
            workspace: lifecycle_workspace("main", main_incarnation.clone(), WorkspaceRole::Main),
            mount_state: MountState::Detached,
            checkpoints: Vec::new(),
        };
        let metadata = main_metadata(&persisted_old_root, main_incarnation);
        validate_workspace_controller_root(
            &main,
            &metadata,
            &retired_controller_root,
            crate::metadata::CheckoutLayout::DirectMount,
            ProjectRootValidation::AllowDetachedMainRelocation,
        )
        .expect("explicit detached-main relocation accepts retired roots");

        let image = layout
            .main_image(ImageFormat::Asif)
            .expect("main paths")
            .image()
            .to_owned();
        std::fs::write(&image, b"main image").expect("image");
        metadata.write_for_image(&image).expect("sidecar");
        let record = crate::checkout::CheckoutRecord {
            mount_point: retired_controller_root.clone(),
            image,
        };
        let retired_main_mount = layout
            .workspace_mount(&WorkspaceName::new("main").expect("main"))
            .expect("retired main mount");
        std::os::unix::fs::symlink(&retired_main_mount, &destination)
            .expect("retired checkout link");
        classify_move_destination(&destination, &[])
            .expect_err("only detached direct-main relocation may replace the retired link");
        let targets = known_retired_main_targets(&retired_main_mount, &temp, &binding)
            .expect("known retired roots");
        let destination_state = classify_move_destination(&destination, &targets)
            .expect("exact dangling retired main link");
        prepare_detached_checkout_relocation(
            &record,
            &layout,
            crate::metadata::CheckoutLayout::DirectMount,
            &retired_controller_root,
            &destination,
            &destination_state,
        )
        .expect("relocate detached main");

        assert!(
            destination.is_dir(),
            "the explicit destination becomes the mountpoint"
        );
        assert_eq!(
            DetachedWorkspaceMetadata::read_for_image(&record.image)
                .expect("updated sidecar")
                .require_info_snapshot()
                .expect("main snapshot")
                .project_root,
            destination
        );
        assert_eq!(
            layout.checkout_layout().expect("updated layout"),
            crate::metadata::CheckoutLayout::DirectMount
        );
        assert!(
            !std::fs::symlink_metadata(&destination)
                .expect("destination metadata")
                .file_type()
                .is_symlink(),
            "the retired symlink is replaced rather than followed"
        );
        assert!(
            !retired_controller_root.exists(),
            "the absent retired checkout is not recreated"
        );
        std::fs::remove_dir_all(&temp).ok();
    }

    #[test]
    fn detached_relocation_accepts_home_root_legacy_main_symlink() {
        let temp = temp_directory("home-root-retired-link");
        let source = temp.join("missing-retired-checkout");
        let destination = temp.join("explicit-destination");
        let store = temp.join("store");
        let repo_id = RepoId::parse("example-org/example-app").expect("live repository identity");
        let layout = crate::storage::StorageLayout::with_mount_root(
            &store,
            temp.join("current-mnt"),
            &repo_id,
        )
        .expect("layout");
        std::fs::create_dir_all(&layout.project().project_root).expect("project store");
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id,
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://example.test/example-org/example-app.git".to_owned()),
            primary: true,
        }])
        .expect("validated live binding");
        let current_main_mount = layout
            .workspace_mount(&WorkspaceName::new("main").expect("main"))
            .expect("current main mount");
        let invoking_home = Path::new("/Users/alice");
        let historical_main_mount =
            Path::new("/Users/alice/.cowshed/mnt/example-org/example-app/main");
        let targets = known_retired_main_targets(&current_main_mount, invoking_home, &binding)
            .expect("same-repository retired roots");
        assert!(targets.contains(&historical_main_mount.to_owned()));
        assert!(targets.contains(
            &Path::new("/private/cowshed/store/mnt/example-org/example-app/main").to_owned()
        ));
        assert_eq!(
            targets.len(),
            3,
            "all distinct historical forms are retained"
        );
        assert_eq!(
            known_retired_main_targets(historical_main_mount, invoking_home, &binding)
                .expect("duplicate home and current root"),
            vec![
                historical_main_mount.to_owned(),
                Path::new("/private/cowshed/store/mnt/example-org/example-app/main").to_owned(),
            ],
            "an unchanged current home root is listed only once"
        );

        let image = layout
            .main_image(ImageFormat::Asif)
            .expect("main paths")
            .image()
            .to_owned();
        std::fs::write(&image, b"main image").expect("image");
        let mut metadata = main_metadata(&source, incarnation("00000000000000000000000000000004"));
        metadata.repo_id = binding.primary().expect("primary binding").repo_id.clone();
        metadata.write_for_image(&image).expect("sidecar");
        let record = crate::checkout::CheckoutRecord {
            mount_point: source.clone(),
            image,
        };
        std::os::unix::fs::symlink(historical_main_mount, &destination)
            .expect("exact live historical checkout link");
        let destination_state = classify_move_destination(&destination, &targets)
            .expect("same-repository home-root link");

        prepare_detached_checkout_relocation(
            &record,
            &layout,
            crate::metadata::CheckoutLayout::DirectMount,
            &source,
            &destination,
            &destination_state,
        )
        .expect("explicit destination replaces the retired link");

        assert!(destination.is_dir());
        assert_eq!(
            DetachedWorkspaceMetadata::read_for_image(&record.image)
                .expect("updated sidecar")
                .require_info_snapshot()
                .expect("main snapshot")
                .project_root,
            destination
        );
        std::fs::remove_dir_all(&temp).ok();
    }

    #[test]
    fn retired_link_targets_are_compared_by_canonical_lexical_spelling() {
        let temp = temp_directory("retired-link-lexical");
        let destination = temp.join("destination");
        let retired_main_mount = temp.join("mnt/acme/widget/main");
        let raw_target = temp.join("mnt/acme/../acme/widget/./main");

        std::os::unix::fs::symlink(&raw_target, &destination).expect("lexical retired link");
        assert_eq!(
            classify_move_destination(&destination, std::slice::from_ref(&retired_main_mount))
                .expect("lexically equivalent dangling target"),
            MoveDestination::ReplaceDanglingLegacySymlink {
                target: retired_main_mount,
            }
        );

        std::fs::remove_dir_all(&temp).ok();
    }

    #[test]
    fn detached_relocation_rejects_unrelated_or_live_symlinks() {
        let temp = temp_directory("retired-link-conflicts");
        let destination = temp.join("destination");
        let retired_main_mount = temp.join("mnt/acme/widget/main");
        let unrelated = temp.join("unrelated-missing");

        std::os::unix::fs::symlink(&unrelated, &destination).expect("unrelated link");
        classify_move_destination(&destination, std::slice::from_ref(&retired_main_mount))
            .expect_err("an arbitrary dangling link remains an occupant");
        std::fs::remove_file(&destination).expect("remove unrelated link");

        std::fs::create_dir_all(&retired_main_mount).expect("live retired-layout mount");
        std::os::unix::fs::symlink(&retired_main_mount, &destination)
            .expect("link to live retired mount");
        classify_move_destination(&destination, std::slice::from_ref(&retired_main_mount))
            .expect_err("a link whose target exists remains an occupant");

        std::fs::remove_dir_all(&temp).ok();
    }

    #[test]
    fn detached_relocation_rejects_ordinary_occupied_destinations() {
        let temp = temp_directory("retired-link-ordinary-occupants");
        let destination = temp.join("destination");
        let retired_main_mount = temp.join("mnt/acme/widget/main");

        std::fs::create_dir(&destination).expect("occupied directory");
        classify_move_destination(&destination, std::slice::from_ref(&retired_main_mount))
            .expect_err("an ordinary directory remains an occupant");
        std::fs::remove_dir(&destination).expect("remove directory");

        std::fs::write(&destination, b"occupant").expect("occupied file");
        classify_move_destination(&destination, std::slice::from_ref(&retired_main_mount))
            .expect_err("an ordinary file remains an occupant");

        std::fs::remove_dir_all(&temp).ok();
    }

    #[test]
    fn mounted_main_still_rejects_disagreeing_persisted_and_controller_roots() {
        let main_incarnation = incarnation("00000000000000000000000000000003");
        let main = DerivedWorkspace {
            workspace: lifecycle_workspace("main", main_incarnation.clone(), WorkspaceRole::Main),
            mount_state: MountState::Mounted { mount_id: 42 },
            checkpoints: Vec::new(),
        };
        let metadata = main_metadata(Path::new("/historical/a"), main_incarnation);

        let error = validate_workspace_controller_root(
            &main,
            &metadata,
            Path::new("/retired/controller/b"),
            crate::metadata::CheckoutLayout::DirectMount,
            ProjectRootValidation::AllowDetachedMainRelocation,
        )
        .expect_err("mounted main retains strict root agreement");
        assert!(
            error
                .to_string()
                .contains("persisted project root /historical/a disagrees with controller root /retired/controller/b"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn mains_marker_must_still_name_the_root_it_sits_in_and_roles_must_agree() {
        let temp = temp_directory("main");
        let checkout = temp.join("checkout");
        std::fs::create_dir_all(&checkout).expect("checkout");

        write_marker(&checkout, "main", WorkspaceRole::Main, &checkout);
        let origin = workspace_origin_from_marker(&checkout)
            .await
            .expect("coherent main marker")
            .expect("marker present");
        assert_eq!(origin.project_root, checkout);

        // Main recorded somewhere it is not: the one project-root disagreement that is still real.
        write_marker(
            &checkout,
            "main",
            WorkspaceRole::Main,
            &temp.join("elsewhere"),
        );
        assert!(workspace_origin_from_marker(&checkout).await.is_err());

        // Role and name disagreeing is corruption in either direction.
        write_marker(&checkout, "task", WorkspaceRole::Main, &checkout);
        assert!(workspace_origin_from_marker(&checkout).await.is_err());
        write_marker(&checkout, "main", WorkspaceRole::Workspace, &checkout);
        assert!(workspace_origin_from_marker(&checkout).await.is_err());

        std::fs::remove_dir_all(&temp).ok();
    }

    /// The dirty-target merge block must be distinguishable from a conflict and from the
    /// generic fallback: its recourse is commit-or-discard in that tree, not "resolve the
    /// conflict" and not bare `git status`.
    #[test]
    fn dirty_target_merge_block_names_its_own_recourse() {
        use std::os::unix::process::ExitStatusExt;

        let failed = |stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        };

        let error = require_git_success(
            "git operation",
            &failed(
                "error: Your local changes to the following files would be overwritten by merge:\n\tREADME.md\nPlease commit your changes or stash them before you merge.",
            ),
        )
        .expect_err("a blocked merge is a failure");
        assert!(
            error.hint.contains("commit or discard"),
            "dirty-target hint must name the remedy: {}",
            error.hint
        );
        assert!(
            error.message.contains("would be overwritten by merge"),
            "git's own diagnosis survives verbatim: {}",
            error.message
        );
        assert!(!error.hint.contains("resolve the git conflict"));

        // The diverged case routes to cowshed's own verb, not git's merge menu.
        let error = require_git_success(
            "git operation",
            &failed(
                "hint: Diverging branches can't be fast-forwarded, you need to either:\nfatal: Not possible to fast-forward, aborting.",
            ),
        )
        .expect_err("diverged land is a failure");
        assert!(
            error.hint.contains("cowshed rebase"),
            "divergence recourse is cowshed's vocabulary: {}",
            error.hint
        );
        assert!(!error.hint.contains("git status"));
    }

    /// The sandbox-denial detector fires on the kernel's EPERM wording (the only evidence a
    /// denied child produces) and stays silent on ordinary failures — a detector that fired
    /// on everything would mislabel every genuine check failure as environmental.
    #[test]
    fn sandbox_denial_detection_fires_on_eperm_and_nothing_else() {
        let denial = sandbox_denial_in(
            "error: failed to run custom build command for `foo`\n  cat: /Users/dev/projects/example-app/Cargo.toml: Operation not permitted",
        )
        .expect("EPERM wording must classify as a denial");
        assert!(denial.contains("Operation not permitted"));

        assert!(sandbox_denial_in("cat: /x: Permission denied").is_some());
        assert!(
            sandbox_denial_in("thread 'main' panicked at src/lib.rs:1:1:\nexplicit panic")
                .is_none(),
            "a plain test failure is not an environment refusal"
        );
        assert!(
            sandbox_denial_in("").is_none(),
            "empty output owes no claim"
        );
    }

    /// A failed git invocation must not be reported as a conflict unless git said so: the phantom
    /// "resolve the git conflict" hint sent users looking for markers and a rebase that never
    /// existed, and hid git's own diagnosis behind it.
    #[test]
    fn only_a_real_conflict_gets_the_conflict_hint() {
        use std::os::unix::process::ExitStatusExt;

        let failed = |stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        };

        let error = require_git_success("rebase", &failed("fatal: invalid upstream 'main/main'"))
            .expect_err("a missing upstream is a failure");
        assert!(
            !error.hint.contains("resolve the git conflict"),
            "a missing upstream is not a conflict: {}",
            error.hint
        );
        assert!(
            error.message.contains("invalid upstream"),
            "git's own diagnosis survives: {}",
            error.message
        );

        let error = require_git_success(
            "rebase",
            &failed("CONFLICT (content): Merge conflict in src/lib.rs"),
        )
        .expect_err("a conflict is a failure");
        assert!(
            error.hint.contains("resolve the git conflict"),
            "{}",
            error.hint
        );
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

    /// A local-path backup remote is ordinary Git and yields no owner/repo. It
    /// must not brick a checkout whose other remotes identify it perfectly well
    /// — that failure reached every command, including read-only ones.
    #[test]
    fn a_remote_without_a_derivable_identity_is_skipped_not_fatal() {
        let remotes = [
            remote("origin", "https://example.com/example-org/example-app.git"),
            remote("backup", "/Volumes/Backup/example-app.git"),
        ];

        let binding =
            binding_from_remotes(&remotes, None).expect("the usable remote identifies it");

        let primary = binding.primary().expect("primary");
        assert_eq!(primary.repo_id, repo_id("example-org/example-app"));
        assert_eq!(primary.remote_name.as_deref(), Some("origin"));
    }

    #[test]
    fn a_checkout_whose_every_remote_is_unusable_reports_what_it_skipped() {
        let remotes = [remote("backup", "/Volumes/Backup/example-app.git")];

        let error =
            binding_from_remotes(&remotes, None).expect_err("nothing identifies the repository");
        assert_eq!(error.code.as_str(), "environment-missing");
        assert!(error.message.contains("backup"), "{}", error.message);

        // An explicit identity is still enough to proceed.
        let requested = repo_id("example-org/example-app");
        let binding =
            binding_from_remotes(&remotes, Some(&requested)).expect("explicit identity suffices");
        assert_eq!(binding.primary().expect("primary").repo_id, requested);
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

    #[cfg(target_os = "macos")]
    #[test]
    fn matching_repository_identity_with_a_renamed_remote_is_a_binding_mismatch() {
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("smoothbricks/codebase"),
            remote_name: Some("codebase".to_owned()),
            remote_url: Some("https://github.com/smoothbricks/codebase.git".to_owned()),
            primary: true,
        }])
        .expect("recorded binding");
        let remotes = [remote(
            "origin",
            "https://github.com/smoothbricks/codebase.git",
        )];

        let error = validate_binding_against_remotes(&binding, &remotes)
            .expect_err("the recorded remote name is part of the binding");

        assert_eq!(error.code, ErrorCode::Conflict);
        assert_eq!(
            error.message,
            "repository binding remote codebase does not match Git configuration"
        );
    }
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn checkout_identity_accepts_only_the_symlink_into_mains_mount() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-checkout-identity-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let main_mount = root.join("mnt").join("main");
        let elsewhere = root.join("elsewhere");
        let checkout = root.join("project");
        std::fs::create_dir_all(&main_mount).expect("main mount");
        std::fs::create_dir_all(&elsewhere).expect("foreign target");

        // The symlink adoption plants: accepted, and it resolves to main's mount.
        std::os::unix::fs::symlink(&main_mount, &checkout).expect("adopted symlink");
        let metadata = std::fs::symlink_metadata(&checkout).expect("symlink metadata");
        let resolved =
            resolve_checkout_identity_path(&checkout, &metadata, &main_mount, "checkout")
                .await
                .expect("adopted symlink is accepted");
        assert_eq!(
            resolved,
            std::fs::canonicalize(&main_mount).expect("canonical main mount")
        );

        // A symlink to anything else stays a conflict.
        std::fs::remove_file(&checkout).expect("clear symlink");
        std::os::unix::fs::symlink(&elsewhere, &checkout).expect("foreign symlink");
        let metadata = std::fs::symlink_metadata(&checkout).expect("symlink metadata");
        resolve_checkout_identity_path(&checkout, &metadata, &main_mount, "checkout")
            .await
            .expect_err("a foreign symlink is not the adopted checkout");

        // A real directory is inspected in place, exactly as before.
        std::fs::remove_file(&checkout).expect("clear symlink");
        std::fs::create_dir_all(&checkout).expect("real checkout");
        let metadata = std::fs::symlink_metadata(&checkout).expect("directory metadata");
        assert_eq!(
            resolve_checkout_identity_path(&checkout, &metadata, &main_mount, "checkout")
                .await
                .expect("a real directory is accepted"),
            checkout
        );

        std::fs::remove_dir_all(&root).expect("fixture cleanup");
    }

    #[tokio::test]
    async fn startup_pending_restore_destination_is_absent_until_restore_fence() {
        use crate::metadata::{ImageFormat, WorkspaceRole};
        use crate::runtime::supervisor::{CommitmentDraft, CommitmentPublisher, CommitmentSink};
        use crate::storage::apfs::PendingPublicationFact;
        use crate::storage::lifecycle::{LifecycleWorkspace, Revision, StorageFact};

        let root = std::env::temp_dir().join(format!(
            "cowshed-project-pending-restore-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let telemetry = root.join("telemetry");
        let repo = repo_id("acme/widget");
        let source = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").expect("source");
        let destination =
            WorkspaceIncarnation::new("1198f2c0b7e34dc795f17b238b331c80").expect("destination");
        let source_workspace = LifecycleWorkspace::new(
            repo.clone(),
            WorkspaceName::new("main").expect("main"),
            source.clone(),
            Revision::new(1),
            Revision::new(11),
            WorkspaceRole::Main,
            ImageFormat::Sparse,
        )
        .expect("source workspace");
        let destination_workspace = LifecycleWorkspace::new(
            repo.clone(),
            WorkspaceName::new("main").expect("main"),
            destination.clone(),
            Revision::new(2),
            Revision::new(11),
            WorkspaceRole::Main,
            ImageFormat::Sparse,
        )
        .expect("destination workspace");
        let facts = vec![
            StorageFact {
                workspace: source_workspace,
                volume_key: "cowshed.acme--widget.main".to_owned(),
            },
            StorageFact {
                workspace: destination_workspace.clone(),
                volume_key: "cowshed.acme--widget.main".to_owned(),
            },
        ];
        let pending = PendingPublicationFact {
            workspace: destination_workspace,
            image: root.join("main.sparseimage"),
            mount_point: root.join("mount"),
            source_checkpoint: "baseline".to_owned(),
            source_incarnation: source.clone(),
            replaced_incarnation: source.clone(),
            destination_incarnation: destination.clone(),
        };
        let pending_slice = std::slice::from_ref(&pending);
        let verified = verified_recovery_facts(&facts, pending_slice);
        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].workspace.incarnation(), &source);

        // The pending destination is not an active fact until its restore fence activates the
        // image; the audit record of the restore is telemetry and gates nothing.
        let mut commitments =
            CommitmentPublisher::open(&telemetry, crate::storage::audit::ContinuityAudit::Arrow, 8)
                .expect("open audit publisher");
        commitments
            .record(CommitmentDraft::Restore {
                repo_id: repo,
                source_checkpoint: pending.source_checkpoint.clone(),
                source_incarnation: pending.source_incarnation.clone(),
                replaced_incarnation: pending.replaced_incarnation.clone(),
                destination_incarnation: pending.destination_incarnation.clone(),
            })
            .await
            .expect("record recovered restore");
        let health = commitments.health().await.expect("audit health");
        assert_eq!((health.recorded, health.failed), (1, 0));
        drop(destination);
        drop(commitments);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(all(test, target_os = "macos"))]
mod port_reservation_tests {
    use super::claim_port_block;
    use std::os::unix::fs::symlink;

    fn root(label: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "cowshed-port-reservation-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn live_reservation_excludes_a_second_allocator_until_release() {
        let root = root("live");
        let first = claim_port_block(&root, 40_960)
            .expect("first claim")
            .expect("reservation");
        assert!(
            claim_port_block(&root, 40_960)
                .expect("second claim")
                .is_none()
        );
        std::fs::remove_file(first).expect("release");
        assert!(
            claim_port_block(&root, 40_960)
                .expect("claim after release")
                .is_some()
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn dead_process_reservation_is_reclaimed() {
        let root = root("stale");
        std::fs::create_dir_all(&root).expect("root");
        let marker = root.join("port-40960.reservation");
        symlink(i32::MAX.to_string(), &marker).expect("stale marker");
        assert!(claim_port_block(&root, 40_960).expect("reclaim").is_some());
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}

#[cfg(all(test, target_os = "macos"))]
mod terminal_project_cleanup_tests {
    use super::clean_terminal_project_storage;

    fn root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "cowshed-terminal-project-{label}-{}",
            uuid::Uuid::new_v4()
        ))
    }

    #[test]
    fn cleanup_removes_only_empty_structure_and_zero_length_locks() {
        let root = root("safe");
        let binding = root.join("repository.json");
        std::fs::create_dir_all(root.join(".staging")).expect("staging");
        std::fs::create_dir_all(root.join("checkpoints/main")).expect("checkpoints");
        std::fs::create_dir_all(root.join("sessions/.trash")).expect("trash");
        std::fs::write(root.join("sessions/raven.sparseimage.lock"), b"").expect("session lock");
        std::fs::write(root.join("main.sparseimage.lock"), b"").expect("main lock");
        std::fs::write(&binding, b"binding").expect("binding");
        std::fs::write(root.join("policy.json"), b"preserve").expect("policy");

        clean_terminal_project_storage(&root, &binding).expect("terminal cleanup");
        assert!(binding.is_file());
        assert_eq!(
            std::fs::read(root.join("policy.json")).expect("policy"),
            b"preserve"
        );
        assert!(!root.join(".staging").exists());
        assert!(!root.join("checkpoints").exists());
        assert!(!root.join("sessions").exists());
        assert!(!root.join("main.sparseimage.lock").exists());
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn cleanup_preserves_and_rejects_an_unreclaimed_image() {
        let root = root("blocked");
        let binding = root.join("repository.json");
        let image = root.join("sessions/.trash/main-retired.sparseimage");
        std::fs::create_dir_all(image.parent().expect("trash")).expect("trash");
        std::fs::write(&image, b"image").expect("retained image");
        std::fs::write(&binding, b"binding").expect("binding");

        let error = clean_terminal_project_storage(&root, &binding)
            .expect_err("unreclaimed image must block binding cleanup");
        assert_eq!(error.code.as_str(), "integrity");
        assert_eq!(std::fs::read(&image).expect("image preserved"), b"image");
        assert!(binding.is_file());
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}

#[cfg(all(test, target_os = "macos"))]
mod adopt_secret_policy_tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::enforce_adopt_secret_policy;
    use crate::error::ErrorCode;
    use crate::secrets::WAIVER_EXAMPLE;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    /// A throwaway repository tree plus the controller-owned paths the policy reads.
    struct PolicyTree(PathBuf);

    impl PolicyTree {
        fn new(name: &str) -> Self {
            let sequence = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "cowshed-adopt-policy-{name}-{}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir_all(&root).expect("temporary policy tree is created");
            Self(root)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn waivers_path(&self) -> PathBuf {
            self.0.join("waivers.json")
        }

        fn write(&self, relative: &str, contents: &str) {
            let path = self.0.join(relative);
            std::fs::create_dir_all(path.parent().expect("parent exists"))
                .expect("fixture parent is created");
            std::fs::write(path, contents).expect("fixture is written");
        }

        fn write_waivers(&self, contents: &str) {
            std::fs::write(self.waivers_path(), contents).expect("waivers file is written");
        }
    }

    impl Drop for PolicyTree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn policy(tree: &PolicyTree, quarantine: bool) -> Result<(), crate::error::CowshedError> {
        enforce_adopt_secret_policy(
            tree.path().to_path_buf(),
            tree.waivers_path(),
            tree.path().join("quarantine"),
            quarantine,
        )
        .await
    }

    #[tokio::test]
    async fn clean_tree_without_a_waivers_file_is_accepted() {
        let tree = PolicyTree::new("clean");
        policy(&tree, false)
            .await
            .expect("no findings and no waivers file means no refusal");
    }

    #[tokio::test]
    async fn findings_refusal_prints_the_complete_waiver_contract() {
        let tree = PolicyTree::new("refusal");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");

        let error = policy(&tree, false)
            .await
            .expect_err("findings must refuse adoption without a waiver");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.message.contains(".env.local"), "{}", error.message);
        for expected in [
            tree.waivers_path().display().to_string(),
            WAIVER_EXAMPLE.to_owned(),
            "exact repository-relative path".to_owned(),
            "non-empty reason".to_owned(),
            "can never hold live credentials".to_owned(),
            "developer-local".to_owned(),
            "retained for audit".to_owned(),
            "--quarantine".to_owned(),
        ] {
            assert!(
                error.hint.contains(&expected),
                "hint must contain {expected:?}: {}",
                error.hint
            );
        }
    }

    #[tokio::test]
    async fn valid_reasoned_waiver_suppresses_blocking() {
        let tree = PolicyTree::new("waived");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write_waivers(
            r#"[{"path": ".env.local", "reason": "intentionally committed synthetic detector fixture"}]"#,
        );

        policy(&tree, false)
            .await
            .expect("an exact, reasoned waiver unblocks adoption");
    }

    #[tokio::test]
    async fn malformed_waivers_file_fails_closed_with_the_contract() {
        let tree = PolicyTree::new("malformed");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write_waivers("{not json");

        let error = policy(&tree, false)
            .await
            .expect_err("a malformed waivers file must fail closed");
        assert_eq!(error.code, ErrorCode::Integrity);
        assert!(
            error
                .message
                .contains(&tree.waivers_path().display().to_string()),
            "{}",
            error.message
        );
        assert!(error.hint.contains(WAIVER_EXAMPLE), "{}", error.hint);
        assert!(error.hint.contains("delete it"), "{}", error.hint);
    }

    #[tokio::test]
    async fn empty_reason_waiver_names_the_file_and_the_contract() {
        let tree = PolicyTree::new("blank-reason");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write_waivers(r#"[{"path": ".env.local", "reason": "   "}]"#);

        let error = policy(&tree, false)
            .await
            .expect_err("a blank reason must not waive anything");
        assert_eq!(error.code, ErrorCode::Integrity);
        assert!(
            error.message.contains("reason is required"),
            "{}",
            error.message
        );
        assert!(
            error
                .hint
                .contains(&tree.waivers_path().display().to_string()),
            "{}",
            error.hint
        );
        assert!(error.hint.contains(WAIVER_EXAMPLE), "{}", error.hint);
    }

    #[tokio::test]
    async fn duplicate_waiver_names_the_file_and_the_contract() {
        let tree = PolicyTree::new("duplicate");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write_waivers(
            r#"[{"path": ".env.local", "reason": "one"}, {"path": ".env.local", "reason": "two"}]"#,
        );

        let error = policy(&tree, false)
            .await
            .expect_err("a duplicate waiver entry must be refused");
        assert_eq!(error.code, ErrorCode::Integrity);
        assert!(
            error.message.contains("duplicate waiver"),
            "{}",
            error.message
        );
        assert!(
            error
                .hint
                .contains(&tree.waivers_path().display().to_string()),
            "{}",
            error.hint
        );
        assert!(error.hint.contains(WAIVER_EXAMPLE), "{}", error.hint);
    }
}
