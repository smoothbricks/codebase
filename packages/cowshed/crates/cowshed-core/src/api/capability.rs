use super::call::{Binder, Binding, JobFields, RepoFields, WorkspaceFields};
use super::dto::{
    AdoptOptions, AttachOptions, CheckpointOptions, CheckpointQuota, CreateOptions,
    DefragmentResult, DoctorReport, EmptyResult, ExecRequest, GcOptions, GcReport, GrantDelta,
    GrantSet, JobId, JobInfo, JobJournalCursor, JobTail, JobTailLimits, LandOptions, LandReport,
    MirrorInfo, ProjectGrantDelta, ProjectGrants, PushOptions, PushReport, RebaseOptions,
    RebaseReport, RemoveOptions, RemoveProjectOptions, RemoveProjectReport, RemoveReport,
    ReseedResult, ResizeResult, ResizeVolume, SealedJob, StdinSource, StepReport,
    WorkspaceIncarnation, WorkspaceInfo, WorkspaceTarget,
};
use super::frame;
use super::operations::{
    self, AdoptRequest, ChangeRepoIdRequest, CheckpointRequest, CreateRequest, DestroyRequest,
    ExecParams, ExecStdin, GcRequest, GrantRequest, JobRequest, JobStream, LandRequest, LogsChunk,
    LogsRequest, MirrorRequest, MoveCheckoutRequest, Operation, ProjectGrantRequest,
    ProjectOpenRequest, PushRequest, QuotaRequest, RebaseRequest, RemoveProjectRequest,
    RepoRequest, ResizeRequest, RestoreRequest, SessionRequest, SlotRequest,
    SourceDestinationRequest, TailRequest, WorkerScope, WorkerView, WorkspaceAtRequest,
    WorkspaceAttachRequest,
    WorkspaceGrantsRequest, WorkspaceRequest, WorkspaceView, decode_result, encode_request,
};
use super::peer_credentials::PeerCredentialsError;
use super::server::MAX_BINARY_FRAME_BYTES;
#[cfg(unix)]
use super::server::{
    HANDSHAKE_VERSION, MAX_HANDSHAKE_BYTES, MAX_JSON_FRAME_BYTES as MAX_RPC_BYTES, codec,
};
use crate::error::{CowshedError, ErrorCode, OtherBuild, Result};
use crate::metadata::WorkspaceName;
use crate::repository::{ProjectPaths, RepoId, RepositoryBinding};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use serde_json::value::RawValue;
use std::fmt;
#[cfg(unix)]
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(unix)]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;
#[cfg(unix)]
use tokio::sync::{mpsc, oneshot};
use url::Url;

pub(crate) struct BinaryDownload {
    bytes: Vec<u8>,
    eof: bool,
}

#[derive(Debug)]
pub(crate) struct WorkspaceAuthority {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
}

impl WorkspaceAuthority {
    fn from_info(info: &WorkspaceInfo) -> Self {
        Self {
            repo_id: info.repo_id.clone(),
            workspace: info.workspace.clone(),
            workspace_incarnation: info.workspace_incarnation.clone(),
        }
    }

    /// This workspace incarnation, as the request fields a worker binds.
    fn fields(&self) -> WorkspaceFields<'_> {
        WorkspaceFields {
            repo_id: &self.repo_id,
            workspace: &self.workspace,
            workspace_incarnation: &self.workspace_incarnation,
        }
    }

    /// This workspace incarnation and `job`, as the request fields a job handle binds.
    fn job_fields<'a>(&'a self, job: &'a JobId) -> JobFields<'a> {
        JobFields {
            repo_id: &self.repo_id,
            workspace: &self.workspace,
            workspace_incarnation: &self.workspace_incarnation,
            job_id: job,
        }
    }

    fn scope(&self) -> WorkerScope {
        WorkerScope {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
            workspace_incarnation: self.workspace_incarnation.clone(),
        }
    }

    fn job(&self, job_id: JobId) -> JobRequest {
        JobRequest {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
            workspace_incarnation: self.workspace_incarnation.clone(),
            job_id,
        }
    }

    fn session(&self, session: Option<String>) -> SessionRequest {
        SessionRequest {
            repo_id: self.repo_id.clone(),
            workspace: self.workspace.clone(),
            workspace_incarnation: self.workspace_incarnation.clone(),
            session,
        }
    }
}

/// A call's params as its declared operation serialized them, framed verbatim.
pub(crate) type Params = Box<RawValue>;

#[async_trait]
pub(crate) trait ControllerRuntime: Send + Sync {
    async fn call(&self, method: &'static str, params: Params) -> Result<Value>;
    /// A call whose lifecycle steps are sent to `steps` as the controller reports them, all of
    /// them before the call returns.
    async fn call_reporting(
        &self,
        method: &'static str,
        params: Params,
        steps: tokio::sync::mpsc::UnboundedSender<StepReport>,
    ) -> Result<Value>;
    async fn upload(&self, method: &'static str, params: Params, bytes: Bytes) -> Result<Value>;
    async fn download(
        &self,
        method: &'static str,
        params: Params,
        expected_offset: u64,
    ) -> Result<BinaryDownload>;
    async fn exec(
        &self,
        authority: &WorkspaceAuthority,
        session: Option<&str>,
        request: ExecRequest,
    ) -> Result<JobId>;
    async fn logs(
        &self,
        authority: Arc<WorkspaceAuthority>,
        id: JobId,
        stream: JobStream,
        offset: u64,
        follow: bool,
    ) -> Result<RawByteStream>;
    /// A view of the job's raw streams, each resumed at its offset in `cursor`.
    async fn attach(
        &self,
        authority: Arc<WorkspaceAuthority>,
        id: JobId,
        cursor: JobJournalCursor,
    ) -> Result<JobAttachment>;
    async fn kill(&self, authority: &WorkspaceAuthority, id: JobId) -> Result<()>;
}

#[cfg(unix)]
enum ActorMessage {
    Json {
        method: &'static str,
        params: Params,
        /// Where the call's step frames go; `None` asks for none.
        steps: Option<mpsc::UnboundedSender<StepReport>>,
        reply: oneshot::Sender<Result<ActorResponse>>,
    },
    Upload {
        method: &'static str,
        params: Params,
        bytes: Bytes,
        reply: oneshot::Sender<Result<ActorResponse>>,
    },
    Download {
        method: &'static str,
        params: Params,
        expected_offset: u64,
        reply: oneshot::Sender<Result<ActorResponse>>,
    },
}

#[cfg(unix)]
enum ActorResponse {
    Json(Value),
    Download(BinaryDownload),
}

#[cfg(unix)]
enum ActorLane {
    Json,
    Upload(Bytes),
    Download(u64),
}

#[cfg(unix)]
#[derive(Clone)]
struct ActorRuntime {
    sender: mpsc::Sender<ActorMessage>,
}

#[cfg(unix)]
impl ActorRuntime {
    async fn json(
        &self,
        method: &'static str,
        params: Params,
        steps: Option<mpsc::UnboundedSender<StepReport>>,
    ) -> Result<Value> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ActorMessage::Json {
                method,
                params,
                steps,
                reply,
            })
            .await
            .map_err(|_| actor_send_error())?;
        match response.await.map_err(|_| actor_reply_error())?? {
            ActorResponse::Json(value) => Ok(value),
            ActorResponse::Download(_) => Err(CowshedError::internal(
                "controller actor returned binary data to a JSON-only call",
            )),
        }
    }
}

#[cfg(unix)]
#[async_trait]
impl ControllerRuntime for ActorRuntime {
    async fn call(&self, method: &'static str, params: Params) -> Result<Value> {
        self.json(method, params, None).await
    }

    async fn call_reporting(
        &self,
        method: &'static str,
        params: Params,
        steps: mpsc::UnboundedSender<StepReport>,
    ) -> Result<Value> {
        self.json(method, params, Some(steps)).await
    }

    async fn upload(&self, method: &'static str, params: Params, bytes: Bytes) -> Result<Value> {
        if bytes.len() > MAX_BINARY_FRAME_BYTES {
            return Err(CowshedError::internal(
                "controller RPC binary request exceeds the 64 KiB frame limit",
            ));
        }
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ActorMessage::Upload {
                method,
                params,
                bytes,
                reply,
            })
            .await
            .map_err(|_| actor_send_error())?;
        match response.await.map_err(|_| actor_reply_error())?? {
            ActorResponse::Json(value) => Ok(value),
            ActorResponse::Download(_) => Err(CowshedError::internal(
                "controller actor returned binary data to an upload call",
            )),
        }
    }

    async fn download(
        &self,
        method: &'static str,
        params: Params,
        expected_offset: u64,
    ) -> Result<BinaryDownload> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ActorMessage::Download {
                method,
                params,
                expected_offset,
                reply,
            })
            .await
            .map_err(|_| actor_send_error())?;
        match response.await.map_err(|_| actor_reply_error())?? {
            ActorResponse::Download(download) => Ok(download),
            ActorResponse::Json(_) => Err(CowshedError::internal(
                "controller actor omitted binary data from a download call",
            )),
        }
    }

    async fn exec(
        &self,
        authority: &WorkspaceAuthority,
        session: Option<&str>,
        request: ExecRequest,
    ) -> Result<JobId> {
        request.command.validate().map_err(|error| {
            CowshedError::usage(error.to_string(), "provide a valid bounded command")
        })?;
        let ExecRequest {
            command,
            cwd,
            mode,
            env,
            trace,
            stdin,
            stdout_copy,
            stderr_copy,
        } = request;
        let (stdin, inline, mut stream) = match stdin {
            StdinSource::Empty => (ExecStdin::Empty, None, None),
            StdinSource::Inline(bytes) => (ExecStdin::Inline, Some(bytes), None),
            StdinSource::WorkspaceFile(workspace_path) => {
                (ExecStdin::WorkspaceFile { workspace_path }, None, None)
            }
            StdinSource::Stream(stream) => (ExecStdin::Stream, None, Some(stream)),
        };
        let (argv, script) = match command {
            crate::api::dto::ExecCommand::Argv(argv) => (Some(argv), None),
            crate::api::dto::ExecCommand::Script(script) => (None, Some(script)),
        };
        let params = ExecParams {
            repo_id: authority.repo_id.clone(),
            workspace: authority.workspace.clone(),
            workspace_incarnation: authority.workspace_incarnation.clone(),
            session: session.map(str::to_owned),
            argv,
            script,
            cwd,
            mode,
            env,
            trace,
            stdin,
            stdout_copy,
            stderr_copy,
        };
        let job_id = match inline {
            Some(bytes) => invoke_upload::<operations::WorkerExec>(self, &params, bytes).await?,
            None => invoke::<operations::WorkerExec>(self, &params).await?,
        };
        if let Some(reader) = stream.as_mut() {
            let job = authority.job(job_id);
            let mut buffer = [0_u8; MAX_BINARY_FRAME_BYTES];
            loop {
                let count = reader.read(&mut buffer).await.map_err(|error| {
                    CowshedError::new(
                        ErrorCode::EnvironmentMissing,
                        format!("stdin stream failed: {error}"),
                        "retry the exec with a readable stdin source",
                    )
                })?;
                if count == 0 {
                    break;
                }
                let EmptyResult {} = invoke_upload::<operations::WorkerStdinChunk>(
                    self,
                    &job,
                    Bytes::copy_from_slice(&buffer[..count]),
                )
                .await?;
            }
            let EmptyResult {} = invoke::<operations::WorkerStdinClose>(self, &job).await?;
        }
        Ok(job_id)
    }

    async fn logs(
        &self,
        authority: Arc<WorkspaceAuthority>,
        id: JobId,
        stream: JobStream,
        offset: u64,
        follow: bool,
    ) -> Result<RawByteStream> {
        Ok(poll_job_stream(
            Arc::new(self.clone()),
            authority,
            id,
            stream,
            offset,
            follow,
        ))
    }

    async fn attach(
        &self,
        authority: Arc<WorkspaceAuthority>,
        id: JobId,
        cursor: JobJournalCursor,
    ) -> Result<JobAttachment> {
        let stdout = self
            .logs(
                Arc::clone(&authority),
                id,
                JobStream::Stdout,
                cursor.stdout,
                true,
            )
            .await?;
        let stderr = self
            .logs(
                Arc::clone(&authority),
                id,
                JobStream::Stderr,
                cursor.stderr,
                true,
            )
            .await?;
        let runtime: Arc<dyn ControllerRuntime> = Arc::new(self.clone());
        Ok(JobAttachment {
            authority: Arc::clone(&authority),
            id,
            stdin: JobStdin {
                authority,
                id,
                runtime: Arc::clone(&runtime),
            },
            stdout,
            stderr,
            runtime,
        })
    }

    async fn kill(&self, authority: &WorkspaceAuthority, id: JobId) -> Result<()> {
        invoke::<operations::JobKill>(self, &authority.job(id))
            .await
            .map(|EmptyResult {}| ())
    }
}

#[cfg(unix)]
fn actor_send_error() -> CowshedError {
    CowshedError::new(
        ErrorCode::EnvironmentMissing,
        "controller actor channel closed",
        "restart the trusted cowshed controller",
    )
}

#[cfg(unix)]
fn actor_reply_error() -> CowshedError {
    CowshedError::new(
        ErrorCode::EnvironmentMissing,
        "controller actor stopped before replying",
        "restart the trusted cowshed controller",
    )
}

/// The bytes of one stream from `offset` on, page by page; following asks again past end of file
/// until the job is terminal.
fn poll_job_stream(
    runtime: Arc<dyn ControllerRuntime>,
    authority: Arc<WorkspaceAuthority>,
    id: JobId,
    stream: JobStream,
    mut offset: u64,
    follow: bool,
) -> RawByteStream {
    let (sender, receiver) = mpsc::channel(8);
    tokio::spawn(async move {
        loop {
            let request = LogsRequest {
                repo_id: authority.repo_id.clone(),
                workspace: authority.workspace.clone(),
                workspace_incarnation: authority.workspace_incarnation.clone(),
                job_id: id,
                stream,
                follow,
                offset,
            };
            let chunk = tokio::select! {
                _ = sender.closed() => break,
                value = invoke_download::<operations::JobLogs>(&*runtime, request) => value,
            };
            let (chunk, bytes) = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    tokio::select! {
                        _ = sender.closed() => {}
                        _ = sender.send(Err(error)) => {}
                    }
                    break;
                }
            };
            let had_bytes = !bytes.is_empty();
            let eof = chunk.eof;
            offset = chunk.next_offset;
            if had_bytes {
                let sent = tokio::select! {
                    _ = sender.closed() => false,
                    result = sender.send(Ok(bytes)) => result.is_ok(),
                };
                if !sent {
                    break;
                }
            }
            if eof {
                if !follow {
                    break;
                }
                let job = authority.job(id);
                let status = tokio::select! {
                    _ = sender.closed() => break,
                    value = invoke::<operations::JobStatus>(&*runtime, &job) => value,
                };
                match status {
                    Ok(info) if info.state.is_terminal() => break,
                    Ok(_) => {}
                    Err(error) => {
                        tokio::select! {
                            _ = sender.closed() => {}
                            _ = sender.send(Err(error)) => {}
                        }
                        break;
                    }
                }
            }
            if !had_bytes || eof {
                tokio::select! {
                    _ = sender.closed() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
                }
            }
        }
    });
    RawByteStream { receiver }
}

/// Calls a declared operation and decodes its declared result.
pub(super) async fn invoke<O: Operation>(
    runtime: &(impl ControllerRuntime + ?Sized),
    request: &O::Request,
) -> Result<O::Result> {
    let params = encode_request::<O>(request)?;
    decode_result::<O>(runtime.call(O::METHOD, params).await?)
}

/// [`invoke`], with the call's lifecycle steps sent to `steps` as the controller reports them.
async fn invoke_reporting<O: Operation>(
    runtime: &(impl ControllerRuntime + ?Sized),
    request: &O::Request,
    steps: tokio::sync::mpsc::UnboundedSender<StepReport>,
) -> Result<O::Result> {
    let params = encode_request::<O>(request)?;
    decode_result::<O>(runtime.call_reporting(O::METHOD, params, steps).await?)
}

/// [`invoke`] for an upload operation, with `bytes` as its raw-byte frame.
pub(super) async fn invoke_upload<O: Operation>(
    runtime: &(impl ControllerRuntime + ?Sized),
    request: &O::Request,
    bytes: Bytes,
) -> Result<O::Result> {
    let params = encode_request::<O>(request)?;
    decode_result::<O>(runtime.upload(O::METHOD, params, bytes).await?)
}

/// Calls a download operation: the chunk's metadata and its bytes, which start at the offset the
/// request declares. The connection has already proved the chunk ends exactly at `nextOffset`.
pub(super) async fn invoke_download<O: Operation<Result = LogsChunk>>(
    runtime: &(impl ControllerRuntime + ?Sized),
    request: O::Request,
) -> Result<(LogsChunk, Bytes)> {
    let params = encode_request::<O>(&request)?;
    let offset = O::request(request).download_offset().ok_or_else(|| {
        CowshedError::internal(format!("{} declares no download offset", O::METHOD))
    })?;
    let download = runtime.download(O::METHOD, params, offset).await?;
    let next_offset = u64::try_from(download.bytes.len())
        .ok()
        .and_then(|length| offset.checked_add(length))
        .ok_or_else(|| CowshedError::internal(format!("{} offset overflowed", O::METHOD)))?;
    Ok((
        LogsChunk {
            eof: download.eof,
            next_offset,
        },
        Bytes::from(download.bytes),
    ))
}

pub struct RawByteStream {
    receiver: mpsc::Receiver<Result<Bytes>>,
}

impl RawByteStream {
    pub async fn next(&mut self) -> Option<Result<Bytes>> {
        self.receiver.recv().await
    }
}

pub struct JobStdin {
    authority: Arc<WorkspaceAuthority>,
    id: JobId,
    runtime: Arc<dyn ControllerRuntime>,
}

impl JobStdin {
    pub async fn write(&self, bytes: Bytes) -> Result<()> {
        invoke_upload::<operations::JobAttachWrite>(
            &*self.runtime,
            &self.authority.job(self.id),
            bytes,
        )
        .await
        .map(|EmptyResult {}| ())
    }
}

impl fmt::Debug for JobStdin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobStdin")
            .field("authority", &self.authority)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

pub struct JobAttachment {
    authority: Arc<WorkspaceAuthority>,
    id: JobId,
    stdin: JobStdin,
    stdout: RawByteStream,
    stderr: RawByteStream,
    runtime: Arc<dyn ControllerRuntime>,
}

impl JobAttachment {
    pub fn into_parts(self) -> (JobStdin, RawByteStream, RawByteStream) {
        (self.stdin, self.stdout, self.stderr)
    }

    pub async fn detach(self) -> Result<()> {
        invoke::<operations::JobDetach>(&*self.runtime, &self.authority.job(self.id))
            .await
            .map(|EmptyResult {}| ())
    }
}

impl fmt::Debug for JobAttachment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobAttachment")
            .field("authority", &self.authority)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

fn workspace_name(name: &str) -> Result<WorkspaceName> {
    WorkspaceName::new(name).map_err(|error| {
        CowshedError::usage(error.to_string(), "use a valid cowshed workspace name")
    })
}

/// Explicit cowshed client. Its sealed runtime delegates to a single-owner controller actor.
pub struct Cowshed {
    runtime: Arc<dyn ControllerRuntime>,
    other_build: watch::Receiver<Option<OtherBuild>>,
}

impl Cowshed {
    pub async fn open(&self, path: impl AsRef<Path>) -> Result<Project> {
        let path = path.as_ref().to_str().ok_or_else(|| {
            CowshedError::usage(
                "project path is not valid UTF-8",
                "use a UTF-8 project path",
            )
        })?;
        let wire = invoke::<operations::ProjectOpen>(
            &*self.runtime,
            &ProjectOpenRequest {
                path: path.to_owned(),
            },
        )
        .await?;
        let paths = crate::storage::StorageLayout::new(&wire.store_root, &wire.repo_id)
            .map(|layout| layout.project().clone())
            .map_err(|error| {
                CowshedError::new(
                    ErrorCode::Internal,
                    format!("controller returned invalid project paths: {error}"),
                    "cowshed doctor --json",
                )
            })?;
        Ok(Project {
            repo_id: wire.repo_id,
            binding: wire.binding,
            git_root: wire.git_root,
            paths,
            runtime: Arc::clone(&self.runtime),
        })
    }

    #[cfg(unix)]
    pub async fn connect(descriptor: OwnedFd) -> Result<(Self, CoordinatorToken)> {
        acquire_coordinator_token(descriptor).await
    }

    pub fn coordinator(&self, project: &Project, token: CoordinatorToken) -> Result<Coordinator> {
        if !Arc::ptr_eq(&self.runtime, &project.runtime)
            || !Arc::ptr_eq(&self.runtime, &token.channel.runtime)
            || project.repo_id != token.repo_id
        {
            return Err(CowshedError::new(
                ErrorCode::Conflict,
                "coordinator token is bound to a different project or controller channel",
                "reopen the project and reacquire coordinator authority",
            ));
        }
        Ok(Coordinator {
            project: project.clone(),
            runtime: Arc::clone(&self.runtime),
            other_build: self.other_build.clone(),
            _channel: token.channel,
        })
    }
}

impl fmt::Debug for Cowshed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Cowshed(<controller actor>)")
    }
}

/// Discovery-only project identity. It contains no controller or worker authority.
#[derive(Clone)]
pub struct Project {
    repo_id: RepoId,
    binding: Arc<RepositoryBinding>,
    git_root: Arc<Path>,
    paths: ProjectPaths,
    runtime: Arc<dyn ControllerRuntime>,
}

impl Project {
    pub fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub fn binding(&self) -> &RepositoryBinding {
        &self.binding
    }

    pub fn git_root(&self) -> &Path {
        &self.git_root
    }

    pub fn paths(&self) -> &ProjectPaths {
        &self.paths
    }

    pub async fn main(&self) -> Result<WorkspaceRef> {
        self.workspace("main").await
    }

    pub async fn workspace(&self, name: &str) -> Result<WorkspaceRef> {
        let request = WorkspaceRequest {
            repo_id: self.repo_id.clone(),
            workspace: workspace_name(name)?,
        };
        let view = invoke::<operations::ProjectWorkspace>(&*self.runtime, &request).await?;
        Ok(WorkspaceRef::from_view(view, Arc::clone(&self.runtime)))
    }

    /// Resolves an existing path through the controller's authoritative storage and mount facts.
    pub async fn workspace_at(&self, path: impl AsRef<Path>) -> Result<WorkspaceRef> {
        let path = path.as_ref().to_str().ok_or_else(|| {
            CowshedError::usage(
                "workspace path is not valid UTF-8",
                "use a UTF-8 workspace path",
            )
        })?;
        let request = WorkspaceAtRequest {
            repo_id: self.repo_id.clone(),
            path: path.to_owned(),
        };
        let view = invoke::<operations::ProjectWorkspaceAt>(&*self.runtime, &request).await?;
        Ok(WorkspaceRef::from_view(view, Arc::clone(&self.runtime)))
    }

    pub async fn list(&self) -> Result<Vec<WorkspaceRef>> {
        let request = RepoRequest {
            repo_id: self.repo_id.clone(),
        };
        let views = invoke::<operations::ProjectList>(&*self.runtime, &request).await?;
        Ok(views
            .into_iter()
            .map(|view| WorkspaceRef::from_view(view, Arc::clone(&self.runtime)))
            .collect())
    }
}

impl fmt::Debug for Project {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Project")
            .field("repo_id", &self.repo_id)
            .field("binding", &self.binding)
            .field("git_root", &self.git_root)
            .field("paths", &self.paths)
            .finish_non_exhaustive()
    }
}

/// Read-only identity and detached snapshot for exactly one workspace.
#[derive(Clone)]
pub struct WorkspaceRef {
    info: WorkspaceInfo,
    grants: GrantSet,
    runtime: Arc<dyn ControllerRuntime>,
}

impl WorkspaceRef {
    pub(super) fn from_view(view: WorkspaceView, runtime: Arc<dyn ControllerRuntime>) -> Self {
        Self {
            info: view.info,
            grants: view.grants,
            runtime,
        }
    }

    fn request(&self) -> WorkspaceRequest {
        WorkspaceRequest {
            repo_id: self.info.repo_id.clone(),
            workspace: self.info.workspace.clone(),
        }
    }

    pub fn name(&self) -> &WorkspaceName {
        &self.info.workspace
    }

    pub fn mount_path(&self) -> &Path {
        &self.info.mount
    }

    /// Returns the immutable information captured by the RPC that created this reference.
    pub fn info(&self) -> &WorkspaceInfo {
        &self.info
    }

    /// Returns the immutable grants captured by the RPC that created this reference.
    pub fn grants(&self) -> &GrantSet {
        &self.grants
    }

    pub fn snapshot(&self) -> (&WorkspaceInfo, &GrantSet) {
        (&self.info, &self.grants)
    }

    pub fn into_info(self) -> WorkspaceInfo {
        self.info
    }

    pub fn into_snapshot(self) -> (WorkspaceInfo, GrantSet) {
        (self.info, self.grants)
    }

    /// This workspace as a land or rebase target, pinned to the incarnation this reference was
    /// resolved at.
    pub fn target(&self) -> WorkspaceTarget {
        WorkspaceTarget::new(
            self.info.workspace.clone(),
            self.info.workspace_incarnation.clone(),
        )
    }

    /// Refreshes workspace information from the controller without changing this snapshot.
    pub async fn refresh_info(&self) -> Result<WorkspaceInfo> {
        invoke::<operations::WorkspaceInfoRead>(&*self.runtime, &self.request()).await
    }

    pub async fn attach(&self, options: AttachOptions) -> Result<()> {
        let request = WorkspaceAttachRequest {
            repo_id: self.info.repo_id.clone(),
            workspace: self.info.workspace.clone(),
            options,
        };
        invoke::<operations::WorkspaceAttach>(&*self.runtime, &request)
            .await
            .map(|EmptyResult {}| ())
    }

    /// Refreshes grants from the controller without changing this snapshot.
    pub async fn refresh_grants(&self) -> Result<GrantSet> {
        let request = WorkspaceGrantsRequest {
            repo_id: self.info.repo_id.clone(),
            workspace: self.info.workspace.clone(),
            workspace_incarnation: None,
        };
        invoke::<operations::WorkspaceGrants>(&*self.runtime, &request).await
    }

    /// The build volume a job of this workspace incarnation would be granted now
    /// (16_build_volumes.md, "Process lifetime across a swap"), or `None` when its checkout links
    /// none. A land renames the build link, so the answer holds until the next one; a caller that
    /// follows the link compares what it names against this. Refuses a detached workspace and a
    /// name recreated since this reference was resolved.
    pub async fn build_volume(&self) -> Result<Option<PathBuf>> {
        let request = WorkerScope {
            repo_id: self.info.repo_id.clone(),
            workspace: self.info.workspace.clone(),
            workspace_incarnation: self.info.workspace_incarnation.clone(),
        };
        invoke::<operations::WorkspaceBuildVolume>(&*self.runtime, &request)
            .await
            .map(|answer| answer.volume)
    }
}

impl fmt::Debug for WorkspaceRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceRef")
            .field("info", &self.info)
            .field("grants", &self.grants)
            .finish_non_exhaustive()
    }
}

/// Affine proof produced only by the inherited descriptor handshake.
pub struct CoordinatorToken {
    repo_id: RepoId,
    channel: AuthenticatedControllerChannel,
}

impl fmt::Debug for CoordinatorToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoordinatorToken")
            .field("repo_id", &self.repo_id)
            .field("channel", &"<authenticated controller channel>")
            .finish()
    }
}

struct AuthenticatedControllerChannel {
    runtime: Arc<dyn ControllerRuntime>,
}

impl fmt::Debug for AuthenticatedControllerChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthenticatedControllerChannel(<redacted>)")
    }
}

#[cfg(unix)]
fn handshake_error(message: impl Into<String>) -> CowshedError {
    CowshedError::new(
        ErrorCode::EnvironmentMissing,
        message,
        "start cowshed from a trusted controller with an inherited coordinator descriptor",
    )
}

#[cfg(unix)]
fn verify_peer(descriptor: &OwnedFd) -> Result<()> {
    frame::verify_peer(descriptor, |error| match error {
        PeerCredentialsError::SocketTypeSizeOverflow
        | PeerCredentialsError::SocketTypeQueryFailed
        | PeerCredentialsError::NotStream => {
            handshake_error("coordinator descriptor is not a stream socket")
        }
        PeerCredentialsError::PeerCredentialQueryFailed => {
            handshake_error("coordinator descriptor peer does not match the current uid")
        }
    })
}

#[cfg(unix)]
fn fresh_nonce() -> String {
    let first = uuid::Uuid::new_v4().simple().to_string();
    let second = uuid::Uuid::new_v4().simple().to_string();
    format!("{first}{second}")
}

#[cfg(unix)]
async fn write_frame(stream: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<()> {
    frame::write_frame(
        stream,
        bytes,
        MAX_HANDSHAKE_BYTES,
        || handshake_error("coordinator handshake request is too large"),
        |error| handshake_error(format!("coordinator handshake write failed: {error}")),
    )
    .await
}

#[cfg(unix)]
async fn read_frame(stream: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    frame::read_frame(
        stream,
        MAX_HANDSHAKE_BYTES,
        || handshake_error("coordinator handshake response has invalid length"),
        |error| handshake_error(format!("coordinator handshake read failed: {error}")),
    )
    .await
}

#[cfg(unix)]
async fn write_rpc_frame(stream: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_RPC_BYTES {
        return Err(CowshedError::internal(
            "controller RPC request is too large",
        ));
    }
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|error| {
            CowshedError::new(
                ErrorCode::EnvironmentMissing,
                format!("controller RPC write failed: {error}"),
                "restart the trusted cowshed controller",
            )
        })?;
    stream.write_all(bytes).await.map_err(|error| {
        CowshedError::new(
            ErrorCode::EnvironmentMissing,
            format!("controller RPC write failed: {error}"),
            "restart the trusted cowshed controller",
        )
    })
}

#[cfg(unix)]
async fn read_rpc_frame(stream: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    let length = stream.read_u32().await.map_err(|error| {
        CowshedError::new(
            ErrorCode::EnvironmentMissing,
            format!("controller RPC read failed: {error}"),
            "restart the trusted cowshed controller",
        )
    })? as usize;
    if length == 0 || length > MAX_RPC_BYTES {
        return Err(CowshedError::internal(
            "controller RPC response has invalid length",
        ));
    }
    let mut bytes = vec![0_u8; length];
    stream.read_exact(&mut bytes).await.map_err(|error| {
        CowshedError::new(
            ErrorCode::EnvironmentMissing,
            format!("controller RPC read failed: {error}"),
            "restart the trusted cowshed controller",
        )
    })?;
    Ok(bytes)
}

#[cfg(unix)]
async fn write_binary_frame(stream: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_BINARY_FRAME_BYTES {
        return Err(CowshedError::internal(
            "controller RPC binary request exceeds the 64 KiB frame limit",
        ));
    }
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|error| {
            CowshedError::new(
                ErrorCode::EnvironmentMissing,
                format!("controller RPC binary write failed: {error}"),
                "restart the trusted cowshed controller",
            )
        })?;
    stream.write_all(bytes).await.map_err(|error| {
        CowshedError::new(
            ErrorCode::EnvironmentMissing,
            format!("controller RPC binary write failed: {error}"),
            "restart the trusted cowshed controller",
        )
    })
}

#[cfg(unix)]
async fn read_binary_frame(
    stream: &mut (impl AsyncRead + Unpin),
    expected_length: usize,
) -> Result<Vec<u8>> {
    if expected_length > MAX_BINARY_FRAME_BYTES {
        return Err(CowshedError::internal(
            "controller RPC binary response exceeds the 64 KiB frame limit",
        ));
    }
    let actual_length = stream.read_u32().await.map_err(|error| {
        CowshedError::new(
            ErrorCode::EnvironmentMissing,
            format!("controller RPC binary read failed: {error}"),
            "restart the trusted cowshed controller",
        )
    })? as usize;
    if actual_length > MAX_BINARY_FRAME_BYTES {
        return Err(CowshedError::internal(
            "controller RPC binary response has an oversized frame",
        ));
    }
    if actual_length != expected_length {
        return Err(CowshedError::internal(format!(
            "controller RPC binary response length mismatch: declared {expected_length}, framed {actual_length}"
        )));
    }
    let mut bytes = vec![0_u8; actual_length];
    stream.read_exact(&mut bytes).await.map_err(|error| {
        CowshedError::new(
            ErrorCode::EnvironmentMissing,
            format!("controller RPC binary read failed: {error}"),
            "restart the trusted cowshed controller",
        )
    })?;
    Ok(bytes)
}

/// One controller connection shared by every call a client makes.
///
/// Calls are sent in order, each under the next id, and answered as the controller completes
/// them, so a call that waits on a job (`job.wait`, a follow read) never holds the calls behind
/// it. A reader task takes each answer off the socket whole, reading a binary frame only for a
/// call that expects one; the actor matches the answer to its call by id. A failure that leaves
/// the connection unusable fails every call still waiting, and the calls after it find the
/// actor gone.
///
/// The reader also notes the daemon's refusal of the controller's build ([`OtherBuild`]) the
/// first time an answer carries it, before that answer reaches its call: it says the controller
/// behind this connection can start nothing in a workspace, whichever call met it.
#[cfg(unix)]
fn spawn_controller_actor(
    stream: tokio::net::UnixStream,
) -> (
    Arc<dyn ControllerRuntime>,
    watch::Receiver<Option<OtherBuild>>,
) {
    let (sender, receiver) = mpsc::channel::<ActorMessage>(32);
    let (reader, writer) = stream.into_split();
    let (answers, answer_receiver) = mpsc::unbounded_channel();
    let (other_build, refused) = watch::channel(None);
    let downloads = Downloads::default();
    tokio::spawn(read_answers(
        reader,
        Arc::clone(&downloads),
        answers,
        other_build,
    ));
    tokio::spawn(run_controller_actor(
        receiver,
        writer,
        downloads,
        answer_receiver,
    ));
    (Arc::new(ActorRuntime { sender }), refused)
}

/// The stream offset each sent download call continues from, by call id. The actor adds an
/// entry before it sends the call; the reader takes it out with the answer, and only an answer
/// that had one may carry a binary frame.
#[cfg(unix)]
type Downloads = Arc<std::sync::Mutex<std::collections::HashMap<u64, u64>>>;

/// One answer, validated and taken off the connection whole.
#[cfg(unix)]
struct Answer {
    id: u64,
    /// What the call gets: its response, or the controller's recoverable error.
    outcome: Result<ActorResponse>,
}

/// What the reader takes off the connection for the actor: one step of a call that asked for
/// its steps, or a call's answer.
#[cfg(unix)]
enum Inbound {
    Step { id: u64, report: StepReport },
    Answer(Answer),
}

#[cfg(unix)]
async fn read_answers(
    mut reader: tokio::net::unix::OwnedReadHalf,
    downloads: Downloads,
    answers: mpsc::UnboundedSender<Result<Inbound>>,
    other_build: watch::Sender<Option<OtherBuild>>,
) {
    loop {
        let inbound = read_inbound(&mut reader, &downloads).await;
        if let Ok(Inbound::Answer(Answer {
            outcome: Err(error),
            ..
        })) = &inbound
            && let Some(refused) = error.other_build_source()
        {
            other_build.send_if_modified(|noted| {
                let first = noted.is_none();
                if first {
                    *noted = Some(refused.clone());
                }
                first
            });
        }
        let failed = inbound.is_err();
        if answers.send(inbound).is_err() || failed {
            return;
        }
    }
}

/// Read and check one frame. `Err` means the connection cannot be read past it.
#[cfg(unix)]
async fn read_inbound(
    reader: &mut tokio::net::unix::OwnedReadHalf,
    downloads: &Downloads,
) -> Result<Inbound> {
    let frame = read_rpc_frame(reader).await?;
    match codec::decode_server_frame(&frame).map_err(|error| {
        CowshedError::internal(format!("controller RPC response decoding failed: {error}"))
    })? {
        codec::DecodedServerFrame::Step { id, report } => Ok(Inbound::Step { id, report }),
        codec::DecodedServerFrame::Response(response) => read_answer(reader, downloads, response)
            .await
            .map(Inbound::Answer),
    }
}

/// Check one answer. `Err` means the connection cannot be read past it; every frame is checked
/// against its declaration before its bytes are read.
#[cfg(unix)]
async fn read_answer(
    reader: &mut tokio::net::unix::OwnedReadHalf,
    downloads: &Downloads,
    response: codec::DecodedRpcResponse,
) -> Result<Answer> {
    let (id, ok, result, error, binary_length) = response.into_parts();
    let download = downloads
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&id);
    let result = match (ok, result, error) {
        (true, Some(result), None) => result,
        (false, None, Some(error)) => {
            if binary_length.is_some() {
                return Err(CowshedError::internal(
                    "controller RPC error response declared unsolicited binary data",
                ));
            }
            return Ok(Answer {
                id,
                outcome: Err(error),
            });
        }
        _ => {
            return Err(CowshedError::internal(
                "controller RPC response has an invalid envelope",
            ));
        }
    };
    let Some(expected_offset) = download else {
        if binary_length.is_some() {
            return Err(CowshedError::internal(
                "controller RPC response declared unsolicited binary data",
            ));
        }
        return Ok(Answer {
            id,
            outcome: Ok(ActorResponse::Json(result)),
        });
    };
    let length = binary_length.ok_or_else(|| {
        CowshedError::internal("controller RPC download response omitted binaryLength")
    })?;
    let length = usize::try_from(length).map_err(|_| {
        CowshedError::internal("controller RPC binary response length does not fit this platform")
    })?;
    if length > MAX_BINARY_FRAME_BYTES {
        return Err(CowshedError::internal(
            "controller RPC binary response exceeds the 64 KiB frame limit",
        ));
    }
    let metadata: LogsChunk = serde_json::from_value(result).map_err(|error| {
        CowshedError::internal(format!(
            "controller RPC download metadata is invalid: {error}"
        ))
    })?;
    let expected_next = u64::try_from(length)
        .ok()
        .and_then(|length| expected_offset.checked_add(length))
        .ok_or_else(|| CowshedError::internal("controller RPC download offset overflowed"))?;
    if metadata.next_offset != expected_next {
        return Err(CowshedError::internal(
            "controller RPC download nextOffset was not exact",
        ));
    }
    let bytes = read_binary_frame(reader, length).await?;
    Ok(Answer {
        id,
        outcome: Ok(ActorResponse::Download(BinaryDownload {
            bytes,
            eof: metadata.eof,
        })),
    })
}

#[cfg(unix)]
async fn run_controller_actor(
    mut messages: mpsc::Receiver<ActorMessage>,
    mut writer: tokio::net::unix::OwnedWriteHalf,
    downloads: Downloads,
    mut answers: mpsc::UnboundedReceiver<Result<Inbound>>,
) {
    let mut pending =
        std::collections::HashMap::<u64, oneshot::Sender<Result<ActorResponse>>>::new();
    // The step listeners of the pending calls that asked for steps; a listener is dropped with
    // its call's answer, so the caller's step stream ends exactly when the call does.
    let mut listeners = std::collections::HashMap::<u64, mpsc::UnboundedSender<StepReport>>::new();
    let mut next_id = 1_u64;
    let failure = loop {
        tokio::select! {
            message = messages.recv() => {
                // Every handle is gone, and with it every caller that could wait on an answer.
                let Some(message) = message else { return };
                let id = next_id;
                next_id = next_id.saturating_add(1);
                let (method, params, lane, steps, reply) = match message {
                    ActorMessage::Json { method, params, steps, reply } => {
                        (method, params, ActorLane::Json, steps, reply)
                    }
                    ActorMessage::Upload { method, params, bytes, reply } => {
                        (method, params, ActorLane::Upload(bytes), None, reply)
                    }
                    ActorMessage::Download { method, params, expected_offset, reply } => {
                        (method, params, ActorLane::Download(expected_offset), None, reply)
                    }
                };
                if let ActorLane::Download(offset) = lane {
                    downloads
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(id, offset);
                }
                let call = send_call(&mut writer, id, method, &params, &lane, steps.is_some());
                if let Err(error) = call.await {
                    let _ = reply.send(Err(error.clone()));
                    break error;
                }
                pending.insert(id, reply);
                if let Some(steps) = steps {
                    listeners.insert(id, steps);
                }
            }
            inbound = answers.recv() => {
                let answer = match inbound {
                    Some(Ok(Inbound::Answer(answer))) => answer,
                    Some(Ok(Inbound::Step { id, report })) => {
                        let Some(listener) = listeners.get(&id) else {
                            break CowshedError::internal(
                                "controller RPC step did not match a pending call that asked for steps",
                            );
                        };
                        // A caller that stopped listening still gets its answer.
                        let _ = listener.send(report);
                        continue;
                    }
                    Some(Err(error)) => break error,
                    None => break actor_reply_error(),
                };
                listeners.remove(&answer.id);
                let Some(reply) = pending.remove(&answer.id) else {
                    break CowshedError::internal(
                        "controller RPC response id did not match a pending request",
                    );
                };
                let _ = reply.send(answer.outcome);
            }
        }
    };
    for (_, reply) in pending.drain() {
        let _ = reply.send(Err(failure.clone()));
    }
}

/// Write one call, and its upload frame, under `id`. Any failure leaves the connection
/// unusable: the stream may hold part of a frame.
#[cfg(unix)]
async fn send_call(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    id: u64,
    method: &str,
    params: &RawValue,
    lane: &ActorLane,
    steps: bool,
) -> Result<()> {
    let binary_length = match lane {
        ActorLane::Upload(bytes) => Some(u32::try_from(bytes.len()).map_err(|_| {
            CowshedError::internal("controller RPC binary request exceeds the 64 KiB frame limit")
        })?),
        ActorLane::Json | ActorLane::Download(_) => None,
    };
    let request =
        codec::encode_rpc_request(id, method, params, binary_length, steps).map_err(|error| {
            if error.is_too_large() {
                CowshedError::internal("controller RPC request is too large")
            } else {
                CowshedError::internal(format!("controller RPC request encoding failed: {error}"))
            }
        })?;
    write_rpc_frame(writer, &request).await?;
    if let ActorLane::Upload(bytes) = lane {
        write_binary_frame(writer, bytes).await?;
    }
    Ok(())
}

#[cfg(unix)]
async fn acquire_coordinator_token(descriptor: OwnedFd) -> Result<(Cowshed, CoordinatorToken)> {
    verify_peer(&descriptor)?;
    let stream = std::os::unix::net::UnixStream::from(descriptor);
    stream.set_nonblocking(true).map_err(|error| {
        handshake_error(format!("coordinator descriptor setup failed: {error}"))
    })?;
    let mut stream = tokio::net::UnixStream::from_std(stream).map_err(|error| {
        handshake_error(format!("coordinator descriptor setup failed: {error}"))
    })?;
    let nonce = fresh_nonce();
    let hello = codec::encode_client_hello(&nonce).map_err(|error| {
        handshake_error(format!("coordinator handshake encoding failed: {error}"))
    })?;
    write_frame(&mut stream, &hello).await?;
    let response = read_frame(&mut stream).await?;
    let response = codec::decode_server_hello(&response).map_err(|error| {
        handshake_error(format!(
            "coordinator handshake response is invalid: {error}"
        ))
    })?;
    let (version, response_nonce, repo_id) = response.into_parts();
    if version != HANDSHAKE_VERSION || response_nonce != nonce {
        return Err(handshake_error(
            "coordinator handshake nonce or protocol version did not match",
        ));
    }
    let (runtime, other_build) = spawn_controller_actor(stream);
    let token = CoordinatorToken {
        repo_id,
        channel: AuthenticatedControllerChannel {
            runtime: Arc::clone(&runtime),
        },
    };
    Ok((
        Cowshed {
            runtime,
            other_build,
        },
        token,
    ))
}

/// Sole project mutation and cross-workspace authority.
pub struct Coordinator {
    project: Project,
    runtime: Arc<dyn ControllerRuntime>,
    other_build: watch::Receiver<Option<OtherBuild>>,
    _channel: AuthenticatedControllerChannel,
}

impl Coordinator {
    pub fn project(&self) -> &Project {
        &self.project
    }

    /// The daemon's refusal of this controller's build, once any call on this connection has met
    /// it. From then on the daemon refuses every call of this controller that needs a workspace
    /// supervisor, while a supervisor of the controller's own build still serving — one draining
    /// its jobs — keeps answering the calls that reach it. A controller of the daemon's build is
    /// the remedy: after an install, the host's `cowshed controller` started again.
    pub fn other_build(&self) -> watch::Receiver<Option<OtherBuild>> {
        self.other_build.clone()
    }

    fn repo_id(&self) -> RepoId {
        self.project.repo_id.clone()
    }

    fn workspace_request(&self, workspace: &str) -> Result<WorkspaceRequest> {
        Ok(WorkspaceRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
        })
    }

    fn workspace_ref(&self, view: WorkspaceView) -> WorkspaceRef {
        WorkspaceRef::from_view(view, Arc::clone(&self.runtime))
    }

    pub async fn adopt(&self, options: AdoptOptions) -> Result<WorkspaceRef> {
        let request = AdoptRequest {
            repo_id: self.repo_id(),
            options,
        };
        invoke::<operations::CoordinatorAdopt>(&*self.runtime, &request)
            .await
            .map(|view| self.workspace_ref(view))
    }

    pub async fn create(&self, name: &str, options: CreateOptions) -> Result<WorkspaceRef> {
        self.create_call(name, options, None).await
    }

    /// [`Self::create`], with each of its lifecycle steps sent to `steps` as the controller
    /// reports it — the clone, the first write into it, its mount, the checkout and the rest —
    /// nested under the step it runs inside. Every step is sent before this returns, and the
    /// stream ends with it.
    pub async fn create_reporting(
        &self,
        name: &str,
        options: CreateOptions,
        steps: tokio::sync::mpsc::UnboundedSender<StepReport>,
    ) -> Result<WorkspaceRef> {
        self.create_call(name, options, Some(steps)).await
    }

    async fn create_call(
        &self,
        name: &str,
        options: CreateOptions,
        steps: Option<tokio::sync::mpsc::UnboundedSender<StepReport>>,
    ) -> Result<WorkspaceRef> {
        let workspace = WorkspaceName::session(name).map_err(|error| {
            CowshedError::usage(error.to_string(), "use a valid non-main workspace name")
        })?;
        let request = CreateRequest {
            repo_id: self.repo_id(),
            workspace,
            options,
        };
        let view = match steps {
            Some(steps) => {
                invoke_reporting::<operations::CoordinatorCreate>(&*self.runtime, &request, steps)
                    .await?
            }
            None => invoke::<operations::CoordinatorCreate>(&*self.runtime, &request).await?,
        };
        Ok(self.workspace_ref(view))
    }

    pub async fn rename(&self, source: &str, destination: &str) -> Result<WorkspaceRef> {
        let source = WorkspaceName::session(source).map_err(|error| {
            CowshedError::usage(
                error.to_string(),
                "use a valid non-main source workspace name",
            )
        })?;
        let destination = WorkspaceName::session(destination).map_err(|error| {
            CowshedError::usage(error.to_string(), "use a valid non-main destination name")
        })?;
        let request = SourceDestinationRequest {
            repo_id: self.repo_id(),
            source,
            destination,
        };
        invoke::<operations::CoordinatorRename>(&*self.runtime, &request)
            .await
            .map(|view| self.workspace_ref(view))
    }

    /// Move the project's checkout — `cowshed mv main <path>`.
    pub async fn move_checkout(&self, destination: &std::path::Path) -> Result<WorkspaceRef> {
        let request = MoveCheckoutRequest {
            repo_id: self.repo_id(),
            destination: destination.to_path_buf(),
        };
        invoke::<operations::CoordinatorMoveCheckout>(&*self.runtime, &request)
            .await
            .map(|view| self.workspace_ref(view))
    }

    /// Change the adopted project's repository identity — `cowshed mv main --repo-id`.
    pub async fn change_repo_id(&self, repo_id: &RepoId) -> Result<WorkspaceRef> {
        let request = ChangeRepoIdRequest {
            repo_id: self.repo_id(),
            new_repo_id: repo_id.clone(),
        };
        invoke::<operations::CoordinatorChangeRepoId>(&*self.runtime, &request)
            .await
            .map(|view| self.workspace_ref(view))
    }

    pub async fn fork(&self, source: &str, destination: &str) -> Result<WorkspaceRef> {
        let source = WorkspaceName::new(source).map_err(|error| {
            CowshedError::usage(error.to_string(), "use a valid source workspace name")
        })?;
        let destination = WorkspaceName::session(destination).map_err(|error| {
            CowshedError::usage(error.to_string(), "use a valid non-main destination name")
        })?;
        let request = SourceDestinationRequest {
            repo_id: self.repo_id(),
            source,
            destination,
        };
        invoke::<operations::CoordinatorFork>(&*self.runtime, &request)
            .await
            .map(|view| self.workspace_ref(view))
    }

    pub async fn grant(&self, workspace: &str, delta: GrantDelta) -> Result<GrantSet> {
        let request = self.grant_request(workspace, delta)?;
        invoke::<operations::CoordinatorGrant>(&*self.runtime, &request).await
    }

    pub async fn revoke(&self, workspace: &str, delta: GrantDelta) -> Result<GrantSet> {
        let request = self.grant_request(workspace, delta)?;
        invoke::<operations::CoordinatorRevoke>(&*self.runtime, &request).await
    }

    fn grant_request(&self, workspace: &str, delta: GrantDelta) -> Result<GrantRequest> {
        Ok(GrantRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            delta,
        })
    }

    /// The project's standing grants: what every workspace of this project runs under in
    /// addition to its own.
    pub async fn project_grants(&self) -> Result<ProjectGrants> {
        let request = RepoRequest {
            repo_id: self.repo_id(),
        };
        invoke::<operations::CoordinatorProjectGrants>(&*self.runtime, &request).await
    }

    pub async fn grant_project(&self, delta: ProjectGrantDelta) -> Result<ProjectGrants> {
        let request = ProjectGrantRequest {
            repo_id: self.repo_id(),
            delta,
        };
        invoke::<operations::CoordinatorGrantProject>(&*self.runtime, &request).await
    }

    pub async fn revoke_project(&self, delta: ProjectGrantDelta) -> Result<ProjectGrants> {
        let request = ProjectGrantRequest {
            repo_id: self.repo_id(),
            delta,
        };
        invoke::<operations::CoordinatorRevokeProject>(&*self.runtime, &request).await
    }

    /// Rebase `workspace` onto what it lands into: `into`'s checked-out branch, or main's `main`
    /// when `into` is `None`. `into` and `options.onto` are exclusive. The report carries the new
    /// head and what the workspace's build volume took of its target's Nx cache.
    pub async fn rebase(
        &self,
        workspace: &str,
        into: Option<&WorkspaceRef>,
        options: RebaseOptions,
    ) -> Result<RebaseReport> {
        let request = RebaseRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            into: into.map(WorkspaceRef::target),
            options,
        };
        invoke::<operations::CoordinatorRebase>(&*self.runtime, &request).await
    }

    /// Land `workspace` into `into` — fast-forward the branch `into` has checked out and retire
    /// the unit — or into main when `into` is `None`.
    pub async fn land(
        &self,
        workspace: &str,
        into: Option<&WorkspaceRef>,
        options: LandOptions,
    ) -> Result<LandReport> {
        let request = LandRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            into: into.map(WorkspaceRef::target),
            options,
        };
        invoke::<operations::CoordinatorLand>(&*self.runtime, &request).await
    }

    pub async fn restore(&self, workspace: &str, label: &str) -> Result<()> {
        let request = RestoreRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            label: label.to_owned(),
        };
        invoke::<operations::CoordinatorRestore>(&*self.runtime, &request)
            .await
            .map(|EmptyResult {}| ())
    }

    pub async fn detach(&self, workspace: &str) -> Result<EmptyResult> {
        let request = self.workspace_request(workspace)?;
        invoke::<operations::CoordinatorDetach>(&*self.runtime, &request).await
    }

    /// Grow a workspace's image, or its build volume and seed. Capacity only ever goes up; a
    /// smaller request is refused.
    pub async fn resize(
        &self,
        workspace: &str,
        capacity: &str,
        volume: ResizeVolume,
    ) -> Result<ResizeResult> {
        let request = ResizeRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            capacity: capacity.to_owned(),
            volume,
        };
        invoke::<operations::CoordinatorResize>(&*self.runtime, &request).await
    }

    /// Rewrite a workspace's image contiguously, so a clone of it stops paying for its extents on
    /// the first write. A busy workspace refuses before its image is touched.
    pub async fn defragment(&self, workspace: &str) -> Result<DefragmentResult> {
        let request = self.workspace_request(workspace)?;
        invoke::<operations::CoordinatorDefragment>(&*self.runtime, &request).await
    }

    /// Refreeze a target's seed from its live build volume when the seed is behind it and the
    /// volume has no writer; a writer leaves the seed as it is and is named.
    pub async fn reseed(&self, workspace: &str) -> Result<ReseedResult> {
        let request = self.workspace_request(workspace)?;
        invoke::<operations::CoordinatorReseed>(&*self.runtime, &request).await
    }

    pub async fn assign_slot(&self, workspace: &str, slot: u32) -> Result<()> {
        let request = SlotRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            slot,
        };
        invoke::<operations::CoordinatorAssignSlot>(&*self.runtime, &request)
            .await
            .map(|EmptyResult {}| ())
    }

    pub async fn destroy(&self, workspace: &str, options: RemoveOptions) -> Result<RemoveReport> {
        let request = DestroyRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            options,
        };
        invoke::<operations::CoordinatorDestroy>(&*self.runtime, &request).await
    }

    pub async fn gc(&self, options: GcOptions) -> Result<GcReport> {
        let request = GcRequest {
            repo_id: self.repo_id(),
            options,
        };
        invoke::<operations::CoordinatorGc>(&*self.runtime, &request).await
    }

    /// Remove the adopted project end to end: every session workspace, the collection of their
    /// images, the abandon bundles (under `abandon`), and main's restore, which unbinds it. A
    /// refusal leaves the rest for the next call; a store that changed under the collection is
    /// `Conflict` with [`crate::error::Retry::GcPlanStale`], resolved by calling again.
    pub async fn remove_project(
        &self,
        options: RemoveProjectOptions,
    ) -> Result<RemoveProjectReport> {
        let request = RemoveProjectRequest {
            repo_id: self.repo_id(),
            options,
        };
        invoke::<operations::CoordinatorRemoveProject>(&*self.runtime, &request).await
    }

    pub async fn repo_mirror(&self, workspace: &str, url: &Url) -> Result<MirrorInfo> {
        let request = MirrorRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            url: url.as_str().to_owned(),
        };
        invoke::<operations::CoordinatorRepoMirror>(&*self.runtime, &request).await
    }

    pub async fn set_checkpoint_quota(
        &self,
        workspace: &str,
        quota: CheckpointQuota,
    ) -> Result<()> {
        let request = QuotaRequest {
            repo_id: self.repo_id(),
            workspace: workspace_name(workspace)?,
            quota,
        };
        invoke::<operations::CoordinatorSetCheckpointQuota>(&*self.runtime, &request)
            .await
            .map(|EmptyResult {}| ())
    }

    pub async fn doctor(&self) -> Result<DoctorReport> {
        let request = RepoRequest {
            repo_id: self.repo_id(),
        };
        invoke::<operations::CoordinatorDoctor>(&*self.runtime, &request).await
    }

    pub async fn worker(&self, workspace: &str) -> Result<WorkspaceHandle> {
        let request = self.workspace_request(workspace)?;
        let WorkerView(view) =
            invoke::<operations::CoordinatorWorker>(&*self.runtime, &request).await?;
        Ok(self.worker_handle(view))
    }

    /// The worker capability the controller minted as `view`.
    pub(super) fn worker_handle(&self, view: WorkspaceView) -> WorkspaceHandle {
        WorkspaceHandle::new(self.workspace_ref(view), Arc::clone(&self.runtime))
    }
}

impl Binder for Project {
    type Fields<'a> = RepoFields<'a>;

    fn binding(&self) -> Binding<'_, RepoFields<'_>> {
        Binding {
            runtime: &self.runtime,
            authority: RepoFields {
                repo_id: &self.repo_id,
            },
        }
    }
}

impl Binder for WorkspaceRef {
    type Fields<'a> = WorkspaceFields<'a>;

    fn binding(&self) -> Binding<'_, WorkspaceFields<'_>> {
        Binding {
            runtime: &self.runtime,
            authority: WorkspaceFields {
                repo_id: &self.info.repo_id,
                workspace: &self.info.workspace,
                workspace_incarnation: &self.info.workspace_incarnation,
            },
        }
    }
}

impl Binder for Coordinator {
    type Fields<'a> = RepoFields<'a>;

    fn binding(&self) -> Binding<'_, RepoFields<'_>> {
        Binding {
            runtime: &self.runtime,
            authority: RepoFields {
                repo_id: &self.project.repo_id,
            },
        }
    }
}

impl Binder for WorkspaceHandle {
    type Fields<'a> = WorkspaceFields<'a>;

    fn binding(&self) -> Binding<'_, WorkspaceFields<'_>> {
        Binding {
            runtime: &self.runtime,
            authority: self.authority.fields(),
        }
    }
}

impl Binder for JobHandle {
    type Fields<'a> = JobFields<'a>;

    fn binding(&self) -> Binding<'_, JobFields<'_>> {
        Binding {
            runtime: &self.runtime,
            authority: self.authority.job_fields(&self.id),
        }
    }
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Coordinator")
            .field("project", &self.project)
            .finish_non_exhaustive()
    }
}

/// Non-escalating capability for exactly one workspace.
pub struct WorkspaceHandle {
    workspace: WorkspaceRef,
    authority: Arc<WorkspaceAuthority>,
    runtime: Arc<dyn ControllerRuntime>,
}

impl WorkspaceHandle {
    fn new(workspace: WorkspaceRef, runtime: Arc<dyn ControllerRuntime>) -> Self {
        let authority = Arc::new(WorkspaceAuthority::from_info(workspace.info()));
        Self {
            workspace,
            authority,
            runtime,
        }
    }

    pub fn name(&self) -> &WorkspaceName {
        &self.authority.workspace
    }

    pub fn mount_path(&self) -> &Path {
        self.workspace.mount_path()
    }

    /// Returns the immutable information this handle was minted on, incarnation included: the fence
    /// every call it makes carries, so a caller proves the incarnation without another inventory RPC.
    pub fn info(&self) -> &WorkspaceInfo {
        self.workspace.info()
    }

    pub async fn exec(&self, request: ExecRequest) -> Result<JobHandle> {
        exec_job(&self.runtime, Arc::clone(&self.authority), None, request).await
    }

    pub async fn shell(&self, session: Option<&str>) -> Result<Session> {
        let name = session.map(str::to_owned);
        let EmptyResult {} = invoke::<operations::WorkerShell>(
            &*self.runtime,
            &self.authority.session(name.clone()),
        )
        .await?;
        Ok(Session {
            authority: Arc::clone(&self.authority),
            name,
            runtime: Arc::clone(&self.runtime),
        })
    }

    pub async fn list_jobs(&self) -> Result<Vec<JobInfo>> {
        invoke::<operations::WorkerListJobs>(&*self.runtime, &self.authority.scope()).await
    }

    pub async fn job(&self, id: JobId) -> Result<JobHandle> {
        let _: JobInfo =
            invoke::<operations::WorkerJob>(&*self.runtime, &self.authority.job(id)).await?;
        Ok(self.job_handle(id))
    }

    /// A job's terminal record from the workspace's durable records, and a handle whose
    /// [`JobHandle::logs`] reads its sealed output from any offset. Answered for any job of this
    /// incarnation that has ended — including one an earlier supervisor ran and sealed, which
    /// [`Self::job`] and [`JobHandle::status`] answer only while that supervisor serves: a drained
    /// supervisor of another build retires the moment its last job ends.
    pub async fn sealed(&self, id: JobId) -> Result<(SealedJob, JobHandle)> {
        let sealed =
            invoke::<operations::JobSealed>(&*self.runtime, &self.authority.job(id)).await?;
        Ok((sealed, self.job_handle(id)))
    }

    pub(super) fn job_handle(&self, id: JobId) -> JobHandle {
        JobHandle {
            authority: Arc::clone(&self.authority),
            id,
            runtime: Arc::clone(&self.runtime),
        }
    }

    pub async fn checkpoint(&self, options: CheckpointOptions) -> Result<String> {
        let WorkerScope {
            repo_id,
            workspace,
            workspace_incarnation,
        } = self.authority.scope();
        let request = CheckpointRequest {
            repo_id,
            workspace,
            workspace_incarnation,
            options,
        };
        invoke::<operations::WorkerCheckpoint>(&*self.runtime, &request)
            .await
            .map(|result| result.label)
    }

    pub async fn push(&self, options: PushOptions) -> Result<PushReport> {
        let WorkerScope {
            repo_id,
            workspace,
            workspace_incarnation,
        } = self.authority.scope();
        let request = PushRequest {
            repo_id,
            workspace,
            workspace_incarnation,
            options,
        };
        invoke::<operations::WorkerPush>(&*self.runtime, &request).await
    }

    pub async fn grants(&self) -> Result<GrantSet> {
        let WorkerScope {
            repo_id,
            workspace,
            workspace_incarnation,
        } = self.authority.scope();
        let request = WorkspaceGrantsRequest {
            repo_id,
            workspace,
            workspace_incarnation: Some(workspace_incarnation),
        };
        invoke::<operations::WorkspaceGrants>(&*self.runtime, &request).await
    }
}

async fn exec_job(
    runtime: &Arc<dyn ControllerRuntime>,
    authority: Arc<WorkspaceAuthority>,
    session: Option<&str>,
    request: ExecRequest,
) -> Result<JobHandle> {
    let id = runtime.exec(&authority, session, request).await?;
    Ok(JobHandle {
        authority,
        id,
        runtime: Arc::clone(runtime),
    })
}

impl fmt::Debug for WorkspaceHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceHandle")
            .field("workspace", &self.workspace)
            .finish_non_exhaustive()
    }
}

pub struct JobHandle {
    authority: Arc<WorkspaceAuthority>,
    id: JobId,
    runtime: Arc<dyn ControllerRuntime>,
}

impl JobHandle {
    pub fn id(&self) -> JobId {
        self.id
    }

    pub async fn status(&self) -> Result<JobInfo> {
        invoke::<operations::JobStatus>(&*self.runtime, &self.authority.job(self.id)).await
    }

    /// A bounded slice of both streams: after `cursor` when it is named, else their latest
    /// bounded tail. `next` continues after the slice; a cursor past a stream's admitted bytes is
    /// a usage error.
    pub async fn tail(
        &self,
        cursor: Option<JobJournalCursor>,
        limits: JobTailLimits,
    ) -> Result<JobTail> {
        let request = TailRequest {
            repo_id: self.authority.repo_id.clone(),
            workspace: self.authority.workspace.clone(),
            workspace_incarnation: self.authority.workspace_incarnation.clone(),
            job_id: self.id,
            cursor,
            limits,
        };
        invoke::<operations::JobTailRead>(&*self.runtime, &request).await
    }

    /// One stream's bytes from `offset` on: a reader that holds the first `offset` bytes already
    /// continues from there. `follow` keeps reading past end of file until the job is terminal.
    pub async fn logs(
        &self,
        stream: JobStream,
        offset: u64,
        follow: bool,
    ) -> Result<RawByteStream> {
        self.runtime
            .logs(Arc::clone(&self.authority), self.id, stream, offset, follow)
            .await
    }

    /// Attaches to the running or ended job without starting a process: its stdin, and its two
    /// raw streams resumed at `cursor`, or at byte zero when it is omitted.
    pub async fn attach(&self, cursor: Option<JobJournalCursor>) -> Result<JobAttachment> {
        self.runtime
            .attach(
                Arc::clone(&self.authority),
                self.id,
                cursor.unwrap_or_default(),
            )
            .await
    }

    pub async fn detach(&self) -> Result<()> {
        invoke::<operations::JobDetach>(&*self.runtime, &self.authority.job(self.id))
            .await
            .map(|EmptyResult {}| ())
    }

    pub async fn wait(&self) -> Result<JobInfo> {
        invoke::<operations::JobWait>(&*self.runtime, &self.authority.job(self.id)).await
    }

    pub async fn kill(&self) -> Result<()> {
        self.runtime.kill(&self.authority, self.id).await
    }
}

impl fmt::Debug for JobHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobHandle")
            .field("authority", &self.authority)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

pub struct Session {
    authority: Arc<WorkspaceAuthority>,
    name: Option<String>,
    runtime: Arc<dyn ControllerRuntime>,
}

impl Session {
    pub async fn run(&self, request: ExecRequest) -> Result<JobHandle> {
        exec_job(
            &self.runtime,
            Arc::clone(&self.authority),
            self.name.as_deref(),
            request,
        )
        .await
    }

    pub fn is_named(&self) -> bool {
        self.name.is_some()
    }

    pub async fn close(self) -> Result<()> {
        invoke::<operations::SessionClose>(&*self.runtime, &self.authority.session(self.name))
            .await
            .map(|EmptyResult {}| ())
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("authority", &self.authority)
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::call::{Arguments, Serves, call};
    use crate::api::dto::RunSandboxMode;
    use crate::api::operations::{self, OPERATIONS, Scope};
    use serde_json::json;
    use std::future;
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

    fn empty_params() -> Params {
        serde_json::value::to_raw_value(&json!({})).expect("raw params")
    }

    #[derive(Default)]
    struct TestRuntime {
        mode: AtomicU8,
        log_calls: AtomicUsize,
        status_calls: AtomicUsize,
        stdin_writes: AtomicUsize,
        active_calls: AtomicUsize,
        rpc_calls: AtomicUsize,
    }

    struct ActiveCall<'a>(&'a AtomicUsize);

    impl Drop for ActiveCall<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ControllerRuntime for TestRuntime {
        async fn call(&self, method: &'static str, _params: Params) -> Result<Value> {
            self.rpc_calls.fetch_add(1, Ordering::SeqCst);
            match method {
                "job.status" => {
                    let call = self.status_calls.fetch_add(1, Ordering::SeqCst);
                    if self.mode.load(Ordering::SeqCst) == 4 && call == 0 {
                        Ok(running_job_value())
                    } else {
                        Ok(terminal_job_value())
                    }
                }
                _ => Ok(json!({})),
            }
        }

        async fn call_reporting(
            &self,
            method: &'static str,
            _params: Params,
            _steps: tokio::sync::mpsc::UnboundedSender<StepReport>,
        ) -> Result<Value> {
            unreachable!("the job-stream tests make no reported call, and {method} is one")
        }

        async fn upload(
            &self,
            method: &'static str,
            _params: Params,
            _bytes: Bytes,
        ) -> Result<Value> {
            if method == "job.attachWrite" {
                self.stdin_writes.fetch_add(1, Ordering::SeqCst);
            }
            Ok(json!({}))
        }

        async fn download(
            &self,
            method: &'static str,
            _params: Params,
            _expected_offset: u64,
        ) -> Result<BinaryDownload> {
            assert_eq!(method, "job.logs");
            let call = self.log_calls.fetch_add(1, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                1 => {
                    self.active_calls.fetch_add(1, Ordering::SeqCst);
                    let _active = ActiveCall(&self.active_calls);
                    future::pending().await
                }
                2 => match call {
                    0 => Ok(BinaryDownload {
                        bytes: b"abc".to_vec(),
                        eof: false,
                    }),
                    1 => Ok(BinaryDownload {
                        bytes: b"def".to_vec(),
                        eof: false,
                    }),
                    _ => Ok(BinaryDownload {
                        bytes: b"ghi".to_vec(),
                        eof: true,
                    }),
                },
                3 => Ok(BinaryDownload {
                    bytes: vec![b'x'],
                    eof: false,
                }),
                4 if call == 0 => Ok(BinaryDownload {
                    bytes: Vec::new(),
                    eof: true,
                }),
                4 => Ok(BinaryDownload {
                    bytes: b"after-eof".to_vec(),
                    eof: true,
                }),
                _ => Ok(BinaryDownload {
                    bytes: Vec::new(),
                    eof: true,
                }),
            }
        }

        async fn exec(
            &self,
            _authority: &WorkspaceAuthority,
            _session: Option<&str>,
            _request: ExecRequest,
        ) -> Result<JobId> {
            Err(CowshedError::internal("unexpected test exec"))
        }

        async fn logs(
            &self,
            _authority: Arc<WorkspaceAuthority>,
            _id: JobId,
            _stream: JobStream,
            _offset: u64,
            _follow: bool,
        ) -> Result<RawByteStream> {
            Err(CowshedError::internal("unexpected test logs"))
        }

        async fn attach(
            &self,
            _authority: Arc<WorkspaceAuthority>,
            _id: JobId,
            _cursor: JobJournalCursor,
        ) -> Result<JobAttachment> {
            Err(CowshedError::internal("unexpected test attach"))
        }

        async fn kill(&self, _authority: &WorkspaceAuthority, _id: JobId) -> Result<()> {
            Err(CowshedError::internal("test controller rejected kill"))
        }
    }

    fn terminal_job_value() -> Value {
        json!({
            "repoId": "acme/widget",
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "jobId": 7,
            "state": "exited",
            "grantRevision": 1,
            "argv": [{"encoding": "utf8", "data": "true"}],
            "cwd": "packages/app",
            "started": "2016-12-31T23:59:60Z",
            "durationMs": 1,
            "exit": {"kind": "exited", "code": 0},
            "stdout": {
                "storage": {
                    "kind": "captured",
                    "artifact": {
                        "kind": "inline",
                        "data": {"encoding": "utf8", "data": ""}
                    }
                },
                "bytes": 0,
                "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "summary": {"version": 1, "text": "", "truncated": false}
            },
            "stderr": {
                "storage": {
                    "kind": "captured",
                    "artifact": {
                        "kind": "inline",
                        "data": {"encoding": "utf8", "data": ""}
                    }
                },
                "bytes": 0,
                "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "summary": {"version": 1, "text": "", "truncated": false}
            },
            "trace": {
                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId": "00f067aa0ba902b7"
            },
            "stdin": {"kind": "empty", "bytes": 0, "complete": true}
        })
    }

    fn running_job_value() -> Value {
        let mut value = terminal_job_value();
        let object = value.as_object_mut().unwrap();
        object.insert("state".into(), json!("running"));
        object.remove("durationMs");
        object.remove("exit");
        value
    }

    #[cfg(unix)]
    async fn handshake_server(
        stream: std::os::unix::net::UnixStream,
        echo_nonce: bool,
    ) -> Result<()> {
        stream.set_nonblocking(true).unwrap();
        let mut stream = tokio::net::UnixStream::from_std(stream).unwrap();
        let request = read_frame(&mut stream).await?;
        let request: Value = serde_json::from_slice(&request).unwrap();
        let nonce = if echo_nonce {
            request["nonce"].as_str().unwrap()
        } else {
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        };
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let response = codec::encode_server_hello(nonce, &repo_id).unwrap();
        write_frame(&mut stream, &response).await
    }

    #[cfg(unix)]
    fn actor_pair() -> (Arc<dyn ControllerRuntime>, tokio::net::UnixStream) {
        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        let (runtime, _) = spawn_controller_actor(client);
        (runtime, server)
    }

    #[cfg(unix)]
    async fn read_rpc_request(stream: &mut tokio::net::UnixStream) -> (Vec<u8>, Value) {
        let bytes = read_rpc_frame(stream).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap();
        (bytes, value)
    }

    #[cfg(unix)]
    async fn write_rpc_success(
        stream: &mut tokio::net::UnixStream,
        id: u64,
        result: Value,
        binary_length: Option<usize>,
    ) {
        let binary_length = binary_length
            .map(u32::try_from)
            .transpose()
            .expect("test binary length fits wire");
        let response = codec::encode_rpc_success(id, &result, binary_length).unwrap();
        write_rpc_frame(stream, &response).await.unwrap();
    }

    #[cfg(unix)]
    async fn write_raw_frame(stream: &mut tokio::net::UnixStream, bytes: &[u8]) {
        stream.write_u32(bytes.len() as u32).await.unwrap();
        stream.write_all(bytes).await.unwrap();
    }

    fn exec_request(stdin: StdinSource) -> ExecRequest {
        ExecRequest {
            command: crate::api::dto::ExecCommand::Argv(vec!["cat".into()]),
            cwd: None,
            mode: RunSandboxMode::ReadWrite,
            env: std::collections::HashMap::new(),
            trace: None,
            stdin,
            stdout_copy: None,
            stderr_copy: None,
        }
    }
    fn workspace_ref(runtime: Arc<dyn ControllerRuntime>) -> WorkspaceRef {
        WorkspaceRef {
            info: WorkspaceInfo {
                repo_id: RepoId::parse("acme/widget").unwrap(),
                workspace: WorkspaceName::new("raven").unwrap(),
                workspace_incarnation: WorkspaceIncarnation::new(
                    "0198f2c0b7e34dc795f17b238b331c80",
                )
                .unwrap(),
                role: crate::metadata::WorkspaceRole::Workspace,
                mount: PathBuf::from("/mnt/raven"),
                state: super::super::dto::WorkspaceState::Detached,
                branch: None,
                base_commit: None,
                created_at: None,
                checkpoints: Vec::new(),
                snapshot_stale: false,
                landing: None,
            },
            grants: GrantSet::default(),
            runtime,
        }
    }

    fn workspace_handle(runtime: Arc<dyn ControllerRuntime>) -> WorkspaceHandle {
        WorkspaceHandle::new(workspace_ref(Arc::clone(&runtime)), runtime)
    }

    fn test_authority() -> Arc<WorkspaceAuthority> {
        Arc::new(WorkspaceAuthority {
            repo_id: RepoId::parse("acme/widget").unwrap(),
            workspace: WorkspaceName::new("raven").unwrap(),
            workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .unwrap(),
        })
    }
    #[cfg(unix)]
    fn coordinator(runtime: Arc<dyn ControllerRuntime>) -> Coordinator {
        coordinator_over(runtime, watch::channel(None).1)
    }

    #[cfg(unix)]
    fn coordinator_over(
        runtime: Arc<dyn ControllerRuntime>,
        other_build: watch::Receiver<Option<OtherBuild>>,
    ) -> Coordinator {
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id.clone(),
            remote_name: None,
            remote_url: None,
            primary: true,
        }])
        .unwrap();
        let project = Project {
            repo_id: repo_id.clone(),
            binding: Arc::new(binding),
            git_root: Arc::from(Path::new("/repo")),
            paths: ProjectPaths::with_mount_root(
                "/tmp/cowshed-capability-tests",
                "/tmp/cowshed-capability-tests/mnt",
                &repo_id,
            )
            .unwrap(),
            runtime: Arc::clone(&runtime),
        };
        Coordinator {
            project,
            runtime: Arc::clone(&runtime),
            other_build,
            _channel: AuthenticatedControllerChannel { runtime },
        }
    }

    #[test]
    fn workspace_snapshot_accessors_do_not_call_the_controller_or_copy_on_consumption() {
        let runtime = Arc::new(TestRuntime::default());
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let workspace = workspace_ref(runtime_trait);
        let (info, grants) = workspace.snapshot();
        assert!(std::ptr::eq(info, workspace.info()));
        assert!(std::ptr::eq(grants, workspace.grants()));
        assert_eq!(info.workspace.as_str(), "raven");

        let (info, grants) = workspace.into_snapshot();
        assert_eq!(info.workspace.as_str(), "raven");
        assert_eq!(grants, GrantSet::default());
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        assert_eq!(
            workspace_ref(runtime_trait).into_info().workspace.as_str(),
            "raven"
        );
        assert_eq!(runtime.rpc_calls.load(Ordering::SeqCst), 0);
    }

    /// The answer travels wrapped: a bare `null` for a checkout linking no volume would be an
    /// envelope without a result, which the client refuses as invalid.
    #[cfg(unix)]
    #[tokio::test]
    async fn build_volume_is_fenced_and_answers_a_volume_or_none() {
        let (runtime, mut server) = actor_pair();
        let workspace = workspace_ref(runtime);
        let server_task = tokio::spawn(async move {
            for volume in [json!("/mnt/.build/acme/widget/0123"), Value::Null] {
                let (_, request) = read_rpc_request(&mut server).await;
                assert_eq!(request["method"], "workspace.buildVolume");
                assert_eq!(
                    request["params"],
                    json!({
                        "repoId": "acme/widget",
                        "workspace": "raven",
                        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    })
                );
                let id = request["id"].as_u64().unwrap();
                write_rpc_success(&mut server, id, json!({ "volume": volume }), None).await;
            }
        });
        assert_eq!(
            workspace.build_volume().await.expect("linked"),
            Some(PathBuf::from("/mnt/.build/acme/widget/0123"))
        );
        assert_eq!(workspace.build_volume().await.expect("unlinked"), None);
        server_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn project_workspace_at_forwards_the_authoritative_path_rpc() {
        let template_runtime: Arc<dyn ControllerRuntime> = Arc::new(TestRuntime::default());
        let template = workspace_ref(template_runtime);
        let response = json!({ "info": template.info, "grants": template.grants });
        let (runtime, mut server) = actor_pair();
        let coordinator = coordinator(runtime);
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "project.workspaceAt");
            assert_eq!(
                request["params"],
                json!({ "repoId": "acme/widget", "path": "/mnt/raven/src/lib.rs" })
            );
            write_rpc_success(&mut server, request["id"].as_u64().unwrap(), response, None).await;
        });

        let workspace = coordinator
            .project()
            .workspace_at("/mnt/raven/src/lib.rs")
            .await
            .expect("workspace resolution");
        assert_eq!(workspace.name().as_str(), "raven");
        server_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_handle_info_is_the_minted_snapshot_its_calls_fence_on() {
        let template_runtime: Arc<dyn ControllerRuntime> = Arc::new(TestRuntime::default());
        let mut minted = workspace_ref(template_runtime).into_info();
        minted.workspace_incarnation =
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c81").unwrap();
        let response = json!({ "info": minted, "grants": GrantSet::default() });
        let (runtime, mut server) = actor_pair();
        let coordinator = coordinator(runtime);
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "coordinator.worker");
            write_rpc_success(&mut server, request["id"].as_u64().unwrap(), response, None).await;
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "workspace.grants");
            let fence = request["params"]["workspaceIncarnation"].clone();
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                json!(GrantSet::default()),
                None,
            )
            .await;
            fence
        });

        let handle = coordinator.worker("raven").await.expect("worker mint");
        assert_eq!(handle.info(), &minted);
        handle.grants().await.expect("grants read");
        assert_eq!(
            server_task.await.unwrap(),
            json!(handle.info().workspace_incarnation)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checkpoint_forwards_label_and_pin_intent() {
        let (runtime, mut server) = actor_pair();
        let handle = workspace_handle(runtime);
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "worker.checkpoint");
            assert_eq!(
                request["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "options": {"label": "before-write", "keep": true},
                })
            );
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                json!({"label": "before-write"}),
                None,
            )
            .await;
        });

        let label = handle
            .checkpoint(CheckpointOptions {
                label: Some("before-write".into()),
                keep: true,
            })
            .await
            .unwrap();
        assert_eq!(label, "before-write");
        server_task.await.unwrap();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn workspace_handle_keeps_the_original_incarnation_after_snapshot_mutation() {
        let (runtime, mut server) = actor_pair();
        let mut handle = workspace_handle(runtime);
        handle.workspace.info.workspace_incarnation =
            WorkspaceIncarnation::new("1198f2c0b7e34dc795f17b238b331c80").unwrap();
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "worker.listJobs");
            assert_eq!(
                request["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                })
            );
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                json!([]),
                None,
            )
            .await;
        });

        assert!(handle.list_jobs().await.unwrap().is_empty());
        server_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn coordinator_doctor_uses_the_exact_owned_capability_method() {
        let (runtime, mut server) = actor_pair();
        let coordinator = coordinator(runtime);
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "coordinator.doctor");
            assert_eq!(request["params"], json!({"repoId": "acme/widget"}));
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                json!({"healthy": true, "findings": []}),
                None,
            )
            .await;
        });

        assert_eq!(
            coordinator.doctor().await.unwrap(),
            DoctorReport {
                healthy: true,
                findings: Vec::new(),
            }
        );
        server_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn inherited_socket_handshake_binds_actor_and_repo() {
        let (client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let server = tokio::spawn(handshake_server(server, true));
        let descriptor: OwnedFd = client.into();
        let (cowshed, token) = Cowshed::connect(descriptor).await.unwrap();
        assert_eq!(token.repo_id.as_str(), "acme/widget");
        assert!(Arc::ptr_eq(&cowshed.runtime, &token.channel.runtime));
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn inherited_socket_handshake_rejects_wrong_nonce() {
        let (client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let server = tokio::spawn(handshake_server(server, false));
        let descriptor: OwnedFd = client.into();
        let error = Cowshed::connect(descriptor).await.unwrap_err();
        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        assert!(error.message.contains("nonce"));
        server.await.unwrap().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn non_utf8_project_path_is_a_typed_usage_error() {
        use std::os::unix::ffi::OsStringExt;

        let (sender, _receiver) = mpsc::channel(1);
        let cowshed = Cowshed {
            runtime: Arc::new(ActorRuntime { sender }),
            other_build: watch::channel(None).1,
        };
        let path = PathBuf::from(std::ffi::OsString::from_vec(vec![b'/', 0xff]));
        let error = cowshed.open(path).await.unwrap_err();
        assert_eq!(error.code, ErrorCode::Usage);
        assert!(error.message.contains("UTF-8"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn inline_stdin_is_a_raw_frame_and_never_json_bytes() {
        let (runtime, mut server) = actor_pair();
        let payload = Bytes::from_static(&[0, 0xff, 0x80, b'[', b'1', b',', b'2', b']']);
        let expected = payload.clone();
        let server_task = tokio::spawn(async move {
            let (header, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "worker.exec");
            assert_eq!(request["binaryLength"], expected.len());
            assert_eq!(
                request["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "session": null,
                    "argv": [{"encoding": "utf8", "data": "cat"}],
                    "cwd": null,
                    "mode": "readWrite",
                    "env": {},
                    "trace": null,
                    "stdin": {"kind": "inline"},
                    "stdoutCopy": null,
                    "stderrCopy": null,
                })
            );
            assert!(!header.contains(&0));
            assert!(!header.contains(&0xff));
            let frame = read_binary_frame(&mut server, expected.len())
                .await
                .unwrap();
            assert_eq!(frame, expected.as_ref());
            write_rpc_success(&mut server, request["id"].as_u64().unwrap(), json!(7), None).await;
        });

        let handle = workspace_handle(runtime);
        let job = handle
            .exec(exec_request(StdinSource::Inline(payload)))
            .await
            .unwrap();
        assert_eq!(job.id(), JobId::new(7).unwrap());
        server_task.await.unwrap();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn named_session_runs_preserve_exact_session_identity() {
        let (runtime, mut server) = actor_pair();
        let server_task = tokio::spawn(async move {
            let (_, shell) = read_rpc_request(&mut server).await;
            assert_eq!(shell["method"], "worker.shell");
            assert_eq!(
                shell["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "session": "build-7",
                })
            );
            write_rpc_success(&mut server, shell["id"].as_u64().unwrap(), json!({}), None).await;

            for job_id in [7_u64, 8] {
                let (_, exec) = read_rpc_request(&mut server).await;
                assert_eq!(exec["method"], "worker.exec");
                assert_eq!(
                    exec["params"],
                    json!({
                        "repoId": "acme/widget",
                        "workspace": "raven",
                        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                        "session": "build-7",
                        "argv": [{"encoding": "utf8", "data": "cat"}],
                        "cwd": null,
                        "mode": "readWrite",
                        "env": {},
                        "trace": null,
                        "stdin": {"kind": "empty"},
                        "stdoutCopy": null,
                        "stderrCopy": null,
                    })
                );
                write_rpc_success(
                    &mut server,
                    exec["id"].as_u64().unwrap(),
                    json!(job_id),
                    None,
                )
                .await;
            }

            let (_, close) = read_rpc_request(&mut server).await;
            assert_eq!(close["method"], "session.close");
            assert_eq!(
                close["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "session": "build-7",
                })
            );
            write_rpc_success(&mut server, close["id"].as_u64().unwrap(), json!({}), None).await;
        });

        let handle = workspace_handle(runtime);
        let session = handle.shell(Some("build-7")).await.unwrap();
        assert!(session.is_named());
        assert_eq!(
            session
                .run(exec_request(StdinSource::Empty))
                .await
                .unwrap()
                .id(),
            JobId::new(7).unwrap()
        );
        assert_eq!(
            session
                .run(exec_request(StdinSource::Empty))
                .await
                .unwrap()
                .id(),
            JobId::new(8).unwrap()
        );
        session.close().await.unwrap();
        server_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn streamed_stdin_chunks_and_close_preserve_binary_framing() {
        let (runtime, mut server) = actor_pair();
        let payload = vec![0, 0xff, 0x80, b'x'];
        let expected = payload.clone();
        let server_task = tokio::spawn(async move {
            let (_, exec) = read_rpc_request(&mut server).await;
            assert_eq!(exec["method"], "worker.exec");
            assert!(exec.get("binaryLength").is_none());
            assert_eq!(
                exec["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "session": null,
                    "argv": [{"encoding": "utf8", "data": "cat"}],
                    "cwd": null,
                    "mode": "readWrite",
                    "env": {},
                    "trace": null,
                    "stdin": {"kind": "stream"},
                    "stdoutCopy": null,
                    "stderrCopy": null,
                })
            );
            write_rpc_success(&mut server, exec["id"].as_u64().unwrap(), json!(7), None).await;

            let (chunk_header, chunk) = read_rpc_request(&mut server).await;
            assert_eq!(chunk["method"], "worker.stdinChunk");
            assert_eq!(chunk["binaryLength"], expected.len());
            assert!(chunk["params"].get("bytes").is_none());
            assert_eq!(
                chunk["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "jobId": 7,
                })
            );
            assert!(!chunk_header.contains(&0));
            assert!(!chunk_header.contains(&0xff));
            let frame = read_binary_frame(&mut server, expected.len())
                .await
                .unwrap();
            assert_eq!(frame, expected);
            write_rpc_success(&mut server, chunk["id"].as_u64().unwrap(), json!({}), None).await;

            let (_, close) = read_rpc_request(&mut server).await;
            assert_eq!(close["method"], "worker.stdinClose");
            assert!(close.get("binaryLength").is_none());
            assert_eq!(
                close["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "jobId": 7,
                })
            );
            write_rpc_success(&mut server, close["id"].as_u64().unwrap(), json!({}), None).await;
        });

        let authority = test_authority();
        let id = runtime
            .exec(
                &authority,
                None,
                exec_request(StdinSource::Stream(Box::pin(std::io::Cursor::new(payload)))),
            )
            .await
            .unwrap();
        assert_eq!(id, JobId::new(7).unwrap());
        server_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn attachment_stdin_write_uses_one_exact_raw_frame() {
        let (runtime, mut server) = actor_pair();
        let payload = Bytes::from_static(&[0, 0xfe, 0xff, b'i', b'n']);
        let expected = payload.clone();
        let server_task = tokio::spawn(async move {
            let (header, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "job.attachWrite");
            assert_eq!(request["binaryLength"], expected.len());
            assert!(request["params"].get("bytes").is_none());
            assert_eq!(
                request["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "jobId": 7,
                })
            );
            assert!(!header.contains(&0));
            assert!(!header.contains(&0xff));
            let frame = read_binary_frame(&mut server, expected.len())
                .await
                .unwrap();
            assert_eq!(frame, expected.as_ref());
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                json!({}),
                None,
            )
            .await;
        });
        let stdin = JobStdin {
            authority: test_authority(),
            id: JobId::new(7).unwrap(),
            runtime,
        };

        stdin.write(payload).await.unwrap();
        server_task.await.unwrap();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn job_status_uses_the_handle_repo_workspace_and_incarnation() {
        let (runtime, mut server) = actor_pair();
        let handle = JobHandle {
            authority: test_authority(),
            id: JobId::new(7).unwrap(),
            runtime,
        };
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "job.status");
            assert_eq!(
                request["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                    "jobId": 7,
                })
            );
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                terminal_job_value(),
                None,
            )
            .await;
        });

        assert_eq!(
            handle.status().await.unwrap().job_id,
            JobId::new(7).unwrap()
        );
        server_task.await.unwrap();
    }

    fn arguments(value: Value) -> Arguments {
        let Value::Object(arguments) = value else {
            panic!("arguments are a JSON object: {value}");
        };
        arguments
    }

    #[track_caller]
    fn refused_as_bound<T>(result: Result<T>, field: &str) {
        let Err(error) = result else {
            panic!("a caller's {field} reached the controller");
        };
        assert_eq!(error.code, ErrorCode::Usage);
        assert!(
            error.message.contains(&format!(
                "arguments name {field}, which the handle supplies"
            )),
            "{}",
            error.message
        );
    }

    /// A caller can never name what its handle binds: a coordinator cannot be pointed at another
    /// repository, a workspace reference or worker at another workspace or incarnation, a job
    /// handle at another job. Each is refused before anything reaches the controller.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_caller_field_the_handle_binds_is_refused_before_the_wire() {
        let runtime = Arc::new(TestRuntime::default());
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let coordinator = coordinator(Arc::clone(&runtime_trait));
        let workspace = workspace_ref(Arc::clone(&runtime_trait));
        let worker = workspace_handle(Arc::clone(&runtime_trait));
        let job = JobHandle {
            authority: test_authority(),
            id: JobId::new(7).unwrap(),
            runtime: runtime_trait,
        };

        refused_as_bound(
            call::<operations::ProjectList, _>(
                coordinator.project(),
                arguments(json!({ "repoId": "acme/other" })),
            )
            .await,
            "repoId",
        );
        refused_as_bound(
            call::<operations::CoordinatorDestroy, _>(
                &coordinator,
                arguments(json!({
                    "repoId": "acme/other",
                    "workspace": "raven",
                    "options": { "force": true },
                })),
            )
            .await,
            "repoId",
        );
        refused_as_bound(
            call::<operations::WorkspaceAttach, _>(
                &workspace,
                arguments(json!({ "workspace": "crow" })),
            )
            .await,
            "workspace",
        );
        for (field, value) in [
            ("repoId", json!("acme/other")),
            ("workspace", json!("crow")),
            (
                "workspaceIncarnation",
                json!("1198f2c0b7e34dc795f17b238b331c80"),
            ),
        ] {
            refused_as_bound(
                call::<operations::WorkerListJobs, _>(&worker, arguments(json!({ field: value })))
                    .await,
                field,
            );
        }
        refused_as_bound(
            call::<operations::JobKill, _>(&job, arguments(json!({ "jobId": 8 }))).await,
            "jobId",
        );
        assert_eq!(runtime.rpc_calls.load(Ordering::SeqCst), 0);
    }

    /// What reaches the controller is the handle's authority merged with the caller's fields, so a
    /// coordinator's call names its own repository and a worker's its own workspace incarnation.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_call_carries_the_handles_authority_beside_the_callers_fields() {
        let (runtime, mut server) = actor_pair();
        let coordinator = coordinator(Arc::clone(&runtime));
        let worker = workspace_handle(runtime);
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "coordinator.destroy");
            assert_eq!(
                request["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "crow",
                    "options": { "force": true, "restore": false, "abandon": false },
                })
            );
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                json!({}),
                None,
            )
            .await;
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "worker.listJobs");
            assert_eq!(
                request["params"],
                json!({
                    "repoId": "acme/widget",
                    "workspace": "raven",
                    "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                })
            );
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                json!([]),
                None,
            )
            .await;
        });

        call::<operations::CoordinatorDestroy, _>(
            &coordinator,
            arguments(json!({ "workspace": "crow", "options": { "force": true } })),
        )
        .await
        .expect("destroy through the coordinator");
        let jobs = call::<operations::WorkerListJobs, _>(&worker, Arguments::new())
            .await
            .expect("list jobs through the worker");
        assert!(jobs.is_empty());
        server_task.await.unwrap();
    }

    /// A workspace reference fences its repository and workspace, not the incarnation it was
    /// resolved at: its grants read names none, exactly as [`WorkspaceRef::refresh_grants`] does,
    /// so a reference resolved before its name was recreated still reads by name. A worker is
    /// fenced on its whole incarnation, so the same stale incarnation is refused. The server here
    /// answers by the controller's rule, which
    /// `a_grants_read_that_holds_an_incarnation_is_fenced_on_a_coordinator_connection` proves.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_stale_reference_reads_grants_by_name_while_a_stale_worker_is_refused() {
        let (runtime, mut server) = actor_pair();
        // Both handles hold incarnation …80; the workspace has since been recreated as …81.
        let reference = workspace_ref(Arc::clone(&runtime));
        let worker = workspace_handle(runtime);
        let stale = reference.info().workspace_incarnation.clone();
        let live = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c81").unwrap();
        let server_task = tokio::spawn(async move {
            let mut sent = Vec::new();
            for _ in 0..3 {
                let (_, request) = read_rpc_request(&mut server).await;
                assert_eq!(request["method"], "workspace.grants");
                let id = request["id"].as_u64().unwrap();
                let params: WorkspaceGrantsRequest =
                    serde_json::from_value(request["params"].clone()).unwrap();
                match params.workspace_incarnation {
                    Some(held) if held != live => {
                        let error = CowshedError::fence_refusal(
                            crate::error::FenceRefusal::IncarnationMoved {
                                workspace: params.workspace,
                                observed: live.clone(),
                            },
                            "workspace incarnation is stale",
                            "resolve the workspace again and retry",
                        );
                        let response = codec::encode_rpc_error(id, &error).unwrap();
                        write_rpc_frame(&mut server, &response).await.unwrap();
                    }
                    _ => write_rpc_success(&mut server, id, json!(GrantSet::default()), None).await,
                }
                sent.push(request["params"].clone());
            }
            sent
        });

        let read = reference
            .refresh_grants()
            .await
            .expect("a stale reference reads its grants by name");
        let called = call::<operations::WorkspaceGrants, _>(&reference, Arguments::new())
            .await
            .expect("a stale reference's generated call reads by name too");
        assert_eq!(called, read);
        let refused = call::<operations::WorkspaceGrants, _>(&worker, Arguments::new())
            .await
            .expect_err("a stale worker is refused");
        assert_eq!(refused.code, ErrorCode::Conflict);
        assert!(refused.fence_source().is_some(), "{refused:?}");

        let by_name = json!({ "repoId": "acme/widget", "workspace": "raven" });
        let mut fenced = by_name.clone();
        fenced["workspaceIncarnation"] = json!(stale);
        assert_eq!(
            server_task.await.unwrap(),
            [by_name.clone(), by_name, fenced]
        );
    }

    /// Every operation a handle serves builds, from the handle's fields and the caller's, exactly
    /// the request the controller decodes from the same whole object: the generated construction
    /// and the declaration agree on every field, spelling and default of the one request corpus.
    /// A bound field the handle supplies as `None` is absent from that object.
    #[cfg(unix)]
    #[test]
    fn every_served_request_is_the_declared_decoding_of_the_same_fields() {
        struct Agrees {
            corpus: std::collections::BTreeMap<String, Value>,
            checked: std::collections::BTreeSet<&'static str>,
        }

        impl operations::served::EachServed for Agrees {
            fn served<O: Operation, H: Serves<O>>(&mut self, handle: &H, absent: &[&str])
            where
                O::Request: PartialEq + fmt::Debug,
            {
                let Some(Value::Object(corpus)) = self.corpus.get(O::METHOD) else {
                    panic!("{} has a corpus request object", O::METHOD);
                };
                let mut whole = corpus.clone();
                // A field the handle supplies as `None` is still bound, so no caller names it; the
                // declaration decodes it as absent.
                for field in absent {
                    assert!(
                        H::BOUND.contains(field),
                        "{}: {field} is supplied as absent, so it must be bound",
                        O::METHOD
                    );
                    whole.remove(*field);
                }
                for (field, value) in [
                    ("repoId", json!("acme/widget")),
                    ("workspace", json!("raven")),
                    (
                        "workspaceIncarnation",
                        json!("0198f2c0b7e34dc795f17b238b331c80"),
                    ),
                    ("jobId", json!(7)),
                ] {
                    if H::BOUND.contains(&field) && !absent.contains(&field) {
                        whole.insert(field.to_owned(), value);
                    }
                }
                let caller: Arguments = whole
                    .iter()
                    .filter(|(field, _)| !H::BOUND.contains(&field.as_str()))
                    .map(|(field, value)| (field.clone(), value.clone()))
                    .collect();
                let declared: O::Request = serde_json::from_value(Value::Object(whole))
                    .unwrap_or_else(|error| panic!("{} corpus request: {error}", O::METHOD));
                let built = H::request(&handle.binding().authority, caller)
                    .unwrap_or_else(|error| panic!("{} built request: {error:?}", O::METHOD));
                assert_eq!(built, declared, "{}", O::METHOD);
                self.checked.insert(O::METHOD);
            }
        }

        let runtime: Arc<dyn ControllerRuntime> = Arc::new(TestRuntime::default());
        let coordinator = coordinator(Arc::clone(&runtime));
        let job = JobHandle {
            authority: test_authority(),
            id: JobId::new(7).unwrap(),
            runtime: Arc::clone(&runtime),
        };
        let mut agrees = Agrees {
            corpus: serde_json::from_str(include_str!("operations.corpus.json"))
                .expect("the corpus is a JSON object of requests"),
            checked: std::collections::BTreeSet::new(),
        };
        operations::served::each_served(
            &mut agrees,
            coordinator.project(),
            &workspace_ref(Arc::clone(&runtime)),
            &coordinator,
            &workspace_handle(runtime),
            &job,
        );
        // `project.open` is how a project is reached, not a method of a handle; every other
        // operation a caller may reach is served by some handle and was checked.
        let reachable: std::collections::BTreeSet<&str> = OPERATIONS
            .iter()
            .filter(|operation| operation.scope != Scope::Internal)
            .map(|operation| operation.method)
            .filter(|method| *method != "project.open")
            .collect();
        assert_eq!(agrees.checked, reachable);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdout_and_stderr_downloads_remain_separate_raw_streams() {
        let (runtime, mut server) = actor_pair();
        let authority = test_authority();
        let id = JobId::new(7).unwrap();
        let mut stdout = poll_job_stream(
            Arc::clone(&runtime),
            Arc::clone(&authority),
            id,
            JobStream::Stdout,
            0,
            false,
        );
        let mut stderr = poll_job_stream(runtime, authority, id, JobStream::Stderr, 0, false);
        let server_task = tokio::spawn(async move {
            for _ in 0..2 {
                let (_, request) = read_rpc_request(&mut server).await;
                assert_eq!(request["method"], "job.logs");
                assert!(request.get("binaryLength").is_none());
                let stream = request["params"]["stream"].as_str().unwrap();
                let offset = request["params"]["offset"].as_u64().unwrap();
                assert_eq!(
                    request["params"],
                    json!({
                        "repoId": "acme/widget",
                        "workspace": "raven",
                        "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
                        "jobId": 7,
                        "stream": stream,
                        "follow": false,
                        "offset": offset,
                    })
                );
                let payload: &[u8] = match stream {
                    "stdout" => &[0, 0xff, b'o'],
                    "stderr" => &[0x80, 0, b'e'],
                    stream => panic!("unexpected stream {stream}"),
                };
                write_rpc_success(
                    &mut server,
                    request["id"].as_u64().unwrap(),
                    json!({"eof": true, "nextOffset": offset + payload.len() as u64}),
                    Some(payload.len()),
                )
                .await;
                write_raw_frame(&mut server, payload).await;
            }
        });

        assert_eq!(
            stdout.next().await.unwrap().unwrap().as_ref(),
            &[0, 0xff, b'o']
        );
        assert_eq!(
            stderr.next().await.unwrap().unwrap().as_ref(),
            &[0x80, 0, b'e']
        );
        assert!(stdout.next().await.is_none());
        assert!(stderr.next().await.is_none());
        server_task.await.unwrap();
    }

    /// A read that names an offset asks the controller for the bytes from there, so a reader that
    /// already holds some continues where it stopped instead of reading them again.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_log_read_continues_from_the_offset_it_names() {
        let (runtime, mut server) = actor_pair();
        let job = JobHandle {
            authority: test_authority(),
            id: JobId::new(7).unwrap(),
            runtime,
        };
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["method"], "job.logs");
            assert_eq!(request["params"]["offset"], 6);
            write_rpc_success(
                &mut server,
                request["id"].as_u64().unwrap(),
                json!({"eof": true, "nextOffset": 12}),
                Some(6),
            )
            .await;
            write_raw_frame(&mut server, b"world\n").await;
        });

        let mut stream = job.logs(JobStream::Stdout, 6, false).await.unwrap();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(bytes, b"world\n");
        server_task.await.unwrap();
    }

    /// The daemon's refusal of the controller's build marks the whole connection, so whoever
    /// holds it learns the controller can run nothing new from data, not from the sentence; any
    /// other refusal leaves the connection unmarked.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_refusal_of_the_controllers_build_marks_its_connection() {
        let (client, mut server) = tokio::net::UnixStream::pair().unwrap();
        let (runtime, other_build) = spawn_controller_actor(client);
        let coordinator = coordinator_over(runtime, other_build);
        let refused = OtherBuild {
            daemon: crate::runtime::supervisor_socket::BuildId::current()
                .unwrap()
                .clone(),
            caller: None,
        };
        let answered = refused.clone();
        let server_task = tokio::spawn(async move {
            for error in [
                CowshedError::conflict("workspace raven is busy", "retry"),
                CowshedError::other_build(answered),
            ] {
                let (_, request) = read_rpc_request(&mut server).await;
                let response =
                    codec::encode_rpc_error(request["id"].as_u64().unwrap(), &error).unwrap();
                write_rpc_frame(&mut server, &response).await.unwrap();
            }
        });
        let marked = coordinator.other_build();

        coordinator.doctor().await.expect_err("busy");
        assert_eq!(*marked.borrow(), None, "another refusal leaves it unmarked");
        let error = coordinator.doctor().await.expect_err("refused");
        assert_eq!(error.other_build_source(), Some(&refused));
        assert_eq!(*marked.borrow(), Some(refused));
        server_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn client_codec_rejects_unknown_response_fields() {
        let (runtime, mut server) = actor_pair();
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            let response = serde_json::to_vec(&json!({
                "id": request["id"],
                "ok": true,
                "result": {},
                "error": null,
                "binaryLength": null,
                "extra": true,
            }))
            .unwrap();
            write_rpc_frame(&mut server, &response).await.unwrap();
        });

        let error = runtime
            .call("project.list", empty_params())
            .await
            .unwrap_err();
        assert!(error.message.contains("response decoding failed"));
        assert!(error.message.contains("unknown field"));
        server_task.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn actor_rejects_binary_protocol_violations() {
        {
            let (runtime, mut server) = actor_pair();
            let server_task = tokio::spawn(async move {
                let (_, request) = read_rpc_request(&mut server).await;
                write_rpc_success(
                    &mut server,
                    request["id"].as_u64().unwrap() + 1,
                    json!({}),
                    None,
                )
                .await;
            });
            let error = runtime
                .call("project.list", empty_params())
                .await
                .unwrap_err();
            assert!(error.message.contains("id did not match"));
            server_task.await.unwrap();
        }
        {
            let (runtime, mut server) = actor_pair();
            let server_task = tokio::spawn(async move {
                let (_, request) = read_rpc_request(&mut server).await;
                write_rpc_success(
                    &mut server,
                    request["id"].as_u64().unwrap(),
                    json!({}),
                    Some(0),
                )
                .await;
            });
            let error = runtime
                .call("project.list", empty_params())
                .await
                .unwrap_err();
            assert!(error.message.contains("unsolicited binary"));
            server_task.await.unwrap();
        }
        {
            let (runtime, mut server) = actor_pair();
            let server_task = tokio::spawn(async move {
                let (_, request) = read_rpc_request(&mut server).await;
                write_rpc_success(
                    &mut server,
                    request["id"].as_u64().unwrap(),
                    json!({"eof": true, "nextOffset": 0}),
                    None,
                )
                .await;
            });
            let error = runtime
                .download("job.logs", empty_params(), 0)
                .await
                .err()
                .unwrap();
            assert!(error.message.contains("omitted binaryLength"));
            server_task.await.unwrap();
        }
        {
            let (runtime, mut server) = actor_pair();
            let server_task = tokio::spawn(async move {
                let (_, request) = read_rpc_request(&mut server).await;
                write_rpc_success(
                    &mut server,
                    request["id"].as_u64().unwrap(),
                    json!({"eof": false, "nextOffset": MAX_BINARY_FRAME_BYTES + 1}),
                    Some(MAX_BINARY_FRAME_BYTES + 1),
                )
                .await;
            });
            let error = runtime
                .download("job.logs", empty_params(), 0)
                .await
                .err()
                .unwrap();
            assert!(error.message.contains("64 KiB"));
            server_task.await.unwrap();
        }
        {
            let (runtime, mut server) = actor_pair();
            let server_task = tokio::spawn(async move {
                let (_, request) = read_rpc_request(&mut server).await;
                write_rpc_success(
                    &mut server,
                    request["id"].as_u64().unwrap(),
                    json!({"eof": false, "nextOffset": 4}),
                    Some(3),
                )
                .await;
            });
            let error = runtime
                .download("job.logs", empty_params(), 0)
                .await
                .err()
                .unwrap();
            assert!(error.message.contains("nextOffset"));
            server_task.await.unwrap();
        }
        {
            let (runtime, mut server) = actor_pair();
            let server_task = tokio::spawn(async move {
                let (_, request) = read_rpc_request(&mut server).await;
                write_rpc_success(
                    &mut server,
                    request["id"].as_u64().unwrap(),
                    json!({"eof": false, "nextOffset": 3}),
                    Some(3),
                )
                .await;
                write_raw_frame(&mut server, b"ab").await;
            });
            let error = runtime
                .download("job.logs", empty_params(), 0)
                .await
                .err()
                .unwrap();
            assert!(error.message.contains("length mismatch"));
            server_task.await.unwrap();
        }
        {
            let (runtime, mut server) = actor_pair();
            let server_task = tokio::spawn(async move {
                let (_, request) = read_rpc_request(&mut server).await;
                write_rpc_success(
                    &mut server,
                    request["id"].as_u64().unwrap(),
                    json!({"eof": false, "nextOffset": 1}),
                    Some(1),
                )
                .await;
            });
            let error = runtime
                .download("job.logs", empty_params(), 0)
                .await
                .err()
                .unwrap();
            assert_eq!(error.code, ErrorCode::EnvironmentMissing);
            assert!(error.message.contains("binary read failed"));
            server_task.await.unwrap();
        }
        {
            let (runtime, mut server) = actor_pair();
            let server_task = tokio::spawn(async move {
                let (_, request) = read_rpc_request(&mut server).await;
                let response = serde_json::to_vec(&json!({
                    "id": request["id"],
                    "ok": true,
                    "result": null,
                    "error": null,
                }))
                .unwrap();
                write_rpc_frame(&mut server, &response).await.unwrap();
            });
            let error = runtime
                .call("project.list", empty_params())
                .await
                .unwrap_err();
            assert!(error.message.contains("invalid envelope"));
            server_task.await.unwrap();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oversized_upload_is_rejected_without_touching_the_wire() {
        let (runtime, mut server) = actor_pair();
        let server_task = tokio::spawn(async move {
            let (_, request) = read_rpc_request(&mut server).await;
            assert_eq!(request["id"], 1);
            assert_eq!(request["method"], "project.list");
            assert!(request.get("binaryLength").is_none());
            write_rpc_success(&mut server, 1, json!([]), None).await;
        });

        let error = runtime
            .upload(
                "job.attachWrite",
                empty_params(),
                Bytes::from(vec![0; MAX_BINARY_FRAME_BYTES + 1]),
            )
            .await
            .unwrap_err();
        assert!(error.message.contains("64 KiB"));
        assert_eq!(
            runtime.call("project.list", empty_params()).await.unwrap(),
            json!([])
        );
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn followed_stream_closes_at_terminal_state_without_empty_chunks() {
        let runtime = Arc::new(TestRuntime::default());
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let mut stream = poll_job_stream(
            runtime_trait,
            test_authority(),
            JobId::new(7).unwrap(),
            JobStream::Stdout,
            0,
            true,
        );

        assert!(stream.next().await.is_none());
        assert_eq!(runtime.log_calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.status_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn followed_stream_resumes_after_nonterminal_eof_then_closes_at_terminal_eof() {
        let runtime = Arc::new(TestRuntime::default());
        runtime.mode.store(4, Ordering::SeqCst);
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let mut stream = poll_job_stream(
            runtime_trait,
            test_authority(),
            JobId::new(7).unwrap(),
            JobStream::Stdout,
            0,
            true,
        );

        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"after-eof")
        );
        assert!(stream.next().await.is_none());
        assert_eq!(runtime.log_calls.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.status_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn non_follow_stream_reads_every_page_byte_for_byte_through_eof() {
        let runtime = Arc::new(TestRuntime::default());
        runtime.mode.store(2, Ordering::SeqCst);
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let mut stream = poll_job_stream(
            runtime_trait,
            test_authority(),
            JobId::new(7).unwrap(),
            JobStream::Stdout,
            0,
            false,
        );
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(bytes, b"abcdefghi");
        assert_eq!(runtime.log_calls.load(Ordering::SeqCst), 3);
        assert_eq!(runtime.status_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn slow_consumer_bounds_completed_log_polls_to_channel_capacity() {
        let runtime = Arc::new(TestRuntime::default());
        runtime.mode.store(3, Ordering::SeqCst);
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let stream = poll_job_stream(
            runtime_trait,
            test_authority(),
            JobId::new(7).unwrap(),
            JobStream::Stdout,
            0,
            false,
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while runtime.log_calls.load(Ordering::SeqCst) < 9 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer did not fill the bounded channel");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(runtime.log_calls.load(Ordering::SeqCst), 9);
        drop(stream);
    }
    #[tokio::test]
    async fn dropping_stream_cancels_an_in_flight_poll() {
        let runtime = Arc::new(TestRuntime::default());
        runtime.mode.store(1, Ordering::SeqCst);
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let stream = poll_job_stream(
            runtime_trait,
            test_authority(),
            JobId::new(7).unwrap(),
            JobStream::Stdout,
            0,
            true,
        );
        while runtime.active_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while runtime.active_calls.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("poll task did not stop after receiver drop");
    }

    #[tokio::test]
    async fn attachment_parts_support_concurrent_stdin_stdout_and_stderr() {
        let runtime = Arc::new(TestRuntime::default());
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let id = JobId::new(7).unwrap();
        let (stdout_sender, stdout_receiver) = mpsc::channel(1);
        let (stderr_sender, stderr_receiver) = mpsc::channel(1);
        stdout_sender
            .send(Ok(Bytes::from_static(b"out")))
            .await
            .unwrap();
        stderr_sender
            .send(Ok(Bytes::from_static(b"err")))
            .await
            .unwrap();
        drop((stdout_sender, stderr_sender));
        let authority = test_authority();
        let attachment = JobAttachment {
            authority: Arc::clone(&authority),
            id,
            stdin: JobStdin {
                authority,
                id,
                runtime: Arc::clone(&runtime_trait),
            },
            stdout: RawByteStream {
                receiver: stdout_receiver,
            },
            stderr: RawByteStream {
                receiver: stderr_receiver,
            },
            runtime: runtime_trait,
        };
        let (stdin, mut stdout, mut stderr) = attachment.into_parts();

        let (write, out, err) = tokio::join!(
            stdin.write(Bytes::from_static(b"input")),
            stdout.next(),
            stderr.next()
        );
        write.unwrap();
        assert_eq!(out.unwrap().unwrap(), Bytes::from_static(b"out"));
        assert_eq!(err.unwrap().unwrap(), Bytes::from_static(b"err"));
        assert_eq!(runtime.stdin_writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn kill_awaits_and_returns_the_controller_result() {
        let runtime: Arc<dyn ControllerRuntime> = Arc::new(TestRuntime::default());
        let handle = JobHandle {
            authority: test_authority(),
            id: JobId::new(7).unwrap(),
            runtime,
        };
        let error = handle.kill().await.unwrap_err();
        assert!(error.message.contains("rejected kill"));
    }
}
