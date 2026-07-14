use super::dto::{
    AdoptOptions, AttachOptions, CheckpointOptions, CheckpointQuota, CheckpointResult,
    CreateOptions, DoctorReport, EmptyResult, ExecRequest, GcOptions, GcReport, GitOid, GrantDelta,
    GrantSet, JobId, JobInfo, JobState, LandOptions, LandReport, MirrorInfo, PushOptions,
    PushReport, RebaseOptions, RemoveOptions, RevisionResult, RunSandboxMode, StdinSource,
    WorkspaceIncarnation, WorkspaceInfo, validate_command_argv,
};
use super::server::MAX_BINARY_FRAME_BYTES;
#[cfg(unix)]
use super::server::{
    HANDSHAKE_VERSION, MAX_HANDSHAKE_BYTES, MAX_JSON_FRAME_BYTES as MAX_RPC_BYTES, codec,
};
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::WorkspaceName;
use crate::repository::{ProjectPaths, RepoId, RepositoryBinding};
use async_trait::async_trait;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt;
#[cfg(unix)]
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
}

#[async_trait]
pub(crate) trait ControllerRuntime: Send + Sync {
    async fn call(&self, method: &'static str, params: Value) -> Result<Value>;
    async fn upload(&self, method: &'static str, params: Value, bytes: Bytes) -> Result<Value>;
    async fn download(
        &self,
        method: &'static str,
        params: Value,
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
        follow: bool,
    ) -> Result<RawByteStream>;
    async fn attach(&self, authority: Arc<WorkspaceAuthority>, id: JobId) -> Result<JobAttachment>;
    async fn kill(&self, authority: &WorkspaceAuthority, id: JobId) -> Result<()>;
}

#[cfg(unix)]
enum ActorMessage {
    Json {
        method: &'static str,
        params: Value,
        reply: oneshot::Sender<Result<ActorResponse>>,
    },
    Upload {
        method: &'static str,
        params: Value,
        bytes: Bytes,
        reply: oneshot::Sender<Result<ActorResponse>>,
    },
    Download {
        method: &'static str,
        params: Value,
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
enum ActorFailure {
    Recoverable(CowshedError),
    Fatal(CowshedError),
}

#[cfg(unix)]
#[derive(Clone)]
struct ActorRuntime {
    sender: mpsc::Sender<ActorMessage>,
}

#[cfg(unix)]
#[async_trait]
impl ControllerRuntime for ActorRuntime {
    async fn call(&self, method: &'static str, params: Value) -> Result<Value> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ActorMessage::Json {
                method,
                params,
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

    async fn upload(&self, method: &'static str, params: Value, bytes: Bytes) -> Result<Value> {
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
        params: Value,
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
        validate_command_argv(&request.argv).map_err(|error| {
            CowshedError::usage(error.to_string(), "provide a valid bounded command argv")
        })?;
        let ExecRequest {
            argv,
            cwd,
            mode,
            env,
            trace,
            stdin,
            stdout_copy,
            stderr_copy,
        } = request;
        let mode = match mode {
            RunSandboxMode::ReadWrite => "readWrite",
            RunSandboxMode::ReadOnly => "readOnly",
        };
        let (stdin_metadata, inline, mut stream) = match stdin {
            StdinSource::Empty => (json!({ "kind": "empty" }), None, None),
            StdinSource::Inline(bytes) => (json!({ "kind": "inline" }), Some(bytes), None),
            StdinSource::WorkspaceFile(path) => (
                json!({ "kind": "workspaceFile", "workspacePath": path }),
                None,
                None,
            ),
            StdinSource::Stream(stream) => (json!({ "kind": "stream" }), None, Some(stream)),
        };
        let params = json!({
            "repoId": authority.repo_id,
            "workspace": authority.workspace,
            "workspaceIncarnation": authority.workspace_incarnation,
            "session": session,
            "argv": argv,
            "cwd": cwd,
            "mode": mode,
            "env": env,
            "trace": trace,
            "stdin": stdin_metadata,
            "stdoutCopy": stdout_copy,
            "stderrCopy": stderr_copy,
        });
        let result = match inline {
            Some(bytes) => self.upload("worker.exec", params, bytes).await?,
            None => self.call("worker.exec", params).await?,
        };
        let job_id: JobId = serde_json::from_value(result).map_err(|error| {
            CowshedError::new(
                ErrorCode::Internal,
                format!("controller returned an invalid worker.exec response: {error}"),
                "cowshed doctor --json",
            )
        })?;
        if let Some(reader) = stream.as_mut() {
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
                self.upload(
                    "worker.stdinChunk",
                    json!({
                        "repoId": authority.repo_id,
                        "workspace": authority.workspace,
                        "workspaceIncarnation": authority.workspace_incarnation,
                        "jobId": job_id,
                    }),
                    Bytes::copy_from_slice(&buffer[..count]),
                )
                .await
                .and_then(decode_empty)?;
            }
            self.call(
                "worker.stdinClose",
                json!({
                    "repoId": authority.repo_id,
                    "workspace": authority.workspace,
                    "workspaceIncarnation": authority.workspace_incarnation,
                    "jobId": job_id,
                }),
            )
            .await
            .and_then(decode_empty)?;
        }
        Ok(job_id)
    }

    async fn logs(
        &self,
        authority: Arc<WorkspaceAuthority>,
        id: JobId,
        stream: JobStream,
        follow: bool,
    ) -> Result<RawByteStream> {
        Ok(poll_job_stream(
            Arc::new(self.clone()),
            authority,
            id,
            stream,
            follow,
        ))
    }

    async fn attach(&self, authority: Arc<WorkspaceAuthority>, id: JobId) -> Result<JobAttachment> {
        let stdout = self
            .logs(Arc::clone(&authority), id, JobStream::Stdout, true)
            .await?;
        let stderr = self
            .logs(Arc::clone(&authority), id, JobStream::Stderr, true)
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
        self.call(
            "job.kill",
            json!({
                "repoId": authority.repo_id,
                "workspace": authority.workspace,
                "workspaceIncarnation": authority.workspace_incarnation,
                "jobId": id,
            }),
        )
        .await
        .and_then(decode_empty)
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

fn poll_job_stream(
    runtime: Arc<dyn ControllerRuntime>,
    authority: Arc<WorkspaceAuthority>,
    id: JobId,
    stream: JobStream,
    follow: bool,
) -> RawByteStream {
    let (sender, receiver) = mpsc::channel(8);
    tokio::spawn(async move {
        let mut offset = 0_u64;
        loop {
            let chunk = tokio::select! {
                _ = sender.closed() => break,
                value = runtime.download(
                    "job.logs",
                    json!({
                        "repoId": authority.repo_id,
                        "workspace": authority.workspace,
                        "workspaceIncarnation": authority.workspace_incarnation,
                        "jobId": id,
                        "stream": stream,
                        "follow": follow,
                        "offset": offset,
                    }),
                    offset,
                ) => value,
            };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    tokio::select! {
                        _ = sender.closed() => {}
                        _ = sender.send(Err(error)) => {}
                    }
                    break;
                }
            };
            let had_bytes = !chunk.bytes.is_empty();
            let eof = chunk.eof;
            if had_bytes {
                offset = match offset.checked_add(chunk.bytes.len() as u64) {
                    Some(next_offset) => next_offset,
                    None => {
                        let error = CowshedError::internal("job.logs response offset overflowed");
                        tokio::select! {
                            _ = sender.closed() => {}
                            _ = sender.send(Err(error)) => {}
                        }
                        break;
                    }
                };
                let bytes = Bytes::from(chunk.bytes);
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
                let status: Result<JobInfo> = tokio::select! {
                    _ = sender.closed() => break,
                    value = runtime.call(
                        "job.status",
                        json!({
                            "repoId": authority.repo_id,
                            "workspace": authority.workspace,
                            "workspaceIncarnation": authority.workspace_incarnation,
                            "jobId": id,
                        }),
                    ) => value.and_then(|value| {
                        serde_json::from_value(value).map_err(|error| {
                            CowshedError::internal(format!(
                                "controller returned an invalid job.status response: {error}"
                            ))
                        })
                    }),
                };
                match status {
                    Ok(info) if is_terminal_job_state(info.state) => break,
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

fn is_terminal_job_state(state: JobState) -> bool {
    !matches!(state, JobState::Queued | JobState::Running)
}

async fn call_typed<T: DeserializeOwned>(
    runtime: &Arc<dyn ControllerRuntime>,
    method: &'static str,
    params: Value,
) -> Result<T> {
    let value = runtime.call(method, params).await?;
    serde_json::from_value(value).map_err(|error| {
        CowshedError::new(
            ErrorCode::Internal,
            format!("controller returned an invalid {method} response: {error}"),
            "cowshed doctor --json",
        )
    })
}

fn decode_empty(value: Value) -> Result<()> {
    serde_json::from_value::<EmptyResult>(value)
        .map(|_| ())
        .map_err(|error| {
            CowshedError::internal(format!(
                "controller returned an invalid empty result: {error}"
            ))
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStream {
    Stdout,
    Stderr,
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
        self.runtime
            .upload(
                "job.attachWrite",
                json!({
                    "repoId": self.authority.repo_id,
                    "workspace": self.authority.workspace,
                    "workspaceIncarnation": self.authority.workspace_incarnation,
                    "jobId": self.id,
                }),
                bytes,
            )
            .await
            .and_then(decode_empty)
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
        let value = self
            .runtime
            .call(
                "job.detach",
                json!({
                    "repoId": self.authority.repo_id,
                    "workspace": self.authority.workspace,
                    "workspaceIncarnation": self.authority.workspace_incarnation,
                    "jobId": self.id,
                }),
            )
            .await?;
        decode_empty(value)
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

fn encode_value<T: Serialize>(kind: &'static str, value: &T) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| {
        CowshedError::new(
            ErrorCode::Usage,
            format!("{kind} is not representable as JSON: {error}"),
            "use UTF-8 paths and validated cowshed option values",
        )
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProjectWire {
    repo_id: RepoId,
    binding: RepositoryBinding,
    git_root: PathBuf,
    store_root: PathBuf,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkspaceWire {
    info: WorkspaceInfo,
    grants: GrantSet,
}

/// Explicit cowshed client. Its sealed runtime delegates to a single-owner controller actor.
pub struct Cowshed {
    runtime: Arc<dyn ControllerRuntime>,
}

impl Cowshed {
    pub async fn open(&self, path: impl AsRef<Path>) -> Result<Project> {
        let path = path.as_ref().to_str().ok_or_else(|| {
            CowshedError::usage(
                "project path is not valid UTF-8",
                "use a UTF-8 project path",
            )
        })?;
        let wire: ProjectWire =
            call_typed(&self.runtime, "project.open", json!({ "path": path })).await?;
        let paths = ProjectPaths::new(&wire.store_root, &wire.repo_id).map_err(|error| {
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
    binding: RepositoryBinding,
    git_root: PathBuf,
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
        let name = WorkspaceName::new(name).map_err(|error| {
            CowshedError::usage(error.to_string(), "use a valid cowshed workspace name")
        })?;
        let wire: WorkspaceWire = call_typed(
            &self.runtime,
            "project.workspace",
            json!({ "repoId": self.repo_id, "workspace": name }),
        )
        .await?;
        Ok(WorkspaceRef::from_wire(wire, Arc::clone(&self.runtime)))
    }

    pub async fn list(&self) -> Result<Vec<WorkspaceRef>> {
        let wires: Vec<WorkspaceWire> = call_typed(
            &self.runtime,
            "project.list",
            json!({ "repoId": self.repo_id }),
        )
        .await?;
        Ok(wires
            .into_iter()
            .map(|wire| WorkspaceRef::from_wire(wire, Arc::clone(&self.runtime)))
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
    fn from_wire(wire: WorkspaceWire, runtime: Arc<dyn ControllerRuntime>) -> Self {
        Self {
            info: wire.info,
            grants: wire.grants,
            runtime,
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

    /// Refreshes workspace information from the controller without changing this snapshot.
    pub async fn refresh_info(&self) -> Result<WorkspaceInfo> {
        call_typed(
            &self.runtime,
            "workspace.info",
            json!({ "repoId": self.info.repo_id, "workspace": self.info.workspace }),
        )
        .await
    }

    pub async fn ensure(&self) -> Result<super::dto::EnsureReport> {
        call_typed(
            &self.runtime,
            "workspace.ensure",
            json!({ "repoId": self.info.repo_id, "workspace": self.info.workspace }),
        )
        .await
    }

    pub async fn attach(&self, options: AttachOptions) -> Result<()> {
        let _: EmptyResult = call_typed(
            &self.runtime,
            "workspace.attach",
            json!({ "repoId": self.info.repo_id, "workspace": self.info.workspace, "options": options }),
        )
        .await?;
        Ok(())
    }

    /// Refreshes grants from the controller without changing this snapshot.
    pub async fn refresh_grants(&self) -> Result<GrantSet> {
        call_typed(
            &self.runtime,
            "workspace.grants",
            json!({ "repoId": self.info.repo_id, "workspace": self.info.workspace }),
        )
        .await
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
    let fd = descriptor.as_raw_fd();
    let mut socket_type: libc::c_int = 0;
    let mut socket_type_len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut socket_type as *mut libc::c_int).cast(),
            &mut socket_type_len,
        )
    };
    if result != 0 || socket_type != libc::SOCK_STREAM {
        return Err(handshake_error(
            "coordinator descriptor is not a stream socket",
        ));
    }

    #[cfg(target_os = "macos")]
    {
        let mut peer_uid: libc::uid_t = 0;
        let mut peer_gid: libc::gid_t = 0;
        let result = unsafe { libc::getpeereid(fd, &mut peer_uid, &mut peer_gid) };
        let current_uid = unsafe { libc::geteuid() };
        if result != 0 || peer_uid != current_uid {
            return Err(handshake_error(
                "coordinator descriptor peer does not match the current uid",
            ));
        }
    }

    #[cfg(target_os = "linux")]
    {
        let mut credentials = libc::ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut credentials_len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut credentials as *mut libc::ucred).cast(),
                &mut credentials_len,
            )
        };
        let current_uid = unsafe { libc::geteuid() };
        if result != 0 || credentials.uid != current_uid {
            return Err(handshake_error(
                "coordinator descriptor peer does not match the current uid",
            ));
        }
    }

    Ok(())
}

#[cfg(unix)]
fn fresh_nonce() -> String {
    let first = uuid::Uuid::new_v4().simple().to_string();
    let second = uuid::Uuid::new_v4().simple().to_string();
    format!("{first}{second}")
}

#[cfg(unix)]
async fn write_frame(stream: &mut tokio::net::UnixStream, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_HANDSHAKE_BYTES {
        return Err(handshake_error(
            "coordinator handshake request is too large",
        ));
    }
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(|error| handshake_error(format!("coordinator handshake write failed: {error}")))?;
    stream
        .write_all(bytes)
        .await
        .map_err(|error| handshake_error(format!("coordinator handshake write failed: {error}")))
}

#[cfg(unix)]
async fn read_frame(stream: &mut tokio::net::UnixStream) -> Result<Vec<u8>> {
    let length =
        stream.read_u32().await.map_err(|error| {
            handshake_error(format!("coordinator handshake read failed: {error}"))
        })? as usize;
    if length == 0 || length > MAX_HANDSHAKE_BYTES {
        return Err(handshake_error(
            "coordinator handshake response has invalid length",
        ));
    }
    let mut bytes = vec![0_u8; length];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|error| handshake_error(format!("coordinator handshake read failed: {error}")))?;
    Ok(bytes)
}

#[cfg(unix)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RpcBinaryResult {
    eof: bool,
    next_offset: u64,
}

#[cfg(unix)]
async fn write_rpc_frame(stream: &mut tokio::net::UnixStream, bytes: &[u8]) -> Result<()> {
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
async fn read_rpc_frame(stream: &mut tokio::net::UnixStream) -> Result<Vec<u8>> {
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
async fn write_binary_frame(stream: &mut tokio::net::UnixStream, bytes: &[u8]) -> Result<()> {
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
    stream: &mut tokio::net::UnixStream,
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

#[cfg(unix)]
fn spawn_controller_actor(mut stream: tokio::net::UnixStream) -> Arc<dyn ControllerRuntime> {
    let (sender, mut receiver) = mpsc::channel::<ActorMessage>(32);
    tokio::spawn(async move {
        let mut next_id = 1_u64;
        while let Some(message) = receiver.recv().await {
            let id = next_id;
            next_id = next_id.saturating_add(1);
            let (method, params, lane, reply) = match message {
                ActorMessage::Json {
                    method,
                    params,
                    reply,
                } => (method, params, ActorLane::Json, reply),
                ActorMessage::Upload {
                    method,
                    params,
                    bytes,
                    reply,
                } => (method, params, ActorLane::Upload(bytes), reply),
                ActorMessage::Download {
                    method,
                    params,
                    expected_offset,
                    reply,
                } => (method, params, ActorLane::Download(expected_offset), reply),
            };
            let exchange = async {
                let binary_length = match &lane {
                    ActorLane::Upload(bytes) => Some(u32::try_from(bytes.len()).map_err(|_| {
                        ActorFailure::Fatal(CowshedError::internal(
                            "controller RPC binary request exceeds the 64 KiB frame limit",
                        ))
                    })?),
                    ActorLane::Json | ActorLane::Download(_) => None,
                };
                let request = codec::encode_rpc_request(id, method, &params, binary_length)
                    .map_err(|error| {
                        if error.is_too_large() {
                            ActorFailure::Fatal(CowshedError::internal(
                                "controller RPC request is too large",
                            ))
                        } else {
                            ActorFailure::Fatal(CowshedError::internal(format!(
                                "controller RPC request encoding failed: {error}"
                            )))
                        }
                    })?;
                write_rpc_frame(&mut stream, &request)
                    .await
                    .map_err(ActorFailure::Fatal)?;
                if let ActorLane::Upload(bytes) = &lane {
                    write_binary_frame(&mut stream, bytes)
                        .await
                        .map_err(ActorFailure::Fatal)?;
                }
                let response = read_rpc_frame(&mut stream)
                    .await
                    .map_err(ActorFailure::Fatal)?;
                let response = codec::decode_rpc_response(&response).map_err(|error| {
                    ActorFailure::Fatal(CowshedError::internal(format!(
                        "controller RPC response decoding failed: {error}"
                    )))
                })?;
                let (response_id, response_ok, response_result, response_error, binary_length) =
                    response.into_parts();
                if response_id != id {
                    return Err(ActorFailure::Fatal(CowshedError::internal(
                        "controller RPC response id did not match request",
                    )));
                }
                let result = match (response_ok, response_result, response_error) {
                    (true, Some(result), None) => result,
                    (false, None, Some(error)) => {
                        if binary_length.is_some() {
                            return Err(ActorFailure::Fatal(CowshedError::internal(
                                "controller RPC error response declared unsolicited binary data",
                            )));
                        }
                        return Err(ActorFailure::Recoverable(error));
                    }
                    _ => {
                        return Err(ActorFailure::Fatal(CowshedError::internal(
                            "controller RPC response has an invalid envelope",
                        )));
                    }
                };
                match lane {
                    ActorLane::Json | ActorLane::Upload(_) => {
                        if binary_length.is_some() {
                            return Err(ActorFailure::Fatal(CowshedError::internal(
                                "controller RPC response declared unsolicited binary data",
                            )));
                        }
                        Ok(ActorResponse::Json(result))
                    }
                    ActorLane::Download(expected_offset) => {
                        let binary_length = binary_length.ok_or_else(|| {
                            ActorFailure::Fatal(CowshedError::internal(
                                "controller RPC download response omitted binaryLength",
                            ))
                        })?;
                        let binary_length = usize::try_from(binary_length).map_err(|_| {
                            ActorFailure::Fatal(CowshedError::internal(
                                "controller RPC binary response length does not fit this platform",
                            ))
                        })?;
                        if binary_length > MAX_BINARY_FRAME_BYTES {
                            return Err(ActorFailure::Fatal(CowshedError::internal(
                                "controller RPC binary response exceeds the 64 KiB frame limit",
                            )));
                        }
                        let metadata: RpcBinaryResult =
                            serde_json::from_value(result).map_err(|error| {
                                ActorFailure::Fatal(CowshedError::internal(format!(
                                    "controller RPC download metadata is invalid: {error}"
                                )))
                            })?;
                        let binary_length_u64 = u64::try_from(binary_length).map_err(|_| {
                            ActorFailure::Fatal(CowshedError::internal(
                                "controller RPC download offset overflowed",
                            ))
                        })?;
                        let expected_next = expected_offset
                            .checked_add(binary_length_u64)
                            .ok_or_else(|| {
                                ActorFailure::Fatal(CowshedError::internal(
                                    "controller RPC download offset overflowed",
                                ))
                            })?;
                        if metadata.next_offset != expected_next {
                            return Err(ActorFailure::Fatal(CowshedError::internal(
                                "controller RPC download nextOffset was not exact",
                            )));
                        }
                        let bytes = read_binary_frame(&mut stream, binary_length)
                            .await
                            .map_err(ActorFailure::Fatal)?;
                        Ok(ActorResponse::Download(BinaryDownload {
                            bytes,
                            eof: metadata.eof,
                        }))
                    }
                }
            }
            .await;
            let (result, stop) = match exchange {
                Ok(response) => (Ok(response), false),
                Err(ActorFailure::Recoverable(error)) => (Err(error), false),
                Err(ActorFailure::Fatal(error)) => (Err(error), true),
            };
            let _ = reply.send(result);
            if stop {
                break;
            }
        }
    });
    Arc::new(ActorRuntime { sender })
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
    let runtime = spawn_controller_actor(stream);
    let token = CoordinatorToken {
        repo_id,
        channel: AuthenticatedControllerChannel {
            runtime: Arc::clone(&runtime),
        },
    };
    Ok((Cowshed { runtime }, token))
}

/// Sole project mutation and cross-workspace authority.
pub struct Coordinator {
    project: Project,
    runtime: Arc<dyn ControllerRuntime>,
    _channel: AuthenticatedControllerChannel,
}

impl Coordinator {
    pub fn project(&self) -> &Project {
        &self.project
    }

    async fn workspace_result(&self, method: &'static str, params: Value) -> Result<WorkspaceRef> {
        let wire: WorkspaceWire = call_typed(&self.runtime, method, params).await?;
        Ok(WorkspaceRef::from_wire(wire, Arc::clone(&self.runtime)))
    }

    pub async fn adopt(&self, options: AdoptOptions) -> Result<WorkspaceRef> {
        let options = encode_value("adopt options", &options)?;
        self.workspace_result(
            "coordinator.adopt",
            json!({ "repoId": self.project.repo_id, "options": options }),
        )
        .await
    }

    pub async fn create(&self, name: &str, options: CreateOptions) -> Result<WorkspaceRef> {
        let name = WorkspaceName::session(name).map_err(|error| {
            CowshedError::usage(error.to_string(), "use a valid non-main workspace name")
        })?;
        self.workspace_result(
            "coordinator.create",
            json!({ "repoId": self.project.repo_id, "workspace": name, "options": options }),
        )
        .await
    }

    pub async fn fork(&self, source: &str, destination: &str) -> Result<WorkspaceRef> {
        let source = WorkspaceName::new(source).map_err(|error| {
            CowshedError::usage(error.to_string(), "use a valid source workspace name")
        })?;
        let destination = WorkspaceName::session(destination).map_err(|error| {
            CowshedError::usage(error.to_string(), "use a valid non-main destination name")
        })?;
        self.workspace_result(
            "coordinator.fork",
            json!({ "repoId": self.project.repo_id, "source": source, "destination": destination }),
        )
        .await
    }

    pub async fn grant(&self, workspace: &str, delta: GrantDelta) -> Result<GrantSet> {
        self.grant_call("coordinator.grant", workspace, delta).await
    }

    pub async fn revoke(&self, workspace: &str, delta: GrantDelta) -> Result<GrantSet> {
        self.grant_call("coordinator.revoke", workspace, delta)
            .await
    }

    async fn grant_call(
        &self,
        method: &'static str,
        workspace: &str,
        delta: GrantDelta,
    ) -> Result<GrantSet> {
        let delta = encode_value("grant delta", &delta)?;
        call_typed(
            &self.runtime,
            method,
            json!({ "repoId": self.project.repo_id, "workspace": workspace, "delta": delta }),
        )
        .await
    }

    pub async fn rebase(&self, workspace: &str, options: RebaseOptions) -> Result<GitOid> {
        let result: RevisionResult = call_typed(
            &self.runtime,
            "coordinator.rebase",
            json!({ "repoId": self.project.repo_id, "workspace": workspace, "options": options }),
        )
        .await?;
        Ok(result.oid)
    }

    pub async fn land(&self, workspace: &str, options: LandOptions) -> Result<LandReport> {
        call_typed(
            &self.runtime,
            "coordinator.land",
            json!({ "repoId": self.project.repo_id, "workspace": workspace, "options": options }),
        )
        .await
    }

    pub async fn restore(&self, workspace: &str, label: &str) -> Result<()> {
        self.empty_call(
            "coordinator.restore",
            json!({ "repoId": self.project.repo_id, "workspace": workspace, "label": label }),
        )
        .await
    }

    pub async fn detach(&self, workspace: &str) -> Result<EmptyResult> {
        call_typed(
            &self.runtime,
            "coordinator.detach",
            json!({ "repoId": self.project.repo_id, "workspace": workspace }),
        )
        .await
    }

    pub async fn assign_slot(&self, workspace: &str, slot: u32) -> Result<()> {
        self.empty_call(
            "coordinator.assignSlot",
            json!({ "repoId": self.project.repo_id, "workspace": workspace, "slot": slot }),
        )
        .await
    }

    pub async fn destroy(&self, workspace: &str, options: RemoveOptions) -> Result<()> {
        self.empty_call(
            "coordinator.destroy",
            json!({ "repoId": self.project.repo_id, "workspace": workspace, "options": options }),
        )
        .await
    }

    pub async fn gc(&self, options: GcOptions) -> Result<GcReport> {
        call_typed(
            &self.runtime,
            "coordinator.gc",
            json!({ "repoId": self.project.repo_id, "options": options }),
        )
        .await
    }

    pub async fn repo_mirror(&self, workspace: &str, url: &Url) -> Result<MirrorInfo> {
        call_typed(
            &self.runtime,
            "coordinator.repoMirror",
            json!({ "repoId": self.project.repo_id, "workspace": workspace, "url": url.as_str() }),
        )
        .await
    }

    pub async fn set_checkpoint_quota(
        &self,
        workspace: &str,
        quota: CheckpointQuota,
    ) -> Result<()> {
        self.empty_call(
            "coordinator.setCheckpointQuota",
            json!({ "repoId": self.project.repo_id, "workspace": workspace, "quota": quota }),
        )
        .await
    }

    pub async fn doctor(&self) -> Result<DoctorReport> {
        call_typed(
            &self.runtime,
            "coordinator.doctor",
            json!({ "repoId": self.project.repo_id }),
        )
        .await
    }

    pub async fn worker(&self, workspace: &str) -> Result<WorkspaceHandle> {
        let wire: WorkspaceWire = call_typed(
            &self.runtime,
            "coordinator.worker",
            json!({ "repoId": self.project.repo_id, "workspace": workspace }),
        )
        .await?;
        let workspace = WorkspaceRef::from_wire(wire, Arc::clone(&self.runtime));
        Ok(WorkspaceHandle::new(workspace, Arc::clone(&self.runtime)))
    }

    async fn empty_call(&self, method: &'static str, params: Value) -> Result<()> {
        let _: EmptyResult = call_typed(&self.runtime, method, params).await?;
        Ok(())
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

    pub async fn exec(&self, request: ExecRequest) -> Result<JobHandle> {
        exec_job(&self.runtime, Arc::clone(&self.authority), None, request).await
    }

    pub async fn shell(&self, session: Option<&str>) -> Result<Session> {
        let _: EmptyResult = call_typed(
            &self.runtime,
            "worker.shell",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
                "session": session,
            }),
        )
        .await?;
        Ok(Session {
            authority: Arc::clone(&self.authority),
            name: session.map(str::to_owned),
            runtime: Arc::clone(&self.runtime),
        })
    }

    pub async fn list_jobs(&self) -> Result<Vec<JobInfo>> {
        call_typed(
            &self.runtime,
            "worker.listJobs",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
            }),
        )
        .await
    }

    pub async fn job(&self, id: JobId) -> Result<JobHandle> {
        let _: JobInfo = call_typed(
            &self.runtime,
            "worker.job",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
                "jobId": id,
            }),
        )
        .await?;
        Ok(JobHandle {
            authority: Arc::clone(&self.authority),
            id,
            runtime: Arc::clone(&self.runtime),
        })
    }

    pub async fn checkpoint(&self, options: CheckpointOptions) -> Result<String> {
        let result: CheckpointResult = call_typed(
            &self.runtime,
            "worker.checkpoint",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
                "options": options,
            }),
        )
        .await?;
        Ok(result.label)
    }

    pub async fn push(&self, options: PushOptions) -> Result<PushReport> {
        call_typed(
            &self.runtime,
            "worker.push",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
                "options": options,
            }),
        )
        .await
    }

    pub async fn grants(&self) -> Result<GrantSet> {
        call_typed(
            &self.runtime,
            "workspace.grants",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
            }),
        )
        .await
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
        call_typed(
            &self.runtime,
            "job.status",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
                "jobId": self.id,
            }),
        )
        .await
    }

    pub async fn logs(&self, stream: JobStream, follow: bool) -> Result<RawByteStream> {
        self.runtime
            .logs(Arc::clone(&self.authority), self.id, stream, follow)
            .await
    }

    pub async fn attach(&self) -> Result<JobAttachment> {
        self.runtime
            .attach(Arc::clone(&self.authority), self.id)
            .await
    }

    pub async fn detach(&self) -> Result<()> {
        self.empty_call("job.detach").await
    }

    pub async fn wait(&self) -> Result<JobInfo> {
        call_typed(
            &self.runtime,
            "job.wait",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
                "jobId": self.id,
            }),
        )
        .await
    }

    pub async fn kill(&self) -> Result<()> {
        self.runtime.kill(&self.authority, self.id).await
    }

    async fn empty_call(&self, method: &'static str) -> Result<()> {
        let _: EmptyResult = call_typed(
            &self.runtime,
            method,
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
                "jobId": self.id,
            }),
        )
        .await?;
        Ok(())
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

    pub async fn background(&self, request: ExecRequest) -> Result<JobHandle> {
        self.run(request).await
    }

    pub fn is_named(&self) -> bool {
        self.name.is_some()
    }

    pub async fn close(self) -> Result<()> {
        let _: EmptyResult = call_typed(
            &self.runtime,
            "session.close",
            json!({
                "repoId": self.authority.repo_id,
                "workspace": self.authority.workspace,
                "workspaceIncarnation": self.authority.workspace_incarnation,
                "session": self.name,
            }),
        )
        .await?;
        Ok(())
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
    use std::future;
    use std::mem::{needs_drop, size_of};
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

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
        async fn call(&self, method: &'static str, _params: Value) -> Result<Value> {
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

        async fn upload(
            &self,
            method: &'static str,
            _params: Value,
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
            _params: Value,
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
            _follow: bool,
        ) -> Result<RawByteStream> {
            Err(CowshedError::internal("unexpected test logs"))
        }

        async fn attach(
            &self,
            _authority: Arc<WorkspaceAuthority>,
            _id: JobId,
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
            "argv": ["true"],
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

    #[test]
    fn authority_tokens_and_handles_are_affine_owned_values() {
        assert!(needs_drop::<CoordinatorToken>());
        assert!(needs_drop::<Coordinator>());
        assert!(needs_drop::<WorkspaceHandle>());
        assert!(size_of::<CoordinatorToken>() > 0);
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
        (spawn_controller_actor(client), server)
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
            argv: vec!["cat".into()],
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
                image_format: crate::metadata::ImageFormat::Asif,
                mount: PathBuf::from("/mnt/raven"),
                state: super::super::dto::WorkspaceState::Detached,
                branch: None,
                base_commit: None,
                created_at: None,
                checkpoints: Vec::new(),
                snapshot_stale: false,
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
            binding,
            git_root: PathBuf::from("/repo"),
            paths: ProjectPaths::new("/tmp/cowshed-capability-tests", &repo_id).unwrap(),
            runtime: Arc::clone(&runtime),
        };
        Coordinator {
            project,
            runtime: Arc::clone(&runtime),
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
                    "argv": ["cat"],
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
    async fn named_session_run_and_background_preserve_exact_session_identity() {
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
                        "argv": ["cat"],
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
                .background(exec_request(StdinSource::Empty))
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
                    "argv": ["cat"],
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
            false,
        );
        let mut stderr = poll_job_stream(runtime, authority, id, JobStream::Stderr, false);
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

        let error = runtime.call("project.list", json!({})).await.unwrap_err();
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
            let error = runtime.call("project.list", json!({})).await.unwrap_err();
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
            let error = runtime.call("project.list", json!({})).await.unwrap_err();
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
                .download("job.logs", json!({}), 0)
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
                .download("job.logs", json!({}), 0)
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
                .download("job.logs", json!({}), 0)
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
                .download("job.logs", json!({}), 0)
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
                .download("job.logs", json!({}), 0)
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
            let error = runtime.call("project.list", json!({})).await.unwrap_err();
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
                json!({}),
                Bytes::from(vec![0; MAX_BINARY_FRAME_BYTES + 1]),
            )
            .await
            .unwrap_err();
        assert!(error.message.contains("64 KiB"));
        assert_eq!(
            runtime.call("project.list", json!({})).await.unwrap(),
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
