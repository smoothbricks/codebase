use super::dto::{
    AdoptOptions, AttachOptions, CheckpointQuota, CheckpointResult, CreateOptions, EmptyResult,
    ExecRequest, GcOptions, GcReport, GitOid, GrantDelta, GrantSet, JobId, JobInfo, JobState,
    LandOptions, LandReport, MirrorInfo, PushOptions, PushReport, RebaseOptions, RemoveOptions,
    RevisionResult, RunSandboxMode, StdinSource, WorkspaceInfo,
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

const HANDSHAKE_VERSION: u32 = 1;
const MAX_HANDSHAKE_BYTES: usize = 4096;
#[cfg(unix)]
const MAX_RPC_BYTES: usize = 16 * 1024 * 1024;

#[async_trait]
pub(crate) trait ControllerRuntime: Send + Sync {
    async fn call(&self, method: &'static str, params: Value) -> Result<Value>;
    async fn exec(&self, workspace: &WorkspaceName, request: ExecRequest) -> Result<JobId>;
    async fn logs(
        &self,
        workspace: &WorkspaceName,
        id: JobId,
        stream: JobStream,
        follow: bool,
    ) -> Result<RawByteStream>;
    async fn attach(&self, workspace: &WorkspaceName, id: JobId) -> Result<JobAttachment>;
    async fn kill(&self, workspace: &WorkspaceName, id: JobId) -> Result<()>;
}

#[cfg(unix)]
struct ActorMessage {
    method: &'static str,
    params: Value,
    reply: oneshot::Sender<Result<Value>>,
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
            .send(ActorMessage {
                method,
                params,
                reply,
            })
            .await
            .map_err(|_| {
                CowshedError::new(
                    ErrorCode::EnvironmentMissing,
                    "controller actor channel closed",
                    "restart the trusted cowshed controller",
                )
            })?;
        response.await.map_err(|_| {
            CowshedError::new(
                ErrorCode::EnvironmentMissing,
                "controller actor stopped before replying",
                "restart the trusted cowshed controller",
            )
        })?
    }

    async fn exec(&self, workspace: &WorkspaceName, request: ExecRequest) -> Result<JobId> {
        let ExecRequest {
            argv,
            cwd,
            mode,
            env,
            trace,
            stdin,
        } = request;
        let mode = match mode {
            RunSandboxMode::ReadWrite => "readWrite",
            RunSandboxMode::ReadOnly => "readOnly",
        };
        let (stdin_metadata, mut stream) = match stdin {
            StdinSource::Empty => (json!({ "kind": "empty" }), None),
            StdinSource::Inline(bytes) => {
                (json!({ "kind": "inline", "bytes": bytes.as_ref() }), None)
            }
            StdinSource::WorkspaceFile(path) => (
                json!({ "kind": "workspaceFile", "workspacePath": path }),
                None,
            ),
            StdinSource::Stream(stream) => (json!({ "kind": "stream" }), Some(stream)),
        };
        let result = self
            .call(
                "worker.exec",
                json!({
                    "workspace": workspace,
                    "argv": argv,
                    "cwd": cwd,
                    "mode": mode,
                    "env": env,
                    "trace": trace,
                    "stdin": stdin_metadata,
                }),
            )
            .await?;
        let job_id: JobId = serde_json::from_value(result).map_err(|error| {
            CowshedError::new(
                ErrorCode::Internal,
                format!("controller returned an invalid worker.exec response: {error}"),
                "cowshed doctor --json",
            )
        })?;
        if let Some(reader) = stream.as_mut() {
            let mut buffer = [0_u8; 64 * 1024];
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
                let _: EmptyResult = serde_json::from_value(
                    self.call(
                        "worker.stdinChunk",
                        json!({
                            "workspace": workspace,
                            "jobId": job_id,
                            "bytes": &buffer[..count],
                        }),
                    )
                    .await?,
                )
                .map_err(|error| {
                    CowshedError::new(
                        ErrorCode::Internal,
                        format!("controller rejected a stdin chunk response: {error}"),
                        "cowshed doctor --json",
                    )
                })?;
            }
            let _: EmptyResult = serde_json::from_value(
                self.call(
                    "worker.stdinClose",
                    json!({ "workspace": workspace, "jobId": job_id }),
                )
                .await?,
            )
            .map_err(|error| {
                CowshedError::new(
                    ErrorCode::Internal,
                    format!("controller rejected stdin close response: {error}"),
                    "cowshed doctor --json",
                )
            })?;
        }
        Ok(job_id)
    }

    async fn logs(
        &self,
        workspace: &WorkspaceName,
        id: JobId,
        stream: JobStream,
        follow: bool,
    ) -> Result<RawByteStream> {
        Ok(poll_job_stream(
            Arc::new(self.clone()),
            workspace.clone(),
            id,
            stream,
            follow,
        ))
    }

    async fn attach(&self, workspace: &WorkspaceName, id: JobId) -> Result<JobAttachment> {
        let stdout = self.logs(workspace, id, JobStream::Stdout, true).await?;
        let stderr = self.logs(workspace, id, JobStream::Stderr, true).await?;
        let runtime: Arc<dyn ControllerRuntime> = Arc::new(self.clone());
        Ok(JobAttachment {
            workspace: workspace.clone(),
            id,
            stdin: JobStdin {
                workspace: workspace.clone(),
                id,
                runtime: Arc::clone(&runtime),
            },
            stdout,
            stderr,
            runtime,
        })
    }

    async fn kill(&self, workspace: &WorkspaceName, id: JobId) -> Result<()> {
        self.call("job.kill", json!({ "workspace": workspace, "jobId": id }))
            .await
            .and_then(decode_empty)
    }
}

fn poll_job_stream(
    runtime: Arc<dyn ControllerRuntime>,
    workspace: WorkspaceName,
    id: JobId,
    stream: JobStream,
    follow: bool,
) -> RawByteStream {
    let (sender, receiver) = mpsc::channel(8);
    tokio::spawn(async move {
        let mut offset = 0_u64;
        loop {
            let value = tokio::select! {
                _ = sender.closed() => break,
                value = runtime.call(
                    "job.logs",
                    json!({
                        "workspace": workspace,
                        "jobId": id,
                        "stream": stream,
                        "follow": follow,
                        "offset": offset,
                    }),
                ) => value,
            };
            let chunk = match value.and_then(|value| {
                serde_json::from_value::<LogChunk>(value).map_err(|error| {
                    CowshedError::internal(format!(
                        "controller returned an invalid job.logs response: {error}"
                    ))
                })
            }) {
                Ok(chunk) => chunk,
                Err(error) => {
                    tokio::select! {
                        _ = sender.closed() => {}
                        _ = sender.send(Err(error)) => {}
                    }
                    break;
                }
            };
            if !chunk.bytes.is_empty() {
                offset = offset.saturating_add(chunk.bytes.len() as u64);
                let bytes = Bytes::from(chunk.bytes);
                let sent = tokio::select! {
                    _ = sender.closed() => false,
                    result = sender.send(Ok(bytes)) => result.is_ok(),
                };
                if !sent {
                    break;
                }
            }
            if !follow {
                break;
            }
            if chunk.eof {
                let status: Result<JobInfo> = tokio::select! {
                    _ = sender.closed() => break,
                    value = runtime.call(
                        "job.status",
                        json!({ "workspace": workspace, "jobId": id }),
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
            tokio::select! {
                _ = sender.closed() => break,
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LogChunk {
    bytes: Vec<u8>,
    eof: bool,
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
    workspace: WorkspaceName,
    id: JobId,
    runtime: Arc<dyn ControllerRuntime>,
}

impl JobStdin {
    pub async fn write(&self, bytes: Bytes) -> Result<()> {
        let value = self
            .runtime
            .call(
                "job.attachWrite",
                json!({
                    "workspace": self.workspace,
                    "jobId": self.id,
                    "bytes": bytes.as_ref(),
                }),
            )
            .await?;
        decode_empty(value)
    }
}

impl fmt::Debug for JobStdin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobStdin")
            .field("workspace", &self.workspace)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

pub struct JobAttachment {
    workspace: WorkspaceName,
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
                json!({ "workspace": self.workspace, "jobId": self.id }),
            )
            .await?;
        decode_empty(value)
    }
}

impl fmt::Debug for JobAttachment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobAttachment")
            .field("workspace", &self.workspace)
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

    pub async fn info(&self) -> Result<WorkspaceInfo> {
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

    pub async fn grants(&self) -> Result<GrantSet> {
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
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientHello<'a> {
    version: u32,
    nonce: &'a str,
}

#[cfg(unix)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServerHello {
    version: u32,
    nonce: String,
    repo_id: RepoId,
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
#[derive(Serialize)]
struct RpcRequest<'a> {
    id: u64,
    method: &'a str,
    params: &'a Value,
}

#[cfg(unix)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcResponse {
    id: u64,
    ok: bool,
    result: Option<Value>,
    error: Option<CowshedError>,
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
fn spawn_controller_actor(mut stream: tokio::net::UnixStream) -> Arc<dyn ControllerRuntime> {
    let (sender, mut receiver) = mpsc::channel::<ActorMessage>(32);
    tokio::spawn(async move {
        let mut next_id = 1_u64;
        while let Some(message) = receiver.recv().await {
            let id = next_id;
            next_id = next_id.saturating_add(1);
            let result = async {
                let request = serde_json::to_vec(&RpcRequest {
                    id,
                    method: message.method,
                    params: &message.params,
                })
                .map_err(|error| {
                    CowshedError::internal(format!(
                        "controller RPC request encoding failed: {error}"
                    ))
                })?;
                write_rpc_frame(&mut stream, &request).await?;
                let response = read_rpc_frame(&mut stream).await?;
                let response: RpcResponse = serde_json::from_slice(&response).map_err(|error| {
                    CowshedError::internal(format!(
                        "controller RPC response decoding failed: {error}"
                    ))
                })?;
                if response.id != id {
                    return Err(CowshedError::internal(
                        "controller RPC response id did not match request",
                    ));
                }
                match (response.ok, response.result, response.error) {
                    (true, Some(result), None) => Ok(result),
                    (false, None, Some(error)) => Err(error),
                    _ => Err(CowshedError::internal(
                        "controller RPC response has an invalid envelope",
                    )),
                }
            }
            .await;
            let stop = result
                .as_ref()
                .is_err_and(|error| error.code == ErrorCode::EnvironmentMissing);
            let _ = message.reply.send(result);
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
    let hello = serde_json::to_vec(&ClientHello {
        version: HANDSHAKE_VERSION,
        nonce: &nonce,
    })
    .map_err(|error| handshake_error(format!("coordinator handshake encoding failed: {error}")))?;
    write_frame(&mut stream, &hello).await?;
    let response = read_frame(&mut stream).await?;
    let response: ServerHello = serde_json::from_slice(&response).map_err(|error| {
        handshake_error(format!(
            "coordinator handshake response is invalid: {error}"
        ))
    })?;
    if response.version != HANDSHAKE_VERSION || response.nonce != nonce {
        return Err(handshake_error(
            "coordinator handshake nonce or protocol version did not match",
        ));
    }
    let runtime = spawn_controller_actor(stream);
    let token = CoordinatorToken {
        repo_id: response.repo_id,
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

    pub async fn worker(&self, workspace: &str) -> Result<WorkspaceHandle> {
        let wire: WorkspaceWire = call_typed(
            &self.runtime,
            "coordinator.worker",
            json!({ "repoId": self.project.repo_id, "workspace": workspace }),
        )
        .await?;
        Ok(WorkspaceHandle {
            workspace: WorkspaceRef::from_wire(wire, Arc::clone(&self.runtime)),
            runtime: Arc::clone(&self.runtime),
        })
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
    runtime: Arc<dyn ControllerRuntime>,
}

impl WorkspaceHandle {
    pub fn name(&self) -> &WorkspaceName {
        self.workspace.name()
    }

    pub fn mount_path(&self) -> &Path {
        self.workspace.mount_path()
    }

    pub async fn exec(&self, request: ExecRequest) -> Result<JobHandle> {
        let id = self.runtime.exec(self.name(), request).await?;
        Ok(JobHandle {
            workspace: self.name().clone(),
            id,
            runtime: Arc::clone(&self.runtime),
        })
    }

    pub async fn shell(&self, session: Option<&str>) -> Result<Session> {
        let _: EmptyResult = call_typed(
            &self.runtime,
            "worker.shell",
            json!({ "workspace": self.name(), "session": session }),
        )
        .await?;
        Ok(Session {
            workspace: self.name().clone(),
            name: session.map(str::to_owned),
            runtime: Arc::clone(&self.runtime),
        })
    }

    pub async fn list_jobs(&self) -> Result<Vec<JobInfo>> {
        call_typed(
            &self.runtime,
            "worker.listJobs",
            json!({ "workspace": self.name() }),
        )
        .await
    }

    pub async fn job(&self, id: JobId) -> Result<JobHandle> {
        let _: JobInfo = call_typed(
            &self.runtime,
            "worker.job",
            json!({ "workspace": self.name(), "jobId": id }),
        )
        .await?;
        Ok(JobHandle {
            workspace: self.name().clone(),
            id,
            runtime: Arc::clone(&self.runtime),
        })
    }

    pub async fn checkpoint(&self, label: Option<&str>) -> Result<String> {
        let result: CheckpointResult = call_typed(
            &self.runtime,
            "worker.checkpoint",
            json!({ "workspace": self.name(), "label": label }),
        )
        .await?;
        Ok(result.label)
    }

    pub async fn push(&self, options: PushOptions) -> Result<PushReport> {
        call_typed(
            &self.runtime,
            "worker.push",
            json!({ "workspace": self.name(), "options": options }),
        )
        .await
    }

    pub async fn grants(&self) -> Result<GrantSet> {
        self.workspace.grants().await
    }
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
    workspace: WorkspaceName,
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
            json!({ "workspace": self.workspace, "jobId": self.id }),
        )
        .await
    }

    pub async fn logs(&self, stream: JobStream, follow: bool) -> Result<RawByteStream> {
        self.runtime
            .logs(&self.workspace, self.id, stream, follow)
            .await
    }

    pub async fn attach(&self) -> Result<JobAttachment> {
        self.runtime.attach(&self.workspace, self.id).await
    }

    pub async fn detach(&self) -> Result<()> {
        self.empty_call("job.detach").await
    }

    pub async fn wait(&self) -> Result<JobInfo> {
        call_typed(
            &self.runtime,
            "job.wait",
            json!({ "workspace": self.workspace, "jobId": self.id }),
        )
        .await
    }

    pub async fn kill(&self) -> Result<()> {
        self.runtime.kill(&self.workspace, self.id).await
    }

    async fn empty_call(&self, method: &'static str) -> Result<()> {
        let _: EmptyResult = call_typed(
            &self.runtime,
            method,
            json!({ "workspace": self.workspace, "jobId": self.id }),
        )
        .await?;
        Ok(())
    }
}

impl fmt::Debug for JobHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobHandle")
            .field("workspace", &self.workspace)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

pub struct Session {
    workspace: WorkspaceName,
    name: Option<String>,
    runtime: Arc<dyn ControllerRuntime>,
}

impl Session {
    pub async fn run(&self, request: ExecRequest) -> Result<JobHandle> {
        let id = self.runtime.exec(&self.workspace, request).await?;
        Ok(JobHandle {
            workspace: self.workspace.clone(),
            id,
            runtime: Arc::clone(&self.runtime),
        })
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
            json!({ "workspace": self.workspace, "session": self.name }),
        )
        .await?;
        Ok(())
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("workspace", &self.workspace)
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
            match method {
                "job.logs" => {
                    self.log_calls.fetch_add(1, Ordering::SeqCst);
                    if self.mode.load(Ordering::SeqCst) == 1 {
                        self.active_calls.fetch_add(1, Ordering::SeqCst);
                        let _active = ActiveCall(&self.active_calls);
                        future::pending().await
                    } else {
                        Ok(json!({"bytes": [], "eof": true}))
                    }
                }
                "job.status" => {
                    self.status_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(terminal_job_value())
                }
                "job.attachWrite" => {
                    self.stdin_writes.fetch_add(1, Ordering::SeqCst);
                    Ok(json!({}))
                }
                _ => Ok(json!({})),
            }
        }

        async fn exec(&self, _workspace: &WorkspaceName, _request: ExecRequest) -> Result<JobId> {
            Err(CowshedError::internal("unexpected test exec"))
        }

        async fn logs(
            &self,
            _workspace: &WorkspaceName,
            _id: JobId,
            _stream: JobStream,
            _follow: bool,
        ) -> Result<RawByteStream> {
            Err(CowshedError::internal("unexpected test logs"))
        }

        async fn attach(&self, _workspace: &WorkspaceName, _id: JobId) -> Result<JobAttachment> {
            Err(CowshedError::internal("unexpected test attach"))
        }

        async fn kill(&self, _workspace: &WorkspaceName, _id: JobId) -> Result<()> {
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
                "path": ".cowshed/job/7/out",
                "bytes": 0,
                "summary": {"version": 1, "text": "", "truncated": false}
            },
            "stderr": {
                "path": ".cowshed/job/7/err",
                "bytes": 0,
                "summary": {"version": 1, "text": "", "truncated": false}
            },
            "trace": {
                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId": "00f067aa0ba902b7"
            },
            "stdin": {"kind": "empty", "bytes": 0, "complete": true}
        })
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
        let response = serde_json::to_vec(&json!({
            "version": HANDSHAKE_VERSION,
            "nonce": nonce,
            "repoId": "acme/widget",
        }))
        .unwrap();
        write_frame(&mut stream, &response).await
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

    #[tokio::test]
    async fn followed_stream_closes_at_terminal_state_without_empty_chunks() {
        let runtime = Arc::new(TestRuntime::default());
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let mut stream = poll_job_stream(
            runtime_trait,
            WorkspaceName::new("raven").unwrap(),
            JobId::new(7).unwrap(),
            JobStream::Stdout,
            true,
        );

        assert!(stream.next().await.is_none());
        assert_eq!(runtime.log_calls.load(Ordering::SeqCst), 1);
        assert_eq!(runtime.status_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dropping_stream_cancels_an_in_flight_poll() {
        let runtime = Arc::new(TestRuntime::default());
        runtime.mode.store(1, Ordering::SeqCst);
        let runtime_trait: Arc<dyn ControllerRuntime> = runtime.clone();
        let stream = poll_job_stream(
            runtime_trait,
            WorkspaceName::new("raven").unwrap(),
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
        let workspace = WorkspaceName::new("raven").unwrap();
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
        let attachment = JobAttachment {
            workspace: workspace.clone(),
            id,
            stdin: JobStdin {
                workspace,
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
            workspace: WorkspaceName::new("raven").unwrap(),
            id: JobId::new(7).unwrap(),
            runtime,
        };
        let error = handle.kill().await.unwrap_err();
        assert!(error.message.contains("rejected kill"));
    }
}
