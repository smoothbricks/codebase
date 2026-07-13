use super::dto::{
    AdoptOptions, AttachOptions, CheckpointQuota, CheckpointResult, CreateOptions, EmptyResult,
    ExecRequest, GcOptions, GcReport, GitOid, GrantDelta, GrantSet, JobId, JobInfo, LandOptions,
    LandReport, MirrorInfo, PushOptions, PushReport, RebaseOptions, RemoveOptions, RevisionResult,
    RunSandboxMode, StdinSource, WorkspaceInfo,
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
    ) -> Result<JobByteStream>;
    async fn attach(&self, workspace: &WorkspaceName, id: JobId) -> Result<JobAttachment>;
    fn kill(&self, workspace: &WorkspaceName, id: JobId) -> Result<()>;
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
    ) -> Result<JobByteStream> {
        let (sender, receiver) = mpsc::channel(8);
        let runtime = self.clone();
        let workspace = workspace.clone();
        tokio::spawn(async move {
            let mut offset = 0_u64;
            loop {
                let value = runtime
                    .call(
                        "job.logs",
                        json!({
                            "workspace": workspace,
                            "jobId": id,
                            "stream": stream,
                            "follow": follow,
                            "offset": offset,
                        }),
                    )
                    .await;
                let chunk = match value.and_then(|value| {
                    serde_json::from_value::<LogChunk>(value).map_err(|error| {
                        CowshedError::internal(format!(
                            "controller returned an invalid job.logs response: {error}"
                        ))
                    })
                }) {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                };
                if !chunk.bytes.is_empty() {
                    offset = offset.saturating_add(chunk.bytes.len() as u64);
                    if sender.send(Ok(Bytes::from(chunk.bytes))).await.is_err() {
                        break;
                    }
                }
                if chunk.eof && !follow {
                    break;
                }
                if chunk.eof {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        });
        Ok(JobByteStream { receiver })
    }

    async fn attach(&self, workspace: &WorkspaceName, id: JobId) -> Result<JobAttachment> {
        let stdout = self.logs(workspace, id, JobStream::Stdout, true).await?;
        let stderr = self.logs(workspace, id, JobStream::Stderr, true).await?;
        let (stdin, mut writes) = mpsc::channel::<StdinWrite>(8);
        let runtime = self.clone();
        let attached_workspace = workspace.clone();
        tokio::spawn(async move {
            while let Some(write) = writes.recv().await {
                let result = runtime
                    .call(
                        "job.attachWrite",
                        json!({
                            "workspace": attached_workspace,
                            "jobId": id,
                            "bytes": write.bytes.as_ref(),
                        }),
                    )
                    .await
                    .and_then(decode_empty);
                let _ = write.reply.send(result);
            }
        });
        Ok(JobAttachment {
            workspace: workspace.clone(),
            id,
            stdin,
            stdout,
            stderr,
            runtime: Arc::new(self.clone()),
        })
    }

    fn kill(&self, workspace: &WorkspaceName, id: JobId) -> Result<()> {
        let (reply, _response) = oneshot::channel();
        self.sender
            .try_send(ActorMessage {
                method: "job.kill",
                params: json!({ "workspace": workspace, "jobId": id }),
                reply,
            })
            .map_err(|error| {
                CowshedError::new(
                    ErrorCode::EnvironmentMissing,
                    format!("could not queue job kill: {error}"),
                    "retry cowshed job kill",
                )
            })
    }
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

pub struct JobByteStream {
    receiver: mpsc::Receiver<Result<Bytes>>,
}

impl JobByteStream {
    pub async fn next(&mut self) -> Option<Result<Bytes>> {
        self.receiver.recv().await
    }
}

struct StdinWrite {
    bytes: Bytes,
    reply: oneshot::Sender<Result<()>>,
}

pub struct JobAttachment {
    workspace: WorkspaceName,
    id: JobId,
    stdin: mpsc::Sender<StdinWrite>,
    stdout: JobByteStream,
    stderr: JobByteStream,
    runtime: Arc<dyn ControllerRuntime>,
}

impl JobAttachment {
    pub fn stdout(&mut self) -> &mut JobByteStream {
        &mut self.stdout
    }

    pub fn stderr(&mut self) -> &mut JobByteStream {
        &mut self.stderr
    }

    pub async fn write(&self, bytes: Bytes) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.stdin
            .send(StdinWrite { bytes, reply })
            .await
            .map_err(|_| {
                CowshedError::new(
                    ErrorCode::EnvironmentMissing,
                    "job attachment stdin channel closed",
                    "reattach to the durable job",
                )
            })?;
        response.await.map_err(|_| {
            CowshedError::new(
                ErrorCode::EnvironmentMissing,
                "job attachment closed before stdin was acknowledged",
                "reattach to the durable job",
            )
        })?
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

    pub async fn logs(&self, stream: JobStream, follow: bool) -> Result<JobByteStream> {
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

    pub fn kill(&self) -> Result<()> {
        self.runtime.kill(&self.workspace, self.id)
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
    use std::mem::{needs_drop, size_of};

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
}
