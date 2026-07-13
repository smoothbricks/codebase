use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::repository::RepoId;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, OwnedFd};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

pub const HANDSHAKE_VERSION: u32 = 1;
pub const MAX_HANDSHAKE_BYTES: usize = 4096;
pub const MAX_JSON_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BINARY_FRAME_BYTES: usize = 64 * 1024;

const ROUTER_CLOSED_HINT: &str = "restart the trusted cowshed controller";

/// Methods emitted by the capability API. Coordinator connections may call every one of them.
pub const CAPABILITY_METHODS: &[&str] = &[
    "project.open",
    "project.workspace",
    "project.list",
    "workspace.info",
    "workspace.ensure",
    "workspace.attach",
    "workspace.grants",
    "coordinator.adopt",
    "coordinator.create",
    "coordinator.fork",
    "coordinator.grant",
    "coordinator.revoke",
    "coordinator.rebase",
    "coordinator.land",
    "coordinator.restore",
    "coordinator.detach",
    "coordinator.assignSlot",
    "coordinator.destroy",
    "coordinator.gc",
    "coordinator.repoMirror",
    "coordinator.setCheckpointQuota",
    "coordinator.doctor",
    "coordinator.worker",
    "worker.exec",
    "worker.stdinChunk",
    "worker.stdinClose",
    "worker.shell",
    "worker.listJobs",
    "worker.job",
    "worker.checkpoint",
    "worker.push",
    "job.status",
    "job.logs",
    "job.attachWrite",
    "job.detach",
    "job.wait",
    "job.kill",
    "session.close",
];

/// Capability methods a workspace-bound worker connection may call.
pub const WORKER_METHODS: &[&str] = &[
    "workspace.grants",
    "worker.exec",
    "worker.stdinChunk",
    "worker.stdinClose",
    "worker.shell",
    "worker.listJobs",
    "worker.job",
    "worker.checkpoint",
    "worker.push",
    "job.status",
    "job.logs",
    "job.attachWrite",
    "job.detach",
    "job.wait",
    "job.kill",
    "session.close",
];

const UPLOAD_METHODS: &[&str] = &["worker.exec", "worker.stdinChunk", "job.attachWrite"];

/// Authority fixed when the trusted controller accepts a connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionAuthority {
    Coordinator {
        repo_id: RepoId,
    },
    Worker {
        repo_id: RepoId,
        workspace: WorkspaceName,
        workspace_incarnation: WorkspaceIncarnation,
    },
}

impl ConnectionAuthority {
    pub fn repo_id(&self) -> &RepoId {
        match self {
            Self::Coordinator { repo_id } | Self::Worker { repo_id, .. } => repo_id,
        }
    }
}

/// A request already authenticated and fenced to its immutable connection authority.
#[derive(Debug)]
pub struct RouterRequest {
    authority: ConnectionAuthority,
    method: String,
    params: Value,
    upload: Option<Bytes>,
}

impl RouterRequest {
    pub fn authority(&self) -> &ConnectionAuthority {
        &self.authority
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn params(&self) -> &Value {
        &self.params
    }

    pub fn upload(&self) -> Option<&Bytes> {
        self.upload.as_ref()
    }

    pub fn into_parts(self) -> (ConnectionAuthority, String, Value, Option<Bytes>) {
        (self.authority, self.method, self.params, self.upload)
    }
}

/// The router's JSON result and optional single bounded raw-byte lane.
#[derive(Debug)]
pub struct RouterResponse {
    result: Value,
    binary: Option<Bytes>,
}

impl RouterResponse {
    pub fn json(result: Value) -> Self {
        Self {
            result,
            binary: None,
        }
    }

    pub fn binary(result: Value, binary: Bytes) -> Result<Self> {
        if binary.len() > MAX_BINARY_FRAME_BYTES {
            return Err(CowshedError::internal(
                "controller router binary response exceeds the 64 KiB frame limit",
            ));
        }
        Ok(Self {
            result,
            binary: Some(binary),
        })
    }

    pub fn into_parts(self) -> (Value, Option<Bytes>) {
        (self.result, self.binary)
    }
}

/// One actor command. Consuming it separates the immutable request from its affine reply.
#[derive(Debug)]
pub struct RouterCommand {
    request: RouterRequest,
    reply: RouterReply,
}

impl RouterCommand {
    pub fn request(&self) -> &RouterRequest {
        &self.request
    }

    pub fn into_parts(self) -> (RouterRequest, RouterReply) {
        (self.request, self.reply)
    }
}

#[derive(Debug)]
pub struct RouterReply(oneshot::Sender<Result<RouterResponse>>);

impl RouterReply {
    pub fn send(
        self,
        response: Result<RouterResponse>,
    ) -> std::result::Result<(), Result<RouterResponse>> {
        self.0.send(response)
    }
}

/// Cloneable ingress for a single-owner router actor.
#[derive(Clone, Debug)]
pub struct RouterHandle {
    sender: mpsc::Sender<RouterCommand>,
}

impl RouterHandle {
    pub fn channel(capacity: NonZeroUsize) -> (Self, mpsc::Receiver<RouterCommand>) {
        let (sender, receiver) = mpsc::channel(capacity.get());
        (Self { sender }, receiver)
    }

    pub async fn route(
        &self,
        authority: ConnectionAuthority,
        method: String,
        params: Value,
        upload: Option<Bytes>,
    ) -> Result<RouterResponse> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(RouterCommand {
                request: RouterRequest {
                    authority,
                    method,
                    params,
                    upload,
                },
                reply: RouterReply(reply),
            })
            .await
            .map_err(|_| router_closed("controller router actor channel closed"))?;
        response
            .await
            .map_err(|_| router_closed("controller router actor stopped before replying"))?
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClientHello {
    version: u32,
    nonce: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ServerHello<'a> {
    version: u32,
    nonce: &'a str,
    repo_id: &'a RepoId,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RpcRequest {
    id: u64,
    method: String,
    params: Value,
    binary_length: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcResponse<'a> {
    id: u64,
    ok: bool,
    result: Option<&'a Value>,
    error: Option<&'a CowshedError>,
    binary_length: Option<u32>,
}

/// Serves one inherited stream descriptor until a clean disconnect or a fatal protocol failure.
/// Dropping this future owns only connection state; routed jobs remain owned by the router actor.
pub async fn serve_controller_connection(
    descriptor: OwnedFd,
    authority: ConnectionAuthority,
    router: RouterHandle,
) -> Result<()> {
    verify_peer(&descriptor)?;
    let stream = std::os::unix::net::UnixStream::from(descriptor);
    stream.set_nonblocking(true).map_err(|error| {
        connection_error(format!("controller descriptor setup failed: {error}"))
    })?;
    let mut stream = tokio::net::UnixStream::from_std(stream).map_err(|error| {
        connection_error(format!("controller descriptor setup failed: {error}"))
    })?;

    let hello = read_required_frame(
        &mut stream,
        MAX_HANDSHAKE_BYTES,
        "controller handshake request",
    )
    .await?;
    let hello: ClientHello = serde_json::from_slice(&hello).map_err(|error| {
        protocol_error(format!("controller handshake request is invalid: {error}"))
    })?;
    validate_hello(&hello)?;
    let response = serde_json::to_vec(&ServerHello {
        version: HANDSHAKE_VERSION,
        nonce: &hello.nonce,
        repo_id: authority.repo_id(),
    })
    .map_err(|error| protocol_error(format!("controller handshake encoding failed: {error}")))?;
    write_frame(
        &mut stream,
        &response,
        MAX_HANDSHAKE_BYTES,
        "controller handshake response",
    )
    .await?;

    let mut next_id = 1_u64;
    loop {
        let Some(frame) =
            read_optional_frame(&mut stream, MAX_JSON_FRAME_BYTES, "controller RPC request")
                .await?
        else {
            return Ok(());
        };
        let request: RpcRequest = serde_json::from_slice(&frame).map_err(|error| {
            protocol_error(format!("controller RPC request is invalid: {error}"))
        })?;
        if request.id != next_id {
            let error =
                protocol_error("controller RPC request id was replayed or arrived out of order");
            write_rpc_error(&mut stream, request.id, &error).await?;
            return Err(error);
        }
        next_id = next_id
            .checked_add(1)
            .ok_or_else(|| protocol_error("controller RPC request id overflowed"))?;

        if let Err(error) = validate_request(&authority, &request) {
            write_rpc_error(&mut stream, request.id, &error).await?;
            if error.code == ErrorCode::Integrity {
                return Err(error);
            }
            continue;
        }

        let download_offset = request
            .params
            .get("offset")
            .and_then(Value::as_u64)
            .filter(|_| request.method == "job.logs");

        let upload = match request.binary_length {
            Some(length) => Some(
                read_binary_frame(
                    &mut stream,
                    usize::try_from(length).map_err(|_| {
                        protocol_error("controller RPC binary length does not fit this platform")
                    })?,
                )
                .await?,
            ),
            None => None,
        };

        let response = router.route(
            authority.clone(),
            request.method.clone(),
            request.params,
            upload,
        );
        tokio::pin!(response);
        let response = loop {
            tokio::select! {
                response = &mut response => break response,
                readiness = stream.readable() => {
                    readiness.map_err(|error| {
                        connection_error(format!(
                            "controller RPC disconnect check failed: {error}"
                        ))
                    })?;
                    let mut probe = [0_u8; 1];
                    match stream.try_read(&mut probe) {
                        Ok(0) => return Ok(()),
                        Ok(_) => {
                            return Err(protocol_error(
                                "controller RPC requests must not be pipelined",
                            ));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(error) => {
                            return Err(connection_error(format!(
                                "controller RPC disconnect check failed: {error}"
                            )));
                        }
                    }
                }
            }
        };
        match response {
            Ok(response) => {
                let (result, binary) = response.into_parts();
                match (download_offset, binary.as_ref()) {
                    (Some(offset), Some(bytes)) => {
                        if let Err(error) = validate_raw_response(&result, offset, bytes.len()) {
                            write_rpc_error(&mut stream, request.id, &error).await?;
                            return Err(error);
                        }
                    }
                    (Some(_), None) => {
                        let error =
                            protocol_error("controller router omitted the requested raw-byte lane");
                        write_rpc_error(&mut stream, request.id, &error).await?;
                        return Err(error);
                    }
                    (None, Some(_)) => {
                        let error = protocol_error(
                            "controller router attempted a second or unsolicited raw-byte lane",
                        );
                        write_rpc_error(&mut stream, request.id, &error).await?;
                        return Err(error);
                    }
                    (None, None) => {}
                }
                write_rpc_success(&mut stream, request.id, &result, binary.as_ref()).await?;
                if let Some(binary) = binary {
                    write_binary_frame(&mut stream, &binary).await?;
                }
            }
            Err(error) => write_rpc_error(&mut stream, request.id, &error).await?,
        }
    }
}

fn validate_hello(hello: &ClientHello) -> Result<()> {
    if hello.version != HANDSHAKE_VERSION {
        return Err(protocol_error(
            "controller handshake protocol version did not match",
        ));
    }
    if hello.nonce.len() != 64
        || !hello
            .nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(protocol_error("controller handshake nonce is invalid"));
    }
    Ok(())
}

fn validate_request(authority: &ConnectionAuthority, request: &RpcRequest) -> Result<()> {
    if !CAPABILITY_METHODS.contains(&request.method.as_str()) {
        return Err(authority_error(format!(
            "controller method is not in the capability allowlist: {}",
            request.method
        )));
    }
    let params = request.params.as_object().ok_or_else(|| {
        CowshedError::usage(
            "controller RPC params must be a JSON object",
            "send the exact parameters required by the capability method",
        )
    })?;

    match authority {
        ConnectionAuthority::Coordinator { repo_id } => {
            if request.method != "project.open" {
                require_string(params, "repoId", repo_id.as_str())?;
            }
        }
        ConnectionAuthority::Worker {
            repo_id,
            workspace,
            workspace_incarnation,
        } => {
            if !WORKER_METHODS.contains(&request.method.as_str()) {
                return Err(authority_error(format!(
                    "worker authority cannot call coordinator method {}",
                    request.method
                )));
            }
            require_string(params, "repoId", repo_id.as_str())?;
            require_string(params, "workspace", workspace.as_str())?;
            require_string(
                params,
                "workspaceIncarnation",
                workspace_incarnation.as_str(),
            )?;
        }
    }

    if request.method == "job.logs" && params.get("offset").and_then(Value::as_u64).is_none() {
        return Err(CowshedError::usage(
            "job.logs offset must be an unsigned integer",
            "send the offset returned by the preceding raw-byte frame",
        ));
    }

    match request.binary_length {
        Some(length) if usize::try_from(length).unwrap_or(usize::MAX) > MAX_BINARY_FRAME_BYTES => {
            Err(protocol_error(
                "controller RPC binary request exceeds the 64 KiB frame limit",
            ))
        }
        Some(_) if !UPLOAD_METHODS.contains(&request.method.as_str()) => Err(protocol_error(
            "controller RPC method does not accept a raw-byte upload lane",
        )),
        None => Ok(()),
        Some(_) => Ok(()),
    }
}

fn validate_raw_response(result: &Value, offset: u64, length: usize) -> Result<()> {
    let object = result.as_object().ok_or_else(|| {
        protocol_error("controller router raw-byte metadata must be a JSON object")
    })?;
    if object.len() != 2 || object.get("eof").and_then(Value::as_bool).is_none() {
        return Err(protocol_error(
            "controller router raw-byte metadata has an invalid envelope",
        ));
    }
    let next_offset = object
        .get("nextOffset")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            protocol_error("controller router raw-byte metadata has an invalid nextOffset")
        })?;
    let length = u64::try_from(length)
        .map_err(|_| protocol_error("controller router raw-byte response length overflowed"))?;
    let expected = offset
        .checked_add(length)
        .ok_or_else(|| protocol_error("controller router raw-byte response offset overflowed"))?;
    if next_offset != expected {
        return Err(protocol_error(
            "controller router raw-byte response nextOffset was not exact",
        ));
    }
    Ok(())
}

fn require_string(
    params: &serde_json::Map<String, Value>,
    field: &'static str,
    expected: &str,
) -> Result<()> {
    if params.get(field).and_then(Value::as_str) == Some(expected) {
        Ok(())
    } else {
        Err(authority_error(format!(
            "controller RPC {field} does not match connection authority"
        )))
    }
}

async fn read_required_frame(
    stream: &mut tokio::net::UnixStream,
    maximum: usize,
    description: &'static str,
) -> Result<Vec<u8>> {
    read_optional_frame(stream, maximum, description)
        .await?
        .ok_or_else(|| connection_error(format!("{description} ended before a frame arrived")))
}

async fn read_optional_frame(
    stream: &mut tokio::net::UnixStream,
    maximum: usize,
    description: &'static str,
) -> Result<Option<Vec<u8>>> {
    let mut length_bytes = [0_u8; 4];
    let first = stream
        .read(&mut length_bytes[..1])
        .await
        .map_err(|error| connection_error(format!("{description} read failed: {error}")))?;
    if first == 0 {
        return Ok(None);
    }
    stream
        .read_exact(&mut length_bytes[1..])
        .await
        .map_err(|error| {
            connection_error(format!("{description} header was truncated: {error}"))
        })?;
    let length = usize::try_from(u32::from_be_bytes(length_bytes))
        .map_err(|_| protocol_error(format!("{description} length does not fit this platform")))?;
    if length == 0 || length > maximum {
        return Err(protocol_error(format!("{description} has invalid length")));
    }
    let mut bytes = vec![0_u8; length];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|error| connection_error(format!("{description} was truncated: {error}")))?;
    Ok(Some(bytes))
}

async fn write_frame(
    stream: &mut tokio::net::UnixStream,
    bytes: &[u8],
    maximum: usize,
    description: &'static str,
) -> Result<()> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(protocol_error(format!("{description} has invalid length")));
    }
    let length = u32::try_from(bytes.len())
        .map_err(|_| protocol_error(format!("{description} length does not fit the wire")))?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|error| connection_error(format!("{description} write failed: {error}")))?;
    stream
        .write_all(bytes)
        .await
        .map_err(|error| connection_error(format!("{description} write failed: {error}")))
}

async fn read_binary_frame(
    stream: &mut tokio::net::UnixStream,
    expected_length: usize,
) -> Result<Bytes> {
    if expected_length > MAX_BINARY_FRAME_BYTES {
        return Err(protocol_error(
            "controller RPC binary request exceeds the 64 KiB frame limit",
        ));
    }
    let mut length_bytes = [0_u8; 4];
    stream
        .read_exact(&mut length_bytes)
        .await
        .map_err(|error| connection_error(format!("controller RPC binary read failed: {error}")))?;
    let actual_length = usize::try_from(u32::from_be_bytes(length_bytes)).map_err(|_| {
        protocol_error("controller RPC binary request length does not fit this platform")
    })?;
    if actual_length > MAX_BINARY_FRAME_BYTES {
        return Err(protocol_error(
            "controller RPC binary request has an oversized frame",
        ));
    }
    if actual_length != expected_length {
        return Err(protocol_error(format!(
            "controller RPC binary request length mismatch: declared {expected_length}, framed {actual_length}"
        )));
    }
    let mut bytes = vec![0_u8; actual_length];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|error| connection_error(format!("controller RPC binary read failed: {error}")))?;
    Ok(Bytes::from(bytes))
}

async fn write_binary_frame(stream: &mut tokio::net::UnixStream, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_BINARY_FRAME_BYTES {
        return Err(protocol_error(
            "controller RPC binary response exceeds the 64 KiB frame limit",
        ));
    }
    let length = u32::try_from(bytes.len()).map_err(|_| {
        protocol_error("controller RPC binary response length does not fit the wire")
    })?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|error| {
            connection_error(format!("controller RPC binary write failed: {error}"))
        })?;
    stream
        .write_all(bytes)
        .await
        .map_err(|error| connection_error(format!("controller RPC binary write failed: {error}")))
}

async fn write_rpc_success(
    stream: &mut tokio::net::UnixStream,
    id: u64,
    result: &Value,
    binary: Option<&Bytes>,
) -> Result<()> {
    let binary_length = binary
        .map(Bytes::len)
        .map(u32::try_from)
        .transpose()
        .map_err(|_| {
            protocol_error("controller RPC binary response length does not fit the wire")
        })?;
    let response = serde_json::to_vec(&RpcResponse {
        id,
        ok: true,
        result: Some(result),
        error: None,
        binary_length,
    })
    .map_err(|error| protocol_error(format!("controller RPC response encoding failed: {error}")))?;
    write_frame(
        stream,
        &response,
        MAX_JSON_FRAME_BYTES,
        "controller RPC response",
    )
    .await
}

async fn write_rpc_error(
    stream: &mut tokio::net::UnixStream,
    id: u64,
    error: &CowshedError,
) -> Result<()> {
    let response = serde_json::to_vec(&RpcResponse {
        id,
        ok: false,
        result: None,
        error: Some(error),
        binary_length: None,
    })
    .map_err(|encoding| {
        protocol_error(format!(
            "controller RPC error response encoding failed: {encoding}"
        ))
    })?;
    write_frame(
        stream,
        &response,
        MAX_JSON_FRAME_BYTES,
        "controller RPC response",
    )
    .await
}

fn verify_peer(descriptor: &OwnedFd) -> Result<()> {
    let fd = descriptor.as_raw_fd();
    let mut socket_type: libc::c_int = 0;
    let mut socket_type_len = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .map_err(|_| connection_error("socket type size does not fit socklen_t"))?;
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            std::ptr::from_mut(&mut socket_type).cast(),
            &mut socket_type_len,
        )
    };
    if result != 0 || socket_type != libc::SOCK_STREAM {
        return Err(connection_error(
            "controller descriptor is not a stream socket",
        ));
    }

    #[cfg(target_os = "macos")]
    {
        let mut peer_uid: libc::uid_t = 0;
        let mut peer_gid: libc::gid_t = 0;
        let result = unsafe { libc::getpeereid(fd, &mut peer_uid, &mut peer_gid) };
        let current_uid = unsafe { libc::geteuid() };
        if result != 0 || peer_uid != current_uid {
            return Err(connection_error(
                "controller descriptor peer does not match the current uid",
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
        let mut credentials_len = libc::socklen_t::try_from(std::mem::size_of::<libc::ucred>())
            .map_err(|_| connection_error("peer credential size does not fit socklen_t"))?;
        let result = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::from_mut(&mut credentials).cast(),
                &mut credentials_len,
            )
        };
        let current_uid = unsafe { libc::geteuid() };
        if result != 0 || credentials.uid != current_uid {
            return Err(connection_error(
                "controller descriptor peer does not match the current uid",
            ));
        }
    }

    Ok(())
}

fn router_closed(message: &'static str) -> CowshedError {
    CowshedError::new(ErrorCode::EnvironmentMissing, message, ROUTER_CLOSED_HINT)
}

fn connection_error(message: impl Into<String>) -> CowshedError {
    CowshedError::new(ErrorCode::EnvironmentMissing, message, ROUTER_CLOSED_HINT)
}

fn protocol_error(message: impl Into<String>) -> CowshedError {
    CowshedError::integrity(message, "restart the trusted cowshed controller")
}

fn authority_error(message: impl Into<String>) -> CowshedError {
    CowshedError::new(
        ErrorCode::Conflict,
        message,
        "use a capability bound to the requested repository and workspace incarnation",
    )
}
