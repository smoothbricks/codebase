use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::repository::RepoId;
use bytes::Bytes;
use serde_json::Value;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, OwnedFd};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

pub const HANDSHAKE_VERSION: u32 = 1;
pub const MAX_HANDSHAKE_BYTES: usize = 4096;
pub const MAX_JSON_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BINARY_FRAME_BYTES: usize = 64 * 1024;

pub(crate) mod codec {
    use super::{
        CowshedError, HANDSHAKE_VERSION, MAX_HANDSHAKE_BYTES, MAX_JSON_FRAME_BYTES, RepoId, Value,
    };
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Serialize};
    use std::borrow::Cow;
    use std::fmt;
    use std::io;

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct ClientHelloFields<'a> {
        version: u32,
        nonce: Cow<'a, str>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct ServerHelloFields<'a> {
        version: u32,
        nonce: Cow<'a, str>,
        repo_id: Cow<'a, RepoId>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct RpcRequestFields<'a> {
        id: u64,
        method: Cow<'a, str>,
        params: Cow<'a, Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        binary_length: Option<u32>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct RpcResponseFields<'a> {
        id: u64,
        ok: bool,
        result: Option<Cow<'a, Value>>,
        error: Option<Cow<'a, CowshedError>>,
        binary_length: Option<u32>,
    }

    #[derive(Debug)]
    pub(crate) enum WireCodecError {
        Empty,
        TooLarge { maximum: usize },
        Json(serde_json::Error),
    }

    impl WireCodecError {
        pub(crate) const fn is_too_large(&self) -> bool {
            matches!(self, Self::TooLarge { .. })
        }
    }

    impl fmt::Display for WireCodecError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Empty => formatter.write_str("controller JSON frame is empty"),
                Self::TooLarge { maximum } => {
                    write!(
                        formatter,
                        "controller JSON frame exceeds the {maximum}-byte limit"
                    )
                }
                Self::Json(error) => error.fmt(formatter),
            }
        }
    }

    impl std::error::Error for WireCodecError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Json(error) => Some(error),
                Self::Empty | Self::TooLarge { .. } => None,
            }
        }
    }

    struct BoundedVecWriter {
        bytes: Vec<u8>,
        maximum: usize,
        exceeded: bool,
    }

    impl BoundedVecWriter {
        fn new(maximum: usize) -> Self {
            Self {
                bytes: Vec::with_capacity(maximum.min(1024)),
                maximum,
                exceeded: false,
            }
        }
    }

    impl io::Write for BoundedVecWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let fits = self
                .bytes
                .len()
                .checked_add(bytes.len())
                .is_some_and(|length| length <= self.maximum);
            if !fits {
                self.exceeded = true;
                return Err(io::Error::other("controller JSON frame limit exceeded"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn encode<T: Serialize>(value: &T, maximum: usize) -> Result<Vec<u8>, WireCodecError> {
        let mut writer = BoundedVecWriter::new(maximum);
        match serde_json::to_writer(&mut writer, value) {
            Ok(()) if writer.bytes.is_empty() => Err(WireCodecError::Empty),
            Ok(()) => Ok(writer.bytes),
            Err(_) if writer.exceeded => Err(WireCodecError::TooLarge { maximum }),
            Err(error) => Err(WireCodecError::Json(error)),
        }
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8], maximum: usize) -> Result<T, WireCodecError> {
        if bytes.is_empty() {
            return Err(WireCodecError::Empty);
        }
        if bytes.len() > maximum {
            return Err(WireCodecError::TooLarge { maximum });
        }
        serde_json::from_slice(bytes).map_err(WireCodecError::Json)
    }

    #[derive(Debug)]
    pub(crate) struct DecodedClientHello(ClientHelloFields<'static>);

    impl DecodedClientHello {
        pub(crate) fn into_parts(self) -> (u32, String) {
            (self.0.version, self.0.nonce.into_owned())
        }
    }

    #[derive(Debug)]
    pub(crate) struct DecodedServerHello(ServerHelloFields<'static>);

    impl DecodedServerHello {
        pub(crate) fn into_parts(self) -> (u32, String, RepoId) {
            (
                self.0.version,
                self.0.nonce.into_owned(),
                self.0.repo_id.into_owned(),
            )
        }
    }

    #[derive(Debug)]
    pub(crate) struct DecodedRpcRequest(RpcRequestFields<'static>);

    impl DecodedRpcRequest {
        pub(crate) const fn id(&self) -> u64 {
            self.0.id
        }

        pub(crate) fn method(&self) -> &str {
            &self.0.method
        }

        pub(crate) fn params(&self) -> &Value {
            &self.0.params
        }

        pub(crate) const fn binary_length(&self) -> Option<u32> {
            self.0.binary_length
        }

        pub(crate) fn into_parts(self) -> (u64, String, Value, Option<u32>) {
            (
                self.0.id,
                self.0.method.into_owned(),
                self.0.params.into_owned(),
                self.0.binary_length,
            )
        }
    }

    #[derive(Debug)]
    pub(crate) struct DecodedRpcResponse(RpcResponseFields<'static>);

    impl DecodedRpcResponse {
        pub(crate) fn into_parts(
            self,
        ) -> (u64, bool, Option<Value>, Option<CowshedError>, Option<u32>) {
            (
                self.0.id,
                self.0.ok,
                self.0.result.map(Cow::into_owned),
                self.0.error.map(Cow::into_owned),
                self.0.binary_length,
            )
        }
    }

    pub(crate) fn encode_client_hello(nonce: &str) -> Result<Vec<u8>, WireCodecError> {
        encode(
            &ClientHelloFields {
                version: HANDSHAKE_VERSION,
                nonce: Cow::Borrowed(nonce),
            },
            MAX_HANDSHAKE_BYTES,
        )
    }

    pub(crate) fn decode_client_hello(bytes: &[u8]) -> Result<DecodedClientHello, WireCodecError> {
        decode::<ClientHelloFields<'static>>(bytes, MAX_HANDSHAKE_BYTES).map(DecodedClientHello)
    }

    pub(crate) fn encode_server_hello(
        nonce: &str,
        repo_id: &RepoId,
    ) -> Result<Vec<u8>, WireCodecError> {
        encode(
            &ServerHelloFields {
                version: HANDSHAKE_VERSION,
                nonce: Cow::Borrowed(nonce),
                repo_id: Cow::Borrowed(repo_id),
            },
            MAX_HANDSHAKE_BYTES,
        )
    }

    pub(crate) fn decode_server_hello(bytes: &[u8]) -> Result<DecodedServerHello, WireCodecError> {
        decode::<ServerHelloFields<'static>>(bytes, MAX_HANDSHAKE_BYTES).map(DecodedServerHello)
    }

    pub(crate) fn encode_rpc_request(
        id: u64,
        method: &str,
        params: &Value,
        binary_length: Option<u32>,
    ) -> Result<Vec<u8>, WireCodecError> {
        encode(
            &RpcRequestFields {
                id,
                method: Cow::Borrowed(method),
                params: Cow::Borrowed(params),
                binary_length,
            },
            MAX_JSON_FRAME_BYTES,
        )
    }

    pub(crate) fn decode_rpc_request(bytes: &[u8]) -> Result<DecodedRpcRequest, WireCodecError> {
        decode::<RpcRequestFields<'static>>(bytes, MAX_JSON_FRAME_BYTES).map(DecodedRpcRequest)
    }

    pub(crate) fn encode_rpc_success(
        id: u64,
        result: &Value,
        binary_length: Option<u32>,
    ) -> Result<Vec<u8>, WireCodecError> {
        encode(
            &RpcResponseFields {
                id,
                ok: true,
                result: Some(Cow::Borrowed(result)),
                error: None,
                binary_length,
            },
            MAX_JSON_FRAME_BYTES,
        )
    }

    pub(crate) fn encode_rpc_error(
        id: u64,
        error: &CowshedError,
    ) -> Result<Vec<u8>, WireCodecError> {
        encode(
            &RpcResponseFields {
                id,
                ok: false,
                result: None,
                error: Some(Cow::Borrowed(error)),
                binary_length: None,
            },
            MAX_JSON_FRAME_BYTES,
        )
    }

    pub(crate) fn decode_rpc_response(bytes: &[u8]) -> Result<DecodedRpcResponse, WireCodecError> {
        decode::<RpcResponseFields<'static>>(bytes, MAX_JSON_FRAME_BYTES).map(DecodedRpcResponse)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::error::ErrorCode;
        use serde_json::json;

        fn repo() -> RepoId {
            RepoId::parse("acme/widget").expect("repo id")
        }

        #[test]
        fn directional_hello_codecs_share_one_strict_schema() {
            let client = encode_client_hello("nonce").expect("encode client hello");
            assert_eq!(
                decode_client_hello(&client)
                    .expect("decode client hello")
                    .into_parts(),
                (HANDSHAKE_VERSION, "nonce".into())
            );

            let server = encode_server_hello("nonce", &repo()).expect("encode server hello");
            assert_eq!(
                decode_server_hello(&server)
                    .expect("decode server hello")
                    .into_parts(),
                (HANDSHAKE_VERSION, "nonce".into(), repo())
            );
        }

        #[test]
        fn directional_rpc_codecs_share_one_strict_schema() {
            let params = json!({"repoId": "acme/widget"});
            let request =
                encode_rpc_request(7, "project.list", &params, None).expect("encode request");
            let request_value: Value = serde_json::from_slice(&request).expect("request JSON");
            assert!(request_value.get("binaryLength").is_none());
            assert_eq!(
                decode_rpc_request(&request)
                    .expect("decode request")
                    .into_parts(),
                (7, "project.list".into(), params, None)
            );

            let result = json!({"healthy": true});
            let success = encode_rpc_success(7, &result, Some(4)).expect("encode success");
            assert_eq!(
                decode_rpc_response(&success)
                    .expect("decode success")
                    .into_parts(),
                (7, true, Some(result), None, Some(4))
            );

            let error = CowshedError::new(ErrorCode::Conflict, "stale", "retry");
            let failure = encode_rpc_error(8, &error).expect("encode failure");
            assert_eq!(
                decode_rpc_response(&failure)
                    .expect("decode failure")
                    .into_parts(),
                (8, false, None, Some(error), None)
            );
        }

        #[test]
        fn all_directional_decoders_reject_unknown_fields() {
            assert!(decode_client_hello(br#"{"version":1,"nonce":"n","extra":true}"#).is_err());
            assert!(
                decode_server_hello(
                    br#"{"version":1,"nonce":"n","repoId":"acme/widget","extra":true}"#
                )
                .is_err()
            );
            assert!(
                decode_rpc_request(br#"{"id":1,"method":"project.list","params":{},"extra":true}"#)
                    .is_err()
            );
            assert!(
                decode_rpc_response(
                    br#"{"id":1,"ok":true,"result":{},"error":null,"binaryLength":null,"extra":true}"#
                )
                .is_err()
            );
        }

        #[test]
        fn bounded_codec_rejects_before_exceeding_its_output_limit() {
            let value = ClientHelloFields {
                version: HANDSHAKE_VERSION,
                nonce: Cow::Borrowed("long-nonce"),
            };
            assert!(matches!(
                encode(&value, 8),
                Err(WireCodecError::TooLarge { maximum: 8 })
            ));
            assert!(matches!(
                decode::<ClientHelloFields<'static>>(b"{}", 1),
                Err(WireCodecError::TooLarge { maximum: 1 })
            ));
            assert!(matches!(
                decode::<ClientHelloFields<'static>>(b"", MAX_HANDSHAKE_BYTES),
                Err(WireCodecError::Empty)
            ));
        }
    }
}

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
    let hello = codec::decode_client_hello(&hello).map_err(|error| {
        protocol_error(format!("controller handshake request is invalid: {error}"))
    })?;
    let (version, nonce) = hello.into_parts();
    validate_hello(version, &nonce)?;
    let response = codec::encode_server_hello(&nonce, authority.repo_id()).map_err(|error| {
        if error.is_too_large() {
            protocol_error("controller handshake response has invalid length")
        } else {
            protocol_error(format!("controller handshake encoding failed: {error}"))
        }
    })?;
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
        let request = codec::decode_rpc_request(&frame).map_err(|error| {
            protocol_error(format!("controller RPC request is invalid: {error}"))
        })?;
        if request.id() != next_id {
            let error =
                protocol_error("controller RPC request id was replayed or arrived out of order");
            write_rpc_error(&mut stream, request.id(), &error).await?;
            return Err(error);
        }
        next_id = next_id
            .checked_add(1)
            .ok_or_else(|| protocol_error("controller RPC request id overflowed"))?;

        if let Err(error) = validate_request(&authority, &request) {
            write_rpc_error(&mut stream, request.id(), &error).await?;
            if error.code == ErrorCode::Integrity {
                return Err(error);
            }
            continue;
        }

        let download_offset = request
            .params()
            .get("offset")
            .and_then(Value::as_u64)
            .filter(|_| request.method() == "job.logs");
        let (request_id, request_method, request_params, binary_length) = request.into_parts();

        let upload = match binary_length {
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

        let response = router.route(authority.clone(), request_method, request_params, upload);
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
                            write_rpc_error(&mut stream, request_id, &error).await?;
                            return Err(error);
                        }
                    }
                    (Some(_), None) => {
                        let error =
                            protocol_error("controller router omitted the requested raw-byte lane");
                        write_rpc_error(&mut stream, request_id, &error).await?;
                        return Err(error);
                    }
                    (None, Some(_)) => {
                        let error = protocol_error(
                            "controller router attempted a second or unsolicited raw-byte lane",
                        );
                        write_rpc_error(&mut stream, request_id, &error).await?;
                        return Err(error);
                    }
                    (None, None) => {}
                }
                write_rpc_success(&mut stream, request_id, &result, binary.as_ref()).await?;
                if let Some(binary) = binary {
                    write_binary_frame(&mut stream, &binary).await?;
                }
            }
            Err(error) => write_rpc_error(&mut stream, request_id, &error).await?,
        }
    }
}

fn validate_hello(version: u32, nonce: &str) -> Result<()> {
    if version != HANDSHAKE_VERSION {
        return Err(protocol_error(
            "controller handshake protocol version did not match",
        ));
    }
    if nonce.len() != 64
        || !nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(protocol_error("controller handshake nonce is invalid"));
    }
    Ok(())
}

fn validate_request(
    authority: &ConnectionAuthority,
    request: &codec::DecodedRpcRequest,
) -> Result<()> {
    if !CAPABILITY_METHODS.contains(&request.method()) {
        return Err(authority_error(format!(
            "controller method is not in the capability allowlist: {}",
            request.method()
        )));
    }
    let params = request.params().as_object().ok_or_else(|| {
        CowshedError::usage(
            "controller RPC params must be a JSON object",
            "send the exact parameters required by the capability method",
        )
    })?;

    match authority {
        ConnectionAuthority::Coordinator { repo_id } => {
            if request.method() != "project.open" {
                require_string(params, "repoId", repo_id.as_str())?;
            }
        }
        ConnectionAuthority::Worker {
            repo_id,
            workspace,
            workspace_incarnation,
        } => {
            if !WORKER_METHODS.contains(&request.method()) {
                return Err(authority_error(format!(
                    "worker authority cannot call coordinator method {}",
                    request.method()
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

    if request.method() == "job.logs" && params.get("offset").and_then(Value::as_u64).is_none() {
        return Err(CowshedError::usage(
            "job.logs offset must be an unsigned integer",
            "send the offset returned by the preceding raw-byte frame",
        ));
    }

    match request.binary_length() {
        Some(length) if usize::try_from(length).unwrap_or(usize::MAX) > MAX_BINARY_FRAME_BYTES => {
            Err(protocol_error(
                "controller RPC binary request exceeds the 64 KiB frame limit",
            ))
        }
        Some(_) if !UPLOAD_METHODS.contains(&request.method()) => Err(protocol_error(
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
    let response = codec::encode_rpc_success(id, result, binary_length).map_err(|error| {
        if error.is_too_large() {
            protocol_error("controller RPC response has invalid length")
        } else {
            protocol_error(format!("controller RPC response encoding failed: {error}"))
        }
    })?;
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
    let response = codec::encode_rpc_error(id, error).map_err(|encoding| {
        if encoding.is_too_large() {
            protocol_error("controller RPC response has invalid length")
        } else {
            protocol_error(format!(
                "controller RPC error response encoding failed: {encoding}"
            ))
        }
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
