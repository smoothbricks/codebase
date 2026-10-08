use super::dto::StepReport;
use super::frame;
use super::operations::{
    self, Lane, LogsChunk, Operation, OperationRequest, ProjectOpen, Scope, decode_result,
};
use super::peer_credentials::PeerCredentialsError;
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
use crate::repository::RepoId;
use crate::timing::StepSink;
use async_trait::async_trait;
use bytes::Bytes;
use codec::Demand;
use serde::Deserialize;
use serde_json::Value;
use std::num::NonZeroUsize;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

pub const HANDSHAKE_VERSION: u32 = 1;
pub const MAX_HANDSHAKE_BYTES: usize = 4096;
pub const MAX_JSON_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BINARY_FRAME_BYTES: usize = 64 * 1024;

pub(crate) mod codec {
    use super::{
        CowshedError, HANDSHAKE_VERSION, MAX_HANDSHAKE_BYTES, MAX_JSON_FRAME_BYTES, RepoId, Value,
    };
    use crate::api::dto::StepReport;
    use serde::de::DeserializeOwned;
    use serde::{Deserialize, Serialize};
    use serde_json::value::RawValue;
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

    /// One schema for both directions: a client frames its operation's pre-serialized params
    /// verbatim (`P = &RawValue`), and the controller decodes them as JSON (`P = Value`).
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct RpcRequestFields<'a, P> {
        id: u64,
        method: Cow<'a, str>,
        params: P,
        #[serde(skip_serializing_if = "Option::is_none")]
        binary_length: Option<u32>,
        /// The caller asks to hear the call's lifecycle steps as step frames ahead of its answer.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        steps: bool,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct RpcResponseFields<'a> {
        id: u64,
        ok: bool,
        #[serde(default)]
        result: PresentResult<Cow<'a, Value>>,
        error: Option<Cow<'a, CowshedError>>,
        binary_length: Option<u32>,
    }

    /// Unlike `Option`, a present JSON null stays present; only an omitted result is missing.
    #[derive(Debug, Serialize)]
    #[serde(transparent)]
    struct PresentResult<T>(Option<T>);

    impl<T> Default for PresentResult<T> {
        fn default() -> Self {
            Self(None)
        }
    }

    impl<'de, T: Deserialize<'de>> Deserialize<'de> for PresentResult<T> {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            T::deserialize(deserializer).map(|value| Self(Some(value)))
        }
    }

    /// One step of a call that asked for its steps, sent before the call's answer. Only a request
    /// that set `steps` ever gets one, so a client that never asks never reads this shape.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct RpcStepFields<'a> {
        id: u64,
        step: Cow<'a, StepReport>,
    }

    /// What a caller asks of a stream-lane call it opened: its next event, or its end. The
    /// request that opens the call is its first demand.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub(crate) enum Demand {
        Next,
        Close,
    }

    /// A caller's demand on a stream-lane call it opened.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct RpcDemandFields {
        id: u64,
        demand: Demand,
    }

    /// One event of a stream-lane call: the answer to one demand that is not the call's end.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct RpcEventFields<'a> {
        id: u64,
        event: Cow<'a, Value>,
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
    pub(crate) struct DecodedRpcRequest(RpcRequestFields<'static, Value>);

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

        /// Whether the caller asked to hear the call's lifecycle steps.
        pub(crate) const fn steps(&self) -> bool {
            self.0.steps
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
                self.0.result.0.map(Cow::into_owned),
                self.0.error.map(Cow::into_owned),
                self.0.binary_length,
            )
        }
    }

    /// What a client reads off the connection: one step of a call that asked for its steps, one
    /// event of a stream-lane call, or a call's answer.
    #[derive(Debug)]
    pub(crate) enum DecodedServerFrame {
        Step { id: u64, report: StepReport },
        Event { id: u64, event: Value },
        Response(DecodedRpcResponse),
    }

    /// What a controller reads off the connection: a call, or a demand on a stream it opened.
    #[derive(Debug)]
    pub(crate) enum DecodedClientFrame {
        Request(DecodedRpcRequest),
        Demand { id: u64, demand: Demand },
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

    /// `params` is the request already serialized by its declared operation, framed verbatim.
    pub(crate) fn encode_rpc_request(
        id: u64,
        method: &str,
        params: &RawValue,
        binary_length: Option<u32>,
        steps: bool,
    ) -> Result<Vec<u8>, WireCodecError> {
        encode(
            &RpcRequestFields {
                id,
                method: Cow::Borrowed(method),
                params,
                binary_length,
                steps,
            },
            MAX_JSON_FRAME_BYTES,
        )
    }

    /// A request decodes as one; only a frame that is not one is read as a demand. Neither shape
    /// accepts the other's fields, and a frame that is neither reports why it is not a request.
    pub(crate) fn decode_client_frame(bytes: &[u8]) -> Result<DecodedClientFrame, WireCodecError> {
        match decode::<RpcRequestFields<'static, Value>>(bytes, MAX_JSON_FRAME_BYTES) {
            Ok(request) => Ok(DecodedClientFrame::Request(DecodedRpcRequest(request))),
            Err(not_a_request) => decode::<RpcDemandFields>(bytes, MAX_JSON_FRAME_BYTES)
                .map(|fields| DecodedClientFrame::Demand {
                    id: fields.id,
                    demand: fields.demand,
                })
                .map_err(|_| not_a_request),
        }
    }

    pub(crate) fn encode_rpc_demand(id: u64, demand: Demand) -> Result<Vec<u8>, WireCodecError> {
        encode(&RpcDemandFields { id, demand }, MAX_JSON_FRAME_BYTES)
    }

    pub(crate) fn encode_rpc_event(id: u64, event: &Value) -> Result<Vec<u8>, WireCodecError> {
        encode(
            &RpcEventFields {
                id,
                event: Cow::Borrowed(event),
            },
            MAX_JSON_FRAME_BYTES,
        )
    }

    pub(crate) fn encode_rpc_step(id: u64, report: &StepReport) -> Result<Vec<u8>, WireCodecError> {
        encode(
            &RpcStepFields {
                id,
                step: Cow::Borrowed(report),
            },
            MAX_JSON_FRAME_BYTES,
        )
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
                result: PresentResult(Some(Cow::Borrowed(result))),
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
                result: PresentResult(None),
                error: Some(Cow::Borrowed(error)),
                binary_length: None,
            },
            MAX_JSON_FRAME_BYTES,
        )
    }

    /// An answer decodes as one; only a frame that is not one is read as a step, then as an
    /// event. No shape accepts another's fields, so no frame is two, and a frame that is none
    /// reports why it is not an answer.
    pub(crate) fn decode_server_frame(bytes: &[u8]) -> Result<DecodedServerFrame, WireCodecError> {
        let not_an_answer = match decode::<RpcResponseFields<'static>>(bytes, MAX_JSON_FRAME_BYTES)
        {
            Ok(response) => return Ok(DecodedServerFrame::Response(DecodedRpcResponse(response))),
            Err(not_an_answer) => not_an_answer,
        };
        if let Ok(fields) = decode::<RpcStepFields<'static>>(bytes, MAX_JSON_FRAME_BYTES) {
            return Ok(DecodedServerFrame::Step {
                id: fields.id,
                report: fields.step.into_owned(),
            });
        }
        decode::<RpcEventFields<'static>>(bytes, MAX_JSON_FRAME_BYTES)
            .map(|fields| DecodedServerFrame::Event {
                id: fields.id,
                event: fields.event.into_owned(),
            })
            .map_err(|_| not_an_answer)
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

        fn response(frame: &[u8]) -> (u64, bool, Option<Value>, Option<CowshedError>, Option<u32>) {
            match decode_server_frame(frame).expect("decode server frame") {
                DecodedServerFrame::Response(response) => response.into_parts(),
                DecodedServerFrame::Step { id, report } => {
                    panic!("call {id}'s answer decoded as step {report:?}")
                }
                DecodedServerFrame::Event { id, event } => {
                    panic!("call {id}'s answer decoded as event {event}")
                }
            }
        }

        fn decode_request(frame: &[u8]) -> Result<DecodedRpcRequest, WireCodecError> {
            decode_client_frame(frame).map(|frame| match frame {
                DecodedClientFrame::Request(request) => request,
                DecodedClientFrame::Demand { id, demand } => {
                    panic!("a request decoded as demand {demand:?} on {id}")
                }
            })
        }

        #[test]
        fn directional_rpc_codecs_share_one_strict_schema() {
            let params = json!({"repoId": "acme/widget"});
            let raw = serde_json::value::to_raw_value(&params).expect("raw params");
            let request =
                encode_rpc_request(7, "project.list", &raw, None, false).expect("encode request");
            let request_value: Value = serde_json::from_slice(&request).expect("request JSON");
            assert!(request_value.get("binaryLength").is_none());
            assert!(
                request_value.get("steps").is_none(),
                "a call that does not ask for its steps sends what a controller without them reads"
            );
            let decoded = decode_request(&request).expect("decode request");
            assert!(!decoded.steps());
            assert_eq!(
                (
                    decoded.id(),
                    decoded.method(),
                    decoded.params(),
                    decoded.binary_length()
                ),
                (7, "project.list", &params, None)
            );

            let result = json!({"healthy": true});
            let success = encode_rpc_success(7, &result, Some(4)).expect("encode success");
            assert_eq!(response(&success), (7, true, Some(result), None, Some(4)));

            let error = CowshedError::new(ErrorCode::Conflict, "stale", "retry");
            let failure = encode_rpc_error(8, &error).expect("encode failure");
            assert_eq!(
                response(&failure),
                (8, false, Some(Value::Null), Some(error), None)
            );
        }

        #[test]
        fn a_present_null_result_is_not_a_missing_result() {
            let success = encode_rpc_success(7, &Value::Null, None).expect("encode null success");
            assert_eq!(response(&success), (7, true, Some(Value::Null), None, None));
            assert_eq!(
                response(br#"{"id":8,"ok":true,"error":null,"binaryLength":null}"#),
                (8, true, None, None, None)
            );
        }

        #[test]
        fn a_call_that_asks_for_its_steps_reads_them_as_step_frames() {
            let raw = serde_json::value::to_raw_value(&json!({})).expect("raw params");
            let request = encode_rpc_request(9, "coordinator.create", &raw, None, true)
                .expect("encode request");
            assert!(decode_request(&request).expect("decode request").steps());

            let report = StepReport::Started {
                step: 3,
                parent: Some(1),
                scope: "apfs".into(),
                name: "canonical/mount".into(),
            };
            let frame = encode_rpc_step(9, &report).expect("encode step");
            match decode_server_frame(&frame).expect("decode step") {
                DecodedServerFrame::Step {
                    id,
                    report: decoded,
                } => {
                    assert_eq!((id, decoded), (9, report));
                }
                other => panic!("a step decoded as another frame: {other:?}"),
            }
        }

        #[test]
        fn a_stream_call_s_demands_and_events_have_frames_of_their_own() {
            for demand in [Demand::Next, Demand::Close] {
                let frame = encode_rpc_demand(4, demand).expect("encode demand");
                match decode_client_frame(&frame).expect("decode demand") {
                    DecodedClientFrame::Demand {
                        id,
                        demand: decoded,
                    } => {
                        assert_eq!((id, decoded), (4, demand));
                    }
                    DecodedClientFrame::Request(request) => {
                        panic!("a demand decoded as request {}", request.method())
                    }
                }
            }
            let event = json!({"jobId": 7, "wallMs": 12});
            let frame = encode_rpc_event(4, &event).expect("encode event");
            match decode_server_frame(&frame).expect("decode event") {
                DecodedServerFrame::Event { id, event: decoded } => {
                    assert_eq!((id, decoded), (4, event));
                }
                other => panic!("an event decoded as another frame: {other:?}"),
            }
            assert!(decode_client_frame(br#"{"id":4,"demand":"next","extra":true}"#).is_err());
            assert!(decode_client_frame(br#"{"id":4,"demand":"rewind"}"#).is_err());
            assert!(decode_server_frame(br#"{"id":4,"event":{},"extra":true}"#).is_err());
            assert!(
                decode_server_frame(br#"{"id":4,"event":{},"step":{"event":"ended","step":0}}"#)
                    .is_err(),
                "a frame is an event or a step, never both"
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
                decode_request(br#"{"id":1,"method":"project.list","params":{},"extra":true}"#)
                    .is_err()
            );
            assert!(
                decode_server_frame(
                    br#"{"id":1,"ok":true,"result":{},"error":null,"binaryLength":null,"extra":true}"#
                )
                .is_err()
            );
            assert!(
                decode_server_frame(br#"{"id":1,"step":{"event":"ended","step":0,"extra":true}}"#)
                    .is_err()
            );
            assert!(
                decode_server_frame(
                    br#"{"id":1,"ok":true,"result":{},"error":null,"binaryLength":null,"step":{"event":"ended","step":0}}"#
                )
                .is_err(),
                "a frame is an answer or a step, never both"
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

/// A request already authenticated, fenced to its immutable connection authority, and decoded as
/// its declared operation.
#[derive(Debug)]
pub struct RouterRequest {
    authority: ConnectionAuthority,
    operation: OperationRequest,
    upload: Option<Bytes>,
    steps: Option<StepSink>,
}

impl RouterRequest {
    pub fn authority(&self) -> &ConnectionAuthority {
        &self.authority
    }

    pub fn method(&self) -> &'static str {
        self.operation.method()
    }

    pub fn operation(&self) -> &OperationRequest {
        &self.operation
    }

    pub fn upload(&self) -> Option<&Bytes> {
        self.upload.as_ref()
    }

    /// Where the call's lifecycle steps are reported, when its caller asked to hear them.
    pub fn steps(&self) -> Option<&StepSink> {
        self.steps.as_ref()
    }

    pub fn into_parts(
        self,
    ) -> (
        ConnectionAuthority,
        OperationRequest,
        Option<Bytes>,
        Option<StepSink>,
    ) {
        (self.authority, self.operation, self.upload, self.steps)
    }
}

/// The events of a stream-lane call, taken one for each demand of its caller.
#[async_trait]
pub trait EventSource: Send {
    /// The next event, waiting for it; `None` once the stream ended. An error ends it too.
    async fn next(&mut self) -> Option<Result<Value>>;
}

/// The router's answer to one call.
pub struct RouterResponse {
    body: RouterBody,
}

/// What a call is answered with: its JSON result and optional single bounded raw-byte lane, or,
/// for a stream-lane call, the events its caller demands.
pub enum RouterBody {
    Answer {
        result: Value,
        binary: Option<Bytes>,
    },
    Events(Box<dyn EventSource>),
}

impl RouterResponse {
    pub fn json(result: Value) -> Self {
        Self {
            body: RouterBody::Answer {
                result,
                binary: None,
            },
        }
    }

    pub fn binary(result: Value, binary: Bytes) -> Result<Self> {
        if binary.len() > MAX_BINARY_FRAME_BYTES {
            return Err(CowshedError::internal(
                "controller router binary response exceeds the 64 KiB frame limit",
            ));
        }
        Ok(Self {
            body: RouterBody::Answer {
                result,
                binary: Some(binary),
            },
        })
    }

    pub fn events(source: Box<dyn EventSource>) -> Self {
        Self {
            body: RouterBody::Events(source),
        }
    }

    pub fn into_body(self) -> RouterBody {
        self.body
    }

    /// The one answer of a call that is no stream: its JSON result and its raw-byte frame, if
    /// any. Events where one answer was due are an internal error.
    pub fn into_answer(self) -> Result<(Value, Option<Bytes>)> {
        match self.body {
            RouterBody::Answer { result, binary } => Ok((result, binary)),
            RouterBody::Events(_) => Err(CowshedError::internal(
                "controller router answered with events where one answer was due",
            )),
        }
    }
}

impl std::fmt::Debug for RouterResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.body {
            RouterBody::Answer { result, binary } => formatter
                .debug_struct("RouterResponse")
                .field("result", result)
                .field("binary", binary)
                .finish(),
            RouterBody::Events(_) => formatter
                .debug_struct("RouterResponse")
                .field("events", &"…")
                .finish(),
        }
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

    /// Route one call; `steps`, when present, hears its lifecycle steps as they run.
    pub async fn route(
        &self,
        authority: ConnectionAuthority,
        operation: OperationRequest,
        upload: Option<Bytes>,
        steps: Option<StepSink>,
    ) -> Result<RouterResponse> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(RouterCommand {
                request: RouterRequest {
                    authority,
                    operation,
                    upload,
                    steps,
                },
                reply: RouterReply(reply),
            })
            .await
            .map_err(|_| router_closed("controller router actor channel closed"))?;
        response
            .await
            .map_err(|_| router_closed("controller router actor stopped before replying"))?
    }

    /// Route one call of a declared operation from inside the controller process and decode its
    /// declared result.
    pub async fn call<O: Operation>(
        &self,
        authority: ConnectionAuthority,
        request: O::Request,
    ) -> Result<O::Result> {
        let (result, _) = self
            .route(authority, O::request(request), None, None)
            .await?
            .into_answer()?;
        decode_result::<O>(result)
    }
}

/// Serves one inherited stream descriptor until a clean disconnect or a fatal protocol failure.
///
/// Requests arrive in id order and are answered as each completes, not in arrival order: a
/// request that waits on a job (its end, its next output) must not hold the answers to the
/// requests behind it. Each answer, with its binary frame, is written whole under the writer
/// lock. Frames are read for as long as the connection is open, so a demand on an open stream is
/// always read. At most [`MAX_IN_FLIGHT_REQUESTS`] calls that are not streams are open at once;
/// a call past that waits, unrouted, until one completes, at most [`MAX_WAITING_REQUESTS`] of
/// them, and a call past both is refused. A stream-lane call is open until its caller ends it, so
/// it counts against [`MAX_OPEN_STREAMS`] instead, and a stream request past that is refused.
///
/// Dropping this future owns only connection state: its unanswered requests are abandoned, its
/// streams end, and routed jobs remain owned by the router actor.
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

    let (reader, writer) = stream.into_split();
    let writer = Arc::new(tokio::sync::Mutex::new(writer));
    let mut answers = tokio::task::JoinSet::new();
    // Where each open stream-lane call's demands go, by call id: a subset of `answers`, entered
    // when its answer starts and removed when that answer ends.
    let mut streams = std::collections::HashMap::<u64, mpsc::UnboundedSender<Demand>>::new();
    // Ordinary calls admitted past the open-call cap, started in order as open ones complete.
    let mut waiting = std::collections::VecDeque::<Admitted>::new();
    let mut incoming = std::pin::pin!(next_frame(reader));
    // Set once a request ends the connection: no further request is read, and the loop ends
    // with the answer that reports it.
    let mut closing = false;
    let mut next_id = 1_u64;
    loop {
        tokio::select! {
            (reader, frame) = incoming.as_mut(), if !closing => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                incoming.set(next_frame(reader));
                let (request, upload) = match frame {
                    ClientFrame::Request(request, upload) => (request, upload),
                    ClientFrame::Demand { id, demand } => {
                        let delivered = streams
                            .get(&id)
                            .is_some_and(|stream| stream.send(demand).is_ok());
                        // A close may cross the end its stream answered, so one for a call
                        // that was sent and has ended is no error; any other demand needs an
                        // open stream to answer it.
                        if !delivered && (demand == Demand::Next || id >= next_id) {
                            closing = true;
                            let error = protocol_error(
                                "controller RPC demand names no open stream-lane call",
                            );
                            answers.spawn(refuse(Arc::clone(&writer), id, error, true));
                        }
                        continue;
                    }
                };
                let id = request.id();
                if id != next_id {
                    closing = true;
                    let error = protocol_error(
                        "controller RPC request id was replayed or arrived out of order",
                    );
                    answers.spawn(refuse(Arc::clone(&writer), id, error, true));
                    continue;
                }
                next_id = next_id
                    .checked_add(1)
                    .ok_or_else(|| protocol_error("controller RPC request id overflowed"))?;
                let (operation, lane) = match validate_request(&authority, &request) {
                    Ok(validated) => validated,
                    Err(error) => {
                        let fatal = error.code == ErrorCode::Integrity;
                        closing |= fatal;
                        answers.spawn(refuse(Arc::clone(&writer), id, error, fatal));
                        continue;
                    }
                };
                let mut call = Admitted {
                    id,
                    steps: request.steps(),
                    operation,
                    upload,
                    demands: None,
                };
                let open = answers.len() - streams.len();
                let refusal = if lane == Lane::Stream {
                    if streams.len() < MAX_OPEN_STREAMS {
                        let (demand, demands) = mpsc::unbounded_channel();
                        streams.insert(id, demand);
                        call.demands = Some(demands);
                        None
                    } else {
                        Some(CowshedError::conflict(
                            format!("this connection already has {MAX_OPEN_STREAMS} open streams"),
                            "end a stream before opening another",
                        ))
                    }
                } else if open < MAX_IN_FLIGHT_REQUESTS {
                    None
                } else if waiting.len() < MAX_WAITING_REQUESTS {
                    waiting.push_back(call);
                    continue;
                } else {
                    Some(CowshedError::conflict(
                        format!(
                            "this connection already has {MAX_IN_FLIGHT_REQUESTS} open calls and \
                             {MAX_WAITING_REQUESTS} waiting"
                        ),
                        "wait for an open call to complete before sending another",
                    ))
                };
                match refusal {
                    Some(error) => {
                        answers.spawn(refuse(Arc::clone(&writer), id, error, false));
                    }
                    None => start(&mut answers, &writer, &router, &authority, call),
                }
            }
            Some(outcome) = answers.join_next() => {
                let (id, outcome) = outcome.map_err(|error| {
                    CowshedError::internal(format!("controller RPC answer task failed: {error}"))
                })?;
                streams.remove(&id);
                outcome?;
                while answers.len() - streams.len() < MAX_IN_FLIGHT_REQUESTS
                    && let Some(call) = waiting.pop_front()
                {
                    start(&mut answers, &writer, &router, &authority, call);
                }
            }
            // Reading stops only once the connection is closing, which leaves the answer that
            // reports why pending.
            else => {
                return Err(CowshedError::internal(
                    "controller connection has no request to read and none to answer",
                ));
            }
        }
    }
}

/// A validated call, ready to route: its id, whether it asked for its steps, its decoded request
/// and upload frame, and, for a stream-lane call, where its demands arrive.
struct Admitted {
    id: u64,
    steps: bool,
    operation: OperationRequest,
    upload: Option<Bytes>,
    demands: Option<mpsc::UnboundedReceiver<Demand>>,
}

/// Routes `call` and writes its answer on a task of its own.
fn start(
    answers: &mut tokio::task::JoinSet<(u64, Result<()>)>,
    writer: &ConnectionWriter,
    router: &RouterHandle,
    authority: &ConnectionAuthority,
    call: Admitted,
) {
    answers.spawn(answer(
        Arc::clone(writer),
        router.clone(),
        authority.clone(),
        call.id,
        call.steps,
        call.operation,
        call.upload,
        call.demands,
    ));
}

/// Calls other than streams one connection may have open at once.
const MAX_IN_FLIGHT_REQUESTS: usize = 64;

/// Calls other than streams one connection may have waiting for an open one to complete.
const MAX_WAITING_REQUESTS: usize = 64;

/// Stream-lane calls one connection may have open at once.
const MAX_OPEN_STREAMS: usize = 64;

type ConnectionWriter = Arc<tokio::sync::Mutex<tokio::net::unix::OwnedWriteHalf>>;

/// One frame a client sends: a request with its upload frame, or a demand on an open stream.
enum ClientFrame {
    Request(codec::DecodedRpcRequest, Option<Bytes>),
    Demand { id: u64, demand: Demand },
}

/// The next frame, or `None` at a clean disconnect. The reader comes back with it, so the
/// connection can ask for the one after.
async fn next_frame(
    mut reader: tokio::net::unix::OwnedReadHalf,
) -> (tokio::net::unix::OwnedReadHalf, Result<Option<ClientFrame>>) {
    let frame = read_client_frame(&mut reader).await;
    (reader, frame)
}

async fn read_client_frame(
    reader: &mut tokio::net::unix::OwnedReadHalf,
) -> Result<Option<ClientFrame>> {
    let Some(frame) =
        read_optional_frame(reader, MAX_JSON_FRAME_BYTES, "controller RPC request").await?
    else {
        return Ok(None);
    };
    let request = match codec::decode_client_frame(&frame)
        .map_err(|error| protocol_error(format!("controller RPC request is invalid: {error}")))?
    {
        codec::DecodedClientFrame::Request(request) => request,
        codec::DecodedClientFrame::Demand { id, demand } => {
            return Ok(Some(ClientFrame::Demand { id, demand }));
        }
    };
    // The upload frame is read with its request, so a request refused below never leaves it
    // to be misread as the next one. A declared length beyond any frame is left unread:
    // validation refuses it, and that ends the connection.
    let upload = match request.binary_length().map(usize::try_from) {
        Some(Ok(length)) if length <= MAX_BINARY_FRAME_BYTES => {
            Some(read_binary_frame(reader, length).await?)
        }
        Some(_) | None => None,
    };
    Ok(Some(ClientFrame::Request(request, upload)))
}

/// Answer one request with an error; `Err` ends the connection when the error is `fatal`.
async fn refuse(
    writer: ConnectionWriter,
    id: u64,
    error: CowshedError,
    fatal: bool,
) -> (u64, Result<()>) {
    let outcome = match write_rpc_error(&mut *writer.lock().await, id, &error).await {
        Ok(()) if fatal => Err(error),
        written => written,
    };
    (id, outcome)
}

/// Route one request and write its answer -- for a stream-lane call, the events `demands` asks
/// for, then its end; `Err` ends the connection.
#[allow(clippy::too_many_arguments)]
async fn answer(
    writer: ConnectionWriter,
    router: RouterHandle,
    authority: ConnectionAuthority,
    request_id: u64,
    steps: bool,
    operation: OperationRequest,
    upload: Option<Bytes>,
    demands: Option<mpsc::UnboundedReceiver<Demand>>,
) -> (u64, Result<()>) {
    let outcome = async {
        let download_offset = operation.download_offset();
        let response = if steps {
            route_reporting_steps(&writer, &router, authority, request_id, operation, upload)
                .await?
        } else {
            router.route(authority, operation, upload, None).await
        };
        let body = match response {
            Ok(response) => response.into_body(),
            Err(error) => {
                return write_rpc_error(&mut *writer.lock().await, request_id, &error).await;
            }
        };
        match (body, demands) {
            (RouterBody::Events(source), Some(demands)) => {
                stream_events(&writer, request_id, source, demands).await
            }
            (RouterBody::Answer { result, binary }, None) => {
                let mut writer = writer.lock().await;
                let writer = &mut *writer;
                let lane = match (download_offset, binary.as_ref()) {
                    (Some(offset), Some(bytes)) => {
                        validate_raw_response(&result, offset, bytes.len())
                    }
                    (Some(_), None) => Err(protocol_error(
                        "controller router omitted the requested raw-byte lane",
                    )),
                    (None, Some(_)) => Err(protocol_error(
                        "controller router attempted a second or unsolicited raw-byte lane",
                    )),
                    (None, None) => Ok(()),
                };
                if let Err(error) = lane {
                    write_rpc_error(writer, request_id, &error).await?;
                    return Err(error);
                }
                write_rpc_success(writer, request_id, &result, binary.as_ref()).await?;
                if let Some(binary) = binary {
                    write_binary_frame(writer, &binary).await?;
                }
                Ok(())
            }
            (RouterBody::Answer { .. }, Some(_)) => {
                let error = protocol_error("controller router answered a stream-lane call once");
                write_rpc_error(&mut *writer.lock().await, request_id, &error).await?;
                Err(error)
            }
            (RouterBody::Events(_), None) => {
                let error = protocol_error(
                    "controller router answered a call that is no stream with events",
                );
                write_rpc_error(&mut *writer.lock().await, request_id, &error).await?;
                Err(error)
            }
        }
    }
    .await;
    (request_id, outcome)
}

/// Answer each demand on a stream-lane call with its next event, or with the call's end: an
/// empty result once the events ended or the caller closed the call, or the error that ended
/// them. The request was the first demand. `Err` ends the connection.
async fn stream_events(
    writer: &ConnectionWriter,
    id: u64,
    mut source: Box<dyn EventSource>,
    mut demands: mpsc::UnboundedReceiver<Demand>,
) -> Result<()> {
    let ended = Value::Object(serde_json::Map::new());
    loop {
        let event = tokio::select! {
            biased;
            demand = demands.recv() => match demand {
                Some(Demand::Close) => {
                    return write_rpc_success(&mut *writer.lock().await, id, &ended, None).await;
                }
                Some(Demand::Next) => {
                    let error = protocol_error(
                        "controller RPC demanded a stream event while one was unanswered",
                    );
                    write_rpc_error(&mut *writer.lock().await, id, &error).await?;
                    return Err(error);
                }
                // The connection is gone, and nobody reads an answer.
                None => return Ok(()),
            },
            event = source.next() => event,
        };
        match event {
            Some(Ok(event)) => write_rpc_event(&mut *writer.lock().await, id, &event).await?,
            Some(Err(error)) => {
                return write_rpc_error(&mut *writer.lock().await, id, &error).await;
            }
            None => return write_rpc_success(&mut *writer.lock().await, id, &ended, None).await,
        }
        match demands.recv().await {
            Some(Demand::Next) => {}
            Some(Demand::Close) => {
                return write_rpc_success(&mut *writer.lock().await, id, &ended, None).await;
            }
            None => return Ok(()),
        }
    }
}

/// Route a call that asked for its steps, writing each step as it is reported, so the steps reach
/// the caller while the call runs and all of them precede its answer. `Err` is a failed write,
/// which ends the connection.
async fn route_reporting_steps(
    writer: &ConnectionWriter,
    router: &RouterHandle,
    authority: ConnectionAuthority,
    request_id: u64,
    operation: OperationRequest,
    upload: Option<Bytes>,
) -> Result<Result<RouterResponse>> {
    let (sender, mut reports) = mpsc::unbounded_channel();
    let routed = router.route(authority, operation, upload, Some(StepSink::new(sender)));
    let mut routed = std::pin::pin!(routed);
    loop {
        tokio::select! {
            biased;
            Some(report) = reports.recv() => {
                write_rpc_step(&mut *writer.lock().await, request_id, &report).await?;
            }
            response = &mut routed => {
                // Every step the call reported before it answered is already queued.
                while let Ok(report) = reports.try_recv() {
                    write_rpc_step(&mut *writer.lock().await, request_id, &report).await?;
                }
                return Ok(response);
            }
        }
    }
}

fn validate_hello(version: u32, nonce: &str) -> Result<()> {
    if version != HANDSHAKE_VERSION {
        return Err(protocol_error(
            "controller handshake protocol version did not match",
        ));
    }
    if nonce.len() != 64 || !super::dto::is_lowercase_hex(nonce) {
        return Err(protocol_error("controller handshake nonce is invalid"));
    }
    Ok(())
}

/// Checks a request against its declared operation and the connection's authority, then decodes
/// it. Fence and lane checks read the raw params, so a refused request is never decoded.
fn validate_request(
    authority: &ConnectionAuthority,
    request: &codec::DecodedRpcRequest,
) -> Result<(OperationRequest, Lane)> {
    let operation = operations::operation(request.method())
        .filter(|operation| operation.scope != Scope::Internal)
        .ok_or_else(|| {
            authority_error(format!(
                "controller method is not in the capability allowlist: {}",
                request.method()
            ))
        })?;
    let params = request.params().as_object().ok_or_else(|| {
        CowshedError::usage(
            "controller RPC params must be a JSON object",
            "send the exact parameters required by the capability method",
        )
    })?;

    match authority {
        ConnectionAuthority::Coordinator { repo_id } => {
            if operation.method != ProjectOpen::METHOD {
                require_string(params, "repoId", repo_id.as_str())?;
            }
        }
        ConnectionAuthority::Worker {
            repo_id,
            workspace,
            workspace_incarnation,
        } => {
            if operation.scope != Scope::Worker {
                return Err(authority_error(format!(
                    "worker authority cannot call coordinator method {}",
                    operation.method
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

    match request.binary_length() {
        Some(length) if usize::try_from(length).unwrap_or(usize::MAX) > MAX_BINARY_FRAME_BYTES => {
            return Err(protocol_error(
                "controller RPC binary request exceeds the 64 KiB frame limit",
            ));
        }
        Some(_) if operation.lane != Lane::Upload => {
            return Err(protocol_error(
                "controller RPC method does not accept a raw-byte upload lane",
            ));
        }
        Some(_) | None => {}
    }
    OperationRequest::decode(operation.method, request.params())
        .map(|decoded| (decoded, operation.lane))
}

/// A download's JSON half must be its declared envelope, and its `nextOffset` exactly the
/// request's offset plus the bytes framed after it.
fn validate_raw_response(result: &Value, offset: u64, length: usize) -> Result<()> {
    let chunk = LogsChunk::deserialize(result).map_err(|error| {
        protocol_error(format!(
            "controller router raw-byte metadata has an invalid envelope: {error}"
        ))
    })?;
    let length = u64::try_from(length)
        .map_err(|_| protocol_error("controller router raw-byte response length overflowed"))?;
    let expected = offset
        .checked_add(length)
        .ok_or_else(|| protocol_error("controller router raw-byte response offset overflowed"))?;
    if chunk.next_offset != expected {
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
    stream: &mut (impl AsyncRead + Unpin),
    maximum: usize,
    description: &'static str,
) -> Result<Vec<u8>> {
    read_optional_frame(stream, maximum, description)
        .await?
        .ok_or_else(|| connection_error(format!("{description} ended before a frame arrived")))
}

async fn read_optional_frame(
    stream: &mut (impl AsyncRead + Unpin),
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
    stream: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
    maximum: usize,
    description: &'static str,
) -> Result<()> {
    frame::write_frame(
        stream,
        bytes,
        maximum,
        || protocol_error(format!("{description} has invalid length")),
        |error| connection_error(format!("{description} write failed: {error}")),
    )
    .await
}

async fn read_binary_frame(
    stream: &mut (impl AsyncRead + Unpin),
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

async fn write_binary_frame(stream: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<()> {
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
    stream: &mut (impl AsyncWrite + Unpin),
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
    stream: &mut (impl AsyncWrite + Unpin),
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

async fn write_rpc_event(
    stream: &mut (impl AsyncWrite + Unpin),
    id: u64,
    event: &Value,
) -> Result<()> {
    let frame = codec::encode_rpc_event(id, event).map_err(|error| {
        if error.is_too_large() {
            protocol_error("controller RPC event has invalid length")
        } else {
            protocol_error(format!("controller RPC event encoding failed: {error}"))
        }
    })?;
    write_frame(stream, &frame, MAX_JSON_FRAME_BYTES, "controller RPC event").await
}

async fn write_rpc_step(
    stream: &mut (impl AsyncWrite + Unpin),
    id: u64,
    report: &StepReport,
) -> Result<()> {
    let frame = codec::encode_rpc_step(id, report)
        .map_err(|error| protocol_error(format!("controller RPC step encoding failed: {error}")))?;
    write_frame(stream, &frame, MAX_JSON_FRAME_BYTES, "controller RPC step").await
}

fn verify_peer(descriptor: &OwnedFd) -> Result<()> {
    frame::verify_peer(descriptor, |error| match error {
        PeerCredentialsError::SocketTypeSizeOverflow => {
            connection_error("socket type size does not fit socklen_t")
        }
        PeerCredentialsError::SocketTypeQueryFailed | PeerCredentialsError::NotStream => {
            connection_error("controller descriptor is not a stream socket")
        }
        PeerCredentialsError::PeerCredentialQueryFailed => {
            connection_error("controller descriptor peer does not match the current uid")
        }
    })
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
