#![cfg(unix)]

use async_trait::async_trait;
use bytes::Bytes;
use cowshed_core::api::operations::{Lane, OPERATIONS, OperationRequest, Scope};
use cowshed_core::api::server::{
    ConnectionAuthority, EventSource, HANDSHAKE_VERSION, MAX_BINARY_FRAME_BYTES,
    MAX_JSON_FRAME_BYTES, RouterHandle, RouterResponse, serve_controller_connection,
};
use cowshed_core::metadata::{WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::{CowshedError, ErrorCode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::os::fd::OwnedFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const NONCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// One canonical request per declared operation, fenced to [`worker_authority`].
const CORPUS: &str = include_str!("../src/api/operations.corpus.json");

/// The session name that asks the recording router for an unsolicited raw-byte lane.
const UNSOLICITED_BINARY_SESSION: &str = "unsolicited-binary";

#[derive(Debug)]
struct RecordedRequest {
    authority: ConnectionAuthority,
    method: &'static str,
    params: Value,
    upload: Option<Bytes>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServerHello {
    version: u32,
    nonce: String,
    repo_id: RepoId,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RpcResponse {
    id: u64,
    ok: bool,
    result: Option<Value>,
    error: Option<CowshedError>,
    binary_length: Option<u32>,
}

struct ClientResponse {
    envelope: RpcResponse,
    binary: Option<Vec<u8>>,
}

/// A stream with one event, then its end.
struct OneEvent(Option<Value>);

#[async_trait]
impl EventSource for OneEvent {
    async fn next(&mut self) -> Option<cowshed_core::Result<Value>> {
        self.0.take().map(Ok)
    }
}

struct TestClient {
    stream: tokio::net::UnixStream,
    next_id: u64,
}

impl TestClient {
    async fn connect(
        authority: ConnectionAuthority,
        router: RouterHandle,
    ) -> (Self, JoinHandle<cowshed_core::Result<()>>) {
        let expected_repo = authority.repo_id().clone();
        let (client, server) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        client.set_nonblocking(true).expect("nonblocking client");
        let stream = tokio::net::UnixStream::from_std(client).expect("Tokio client stream");
        let descriptor: OwnedFd = server.into();
        let task = tokio::spawn(serve_controller_connection(descriptor, authority, router));
        let mut client = Self { stream, next_id: 1 };
        let hello = client.handshake(HANDSHAKE_VERSION, NONCE).await;
        assert_eq!(hello.version, HANDSHAKE_VERSION);
        assert_eq!(hello.nonce, NONCE);
        assert_eq!(hello.repo_id, expected_repo);
        (client, task)
    }

    async fn handshake(&mut self, version: u32, nonce: &str) -> ServerHello {
        self.write_json(&json!({ "version": version, "nonce": nonce }))
            .await;
        let bytes = self.read_frame().await;
        serde_json::from_slice(&bytes).expect("strict server hello")
    }

    async fn request(
        &mut self,
        method: &str,
        params: Value,
        upload: Option<&[u8]>,
    ) -> ClientResponse {
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).expect("test request id");
        self.request_with_id(id, method, params, upload).await
    }

    async fn request_with_id(
        &mut self,
        id: u64,
        method: &str,
        params: Value,
        upload: Option<&[u8]>,
    ) -> ClientResponse {
        let binary_length = upload
            .map(<[u8]>::len)
            .map(u32::try_from)
            .transpose()
            .expect("test upload fits wire");
        self.write_json(&json!({
            "id": id,
            "method": method,
            "params": params,
            "binaryLength": binary_length,
        }))
        .await;
        if let Some(upload) = upload {
            self.write_binary(upload).await;
        }
        let envelope: RpcResponse =
            serde_json::from_slice(&self.read_frame().await).expect("strict RPC response");
        let binary = match envelope.binary_length {
            Some(length) => Some(self.read_binary(length).await),
            None => None,
        };
        ClientResponse { envelope, binary }
    }

    /// Opens a stream-lane call and takes its first event, then demands the next and returns
    /// both answers: the event frame and what the demand was answered with.
    async fn stream_request(&mut self, method: &str, params: Value) -> (Value, RpcResponse) {
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).expect("test request id");
        self.write_json(&json!({ "id": id, "method": method, "params": params }))
            .await;
        let event: Value =
            serde_json::from_slice(&self.read_frame().await).expect("an event frame");
        self.write_json(&json!({ "id": id, "demand": "next" }))
            .await;
        let end: RpcResponse =
            serde_json::from_slice(&self.read_frame().await).expect("strict RPC response");
        (event, end)
    }

    async fn write_json(&mut self, value: &Value) {
        let bytes = serde_json::to_vec(value).expect("test JSON");
        self.write_frame(&bytes).await;
    }

    async fn write_frame(&mut self, bytes: &[u8]) {
        let length = u32::try_from(bytes.len()).expect("test frame fits wire");
        self.stream
            .write_all(&length.to_be_bytes())
            .await
            .expect("frame header write");
        self.stream
            .write_all(bytes)
            .await
            .expect("frame body write");
    }

    async fn write_binary(&mut self, bytes: &[u8]) {
        self.write_frame(bytes).await;
    }

    async fn read_frame(&mut self) -> Vec<u8> {
        let length = self.stream.read_u32().await.expect("frame header read");
        let length = usize::try_from(length).expect("frame length fits platform");
        let mut bytes = vec![0_u8; length];
        self.stream
            .read_exact(&mut bytes)
            .await
            .expect("frame body read");
        bytes
    }

    async fn read_binary(&mut self, expected: u32) -> Vec<u8> {
        let actual = self.stream.read_u32().await.expect("binary header read");
        assert_eq!(actual, expected);
        let length = usize::try_from(actual).expect("binary length fits platform");
        let mut bytes = vec![0_u8; length];
        self.stream
            .read_exact(&mut bytes)
            .await
            .expect("binary body read");
        bytes
    }
}

fn repo() -> RepoId {
    RepoId::parse("acme/widget").expect("repo id")
}

fn other_repo() -> RepoId {
    RepoId::parse("other/widget").expect("other repo id")
}

fn workspace() -> WorkspaceName {
    WorkspaceName::new("feature").expect("workspace name")
}

fn incarnation() -> WorkspaceIncarnation {
    WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef").expect("incarnation")
}

fn other_incarnation() -> WorkspaceIncarnation {
    WorkspaceIncarnation::new("fedcba9876543210fedcba9876543210").expect("other incarnation")
}

fn coordinator_authority() -> ConnectionAuthority {
    ConnectionAuthority::Coordinator { repo_id: repo() }
}

fn worker_authority() -> ConnectionAuthority {
    ConnectionAuthority::Worker {
        repo_id: repo(),
        workspace: workspace(),
        workspace_incarnation: incarnation(),
    }
}

/// The corpus request of `method`.
fn params(method: &str) -> Value {
    let mut corpus: BTreeMap<String, Value> =
        serde_json::from_str(CORPUS).expect("the corpus is a JSON object of requests");
    corpus
        .remove(method)
        .unwrap_or_else(|| panic!("{method} has no corpus request"))
}

/// Methods a connection may name: every declared operation that is not internal.
fn connection_methods() -> impl Iterator<Item = &'static str> {
    OPERATIONS
        .iter()
        .filter(|operation| operation.scope != Scope::Internal)
        .map(|operation| operation.method)
}

fn is_worker_method(method: &str) -> bool {
    OPERATIONS
        .iter()
        .any(|operation| operation.method == method && operation.scope == Scope::Worker)
}

fn is_upload_method(method: &str) -> bool {
    OPERATIONS
        .iter()
        .any(|operation| operation.method == method && operation.lane == Lane::Upload)
}

fn is_stream_method(method: &str) -> bool {
    OPERATIONS
        .iter()
        .any(|operation| operation.method == method && operation.lane == Lane::Stream)
}

fn recording_router() -> (
    RouterHandle,
    mpsc::UnboundedReceiver<RecordedRequest>,
    JoinHandle<()>,
) {
    let (router, mut commands) =
        RouterHandle::channel(NonZeroUsize::new(8).expect("nonzero router capacity"));
    let (record, records) = mpsc::unbounded_channel();
    let actor = tokio::spawn(async move {
        while let Some(command) = commands.recv().await {
            let (request, reply) = command.into_parts();
            let (authority, operation, upload, _steps) = request.into_parts();
            let method = operation.method();
            let response = match &operation {
                OperationRequest::WorkerExec(exec)
                    if exec.session.as_deref() == Some(UNSOLICITED_BINARY_SESSION) =>
                {
                    RouterResponse::binary(
                        json!({ "eof": true, "nextOffset": 1 }),
                        Bytes::from_static(b"x"),
                    )
                }
                operation => match operation.download_offset() {
                    Some(offset) => {
                        let bytes = Bytes::from_static(b"chunk");
                        let next_offset = offset
                            .checked_add(u64::try_from(bytes.len()).expect("chunk length"))
                            .expect("log offset");
                        RouterResponse::binary(
                            json!({ "eof": true, "nextOffset": next_offset }),
                            bytes,
                        )
                    }
                    None if is_stream_method(method) => Ok(RouterResponse::events(Box::new(
                        OneEvent(Some(json!({ "method": method }))),
                    ))),
                    None => Ok(RouterResponse::json(json!({ "method": method }))),
                },
            };
            record
                .send(RecordedRequest {
                    authority,
                    method,
                    params: operation.params().expect("a decoded request encodes"),
                    upload,
                })
                .expect("record request");
            let _ = reply.send(response);
        }
    });
    (router, records, actor)
}

async fn assert_clean_disconnect(client: TestClient, server: JoinHandle<cowshed_core::Result<()>>) {
    drop(client);
    server
        .await
        .expect("server task joins")
        .expect("clean close");
}

/// Changing a project's identity is coordinator-tier, and stays that way.
///
/// The decision that a workspace credential may remain valid across an identity rename rests
/// entirely on workers being unable to perform one: a credential pins `repo_id`, `workspace` and a
/// random `workspace_incarnation`, and set-validating the identity axis is only safe while the
/// holder of such a credential cannot move that axis itself. That is asserted here rather than left
/// true by inspection, so widening the workspace-authority surface fails a test instead of quietly
/// changing what the credential means.
#[tokio::test]
async fn a_workspace_authority_cannot_reach_any_identity_verb() {
    const IDENTITY_VERBS: &[&str] = &["coordinator.changeRepoId", "coordinator.adopt"];
    for verb in IDENTITY_VERBS {
        assert!(
            connection_methods().any(|method| method == *verb),
            "{verb} must be a real capability method for this test to mean anything"
        );
        assert!(
            !is_worker_method(verb),
            "{verb} is an identity operation and must never be reachable by a workspace authority"
        );
    }

    let (router, mut records, _actor) = recording_router();
    let (mut worker, worker_server) = TestClient::connect(worker_authority(), router).await;
    for verb in IDENTITY_VERBS {
        let response = worker.request(verb, params(verb), None).await;
        assert!(
            !response.envelope.ok,
            "a workspace authority reached {verb}"
        );
        assert!(response.envelope.error.is_some());
    }
    // Refused at the authority gate, so the project router never saw them.
    assert!(records.try_recv().is_err());
    assert_clean_disconnect(worker, worker_server).await;
}

#[tokio::test]
async fn every_capability_method_is_explicitly_routed_or_rejected_by_authority() {
    let (router, mut records, _actor) = recording_router();
    let (mut coordinator, coordinator_server) =
        TestClient::connect(coordinator_authority(), router.clone()).await;

    for method in connection_methods() {
        if is_stream_method(method) {
            let id = coordinator.next_id;
            let (event, end) = coordinator.stream_request(method, params(method)).await;
            assert_eq!(event, json!({ "id": id, "event": { "method": method } }));
            assert!(
                end.ok,
                "coordinator stream {method} ended with {:?}",
                end.error
            );
            assert_eq!((end.id, end.result), (id, Some(json!({}))));
            let recorded = records.recv().await.expect("coordinator request recorded");
            assert_eq!((recorded.method, recorded.params), (method, params(method)));
            continue;
        }
        let upload = is_upload_method(method).then_some(&b"input"[..]);
        let response = coordinator.request(method, params(method), upload).await;
        assert!(response.envelope.ok, "coordinator rejected {method}");
        assert_eq!(response.envelope.id + 1, coordinator.next_id);
        assert!(response.envelope.error.is_none());
        let recorded = records.recv().await.expect("coordinator request recorded");
        assert_eq!(recorded.authority, coordinator_authority());
        assert_eq!(recorded.method, method);
        assert_eq!(recorded.params, params(method));
        assert_eq!(recorded.upload.as_deref(), upload);
        if method == "job.logs" {
            assert_eq!(response.binary.as_deref(), Some(&b"chunk"[..]));
            assert_eq!(
                response.envelope.result,
                Some(json!({ "eof": true, "nextOffset": 5 }))
            );
        } else {
            assert!(response.binary.is_none());
            assert_eq!(response.envelope.result, Some(json!({ "method": method })));
        }
    }
    assert_clean_disconnect(coordinator, coordinator_server).await;

    let (mut worker, worker_server) = TestClient::connect(worker_authority(), router).await;
    for method in connection_methods() {
        let allowed = is_worker_method(method);
        if allowed && is_stream_method(method) {
            let id = worker.next_id;
            let (event, end) = worker.stream_request(method, params(method)).await;
            assert_eq!(event, json!({ "id": id, "event": { "method": method } }));
            assert_eq!((end.ok, end.id, end.result), (true, id, Some(json!({}))));
            let recorded = records.recv().await.expect("worker request recorded");
            assert_eq!(recorded.authority, worker_authority());
            assert_eq!((recorded.method, recorded.params), (method, params(method)));
            continue;
        }
        let upload = (allowed && is_upload_method(method)).then_some(&b"input"[..]);
        let response = worker.request(method, params(method), upload).await;
        assert_eq!(
            response.envelope.ok, allowed,
            "worker decision for {method}"
        );
        if allowed {
            let recorded = records.recv().await.expect("worker request recorded");
            assert_eq!(recorded.authority, worker_authority());
            assert_eq!(recorded.method, method);
            assert_eq!(recorded.params, params(method));
            if method == "job.logs" {
                assert_eq!(
                    response.envelope.result,
                    Some(json!({ "eof": true, "nextOffset": 5 }))
                );
            } else {
                assert_eq!(response.envelope.result, Some(json!({ "method": method })));
            }
            assert_eq!(recorded.upload.as_deref(), upload);
        } else {
            assert_eq!(
                response.envelope.error.expect("typed authority error").code,
                ErrorCode::Conflict
            );
            assert!(records.try_recv().is_err());
        }
    }
    assert_clean_disconnect(worker, worker_server).await;
}

/// Events the test feeds, one per demand; dropping the source tells the test.
struct Fed {
    events: mpsc::UnboundedReceiver<Value>,
    dropped: Option<oneshot::Sender<()>>,
}

#[async_trait]
impl EventSource for Fed {
    async fn next(&mut self) -> Option<cowshed_core::Result<Value>> {
        self.events.recv().await.map(Ok)
    }
}

impl Drop for Fed {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

/// A router whose one `job.progress` call streams `source`, and whose other calls answer at once.
fn streaming_router(source: Fed) -> RouterHandle {
    let (router, mut commands) =
        RouterHandle::channel(NonZeroUsize::new(8).expect("nonzero router capacity"));
    tokio::spawn(async move {
        let mut source = Some(source);
        while let Some(command) = commands.recv().await {
            let (request, reply) = command.into_parts();
            let method = request.method();
            let response = match (method, source.take()) {
                ("job.progress", Some(source)) => RouterResponse::events(Box::new(source)),
                (_, unused) => {
                    source = unused;
                    RouterResponse::json(json!({ "method": method }))
                }
            };
            let _ = reply.send(Ok(response));
        }
    });
    router
}

async fn read_value(client: &mut TestClient) -> Value {
    serde_json::from_slice(&client.read_frame().await).expect("a JSON frame")
}

/// A stream-lane call is sent an event only when its caller demands one: unread, it holds up no
/// other call on its connection, and a close ends it with an empty answer and drops its source.
#[tokio::test]
async fn a_stream_sends_each_event_on_demand_and_ends_when_closed() {
    let (feed, events) = mpsc::unbounded_channel();
    let (dropped, source_dropped) = oneshot::channel();
    let router = streaming_router(Fed {
        events,
        dropped: Some(dropped),
    });
    let (mut client, server) = TestClient::connect(worker_authority(), router).await;
    client
        .write_json(&json!({ "id": 1, "method": "job.progress", "params": params("job.progress") }))
        .await;
    client.next_id = 2;
    // Nothing to send yet: another call is answered meanwhile.
    let status = client
        .request("job.status", params("job.status"), None)
        .await;
    assert_eq!(status.envelope.id, 2);

    feed.send(json!({ "n": 1 })).expect("feed");
    assert_eq!(
        read_value(&mut client).await,
        json!({ "id": 1, "event": { "n": 1 } })
    );
    // An event waits for its demand: the next frame answers the call made after it.
    feed.send(json!({ "n": 2 })).expect("feed");
    let status = client
        .request("job.status", params("job.status"), None)
        .await;
    assert_eq!(status.envelope.id, 3);
    client
        .write_json(&json!({ "id": 1, "demand": "next" }))
        .await;
    assert_eq!(
        read_value(&mut client).await,
        json!({ "id": 1, "event": { "n": 2 } })
    );

    client
        .write_json(&json!({ "id": 1, "demand": "close" }))
        .await;
    let end: RpcResponse = serde_json::from_slice(&client.read_frame().await).expect("end");
    assert_eq!((end.id, end.ok, end.result), (1, true, Some(json!({}))));
    source_dropped
        .await
        .expect("the closed stream's source is dropped");
    // A close that crosses the end is no error.
    client
        .write_json(&json!({ "id": 1, "demand": "close" }))
        .await;
    let status = client
        .request("job.status", params("job.status"), None)
        .await;
    assert_eq!(status.envelope.id, 4);
    assert_clean_disconnect(client, server).await;
}

/// A demand the protocol does not allow -- a second one while one is unanswered, or one naming
/// no open stream -- ends the connection.
#[tokio::test]
async fn a_demand_without_an_open_stream_to_answer_it_ends_the_connection() {
    let (_feed, events) = mpsc::unbounded_channel();
    let router = streaming_router(Fed {
        events,
        dropped: None,
    });
    let (mut client, server) = TestClient::connect(worker_authority(), router).await;
    client
        .write_json(&json!({ "id": 1, "method": "job.progress", "params": params("job.progress") }))
        .await;
    client
        .write_json(&json!({ "id": 1, "demand": "next" }))
        .await;
    let refused: RpcResponse = serde_json::from_slice(&client.read_frame().await).expect("refusal");
    assert_eq!((refused.id, refused.ok), (1, false));
    assert_eq!(refused.error.expect("typed").code, ErrorCode::Integrity);
    assert!(server.await.expect("server task joins").is_err());

    let (_feed, events) = mpsc::unbounded_channel();
    let router = streaming_router(Fed {
        events,
        dropped: None,
    });
    let (mut client, server) = TestClient::connect(worker_authority(), router).await;
    client
        .write_json(&json!({ "id": 9, "demand": "next" }))
        .await;
    let refused: RpcResponse = serde_json::from_slice(&client.read_frame().await).expect("refusal");
    assert_eq!((refused.id, refused.ok), (9, false));
    assert!(server.await.expect("server task joins").is_err());
}

#[tokio::test]
async fn internal_operations_are_refused_over_a_connection() {
    let internal: Vec<&str> = OPERATIONS
        .iter()
        .filter(|operation| operation.scope == Scope::Internal)
        .map(|operation| operation.method)
        .collect();
    assert!(!internal.is_empty());
    let (router, mut records, _actor) = recording_router();
    let (mut coordinator, server) = TestClient::connect(coordinator_authority(), router).await;
    for method in internal {
        let response = coordinator.request(method, params(method), None).await;
        assert!(!response.envelope.ok, "a connection reached {method}");
        assert_eq!(
            response.envelope.error.expect("typed authority error").code,
            ErrorCode::Conflict
        );
    }
    assert!(records.try_recv().is_err());
    assert_clean_disconnect(coordinator, server).await;
}

#[tokio::test]
async fn a_request_its_declaration_refuses_never_reaches_the_router() {
    let (router, mut records, _actor) = recording_router();
    let (mut coordinator, server) = TestClient::connect(coordinator_authority(), router).await;
    let mut mutated = params("job.kill");
    let job = mutated
        .as_object_mut()
        .expect("job.kill params")
        .remove("jobId")
        .expect("jobId");
    mutated
        .as_object_mut()
        .expect("job.kill params")
        .insert("job".into(), job);
    let response = coordinator.request("job.kill", mutated, None).await;
    assert!(!response.envelope.ok);
    let error = response.envelope.error.expect("typed decode error");
    assert_eq!(error.code, ErrorCode::Usage);
    assert!(
        error.message.contains("invalid job.kill parameters"),
        "{}",
        error.message
    );
    assert!(records.try_recv().is_err());
    assert_clean_disconnect(coordinator, server).await;
}

#[tokio::test]
async fn worker_fence_rejects_wrong_repo_workspace_and_incarnation_before_router_effects() {
    let (router, mut records, _actor) = recording_router();
    let (mut client, server) = TestClient::connect(worker_authority(), router).await;

    let mismatches = [
        json!({
            "repoId": other_repo(),
            "workspace": workspace(),
            "workspaceIncarnation": incarnation(),
        }),
        json!({
            "repoId": repo(),
            "workspace": "other",
            "workspaceIncarnation": incarnation(),
        }),
        json!({
            "repoId": repo(),
            "workspace": workspace(),
            "workspaceIncarnation": other_incarnation(),
        }),
    ];
    for params in mismatches {
        let response = client.request("job.status", params, None).await;
        assert!(!response.envelope.ok);
        assert_eq!(
            response.envelope.error.expect("typed fence failure").code,
            ErrorCode::Conflict
        );
        assert!(records.try_recv().is_err());
    }

    let response = client
        .request("job.status", params("job.status"), None)
        .await;
    assert!(response.envelope.ok);
    assert_eq!(
        records.recv().await.expect("valid route").method,
        "job.status"
    );
    assert_clean_disconnect(client, server).await;
}

#[tokio::test]
async fn worker_cannot_call_coordinator_method() {
    let (router, mut records, _actor) = recording_router();
    let (mut client, server) = TestClient::connect(worker_authority(), router).await;
    let response = client
        .request("coordinator.destroy", params("coordinator.destroy"), None)
        .await;
    assert!(!response.envelope.ok);
    assert_eq!(
        response.envelope.error.expect("typed authority error").code,
        ErrorCode::Conflict
    );
    assert!(records.try_recv().is_err());
    assert_clean_disconnect(client, server).await;
}

#[tokio::test]
async fn malformed_and_oversized_json_frames_stop_only_the_connection() {
    let (router, mut records, _actor) = recording_router();

    let (mut malformed, malformed_server) =
        TestClient::connect(coordinator_authority(), router.clone()).await;
    malformed
        .write_frame(br#"{"id":1,"method":"project.list","params":{},"extra":true}"#)
        .await;
    drop(malformed);
    let error = malformed_server
        .await
        .expect("malformed server joins")
        .expect_err("unknown envelope field rejected");
    assert_eq!(error.code, ErrorCode::Integrity);
    assert!(records.try_recv().is_err());

    let (mut oversized, oversized_server) =
        TestClient::connect(coordinator_authority(), router.clone()).await;
    let oversized_length = u32::try_from(MAX_JSON_FRAME_BYTES + 1).expect("wire length");
    oversized
        .stream
        .write_all(&oversized_length.to_be_bytes())
        .await
        .expect("oversized header");
    drop(oversized);
    let error = oversized_server
        .await
        .expect("oversized server joins")
        .expect_err("oversized frame rejected");
    assert_eq!(error.code, ErrorCode::Integrity);
    assert!(records.try_recv().is_err());

    let (mut valid, valid_server) = TestClient::connect(coordinator_authority(), router).await;
    assert!(
        valid
            .request("project.list", params("project.list"), None)
            .await
            .envelope
            .ok
    );
    assert_eq!(
        records.recv().await.expect("isolated valid route").method,
        "project.list"
    );
    assert_clean_disconnect(valid, valid_server).await;
}

#[tokio::test]
async fn oversized_binary_and_second_raw_lane_are_rejected_before_router_effects() {
    let (router, mut records, _actor) = recording_router();

    let (mut oversized, oversized_server) =
        TestClient::connect(worker_authority(), router.clone()).await;
    let declared = u32::try_from(MAX_BINARY_FRAME_BYTES + 1).expect("binary wire length");
    oversized
        .write_json(&json!({
            "id": 1,
            "method": "worker.stdinChunk",
            "params": params("worker.stdinChunk"),
            "binaryLength": declared,
        }))
        .await;
    let response: RpcResponse =
        serde_json::from_slice(&oversized.read_frame().await).expect("typed oversized response");
    assert!(!response.ok);
    assert_eq!(
        response.error.expect("oversize error").code,
        ErrorCode::Integrity
    );
    assert!(records.try_recv().is_err());
    drop(oversized);
    assert_eq!(
        oversized_server
            .await
            .expect("oversized binary server joins")
            .expect_err("oversized binary is fatal")
            .code,
        ErrorCode::Integrity
    );

    let (mut second_lane, second_lane_server) =
        TestClient::connect(worker_authority(), router).await;
    let response = second_lane
        .request("job.logs", params("job.logs"), Some(b"upload"))
        .await;
    assert!(!response.envelope.ok);
    assert_eq!(
        response.envelope.error.expect("second lane error").code,
        ErrorCode::Integrity
    );
    assert!(records.try_recv().is_err());
    drop(second_lane);
    assert_eq!(
        second_lane_server
            .await
            .expect("second lane server joins")
            .expect_err("second raw lane is fatal")
            .code,
        ErrorCode::Integrity
    );
}

#[tokio::test]
async fn router_cannot_return_an_unsolicited_second_raw_lane() {
    let (router, mut records, _actor) = recording_router();
    let (mut client, server) = TestClient::connect(worker_authority(), router).await;
    let mut exec = params("worker.exec");
    exec.as_object_mut()
        .expect("worker.exec params")
        .insert("session".into(), json!(UNSOLICITED_BINARY_SESSION));
    let response = client.request("worker.exec", exec, Some(b"stdin")).await;
    assert!(!response.envelope.ok);
    assert_eq!(
        response.envelope.error.expect("second lane error").code,
        ErrorCode::Integrity
    );
    let recorded = records.recv().await.expect("router response recorded");
    assert_eq!(recorded.upload.as_deref(), Some(&b"stdin"[..]));
    drop(client);
    assert_eq!(
        server
            .await
            .expect("server joins")
            .expect_err("unsolicited lane is fatal")
            .code,
        ErrorCode::Integrity
    );
}

#[tokio::test]
async fn disconnect_cancels_only_the_connection_while_routed_work_continues() {
    let (router, mut commands) =
        RouterHandle::channel(NonZeroUsize::new(1).expect("nonzero router capacity"));
    let (started, started_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let (finished, finished_rx) = oneshot::channel();
    let actor = tokio::spawn(async move {
        let command = commands.recv().await.expect("routed command");
        let (_request, reply) = command.into_parts();
        started.send(()).expect("signal command start");
        let _ = release_rx.await;
        let disconnected = reply
            .send(Ok(RouterResponse::json(json!({ "done": true }))))
            .is_err();
        finished
            .send(disconnected)
            .expect("signal command completion");
    });

    let (mut client, server) = TestClient::connect(coordinator_authority(), router).await;
    client
        .write_json(&json!({
            "id": 1,
            "method": "job.status",
            "params": params("job.status"),
            "binaryLength": null,
        }))
        .await;
    started_rx.await.expect("router began work");
    drop(client);
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .expect("connection task observes disconnect")
        .expect("connection task joins")
        .expect("disconnect is clean");

    release.send(()).expect("release routed work");
    assert!(
        finished_rx.await.expect("router completed work"),
        "the routed command completed after its connection reply was dropped"
    );
    actor.await.expect("router actor joins");
}

#[tokio::test]
async fn malformed_connection_does_not_poison_a_concurrent_connection() {
    let (router, mut records, _actor) = recording_router();
    let (mut bad, bad_server) = TestClient::connect(coordinator_authority(), router.clone()).await;
    let (mut good, good_server) = TestClient::connect(coordinator_authority(), router).await;

    bad.write_frame(b"not-json").await;
    drop(bad);
    assert_eq!(
        bad_server
            .await
            .expect("bad server joins")
            .expect_err("bad JSON rejected")
            .code,
        ErrorCode::Integrity
    );

    let response = good
        .request("project.list", params("project.list"), None)
        .await;
    assert!(response.envelope.ok);
    assert_eq!(
        records.recv().await.expect("good route").method,
        "project.list"
    );
    assert_clean_disconnect(good, good_server).await;
}

#[tokio::test]
async fn invalid_version_nonce_replay_and_non_socket_peer_fail_before_router_effects() {
    let (router, mut records, _actor) = recording_router();

    for (version, nonce) in [(HANDSHAKE_VERSION + 1, NONCE), (HANDSHAKE_VERSION, "bad")] {
        let (client, server) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        client.set_nonblocking(true).expect("nonblocking client");
        let mut stream = tokio::net::UnixStream::from_std(client).expect("Tokio client");
        let descriptor: OwnedFd = server.into();
        let task = tokio::spawn(serve_controller_connection(
            descriptor,
            coordinator_authority(),
            router.clone(),
        ));
        let bytes =
            serde_json::to_vec(&json!({ "version": version, "nonce": nonce })).expect("hello JSON");
        let length = u32::try_from(bytes.len()).expect("hello length");
        stream
            .write_all(&length.to_be_bytes())
            .await
            .expect("hello header");
        stream.write_all(&bytes).await.expect("hello body");
        drop(stream);
        assert_eq!(
            task.await
                .expect("handshake task joins")
                .expect_err("invalid hello rejected")
                .code,
            ErrorCode::Integrity
        );
        assert!(records.try_recv().is_err());
    }

    let (mut replay, replay_server) =
        TestClient::connect(coordinator_authority(), router.clone()).await;
    assert!(
        replay
            .request_with_id(1, "project.list", params("project.list"), None)
            .await
            .envelope
            .ok
    );
    assert_eq!(
        records.recv().await.expect("first request routed").method,
        "project.list"
    );
    let repeated = replay
        .request_with_id(1, "project.list", params("project.list"), None)
        .await;
    assert!(!repeated.envelope.ok);
    assert_eq!(
        repeated.envelope.error.expect("replay error").code,
        ErrorCode::Integrity
    );
    assert!(records.try_recv().is_err());
    drop(replay);
    assert_eq!(
        replay_server
            .await
            .expect("replay server joins")
            .expect_err("replay closes connection")
            .code,
        ErrorCode::Integrity
    );

    let file = std::fs::File::open("/dev/null").expect("open non-socket descriptor");
    let descriptor: OwnedFd = file.into();
    let error = serve_controller_connection(descriptor, coordinator_authority(), router)
        .await
        .expect_err("non-socket peer rejected");
    assert_eq!(error.code, ErrorCode::EnvironmentMissing);
    assert!(records.try_recv().is_err());
}

/// A connection with 64 open ordinary calls keeps reading: 64 more wait unrouted, one past
/// those is refused with a conflict, and a stream's close is still read and answered. As open
/// calls complete, the waiting ones are routed in order, and every one is answered.
#[tokio::test]
async fn a_full_connection_queues_then_refuses_calls_and_still_reads_a_close() {
    let (router, mut commands) =
        RouterHandle::channel(NonZeroUsize::new(256).expect("router capacity"));
    let (held, mut pending) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(command) = commands.recv().await {
            let (request, reply) = command.into_parts();
            if request.method() == "job.progress" {
                let _ = reply.send(Ok(RouterResponse::events(Box::new(OneEvent(Some(
                    json!({ "n": 1 }),
                ))))));
            } else {
                held.send(reply).expect("the test holds ordinary calls");
            }
        }
    });
    let (mut client, server) = TestClient::connect(worker_authority(), router).await;
    client
        .write_json(&json!({ "id": 1, "method": "job.progress", "params": params("job.progress") }))
        .await;
    assert_eq!(
        read_value(&mut client).await,
        json!({ "id": 1, "event": { "n": 1 } })
    );
    let mut open = Vec::new();
    for id in 2..=65 {
        client
            .write_json(
                &json!({ "id": id, "method": "job.status", "params": params("job.status") }),
            )
            .await;
        open.push(pending.recv().await.expect("an open call is routed"));
    }
    for id in 66..=130 {
        client
            .write_json(
                &json!({ "id": id, "method": "job.status", "params": params("job.status") }),
            )
            .await;
    }
    // Nothing has been answered, so the first frame is the refusal of the call past both caps.
    let refusal: RpcResponse = serde_json::from_slice(&client.read_frame().await).expect("refusal");
    assert_eq!((refusal.id, refusal.ok), (130, false));
    assert_eq!(
        refusal.error.expect("typed cap error").code,
        ErrorCode::Conflict
    );
    client
        .write_json(&json!({ "id": 1, "demand": "close" }))
        .await;
    assert_eq!(
        read_value(&mut client).await,
        json!({ "id": 1, "ok": true, "result": {}, "error": null, "binaryLength": null }),
        "the close is read and answered while every call slot is taken"
    );
    // Completing the open calls routes the waiting ones.
    for reply in open {
        reply
            .send(Ok(RouterResponse::json(json!({}))))
            .expect("the connection awaits its call");
    }
    for _ in 66..=129 {
        pending
            .recv()
            .await
            .expect("a waiting call is routed")
            .send(Ok(RouterResponse::json(json!({}))))
            .expect("the connection awaits its call");
    }
    let mut answered = Vec::new();
    for _ in 2..=129 {
        let answer: RpcResponse =
            serde_json::from_slice(&client.read_frame().await).expect("an answer");
        assert!(answer.ok, "{answer:?}");
        answered.push(answer.id);
    }
    answered.sort_unstable();
    assert_eq!(answered, (2..=129).collect::<Vec<_>>());
    drop(client);
    server
        .await
        .expect("server joins")
        .expect("clean disconnect");
}

/// A sixty-fifth open stream is refused with a conflict, and the connection still reads a close
/// on one of the 64.
#[tokio::test]
async fn the_stream_cap_refuses_a_sixty_fifth_stream_and_still_reads_a_close() {
    let (router, mut commands) =
        RouterHandle::channel(NonZeroUsize::new(128).expect("router capacity"));
    tokio::spawn(async move {
        while let Some(command) = commands.recv().await {
            let (_, reply) = command.into_parts();
            let _ = reply.send(Ok(RouterResponse::events(Box::new(OneEvent(Some(
                json!({ "n": 1 }),
            ))))));
        }
    });
    let (mut client, server) = TestClient::connect(worker_authority(), router).await;
    for id in 1..=64 {
        client
            .write_json(
                &json!({ "id": id, "method": "job.progress", "params": params("job.progress") }),
            )
            .await;
        assert_eq!(
            read_value(&mut client).await,
            json!({ "id": id, "event": { "n": 1 } })
        );
    }
    client
        .write_json(
            &json!({ "id": 65, "method": "job.progress", "params": params("job.progress") }),
        )
        .await;
    let refusal: RpcResponse = serde_json::from_slice(&client.read_frame().await).expect("refusal");
    assert_eq!((refusal.id, refusal.ok), (65, false));
    assert_eq!(
        refusal.error.expect("typed cap error").code,
        ErrorCode::Conflict
    );
    client
        .write_json(&json!({ "id": 1, "demand": "close" }))
        .await;
    assert_eq!(
        read_value(&mut client).await,
        json!({ "id": 1, "ok": true, "result": {}, "error": null, "binaryLength": null })
    );
    drop(client);
    server
        .await
        .expect("server joins")
        .expect("clean disconnect");
}
