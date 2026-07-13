#![cfg(unix)]

use bytes::Bytes;
use cowshed_core::api::server::{
    CAPABILITY_METHODS, ConnectionAuthority, HANDSHAKE_VERSION, MAX_BINARY_FRAME_BYTES,
    MAX_JSON_FRAME_BYTES, RouterHandle, RouterResponse, WORKER_METHODS,
    serve_controller_connection,
};
use cowshed_core::metadata::{WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::{CowshedError, ErrorCode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::num::NonZeroUsize;
use std::os::fd::OwnedFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const NONCE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Debug)]
struct RecordedRequest {
    authority: ConnectionAuthority,
    method: String,
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

fn coordinator_params(method: &str) -> Value {
    if method == "project.open" {
        json!({ "path": "/trusted/acme/widget" })
    } else if method == "job.logs" {
        json!({ "repoId": repo(), "offset": 0 })
    } else {
        json!({ "repoId": repo() })
    }
}

fn worker_params(method: &str) -> Value {
    let mut params = json!({
        "repoId": repo(),
        "workspace": workspace(),
        "workspaceIncarnation": incarnation(),
    });
    if method == "job.logs" {
        params
            .as_object_mut()
            .expect("worker params object")
            .insert("offset".into(), json!(0));
    }
    params
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
            let (authority, method, params, upload) = request.into_parts();
            let response = if method == "job.logs" {
                let offset = params
                    .get("offset")
                    .and_then(Value::as_u64)
                    .expect("validated log offset");
                let bytes = Bytes::from_static(b"chunk");
                let next_offset = offset
                    .checked_add(u64::try_from(bytes.len()).expect("chunk length"))
                    .expect("log offset");
                RouterResponse::binary(json!({ "eof": true, "nextOffset": next_offset }), bytes)
            } else if params.get("routerBinary") == Some(&Value::Bool(true)) {
                RouterResponse::binary(
                    json!({ "eof": true, "nextOffset": 1 }),
                    Bytes::from_static(b"x"),
                )
            } else {
                Ok(RouterResponse::json(json!({ "method": method })))
            };
            record
                .send(RecordedRequest {
                    authority,
                    method,
                    params,
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

#[tokio::test]
async fn every_capability_method_is_explicitly_routed_or_rejected_by_authority() {
    let (router, mut records, _actor) = recording_router();
    let (mut coordinator, coordinator_server) =
        TestClient::connect(coordinator_authority(), router.clone()).await;

    for method in CAPABILITY_METHODS {
        let upload = ["worker.exec", "worker.stdinChunk", "job.attachWrite"]
            .contains(method)
            .then_some(&b"input"[..]);
        let response = coordinator
            .request(method, coordinator_params(method), upload)
            .await;
        assert!(response.envelope.ok, "coordinator rejected {method}");
        assert_eq!(response.envelope.id + 1, coordinator.next_id);
        assert!(response.envelope.error.is_none());
        let recorded = records.recv().await.expect("coordinator request recorded");
        assert_eq!(recorded.authority, coordinator_authority());
        assert_eq!(recorded.method, *method);
        assert_eq!(recorded.params, coordinator_params(method));
        assert_eq!(recorded.upload.as_deref(), upload);
        if *method == "job.logs" {
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
    for method in CAPABILITY_METHODS {
        let allowed = WORKER_METHODS.contains(method);
        let upload = (allowed
            && ["worker.exec", "worker.stdinChunk", "job.attachWrite"].contains(method))
        .then_some(&b"input"[..]);
        let response = worker.request(method, worker_params(method), upload).await;
        assert_eq!(
            response.envelope.ok, allowed,
            "worker decision for {method}"
        );
        if allowed {
            let recorded = records.recv().await.expect("worker request recorded");
            assert_eq!(recorded.authority, worker_authority());
            assert_eq!(recorded.method, *method);
            assert_eq!(recorded.params, worker_params(method));
            if *method == "job.logs" {
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
        .request("job.status", worker_params("job.status"), None)
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
        .request(
            "coordinator.destroy",
            worker_params("coordinator.destroy"),
            None,
        )
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
            .request("project.list", coordinator_params("project.list"), None)
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
            "params": worker_params("worker.stdinChunk"),
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
        .request("job.logs", worker_params("job.logs"), Some(b"upload"))
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
    let mut params = worker_params("worker.exec");
    params
        .as_object_mut()
        .expect("worker params")
        .insert("routerBinary".into(), Value::Bool(true));
    let response = client.request("worker.exec", params, Some(b"stdin")).await;
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
            "params": coordinator_params("job.status"),
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
        .request("project.list", coordinator_params("project.list"), None)
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
            .request_with_id(1, "project.list", coordinator_params("project.list"), None)
            .await
            .envelope
            .ok
    );
    assert_eq!(
        records.recv().await.expect("first request routed").method,
        "project.list"
    );
    let repeated = replay
        .request_with_id(1, "project.list", coordinator_params("project.list"), None)
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
