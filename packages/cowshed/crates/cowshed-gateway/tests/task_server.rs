#![cfg(target_os = "macos")]
//! The task server over a real control socket: work units admitted and selected without any
//! human assignment, the caps reported as declared, and a queue view that names what waits on
//! what and who holds each serialization class.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;

use cowshed_os::test_backstop::AsyncUnixStreamBackstop as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CredentialError,
    CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_gateway_types::{
    LeaseClass, TaskAdmitRequest, TaskClass, TaskOutcome, TaskRunCaps, TaskTerminalRequest,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;

struct NoCredentials;

#[async_trait]
impl CredentialProvider for NoCredentials {
    async fn lookup(
        &self,
        _query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        Ok(None)
    }
}

struct NoConnector;

#[async_trait]
impl UpstreamConnector for NoConnector {
    async fn health(&self, _target: &cowshed_gateway::CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Unknown
    }

    async fn connect(
        &self,
        _target: &AuthorizedTarget,
    ) -> Result<UpstreamConnection, ConnectError> {
        Err(ConnectError::NoAddresses)
    }
}

struct DiscardAudit;

#[async_trait]
impl AuditSink for DiscardAudit {
    async fn record(&self, _event: AuditEvent) -> Result<(), AuditError> {
        Ok(())
    }

    async fn flush(&self) -> Result<(), AuditError> {
        Ok(())
    }
}

/// A gateway on a socket under `/tmp` (a bind path is capped at `sun_path`'s 104 bytes), and the
/// guard that removes its root on every exit path.
async fn gateway() -> (Gateway, fixture_dir::FixtureDir, PathBuf) {
    let id = Uuid::new_v4().simple().to_string();
    let root = fixture_dir::secure_fixture_dir(&format!("cstask-{}", &id[..8]));
    let cache = root.path().join("cache");
    std::fs::create_dir(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = root.path().join("gateway.sock");
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            mirror_cache: MirrorCacheConfig::new(cache),
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway");
    (gateway, root, socket)
}

/// A gate-class request: a gate unit never waits for the host's memory pressure, so the units
/// these tests run are selected whatever the host is doing.
fn admit(unit: &str, deps: &[&str], caps: Option<TaskRunCaps>) -> String {
    let mut request = TaskAdmitRequest::new(unit, "workspace-1", "nx run gate");
    request.class = TaskClass::Gate;
    request.deps = deps.to_vec();
    request.caps = caps;
    let mut line = serde_json::to_string(&request).expect("encode");
    line.push('\n');
    line
}

/// A request for the image window of `class`, which `TaskAdmitRequest` leaves off the wire when it
/// is the default.
fn admit_image(unit: &str, class: TaskClass) -> String {
    let mut request = TaskAdmitRequest::new(unit, "workspace-1", "nx run gate");
    request.class = class;
    request.leases = vec![LeaseClass::Image];
    let mut line = serde_json::to_string(&request).expect("encode");
    line.push('\n');
    line
}

fn terminal(unit: &str, outcome: TaskOutcome) -> String {
    let mut line = serde_json::to_string(&TaskTerminalRequest::new(unit, outcome)).expect("encode");
    line.push('\n');
    line
}

/// One held `task-admit` connection in this process, answered `queued`.
struct Asker {
    lines: BufReader<tokio::net::unix::OwnedReadHalf>,
    _writer: tokio::net::unix::OwnedWriteHalf,
}

impl Asker {
    async fn ask(socket: &Path, line: String) -> Self {
        let stream = UnixStream::connect_bounded(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        writer.write_all(line.as_bytes()).await.expect("ask");
        let mut asker = Self {
            lines: BufReader::new(reader),
            _writer: writer,
        };
        let queued = asker.answer().await;
        assert_eq!(queued["lease"], "queued", "{queued}");
        asker
    }

    async fn answer(&mut self) -> Value {
        let mut line = String::new();
        self.lines.read_line(&mut line).await.expect("answer");
        serde_json::from_str(&line).expect("an answer is JSON")
    }

    /// The grant named by the next answer, waiting for it.
    async fn granted(&mut self) -> Value {
        let answer = self.answer().await;
        assert_eq!(answer["lease"], "granted", "{answer}");
        answer["taskGrant"].clone()
    }

    /// The refusal, waiting for the answer.
    async fn refused(&mut self) -> Value {
        let answer = self.answer().await;
        assert_eq!(answer["ok"], Value::Bool(false), "{answer}");
        answer
    }
}

/// A one-shot control request's answer.
async fn one_shot(socket: &Path, line: &str) -> Value {
    let mut stream = UnixStream::connect_bounded(socket).await.expect("connect");
    stream.write_all(line.as_bytes()).await.expect("ask");
    stream.shutdown().await.expect("half-close");
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.expect("answer");
    serde_json::from_str(answer.trim_end()).expect("an answer is JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn units_run_without_an_assignment_and_the_queue_names_what_waits_on_what() {
    let (gateway, _root, socket) = gateway().await;
    // unit-a runs; unit-b waits on unit-a and runs after its fact alone.
    let mut runner = Asker::ask(
        &socket,
        admit(
            "unit-a",
            &[],
            Some(TaskRunCaps {
                nx_parallel: 2,
                cargo_jobs: 4,
                test_threads: 6,
            }),
        ),
    )
    .await;
    let mut dependent = Asker::ask(
        &socket,
        admit(
            "unit-b",
            &["unit-a"],
            Some(TaskRunCaps {
                nx_parallel: 4,
                cargo_jobs: 8,
                test_threads: 8,
            }),
        ),
    )
    .await;
    let grant = runner.granted().await;
    // The caps the server reports: the ceiling as declared.
    assert_eq!(grant["unit"], "unit-a");
    assert_eq!(grant["caps"]["nxParallel"], 2);
    assert_eq!(grant["caps"]["cargoJobs"], 4);
    assert_eq!(grant["caps"]["testThreads"], 6);
    // The queue view: what runs, what waits on what, and the serialization classes' places.
    let queue = one_shot(&socket, "{\"op\":\"task-queue\"}\n").await;
    let queue = &queue["taskQueue"];
    assert_eq!(queue["running"][0]["unit"], "unit-a");
    assert_eq!(queue["waiting"][0]["unit"], "unit-b");
    assert_eq!(queue["waiting"][0]["deps"][0]["unit"], "unit-a");
    assert_eq!(queue["waiting"][0]["deps"][0]["outcome"], Value::Null);
    assert_eq!(queue["leases"][2]["class"], "land-publication");
    assert_eq!(queue["leases"][2]["places"], 1);
    // unit-a lands: the terminal fact alone advances unit-b, no human in the loop.
    let settled = one_shot(&socket, &terminal("unit-a", TaskOutcome::Landed)).await;
    assert_eq!(settled["ok"], Value::Bool(true));
    let grant = dependent.granted().await;
    assert_eq!(grant["unit"], "unit-b");
    assert_eq!(grant["caps"]["nxParallel"], 4);
    assert_eq!(grant["caps"]["cargoJobs"], 8);
    assert_eq!(grant["caps"]["testThreads"], 8);
    drop(runner);
    drop(dependent);
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_unit_keeps_one_live_attempt_over_the_wire() {
    let (gateway, _root, socket) = gateway().await;
    let mut runner = Asker::ask(&socket, admit("unit", &[], None)).await;
    runner.granted().await;
    let mut second = Asker::ask(&socket, admit("unit", &[], None)).await;
    let refused = second.refused().await;
    assert_eq!(refused["code"], "rejected", "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .expect("an error")
            .contains("live attempt"),
        "{refused}"
    );
    drop(runner);
    drop(second);
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_band_units_are_granted_concurrently() {
    let (gateway, _root, socket) = gateway().await;
    // Both requests name the gate class. Their grants coexist: a task band is not an exclusive
    // gate root.
    let mut first = Asker::ask(&socket, admit("first", &[], None)).await;
    let mut second = Asker::ask(&socket, admit("second", &[], None)).await;
    for runner in [&mut first, &mut second] {
        assert_eq!(runner.granted().await["band"], "normal");
    }
    let queue = one_shot(&socket, "{\"op\":\"task-queue\"}\n").await;
    assert_eq!(queue["taskQueue"]["running"].as_array().unwrap().len(), 2);
    drop(first);
    drop(second);
    gateway.drain().await.expect("drain");
}

/// A request that omits its class is background (05_gateway.md "Classes"): the wire line leaves
/// the default off, the queue names the waiting unit's class and band, and a request that names
/// gate sends its class and is granted the gate band.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_that_omits_its_class_is_background_and_a_gate_request_names_its_own() {
    let (gateway, _root, socket) = gateway().await;
    let held = admit_image("holder", TaskClass::Gate);
    assert!(held.contains(r#""class":"gate""#), "{held}");
    let waiting = admit_image("waiter", TaskClass::Background);
    assert!(!waiting.contains("class"), "{waiting}");
    let mut holder = Asker::ask(&socket, held).await;
    assert_eq!(holder.granted().await["band"], "normal");
    // The waiter wants the window the holder has, so the queue shows it whatever the host's memory
    // pressure is.
    let waiter = Asker::ask(&socket, waiting).await;
    let queue = one_shot(&socket, "{\"op\":\"task-queue\"}\n").await;
    let ready = &queue["taskQueue"]["ready"][0];
    assert_eq!(ready["unit"], "waiter", "{queue}");
    assert_eq!(ready["class"], "background", "{queue}");
    assert_eq!(ready["band"], "background", "{queue}");
    drop(holder);
    drop(waiter);
    gateway.drain().await.expect("drain");
}
