//! Outage and midstream cases for the gateway's `/nx/v1/cache/{hash}` route, driven by the stock
//! Nx client in a real workspace. Each gateway case is a fresh private fixture with its own gateway
//! and its own `NX_SELF_HOSTED_REMOTE_CACHE_SERVER` pointed at the gateway's session listener. Direct
//! controls point the same client at the fixture backend with no gateway in between, so the
//! backend's 5xx reaches the client, which reads it as its own outage (05_gateway.md "The client
//! half"): one line, a miss or a skipped store, and nothing more asked of that server.
//!
//! Contract for the gateway route, as implemented in `proxy.rs` (`nx_backend_unavailable`): when the
//! project backend is offline, unreachable, or answers 5xx before the response head, a GET is 404 and
//! a PUT is 403, audited as `Failed` with classification `nx-backend-unavailable`. The local build
//! still runs once and succeeds.

use cowshed_os::test_backstop::AsyncCommandBackstop as _;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

use cowshed_gateway::{NxCacheBackend, NxCacheServiceConfig};
use cowshed_gateway_types::WorkspacePolicy;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::process::Command;

const REPO: &str = "owner/nx-outage";
const PLUGIN_RECEIPTS: &str = ".nx/cache/.cowshed-task-results";
const RUN_RECEIPT: &str = "00000000000000000000000000000071";
const PUBLISH_RECEIPT: &str = "00000000000000000000000000000072";
const FIXTURE_CONNECTION_LIMIT: usize = 16;
const FIXTURE_HEAD_LIMIT: usize = 16 * 1024;

/// The repository root from the gateway crate's manifest, which is four levels below it.
fn repository_root() -> PathBuf {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    std::fs::canonicalize(Path::new(&manifest).join("../../../.."))
        .expect("repository root from the crate manifest")
}

fn write_file(path: &Path, contents: impl AsRef<[u8]>) {
    std::fs::write(path, contents)
        .unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

/// One cached task, `app:build`, that appends a `run` line per real execution and writes
/// `result.txt`. Its cache key covers `source.txt`, so the plugin's hint names exactly one hash.
pub(super) fn write_workspace(root: &Path) {
    let repository = repository_root();
    let plugin = std::fs::canonicalize(repository.join("packages/nx-plugin/loader.js"))
        .expect("this repository's nx-plugin loader");
    std::os::unix::fs::symlink(repository.join("node_modules"), root.join("node_modules"))
        .expect("link the repository's node_modules");
    std::fs::create_dir(root.join("app")).expect("create the app project");
    std::fs::create_dir(root.join("sockets")).expect("create the Nx socket directory");
    write_file(
        &root.join("package.json"),
        json!({"name": "nx-outage-fixture", "private": true}).to_string(),
    );
    write_file(
        &root.join("nx.json"),
        json!({"cacheDirectory": ".nx/cache", "plugins": [plugin.display().to_string()]})
            .to_string(),
    );
    write_file(
        &root.join("app/project.json"),
        json!({
            "name": "app",
            "targets": {
                "build": {
                    "executor": "nx:run-commands",
                    "cache": true,
                    "inputs": ["{projectRoot}/source.txt"],
                    "outputs": ["{projectRoot}/result.txt"],
                    "options": {"cwd": "app", "command": "echo run >> runs.txt && cp source.txt result.txt"}
                }
            }
        })
        .to_string(),
    );
    write_file(&root.join("app/source.txt"), "one");
}

/// The environment of every stock Nx process here: only the managed `PATH` is inherited, every
/// Nx path lives inside the fixture, and loopback is never sent through a proxy.
pub(super) fn scrub(command: &mut Command, root: &Path) {
    command
        .env_clear()
        .env(
            "PATH",
            std::env::var_os("PATH").expect("PATH is set for the managed toolchain"),
        )
        .env("HOME", root)
        .env("NO_COLOR", "1")
        .env("NX_DAEMON", "false")
        .env("NX_ISOLATE_PLUGINS", "false")
        .env("NX_USE_LOCAL", "true")
        .env("NX_WORKSPACE_ROOT_PATH", root)
        .env("NX_SOCKET_DIR", root.join("sockets"))
        .env(
            "NX_WORKSPACE_DATA_DIRECTORY",
            root.join(".nx/workspace-data"),
        )
        .env("NX_CACHE_DIRECTORY", root.join(".nx/cache"))
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost");
}

/// Runs the stock `nx run app:build` once. With `remote`, the client's self-hosted cache points at
/// that origin; with a receipt, the plugin records a hint for this run.
async fn nx_run(root: &Path, remote: Option<&str>, receipt: Option<&str>) -> Output {
    let mut command = Command::new("node");
    command
        .arg(root.join("node_modules/.bin/nx"))
        .args(["run", "app:build", "--outputStyle=static"])
        .current_dir(root);
    scrub(&mut command, root);
    if let Some(id) = receipt {
        command.env("COWSHED_NX_TASK_RECEIPT_ID", id);
    }
    if let Some(origin) = remote {
        command.env("NX_SELF_HOSTED_REMOTE_CACHE_SERVER", origin);
    }
    command.output_bounded().await.expect("run the stock Nx")
}

/// Real executions of `app:build`: one `run` line each. A remote restore appends nothing, and a
/// run that failed before executing leaves no marker at all, which is zero executions.
pub(super) fn executions(root: &Path) -> usize {
    match std::fs::read_to_string(root.join("app/runs.txt")) {
        Ok(text) => text.lines().count(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => panic!("read the execution marker: {error}"),
    }
}

pub(super) fn run_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The task hashes the plugin recorded for one receipt, one per cacheable task in the run.
fn recorded_hashes(root: &Path, receipt: &str) -> Vec<String> {
    let namespace = root.join(PLUGIN_RECEIPTS).join(receipt);
    let mut hashes = Vec::new();
    for entry in std::fs::read_dir(&namespace)
        .unwrap_or_else(|error| panic!("hint namespace {}: {error}", namespace.display()))
    {
        let file = entry.expect("hint entry").path();
        let hint: Value = serde_json::from_slice(&std::fs::read(&file).expect("read hint"))
            .expect("hint is JSON");
        for task in hint["tasks"].as_array().expect("hint lists its tasks") {
            hashes.push(
                task["task"]["hash"]
                    .as_str()
                    .expect("task names its hash")
                    .to_owned(),
            );
        }
    }
    hashes
}

/// The single task hash the plugin recorded for a receipt.
fn recorded_hash(root: &Path, receipt: &str) -> String {
    let mut hashes = recorded_hashes(root, receipt);
    assert_eq!(hashes.len(), 1, "one cacheable task per run");
    hashes.pop().expect("one hash")
}

/// A fixture backend that answers each request by its method from a fixed table and records its
/// request line. Connections are bounded, so the task always ends.
struct FixtureBackend {
    address: std::net::SocketAddr,
    lines: tokio::sync::mpsc::UnboundedReceiver<String>,
    task: tokio::task::JoinHandle<tokio::net::TcpListener>,
}

async fn spawn_backend(get: u16, put: u16) -> FixtureBackend {
    let listener = super::fixture_ports::bind_fixture_listener().await;
    let address = listener.local_addr().expect("backend fixture address");
    let (sender, lines) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        for _ in 0..FIXTURE_CONNECTION_LIMIT {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut head = Vec::new();
            let mut byte = [0_u8; 1];
            while head.len() < FIXTURE_HEAD_LIMIT && !head.ends_with(b"\r\n\r\n") {
                match stream.read(&mut byte).await {
                    Ok(1) => head.push(byte[0]),
                    _ => break,
                }
            }
            let head = String::from_utf8_lossy(&head).into_owned();
            let line = head.lines().next().unwrap_or_default().to_owned();
            let method = line
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_owned();
            let _ = sender.send(line);
            let status = match method.as_str() {
                "GET" => get,
                "PUT" => put,
                _ => 400,
            };
            let reply = format!(
                "HTTP/1.1 {status} fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let _ = stream.write_all(reply.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
        listener
    });
    FixtureBackend {
        address,
        lines,
        task,
    }
}

/// A loopback address that nothing listens on: a configured backend whose socket is dead.
fn dead_address() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("reserve a loopback port");
    let address = listener.local_addr().expect("reserved address");
    drop(listener);
    address
}

fn backend_config(address: std::net::SocketAddr) -> NxCacheBackend {
    NxCacheBackend {
        repo_id: REPO.to_owned(),
        address,
        reclaimed_staging_files: 0,
    }
}

/// A session on its own listener for the project.
async fn install_session(
    handle: &cowshed_gateway::GatewayHandle,
    workspace_id: &str,
    token_byte: u8,
) -> std::net::SocketAddr {
    let (session, _, _) = super::session(
        workspace_id,
        REPO,
        super::block_endpoint(super::free_endpoint()),
        token_byte,
        1,
        WorkspacePolicy::default(),
    );
    super::install_in_free_block(handle, session).await
}

/// The Nx route's audit events for one hash and method: the HTTP status and classification of each.
pub(super) fn nx_events(
    audit: &super::NxRecordingAudit,
    hash: &str,
    method: &str,
) -> Vec<(Option<u16>, Option<String>)> {
    let path = format!("/nx/v1/cache/{hash}");
    audit
        .events
        .lock()
        .expect("audit events")
        .iter()
        .filter(|event| {
            event.path.as_deref() == Some(path.as_str()) && event.method.as_deref() == Some(method)
        })
        .map(|event| (event.http_status, event.classification.clone()))
        .collect()
}

pub(super) async fn gateway_with(
    config: cowshed_gateway::GatewayConfig,
    health: super::UpstreamHealth,
) -> (cowshed_gateway::Gateway, Arc<super::NxRecordingAudit>) {
    let audit = Arc::new(super::NxRecordingAudit::default());
    let gateway = super::gateway(
        config,
        Arc::new(super::NoCredentials),
        Arc::new(super::LocalConnector {
            health,
            observed: None,
        }),
        audit.clone(),
    )
    .await;
    (gateway, audit)
}

/// An unavailable project backend: the stock client's GET is a 404 miss and its PUT a 403 refusal,
/// both audited, and the build executes locally once and succeeds.
fn assert_unavailable_backend(
    root: &Path,
    output: &Output,
    audit: &super::NxRecordingAudit,
    hash: &str,
) {
    assert!(output.status.success(), "{}", run_text(output));
    assert_eq!(
        executions(root),
        1,
        "a remote restore must not stand in for execution"
    );
    let gets = nx_events(audit, hash, "GET");
    assert!(
        gets.iter().any(|(status, class)| *status == Some(404)
            && class.as_deref() == Some("nx-backend-unavailable")),
        "GET must be a 404 outage: {gets:?}"
    );
    let puts = nx_events(audit, hash, "PUT");
    assert!(
        puts.iter().any(|(status, class)| *status == Some(403)
            && class.as_deref() == Some("nx-backend-unavailable")),
        "PUT to an unavailable backend must be a 403 refusal: {puts:?}"
    );
}

/// Nothing configured: the installed Nx cache service is unavailable, so the client's GET is a
/// 404 and its PUT a 403, and the build executes locally.
#[tokio::test]
async fn real_nx_client_with_no_service_config_executes_locally() {
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-outage-noconfig-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let (config, _cache) = super::test_config();
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let endpoint = install_session(&gateway.handle(), "nx-outage-noconfig", 71).await;
    let origin = format!("http://{endpoint}/nx");
    let output = nx_run(root.path(), Some(origin.as_str()), Some(RUN_RECEIPT)).await;
    let hash = recorded_hash(root.path(), RUN_RECEIPT);
    assert_unavailable_backend(root.path(), &output, &audit, &hash);
    gateway.drain().await.expect("drain gateway");
}

/// A configured service whose `PATH` has no `bun`: the start fails before any process is spawned,
/// its failure becomes a cooldown, and the client's GET is refused as an outage. The script exists,
/// so the missing interpreter is the only fault.
#[tokio::test]
async fn real_nx_client_with_no_bun_on_path_executes_locally() {
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-outage-missing-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let (mut config, _cache) = super::test_config();
    let store = root.path().join("nx-store");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&store)
        .expect("create the private Nx store under the fixture root");
    let script = root.path().join("nx-cache-server.js");
    std::fs::write(&script, b"").expect("write the fixture server script");
    let empty = root.path().join("empty-bin");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&empty)
        .expect("create a PATH directory without bun");
    config.nx_cache = Some(NxCacheServiceConfig {
        script,
        path: empty.into_os_string(),
        directory: store,
        max_bytes: 1024 * 1024,
        cap_bytes: 64 * 1024 * 1024,
    });
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let endpoint = install_session(&gateway.handle(), "nx-outage-missing", 74).await;
    let origin = format!("http://{endpoint}/nx");
    let output = nx_run(root.path(), Some(origin.as_str()), Some(RUN_RECEIPT)).await;
    let hash = recorded_hash(root.path(), RUN_RECEIPT);
    assert_unavailable_backend(root.path(), &output, &audit, &hash);
    gateway.drain().await.expect("drain gateway");
}

/// A configured backend whose socket is dead: the gateway's connect fails, the route answers as an
/// outage, and the build executes locally once and succeeds.
#[tokio::test]
async fn real_nx_client_with_dead_backend_socket_executes_locally() {
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-outage-dead-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let (config, _cache) = super::test_config();
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let handle = gateway.handle();
    handle
        .configure_nx_cache(backend_config(dead_address()))
        .await
        .expect("configure dead backend");
    let endpoint = install_session(&handle, "nx-outage-dead", 75).await;
    let origin = format!("http://{endpoint}/nx");
    let output = nx_run(root.path(), Some(origin.as_str()), Some(RUN_RECEIPT)).await;
    let hash = recorded_hash(root.path(), RUN_RECEIPT);
    assert_unavailable_backend(root.path(), &output, &audit, &hash);
    gateway.drain().await.expect("drain gateway");
}

/// The connector is offline. The route degrades before any connect, so the fixture backend receives
/// no request at all, the client's GET is a 404, and the build executes once and succeeds.
#[tokio::test]
async fn real_nx_client_with_offline_connector_never_reaches_the_backend() {
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-outage-offline-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let (config, _cache) = super::test_config();
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Offline).await;
    let handle = gateway.handle();
    let mut backend = spawn_backend(200, 200).await;
    handle
        .configure_nx_cache(backend_config(backend.address))
        .await
        .expect("configure fixture backend");
    let endpoint = install_session(&handle, "nx-outage-offline", 76).await;
    let origin = format!("http://{endpoint}/nx");
    let output = nx_run(root.path(), Some(origin.as_str()), Some(RUN_RECEIPT)).await;
    let hash = recorded_hash(root.path(), RUN_RECEIPT);
    assert_unavailable_backend(root.path(), &output, &audit, &hash);
    gateway.drain().await.expect("drain gateway");
    backend.task.abort();
    assert!(
        backend.lines.try_recv().is_err(),
        "an offline connector must not reach the backend"
    );
}

/// The real stock client must execute after an upstream 503 is mapped to a gateway miss.
#[tokio::test]
async fn real_nx_client_with_503_backend_executes_locally() {
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-outage-upstream503-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let (config, _cache) = super::test_config();
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let handle = gateway.handle();
    let mut backend = spawn_backend(503, 503).await;
    handle
        .configure_nx_cache(backend_config(backend.address))
        .await
        .expect("configure 503 backend");
    let endpoint = install_session(&handle, "nx-outage-upstream503", 77).await;
    let origin = format!("http://{endpoint}/nx");
    let output = nx_run(root.path(), Some(origin.as_str()), Some(RUN_RECEIPT)).await;
    assert!(output.status.success(), "{}", run_text(&output));
    let hash = recorded_hash(root.path(), RUN_RECEIPT);
    assert_unavailable_backend(root.path(), &output, &audit, &hash);
    let line = backend
        .lines
        .try_recv()
        .expect("the backend received a GET");
    assert!(
        line.starts_with(&format!("GET /v1/cache/{hash} ")),
        "{line}"
    );
    gateway.drain().await.expect("drain gateway");
    backend.task.abort();
}

/// The upstream closes after a committed 200 and a visible prefix, not after the declared length.
#[tokio::test]
async fn nx_short_content_length_is_a_failed_transfer_not_a_completed_hit() {
    let listener = super::fixture_ports::bind_fixture_listener().await;
    let address = listener.local_addr().expect("short-body backend");
    let (close, closed) = tokio::sync::oneshot::channel();
    let backend = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("backend connection");
        let mut head = Vec::new();
        let mut byte = [0; 1];
        while !head.ends_with(b"\r\n\r\n") {
            assert!(head.len() < FIXTURE_HEAD_LIMIT);
            stream
                .read_exact(&mut byte)
                .await
                .expect("backend request head");
            head.push(byte[0]);
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\nConnection: close\r\n\r\nshort")
            .await
            .expect("committed response prefix");
        closed.await.expect("client observed the prefix");
        stream.shutdown().await.expect("close the incomplete body");
    });
    let (config, _cache) = super::test_config();
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let handle = gateway.handle();
    handle
        .configure_nx_cache(backend_config(address))
        .await
        .expect("short-body backend");
    let endpoint = install_session(&handle, "nx-short-body", 78).await;
    let mut client = tokio::net::TcpStream::connect(endpoint)
        .await
        .expect("gateway client");
    client
        .write_all(super::nx_request(endpoint, "GET", "42").as_bytes())
        .await
        .expect("cache request");
    let mut head = Vec::new();
    let mut byte = [0; 1];
    while !head.ends_with(b"\r\n\r\n") {
        assert!(head.len() < FIXTURE_HEAD_LIMIT);
        client.read_exact(&mut byte).await.expect("response head");
        head.push(byte[0]);
    }
    let head = String::from_utf8(head).expect("HTTP head");
    assert!(head.starts_with("HTTP/1.1 200 "), "{head}");
    assert!(
        head.to_ascii_lowercase().contains("content-length: 16"),
        "{head}"
    );
    let mut prefix = [0; 5];
    client
        .read_exact(&mut prefix)
        .await
        .expect("visible body prefix");
    assert_eq!(&prefix, b"short");
    close
        .send(())
        .expect("cut the upstream after the committed prefix");
    let mut suffix = Vec::new();
    let ended = client.read_to_end(&mut suffix).await;
    if let Err(error) = ended {
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
            ),
            "unexpected client failure: {error}"
        );
    }
    assert!(
        prefix.len() + suffix.len() < 16,
        "the client must see a short body"
    );
    drop(client);
    backend.await.expect("backend fixture completed");
    gateway.drain().await.expect("drain gateway");
    let events = audit.events.lock().expect("audit events");
    let reads: Vec<_> = events
        .iter()
        .filter(|event| {
            event.path.as_deref() == Some("/nx/v1/cache/42")
                && event.method.as_deref() == Some("GET")
        })
        .collect();
    assert!(
        reads.iter().any(|event| event.http_status == Some(200)
            && event.status == cowshed_gateway::AuditStatus::Failed
            && event.classification.as_deref() == Some("nx-transfer-failed")),
        "{reads:?}"
    );
    assert!(
        !reads
            .iter()
            .any(|event| event.status == cowshed_gateway::AuditStatus::Completed),
        "a short stream is never a completed read: {reads:?}"
    );
}

/// Every request line the fixture backend received, in arrival order. The backend queues each line
/// before it answers, so a finished run's requests are all queued.
fn received(backend: &mut FixtureBackend) -> Vec<String> {
    let mut lines = Vec::new();
    while let Ok(line) = backend.lines.try_recv() {
        lines.push(line);
    }
    lines
}

/// A direct run's outage (05_gateway.md "The client half"): exactly one line, naming the server,
/// the request and the backend's 503.
fn assert_direct_outage(output: &Output, origin: &str, request: &str) {
    let text = run_text(output);
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| line.contains(" is unavailable ("))
        .collect();
    let named = format!("Remote cache {origin} is unavailable ({request}: ");
    assert!(
        matches!(lines.as_slice(), [line]
            if line.starts_with(&named) && line.contains("Unexpected response status: 503")),
        "one outage line naming {origin}, {request} and the 503: {text}"
    );
}

/// Direct control A: the client talks to the fixture backend with no gateway, so nothing maps the
/// backend's GET 503 to a miss. The client reads the 5xx as its own outage (05_gateway.md "The
/// client half"): the lookup misses, the build executes once and passes, one line names the server
/// and the request, and the process asks the backend nothing more, so no PUT follows.
#[tokio::test]
async fn direct_nx_client_get_503_is_an_outage_miss_that_executes_locally() {
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-outage-direct503-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let mut backend = spawn_backend(503, 503).await;
    let origin = format!("http://{}", backend.address);
    let output = nx_run(root.path(), Some(origin.as_str()), Some(RUN_RECEIPT)).await;
    assert!(
        output.status.success(),
        "a direct GET 503 is a miss, never a failed run: {}",
        run_text(&output)
    );
    assert_eq!(
        executions(root.path()),
        1,
        "the missed lookup executes the build once"
    );
    let hash = recorded_hash(root.path(), RUN_RECEIPT);
    assert_direct_outage(&output, &origin, &format!("GET /v1/cache/{hash}"));
    let requests = received(&mut backend);
    let lookup = format!("GET /v1/cache/{hash} ");
    assert!(
        matches!(requests.as_slice(), [only] if only.starts_with(&lookup)),
        "one lookup, after whose outage the client asks nothing more: {requests:?}"
    );
    backend.task.abort();
}

/// Direct control B: the fixture backend answers GET 404 and PUT 503 with no gateway. The 404 is a
/// miss, so the build executes once. The store's 503 is the client's own outage (05_gateway.md
/// "The client half"): the store is skipped, the run passes with the task's own result, one line
/// names the PUT, and that PUT is the last request the backend sees.
#[tokio::test]
async fn direct_nx_client_get_404_put_503_skips_the_store_and_keeps_the_local_result() {
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-outage-direct-put503-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let mut backend = spawn_backend(404, 503).await;
    let origin = format!("http://{}", backend.address);
    let output = nx_run(root.path(), Some(origin.as_str()), Some(RUN_RECEIPT)).await;
    assert!(
        output.status.success(),
        "a direct PUT 503 skips the store, never fails the run: {}",
        run_text(&output)
    );
    assert_eq!(
        executions(root.path()),
        1,
        "the GET 404 is a miss, so the build executes once"
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("app/result.txt"))
            .expect("read the task's result"),
        "one",
        "the skipped store keeps the task's own result"
    );
    let hash = recorded_hash(root.path(), RUN_RECEIPT);
    assert_direct_outage(&output, &origin, &format!("PUT /v1/cache/{hash}"));
    let requests = received(&mut backend);
    let lookup = format!("GET /v1/cache/{hash} ");
    let store = format!("PUT /v1/cache/{hash} ");
    assert!(
        matches!(requests.split_last(), Some((last, lookups))
            if last.starts_with(&store)
                && !lookups.is_empty()
                && lookups.iter().all(|line| line.starts_with(&lookup))),
        "lookups, then the one store after whose outage the client asks nothing more: {requests:?}"
    );
    backend.task.abort();
}

/// A store through the gateway to a 503 backend, with the stock store called directly.
///
/// The source workspace runs locally once with a receipt, which gives the cached task's root and
/// hash. The native `HttpRemoteCache.store` is then called on the gateway's cache for that hash,
/// with no credential, and must report `false` without a Node error. The gateway forwards the PUT,
/// the backend's 503 is an outage, and the audit records a 403 failed `nx-backend-unavailable`. The
/// source marker stays at 1.
#[tokio::test]
async fn native_store_through_gateway_to_503_backend_is_audited_and_returns_false() {
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-outage-store-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let (config, _cache) = super::test_config();
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let handle = gateway.handle();
    let mut backend = spawn_backend(404, 503).await;
    handle
        .configure_nx_cache(backend_config(backend.address))
        .await
        .expect("configure fixture backend");
    let endpoint = install_session(&handle, "nx-outage-publisher", 79).await;

    let built = nx_run(root.path(), None, Some(PUBLISH_RECEIPT)).await;
    assert!(built.status.success(), "{}", run_text(&built));
    assert_eq!(executions(root.path()), 1, "the local run executes once");
    let hash = recorded_hash(root.path(), PUBLISH_RECEIPT);

    let origin = format!("http://{endpoint}/nx");
    let store = Command::new("node")
        .arg("-e")
        .arg(
            "const { HttpRemoteCache } = require('nx/src/native');\
             Promise.resolve(new HttpRemoteCache().store(process.argv[1], process.argv[2], '', 0))\
             .then((ok) => process.stdout.write(JSON.stringify({ ok })), (error) => { process.stderr.write(String(error)); process.exit(3); });",
        )
        .arg(&hash)
        .arg(root.path().join(".nx/cache"))
        .current_dir(root.path())
        .env_clear()
        .env(
            "PATH",
            std::env::var_os("PATH").expect("PATH is set for the managed toolchain"),
        )
        .env("HOME", root.path())
        .env("NX_DAEMON", "false")
        .env("NX_CACHE_DIRECTORY", root.path().join(".nx/cache"))
        .env("NX_SELF_HOSTED_REMOTE_CACHE_SERVER", &origin)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .output_bounded()
        .await
        .expect("run the native store");
    assert!(store.status.success(), "{}", run_text(&store));
    let answer: Value = serde_json::from_slice(&store.stdout).expect("store answers JSON");
    assert_eq!(answer["ok"], json!(false), "{answer}");
    assert_eq!(
        executions(root.path()),
        1,
        "a store upload executes nothing"
    );

    let put_line = loop {
        let line = backend.lines.recv().await.expect("backend saw the PUT");
        if line.starts_with("PUT /v1/cache/") {
            break line;
        }
    };
    assert!(
        put_line.starts_with(&format!("PUT /v1/cache/{hash} ")),
        "{put_line}"
    );
    let puts = nx_events(&audit, &hash, "PUT");
    assert!(
        puts.iter().any(|(status, class)| *status == Some(403)
            && class.as_deref() == Some("nx-backend-unavailable")),
        "the gateway PUT must be audited as a failed outage: {puts:?}"
    );
    gateway.drain().await.expect("drain gateway");
    backend.task.abort();
}

/// Ambiguous and non-canonical hash paths are refused before admission by the raw path check, or
/// because the route does not match, and both are request errors answered 400. None reaches a backend.
#[tokio::test]
async fn nx_route_ambiguous_paths_are_refused_without_an_upstream_request() {
    let (config, _cache) = super::test_config();
    let (gateway, _audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let handle = gateway.handle();
    let mut backend = spawn_backend(200, 200).await;
    handle
        .configure_nx_cache(backend_config(backend.address))
        .await
        .expect("configure fixture backend");
    let endpoint = install_session(&handle, "nx-outage-ambiguous", 83).await;
    for path in [
        "/nx/v1/cache/42?query=1",
        "/nx/v1/cache/42/",
        "/nx/v1/cache/4%32",
        "/nx/v1/cache/42.json",
        "/nx/v1/cache/./42",
        "/nx/v1/cache/..%2f42",
        "/nx/v1/cache/42;tail",
    ] {
        let response = super::proxy_request(
            endpoint,
            format!("GET {path} HTTP/1.1\r\nHost: {endpoint}\r\nConnection: close\r\n\r\n"),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "{path}: {response}");
    }
    gateway.drain().await.expect("drain gateway");
    backend.task.abort();
    assert!(
        backend.lines.try_recv().is_err(),
        "an ambiguous path must not reach the backend"
    );
}

/// Every rendering of a task key Nx can send reaches the backend: stock Nx's decimal xxhash and a
/// custom executor hasher's value (sha256 hex here) are both one validated key, 1-128 characters of
/// `[A-Za-z0-9_-]`. A key one character longer is not, and is refused before any upstream request.
#[tokio::test]
async fn nx_route_accepts_every_key_the_backend_accepts() {
    let (config, _cache) = super::test_config();
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let handle = gateway.handle();
    let mut backend = spawn_backend(200, 200).await;
    handle
        .configure_nx_cache(backend_config(backend.address))
        .await
        .expect("configure fixture backend");
    let endpoint = install_session(&handle, "nx-key-format", 85).await;
    let get = |hash: &str| {
        super::proxy_request(
            endpoint,
            format!(
                "GET /nx/v1/cache/{hash} HTTP/1.1\r\nHost: {endpoint}\r\nConnection: close\r\n\r\n"
            ),
        )
    };
    let longest = "a".repeat(128);
    for hash in [
        "10018087211778381691",
        "6f1e2a0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f",
        "Ab9_-z",
        longest.as_str(),
    ] {
        let response = get(hash).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{hash}: {response}");
        let line = backend.lines.recv().await.expect("backend saw the GET");
        assert!(
            line.starts_with(&format!("GET /v1/cache/{hash} ")),
            "{hash}: {line}"
        );
        assert_eq!(
            nx_events(&audit, hash, "GET").last(),
            Some(&(Some(200), None)),
            "{hash}"
        );
    }
    let too_long = "a".repeat(129);
    let response = get(&too_long).await;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(
        nx_events(&audit, &too_long, "GET")
            .iter()
            .any(|(status, class)| *status == Some(400)
                && class.as_deref() == Some("invalid-target")),
        "an oversized key is an audited invalid target"
    );
    gateway.drain().await.expect("drain gateway");
    backend.task.abort();
    assert!(
        backend.lines.try_recv().is_err(),
        "an oversized key must not reach the backend"
    );
}

/// One fresh-root cold measurement. `gateway` selects the normal read-only Gateway route, with a
/// fixture backend that answers GET 404; `false` runs the stock client with no remote endpoint.
/// Returns one observation row. Nothing here asserts a speed: the owner derives medians from rows.
async fn cold_observation(gateway: bool, repetition: usize) -> Value {
    let label = if gateway { "gateway" } else { "no-endpoint" };
    let root = super::fixture_dir::secure_fixture_dir(&format!(
        "cowshed-nx-cold-{label}-{repetition}-{}",
        std::process::id()
    ));
    write_workspace(root.path());
    let receipt = format!("{:032x}", 0x8000 + repetition);
    if !gateway {
        let started = std::time::Instant::now();
        let output = nx_run(root.path(), None, Some(receipt.as_str())).await;
        let wall_ms = started.elapsed().as_millis();
        assert!(output.status.success(), "{}", run_text(&output));
        let hashes = recorded_hashes(root.path(), &receipt);
        return json!({
            "mode": label,
            "repetition": repetition,
            "wallMs": wall_ms,
            "cacheableTasks": hashes.len(),
            "marker": executions(root.path()),
            "getStatuses": [],
            "putStatuses": [],
            "backendInvocations": 0,
        });
    }
    let (config, _cache) = super::test_config();
    let (gateway, audit) = gateway_with(config, super::UpstreamHealth::Healthy).await;
    let handle = gateway.handle();
    let mut backend = spawn_backend(404, 503).await;
    handle
        .configure_nx_cache(backend_config(backend.address))
        .await
        .expect("configure fixture backend");
    let endpoint = install_session(&handle, "nx-cold-reader", 84).await;
    let origin = format!("http://{endpoint}/nx");
    let started = std::time::Instant::now();
    let output = nx_run(root.path(), Some(origin.as_str()), Some(receipt.as_str())).await;
    let wall_ms = started.elapsed().as_millis();
    assert!(output.status.success(), "{}", run_text(&output));
    let hashes = recorded_hashes(root.path(), &receipt);
    gateway.drain().await.expect("drain gateway");
    backend.task.abort();
    let mut invocations = 0_usize;
    while backend.lines.try_recv().is_ok() {
        invocations += 1;
    }
    let mut get_statuses = Vec::new();
    let mut put_statuses = Vec::new();
    for hash in &hashes {
        get_statuses.extend(
            nx_events(&audit, hash, "GET")
                .into_iter()
                .map(|(status, _)| status),
        );
        put_statuses.extend(
            nx_events(&audit, hash, "PUT")
                .into_iter()
                .map(|(status, _)| status),
        );
    }
    json!({
        "mode": label,
        "repetition": repetition,
        "wallMs": wall_ms,
        "cacheableTasks": hashes.len(),
        "marker": executions(root.path()),
        "getStatuses": get_statuses,
        "putStatuses": put_statuses,
        "backendInvocations": invocations,
    })
}

/// Three fresh-root repetitions of the cold run, with and without a Gateway read-only endpoint.
/// Each repetition prints one JSON row. The only asserts are that each run succeeds and executes
/// once; the rows are observations, not speed claims or zero-overhead claims.
#[tokio::test]
async fn real_nx_client_cold_cost_observation() {
    for repetition in 0..3 {
        for gateway in [false, true] {
            let row = cold_observation(gateway, repetition).await;
            assert_eq!(row["marker"], json!(1), "{row}");
            println!("NX_COLD_OBSERVATION {row}");
        }
    }
}
