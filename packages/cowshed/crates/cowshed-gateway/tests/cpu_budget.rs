#![cfg(target_os = "macos")]
//! The host CPU budget over a real control socket: grants, fairness between checkouts, the
//! ledger, and tokens coming back when a holder is killed.

use std::io::{BufRead as _, BufReader as StdBufReader, Write as _};
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CpuBudgetLimits,
    CredentialError, CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use cowshed_gateway_types::{CpuBudgetStatus, CpuTokensRequest};
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

/// A gateway with a budget of `tokens` on a socket under `/tmp` (a bind path is capped at
/// `sun_path`'s 104 bytes), and the root to remove once it has drained.
async fn gateway(tokens: usize) -> (Gateway, PathBuf, PathBuf) {
    let id = Uuid::new_v4().simple().to_string();
    let root = PathBuf::from(format!("/tmp/cscpu-{}", &id[..8]));
    std::fs::create_dir(&root).expect("create fixture root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure fixture root");
    let cache = root.join("cache");
    std::fs::create_dir(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = root.join("gateway.sock");
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            mirror_cache: MirrorCacheConfig::new(cache),
            cpu_budget: CpuBudgetLimits {
                tokens: NonZeroUsize::new(tokens).unwrap(),
            },
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

fn request(want: usize, checkout: &str) -> String {
    let mut line = serde_json::to_string(&CpuTokensRequest::new(
        NonZeroUsize::new(want).unwrap(),
        checkout,
        "cargo nextest run",
    ))
    .expect("encode");
    line.push('\n');
    line
}

/// One held `cpu-tokens` connection in this process, answered `queued`.
struct Asker {
    lines: BufReader<tokio::net::unix::OwnedReadHalf>,
    _writer: tokio::net::unix::OwnedWriteHalf,
}

impl Asker {
    async fn jobserver(socket: &Path, want: usize, checkout: &str, wait_ms: u32) -> Self {
        let stream = UnixStream::connect(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        let request = serde_json::json!({ "op": "jobserver", "want": want, "checkout": checkout,
            "command": "land gate", "class": "gate", "waitMs": wait_ms });
        writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("ask gate");
        let mut asker = Self {
            lines: BufReader::new(reader),
            _writer: writer,
        };
        assert_eq!(asker.answer().await["lease"], "queued");
        asker
    }

    /// A gate job's root request with its trusted job identity, as the supervisor sends it; the
    /// grant names the job's private CPU pool.
    async fn gate_job(socket: &Path, checkout: &str) -> (Self, PathBuf) {
        let stream = UnixStream::connect(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        let request = serde_json::json!({ "op": "jobserver", "want": 1, "checkout": checkout,
            "command": "nx run-many -t gate", "class": "gate", "waitMs": 120_000, "pool": true,
            "job": { "repoId": "owner/repo", "workspaceIncarnation": "0123456789abcdef0123456789abcdef",
                "jobId": 1 } });
        writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("ask gate job");
        let mut asker = Self {
            lines: BufReader::new(reader),
            _writer: writer,
        };
        let queued = asker.answer().await;
        assert_eq!(queued["lease"], "queued", "{queued}");
        let granted = tokio::time::timeout(Duration::from_secs(10), asker.answer())
            .await
            .expect("gate job granted");
        assert_eq!(granted["tokens"], 1, "{granted}");
        let pool = PathBuf::from(
            granted["cpuPool"]
                .as_str()
                .expect("a gate job names its pool"),
        );
        (asker, pool)
    }

    /// One child's ask on a job's CPU pool, as a nested `bounded-exec` or Cargo sends it.
    async fn child(pool: &Path, want: usize) -> Self {
        let stream = UnixStream::connect(pool).await.expect("connect pool");
        let (reader, mut writer) = stream.into_split();
        writer
            .write_all(format!("{{\"op\":\"cpu-pool\",\"want\":{want}}}\n").as_bytes())
            .await
            .expect("ask pool");
        let mut asker = Self {
            lines: BufReader::new(reader),
            _writer: writer,
        };
        assert_eq!(asker.answer().await["lease"], "queued");
        asker
    }

    async fn ask(socket: &Path, want: usize, checkout: &str) -> Self {
        let stream = UnixStream::connect(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        writer
            .write_all(request(want, checkout).as_bytes())
            .await
            .expect("ask");
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

    /// The tokens granted within `within`, or `None`.
    async fn granted(&mut self, within: Duration) -> Option<u64> {
        let answer = tokio::time::timeout(within, self.answer()).await.ok()?;
        assert_eq!(answer["lease"], "granted", "{answer}");
        Some(answer["tokens"].as_u64().expect("a grant names its tokens"))
    }
}

/// A one-shot control request's answer.
async fn one_shot(socket: &Path, line: &str) -> Value {
    let mut stream = UnixStream::connect(socket).await.expect("connect");
    stream.write_all(line.as_bytes()).await.expect("ask");
    stream.shutdown().await.expect("half-close");
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.expect("answer");
    serde_json::from_str(answer.trim_end()).expect("an answer is JSON")
}

async fn ledger(socket: &Path) -> CpuBudgetStatus {
    let answer = one_shot(socket, "{\"op\":\"cpu-budget\"}\n").await;
    serde_json::from_value(answer["cpuBudget"].clone()).expect("a ledger")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_killed_with_sigkill_returns_its_tokens_at_once() {
    let (gateway, root, socket) = gateway(4).await;
    // No cap: the holder takes the whole host, so a second checkout queues whole.
    // The holder is another process, so SIGKILL is a real death: the kernel closes its socket.
    let mut holder = Command::new("/usr/bin/nc")
        .arg("-U")
        .arg(&socket)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn nc");
    let mut input = holder.stdin.take().expect("stdin");
    input
        .write_all(request(4, "/w/killed").as_bytes())
        .expect("ask");
    let mut answers = StdBufReader::new(holder.stdout.take().expect("stdout")).lines();
    let answers = tokio::task::spawn_blocking(move || {
        let queued = answers.next().expect("queued").expect("read");
        let granted = answers.next().expect("granted").expect("read");
        (queued, granted)
    })
    .await
    .expect("read answers");
    assert!(answers.0.contains("\"queued\""), "{}", answers.0);
    assert!(answers.1.contains("\"tokens\":4"), "{}", answers.1);

    let mut waiter = Asker::ask(&socket, 2, "/w/waiting").await;
    assert_eq!(waiter.granted(Duration::from_millis(300)).await, None);
    let held = ledger(&socket).await;
    assert_eq!((held.total, held.held), (4, 4));

    holder.kill().expect("SIGKILL the holder");
    holder.wait().expect("reap");
    assert_eq!(
        waiter.granted(Duration::from_secs(10)).await,
        Some(2),
        "the killed holder's tokens go to the waiter"
    );
    let held = ledger(&socket).await;
    assert_eq!(held.held, 2);
    assert_eq!(
        held.checkouts
            .iter()
            .map(|checkout| (checkout.checkout.as_str(), checkout.held))
            .collect::<Vec<_>>(),
        [("/w/waiting", 2)]
    );
    drop(input);

    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkouts_share_the_host_and_a_lone_grant_fills_it() {
    let (gateway, root, socket) = gateway(8).await;

    // Alone, one grant fills the host; no half is reserved.
    let mut whole = Asker::ask(&socket, 64, "/w/a").await;
    assert_eq!(whole.granted(Duration::from_secs(10)).await, Some(8));
    let mut a_next = Asker::ask(&socket, 8, "/w/a").await;
    assert_eq!(a_next.granted(Duration::from_millis(300)).await, None);
    let mut b = Asker::ask(&socket, 8, "/w/b").await;
    assert_eq!(b.granted(Duration::from_millis(300)).await, None);

    // The runner returns 8: each waiting checkout takes its share of 4.
    drop(whole);
    assert_eq!(a_next.granted(Duration::from_secs(10)).await, Some(4));
    assert_eq!(b.granted(Duration::from_secs(10)).await, Some(4));
    let held = ledger(&socket).await;
    assert_eq!(held.held, 8);
    assert_eq!(
        held.checkouts
            .iter()
            .map(|checkout| (checkout.checkout.as_str(), checkout.held))
            .collect::<Vec<_>>(),
        [("/w/a", 4), ("/w/b", 4)]
    );

    // A refused request names why and holds nothing.
    let refused = one_shot(
        &socket,
        "{\"op\":\"cpu-tokens\",\"want\":0,\"checkout\":\"/w/c\",\"command\":\"x\"}\n",
    )
    .await;
    assert_eq!(refused["ok"], false, "{refused}");
    assert_eq!(refused["code"], "invalid-request", "{refused}");

    drop((a_next, b));
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jobserver_grant_names_a_fifo_and_close_returns_every_token() {
    let (gateway, root, socket) = gateway(4).await;
    let stream = UnixStream::connect(&socket).await.expect("connect");
    let (reader, mut writer) = stream.into_split();
    writer
        .write_all(b"{\"op\":\"jobserver\",\"want\":4,\"checkout\":\"/w/js\",\"command\":\"cargo build\"}\n")
        .await
        .expect("ask");
    let mut lines = BufReader::new(reader);
    let mut line = String::new();
    lines.read_line(&mut line).await.expect("queued");
    let queued: Value = serde_json::from_str(&line).expect("queued is JSON");
    assert_eq!(queued["lease"], "queued", "{queued}");
    line.clear();
    lines.read_line(&mut line).await.expect("granted");
    let granted: Value = serde_json::from_str(&line).expect("granted is JSON");
    assert_eq!(granted["lease"], "granted", "{granted}");
    assert_eq!(granted["tokens"], 4, "{granted}");
    let fifo = PathBuf::from(granted["fifo"].as_str().expect("a grant names its fifo"));
    assert_eq!(ledger(&socket).await.held, 4);

    // The fifo holds g-1 token bytes: read them, put them back, then take one
    // and close without returning it — every token must still be credited.
    let mut client = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fifo)
        .expect("open fifo");
    let mut bytes = [0_u8; 3];
    std::io::Read::read_exact(&mut client, &mut bytes).expect("read token bytes");
    assert_eq!(bytes, [b'+'; 3], "the fifo holds g-1 token bytes");
    std::io::Write::write_all(&mut client, &bytes).expect("return the bytes");
    let mut taken = [0_u8; 1];
    std::io::Read::read_exact(&mut client, &mut taken).expect("take a token");
    drop(client);
    drop((writer, lines));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while fifo.exists() || ledger(&socket).await.held != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the fifo and its tokens outlive the close"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_takes_the_larger_part_of_returned_tokens_before_older_background_work() {
    let (gateway, root, socket) = gateway(4).await;
    let mut holder = Asker::ask(&socket, 4, "/w/background").await;
    assert_eq!(holder.granted(Duration::from_secs(10)).await, Some(4));
    let mut old_background = Asker::ask(&socket, 4, "/w/same").await;
    let mut gate = Asker::jobserver(&socket, 4, "/w/same", 120_000).await;
    assert_eq!(gate.granted(Duration::from_millis(50)).await, None);
    assert_eq!(
        ledger(&socket)
            .await
            .checkouts
            .iter()
            .find(|checkout| checkout.checkout == "/w/background")
            .unwrap()
            .held,
        4,
        "the in-flight background grant must not be fictionally shrunk"
    );
    drop(holder);
    // The starved background checkout keeps one token; the gate takes the other three.
    assert_eq!(gate.granted(Duration::from_secs(10)).await, Some(3));
    assert_eq!(
        old_background.granted(Duration::from_secs(10)).await,
        Some(1)
    );
    let status = ledger(&socket).await;
    assert_eq!((status.total, status.held), (4, 4));
    drop((gate, old_background));
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_gate_holds_up_neither_background_nor_a_later_gate_while_tokens_are_free() {
    let (gateway, root, socket) = gateway(8).await;
    let mut gate = Asker::jobserver(&socket, 4, "/w/gate", 120_000).await;
    assert_eq!(gate.granted(Duration::from_secs(10)).await, Some(4));
    let mut background = Asker::ask(&socket, 1, "/w/background").await;
    assert_eq!(background.granted(Duration::from_secs(10)).await, Some(1));
    let mut next_gate = Asker::jobserver(&socket, 2, "/w/next-gate", 120_000).await;
    assert_eq!(next_gate.granted(Duration::from_secs(10)).await, Some(2));
    assert_eq!(ledger(&socket).await.held, 7);
    drop((gate, background, next_gate));
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_jobs_pool_admits_each_child_while_older_background_work_stays_queued() {
    let (gateway, root, socket) = gateway(4).await;
    // The land's gate job holds only its orchestrator's token.
    let (gate, pool) = Asker::gate_job(&socket, "/w/land").await;
    let mut a = Asker::ask(&socket, 2, "/w/a").await;
    assert_eq!(a.granted(Duration::from_secs(10)).await, Some(2));
    let mut b = Asker::ask(&socket, 1, "/w/b").await;
    assert_eq!(b.granted(Duration::from_secs(10)).await, Some(1));
    let mut queued = Asker::ask(&socket, 1, "/w/c").await;
    // A land task asks the gate job's pool after c queued. The pool lends nothing of its own: the
    // child waits for the host, not for the gate's single token.
    let mut task = Asker::child(&pool, 2).await;
    assert_eq!(task.granted(Duration::from_millis(50)).await, None);
    drop(b);
    assert_eq!(task.granted(Duration::from_secs(10)).await, Some(1));
    assert_eq!(queued.granted(Duration::from_millis(50)).await, None);
    let status = ledger(&socket).await;
    let land = status
        .checkouts
        .iter()
        .find(|checkout| checkout.checkout == "/w/land")
        .expect("the land holds");
    assert_eq!((land.held, land.running), (2, 2), "{status:?}");
    drop(task);
    assert_eq!(queued.granted(Duration::from_secs(10)).await, Some(1));
    drop((gate, a, queued));
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bounded_gate_timeout_names_unchanged_live_holders_and_leaves_no_queue_barrier() {
    let (gateway, root, socket) = gateway(4).await;
    let mut holder = Asker::ask(&socket, 4, "/w/live-test").await;
    assert_eq!(holder.granted(Duration::from_secs(10)).await, Some(4));
    let mut background = Asker::ask(&socket, 4, "/w/after-timeout").await;
    let asked = std::time::Instant::now();
    let mut gate = Asker::jobserver(&socket, 4, "/w/gate", 60).await;
    let refusal = tokio::time::timeout(Duration::from_secs(10), gate.answer())
        .await
        .expect("bounded refusal");
    assert_eq!(refusal["ok"], false, "{refusal}");
    assert_eq!(refusal["code"], "gate-admission-timeout", "{refusal}");
    assert!(
        refusal["error"]
            .as_str()
            .unwrap()
            .contains("/w/live-test: 4 tokens"),
        "{refusal}"
    );
    assert!(asked.elapsed() >= Duration::from_millis(60));
    assert_eq!(refusal["cpuBudget"]["held"], 4);
    assert_eq!(ledger(&socket).await.held, 4);
    drop((gate, holder));
    assert_eq!(background.granted(Duration::from_secs(10)).await, Some(4));
    drop(background);
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jobserver_class_and_gate_bounds_are_validated_without_changing_ordinary_requests() {
    let (gateway, root, socket) = gateway(4).await;
    for fields in [
        serde_json::json!({"class":"gate"}),
        serde_json::json!({"class":"gate","waitMs":0}),
        serde_json::json!({"class":"pretend-gate","waitMs":100}),
        serde_json::json!({"class":"background","waitMs":100}),
    ] {
        let mut request = serde_json::json!({"op":"jobserver","want":4,"checkout":"/w/refused","command":"cargo test"});
        request
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let refused = one_shot(&socket, &format!("{request}\n")).await;
        assert_eq!(refused["code"], "invalid-request", "{refused}");
    }
    assert_eq!(ledger(&socket).await.held, 0);
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}
