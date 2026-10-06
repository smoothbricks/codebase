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

    let mut waiter = Asker::ask(&socket, 4, "/w/waiting").await;
    assert_eq!(waiter.granted(Duration::from_millis(300)).await, None);
    let held = ledger(&socket).await;
    assert_eq!((held.total, held.held), (4, 4));

    holder.kill().expect("SIGKILL the holder");
    holder.wait().expect("reap");
    assert_eq!(
        waiter.granted(Duration::from_secs(10)).await,
        Some(4),
        "the killed holder's tokens go to the waiter"
    );
    let held = ledger(&socket).await;
    assert_eq!(held.held, 4);
    assert_eq!(held.checkouts.len(), 1);
    assert_eq!(held.checkouts[0].checkout, "/w/waiting");
    drop(input);

    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkouts_share_the_host_and_a_want_past_it_gets_the_host() {
    let (gateway, root, socket) = gateway(8).await;

    let mut whole = Asker::ask(&socket, 64, "/w/a").await;
    assert_eq!(whole.granted(Duration::from_secs(10)).await, Some(8));
    let mut a_next = Asker::ask(&socket, 8, "/w/a").await;
    let mut b = Asker::ask(&socket, 8, "/w/b").await;
    assert_eq!(b.granted(Duration::from_millis(300)).await, None);

    // The host frees all 8. Both checkouts wait, so each gets its share of 4: a's waiter asked
    // first, and b holds no more than a.
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
