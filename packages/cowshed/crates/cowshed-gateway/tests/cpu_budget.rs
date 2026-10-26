#![cfg(target_os = "macos")]
//! The host CPU budget over a real control socket: grants, fairness between checkouts, the
//! ledger, and tokens coming back when a holder is killed.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;

use cowshed_os::test_backstop::{
    AsyncChildBackstop as _, AsyncCommandBackstop as _, AsyncUnixStreamBackstop as _,
    ChildBackstop as _,
};
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
    GatewayControlClient, MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
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
/// `sun_path`'s 104 bytes), and the guard that removes its root on every exit path.
async fn gateway(tokens: usize) -> (Gateway, fixture_dir::FixtureDir, PathBuf) {
    let id = Uuid::new_v4().simple().to_string();
    let root = fixture_dir::secure_fixture_dir(&format!("cscpu-{}", &id[..8]));
    let cache = root.path().join("cache");
    std::fs::create_dir(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = root.path().join("gateway.sock");
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            // Beside the socket, as the CLI configures it; nothing serves it unless a test does.
            listener_handoff: Some(cowshed_gateway::handoff_path(&socket)),
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

fn gate_request(want: usize, checkout: &str, command: &str) -> Value {
    let repo = format!(
        "owner/{}",
        Path::new(checkout).file_name().unwrap().to_str().unwrap()
    );
    serde_json::json!({
        "op": "jobserver", "want": want, "checkout": checkout, "command": command,
        "class": "gate", "pool": true,
        "job": { "repoId": repo, "workspaceIncarnation": "0123456789abcdef0123456789abcdef",
            "jobId": 1 }
    })
}

/// A supervisor's ask for a one-token background job it can run on probation (05_gateway.md "A
/// one-token job starts on probation").
fn probation_request(want: usize, checkout: &str, command: &str, job: u64) -> Value {
    serde_json::json!({
        "op": "jobserver", "want": want, "checkout": checkout, "command": command,
        "class": "background", "pool": true, "probation": true,
        "job": { "repoId": "owner/drive", "workspaceIncarnation": "0123456789abcdef0123456789abcdef",
            "jobId": job }
    })
}

/// One held `cpu-tokens` connection in this process, answered `queued`.
struct Asker {
    lines: BufReader<tokio::net::unix::OwnedReadHalf>,
    _writer: tokio::net::unix::OwnedWriteHalf,
}

impl Asker {
    async fn jobserver(socket: &Path, want: usize, checkout: &str) -> Self {
        let stream = UnixStream::connect_bounded(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        let request = gate_request(want, checkout, "land gate");
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
        let stream = UnixStream::connect_bounded(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        let request = gate_request(1, checkout, "nx run-many -t gate");
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
        let granted = asker.answer().await;
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
        Self::send(pool, serde_json::json!({ "op": "cpu-pool", "want": want })).await
    }

    /// One held request line on `socket`, answered `queued`.
    async fn send(socket: &Path, request: Value) -> Self {
        let stream = UnixStream::connect_bounded(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("ask");
        let mut asker = Self {
            lines: BufReader::new(reader),
            _writer: writer,
        };
        assert_eq!(asker.answer().await["lease"], "queued");
        asker
    }

    /// One held request line on `socket`, and the first answer it hears.
    async fn open(socket: &Path, request: Value) -> (Self, Value) {
        let stream = UnixStream::connect_bounded(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("ask");
        let mut asker = Self {
            lines: BufReader::new(reader),
            _writer: writer,
        };
        let first = asker.answer().await;
        (asker, first)
    }

    async fn ask(socket: &Path, want: usize, checkout: &str) -> Self {
        let stream = UnixStream::connect_bounded(socket).await.expect("connect");
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

    /// Write one more line on the held connection.
    async fn write(&mut self, line: Value) {
        self._writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .expect("write");
    }

    /// Absence check: whether a grant arrives within `within`; load only makes `None` pass
    /// vacuously.
    async fn granted_within(&mut self, within: Duration) -> Option<u64> {
        let answer = tokio::time::timeout(within, self.answer()).await.ok()?;
        assert_eq!(answer["lease"], "granted", "{answer}");
        Some(answer["tokens"].as_u64().expect("a grant names its tokens"))
    }

    /// The tokens of the next grant, waiting for it.
    async fn granted(&mut self) -> u64 {
        let answer = self.answer().await;
        assert_eq!(answer["lease"], "granted", "{answer}");
        answer["tokens"].as_u64().expect("a grant names its tokens")
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

/// The ledger as `cowshed gateway status` reads it: the control client's `cpu-budget` request.
async fn ledger(socket: &Path) -> CpuBudgetStatus {
    GatewayControlClient::new(socket.to_path_buf())
        .expect("absolute socket")
        .cpu_budget()
        .await
        .expect("a ledger")
}

/// The deployed daemon does not set COWSHED_TIMING. Exercise the real wire in a subprocess
/// with that variable absent, so a test runner's own environment cannot enable the records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grant_handoff_and_selection_timings_need_no_environment_switch() {
    const CHILD: &str = "COWSHED_TIMING_PROOF_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = tokio::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "grant_handoff_and_selection_timings_need_no_environment_switch",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env_remove("COWSHED_TIMING")
            .output_bounded()
            .await
            .expect("run timing proof");
        let stderr = String::from_utf8(output.stderr).expect("UTF-8 diagnostics");
        assert!(output.status.success(), "{stderr}");
        for (record, duration) in [
            ("lease-grant scheduler=\"CPU budget\"", "elapsed_ns="),
            ("lease-grant scheduler=\"jobserver\"", "elapsed_ns="),
            ("lease-grant scheduler=\"task scheduler\"", "elapsed_ns="),
            ("cpu-tokens grant ", "waited_ns="),
            ("cpu-tokens queue-step cause=ask ", "elapsed_ns="),
            ("cpu-tokens queue-step cause=release ", "elapsed_ns="),
            ("cpu-tokens handoff ", "elapsed_ns="),
            ("task-server select ", "waited_ns="),
        ] {
            let line = stderr
                .lines()
                .find(|line| line.contains(record))
                .unwrap_or_else(|| panic!("missing {record}: {stderr}"));
            let value = line
                .split_whitespace()
                .find_map(|field| field.strip_prefix(duration))
                .unwrap_or_else(|| panic!("missing {duration}: {line}"));
            assert!(value.parse::<u128>().is_ok(), "{line}");
        }
        assert!(stderr.contains("task-server terminal "), "{stderr}");
        let other = stderr
            .lines()
            .find(|line| line.contains("gate_repo=Some(\"owner/other\")"))
            .unwrap_or_else(|| panic!("missing gate identity: {stderr}"));
        assert!(other.contains("head_repo=Some(\"owner/head\")"), "{other}");
        assert!(
            other.contains("gate_job=Some(") && other.contains("head_job=Some("),
            "{other}"
        );
        return;
    }
    assert!(std::env::var_os("COWSHED_TIMING").is_none());
    let (gateway, _root, socket) = gateway(2).await;
    // With no gate work the holder takes the whole host, so the waiter queues.
    let mut holder = Asker::ask(&socket, 2, "/w/holder").await;
    assert_eq!(holder.granted().await, 2);
    let mut waiter = Asker::ask(&socket, 1, "/w/waiter").await;
    let queued = ledger(&socket).await;
    assert_eq!(queued.held, 2);
    assert_eq!(
        queued
            .background
            .iter()
            .map(|row| row.waiters.len())
            .sum::<usize>(),
        1
    );
    drop(holder);
    assert_eq!(waiter.granted().await, 1);
    drop(waiter);
    let mut jobserver = Asker::send(
        &socket,
        serde_json::json!({
            "op": "jobserver", "want": 1, "checkout": "/w/job",
            "command": "jobserver timing proof"
        }),
    )
    .await;
    assert_eq!(jobserver.granted().await, 1);
    drop(jobserver);
    let mut unit = Asker::send(
        &socket,
        serde_json::json!({
            "op": "task-admit", "unit": "timed-unit", "owner": "/w/task",
            "command": "task timing proof", "want": 0
        }),
    )
    .await;
    assert_eq!(unit.answer().await["lease"], "granted");
    let terminal = one_shot(
        &socket,
        "{\"op\":\"task-terminal\",\"unit\":\"timed-unit\",\"outcome\":\"landed\"}\n",
    )
    .await;
    assert_eq!(terminal["ok"], true);
    drop(unit);
    let (head, _) = Asker::gate_job(&socket, "/w/head").await;
    let (other, _) = Asker::gate_job(&socket, "/w/other").await;
    assert_eq!(ledger(&socket).await.held, 2);
    drop((head, other));
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_killed_with_sigkill_returns_its_tokens_at_once() {
    let (gateway, _root, socket) = gateway(8).await;
    // With no gate work the holder takes the whole host, so a second background checkout queues.
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
        .write_all(request(8, "/w/killed").as_bytes())
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
    assert!(answers.1.contains("\"tokens\":8"), "{}", answers.1);

    let mut waiter = Asker::ask(&socket, 2, "/w/waiting").await;
    assert_eq!(
        waiter.granted_within(Duration::from_millis(300)).await,
        None
    );
    let held = ledger(&socket).await;
    assert_eq!((held.total, held.held), (8, 8));

    holder.kill().expect("SIGKILL the holder");
    holder.wait_bounded().expect("reap");
    assert_eq!(
        waiter.granted().await,
        2,
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkouts_share_the_host_and_a_lone_grant_fills_it() {
    let (gateway, _root, socket) = gateway(16).await;

    // Alone, with no gate work, one background grant fills the host, 16 of 16.
    let mut whole = Asker::ask(&socket, 64, "/w/a").await;
    assert_eq!(whole.granted().await, 16);
    let mut a_next = Asker::ask(&socket, 16, "/w/a").await;
    assert_eq!(
        a_next.granted_within(Duration::from_millis(300)).await,
        None
    );
    let mut b = Asker::ask(&socket, 16, "/w/b").await;
    assert_eq!(b.granted_within(Duration::from_millis(300)).await, None);

    // The runner returns 16: each waiting checkout takes its share of 8.
    drop(whole);
    assert_eq!(a_next.granted().await, 8);
    assert_eq!(b.granted().await, 8);
    let held = ledger(&socket).await;
    assert_eq!(held.held, 16);
    assert_eq!(
        held.checkouts
            .iter()
            .map(|checkout| (checkout.checkout.as_str(), checkout.held))
            .collect::<Vec<_>>(),
        [("/w/a", 8), ("/w/b", 8)]
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
}

/// A background ask of a checkout that already holds a token is passed by at most the background
/// limit of later background grants (05_gateway.md "Fair between checkouts within one priority").
/// Measured 2026-10-22 on gateway 89443: a scratch checkout held one token for an idle FIFO
/// reader, and its next ask waited 2,478 s while 51 later background asks of checkouts holding
/// nothing were granted. On a four-token host the limit is two: with no gate work, background
/// fills the host, two later checkouts are served from returned tokens, then it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkout_holding_a_token_is_served_after_at_most_the_background_limit_of_later_grants() {
    let (gateway, _root, socket) = gateway(4).await;
    let mut idle = Asker::ask(&socket, 1, "/w/scratch").await;
    assert_eq!(idle.granted().await, 1);
    let mut holders = std::collections::VecDeque::new();
    for held in 0..3 {
        let mut holder = Asker::ask(&socket, 1, &format!("/w/held{held}")).await;
        assert_eq!(holder.granted().await, 1);
        holders.push_back(holder);
    }
    // Each waiter reports its grant; exactly one waiter is granted each returned token.
    let (granted, mut grants) = tokio::sync::mpsc::unbounded_channel();
    let wait = |name: String, mut asker: Asker| {
        let granted = granted.clone();
        tokio::spawn(async move {
            assert_eq!(asker.granted().await, 1);
            let _ = granted.send((name, asker));
        })
    };
    let mut waiters = vec![wait(
        "scratch".to_owned(),
        Asker::ask(&socket, 1, "/w/scratch").await,
    )];
    let mut order = Vec::new();
    for fresh in 1..=3 {
        let checkout = format!("/w/fresh{fresh}");
        waiters.push(wait(
            format!("fresh{fresh}"),
            Asker::ask(&socket, 1, &checkout).await,
        ));
        drop(holders.pop_front());
        let (name, asker) = grants
            .recv()
            .await
            .expect("a waiter takes the returned token");
        order.push(name);
        holders.push_back(asker);
    }
    assert_eq!(order, ["fresh1", "fresh2", "scratch"]);
    for waiter in waiters {
        waiter.abort();
    }
    drop((idle, holders));
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jobserver_grant_names_a_fifo_and_close_returns_every_token() {
    let (gateway, _root, socket) = gateway(8).await;
    let stream = UnixStream::connect_bounded(&socket).await.expect("connect");
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
    while fifo.exists() || ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    gateway.drain().await.expect("drain");
}

/// The gate's own half is held by an older gate, so it waits for a returned token like anyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_takes_the_larger_part_of_returned_tokens_before_older_background_work() {
    let (gateway, _root, socket) = gateway(8).await;
    let mut holder = Asker::ask(&socket, 4, "/w/background").await;
    assert_eq!(holder.granted().await, 4);
    let mut first_gate = Asker::jobserver(&socket, 4, "/w/first-gate").await;
    assert_eq!(first_gate.granted().await, 4);
    let mut old_background = Asker::ask(&socket, 4, "/w/same").await;
    let mut gate = Asker::jobserver(&socket, 4, "/w/same").await;
    assert_eq!(gate.granted_within(Duration::from_millis(50)).await, None);
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
    assert_eq!(gate.granted().await, 3);
    assert_eq!(old_background.granted().await, 1);
    let status = ledger(&socket).await;
    assert_eq!((status.total, status.held), (8, 8));
    drop((first_gate, gate, old_background));
    gateway.drain().await.expect("drain");
}

/// The owner's ruling of 2026-10-22 over the socket (05_gateway.md "Room for a gate"): with no
/// gate work a background run takes the whole host, so a land's gate job that arrives then queues,
/// since no grant shrinks. When the run returns its tokens, the background checkout holding
/// nothing keeps its floor token and the gate job and its task take all the rest, with no kill and
/// no timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_arriving_after_background_filled_the_host_takes_the_returned_tokens_first() {
    let (gateway, _root, socket) = gateway(8).await;
    let mut smoke = Asker::ask(&socket, 8, "/w/smoke").await;
    assert_eq!(smoke.granted().await, 8);
    let mut other = Asker::ask(&socket, 8, "/w/other").await;
    let mut gate = Asker::send(&socket, gate_request(1, "/w/train", "nx run-many -t gate")).await;
    let status = ledger(&socket).await;
    assert_eq!((status.held, status.gate_queue.len()), (8, 1), "{status:?}");
    // The background run ends.
    drop(smoke);
    assert_eq!(other.granted().await, 1);
    let granted = gate.answer().await;
    assert_eq!(granted["tokens"], 1, "{granted}");
    let pool = PathBuf::from(
        granted["cpuPool"]
            .as_str()
            .expect("a gate job names its pool"),
    );
    let mut task = Asker::child(&pool, 8).await;
    assert_eq!(task.granted().await, 6);
    let status = ledger(&socket).await;
    assert_eq!((status.total, status.held), (8, 8), "{status:?}");
    assert_eq!(
        status
            .background
            .iter()
            .map(|stake| (stake.checkout.as_str(), stake.held))
            .collect::<Vec<_>>(),
        [("/w/other", 1)],
        "{status:?}"
    );
    drop((gate, task, other));
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_gate_holds_up_neither_background_nor_a_later_gate_while_tokens_are_free() {
    let (gateway, _root, socket) = gateway(8).await;
    let mut gate = Asker::jobserver(&socket, 4, "/w/gate").await;
    assert_eq!(gate.granted().await, 4);
    let mut background = Asker::ask(&socket, 1, "/w/background").await;
    assert_eq!(background.granted().await, 1);
    let mut next_gate = Asker::jobserver(&socket, 2, "/w/next-gate").await;
    assert_eq!(next_gate.granted().await, 2);
    assert_eq!(ledger(&socket).await.held, 7);
    drop((gate, background, next_gate));
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_jobs_pool_admits_each_child_while_older_background_work_stays_queued() {
    let (gateway, _root, socket) = gateway(4).await;
    // The land's gate job holds only its orchestrator's token.
    let (gate, pool) = Asker::gate_job(&socket, "/w/land").await;
    let mut a = Asker::ask(&socket, 2, "/w/a").await;
    // No gate ask waits: background takes the free tokens it wants, beside gate work too.
    assert_eq!(a.granted().await, 2);
    let mut b = Asker::ask(&socket, 1, "/w/b").await;
    assert_eq!(b.granted().await, 1);
    let mut queued = Asker::ask(&socket, 1, "/w/c").await;
    // A land task asks the gate job's pool after c queued. The pool lends nothing of its own: the
    // child is granted the next returned tokens, ahead of c, not a share of the gate's single token.
    let mut task = Asker::child(&pool, 2).await;
    assert_eq!(task.granted_within(Duration::from_millis(50)).await, None);
    drop(a);
    assert_eq!(task.granted().await, 2);
    assert_eq!(queued.granted_within(Duration::from_millis(50)).await, None);
    let status = ledger(&socket).await;
    let land = status
        .checkouts
        .iter()
        .find(|checkout| checkout.checkout == "/w/land")
        .expect("the land holds");
    assert_eq!((land.held, land.running), (3, 2), "{status:?}");
    // The task's tokens return and no gate ask waits: c takes one.
    drop(task);
    assert_eq!(queued.granted().await, 1);
    drop((gate, b, queued));
    gateway.drain().await.expect("drain");
}

/// 05_gateway.md "One gate per repository", over the socket: a second gate job of a repository
/// whose land gates is queued with the cause naming that land's gate job.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_gate_of_a_repository_hears_which_gate_holds_it() {
    let (gateway, _root, socket) = gateway(4).await;
    let (land, _pool) = Asker::gate_job(&socket, "/w/land").await;
    let (second, queued) = Asker::open(&socket, gate_request(1, "/x/land", "second gate")).await;
    assert_eq!(queued["lease"], "queued", "{queued}");
    assert_eq!(queued["cause"]["kind"], "repository-gate", "{queued}");
    assert_eq!(
        queued["cause"]["gate"]["repository"], "owner/land",
        "{queued}"
    );
    assert_eq!(queued["cause"]["gate"]["checkout"], "/w/land", "{queued}");
    assert_eq!(
        queued["cause"]["gate"]["command"], "nx run-many -t gate",
        "{queued}"
    );
    drop((second, land));
    gateway.drain().await.expect("drain");
}

/// 05_gateway.md "Deleting released images", over the socket: a released image nudged while a
/// land gates is freed as soon as the cleanup's pass reaches it, beside the gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_released_image_is_freed_beside_gate_work() {
    let (gateway, root, socket) = gateway(4).await;
    let store = root.path().join("store");
    let deleting = store.join("acme/widget/build/.deleting");
    std::fs::create_dir_all(&deleting).expect("deleting directory");
    gateway
        .clean_up_released_images(store.clone())
        .expect("the cleanup starts");
    let (gate, _pool) = Asker::gate_job(&socket, "/w/land").await;
    let entry = deleting.join(format!(
        "{}.dmg",
        cowshed_core::build_volume::BuildVolumeId::mint()
    ));
    std::fs::write(&entry, vec![9_u8; 64 * 1024]).expect("a released image");
    let answer = one_shot(&socket, "{\"op\":\"released\"}\n").await;
    assert_eq!(answer["ok"], true, "{answer}");
    // The gate still holds: the cleanup cuts beside it.
    while entry.exists() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(gate);
    gateway.drain().await.expect("drain");
}

/// 05_gateway.md "A queued CPU ask outlives an install". Measured 2026-10-18: an install
/// restarted the gateway while a land's gate ran, and the gate's next tasks failed with
/// `cpu-tokens parent pool refused: the gateway closed the lease connection`. A restart by an
/// install now tells every queued ask — a background `cpu-tokens` ask, a gate job's admission and
/// a gate pool's child alike — which install cut it off before the connection closes, so its
/// client can ask the gateway that install starts; a holder's grant ends with its connection, as
/// before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_install_restart_tells_every_queued_cpu_ask_which_install_cut_it_off() {
    let (gateway, _root, socket) = gateway(2).await;
    let (_gate, pool) = Asker::gate_job(&socket, "/w/land").await;
    let mut holder = Asker::ask(&socket, 1, "/w/a").await;
    assert_eq!(holder.granted().await, 1);
    // The host is full: each of these queues.
    let mut background = Asker::ask(&socket, 1, "/w/c").await;
    let mut task = Asker::child(&pool, 1).await;
    let mut other_gate = Asker::jobserver(&socket, 1, "/w/other").await;
    let install = cowshed_gateway_types::InstallRestart {
        pid: 4242,
        binary: PathBuf::from("/opt/cowshed/bin/cowshed"),
        serves_within_ms: 180_000,
    };
    let serving = cowshed_gateway::HandoffServer::start(
        &cowshed_gateway::handoff_path(&socket),
        install.clone(),
    )
    .expect("the install's handoff");

    gateway.drain().await.expect("drain");

    for (asker, which) in [
        (&mut background, "a background cpu-tokens ask"),
        (&mut task, "a gate pool's child"),
        (&mut other_gate, "a gate job's admission"),
    ] {
        let answer = asker.answer().await;
        assert_eq!(answer["ok"], false, "{which}: {answer}");
        assert_eq!(answer["code"], "install-restart", "{which}: {answer}");
        let named: cowshed_gateway_types::InstallRestart =
            serde_json::from_value(answer["install"].clone()).expect("the install, typed");
        assert_eq!(named, install, "{which} is told which install cut it off");
    }
    let mut rest = String::new();
    holder
        .lines
        .read_line(&mut rest)
        .await
        .expect("the holder's connection ends");
    assert_eq!(
        rest, "",
        "a holder is told nothing: its grant ends with its connection"
    );
    drop(serving);
}

/// With no install serving the handoff socket a restart is what it always was: a queued ask's
/// connection closes with no answer, so its client is never sent to ask again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_no_install_serves_cuts_a_queued_ask_off_unnamed() {
    let (gateway, _root, socket) = gateway(1).await;
    let mut holder = Asker::ask(&socket, 1, "/w/a").await;
    assert_eq!(holder.granted().await, 1);
    let mut queued = Asker::ask(&socket, 1, "/w/b").await;
    gateway.drain().await.expect("drain");
    let mut rest = String::new();
    queued
        .lines
        .read_line(&mut rest)
        .await
        .expect("the queued connection ends");
    assert_eq!(rest, "", "no install named, so no install-restart answer");
    drop(holder);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_older_land_task_is_served_before_a_younger_lands_and_a_feature_builds_that_asked_first()
{
    let (gateway, _root, socket) = gateway(4).await;
    // A feature shed's two runners start while no gate works; the lands then take the rest of the
    // host, and the feature's next runner waits first.
    let mut feature_one = Asker::ask(&socket, 1, "/w/feature").await;
    assert_eq!(feature_one.granted().await, 1);
    let mut feature_two = Asker::ask(&socket, 1, "/w/feature").await;
    assert_eq!(feature_two.granted().await, 1);
    let (older, older_pool) = Asker::gate_job(&socket, "/w/older").await;
    let (younger, younger_pool) = Asker::gate_job(&socket, "/w/younger").await;
    let mut feature_next = Asker::ask(&socket, 1, "/w/feature").await;
    // Then the younger land's task asks through its pool, and the older land's task asks last.
    let mut younger_task = Asker::child(&younger_pool, 1).await;
    let mut older_task = Asker::child(&older_pool, 1).await;
    let status = ledger(&socket).await;
    // The running gate jobs, the head — the older land — first, each on its orchestrator's token;
    // the feature shed is the background served behind them. Every lease names its client — this
    // process, the socket's peer — its command, its tokens of its want, and the age of its grant,
    // the oldest grant first.
    let me = Some(i32::try_from(std::process::id()).expect("a pid fits i32"));
    let leases = |holders: &[cowshed_gateway_types::CpuHolder]| {
        holders
            .iter()
            .map(|holder| {
                (
                    holder.pid,
                    holder.command.clone(),
                    holder.tokens,
                    holder.want,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        status
            .gate_jobs
            .iter()
            .map(|job| (job.checkout.as_str(), job.held, leases(&job.holders)))
            .collect::<Vec<_>>(),
        [
            (
                "/w/older",
                1,
                vec![(me, "nx run-many -t gate".to_owned(), 1, 1)]
            ),
            (
                "/w/younger",
                1,
                vec![(me, "nx run-many -t gate".to_owned(), 1, 1)]
            ),
        ],
        "{status:?}"
    );
    assert!(
        status.gate_jobs[0].job < status.gate_jobs[1].job,
        "{status:?}"
    );
    let [feature] = status.background.as_slice() else {
        panic!("one background stake: {status:?}");
    };
    assert_eq!(
        (
            feature.checkout.as_str(),
            feature.held,
            feature.waiters.len()
        ),
        ("/w/feature", 2, 1),
        "{status:?}"
    );
    assert_eq!(
        leases(&feature.holders),
        [
            (me, "cargo nextest run".to_owned(), 1, 1),
            (me, "cargo nextest run".to_owned(), 1, 1)
        ],
        "{status:?}"
    );
    assert!(
        feature.holders[0].held_ms >= feature.holders[1].held_ms,
        "the oldest grant first: {status:?}"
    );
    let queue: Vec<(String, u64)> = status
        .gate_queue
        .into_iter()
        .map(|waiter| (waiter.checkout, waiter.job))
        .collect();
    assert_eq!(
        queue.iter().map(|(_, job)| *job).collect::<Vec<_>>(),
        [status.gate_jobs[0].job, status.gate_jobs[1].job],
        "a waiting task names the gate job it serves: {queue:?}"
    );
    assert_eq!(
        queue
            .iter()
            .map(|(checkout, _)| checkout.as_str())
            .collect::<Vec<_>>(),
        ["/w/older", "/w/younger"],
        "{queue:?}"
    );
    assert!(queue[0].1 < queue[1].1, "{queue:?}");
    // Each returned token goes to the oldest land first, then the younger land, then the feature.
    drop(feature_one);
    assert_eq!(older_task.granted().await, 1);
    // A task its pool relays is named too: by its own pid, the pool socket's peer.
    let status = ledger(&socket).await;
    assert_eq!(
        leases(&status.gate_jobs[0].holders),
        [
            (me, "nx run-many -t gate".to_owned(), 1, 1),
            (me, "child CPU pool".to_owned(), 1, 1)
        ],
        "{status:?}"
    );
    assert_eq!(
        younger_task.granted_within(Duration::from_millis(50)).await,
        None
    );
    drop(older_task);
    assert_eq!(younger_task.granted().await, 1);
    // No gate ask waits now: the feature's next runner takes the returned token beside the lands.
    drop(feature_two);
    assert_eq!(feature_next.granted().await, 1);
    drop((older, younger, younger_task, feature_next));
    gateway.drain().await.expect("drain");
}

/// The measured failure, over the socket: an older land's gate job holds most of the host with
/// more of its tasks queued, background holds the rest (one token in the younger land's own
/// checkout), and a younger land's gate job asks for its orchestrator's token. It is served at the
/// first returned token, on its floor, ahead of the older land's queued task, and no admission
/// bound ever refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_younger_land_is_served_its_floor_while_an_older_land_holds_most_of_the_host() {
    let (gateway, _root, socket) = gateway(8).await;
    let (older, older_pool) = Asker::gate_job(&socket, "/w/older").await;
    let mut older_task = Asker::child(&older_pool, 5).await;
    assert_eq!(older_task.granted().await, 5);
    let mut own = Asker::ask(&socket, 1, "/w/younger").await;
    assert_eq!(own.granted().await, 1);
    let mut other = Asker::ask(&socket, 1, "/w/other").await;
    assert_eq!(other.granted().await, 1);
    assert_eq!(ledger(&socket).await.held, 8);
    let mut older_next = Asker::child(&older_pool, 5).await;
    let mut younger = Asker::jobserver(&socket, 1, "/w/younger").await;
    assert_eq!(
        younger.granted_within(Duration::from_millis(50)).await,
        None
    );
    drop(older_task);
    assert_eq!(younger.granted().await, 1);
    assert_eq!(older_next.granted().await, 4);
    drop((older, older_next, own, other, younger));
    gateway.drain().await.expect("drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jobserver_class_is_validated_and_a_wait_bound_is_an_unknown_field() {
    let (gateway, _root, socket) = gateway(4).await;
    for fields in [
        serde_json::json!({"class":"pretend-gate"}),
        serde_json::json!({"class":"gate","waitMs":120_000}),
        serde_json::json!({"class":"background","waitMs":100}),
        // A resident asks with its own operation, never as a token-holding jobserver class.
        serde_json::json!({"class":"resident"}),
        // Gate serialization needs an identity and cannot be bypassed through a parent pool.
        serde_json::json!({"class":"gate"}),
        serde_json::json!({"class":"gate","pool":true}),
        serde_json::json!({"class":"gate","pool":true,"parentPool":"/tmp/parent.sock",
            "job":{"repoId":"owner/repo","workspaceIncarnation":"0123456789abcdef0123456789abcdef","jobId":1}}),
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
}

/// A grant never grows (05_gateway.md "All at once"): `grow` is an unknown field on a root
/// `jobserver` request and on a child's `cpu-pool` ask alike, refused before anything is queued.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_jobserver_or_cpu_pool_request_naming_grow_is_an_invalid_request() {
    let (gateway, _root, socket) = gateway(4).await;
    let (gate, pool) = Asker::gate_job(&socket, "/w/land").await;
    for (target, request) in [
        (
            &socket,
            serde_json::json!({ "op": "jobserver", "want": 3, "checkout": "/w/build",
                "command": "cargo build", "grow": true }),
        ),
        (
            &pool,
            serde_json::json!({ "op": "cpu-pool", "want": 3, "grow": true }),
        ),
    ] {
        let refused = one_shot(target, &format!("{request}\n")).await;
        assert_eq!(refused["code"], "invalid-request", "{refused}");
    }
    assert_eq!(ledger(&socket).await.held, 1);
    drop(gate);
    gateway.drain().await.expect("drain");
}

/// One answer line off a lease connection, byte by byte, so nothing past it is read out of a
/// connection that is handed on afterwards.
async fn answer_line(stream: &mut UnixStream) -> Value {
    let mut line = Vec::new();
    let mut byte = [0_u8; 1];
    while line.last() != Some(&b'\n') {
        assert_eq!(
            stream.read(&mut byte).await.expect("read answer"),
            1,
            "the gateway closed before a whole answer line"
        );
        line.push(byte[0]);
    }
    serde_json::from_slice(&line).expect("an answer is JSON")
}

/// One resident: a real `sleep` that holds its `resident-pool` lease connection as its stdin, the
/// shape of the Nx daemon a shed's supervisor keeps, and the pool its grant names. The grant
/// names the pool alone: no tokens, no fifo. The request is the runtime's own I/O and the sleep is
/// spawned under the fork lock, as the gateway's own children are, so no blocking-pool thread or
/// unfenced spawn sits between the gateway's sockets and the child.
async fn resident(socket: &Path, checkout: &str, job_id: u64) -> (tokio::process::Child, PathBuf) {
    use cowshed_os::fork_lock::Spawn as _;
    let request = serde_json::json!({ "op": "resident-pool", "checkout": checkout,
        "command": "node nx/dist/src/daemon/server/start.js",
        "job": { "repoId": "owner/repo", "workspaceIncarnation": "0123456789abcdef0123456789abcdef",
            "jobId": job_id } });
    let mut stream = UnixStream::connect_bounded(socket)
        .await
        .expect("connect resident");
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .expect("ask resident pool");
    let queued = answer_line(&mut stream).await;
    let granted = answer_line(&mut stream).await;
    assert_eq!(queued["lease"], "queued", "{queued}");
    assert_eq!(granted["lease"], "granted", "{granted}");
    assert!(
        granted.get("tokens").is_none(),
        "a resident holds no token: {granted}"
    );
    assert!(
        granted.get("fifo").is_none(),
        "a resident has no fifo: {granted}"
    );
    let pool = PathBuf::from(
        granted["cpuPool"]
            .as_str()
            .expect("a resident names its pool"),
    );
    // The resident process itself holds the lease from here on, as the daemon's job does.
    let lease = std::os::fd::OwnedFd::from(stream.into_std().expect("the lease as a descriptor"));
    let child = tokio::process::Command::new("/bin/sleep")
        .arg("600")
        .stdin(Stdio::from(lease))
        .kill_on_drop(true)
        .spawn_locked()
        .expect("spawn the resident sleep");
    (child, pool)
}

/// Residents take no token and leave every shed's share of the host whole; each request one runs
/// asks the host through its pool and returns its tokens when it ends. Measured 2026-10-19: 25
/// sheds' Nx daemons, idle, held 15 of 18 tokens, one per shed for each daemon's lifetime (46
/// minutes in `batch-7`, 3 jobs waiting behind it), and a `bun install` waited ~15 minutes
/// behind them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resident_holds_no_token_and_each_request_it_runs_asks_the_host() {
    let (gateway, _root, socket) = gateway(4).await;
    // A resident in each of eight sheds, against a host of four tokens, two of them background's.
    let mut residents = Vec::new();
    for shed in 0..8_u64 {
        residents.push(resident(&socket, &format!("/w/shed-{shed}"), shed + 1).await);
    }
    let pool = residents[0].1.clone();

    let status = ledger(&socket).await;
    assert_eq!((status.total, status.held), (4, 0), "{status:?}");
    assert!(status.checkouts.is_empty(), "{status:?}");
    // A shed's usable concurrency is background's whole half, exactly as without any resident.
    let mut first = Asker::ask(&socket, 1, "/w/shed-0").await;
    let mut second = Asker::ask(&socket, 1, "/w/shed-0").await;
    assert_eq!(first.answer().await["tokens"], 1);
    assert_eq!(second.answer().await["tokens"], 1);
    drop((first, second));
    // The ledger sends no change notification: read it until the returns show. A return that
    // never comes is a hang, which the leg's no-progress backstop decides, never a count here.
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A request a resident runs asks its pool, which asks the host for that resident's shed.
    let mut request = Asker::child(&pool, 2).await;
    assert_eq!(request.answer().await["tokens"], 2);
    let status = ledger(&socket).await;
    assert_eq!(status.held, 2, "{status:?}");
    assert_eq!(
        status
            .background
            .iter()
            .map(|stake| (stake.checkout.as_str(), stake.held))
            .collect::<Vec<_>>(),
        [("/w/shed-0", 2)]
    );
    drop(request);
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A resident's death closes its pool, and the ledger is as it was without any resident.
    for (mut child, pool) in residents {
        child
            .kill_bounded()
            .await
            .expect("SIGKILL and reap the resident");
        while UnixStream::connect_bounded(&pool).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    assert_eq!(ledger(&socket).await.held, 0);
    gateway.drain().await.expect("drain");
}

/// The class an API caller names on its exec request reaches the real ledger through the
/// supervisor's own admission (`GatewayJobCpu`), the boundary where a requester's `ExecClass`
/// becomes the ledger's wire class (05_gateway.md "Bands"; 07_api.md "CLI and API exec parity"):
/// a gate is admitted as a gate job, ahead of background; interactive work is admitted as
/// background tokens for its checkout; and a resident is granted its pool and holds no token
/// while its grant lives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_exec_class_takes_its_own_ledger_place_through_the_supervisors_admission() {
    use cowshed_core::api::{ExecClass, JobId};
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _, JobCpuAsk};

    let (gateway, _root, socket) = gateway(4).await;
    let admission = GatewayJobCpu::new(socket.clone());
    let ask = |class: ExecClass, want: usize, checkout: &str, job: u64| JobCpuAsk {
        class,
        want: NonZeroUsize::new(want).unwrap(),
        checkout: PathBuf::from(checkout),
        command: format!("{class:?} job"),
        job: (
            cowshed_core::repository::RepoId::parse("owner/repo").unwrap(),
            cowshed_core::metadata::WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef")
                .unwrap(),
            JobId::new(job).unwrap(),
        ),
        parent: None,
    };
    let gate = admission
        .admit(ask(ExecClass::Gate, 2, "/w/land", 1))
        .await
        .expect("a gate is granted");
    let interactive = admission
        .admit(ask(ExecClass::Interactive, 1, "/w/feedback", 2))
        .await
        .expect("interactive work is granted");
    let resident = admission
        .admit(ask(ExecClass::Resident, 1, "/w/worker", 3))
        .await
        .expect("a resident is granted its pool");
    assert_eq!(
        (gate.tokens(), interactive.tokens(), resident.tokens()),
        (NonZeroUsize::new(2), NonZeroUsize::new(1), None),
        "the gate and interactive grants hold tokens; the resident's holds none"
    );

    let status = ledger(&socket).await;
    assert_eq!(status.held, 3, "{status:?}");
    assert_eq!(
        status
            .gate_jobs
            .iter()
            .map(|job| (job.checkout.as_str(), job.held))
            .collect::<Vec<_>>(),
        [("/w/land", 2)],
        "the gate is admitted as a gate job: {status:?}"
    );
    assert_eq!(
        status
            .background
            .iter()
            .map(|stake| (stake.checkout.as_str(), stake.held))
            .collect::<Vec<_>>(),
        [("/w/feedback", 1)],
        "interactive work holds background tokens, and the resident holds none: {status:?}"
    );
    assert!(
        status
            .checkouts
            .iter()
            .all(|checkout| checkout.checkout != "/w/worker"),
        "a live resident has no ledger place: {status:?}"
    );

    drop((gate, interactive, resident));
    // The ledger sends no change notification: read it until the returns show. A return that
    // never comes is a hang, which the leg's no-progress backstop decides, never a count here.
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gateway.drain().await.expect("drain");
}

/// A shell activation with no command yet to use a token -- a resident's start, a pool's spare --
/// holds one through the supervisor's admission: a background token on the real ledger for its
/// checkout, with no pool and no job, returned when the stake drops (05_gateway.md "A resident
/// holds no token"; 11_shell.md, spare).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_activation_stake_holds_one_background_token_until_it_drops() {
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _};

    let (gateway, _root, socket) = gateway(4).await;
    let admission = GatewayJobCpu::new(socket.clone());
    let stake = admission
        .activation(
            PathBuf::from("/w/keeper"),
            "shell spare activation in /w/keeper".to_owned(),
            None,
        )
        .await
        .expect("the stake is granted");
    let status = ledger(&socket).await;
    assert_eq!(status.held, 1, "{status:?}");
    assert_eq!(
        status
            .background
            .iter()
            .map(|stake| (stake.checkout.as_str(), stake.held))
            .collect::<Vec<_>>(),
        [("/w/keeper", 1)],
        "the activation holds one background token of its checkout: {status:?}"
    );
    drop(stake);
    // The ledger sends no change notification: read it until the return shows. A return that
    // never comes is a hang, which the leg's no-progress backstop decides, never a count here.
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gateway.drain().await.expect("drain");
}

/// A kept Nx daemon's start that a land waits on is one of the land's gate tasks (05_gateway.md
/// "A resident's start holds one token"). Measured 2026-10-21: the keeper asked
/// its start's stake of the host as background while another repository's gate and background
/// tests filled background's room. The stake waited 324 s, and the land, waiting on the daemon,
/// gave up. Here background fills the host beside the land: the stake asked of the host alone
/// queues behind it, and the same stake drawn on the land's gate pool, one of the gate's tasks, is
/// granted the next returned token ahead of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_kept_daemons_start_drawn_on_a_lands_gate_pool_is_granted_while_background_is_full() {
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _};

    let (gateway, _root, socket) = gateway(4).await;
    let (gate, pool) = Asker::gate_job(&socket, "/w/land").await;
    let mut background = Asker::ask(&socket, 2, "/w/tests").await;
    // No gate ask waits: background takes the free tokens it wants, and the host fills.
    assert_eq!(background.granted().await, 2);
    let mut filler = Asker::ask(&socket, 1, "/w/filler").await;
    assert_eq!(filler.granted().await, 1);
    let admission = GatewayJobCpu::new(socket.clone());
    let start = "node /w/land/node_modules/nx/dist/src/daemon/server/start.js --kept";
    // Asked of the host alone, as the keeper asked it before: it queues at the full host.
    let mut alone =
        Box::pin(admission.activation(PathBuf::from("/w/land"), start.to_owned(), None));
    loop {
        tokio::select! {
            granted = &mut alone => panic!("background had no room left: {granted:?}"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        if ledger(&socket)
            .await
            .background
            .iter()
            .any(|stake| stake.checkout == "/w/land" && !stake.waiters.is_empty())
        {
            break;
        }
    }
    // Drawn on the land's gate pool, it is one of the gate's tasks: granted the next returned token,
    // ahead of the host-alone stake that asked first.
    let pooled = tokio::spawn({
        let admission = GatewayJobCpu::new(socket.clone());
        let pool = pool.to_string_lossy().into_owned();
        async move {
            admission
                .activation(PathBuf::from("/w/land"), start.to_owned(), Some(pool))
                .await
        }
    });
    waiters_reach(&socket, "/w/land", 2).await;
    drop(filler);
    let stake = pooled
        .await
        .expect("the pooled start's task")
        .expect("the land's gate pool grants the start");
    assert_eq!(
        ledger(&socket).await.held,
        4,
        "the gate's, background's two and the start's"
    );
    // Its host ask was withdrawn when the pool won: only the host-alone stake still waits.
    waiters_reach(&socket, "/w/land", 1).await;
    drop(stake);
    drop(background);
    let alone = alone
        .await
        .expect("the host-alone stake is granted once a token returns");
    drop((alone, gate));
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gateway.drain().await.expect("drain");
}

/// Two checkouts of one repository cannot run gates together, even when the queued gate
/// inherits the running gate's pool. A different repository still starts on the same host.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gates_serialize_per_repository_through_supervisor_admission_not_parent_pools() {
    use cowshed_core::api::ExecClass;
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _};

    let (gateway, _root, socket) = gateway(8).await;
    let admission = GatewayJobCpu::new(socket.clone());
    let mut first_ask = exec_ask(1, "/w/first", 1, None);
    first_ask.class = ExecClass::Gate;
    let first = admission.admit(first_ask).await.expect("first gate");
    let mut second_ask = exec_ask(1, "/w/second", 2, Some(first.pool()));
    second_ask.class = ExecClass::Gate;
    let second_admission = GatewayJobCpu::new(socket.clone());
    let mut second = tokio::spawn(async move { second_admission.admit(second_ask).await });
    loop {
        tokio::select! {
            result = &mut second => panic!("a second gate bypassed the repository queue: {result:?}"),
            status = ledger(&socket) => {
                assert_eq!(status.gate_jobs.len(), 1, "{status:?}");
                if status.gate_queue.iter().any(|waiter| waiter.checkout == "/w/second") {
                    break;
                }
            }
        }
    }
    let mut other_ask = exec_ask(1, "/w/other", 3, None);
    other_ask.class = ExecClass::Gate;
    other_ask.job.0 = cowshed_core::repository::RepoId::parse("owner/other").unwrap();
    let other = admission
        .admit(other_ask)
        .await
        .expect("other repository runs");
    let status = ledger(&socket).await;
    assert_eq!(status.held, 2, "{status:?}");
    assert_eq!(
        status
            .gate_jobs
            .iter()
            .map(|job| job.checkout.as_str())
            .collect::<Vec<_>>(),
        ["/w/first", "/w/other"],
        "{status:?}"
    );
    let mut child = Asker::child(first.pool(), 6).await;
    assert_eq!(child.granted().await, 6);
    let status = ledger(&socket).await;
    assert_eq!(status.gate_jobs.len(), 2, "{status:?}");
    assert_eq!(status.gate_jobs[0].held, 7, "{status:?}");
    assert_eq!(status.gate_queue[0].checkout, "/w/second", "{status:?}");
    drop(child);
    drop(first);
    let second = second
        .await
        .expect("admission task")
        .expect("next repository gate");
    assert_eq!(second.tokens(), NonZeroUsize::new(1));
    let status = ledger(&socket).await;
    assert_eq!(status.gate_jobs.len(), 2, "{status:?}");
    assert!(
        status
            .gate_jobs
            .iter()
            .all(|job| job.checkout != "/w/first"),
        "{status:?}"
    );
    assert!(status.gate_queue.is_empty(), "{status:?}");
    drop((second, other));
    while ledger(&socket).await.held != 0 {
        tokio::task::yield_now().await;
    }
    gateway.drain().await.expect("drain");
}

/// A background exec of `checkout`'s job `job`, as a supervisor asks it, from a requester that
/// holds the live pool `parent` or none.
fn exec_ask(
    want: usize,
    checkout: &str,
    job: u64,
    parent: Option<&Path>,
) -> cowshed_core::runtime::job_cpu::JobCpuAsk {
    cowshed_core::runtime::job_cpu::JobCpuAsk {
        class: cowshed_core::api::ExecClass::Background,
        want: NonZeroUsize::new(want).unwrap(),
        checkout: PathBuf::from(checkout),
        command: format!("{checkout} job {job}"),
        job: (
            cowshed_core::repository::RepoId::parse("owner/repo").unwrap(),
            cowshed_core::metadata::WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef")
                .unwrap(),
            cowshed_core::api::JobId::new(job).unwrap(),
        ),
        parent: parent.map(|pool| pool.to_string_lossy().into_owned()),
    }
}

/// Work a holder starts draws on its grant (05_gateway.md "Work a holder starts draws on its
/// grant"). A holder at background's whole room waits on a child job it asked for; asked of the
/// host alone, that child could never be granted, since background holds all of its room and the
/// floor grants only below it. Asked with the holder's live pool, the child is granted at once
/// from the holder's tokens while the host stays saturated, and its host ask is withdrawn, so the
/// host ledger takes no place for it. A pool that answers and refuses before the host grants
/// refuses the child; only a pool that cannot be reached leaves the child to the host alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_at_backgrounds_room_has_its_childs_exec_granted_from_its_own_pool() {
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _};

    let (gateway, _root, socket) = gateway(4).await;
    let admission = GatewayJobCpu::new(socket.clone());
    // With no gate work the holder takes background's whole room, the host: 4 of 4 tokens.
    let holder = admission
        .admit(exec_ask(4, "/w/holder", 1, None))
        .await
        .expect("the holder is granted");
    assert_eq!(holder.tokens(), NonZeroUsize::new(4));
    // Asked of the host, the holder's child's ask waits behind it: the host is saturated. Its job
    // runs meanwhile on the probation token ("A one-token job starts on probation"), and its ask
    // keeps its place in the ledger.
    let mut host_child = admission
        .admit(exec_ask(1, "/w/scratch", 2, None))
        .await
        .expect("the host child is lent the probation token");
    assert!(host_child.on_probation(), "{host_child:?}");
    let host_granted = host_child
        .probation_outcome()
        .expect("a probation's wait for its grant");
    waiters_reach(&socket, "/w/scratch", 1).await;

    // Asked with the holder's pool, it is granted from the holder's tokens while the host stays
    // saturated, and its own pool is served in its scope like any job's.
    let child = admission
        .admit(exec_ask(1, "/w/scratch", 3, Some(holder.pool())))
        .await
        .expect("the holder's pool grants its child");
    assert_eq!(child.tokens(), NonZeroUsize::new(1));
    assert_eq!(child.pool().parent(), holder.pool().parent());
    let status = ledger(&socket).await;
    assert_eq!(
        status.held, 4,
        "the borrowed token is the holder's, not a host one: {status:?}"
    );

    // The pool lends what it holds: a second child takes the holder's other token.
    let second = admission
        .admit(exec_ask(1, "/w/scratch", 4, Some(holder.pool())))
        .await
        .expect("the holder's second token");
    assert_eq!(second.tokens(), NonZeroUsize::new(1));
    // Both children's host asks were withdrawn when the pool won: only the host child waits.
    waiters_reach(&socket, "/w/scratch", 1).await;

    // A pool that answers and refuses before the host grants refuses the child: the gateway's own
    // control socket answers a pool request as the unknown operation it is, and the host is full.
    let refused = admission
        .admit(exec_ask(1, "/w/scratch", 5, Some(&socket)))
        .await
        .expect_err("a pool that answers and refuses refuses the child");
    assert!(
        refused.to_string().contains("requester's CPU pool"),
        "{refused}"
    );
    assert_eq!(ledger(&socket).await.held, 4);

    // A pool that cannot be reached is absent: the holder gone, its child asks the host and is
    // granted there once the host has room.
    let holder_pool = holder.pool().to_path_buf();
    drop(holder);
    drop((child, second));
    // The holder's pool closes before its tokens return, so once its host child is granted the
    // pool is gone.
    host_granted
        .await
        .expect("the probation's wait reports")
        .expect("the host child is granted once room returns");
    let host = host_child;
    let absent = admission
        .admit(exec_ask(1, "/w/scratch", 6, Some(&holder_pool)))
        .await
        .expect("an unreachable pool leaves the child to the host");
    assert_eq!(absent.tokens(), NonZeroUsize::new(1));
    assert_eq!(ledger(&socket).await.held, 2);

    drop((host, absent));
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gateway.drain().await.expect("drain");
}

/// Wait until `checkout`'s background stake has exactly `count` waiters in the ledger. A count
/// that never settles is a hang the leg's no-progress bound names, never a deadline of this test's.
async fn waiters_reach(socket: &Path, checkout: &str, count: usize) {
    let waiters = |status: &CpuBudgetStatus| {
        status
            .background
            .iter()
            .filter(|stake| stake.checkout == checkout)
            .map(|stake| stake.waiters.len())
            .sum::<usize>()
    };
    while waiters(&ledger(socket).await) != count {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A child of the pool at `pool` asking for `want`, held once granted: what a sibling of an exec
/// holds of its holder's pool. Answers the connection and its granted count.
async fn pool_child(pool: &Path, want: usize) -> (UnixStream, u64) {
    let mut stream = UnixStream::connect_bounded(pool)
        .await
        .expect("connect the pool");
    stream
        .write_all(format!("{{\"op\":\"cpu-pool\",\"want\":{want}}}\n").as_bytes())
        .await
        .expect("ask the pool");
    loop {
        let answer = answer_line(&mut stream).await;
        assert_eq!(answer["ok"], true, "{answer}");
        if answer["lease"] == "granted" {
            let tokens = answer["tokens"].as_u64().expect("a grant names its count");
            return (stream, tokens);
        }
    }
}

/// An exec from a holder whose pool is busy waits on the host too, and the host grants it when
/// it has room (05_gateway.md "Work a holder starts draws on its grant"). The holder's one token
/// is lent to a sibling; the exec is granted by the host, which accounts for it, and its ask of
/// the pool is withdrawn: once the sibling closes, the next child of the pool is lent that token
/// at once, never held up behind a waiter that left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holders_exec_whose_pool_is_busy_is_granted_by_the_host_when_it_has_room() {
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _};

    let (gateway, _root, socket) = gateway(4).await;
    let admission = GatewayJobCpu::new(socket.clone());
    let holder = admission
        .admit(exec_ask(1, "/w/holder", 1, None))
        .await
        .expect("the holder is granted");
    assert_eq!(holder.tokens(), NonZeroUsize::new(1));
    let (sibling, lent) = pool_child(holder.pool(), 1).await;
    assert_eq!(lent, 1, "the sibling is lent the holder's one token");

    let exec = admission
        .admit(exec_ask(1, "/w/scratch", 2, Some(holder.pool())))
        .await
        .expect("the host grants the exec while the pool is busy");
    assert_eq!(exec.tokens(), NonZeroUsize::new(1));
    let status = ledger(&socket).await;
    assert_eq!(
        status.held, 2,
        "the host accounts for the exec beside the holder: {status:?}"
    );

    drop(sibling);
    // The withdrawn ask holds up no child of the pool: the next one is lent the token.
    let (next, lent) = pool_child(holder.pool(), 1).await;
    assert_eq!(lent, 1);

    drop((next, exec, holder));
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gateway.drain().await.expect("drain");
}

/// With the host full and its holder's pool busy, an exec from the holder waits at the host,
/// where the ledger shows it, and at the pool at once; whichever frees a token first grants it
/// (05_gateway.md "Work a holder starts draws on its grant"). Freed at the host, the host grants
/// it and accounts for it; freed at the pool, the pool grants it and its host ask leaves the
/// ledger's queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holders_exec_on_a_full_host_with_a_busy_pool_is_granted_by_whichever_frees_first() {
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _};

    let (gateway, _root, socket) = gateway(4).await;
    let admission = GatewayJobCpu::new(socket.clone());
    let holder = admission
        .admit(exec_ask(1, "/w/holder", 1, None))
        .await
        .expect("the holder is granted");
    // With no gate work background's room is the host: the holder and another checkout's job
    // fill it.
    let filler = admission
        .admit(exec_ask(3, "/w/filler", 2, None))
        .await
        .expect("the filler is granted");

    // The host frees first.
    let (sibling, _) = pool_child(holder.pool(), 1).await;
    let mut exec = Box::pin(admission.admit(exec_ask(1, "/w/scratch", 3, Some(holder.pool()))));
    tokio::select! {
        granted = &mut exec => panic!("neither the host nor the pool had a token: {granted:?}"),
        () = waiters_reach(&socket, "/w/scratch", 1) => {}
    }
    drop(filler);
    let by_host = exec.await.expect("the host's freed token grants the exec");
    assert_eq!(by_host.tokens(), NonZeroUsize::new(1));
    let status = ledger(&socket).await;
    assert_eq!(
        status.held, 2,
        "the host accounts for its grant: {status:?}"
    );
    drop(sibling);

    // The pool frees first: its sibling holds the pool's token, and another filler makes the
    // host full again.
    let refill = admission
        .admit(exec_ask(2, "/w/filler", 5, None))
        .await
        .expect("the refill is granted");
    let (sibling, _) = pool_child(holder.pool(), 1).await;
    let mut exec = Box::pin(admission.admit(exec_ask(1, "/w/scratch", 4, Some(holder.pool()))));
    tokio::select! {
        granted = &mut exec => panic!("neither the host nor the pool had a token: {granted:?}"),
        () = waiters_reach(&socket, "/w/scratch", 1) => {}
    }
    drop(sibling);
    let by_pool = exec.await.expect("the pool's freed token grants the exec");
    assert_eq!(by_pool.tokens(), NonZeroUsize::new(1));
    waiters_reach(&socket, "/w/scratch", 0).await;
    let status = ledger(&socket).await;
    assert_eq!(
        status.held, 4,
        "the pool's grant takes no host place: {status:?}"
    );

    drop((by_pool, by_host, holder, refill));
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gateway.drain().await.expect("drain");
}

/// The reader must see the waiting client's stderr while the first connection still owns its
/// grant. No sleep or elapsed-time assertion can establish that causal boundary. The waiter asks
/// two tokens, so it waits rather than run on the probation token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_jobserver_client_names_holders_before_release_and_status_lists_both() {
    use cowshed_core::api::ExecClass;
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _};
    const SOCKET: &str = "COWSHED_QUEUE_PROOF_SOCKET";
    const CLASS: &str = "COWSHED_QUEUE_PROOF_CLASS";
    if let Some(socket) = std::env::var_os(SOCKET) {
        let class = match std::env::var(CLASS).unwrap().as_str() {
            "gate" => ExecClass::Gate,
            "background" => ExecClass::Background,
            other => panic!("unexpected proof class {other}"),
        };
        let mut ask = exec_ask(2, "/w/waiter", 2, None);
        ask.class = class;
        let grant = GatewayJobCpu::new(socket.into())
            .admit(ask)
            .await
            .expect("waiter granted");
        assert_eq!(grant.tokens(), NonZeroUsize::new(2));
        drop(grant);
        return;
    }
    for class in [ExecClass::Gate, ExecClass::Background] {
        let (gateway, _root, socket) = gateway(4).await;
        let admission = GatewayJobCpu::new(socket.clone());
        let mut ask = exec_ask(
            if class == ExecClass::Gate { 1 } else { 4 },
            "/w/holder",
            1,
            None,
        );
        ask.class = class;
        let holder = admission.admit(ask).await.expect("held grant");
        let wire_class = if class == ExecClass::Gate {
            "gate"
        } else {
            "background"
        };
        let mut child = tokio::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "queued_jobserver_client_names_holders_before_release_and_status_lists_both",
                "--nocapture",
            ])
            .env(SOCKET, &socket)
            .env(CLASS, wire_class)
            .env_remove("COWSHED_TIMING")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("waiting client");
        let mut stderr = BufReader::new(child.stderr.take().expect("client stderr pipe"));
        let mut line = String::new();
        loop {
            line.clear();
            assert_ne!(
                stderr.read_line(&mut line).await.expect("client progress"),
                0,
                "client closed before reporting its wait"
            );
            if line.starts_with("cowshed: jobserver wait start ") {
                break;
            }
        }
        assert!(line.contains(r#"repository="owner/repo""#), "{line}");
        assert!(line.contains(&format!("class={wire_class}")), "{line}");
        assert!(line.contains(r#"workspace="/w/waiter""#), "{line}");
        assert!(line.contains("position=1 holders=["), "{line}");
        assert!(
            line.contains(r#"workspace="/w/holder" job=0 command="/w/holder job 1""#),
            "{line}"
        );
        assert!(line.contains("ahead=[]"), "{line}");
        // Status is queried only after the stderr pipe proves the second request was queued,
        // and before this test drops the first grant. Reading it changes no admission.
        let status = ledger(&socket).await;
        assert_eq!(status.held, if class == ExecClass::Gate { 1 } else { 4 });
        assert_eq!(status.checkouts.len(), 2, "{status:?}");
        let json = one_shot(&socket, "{\"op\":\"cpu-budget\"}\n").await;
        if class == ExecClass::Gate {
            assert_eq!(status.gate_jobs[0].repository, "owner/repo");
            assert_eq!(status.gate_jobs[0].checkout, "/w/holder");
            assert_eq!(status.gate_queue[0].repository, "owner/repo");
            assert_eq!(status.gate_queue[0].checkout, "/w/waiter");
            assert_eq!(json["cpuBudget"]["gateJobs"][0]["repository"], "owner/repo");
            assert_eq!(json["cpuBudget"]["gateQueue"][0]["checkout"], "/w/waiter");
        } else {
            assert_eq!(status.background[0].checkout, "/w/holder");
            assert_eq!(status.background[0].holders[0].command, "/w/holder job 1");
            assert_eq!(status.background[1].checkout, "/w/waiter");
            assert_eq!(status.background[1].waiters[0].command, "/w/waiter job 2");
            assert_eq!(
                json["cpuBudget"]["background"][1]["waiters"][0]["command"],
                "/w/waiter job 2"
            );
        }
        // A third request observes the second ahead of it in the same queue. Its queued
        // acknowledgement is the broker's own atomic admission snapshot, not a status guess.
        let stream = UnixStream::connect_bounded(&socket)
            .await
            .expect("third request");
        let (reader, writer) = stream.into_split();
        let mut third = Asker {
            lines: BufReader::new(reader),
            _writer: writer,
        };
        let request = serde_json::json!({"op":"jobserver", "want":1, "checkout":"/w/waiter",
            "command":"third request", "class":wire_class, "pool":true,
            "job":{"repoId":"owner/repo", "workspaceIncarnation":"0123456789abcdef0123456789abcdef", "jobId":3}});
        third
            ._writer
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let queued = third.answer().await;
        assert_eq!(queued["lease"], "queued");
        assert_eq!(queued["queue"]["position"], 2);
        assert_eq!(queued["queue"]["ahead"][0]["checkout"], "/w/waiter");
        assert_eq!(queued["queue"]["ahead"][0]["command"], "/w/waiter job 2");
        drop(third); // EOF cancels only this queued request, not the held grant.
        drop(holder); // Only now may the waiting client's grant and done line arrive.
        let mut remaining = String::new();
        stderr
            .read_to_string(&mut remaining)
            .await
            .expect("remaining stderr");
        let output = child
            .wait_with_output_bounded()
            .await
            .expect("client exits");
        assert!(
            output.status.success(),
            "{}\n{remaining}",
            String::from_utf8_lossy(&output.stdout)
        );
        let done = remaining
            .lines()
            .find(|line| line.starts_with("cowshed: jobserver wait done elapsed="))
            .expect("done line after grant");
        assert!(done.contains("status=ok"), "{done}");
        while ledger(&socket).await.held != 0 {
            tokio::task::yield_now().await;
        }
        gateway.drain().await.expect("drain");
    }
}

/// The probation token over the real socket (05_gateway.md "A one-token job starts on
/// probation"). A long holder holds every token: a one-token job's ask is answered `probation`
/// at once, with a pool that lends its children nothing, and its close lends the token on; a
/// two-token ask is never lent it. A job whose probation ends without a grant passes the token on
/// and keeps its place: the returned tokens grant it one token on the same pool and fifo, its
/// pool lends that token from then on, and the ledger holds no more than its total.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_one_token_job_runs_on_the_probation_token_while_a_long_holder_holds_every_token() {
    let (gateway, _root, socket) = gateway(2).await;
    let mut long = Asker::ask(&socket, 2, "/w/gate").await;
    assert_eq!(long.granted().await, 2);

    let mut read = Asker::send(&socket, probation_request(1, "/w/drive", "git ls-files", 1)).await;
    let lent = read.answer().await;
    assert_eq!(
        (&lent["lease"], &lent["tokens"]),
        (&"probation".into(), &1.into()),
        "{lent}"
    );
    let status = ledger(&socket).await;
    assert_eq!(status.held, 2);
    assert_eq!(
        status.probation.map(|lent| lent.command),
        Some("git ls-files".to_owned())
    );
    let pool = PathBuf::from(
        lent["cpuPool"]
            .as_str()
            .expect("a probation names its pool"),
    );
    let mut child = Asker::child(&pool, 1).await;
    assert_eq!(
        child.granted_within(Duration::from_millis(300)).await,
        None,
        "a probation's pool lends its children nothing"
    );
    drop(child);
    // The read finished.
    drop(read);

    // Only a one-token ask is lent it.
    let mut wide = Asker::send(&socket, probation_request(2, "/w/wide", "cargo build", 2)).await;
    assert_eq!(wide.granted_within(Duration::from_millis(300)).await, None);
    drop(wide);

    let mut heavy = Asker::send(
        &socket,
        probation_request(1, "/w/drive", "codegraph init", 3),
    )
    .await;
    let lent = heavy.answer().await;
    assert_eq!(lent["lease"], "probation", "{lent}");
    let (pool, fifo) = (lent["cpuPool"].clone(), lent["fifo"].clone());
    let mut next = Asker::send(&socket, probation_request(1, "/w/other", "git show", 4)).await;
    // The heavy job's probation ends: the next is lent the token, and the heavy one still waits.
    heavy
        .write(serde_json::json!({ "op": "probation-over" }))
        .await;
    assert_eq!(next.answer().await["lease"], "probation");
    let status = ledger(&socket).await;
    assert_eq!(status.held, 2);
    assert_eq!(
        status.probation.map(|lent| lent.command),
        Some("git show".to_owned())
    );
    let waiting: Vec<String> = status
        .background
        .iter()
        .flat_map(|stake| &stake.waiters)
        .map(|waiter| waiter.command.clone())
        .collect();
    assert!(
        waiting.contains(&"codegraph init".to_owned()),
        "{waiting:?}"
    );

    // The long holder returns its tokens: each waiter is granted its one token in its place.
    drop(long);
    let granted = heavy.answer().await;
    assert_eq!(
        (
            &granted["lease"],
            &granted["tokens"],
            &granted["cpuPool"],
            &granted["fifo"]
        ),
        (&"granted".into(), &1.into(), &pool, &fifo),
        "{granted}"
    );
    assert_eq!(next.granted().await, 1);
    let mut child = Asker::child(Path::new(pool.as_str().expect("a pool path")), 1).await;
    assert_eq!(
        child.granted().await,
        1,
        "a granted probation's pool lends its token"
    );
    let status = ledger(&socket).await;
    assert_eq!((status.held, status.probation), (2, None));
    drop((child, heavy, next));
    gateway.drain().await.expect("drain");
}
