#![cfg(target_os = "macos")]
//! The host CPU budget over a real control socket: grants, fairness between checkouts, the
//! ledger, and tokens coming back when a holder is killed.

use std::io::{BufRead as _, BufReader as StdBufReader, Write as _};
use std::num::NonZeroUsize;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
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
    async fn jobserver(socket: &Path, want: usize, checkout: &str) -> Self {
        let stream = UnixStream::connect(socket).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        let request = serde_json::json!({ "op": "jobserver", "want": want, "checkout": checkout,
            "command": "land gate", "class": "gate" });
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
            "command": "nx run-many -t gate", "class": "gate", "pool": true,
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
        let stream = UnixStream::connect(socket).await.expect("connect");
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

    /// Absence check: whether a growth arrives within `within`; load only makes `None` pass
    /// vacuously.
    async fn grown_within(&mut self, within: Duration) -> Option<u64> {
        let answer = tokio::time::timeout(within, self.answer()).await.ok()?;
        assert_eq!(answer["lease"], "grown", "{answer}");
        Some(answer["tokens"].as_u64().expect("a growth names its total"))
    }

    /// The new total of the next growth, waiting for it.
    async fn grown(&mut self) -> u64 {
        let answer = self.answer().await;
        assert_eq!(answer["lease"], "grown", "{answer}");
        answer["tokens"].as_u64().expect("a growth names its total")
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

/// The ledger as `cowshed gateway status` reads it: the control client's `cpu-budget` request.
async fn ledger(socket: &Path) -> CpuBudgetStatus {
    GatewayControlClient::new(socket.to_path_buf())
        .expect("absolute socket")
        .cpu_budget()
        .await
        .expect("a ledger")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_killed_with_sigkill_returns_its_tokens_at_once() {
    let (gateway, root, socket) = gateway(8).await;
    // The holder takes background's whole half, so a second background checkout queues whole.
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
    assert_eq!(
        waiter.granted_within(Duration::from_millis(300)).await,
        None
    );
    let held = ledger(&socket).await;
    assert_eq!((held.total, held.held), (8, 4));

    holder.kill().expect("SIGKILL the holder");
    holder.wait().expect("reap");
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
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkouts_share_the_background_half_and_a_lone_grant_fills_it() {
    let (gateway, root, socket) = gateway(16).await;

    // Alone, one background grant fills background's half of the host, 8 of 16.
    let mut whole = Asker::ask(&socket, 64, "/w/a").await;
    assert_eq!(whole.granted().await, 8);
    let mut a_next = Asker::ask(&socket, 8, "/w/a").await;
    assert_eq!(
        a_next.granted_within(Duration::from_millis(300)).await,
        None
    );
    let mut b = Asker::ask(&socket, 8, "/w/b").await;
    assert_eq!(b.granted_within(Duration::from_millis(300)).await, None);

    // The runner returns 8: each waiting checkout takes its share of 4.
    drop(whole);
    assert_eq!(a_next.granted().await, 4);
    assert_eq!(b.granted().await, 4);
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
    let (gateway, root, socket) = gateway(8).await;
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
    while fifo.exists() || ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

/// The gate's own half is held by an older gate, so it waits for a returned token like anyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_takes_the_larger_part_of_returned_tokens_before_older_background_work() {
    let (gateway, root, socket) = gateway(8).await;
    let mut holder = Asker::ask(&socket, 8, "/w/background").await;
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
    let _ = std::fs::remove_dir_all(root);
}

/// The measured hole, over the socket (05_gateway.md "Room for a gate"): a background run that
/// asks for the whole host while nothing else asks is granted background's half; a second
/// background request finds no room; and a land's gate job and then its task are granted the
/// other half while that background grant is still held. Every grant here arrives without a token
/// being returned, so the gate's wait is the ledger's own transition, not any run's length.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_is_granted_the_free_half_while_a_lone_background_grant_still_holds_its_own() {
    let (gateway, root, socket) = gateway(8).await;
    let mut smoke = Asker::ask(&socket, 8, "/w/smoke").await;
    assert_eq!(smoke.granted().await, 4);
    let mut other = Asker::ask(&socket, 8, "/w/other").await;
    assert_eq!(other.granted_within(Duration::from_millis(50)).await, None);
    let (gate, pool) = Asker::gate_job(&socket, "/w/train").await;
    let mut task = Asker::child(&pool, 8).await;
    assert_eq!(task.granted().await, 3);
    let status = ledger(&socket).await;
    assert_eq!((status.total, status.held), (8, 8), "{status:?}");
    assert_eq!(
        status
            .background
            .iter()
            .map(|stake| (stake.checkout.as_str(), stake.held))
            .collect::<Vec<_>>(),
        [("/w/other", 0), ("/w/smoke", 4)],
        "{status:?}"
    );
    // The background run ends: the waiting background request takes background's half.
    drop(smoke);
    assert_eq!(other.granted().await, 4);
    drop((gate, task, other));
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_gate_holds_up_neither_background_nor_a_later_gate_while_tokens_are_free() {
    let (gateway, root, socket) = gateway(8).await;
    let mut gate = Asker::jobserver(&socket, 4, "/w/gate").await;
    assert_eq!(gate.granted().await, 4);
    let mut background = Asker::ask(&socket, 1, "/w/background").await;
    assert_eq!(background.granted().await, 1);
    let mut next_gate = Asker::jobserver(&socket, 2, "/w/next-gate").await;
    assert_eq!(next_gate.granted().await, 2);
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
    assert_eq!(a.granted().await, 2);
    // Background holds its half, so c queues.
    let mut queued = Asker::ask(&socket, 1, "/w/c").await;
    // A land task asks the gate job's pool after c queued. The pool lends nothing of its own: the
    // child is granted the host's free token, ahead of c, not a share of the gate's single token.
    let mut task = Asker::child(&pool, 2).await;
    assert_eq!(task.granted().await, 1);
    assert_eq!(queued.granted_within(Duration::from_millis(50)).await, None);
    let status = ledger(&socket).await;
    let land = status
        .checkouts
        .iter()
        .find(|checkout| checkout.checkout == "/w/land")
        .expect("the land holds");
    assert_eq!((land.held, land.running), (2, 2), "{status:?}");
    drop(task);
    // Background still holds its half: c is served when background returns a token.
    assert_eq!(queued.granted_within(Duration::from_millis(50)).await, None);
    drop(a);
    assert_eq!(queued.granted().await, 1);
    drop((gate, queued));
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_older_land_task_is_served_before_a_younger_lands_and_a_feature_builds_that_asked_first()
{
    let (gateway, root, socket) = gateway(4).await;
    let (older, older_pool) = Asker::gate_job(&socket, "/w/older").await;
    let (younger, younger_pool) = Asker::gate_job(&socket, "/w/younger").await;
    // A feature shed holds the rest of the host with two runners, and its next one waits first.
    let mut feature_one = Asker::ask(&socket, 1, "/w/feature").await;
    assert_eq!(feature_one.granted().await, 1);
    let mut feature_two = Asker::ask(&socket, 1, "/w/feature").await;
    assert_eq!(feature_two.granted().await, 1);
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
    assert_eq!(
        feature_next.granted_within(Duration::from_millis(50)).await,
        None
    );
    drop(feature_two);
    assert_eq!(feature_next.granted().await, 1);
    drop((older, younger, younger_task, feature_next));
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

/// The measured failure, over the socket: an older land's gate job holds most of the host with
/// more of its tasks queued, background holds the rest (one token in the younger land's own
/// checkout), and a younger land's gate job asks for its orchestrator's token. It is served at the
/// first returned token, on its floor, ahead of the older land's queued task, and no admission
/// bound ever refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_younger_land_is_served_its_floor_while_an_older_land_holds_most_of_the_host() {
    let (gateway, root, socket) = gateway(8).await;
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
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jobserver_class_is_validated_and_a_wait_bound_is_an_unknown_field() {
    let (gateway, root, socket) = gateway(4).await;
    for fields in [
        serde_json::json!({"class":"pretend-gate"}),
        serde_json::json!({"class":"gate","waitMs":120_000}),
        serde_json::json!({"class":"background","waitMs":100}),
        // A resident asks with its own operation, never as a token-holding jobserver class.
        serde_json::json!({"class":"resident"}),
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
    let mut stream = UnixStream::connect(socket).await.expect("connect resident");
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
    let (gateway, root, socket) = gateway(4).await;
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
        child.kill().await.expect("SIGKILL and reap the resident");
        while UnixStream::connect(&pool).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    assert_eq!(ledger(&socket).await.held, 0);
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_land_build_granted_one_token_grows_through_its_gate_jobs_pool_up_to_its_want() {
    let (gateway, root, socket) = gateway(4).await;
    let (gate, pool) = Asker::gate_job(&socket, "/w/land").await;
    let mut a = Asker::ask(&socket, 2, "/w/a").await;
    assert_eq!(a.granted().await, 2);
    // A land's Cargo build asks the gate job's pool for 3 while background holds its half: it
    // starts at once on the one token left.
    let mut build = Asker::send(
        &pool,
        serde_json::json!({ "op": "cpu-pool", "want": 3, "grow": true }),
    )
    .await;
    assert_eq!(build.granted().await, 1);
    // Two return: the build grows by two to its want of three, and never past it.
    drop(a);
    assert_eq!(build.grown().await, 3);
    assert_eq!(build.grown_within(Duration::from_millis(50)).await, None);
    let status = ledger(&socket).await;
    assert_eq!(status.held, 4, "{status:?}");
    drop((gate, build));
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_root_cargo_build_that_grows_is_handed_its_tokens_through_its_fifo() {
    let (gateway, root, socket) = gateway(8).await;
    let mut holder = Asker::ask(&socket, 3, "/w/x").await;
    assert_eq!(holder.granted().await, 3);
    let mut build = Asker::send(
        &socket,
        serde_json::json!({ "op": "jobserver", "want": 4, "checkout": "/w/build",
            "command": "cargo build", "grow": true }),
    )
    .await;
    let granted = build.answer().await;
    assert_eq!(
        (granted["lease"].as_str(), granted["tokens"].as_u64()),
        (Some("granted"), Some(1))
    );
    let fifo = PathBuf::from(
        granted["fifo"]
            .as_str()
            .expect("a jobserver grant names its fifo"),
    );
    drop(holder);
    assert_eq!(build.grown().await, 4);
    // The fifo now holds every token beyond the implicit first: none at the grant, three grown.
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&fifo)
        .expect("open the grant's fifo");
    let mut bytes = Vec::new();
    let _ = std::io::Read::read_to_end(&mut file, &mut bytes);
    assert_eq!(bytes, b"+++");
    drop(build);
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
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

    let (gateway, root, socket) = gateway(4).await;
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
    let _ = std::fs::remove_dir_all(root);
}

/// Work a holder starts draws on its grant (05_gateway.md "Work a holder starts draws on its
/// grant"). A holder at background's whole room waits on a child job it asked for; asked of the
/// host, that child could never be granted, since background holds all of its room and the floor
/// grants only below it. Asked with the holder's live pool, the child is granted at once from the
/// holder's tokens while the host stays saturated, and the host ledger takes no place for it. A
/// pool that answers and refuses refuses the child, never a second, host grant; only a pool that
/// cannot be reached leaves the child to the host.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_holder_at_backgrounds_room_has_its_childs_exec_granted_from_its_own_pool() {
    use cowshed_core::api::{ExecClass, JobId};
    use cowshed_core::runtime::job_cpu::{GatewayJobCpu, JobCpuAdmission as _, JobCpuAsk};

    let (gateway, root, socket) = gateway(4).await;
    let admission = GatewayJobCpu::new(socket.clone());
    let ask = |want: usize, checkout: &str, job: u64, parent: Option<&Path>| JobCpuAsk {
        class: ExecClass::Background,
        want: NonZeroUsize::new(want).unwrap(),
        checkout: PathBuf::from(checkout),
        command: format!("{checkout} job {job}"),
        job: (
            cowshed_core::repository::RepoId::parse("owner/repo").unwrap(),
            cowshed_core::metadata::WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef")
                .unwrap(),
            JobId::new(job).unwrap(),
        ),
        parent: parent.map(|pool| pool.to_string_lossy().into_owned()),
    };

    // The holder takes background's whole room: 2 of 4 tokens.
    let holder = admission
        .admit(ask(4, "/w/holder", 1, None))
        .await
        .expect("the holder is granted");
    assert_eq!(holder.tokens(), NonZeroUsize::new(2));
    // Asked of the host, the holder's child waits behind it: the host is saturated.
    let mut host_child = Box::pin(admission.admit(ask(1, "/w/scratch", 2, None)));
    loop {
        tokio::select! {
            granted = &mut host_child => panic!("background had no room left: {granted:?}"),
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        if ledger(&socket)
            .await
            .background
            .iter()
            .any(|stake| stake.checkout == "/w/scratch" && !stake.waiters.is_empty())
        {
            break;
        }
    }

    // Asked with the holder's pool, it is granted from the holder's tokens while the host stays
    // saturated, and its own pool is served in its scope like any job's.
    let child = admission
        .admit(ask(1, "/w/scratch", 3, Some(holder.pool())))
        .await
        .expect("the holder's pool grants its child");
    assert_eq!(child.tokens(), NonZeroUsize::new(1));
    assert_eq!(child.pool().parent(), holder.pool().parent());
    let status = ledger(&socket).await;
    assert_eq!(
        status.held, 2,
        "the borrowed token is the holder's, not a host one: {status:?}"
    );

    // The pool lends what it holds: a second child takes the holder's other token.
    let second = admission
        .admit(ask(1, "/w/scratch", 4, Some(holder.pool())))
        .await
        .expect("the holder's second token");
    assert_eq!(second.tokens(), NonZeroUsize::new(1));

    // A pool that answers and refuses refuses the child, and it takes no host grant in its place:
    // the gateway's own control socket answers a pool request as the unknown operation it is.
    let refused = admission
        .admit(ask(1, "/w/scratch", 5, Some(&socket)))
        .await
        .expect_err("a pool that answers and refuses refuses the child");
    assert!(
        refused.to_string().contains("requester's CPU pool"),
        "{refused}"
    );
    assert_eq!(ledger(&socket).await.held, 2);

    // A pool that cannot be reached is absent: the holder gone, its child asks the host and is
    // granted there once the host has room.
    let holder_pool = holder.pool().to_path_buf();
    drop(holder);
    drop((child, second));
    // The holder's pool closes before its tokens return, so once its host child is granted the
    // pool is gone.
    let host = host_child
        .await
        .expect("the host child is granted once room returns");
    let absent = admission
        .admit(ask(1, "/w/scratch", 6, Some(&holder_pool)))
        .await
        .expect("an unreachable pool leaves the child to the host");
    assert_eq!(absent.tokens(), NonZeroUsize::new(1));
    assert_eq!(ledger(&socket).await.held, 2);

    drop((host, absent));
    while ledger(&socket).await.held != 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    gateway.drain().await.expect("drain");
    let _ = std::fs::remove_dir_all(root);
}
