#![cfg(unix)]
//! A job's CPU grant as the make jobserver its holder exports (05_gateway.md "Cargo reads its
//! holder's jobserver"), held to the gateway's own contract over its real control socket. A job
//! grant of `g` names a fifo beside its pool socket holding `g − 1` token bytes. The pool's socket
//! lends from that same count, the fifo's bytes first and the holder's own token last, and returns
//! come back to it. The host ledger holds the job's `g` and nothing more. A reader standing in for
//! Cargo takes and writes back bytes exactly as Cargo's jobserver client does. How Cargo itself
//! reads a jobserver is Cargo's to test; a whole-Cargo proof belongs in a nightly, not the gate.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;

use cowshed_os::test_backstop::AsyncUnixStreamBackstop as _;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read as _, Write as _};
use std::num::NonZeroUsize;
use std::os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CpuBudgetLimits,
    CredentialError, CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    GatewayControlClient, MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

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

/// The job's checkout in the host ledger.
const CHECKOUT: &str = "/w/job";

/// How long a child that must wait is watched for a grant it must not get.
const NOT_GRANTED: Duration = Duration::from_millis(200);

/// A real gateway of 8 tokens, so background's room (the host, with no gate work) holds a job of 3. On Linux
/// `GatewayConfig::validate` demands a data-socket root before anything else, an owned 0o700
/// directory; this test's sockets are its control socket and the pools', so it stays empty.
async fn gateway(root: &Path) -> (Gateway, PathBuf) {
    let cache = root.join("cache");
    std::fs::create_dir(&cache).unwrap();
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = root.join("gateway.sock");
    let config = GatewayConfig {
        control_socket: Some(socket.clone()),
        mirror_cache: MirrorCacheConfig::new(cache),
        cpu_budget: CpuBudgetLimits {
            tokens: NonZeroUsize::new(8).unwrap(),
        },
        ..GatewayConfig::default()
    };
    #[cfg(target_os = "linux")]
    let config = {
        let data = root.join("data");
        std::fs::create_dir(&data).unwrap();
        std::fs::set_permissions(&data, std::fs::Permissions::from_mode(0o700)).unwrap();
        GatewayConfig {
            data_socket_root: Some(data),
            ..config
        }
    };
    let gateway = Gateway::start(
        config,
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway");
    (gateway, socket)
}

/// A job's grant, asked as the supervisor asks it for a `cowshed exec` (`pool: true`): its
/// connection, which holds the grant until it drops, and the fifo and pool socket it names.
struct JobGrant {
    _connection: (
        BufReader<tokio::net::unix::OwnedReadHalf>,
        tokio::net::unix::OwnedWriteHalf,
    ),
    fifo: PathBuf,
    pool: PathBuf,
}

async fn job_grant(socket: &Path, g: usize) -> JobGrant {
    let (reader, mut writer) = UnixStream::connect_bounded(socket)
        .await
        .unwrap()
        .into_split();
    let request = serde_json::json!({ "op": "jobserver", "want": g, "checkout": CHECKOUT,
        "command": "cargo test", "pool": true,
        "job": { "repoId": "owner/repo", "workspaceIncarnation": "0123456789abcdef0123456789abcdef",
            "jobId": 7 } });
    writer
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut lines = BufReader::new(reader);
    assert_eq!(answer(&mut lines).await["lease"], "queued");
    let granted = answer(&mut lines).await;
    assert_eq!(granted["tokens"], g, "{granted}");
    let fifo = PathBuf::from(granted["fifo"].as_str().expect("the job's jobserver fifo"));
    let pool = PathBuf::from(granted["cpuPool"].as_str().expect("the job's pool"));
    JobGrant {
        _connection: (lines, writer),
        fifo,
        pool,
    }
}

async fn answer<R: tokio::io::AsyncBufRead + Unpin>(lines: &mut R) -> Value {
    let mut line = String::new();
    lines.read_line(&mut line).await.expect("answer");
    serde_json::from_str(&line).expect("an answer is JSON")
}

/// A reader of the fifo, as Cargo's jobserver client opens it: it takes what is there now and
/// writes back what it took.
struct FifoReader {
    fifo: File,
    held: usize,
}

impl FifoReader {
    fn open(path: &Path) -> Self {
        let fifo = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .expect("open the job's fifo");
        Self { fifo, held: 0 }
    }

    /// Takes every token byte in the fifo now; answers how many.
    fn take_all(&mut self) -> usize {
        let mut taken = 0;
        let mut byte = [0_u8; 1];
        loop {
            match self.fifo.read(&mut byte) {
                Ok(0) => break,
                Ok(_) => taken += 1,
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => panic!("read the job's fifo: {error}"),
            }
        }
        self.held += taken;
        taken
    }

    /// Takes the fifo's bytes as they arrive until it holds `count`. A return that never arrives
    /// is a hang the leg's no-progress bound names, never a deadline of this test's.
    async fn take_until(&mut self, count: usize) {
        while self.held < count {
            self.take_all();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Writes back every byte it holds.
    fn give_back(&mut self) {
        self.fifo.write_all(&vec![b'+'; self.held]).unwrap();
        self.held = 0;
    }
}

/// A child asking the job's pool socket for `want`, answered `queued`.
async fn child(pool: &Path, want: usize) -> BufReader<UnixStream> {
    let mut reader = BufReader::new(UnixStream::connect_bounded(pool).await.unwrap());
    reader
        .get_mut()
        .write_all(format!("{{\"op\":\"cpu-pool\",\"want\":{want}}}\n").as_bytes())
        .await
        .unwrap();
    assert_eq!(answer(&mut reader).await["lease"], "queued");
    reader
}

/// Quiet-window check: whether the child is granted within `within`; `None` while it waits.
async fn granted_within(child: &mut BufReader<UnixStream>, within: Duration) -> Option<u64> {
    let granted = tokio::time::timeout(within, answer(child)).await.ok()?;
    assert_eq!(granted["lease"], "granted", "{granted}");
    granted["tokens"].as_u64()
}

/// The tokens of the child's grant, waiting for it.
async fn granted(child: &mut BufReader<UnixStream>) -> u64 {
    let granted = answer(child).await;
    assert_eq!(granted["lease"], "granted", "{granted}");
    granted["tokens"]
        .as_u64()
        .expect("a grant names its tokens")
}

/// The host ledger holds the job's `g` alone, for its own checkout, with nobody waiting.
async fn assert_host_holds_only(socket: &Path, g: usize) {
    let budget = GatewayControlClient::new(socket.to_path_buf())
        .unwrap()
        .cpu_budget()
        .await
        .unwrap();
    assert_eq!(budget.held, g, "{budget:?}");
    assert!(
        budget
            .checkouts
            .iter()
            .all(|checkout| checkout.checkout == CHECKOUT && checkout.waiting == 0),
        "{budget:?}"
    );
}

fn scratch() -> fixture_dir::FixtureDir {
    fixture_dir::secure_fixture_dir(&format!(
        "csjob-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ))
}

/// A job grant of `g` names a live fifo beside its pool socket holding exactly `g − 1` token
/// bytes: the job's command holds the implicit first token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_job_grant_names_a_fifo_beside_its_pool_holding_one_fewer_token_than_the_grant() {
    let fixture = scratch();
    let (gateway, socket) = gateway(fixture.path()).await;
    for g in [1, 3] {
        let job = job_grant(&socket, g).await;
        assert_eq!(
            job.fifo.parent(),
            job.pool.parent(),
            "the fifo sits beside the pool socket"
        );
        let metadata = std::fs::metadata(&job.fifo).unwrap();
        assert!(metadata.file_type().is_fifo(), "{}", job.fifo.display());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        let mut reader = FifoReader::open(&job.fifo);
        assert_eq!(
            reader.take_all(),
            g - 1,
            "a grant of {g} puts {} bytes in its fifo",
            g - 1
        );
        reader.give_back();
        assert_host_holds_only(&socket, g).await;
        let (fifo, pool) = (job.fifo.clone(), job.pool.clone());
        drop((reader, job));
        // The grant's end drains and unlinks both.
        while fifo.exists() || pool.exists() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    gateway.drain().await.expect("drain");
}

/// The pool's socket lends from the fifo's count, its bytes first and the holder's own token
/// last. A one-token pool lends its implicit token to a socket child. While a reader holds the
/// bytes of a larger pool, the next child waits, and is granted once the reader writes them back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_child_is_lent_the_fifo_bytes_first_and_the_holders_own_token_last() {
    let fixture = scratch();
    let (gateway, socket) = gateway(fixture.path()).await;

    let one = job_grant(&socket, 1).await;
    let mut lent = child(&one.pool, 8).await;
    assert_eq!(
        granted(&mut lent).await,
        1,
        "a one-token pool lends its holder's own token"
    );
    let mut next = child(&one.pool, 1).await;
    assert_eq!(granted_within(&mut next, NOT_GRANTED).await, None);
    assert_host_holds_only(&socket, 1).await;
    drop((lent, next, one));

    let three = job_grant(&socket, 3).await;
    // Free bytes go first: a child asking for two takes the fifo's two, not the holder's token.
    let mut bytes = child(&three.pool, 2).await;
    assert_eq!(granted(&mut bytes).await, 2);
    let mut reader = FifoReader::open(&three.fifo);
    assert_eq!(reader.take_all(), 0, "the child's bytes left the fifo");
    // Only the holder's own token is left, and it goes last: to the next child.
    let mut last = child(&three.pool, 8).await;
    assert_eq!(granted(&mut last).await, 1);
    drop(bytes);
    // The returned bytes are in the fifo again; a reader holding them leaves the next child waiting.
    reader.take_until(2).await;
    let mut waiting = child(&three.pool, 2).await;
    assert_eq!(granted_within(&mut waiting, NOT_GRANTED).await, None);
    reader.give_back();
    assert_eq!(
        granted(&mut waiting).await,
        2,
        "the bytes the reader wrote back go to the waiting child"
    );
    assert_host_holds_only(&socket, 3).await;
    drop((waiting, last, reader, three));
    gateway.drain().await.expect("drain");
}

/// A closed child's tokens reach the child waiting behind it. Nothing adds to the fifo beyond
/// the grant: once every child has closed it holds `g − 1` bytes again, and the host ledger holds
/// the job's `g` alone throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returns_reach_the_waiting_child_and_the_fifo_never_grows_past_the_grant() {
    let fixture = scratch();
    let (gateway, socket) = gateway(fixture.path()).await;
    let g = 3;
    let job = job_grant(&socket, g).await;

    let mut first = child(&job.pool, 8).await;
    assert_eq!(granted(&mut first).await, g as u64);
    let mut second = child(&job.pool, 8).await;
    assert_eq!(granted_within(&mut second, NOT_GRANTED).await, None);
    assert_host_holds_only(&socket, g).await;
    drop(first);
    assert_eq!(
        granted(&mut second).await,
        g as u64,
        "the closed child's tokens go to the child waiting behind it"
    );
    assert_host_holds_only(&socket, g).await;
    drop(second);

    // Every child has closed: the fifo holds g − 1 bytes again, never more.
    let mut reader = FifoReader::open(&job.fifo);
    reader.take_until(g - 1).await;
    tokio::time::sleep(NOT_GRANTED).await;
    reader.take_all();
    assert_eq!(
        reader.held,
        g - 1,
        "the fifo holds the grant's bytes again and nothing beyond them"
    );
    reader.give_back();
    assert_host_holds_only(&socket, g).await;
    drop((reader, job));
    gateway.drain().await.expect("drain");
}
