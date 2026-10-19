#![cfg(unix)]
//! Real Cargo under a job's CPU grant, configured by nothing but the variables the grant's holder
//! exports (05_gateway.md "Cargo reads its holder's jobserver"): no `cargo` on `PATH` speaks to
//! the gateway. A `cargo test` holding a one-token grant runs one rustc at a time, its test's
//! nested build draws on the same fifo, and a nested job asking the job's pool socket is granted
//! the holder's own token rather than queueing at the host. Without the variables, the same build
//! runs as Cargo always does.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;

use std::ffi::OsStr;
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

/// The variables a Cargo can inherit that would size it some other way than the grant does.
const AMBIENT_SIZING: [&str; 10] = [
    "CARGO_MAKEFLAGS",
    "MAKEFLAGS",
    "MFLAGS",
    "CARGO_BUILD_JOBS",
    "RUST_TEST_THREADS",
    "NEXTEST_TEST_THREADS",
    "BOUNDED_EXEC_CPU_POOL",
    "BOUNDED_EXEC_CPU_TOKENS",
    "RUSTC_WRAPPER",
    "CARGO_BUILD_RUSTC_WRAPPER",
];

/// A rustc wrapper that records each crate compile's start and end, so the log says how many
/// ran at once. It measures; it changes nothing Cargo does.
const RECORDER: &str = r#"#!/bin/sh
name=""; previous=""
for argument in "$@"; do
  [ "$previous" = "--crate-name" ] && name="$argument"
  previous="$argument"
done
[ -n "$name" ] && { echo "+ $name" >> "$WIDTH_LOG"; sleep 0.3; }
"$@"; status=$?
[ -n "$name" ] && echo "- $name" >> "$WIDTH_LOG"
exit $status
"#;

/// A test that starts work of its own, as a scratch-project test does: it asks the CPU pool it
/// inherited for every core, holds what it is granted, and runs a whole Cargo build meanwhile.
const NESTED_TEST: &str = r#"
use std::io::{BufRead as _, Write as _};

#[test]
fn starts_a_nested_job_and_a_nested_build() {
    let pool = std::env::var("BOUNDED_EXEC_CPU_POOL").expect("the holder's pool");
    let mut stream = std::os::unix::net::UnixStream::connect(pool).expect("connect the pool");
    stream.write_all(b"{\"op\":\"cpu-pool\",\"want\":64}\n").expect("ask the pool");
    let mut lines = std::io::BufReader::new(stream.try_clone().expect("read half"));
    let mut granted = String::new();
    while !granted.contains("\"granted\"") {
        granted.clear();
        assert!(lines.read_line(&mut granted).expect("pool answer") > 0, "the pool closed");
    }
    std::fs::write(std::env::var("NESTED_GRANT").unwrap(), &granted).unwrap();
    let directory = std::env::var("NESTED_DIR").unwrap();
    let status = std::process::Command::new(std::env::var("CARGO").unwrap())
        .args(["build", "--offline", "--quiet"])
        .current_dir(&directory)
        .env("CARGO_TARGET_DIR", format!("{directory}/target"))
        .env("WIDTH_LOG", std::env::var("NESTED_LOG").unwrap())
        .status()
        .expect("run the nested build");
    assert!(status.success());
    drop(stream);
}
"#;

/// A workspace of four independent crates and one that depends on all of them: unconfigured,
/// Cargo compiles the four side by side.
fn workspace(root: &Path, test: Option<&str>) {
    let mut members = Vec::new();
    for name in ["a", "b", "c", "d"] {
        let crate_root = root.join(name);
        std::fs::create_dir_all(crate_root.join("src")).unwrap();
        std::fs::write(
            crate_root.join("Cargo.toml"),
            format!("[package]\nname = \"{name}\"\nversion = \"0.0.0\"\nedition = \"2024\"\n"),
        )
        .unwrap();
        std::fs::write(crate_root.join("src/lib.rs"), "pub fn one() -> u32 { 1 }\n").unwrap();
        members.push(format!("\"{name}\""));
    }
    let top = root.join("top");
    std::fs::create_dir_all(top.join("src")).unwrap();
    std::fs::write(
        top.join("Cargo.toml"),
        "[package]\nname = \"top\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[dependencies]\n\
         a = { path = \"../a\" }\nb = { path = \"../b\" }\nc = { path = \"../c\" }\nd = { path = \"../d\" }\n",
    )
    .unwrap();
    std::fs::write(
        top.join("src/lib.rs"),
        "pub fn four() -> u32 { a::one() + b::one() + c::one() + d::one() }\n",
    )
    .unwrap();
    if let Some(test) = test {
        std::fs::create_dir_all(top.join("tests")).unwrap();
        std::fs::write(top.join("tests/nested.rs"), test).unwrap();
    }
    members.push("\"top\"".to_owned());
    std::fs::write(
        root.join("Cargo.toml"),
        format!(
            "[workspace]\nmembers = [{}]\nresolver = \"3\"\n",
            members.join(", ")
        ),
    )
    .unwrap();
}

/// The most compiles the log saw running at once, and how many it saw in all.
fn width(log: &Path) -> (usize, usize) {
    let text = std::fs::read_to_string(log).unwrap_or_default();
    let (mut running, mut most, mut started) = (0_usize, 0_usize, 0_usize);
    for line in text.lines() {
        if line.starts_with('+') {
            running += 1;
            started += 1;
            most = most.max(running);
        } else if line.starts_with('-') {
            running -= 1;
        }
    }
    (most, started)
}

/// A real `cargo` command under exactly `environment`, the ambient sizing removed.
fn cargo(
    directory: &Path,
    arguments: &[&str],
    environment: &[(&str, &OsStr)],
) -> std::process::Output {
    let mut command = std::process::Command::new(
        std::env::var_os("CARGO").expect("Cargo names the real driver it ran this test with"),
    );
    command.args(arguments).current_dir(directory);
    for name in AMBIENT_SIZING {
        command.env_remove(name);
    }
    command.env("CARGO_TARGET_DIR", directory.join("target"));
    for (name, value) in environment {
        command.env(name, value);
    }
    command.output().expect("run the real cargo")
}

async fn answer(lines: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> Value {
    let mut line = String::new();
    lines.read_line(&mut line).await.expect("answer");
    serde_json::from_str(&line).expect("an answer is JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cargo_test_under_a_one_token_grant_runs_one_rustc_and_its_nested_work_draws_on_that_grant()
 {
    let fixture = fixture_dir::secure_fixture_dir(&format!(
        "csjob-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    let root = fixture.path();
    let cache = root.join("cache");
    std::fs::create_dir(&cache).unwrap();
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = root.join("gateway.sock");
    let gateway = Gateway::start(
        GatewayConfig {
            control_socket: Some(socket.clone()),
            mirror_cache: MirrorCacheConfig::new(cache),
            cpu_budget: CpuBudgetLimits {
                tokens: NonZeroUsize::new(4).unwrap(),
            },
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway");

    // The job's one-token grant, asked as the supervisor asks it for a `cowshed exec`.
    let (reader, mut writer) = UnixStream::connect(&socket).await.unwrap().into_split();
    let request = serde_json::json!({ "op": "jobserver", "want": 1, "checkout": "/w/job",
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
    assert_eq!(granted["tokens"], 1, "{granted}");
    let fifo = PathBuf::from(granted["fifo"].as_str().expect("the job's jobserver fifo"));
    let pool = PathBuf::from(granted["cpuPool"].as_str().expect("the job's pool"));
    assert_eq!(
        fifo.parent(),
        pool.parent(),
        "the fifo sits beside the pool socket"
    );
    let makeflags = format!("-j --jobserver-auth=fifo:{}", fifo.display());
    let makeflags = OsStr::new(&makeflags);

    let recorder = root.join("recorder.sh");
    std::fs::write(&recorder, RECORDER).unwrap();
    std::fs::set_permissions(&recorder, std::fs::Permissions::from_mode(0o755)).unwrap();
    let outer = root.join("outer");
    let nested = root.join("nested");
    workspace(&outer, Some(NESTED_TEST));
    workspace(&nested, None);
    let (outer_log, nested_log, nested_grant) = (
        root.join("outer.log"),
        root.join("nested.log"),
        root.join("nested-grant.json"),
    );
    let one = OsStr::new("1");
    let output = cargo(
        &outer,
        &["test", "--offline", "-p", "top", "--test", "nested"],
        &[
            ("CARGO_MAKEFLAGS", makeflags),
            ("MAKEFLAGS", makeflags),
            ("BOUNDED_EXEC_CPU_POOL", pool.as_os_str()),
            ("BOUNDED_EXEC_CPU_TOKENS", one),
            ("RUST_TEST_THREADS", one),
            ("NEXTEST_TEST_THREADS", one),
            ("RUSTC_WRAPPER", recorder.as_os_str()),
            ("WIDTH_LOG", outer_log.as_os_str()),
            ("NESTED_LOG", nested_log.as_os_str()),
            ("NESTED_DIR", nested.as_os_str()),
            ("NESTED_GRANT", nested_grant.as_os_str()),
        ],
    );
    assert!(output.status.success(), "{output:?}");
    let (outer_width, outer_started) = width(&outer_log);
    let (nested_width, nested_started) = width(&nested_log);
    assert!(
        outer_started >= 6 && nested_started >= 5,
        "every crate compiled"
    );
    assert_eq!(outer_width, 1, "a one-token grant runs one rustc at a time");
    assert_eq!(
        nested_width, 1,
        "the nested build reads the same one-token fifo"
    );
    let nested_grant: Value =
        serde_json::from_str(&std::fs::read_to_string(&nested_grant).unwrap()).unwrap();
    assert_eq!(
        nested_grant["tokens"], 1,
        "the nested job is lent the holder's own token: {nested_grant}"
    );

    // Nothing asked the host: the job's own grant is the only one, and nobody waits.
    let budget = GatewayControlClient::new(socket.clone())
        .unwrap()
        .cpu_budget()
        .await
        .unwrap();
    assert_eq!(budget.held, 1, "{budget:?}");
    assert!(
        budget
            .checkouts
            .iter()
            .all(|checkout| checkout.checkout == "/w/job" && checkout.waiting == 0),
        "{budget:?}"
    );

    // With no pool, nothing is exported and Cargo runs as it does unconfigured.
    let free = root.join("free");
    workspace(&free, None);
    let free_log = root.join("free.log");
    let output = cargo(
        &free,
        &["build", "--offline"],
        &[
            ("RUSTC_WRAPPER", recorder.as_os_str()),
            ("WIDTH_LOG", free_log.as_os_str()),
        ],
    );
    assert!(output.status.success(), "{output:?}");
    let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    if cores > 1 {
        assert!(
            width(&free_log).0 > 1,
            "unconfigured, Cargo compiles independent crates side by side"
        );
    }

    drop((writer, lines));
    while fifo.exists() || pool.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    gateway.drain().await.expect("drain");
}
