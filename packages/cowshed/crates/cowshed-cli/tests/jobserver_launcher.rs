#![cfg(target_os = "macos")]
//! The `cargo` jobserver launcher against a real gateway: each of its four
//! cases, the variables it sets or leaves, a `SIGKILL`ed `cargo` mid-build
//! leaving the ledger whole and its fifo unlinked, and a build under a job's
//! CPU pool running on through a gateway restart.

use cowshed_os::fork_lock::{Run as _, Spawn as _};
use std::io::Write as _;
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use cowshed_cli::jobserver_launcher::run_at;
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, CpuBudgetLimits,
    CredentialError, CredentialProvider, CredentialQuery, CredentialRecord, Gateway, GatewayConfig,
    MirrorCacheConfig, UpstreamConnection, UpstreamConnector, UpstreamHealth,
};

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

fn scratch(name: &str) -> PathBuf {
    let root = PathBuf::from(format!("/tmp/cslaunch-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("scratch");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).expect("private");
    root
}

/// Set a variable for the launcher's process-wide environment reads.
fn set(name: &str, value: &str) {
    // SAFETY: this test is the only one in its binary that touches these
    // variables, and it sets them before spawning any child.
    unsafe { std::env::set_var(name, value) };
}

fn unset(name: &str) {
    // SAFETY: see `set`.
    unsafe { std::env::remove_var(name) };
}

fn held(socket: &Path) -> u64 {
    let mut stream = StdUnixStream::connect(socket).expect("connect");
    stream
        .write_all(b"{\"op\":\"cpu-budget\"}\n")
        .expect("ask ledger");
    // The gateway answers a one-shot request once the request is complete:
    // half-close, then read to the end of the answer.
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("half-close");
    let mut answer = String::new();
    std::io::Read::read_to_string(&mut stream, &mut answer).expect("ledger answer");
    let answer: serde_json::Value = serde_json::from_str(answer.trim_end()).expect("ledger JSON");
    answer["cpuBudget"]["held"].as_u64().expect("a ledger")
}

/// A gateway on `root`'s control socket. A restarted gateway starts on the same root and reuses
/// its cache, as launchd's restart of the installed daemon does.
async fn start_gateway(root: &Path, tokens: usize) -> (Gateway, PathBuf) {
    let cache = root.join("cache");
    std::fs::create_dir_all(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700))
        .expect("private cache");
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
    (gateway, socket)
}

const FAKE_CARGO: &str = r#"#!/bin/sh
{
  echo "CARGO_MAKEFLAGS=$CARGO_MAKEFLAGS"
  echo "MAKEFLAGS=$MAKEFLAGS"
  echo "CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS"
  echo "RUST_TEST_THREADS=$RUST_TEST_THREADS"
  echo "NEXTEST_TEST_THREADS=$NEXTEST_TEST_THREADS"
  echo "BOUNDED_EXEC_CPU_TOKENS=$BOUNDED_EXEC_CPU_TOKENS"
} > "$FAKE_CARGO_OUT"
echo $$ > "$FAKE_CARGO_PID"
if [ "$FAKE_CARGO_SLEEP" = "1" ]; then exec sleep 60; fi
if [ -n "$FAKE_CARGO_WAIT" ]; then
  while [ ! -e "$FAKE_CARGO_WAIT" ]; do sleep 0.05; done
  echo finished >> "$FAKE_CARGO_OUT"
fi
exit "${FAKE_CARGO_EXIT:-0}"
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_launcher_sizes_each_case_and_a_killed_cargo_leaves_the_ledger_whole() {
    let root = scratch("cases");
    let original_cwd = std::env::current_dir().expect("original cwd");
    std::env::set_current_dir(&root).expect("explicit nonmanaged checkout");
    let bin = root.join("bin");
    std::fs::create_dir(&bin).expect("bin");
    let fake = bin.join("cargo");
    std::fs::write(&fake, FAKE_CARGO).expect("fake cargo");
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).expect("executable");
    let out = root.join("env.txt");
    let pid_file = root.join("pid.txt");

    let (gateway, socket) = start_gateway(&root, 4).await;

    let original_path = std::env::var("PATH").unwrap_or_default();
    set("PATH", &format!("{}:{original_path}", bin.display()));
    set("FAKE_CARGO_OUT", &out.display().to_string());
    set("FAKE_CARGO_PID", &pid_file.display().to_string());
    for name in [
        "CARGO_MAKEFLAGS",
        "MAKEFLAGS",
        "CARGO_BUILD_JOBS",
        "RUST_TEST_THREADS",
        "NEXTEST_TEST_THREADS",
        "BOUNDED_EXEC_CPU_TOKENS",
        "BOUNDED_EXEC_CPU_POOL",
        "FAKE_CARGO_SLEEP",
    ] {
        unset(name);
    }
    set("FAKE_CARGO_EXIT", "7");
    let run = |arguments: &[&str], socket: &Path| {
        let arguments: Vec<std::ffi::OsString> = arguments.iter().map(Into::into).collect();
        let socket = socket.to_path_buf();
        std::thread::spawn(move || run_at(&arguments, &socket)).join()
    };
    let env_out = || std::fs::read_to_string(&out).expect("fake cargo ran");

    // A build asks for one token per core and is granted min(cores, 4) on this budget of 4: a
    // 3-core runner asks for, and runs with, 3. It runs under the fifo.
    let granted = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(4);
    let code = run(&["build"], &socket).expect("launcher thread");
    assert_eq!(code, 7, "the child's status is the launcher's");
    let shown = env_out();
    let makeflags = shown
        .lines()
        .find_map(|line| line.strip_prefix("CARGO_MAKEFLAGS="))
        .expect("makeflags shown");
    let fifo = makeflags
        .strip_prefix("-j --jobserver-auth=fifo:")
        .expect("the grant's fifo is named");
    assert!(
        fifo.starts_with(&*root.join("jobserver").display().to_string()),
        "{fifo}"
    );
    assert!(
        shown.contains(&format!("BOUNDED_EXEC_CPU_TOKENS={granted}\n")),
        "{shown}"
    );
    assert!(
        shown.contains(&format!("RUST_TEST_THREADS={granted}\n")),
        "{shown}"
    );
    assert!(
        shown.contains(&format!("NEXTEST_TEST_THREADS={granted}\n")),
        "{shown}"
    );
    assert!(
        shown.contains("CARGO_BUILD_JOBS=\n"),
        "no static job count under a jobserver: {shown}"
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while Path::new(fifo).exists() || held(&socket) != 0 {
        assert!(
            Instant::now() < deadline,
            "the finished build\'s grant outlives it"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    // A subcommand that compiles nothing runs directly even with a gateway up.
    let code = run(&["metadata"], &socket).expect("launcher thread");
    assert_eq!(code, 7);
    let shown = env_out();
    assert!(shown.contains("CARGO_MAKEFLAGS=\n"), "{shown}");
    assert_eq!(held(&socket), 0);

    // Under a bounded-exec grant the count sizes cargo where unset.
    set("BOUNDED_EXEC_CPU_TOKENS", "2");
    let code = run(&["build"], &socket).expect("launcher thread");
    assert_eq!(code, 7);
    let shown = env_out();
    assert!(shown.contains("CARGO_BUILD_JOBS=2"), "{shown}");
    assert!(shown.contains("RUST_TEST_THREADS=2"), "{shown}");
    assert!(shown.contains("CARGO_MAKEFLAGS=\n"), "{shown}");
    unset("BOUNDED_EXEC_CPU_TOKENS");

    // An inherited jobserver whose fifo exists runs cargo as asked.
    let inherited = root.join("inherited.fifo");
    std::fs::write(&inherited, []).expect("inherited fifo placeholder");
    let flags = format!("-j --jobserver-auth=fifo:{}", inherited.display());
    set("CARGO_MAKEFLAGS", &flags);
    let code = run(&["build"], &socket).expect("launcher thread");
    assert_eq!(code, 7);
    let shown = env_out();
    assert!(
        shown.contains(&format!("CARGO_MAKEFLAGS={flags}")),
        "{shown}"
    );
    unset("CARGO_MAKEFLAGS");

    // With a missing gateway socket a nonmanaged build runs unbudgeted.
    let absent = root.join("absent.sock");
    let code = run(&["build"], &absent).expect("launcher thread");
    assert_eq!(code, 7);
    let shown = env_out();
    assert!(shown.contains("CARGO_MAKEFLAGS=\n"), "{shown}");

    // Missing and failed admissions never start a managed payload, even below its marker root.
    let metadata = root.join(".cowshed");
    std::fs::create_dir(&metadata).expect("managed metadata directory");
    let marker = metadata.join("workspace.json");
    std::fs::write(&marker, b"{\"role\":\"workspace\"}").expect("managed marker");
    let nested = root.join("crate");
    std::fs::create_dir(&nested).expect("nested crate cwd");
    std::env::set_current_dir(&nested).expect("run below the managed marker");
    std::fs::remove_file(&out).expect("discard prior payload output");
    assert_eq!(run(&["build"], &absent).expect("managed absent outcome"), 1);
    assert!(!out.exists(), "managed gateway absence never starts cargo");
    for (name, answer) in [
        ("refused", "{\"ok\":false,\"error\":\"fixture-refusal\"}\n"),
        ("protocol", "not-json\n"),
        ("closed", "{\"ok\":true,\"lease\":\"queued\"}\n"),
    ] {
        let endpoint = root.join(format!("{name}.sock"));
        let listener = std::os::unix::net::UnixListener::bind(&endpoint).expect("fixture endpoint");
        let server = std::thread::spawn(move || {
            use std::io::BufRead as _;
            let (mut stream, _) = listener.accept().expect("launcher connected");
            let mut request = String::new();
            std::io::BufReader::new(&stream)
                .read_line(&mut request)
                .expect("jobserver request");
            stream.write_all(answer.as_bytes()).expect("fixture reply");
        });
        assert_eq!(
            run(&["build"], &endpoint).expect("managed refusal outcome"),
            1,
            "{name}"
        );
        server.join().expect("fixture server ends");
        assert!(!out.exists(), "managed {name} never starts cargo");
        std::fs::remove_file(&endpoint).expect("retire fixture endpoint");
    }
    // Noncompiling discovery remains exempt, including in a managed checkout.
    assert_eq!(run(&["metadata"], &absent).expect("metadata exemption"), 7);
    std::fs::remove_file(&out).expect("discard exempt payload output");
    std::fs::remove_file(&marker).expect("remove marker");
    std::fs::remove_dir(&metadata).expect("remove metadata directory");
    std::fs::write(&metadata, b"not a directory").expect("uninspectable marker parent");
    assert_eq!(
        run(&["build"], &socket).expect("marker inspection outcome"),
        1
    );
    assert!(!out.exists(), "inspection uncertainty never starts cargo");
    std::fs::remove_file(&metadata).expect("remove malformed marker parent");
    std::env::set_current_dir(&root).expect("return to explicit nonmanaged checkout");

    // A refused socket is not absent, even when this checkout is not managed.
    let refused = root.join("nonmanaged-refused.sock");
    let listener = std::os::unix::net::UnixListener::bind(&refused)
        .expect("nonmanaged refused fixture endpoint");
    drop(listener);
    assert_eq!(
        run(&["build"], &refused).expect("nonmanaged connection-refused outcome"),
        1
    );
    assert!(!out.exists(), "a refused host gateway never starts cargo");
    std::fs::remove_file(&refused).expect("retire nonmanaged refused fixture endpoint");
    // Connected protocol/refusal failures are not absence, even outside cowshed.
    for (name, answer) in [
        (
            "host-refused",
            "{\"ok\":false,\"error\":\"fixture-host-refusal\"}\n",
        ),
        ("host-protocol", "not-json\n"),
    ] {
        let endpoint = root.join(format!("{name}.sock"));
        let listener =
            std::os::unix::net::UnixListener::bind(&endpoint).expect("host fixture endpoint");
        let server = std::thread::spawn(move || {
            use std::io::BufRead as _;
            let (mut stream, _) = listener.accept().expect("host launcher connected");
            let mut request = String::new();
            std::io::BufReader::new(&stream)
                .read_line(&mut request)
                .expect("host jobserver request");
            stream
                .write_all(answer.as_bytes())
                .expect("host fixture reply");
        });
        assert_eq!(
            run(&["build"], &endpoint).expect("host connected refusal outcome"),
            1,
            "{name}"
        );
        server.join().expect("host fixture server ends");
        assert!(!out.exists(), "host {name} cannot be relabeled as absent");
        std::fs::remove_file(&endpoint).expect("retire host fixture endpoint");
    }
    // Exercise the real CLI entrypoint through cargo -> cowshed -> this test build.
    // A metadata command must reach the next Cargo, not the cowshed CLI parser.
    let installed = root.join("installed");
    std::fs::create_dir(&installed).expect("installed bin");
    let cli = std::env::var_os("CARGO_BIN_EXE_cowshed")
        .expect("cargo and nextest publish the runtime CLI binary");
    std::os::unix::fs::symlink(cli, installed.join("cowshed")).expect("stable name");
    let launcher = installed.join("cargo");
    std::os::unix::fs::symlink("cowshed", &launcher).expect("relative launcher link");
    let output = std::process::Command::new(&launcher)
        .arg("metadata")
        .env(
            "PATH",
            format!("{}:{}:{original_path}", installed.display(), bin.display()),
        )
        .output_locked()
        .expect("launch the real cargo entrypoint");
    assert_eq!(output.status.code(), Some(7), "{output:?}");
    assert!(env_out().contains("CARGO_MAKEFLAGS=\n"));

    // A cargo killed mid-build: the launcher reports its death, the gateway
    // unlinks the fifo and credits every token.
    set("FAKE_CARGO_SLEEP", "1");
    unset("FAKE_CARGO_EXIT");
    let arguments: Vec<std::ffi::OsString> = ["build"].iter().map(Into::into).collect();
    let sleeper_socket = socket.clone();
    std::fs::remove_file(&pid_file).expect("discard the previous child identity");
    let handle = std::thread::spawn(move || run_at(&arguments, &sleeper_socket));
    let deadline = Instant::now() + Duration::from_secs(15);
    let pid = loop {
        if held(&socket) == u64::try_from(granted).expect("a grant fits u64")
            && let Ok(identity) = std::fs::read_to_string(&pid_file)
            && let Ok(pid) = identity.trim().parse::<i32>()
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "the granted child never started");
        std::thread::sleep(Duration::from_millis(25));
    };
    let shown = env_out();
    let fifo = shown
        .lines()
        .find_map(|line| line.strip_prefix("CARGO_MAKEFLAGS="))
        .and_then(|flags| flags.strip_prefix("-j --jobserver-auth=fifo:"))
        .expect("the sleeping build holds a fifo")
        .to_owned();
    assert!(
        Path::new(&fifo).exists(),
        "the running child holds a live fifo"
    );
    // SAFETY: the pid is the fake cargo this test spawned.
    unsafe { libc::kill(pid, libc::SIGKILL) };
    let code = handle.join().expect("launcher thread");
    assert_eq!(
        code,
        128 + 9,
        "a cargo killed by SIGKILL ends the launcher likewise"
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    while Path::new(&fifo).exists() || held(&socket) != 0 {
        assert!(
            Instant::now() < deadline,
            "the killed build's grant outlives it"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    unset("FAKE_CARGO_SLEEP");

    // A gateway restart under a running build (05_gateway.md "Host CPU budget": a restart forgets
    // every grant, and what runs under the old gateway runs on over budget until it finishes). A
    // land gate's job holds its root grant and the gateway serves the job's CPU pool; the real
    // CLI, as `cargo` under that pool, holds a child grant from it. The gateway then takes the
    // drain a SIGTERM'd daemon takes and a new one starts on its socket, as launchd's restart of
    // an install does. The build hears its pool close, says so, and ends with its own status.
    let job = {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};
        let mut stream = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("connect the gate job");
        let request = serde_json::json!({ "op": "jobserver", "want": 1,
            "checkout": root.display().to_string(), "command": "nx run-many -t gate",
            "class": "gate", "waitMs": 120_000, "pool": true,
            "job": { "repoId": "owner/repo",
                "workspaceIncarnation": "0123456789abcdef0123456789abcdef", "jobId": 1 } });
        stream
            .write_all(format!("{request}\n").as_bytes())
            .await
            .expect("ask for the gate job's grant");
        let mut lines = tokio::io::BufReader::new(stream);
        let pool = loop {
            let mut line = String::new();
            lines.read_line(&mut line).await.expect("gate job answer");
            let answer: serde_json::Value = serde_json::from_str(&line).expect("gate job JSON");
            if answer["lease"] == "queued" {
                continue;
            }
            assert_eq!(answer["lease"], "granted", "{answer}");
            break PathBuf::from(
                answer["cpuPool"]
                    .as_str()
                    .expect("a gate job names its pool"),
            );
        };
        (lines, pool)
    };
    let go = root.join("go");
    std::fs::remove_file(&pid_file).expect("discard the previous child identity");
    let mut build = std::process::Command::new(&launcher)
        .arg("build")
        .env(
            "PATH",
            format!("{}:{}:{original_path}", installed.display(), bin.display()),
        )
        .env("BOUNDED_EXEC_CPU_POOL", &job.1)
        .env("BOUNDED_EXEC_CPU_TOKENS", "1")
        .env("FAKE_CARGO_WAIT", &go)
        .stderr(std::process::Stdio::piped())
        .spawn_locked()
        .expect("start a build under the job's pool");
    let (tell, heard) = std::sync::mpsc::channel::<String>();
    let stderr = build.stderr.take().expect("the build's stderr is piped");
    let reader = std::thread::spawn(move || {
        use std::io::BufRead as _;
        for line in std::io::BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            if tell.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    while !pid_file.exists() {
        assert!(
            Instant::now() < deadline,
            "the build never started under the job's pool"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    gateway
        .drain()
        .await
        .expect("drain the gateway for its restart");
    let (gateway, restarted) = start_gateway(&root, 4).await;
    assert_eq!(restarted, socket);
    assert_eq!(held(&socket), 0, "a restarted gateway forgets every grant");

    let mut said = Vec::new();
    loop {
        let left = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default()
            .max(Duration::from_secs(5));
        let line = heard.recv_timeout(left).unwrap_or_else(|error| {
            panic!("the build never said its pool closed ({error}); it said: {said:#?}")
        });
        let closed = line.contains("runs on over budget until it ends");
        said.push(line);
        if closed {
            break;
        }
    }
    assert!(
        build.try_wait().expect("poll the build").is_none(),
        "the build outlives the restart: {said:#?}"
    );
    std::fs::write(&go, []).expect("let the build finish");
    let status = build.wait().expect("the build ends");
    reader.join().expect("stderr reader ends");
    said.extend(heard.try_iter());
    assert_eq!(
        status.code(),
        Some(0),
        "the build ends with its own status: {said:#?}"
    );
    assert!(env_out().contains("finished"), "the build ran to its end");
    assert_eq!(held(&socket), 0, "the restarted gateway never held it");
    drop(job);
    std::fs::remove_file(&go).expect("retire the go file");

    gateway.drain().await.expect("drain");
    std::env::set_current_dir(original_cwd).expect("restore original cwd");
    std::fs::remove_dir_all(root).expect("retire launcher fixture");
}
