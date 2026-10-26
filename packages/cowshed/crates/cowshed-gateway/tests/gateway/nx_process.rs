//! The gateway's lazily started Nx cache server, `bun <bundle> --exit-on-stdin-eof`, as a real
//! process: its lock, its refusal frame, its death with the gateway, and its stdin-EOF drain. Raw
//! bytes service and process integration only; no archive restore or cutover claim.
use super::*;
use cowshed_os::test_backstop::AsyncChildBackstop as _;

use cowshed_os::fork_lock::Spawn as _;

use std::ffi::OsString;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::ChildStdout;

const READY_PREFIX: &str = "NX_PROCESS_READY ";
const CHILD_ENV: &str = "COWSHED_NX_PROCESS_ROOT";
const REPO: &str = "owner/nx-process";
/// The libtest name of the child, as the test binary's module tree spells it.
const CHILD_TEST: &str = "macos::nx_process::nx_backend_gateway_child";
/// One frame or startup line may be at most this long; anything longer is a protocol error.
const LINE_LIMIT: u64 = 16 * 1024;
/// The whole startup stream of a child or server is capped too, so no read is unbounded.
const STREAM_LIMIT: u64 = 64 * 1024;

/// The server bundle this repository builds (`nx run nx-cache-server:bundle`). The test asserts it
/// exists rather than falling back to any other server.
pub(super) fn server_script() -> PathBuf {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    let path = Path::new(&manifest)
        .join("../../../../packages/nx-cache-server/dist/server/nx-cache-server.js");
    assert!(
        path.is_file(),
        "server bundle is missing at {}: run nx run nx-cache-server:bundle first",
        path.display()
    );
    path.canonicalize().expect("canonical server bundle path")
}

/// The `PATH` the gateway searches for `bun`: this test's own, as setup records its invoker's.
pub(super) fn server_path() -> OsString {
    std::env::var_os("PATH").expect("PATH names the bun the server runs on")
}

/// Whether a `ps` args line is the gateway's server: `<bun> <script> --exit-on-stdin-eof`.
pub(super) fn is_server_args(args: &str, script: &Path) -> bool {
    args.ends_with(&format!("{} --exit-on-stdin-eof", script.display()))
}

/// A direct run of the server bundle under bun, outside any gateway.
fn server_command(script: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("bun");
    command.arg(script);
    command
}

fn nx_put(endpoint: SocketAddr, hash: &str, body: &str) -> String {
    format!(
        "PUT /nx/v1/cache/{hash} HTTP/1.1\r\nHost: {endpoint}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// The environment every server in this test runs under.
fn server_env(command: &mut tokio::process::Command, entries: &Path) {
    command
        .env_clear()
        .env("PATH", server_path())
        .env("NX_CACHE_SERVER_DIR", entries)
        .env("NX_CACHE_SERVER_PORT", "0")
        .env("NX_CACHE_SERVER_MAX_BYTES", "1048576")
        .env("NX_CACHE_SERVER_CAP_BYTES", "67108864");
}

/// Read one newline-terminated line from a bounded stream, or `None` at its end. A line longer
/// than the limit is a protocol error, not something to keep reading.
async fn bounded_line(lines: &mut BufReader<tokio::io::Take<ChildStdout>>) -> Option<Vec<u8>> {
    let mut line = Vec::new();
    let read = lines
        .read_until(b'\n', &mut line)
        .await
        .expect("read bounded child output");
    if read == 0 {
        return None;
    }
    assert!(
        line.len() as u64 <= LINE_LIMIT && line.ends_with(b"\n"),
        "child output line exceeds {LINE_LIMIT} bytes or has no newline"
    );
    Some(line)
}

/// The gateway process of the parent test. Every request and assertion about the HTTP surface
/// runs here, so the gateway is a real `Gateway` whose Nx backend is a real spawned server.
#[tokio::test]
#[ignore = "child of host_controller_nx_backend_process_serves_and_releases_its_store"]
async fn nx_backend_gateway_child() {
    use cowshed_gateway::{MirrorCacheConfig, NxCacheServiceConfig};

    let root =
        PathBuf::from(std::env::var_os(CHILD_ENV).expect(
            "nx process child requires the parent test's fixture root; run its parent test",
        ));
    // Every path the child's gateway uses lives under the parent's guarded root, so a SIGKILL of
    // the gateway leaves nothing outside it.
    let mirror = root.join("mirror");
    let directory = root.join("nx");
    for path in [&mirror, &directory] {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(path)
            .expect("create child-owned directory under the parent root");
    }

    let config = GatewayConfig {
        mirror_cache: MirrorCacheConfig::new(mirror),
        nx_cache: Some(NxCacheServiceConfig {
            script: server_script(),
            path: server_path(),
            directory: directory.clone(),
            max_bytes: 1024 * 1024,
            cap_bytes: 64 * 1024 * 1024,
        }),
        ..GatewayConfig::default()
    };
    let gateway = gateway(
        config,
        Arc::new(NoCredentials),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let handle = gateway.handle();

    let (source, _, _) = session(
        "nx-process-source",
        REPO,
        block_endpoint(free_endpoint()),
        1,
        1,
        WorkspacePolicy::default(),
    );
    let source_endpoint = install_in_free_block(&handle, source).await;
    let (reader, _, _) = session(
        "nx-process-reader",
        REPO,
        block_endpoint(free_endpoint()),
        2,
        1,
        WorkspacePolicy::default(),
    );
    let reader_endpoint = install_in_free_block(&handle, reader).await;

    // The first read of the project starts the real server and misses.
    let response = proxy_request(source_endpoint, nx_request(source_endpoint, "GET", "42")).await;
    assert!(response.starts_with("HTTP/1.1 404"), "{response}");

    // A workspace of the project stores with no credential. The first writer wins: a later PUT of
    // the same key is 409 and replaces nothing.
    let response = proxy_request(
        source_endpoint,
        nx_put(source_endpoint, "42", "accepted-bytes"),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let response = proxy_request(
        reader_endpoint,
        nx_put(reader_endpoint, "42", "differentbytes"),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 409"), "{response}");

    // The second session reads what the first stored, unchanged.
    let response = proxy_request(reader_endpoint, nx_request(reader_endpoint, "GET", "42")).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("accepted-bytes"), "{response}");
    let response = proxy_request(reader_endpoint, nx_request(reader_endpoint, "GET", "43")).await;
    assert!(response.starts_with("HTTP/1.1 404"), "{response}");
    // Every assertion above passed. The parent learns where the store lives and that the gateway
    // is serving; then it holds stdin until the parent closes it.
    let project = directory.join(format!("{:x}", Sha256::digest(REPO.as_bytes())));
    println!(
        "{READY_PREFIX}{}",
        serde_json::json!({
            "gatewayPid": std::process::id(),
            "entries": project.join("entries"),
        })
    );
    let mut stdin_sink = Vec::new();
    tokio::io::stdin()
        .read_to_end(&mut stdin_sink)
        .await
        .expect("read parent stdin to EOF");
    gateway.drain().await.expect("drain nx gateway");
}

/// Wait for the kernel to report the registered process exit. A null timeout blocks until the
/// event arrives, so the wait has no clock: a server that never exits is the test target's
/// no-progress backstop to decide.
async fn await_exit(kq: OwnedFd) -> bool {
    tokio::task::spawn_blocking(move || {
        // SAFETY: an all-zero kevent is a valid buffer for the kernel to overwrite.
        let mut event = unsafe { std::mem::zeroed::<libc::kevent>() };
        // SAFETY: `kq` is a kqueue this closure owns, the event buffer holds one entry, and a
        // null timeout is the documented block-until-an-event form of `kevent`.
        let count = unsafe {
            libc::kevent(
                kq.as_raw_fd(),
                std::ptr::null(),
                0,
                &mut event,
                1,
                std::ptr::null(),
            )
        };
        assert!(
            count >= 0,
            "wait for server exit: {}",
            std::io::Error::last_os_error()
        );
        count == 1 && event.fflags & libc::NOTE_EXIT != 0
    })
    .await
    .expect("exit waiter task")
}

/// One snapshot of the process table: the Nx cache servers `parent` owns.
fn servers_of(parent: u32, script: &Path) -> Vec<u32> {
    let listing = std::process::Command::new("/bin/ps")
        .args(["-axo", "pid=,ppid=,args="])
        .output_bounded()
        .expect("ps snapshot");
    assert!(
        listing.status.success(),
        "ps snapshot failed: {:?}",
        listing.status
    );
    String::from_utf8_lossy(&listing.stdout)
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 3 {
                return None;
            }
            let pid: u32 = fields[0].parse().ok()?;
            let ppid: u32 = fields[1].parse().ok()?;
            (ppid == parent && is_server_args(&fields[2..].join(" "), script)).then_some(pid)
        })
        .collect()
}

/// A kqueue registered for `pid`'s exit, for [`await_exit`].
fn watch_exit(pid: u32) -> OwnedFd {
    // SAFETY: kqueue returns a new descriptor or -1; the descriptor is owned from here on.
    let kq_raw = unsafe { libc::kqueue() };
    assert!(kq_raw >= 0, "kqueue: {}", std::io::Error::last_os_error());
    // SAFETY: `kq_raw` is a fresh descriptor that nothing else owns.
    let kq = unsafe { OwnedFd::from_raw_fd(kq_raw) };
    let change = libc::kevent {
        ident: usize::try_from(pid).expect("pid fits the kevent identifier"),
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: libc::NOTE_EXIT,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    // SAFETY: one change, no returned events; the change buffer lives across the call.
    let registered = unsafe {
        libc::kevent(
            kq.as_raw_fd(),
            &change,
            1,
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    };
    assert_eq!(
        registered,
        0,
        "register NOTE_EXIT for server {pid}: {}",
        std::io::Error::last_os_error()
    );
    kq
}

/// A project's cache server ends with the project's last session, as when a test discards the
/// scratch project it adopted; another project's server is left serving. Measured 2026-10-19:
/// seven of the gateway's nine servers belonged to scratch projects discarded hours earlier.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_projects_cache_server_ends_with_its_last_session() {
    use cowshed_gateway::{MirrorCacheConfig, NxCacheServiceConfig};

    let server = server_script();
    let root = secure_fixture_dir(&format!("cowshed-nx-lifetime-{}", std::process::id()));
    let (mirror, directory) = (root.path().join("mirror"), root.path().join("nx"));
    for path in [&mirror, &directory] {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(path)
            .expect("create fixture directory");
    }
    let gateway = gateway(
        GatewayConfig {
            mirror_cache: MirrorCacheConfig::new(mirror),
            nx_cache: Some(NxCacheServiceConfig {
                script: server.clone(),
                path: server_path(),
                directory,
                max_bytes: 1024 * 1024,
                cap_bytes: 64 * 1024 * 1024,
            }),
            ..GatewayConfig::default()
        },
        Arc::new(NoCredentials),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let handle = gateway.handle();

    // A scratch project with two workspaces, and a project that stays.
    let mut endpoints = Vec::new();
    for (workspace, repo, byte) in [
        ("nx-lifetime-scratch-a", "owner/nx-lifetime-scratch", 1),
        ("nx-lifetime-scratch-b", "owner/nx-lifetime-scratch", 2),
        ("nx-lifetime-kept", "owner/nx-lifetime-kept", 3),
    ] {
        let (installed, _, _) = session(
            workspace,
            repo,
            block_endpoint(free_endpoint()),
            byte,
            1,
            WorkspacePolicy::default(),
        );
        endpoints.push(install_in_free_block(&handle, installed).await);
    }
    // Each project's first read starts its server and misses; the scratch project's starts first.
    let read = |endpoint: &SocketAddr| {
        let endpoint = *endpoint;
        let request = nx_request(endpoint, "GET", "42");
        async move {
            let response = proxy_request(endpoint, request).await;
            assert!(response.starts_with("HTTP/1.1 404"), "{response}");
        }
    };
    read(&endpoints[0]).await;
    let [scratch] = servers_of(std::process::id(), &server)[..] else {
        panic!("the scratch project's read starts one server")
    };
    read(&endpoints[2]).await;
    let servers = servers_of(std::process::id(), &server);
    let [kept] = servers
        .iter()
        .copied()
        .filter(|&pid| pid != scratch)
        .collect::<Vec<_>>()[..]
    else {
        panic!("the kept project's read starts its own server: {servers:?}")
    };
    let scratch_exit = watch_exit(scratch);

    // One of the scratch project's two workspaces leaves: its server keeps serving the other.
    handle
        .remove("nx-lifetime-scratch-a", 1)
        .await
        .expect("remove one scratch session");
    read(&endpoints[1]).await;
    assert_eq!(servers_of(std::process::id(), &server), servers);

    // The last scratch workspace leaves, as when the test discards the project: its server ends,
    // and the kept project's serves on.
    handle
        .remove("nx-lifetime-scratch-b", 1)
        .await
        .expect("remove the last scratch session");
    assert!(
        await_exit(scratch_exit).await,
        "the scratch project's server {scratch} exits"
    );
    assert_eq!(servers_of(std::process::id(), &server), [kept]);
    read(&endpoints[2]).await;
    gateway.drain().await.expect("drain");
}

/// Parent of the gateway child: starts it, finds its server, and checks the process contract.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_nx_backend_process_serves_and_releases_its_store() {
    let server = server_script();
    let root = secure_fixture_dir(&format!("cowshed-nx-process-{}", std::process::id()));
    let mut child =
        tokio::process::Command::new(std::env::current_exe().expect("test binary path"))
            .args(["--ignored", "--exact", CHILD_TEST, "--nocapture"])
            .env(CHILD_ENV, root.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn_locked()
            .expect("spawn gateway child");
    let child_stdin = child.stdin.take().expect("gateway child stdin");
    let stdout = child.stdout.take().expect("gateway child stdout");
    let mut child_lines = BufReader::new(stdout.take(STREAM_LIMIT));

    // The harness may print its own "test ... " prefix on the same line, so match by position.
    let ready: serde_json::Value = async {
        while let Some(line) = bounded_line(&mut child_lines).await {
            let text = String::from_utf8_lossy(&line);
            if let Some(index) = text.find(READY_PREFIX) {
                return serde_json::from_str(text[index + READY_PREFIX.len()..].trim_end())
                    .expect("typed gateway ready frame");
            }
        }
        panic!("gateway child ended before its ready frame")
    }
    .await;
    let gateway_pid = u32::try_from(ready["gatewayPid"].as_u64().expect("gateway pid"))
        .expect("gateway pid fits u32");
    let entries = PathBuf::from(ready["entries"].as_str().expect("entries path"));

    // One snapshot of the process table: exactly one server, owned by this gateway.
    let servers = servers_of(gateway_pid, &server);
    assert_eq!(
        servers.len(),
        1,
        "gateway {gateway_pid} must own exactly one nx-cache-server: {servers:?}"
    );
    let server_pid = servers[0];

    // Register the exit before anything can kill the gateway, so the event cannot be missed.
    let kq = watch_exit(server_pid);

    // While the gateway is live, a second server on the same store is refused structurally.
    let lock = entries.join(".owner.lock");
    let inode_before = std::fs::metadata(&lock)
        .expect("persistent lock file")
        .ino();
    let mut second = server_command(&server);
    server_env(&mut second, &entries);
    second
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let refused = second.output_bounded().await.expect("run second server");
    assert_eq!(
        refused.status.code(),
        Some(1),
        "second server must exit 1: {:?}",
        refused.status
    );
    assert!(
        refused.stdout.len() as u64 <= LINE_LIMIT,
        "refusal frame exceeds {LINE_LIMIT} bytes"
    );
    let frame: serde_json::Value =
        serde_json::from_slice(&refused.stdout).expect("typed store-locked refusal frame");
    assert_eq!(frame["kind"], "failed");
    assert_eq!(frame["stage"], "store-locked");
    assert_eq!(frame["ok"], false);
    assert!(
        frame["path"]
            .as_str()
            .is_some_and(|path| path.ends_with(".owner.lock")),
        "{frame}"
    );
    let inode_after = std::fs::metadata(&lock)
        .expect("persistent lock file")
        .ino();
    assert_eq!(
        inode_after, inode_before,
        "the refusal must not replace the persistent lock file"
    );

    // Kill the real gateway. Its death closes the server's stdin pipe, and the kernel must report
    // the registered server exit.
    child.start_kill().expect("kill gateway child");
    child.wait_bounded().await.expect("reap gateway child");
    drop(child_stdin);
    assert!(
        await_exit(kq).await,
        "server {server_pid} must exit when its gateway dies"
    );

    // A fresh server on the same directory, with stdin held open, proves the lock was released.
    let mut fresh_command = server_command(&server);
    fresh_command.arg("--exit-on-stdin-eof");
    server_env(&mut fresh_command, &entries);
    fresh_command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut fresh = fresh_command.spawn_locked().expect("spawn fresh server");
    let mut fresh_stdin = fresh.stdin.take().expect("fresh server stdin");
    let fresh_stdout = fresh.stdout.take().expect("fresh server stdout");
    let mut fresh_lines = BufReader::new(fresh_stdout.take(STREAM_LIMIT));
    let ready_frame: serde_json::Value = async {
        let line = bounded_line(&mut fresh_lines)
            .await
            .expect("fresh server ready frame before EOF");
        serde_json::from_slice(&line).expect("typed fresh server frame")
    }
    .await;
    assert_eq!(ready_frame["kind"], "ready", "{ready_frame}");

    // Closing stdin is the drain request: the server exits cleanly.
    fresh_stdin
        .shutdown()
        .await
        .expect("close fresh server stdin");
    drop(fresh_stdin);
    let status = fresh.wait_bounded().await.expect("reap fresh server");
    assert!(
        status.success(),
        "fresh server must drain to exit 0: {status:?}"
    );
}
