//! A Nx read whose backend dies mid-body. The response head already said `200`, so the gateway
//! cannot change it; the audit must name the cut as a failed transfer and never as a completed hit.
//! One real `Gateway` and one real server, the package's bundle run by `bun` from `PATH`, which
//! this test kills with SIGKILL. It proves that stream cut and nothing else: no restore,
//! publication, or cutover claim.
use super::*;

use super::nx_process::{is_server_args, server_path, server_script};
use cowshed_gateway::{
    AuditEvent, AuditStatus, GatewayTimeouts, NxCacheServiceConfig, NxPublication,
};
use cowshed_os::fork_lock::Run as _;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::DirBuilderExt as _;
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::time::timeout;

const REPO: &str = "owner/nx-stream-kill";
const SOURCE: &str = "nx-stream-source";
const HASH: &str = "42";
const ARTIFACT_BYTES: usize = 32 * 1024 * 1024;
const CHUNK_BYTES: usize = 64 * 1024;
const PREVIEW_BYTES: usize = 64 * 1024;
/// Every network wait in this test is bounded by this, so a stuck gateway fails rather than hangs.
const STEP_LIMIT: Duration = Duration::from_secs(60);

/// One request on a fresh connection, read to the connection's end.
async fn exchange(endpoint: SocketAddr, request: String) -> String {
    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let mut response = Vec::new();
    timeout(STEP_LIMIT, stream.read_to_end(&mut response))
        .await
        .expect("gateway response timeout")
        .expect("read gateway response");
    String::from_utf8(response).expect("HTTP response is UTF-8")
}

/// Streams the artifact as one standard PUT, writing from one 64 KiB buffer reused for every chunk.
async fn put_artifact(endpoint: SocketAddr, token: &str) -> String {
    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    let head = format!(
        "PUT /nx/v1/cache/{HASH} HTTP/1.1\r\nHost: {endpoint}\r\nAuthorization: Bearer {token}\r\nContent-Length: {ARTIFACT_BYTES}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(head.as_bytes())
        .await
        .expect("write artifact head");
    let chunk = vec![0x5a_u8; CHUNK_BYTES];
    for _ in 0..(ARTIFACT_BYTES / CHUNK_BYTES) {
        stream
            .write_all(&chunk)
            .await
            .expect("write artifact chunk");
    }
    let mut response = Vec::new();
    timeout(STEP_LIMIT, stream.read_to_end(&mut response))
        .await
        .expect("artifact response timeout")
        .expect("read artifact response");
    String::from_utf8(response).expect("HTTP response is UTF-8")
}

/// Reads whatever the stream has next, at most the buffer's length. Zero bytes is a failure here,
/// because every caller still expects more of the response.
async fn read_some(stream: &mut TcpStream, buffer: &mut [u8]) -> usize {
    let read = timeout(STEP_LIMIT, stream.read(buffer))
        .await
        .expect("gateway read timeout")
        .expect("read gateway bytes");
    assert!(
        read > 0,
        "the gateway ended the stream before the expected bytes"
    );
    read
}

/// The one server this process's gateway started, read from a single `ps` snapshot.
fn own_server_child(gateway_pid: u32, script: &std::path::Path) -> libc::pid_t {
    let listing = std::process::Command::new("/bin/ps")
        .args(["-axo", "pid=,ppid=,args="])
        .output_locked()
        .expect("ps snapshot");
    assert!(
        listing.status.success(),
        "ps snapshot failed: {:?}",
        listing.status
    );
    let servers: Vec<libc::pid_t> = String::from_utf8_lossy(&listing.stdout)
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() < 3 {
                return None;
            }
            let pid: libc::pid_t = fields[0].parse().ok()?;
            let ppid: u32 = fields[1].parse().ok()?;
            (ppid == gateway_pid && is_server_args(&fields[2..].join(" "), script)).then_some(pid)
        })
        .collect();
    assert_eq!(
        servers.len(),
        1,
        "gateway {gateway_pid} must own exactly one nx-cache-server: {servers:?}"
    );
    servers[0]
}

/// A Nx body the backend cuts short after its `200` head is audited as a failed transfer.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_nx_backend_killed_mid_body_is_a_failed_transfer() {
    let root = secure_fixture_dir(&format!("cowshed-nx-stream-{}", std::process::id()));
    let directory = root.path().join("nx");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .expect("create the backend directory under the parent root");
    let (mut config, _cache) = test_config();
    config.timeouts = GatewayTimeouts {
        request_total: STEP_LIMIT,
        response_headers: STEP_LIMIT,
        tunnel_total: STEP_LIMIT,
        ..config.timeouts
    };
    let script = server_script();
    config.nx_cache = Some(NxCacheServiceConfig {
        script: script.clone(),
        path: server_path(),
        directory,
        max_bytes: 64 * 1024 * 1024,
        cap_bytes: 96 * 1024 * 1024,
    });
    let audit = Arc::new(NxRecordingAudit::default());
    let gateway = gateway(
        config,
        Arc::new(NoCredentials),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        audit.clone(),
    )
    .await;
    let handle = gateway.handle();
    let (source, source_token, _) = session(
        SOURCE,
        REPO,
        block_endpoint(free_endpoint()),
        1,
        1,
        WorkspacePolicy::default(),
    );
    let source_endpoint = install_in_free_block(&handle, source).await;

    // The first read starts the real server, which has no entry yet.
    let first = exchange(
        source_endpoint,
        nx_request(source_endpoint, "GET", &source_token, HASH),
    )
    .await;
    assert!(first.starts_with("HTTP/1.1 404"), "{first}");

    // A synthetic publication capability for one hash, not a qualified land.
    let publication = WorkspaceToken::from_bytes([63; 32]);
    handle
        .begin_nx_publication(NxPublication {
            workspace_id: SOURCE.to_owned(),
            repo_id: REPO.to_owned(),
            expected_revision: 1,
            accepted_head: "a".repeat(40),
            hashes: BTreeSet::from([HASH.to_owned()]),
            token: publication.clone(),
        })
        .await
        .expect("synthetic publication capability");
    let stored = put_artifact(source_endpoint, &publication.encode()).await;
    assert!(stored.starts_with("HTTP/1.1 200"), "{stored}");

    // Read the head and a bounded preview of the body, then stop. The receive window is kept small
    // so the body backs up at the gateway rather than sitting in kernel buffers.
    let mut reader = TcpStream::connect(source_endpoint)
        .await
        .expect("connect reader");
    let window: libc::c_int = 4096;
    // SAFETY: setsockopt reads one c_int of the length given from the pointer, which lives across
    // the call; the descriptor is this stream's own.
    let set = unsafe {
        libc::setsockopt(
            reader.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            std::ptr::from_ref(&window).cast(),
            libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
                .expect("c_int size fits socklen_t"),
        )
    };
    assert_eq!(
        set,
        0,
        "set the receive window: {}",
        std::io::Error::last_os_error()
    );
    reader
        .write_all(nx_request(source_endpoint, "GET", &source_token, HASH).as_bytes())
        .await
        .expect("write read request");
    let mut buffer = vec![0_u8; CHUNK_BYTES];
    let mut received = Vec::new();
    let body_start = loop {
        if let Some(index) = received.windows(4).position(|pair| pair == b"\r\n\r\n") {
            break index + 4;
        }
        let read = read_some(&mut reader, &mut buffer).await;
        received.extend_from_slice(&buffer[..read]);
    };
    while received.len() - body_start < PREVIEW_BYTES {
        let read = read_some(&mut reader, &mut buffer).await;
        received.extend_from_slice(&buffer[..read]);
    }
    let head = String::from_utf8_lossy(&received[..body_start]).into_owned();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(
        received.len() - body_start < ARTIFACT_BYTES,
        "the preview must be a fraction of the artifact"
    );

    // Kill the backend, not the gateway: the server is the one child this gateway started.
    let server = own_server_child(std::process::id(), &script);
    // SAFETY: kill signals one pid, the server child this test has just identified; no other pid
    // is named.
    let killed = unsafe { libc::kill(server, libc::SIGKILL) };
    assert_eq!(
        killed,
        0,
        "SIGKILL the backend {server}: {}",
        std::io::Error::last_os_error()
    );

    // Read on until the stream ends or errors. The status is already 200, so the only truth the
    // client sees is the cut, and the audit must name that cut as the failure.
    loop {
        let read = timeout(STEP_LIMIT, reader.read(&mut buffer))
            .await
            .expect("the cut stream never ended");
        match read {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }

    gateway
        .drain()
        .await
        .expect("drain the gateway after its backend died");
    let events = audit.events.lock().expect("audit lock");
    let read_200: Vec<&AuditEvent> = events
        .iter()
        .filter(|event| {
            event.method.as_deref() == Some("GET")
                && event.path.as_deref() == Some("/nx/v1/cache/42")
                && event.http_status == Some(200)
        })
        .collect();
    assert_eq!(read_200.len(), 1, "{events:?}");
    let cut = read_200[0];
    assert!(matches!(cut.status, AuditStatus::Failed), "{cut:?}");
    assert_eq!(
        cut.classification.as_deref(),
        Some("nx-transfer-failed"),
        "{cut:?}"
    );
    assert!(
        cut.bytes < u64::try_from(ARTIFACT_BYTES).expect("artifact size fits u64"),
        "{cut:?}"
    );
    assert!(
        !events.iter().any(|event| {
            event.method.as_deref() == Some("GET")
                && event.path.as_deref() == Some("/nx/v1/cache/42")
                && event.http_status == Some(200)
                && matches!(event.status, AuditStatus::Completed)
        }),
        "a cut read is never a completed hit: {events:?}"
    );
    assert!(
        events.iter().any(|event| {
            event.method.as_deref() == Some("PUT")
                && event.http_status == Some(200)
                && matches!(event.status, AuditStatus::Completed)
        }),
        "the artifact the read cut was published whole: {events:?}"
    );
}
