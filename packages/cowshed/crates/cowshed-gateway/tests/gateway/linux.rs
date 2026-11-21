use super::*;

use cowshed_os::test_backstop::AsyncUnixStreamBackstop as _;
use std::{path::PathBuf, sync::LazyLock};
use tokio::net::UnixStream;
// A static is never dropped, so this root outlives the process: one per Linux test process.
static LINUX_SOCKET_ROOT: LazyLock<FixtureDir> =
    LazyLock::new(|| secure_fixture_dir(&format!("cowshed-gateway-data-{}", std::process::id())));

pub(super) fn socket_root() -> PathBuf {
    LINUX_SOCKET_ROOT.path().to_path_buf()
}
#[tokio::test]
async fn unix_socket_churn_unlinks_every_session() {
    let socket = LINUX_SOCKET_ROOT.path().join("workspace.sock");
    let _ = std::fs::remove_file(&socket);
    let (config, _cache) = test_config();
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
    for revision in 1..=32 {
        let (session, _token, _) = session(
            "churn",
            "owner/repo-churn",
            WorkspaceEndpoint::Unix(socket.clone()),
            revision as u8,
            revision,
            WorkspacePolicy::default(),
        );
        gateway
            .handle()
            .install(session)
            .await
            .expect("install Unix session");
        let stream = UnixStream::connect_bounded(&socket)
            .await
            .expect("connect Unix socket");
        drop(stream);
        gateway
            .handle()
            .remove("churn", revision)
            .await
            .expect("remove Unix session");
        assert!(
            !socket.exists(),
            "socket remained after revision {revision}"
        );
    }
    gateway.drain().await.expect("drain gateway");
    assert!(LINUX_SOCKET_ROOT.path().exists());
}

#[tokio::test]
async fn linux_data_socket_root_is_enforced_and_serves_requests() {
    let socket = LINUX_SOCKET_ROOT.path().join("linux-request.sock");
    let control = LINUX_SOCKET_ROOT.path().join("control.sock");
    let regular = LINUX_SOCKET_ROOT.path().join("regular.sock");
    let outside = std::env::temp_dir().join("outside.sock");
    for path in [&socket, &control, &regular, &outside] {
        let _ = std::fs::remove_file(path);
    }
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let (mut config, _cache) = test_config();
    config.control_socket = Some(control.clone());
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

    let policy = WorkspacePolicy {
        grants: vec![grant("linux-request.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let (installed, token, _) = session(
        "linux-request",
        "owner/repo-linux-request",
        WorkspaceEndpoint::Unix(socket.clone()),
        30,
        1,
        policy.clone(),
    );
    gateway.handle().install(installed).await.expect("install");
    let mut client = UnixStream::connect_bounded(&socket)
        .await
        .expect("connect workspace data socket");
    client
        .write_all(
            absolute_request("linux-request.test", upstream_port, &token, "/allowed").as_bytes(),
        )
        .await
        .expect("write proxy request");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("read Linux proxy response");
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    captured.recv().await.expect("captured Linux request");

    let outside_session = session(
        "outside",
        "owner/repo-outside",
        WorkspaceEndpoint::Unix(outside),
        31,
        1,
        policy.clone(),
    )
    .0;
    assert!(matches!(
        gateway.handle().install(outside_session).await,
        Err(GatewayError::Config(
            cowshed_gateway::ConfigError::EndpointOutsideDataSocketRoot
        ))
    ));

    std::fs::write(&regular, b"preserve").expect("write unrelated file");
    let regular_session = session(
        "regular",
        "owner/repo-regular",
        WorkspaceEndpoint::Unix(regular.clone()),
        32,
        1,
        policy.clone(),
    )
    .0;
    assert!(matches!(
        gateway.handle().install(regular_session).await,
        Err(GatewayError::Io(_))
    ));
    assert_eq!(
        std::fs::read(&regular).expect("read unrelated file"),
        b"preserve"
    );

    let control_session = session(
        "control",
        "owner/repo-control",
        WorkspaceEndpoint::Unix(control.clone()),
        33,
        1,
        policy,
    )
    .0;
    assert!(matches!(
        gateway.handle().install(control_session).await,
        Err(GatewayError::Config(
            cowshed_gateway::ConfigError::EndpointOutsideDataSocketRoot
        ))
    ));
    assert!(control.exists(), "control socket was unlinked");

    gateway.drain().await.expect("drain gateway");
    let _ = std::fs::remove_file(regular);
}

/// One store's gateway, as the daemon configures it: the data socket root is the one
/// `data_socket_root` names for the store, absent until the gateway starts.
struct StoreGateway {
    gateway: Gateway,
    store: FixtureDir,
    _cache: FixtureDir,
}

impl StoreGateway {
    async fn start(name: &str) -> Self {
        let store = secure_fixture_dir(&format!("cg-{name}-{}", std::process::id()));
        let (config, cache) = test_config();
        let config = GatewayConfig {
            data_socket_root: cowshed_gateway::data_socket_root(store.path()),
            ..config
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
        Self {
            gateway,
            store,
            _cache: cache,
        }
    }

    /// The socket of the workspace incarnation `incarnation`, where a job's connector dials it.
    fn socket(&self, incarnation: &str) -> PathBuf {
        let WorkspaceEndpoint::Unix(path) =
            WorkspaceEndpoint::linux_data_socket(self.store.path(), incarnation)
        else {
            unreachable!("a Linux data socket is a Unix endpoint")
        };
        path
    }

    /// Install `workspace_id` at its incarnation's socket; its token.
    async fn install(
        &self,
        workspace_id: &str,
        incarnation: &str,
        token_byte: u8,
        policy: WorkspacePolicy,
    ) -> String {
        let (installed, token, _) = session(
            workspace_id,
            &format!("owner/repo-{workspace_id}"),
            WorkspaceEndpoint::linux_data_socket(self.store.path(), incarnation),
            token_byte,
            1,
            policy,
        );
        self.gateway
            .handle()
            .install(installed)
            .await
            .expect("install the workspace's session");
        token
    }
}

/// What a connector relays: the request's bytes written to the workspace's socket, and every
/// byte the gateway answers until it closes.
async fn socket_request(socket: &std::path::Path, request: String) -> String {
    let mut client = UnixStream::connect_bounded(socket)
        .await
        .expect("connect the workspace data socket");
    client
        .write_all(request.as_bytes())
        .await
        .expect("write the proxy request");
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("read the proxy response");
    String::from_utf8(response).expect("an HTTP response head is UTF-8")
}

fn mode(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::symlink_metadata(path)
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
        .permissions()
        .mode()
        & 0o7777
}

const RAVEN: &str = "00112233445566778899aabbccddeeff";
const CROW: &str = "ffeeddccbbaa99887766554433221100";

/// The whole Linux data plane on the gateway's side: a start makes the store's `run/gateway`
/// for its user alone, a session binds `<incarnation>.sock` there at 0600, a request written to
/// that socket with the session's token reaches the granted upstream, and removing the session
/// unlinks the socket.
#[tokio::test]
async fn a_request_through_a_workspace_socket_reaches_its_granted_upstream() {
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let fixture = StoreGateway::start("serve").await;
    let root = fixture.store.path().join("run/gateway");
    assert_eq!(mode(&root), 0o700, "the gateway made its data socket root");

    let token = fixture
        .install(
            "raven",
            RAVEN,
            41,
            WorkspacePolicy {
                grants: vec![grant("socket.test", upstream_port)],
                mirrors: Vec::new(),
            },
        )
        .await;
    let socket = fixture.socket(RAVEN);
    assert_eq!(socket, root.join(format!("{RAVEN}.sock")));
    assert_eq!(mode(&socket), 0o600);

    let response = socket_request(
        &socket,
        absolute_request("socket.test", upstream_port, &token, "/allowed"),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("ok"), "{response}");
    let forwarded = captured.recv().await.expect("the upstream saw the request");
    assert!(
        forwarded.starts_with("GET /allowed HTTP/1.1"),
        "{forwarded}"
    );
    assert!(
        !forwarded.contains(&token)
            && !forwarded
                .to_ascii_lowercase()
                .contains("proxy-authorization"),
        "the session token stays at the gateway: {forwarded}"
    );

    fixture
        .gateway
        .handle()
        .remove("raven", 1)
        .await
        .expect("remove the session");
    assert!(!socket.exists(), "a removed session's socket is unlinked");
    fixture.gateway.drain().await.expect("drain gateway");
}

/// The socket selects the workspace and its session's token is still required there: a request
/// with no token, a malformed one, or a sibling workspace's own valid token is answered 407 and
/// reaches no upstream, while the sibling's token works at the sibling's socket.
#[tokio::test]
async fn a_request_without_the_sessions_token_is_refused_at_its_socket() {
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let fixture = StoreGateway::start("token").await;
    let policy = WorkspacePolicy {
        grants: vec![grant("socket.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let _raven = fixture.install("raven", RAVEN, 42, policy.clone()).await;
    let crow = fixture.install("crow", CROW, 43, policy).await;
    let raven_socket = fixture.socket(RAVEN);

    let anonymous = format!(
        "GET http://socket.test:{upstream_port}/allowed HTTP/1.1\r\nHost: socket.test:{upstream_port}\r\nConnection: close\r\n\r\n"
    );
    for (case, request) in [
        ("no token", anonymous),
        (
            "a malformed token",
            absolute_request("socket.test", upstream_port, "not-base64!", "/allowed"),
        ),
        (
            "a sibling's token",
            absolute_request("socket.test", upstream_port, &crow, "/allowed"),
        ),
    ] {
        let response = socket_request(&raven_socket, request).await;
        assert!(response.starts_with("HTTP/1.1 407"), "{case}: {response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains("proxy-authenticate: basic realm=\"cowshed\""),
            "{case}: {response}"
        );
    }
    assert!(
        captured.try_recv().is_err(),
        "a refused request reaches no upstream"
    );

    let own = socket_request(
        &fixture.socket(CROW),
        absolute_request("socket.test", upstream_port, &crow, "/allowed"),
    )
    .await;
    assert!(own.starts_with("HTTP/1.1 200"), "{own}");
    captured.recv().await.expect("the sibling's own request");
    fixture.gateway.drain().await.expect("drain gateway");
}
