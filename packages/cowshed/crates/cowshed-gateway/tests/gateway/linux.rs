use super::*;

use std::{path::PathBuf, sync::LazyLock};
use tokio::net::UnixStream;
static LINUX_SOCKET_ROOT: LazyLock<PathBuf> = LazyLock::new(|| {
    use std::os::unix::fs::PermissionsExt as _;

    let root = std::env::temp_dir().join(format!("cowshed-gateway-data-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).expect("create Linux data socket root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure Linux data socket root");
    root
});

pub(super) fn socket_root() -> PathBuf {
    LINUX_SOCKET_ROOT.clone()
}
#[tokio::test]
async fn unix_socket_churn_unlinks_every_session() {
    let socket = LINUX_SOCKET_ROOT.join("workspace.sock");
    let _ = std::fs::remove_file(&socket);
    let gateway = gateway(
        test_config(),
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
        let stream = UnixStream::connect(&socket)
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
    assert!(LINUX_SOCKET_ROOT.exists());
}

#[tokio::test]
async fn linux_data_socket_root_is_enforced_and_serves_requests() {
    let socket = LINUX_SOCKET_ROOT.join("linux-request.sock");
    let control = LINUX_SOCKET_ROOT.join("control.sock");
    let regular = LINUX_SOCKET_ROOT.join("regular.sock");
    let outside = std::env::temp_dir().join("outside.sock");
    for path in [&socket, &control, &regular, &outside] {
        let _ = std::fs::remove_file(path);
    }
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let mut config = test_config();
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
    let mut client = UnixStream::connect(&socket)
        .await
        .expect("connect workspace data socket");
    client
        .write_all(
            absolute_request("linux-request.test", upstream_port, &token, "/allowed").as_bytes(),
        )
        .await
        .expect("write proxy request");
    let mut response = Vec::new();
    timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .expect("Linux proxy timeout")
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
