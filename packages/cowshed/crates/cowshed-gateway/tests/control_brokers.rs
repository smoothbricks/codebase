#![cfg(target_os = "macos")]

use std::{net::SocketAddr, os::unix::fs::PermissionsExt as _, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use cowshed_gateway::{
    AuditError, AuditEvent, AuditSink, AuthorizedTarget, ConnectError, ControlFailureCode,
    ControlTcpConfig, CredentialError, CredentialProvider, CredentialQuery, CredentialRecord,
    Gateway, GatewayConfig, GatewayControlClient, MirrorCacheConfig, UpstreamConnection,
    UpstreamConnector, UpstreamHealth,
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpStream, UnixStream},
};
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

fn fixture_root() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let root = PathBuf::from(format!("/tmp/csctl-{}", &id[..8]));
    std::fs::create_dir(&root).expect("create fixture root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure fixture root");
    root
}

fn write_credential(path: &std::path::Path, byte: u8, controller_domain: bool) {
    let encoded = URL_SAFE_NO_PAD.encode([byte; 32]);
    let value = if controller_domain {
        format!("cctl1_{encoded}\n")
    } else {
        format!("{encoded}\n")
    };
    std::fs::write(path, value).expect("write credential");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .expect("secure credential");
}

async fn raw_tcp(request: serde_json::Value) -> serde_json::Value {
    let mut stream = TcpStream::connect("127.0.0.1:7644")
        .await
        .expect("connect control TCP");
    let mut bytes = serde_json::to_vec(&request).expect("encode request");
    bytes.push(b'\n');
    stream.write_all(&bytes).await.expect("write request");
    stream.shutdown().await.expect("shutdown write");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    serde_json::from_slice(&response).expect("decode response")
}

#[tokio::test]
async fn tcp_credential_is_distinct_unix_is_local_and_shutdown_closes_both() {
    let root = fixture_root();
    let cache = root.join("cache");
    std::fs::create_dir(&cache).expect("cache");
    std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).expect("secure cache");
    let socket = root.join("gateway.sock");
    let credential = root.join("controller.credential");
    let wrong_credential = root.join("wrong.credential");
    let data_token_file = root.join("workspace.token");
    write_credential(&credential, 7, true);
    write_credential(&wrong_credential, 8, true);
    write_credential(&data_token_file, 7, false);

    let config = GatewayConfig {
        control_socket: Some(socket.clone()),
        control_tcp: Some(ControlTcpConfig::new(credential.clone())),
        mirror_cache: MirrorCacheConfig::new(cache),
        ..GatewayConfig::default()
    };
    let gateway = Gateway::start(
        config,
        Arc::new(NoCredentials),
        Arc::new(NoConnector),
        Arc::new(DiscardAudit),
    )
    .await
    .expect("start gateway");

    let unix = GatewayControlClient::new(socket.clone()).expect("Unix client");
    assert!(
        unix.status()
            .await
            .expect("Unix status")
            .sessions
            .is_empty()
    );
    let tcp = GatewayControlClient::new_tcp(
        "127.0.0.1:7644".parse::<SocketAddr>().expect("address"),
        credential,
    )
    .expect("TCP client");
    assert!(tcp.status().await.expect("TCP status").sessions.is_empty());

    let wrong =
        GatewayControlClient::new_tcp("127.0.0.1:7644".parse().expect("address"), wrong_credential)
            .expect("wrong TCP client");
    assert!(matches!(
        wrong.status().await,
        Err(cowshed_gateway::ControlError::Rejected {
            code: ControlFailureCode::Unauthorized,
            ..
        })
    ));
    let confused =
        GatewayControlClient::new_tcp("127.0.0.1:7644".parse().expect("address"), data_token_file)
            .expect("token-confused client");
    assert!(matches!(
        confused.status().await,
        Err(cowshed_gateway::ControlError::InvalidControllerCredential)
    ));

    let data_token = URL_SAFE_NO_PAD.encode([7_u8; 32]);
    let response = raw_tcp(serde_json::json!({
        "controllerCredential": data_token,
        "request": {"op": "status"}
    }))
    .await;
    assert_eq!(response["ok"], false);
    assert_eq!(response["code"], "unauthorized");

    let mut raw_unix = UnixStream::connect(&socket).await.expect("raw Unix");
    raw_unix
        .write_all(b"{\"op\":\"status\",\"unknown\":true}\n")
        .await
        .expect("write unknown field");
    raw_unix.shutdown().await.expect("shutdown raw Unix");
    let mut response = Vec::new();
    raw_unix
        .read_to_end(&mut response)
        .await
        .expect("read raw Unix");
    let response: serde_json::Value = serde_json::from_slice(&response).expect("response");
    assert_eq!(response["code"], "invalid-request");
    assert!(
        unix.status()
            .await
            .expect("status remains available")
            .sessions
            .is_empty()
    );

    gateway.drain().await.expect("drain");
    assert!(!socket.exists());
    assert!(TcpStream::connect("127.0.0.1:7644").await.is_err());
    let _ = std::fs::remove_dir_all(root);
}
