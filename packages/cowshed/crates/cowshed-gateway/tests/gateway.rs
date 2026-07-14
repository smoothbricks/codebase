#[cfg(target_os = "linux")]
#[path = "gateway/linux.rs"]
mod linux;
#[cfg(target_os = "macos")]
#[path = "gateway/macos.rs"]
mod macos;

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use cowshed_gateway::{
    ArrowAuditConfig, ArrowAuditSink, AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus,
    AuthorizedTarget, CanonicalTarget, ConnectError, CredentialError, CredentialProvider,
    CredentialQuery, CredentialRecord, EgressGrant, Gateway, GatewayConfig, GatewayError,
    GatewayTimeouts, HostPattern, MirrorCacheConfig, MirrorCacheStatus, NegotiatedTransport,
    UpstreamConnection, UpstreamConnector, UpstreamHealth, UpstreamPurpose, WorkspaceCa,
    WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession, WorkspaceToken,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::pki_types::CertificateDer;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc},
    task::JoinHandle,
    time::timeout,
};

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Clone)]
struct LocalConnector {
    health: UpstreamHealth,
    observed: Option<mpsc::Sender<AuthorizedTarget>>,
}

#[async_trait]
impl UpstreamConnector for LocalConnector {
    async fn health(&self, _target: &CanonicalTarget) -> UpstreamHealth {
        self.health
    }

    async fn connect(&self, target: &AuthorizedTarget) -> Result<UpstreamConnection, ConnectError> {
        if let Some(observed) = &self.observed {
            let _ = observed.send(target.clone()).await;
        }
        if self.health == UpstreamHealth::Offline {
            return Err(ConnectError::Io(io::Error::other(
                "offline connector must not be called",
            )));
        }
        let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, target.target.port))
            .await
            .map_err(ConnectError::Io)?;
        let transport = match target.purpose {
            UpstreamPurpose::OpaqueTcp => NegotiatedTransport::Raw,
            UpstreamPurpose::PlainHttp | UpstreamPurpose::TlsHttp => NegotiatedTransport::Http1,
        };
        Ok(UpstreamConnection {
            io: Box::new(stream),
            transport,
        })
    }
}

struct CaFixture {
    material: WorkspaceCa,
    certificate: CertificateDer<'static>,
}

fn ca_fixture() -> CaFixture {
    let key = KeyPair::generate().expect("fixture CA key");
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let certificate = params.self_signed(&key).expect("fixture CA certificate");
    CaFixture {
        material: WorkspaceCa::new(certificate.pem(), key.serialize_pem())
            .expect("valid fixture CA"),
        certificate: CertificateDer::from(certificate.der().to_vec()),
    }
}

fn session(
    workspace_id: &str,
    repo_id: &str,
    endpoint: WorkspaceEndpoint,
    token_byte: u8,
    revision: u64,
    policy: WorkspacePolicy,
) -> (WorkspaceSession, String, CertificateDer<'static>) {
    let token = WorkspaceToken::from_bytes([token_byte; 32]);
    let encoded = token.encode();
    let ca = ca_fixture();
    (
        WorkspaceSession {
            workspace_id: workspace_id.to_owned(),
            repo_id: repo_id.to_owned(),
            revision,
            endpoint,
            token,
            ca: ca.material,
            policy,
        },
        encoded,
        ca.certificate,
    )
}

fn grant(host: &str, port: u16) -> EgressGrant {
    EgressGrant::intercept(host, port)
        .expect("valid fixture grant")
        .allow_path("/allowed")
        .expect("valid path")
}

fn test_config() -> GatewayConfig {
    static NEXT_CACHE: AtomicUsize = AtomicUsize::new(0);
    let cache_root = std::env::temp_dir().join(format!(
        "cowshed-gateway-cache-{}-{}",
        std::process::id(),
        NEXT_CACHE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&cache_root);
    std::fs::create_dir(&cache_root).expect("create pre-existing mirror cache root");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&cache_root, std::fs::Permissions::from_mode(0o700))
            .expect("secure mirror cache root");
    }
    let config = GatewayConfig {
        timeouts: GatewayTimeouts {
            request_headers: Duration::from_secs(2),
            connect: Duration::from_secs(1),
            tls_handshake: Duration::from_secs(2),
            response_headers: Duration::from_secs(2),
            body_idle: Duration::from_secs(2),
            request_total: Duration::from_secs(5),
            tunnel_total: Duration::from_secs(5),
            leaf_lifetime: Duration::from_secs(60 * 60),
        },
        mirror_cache: MirrorCacheConfig::new(cache_root),
        ..GatewayConfig::default()
    };
    #[cfg(target_os = "linux")]
    let config = GatewayConfig {
        data_socket_root: Some(linux::socket_root()),
        ..config
    };
    config
}

async fn gateway(
    config: GatewayConfig,
    credentials: Arc<dyn CredentialProvider>,
    connector: Arc<dyn UpstreamConnector>,
    audit: Arc<dyn AuditSink>,
) -> Gateway {
    Gateway::start(config, credentials, connector, audit)
        .await
        .expect("start gateway")
}

async fn http_fixture(
    accepts: usize,
    gate: Option<Arc<Notify>>,
) -> (u16, mpsc::Receiver<String>, JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind upstream fixture");
    let port = listener.local_addr().expect("fixture address").port();
    let (captured, receiver) = mpsc::channel(accepts.max(1));
    let task = tokio::spawn(async move {
        for _ in 0..accepts {
            let (mut stream, _) = listener.accept().await.expect("accept upstream request");
            let captured = captured.clone();
            let gate = gate.clone();
            tokio::spawn(async move {
                let request = read_headers(&mut stream).await;
                captured.send(request).await.expect("capture request");
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nSet-Cookie: upstream=secret\r\nConnection: close\r\n\r\nok",
                    )
                    .await
                    .expect("write fixture response");
            });
        }
    });
    (port, receiver, task)
}

async fn read_headers(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut byte = [0u8; 1];
    while bytes.len() < 64 * 1024 {
        let read = stream.read(&mut byte).await.expect("read fixture headers");
        if read == 0 {
            break;
        }
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).expect("HTTP fixture is UTF-8")
}

fn absolute_request(host: &str, port: u16, token: &str, path: &str) -> String {
    format!(
        "GET http://{host}:{port}{path} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    )
}

#[tokio::test]
async fn arrow_audit_sink_publishes_durable_single_batch_segments() {
    let root = std::env::temp_dir().join(format!("cowshed-gateway-audit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).expect("provision audit root");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("secure audit root");
    }
    let sink =
        ArrowAuditSink::start(ArrowAuditConfig::new(root.clone()).expect("audit configuration"))
            .expect("start Arrow sink");
    sink.record(AuditEvent {
        sequence: 1,
        timestamp_unix_ms: 1_700_000_000_000,
        workspace_id: "audit-ws".to_owned(),
        repo_id: "owner/repo-audit".to_owned(),
        revision: 3,
        endpoint: "127.0.0.1:40960".to_owned(),
        kind: AuditKind::Npm,
        host: Some("example.test:443".to_owned()),
        method: Some("GET".to_owned()),
        path: Some("/v1".to_owned()),
        status: AuditStatus::Completed,
        http_status: Some(200),
        bytes: 12,
        trace_id: Some("00000000000000000000000000000001".to_owned()),
        span_id: 1,
        upstream_span_id: Some(2),
        parent_span_id: None,
        tracestate: None,
        grant_hint: None,
        classification: None,
        mirror_cache_status: Some(MirrorCacheStatus::Filled),
    })
    .await
    .expect("record Arrow audit");
    let invalid = sink
        .record(AuditEvent {
            sequence: 2,
            timestamp_unix_ms: 1_700_000_000_001,
            workspace_id: "audit-ws".to_owned(),
            repo_id: "owner/repo-audit".to_owned(),
            revision: 3,
            endpoint: "127.0.0.1:40960".to_owned(),
            kind: AuditKind::Http,
            host: Some("example.test:443".to_owned()),
            method: Some("GET".to_owned()),
            path: Some("/v1".to_owned()),
            status: AuditStatus::Completed,
            http_status: Some(200),
            bytes: 12,
            trace_id: None,
            span_id: 3,
            upstream_span_id: Some(4),
            parent_span_id: None,
            tracestate: None,
            grant_hint: None,
            classification: None,
            mirror_cache_status: Some(MirrorCacheStatus::Hit),
        })
        .await
        .expect_err("non-mirror cache status must be rejected");
    assert!(invalid.0.contains("non-mirror"));
    sink.flush()
        .await
        .expect_err("invalid event hard-stops the audit writer");
    let partition = std::fs::read_dir(&root)
        .expect("read telemetry root")
        .next()
        .expect("date partition")
        .expect("partition entry")
        .path();
    let segment = std::fs::read_dir(&partition)
        .expect("read partition")
        .next()
        .expect("audit segment")
        .expect("segment entry")
        .path();
    assert_eq!(
        segment.extension().and_then(|value| value.to_str()),
        Some("arrow")
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(
            std::fs::metadata(&segment)
                .expect("segment metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    let mut reader = arrow_ipc::reader::StreamReader::try_new(
        std::fs::File::open(&segment).expect("open Arrow segment"),
        None,
    )
    .expect("read Arrow stream");
    let batch = reader.next().expect("one batch").expect("valid batch");
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.schema().field(0).name(), "timestamp");
    assert_eq!(batch.schema().field(22).name(), "mirror_cache_status");
    let cache_status = batch
        .column(22)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("mirror cache status string column");
    assert_eq!(cache_status.value(0), "filled");
    assert!(reader.next().is_none());
    drop(sink);
    std::fs::remove_dir_all(root).expect("remove audit fixture");
}

#[test]
fn validation_rejects_ambiguous_policy_and_platform_endpoints() {
    assert!(WorkspaceToken::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").is_err());
    assert!(HostPattern::parse("*.*.example.com").is_err());
    assert!(cowshed_gateway::normalize_path("/a/%2f/b").is_err());
    assert!(cowshed_gateway::normalize_path("/a/../b").is_err());
    assert!(
        WorkspaceEndpoint::Tcp(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
            cowshed_gateway::MACOS_PORT_MIN,
        ))
        .validate()
        .is_err()
    );
    #[cfg(target_os = "macos")]
    {
        assert!(
            WorkspaceEndpoint::Unix(std::env::temp_dir().join("gateway.sock"))
                .validate_for_current_platform()
                .is_err()
        );
        assert!(
            WorkspaceEndpoint::Tcp(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                cowshed_gateway::MACOS_PORT_MIN + 1,
            ))
            .validate_for_current_platform()
            .is_err()
        );
    }
    #[cfg(target_os = "linux")]
    assert!(
        WorkspaceEndpoint::Tcp(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            cowshed_gateway::MACOS_PORT_MIN,
        ))
        .validate_for_current_platform()
        .is_err()
    );
}
