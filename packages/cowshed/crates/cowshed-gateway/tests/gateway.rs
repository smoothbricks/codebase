use std::{
    collections::BTreeSet,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use cowshed_gateway::{
    ArrowAuditConfig, ArrowAuditSink, AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus,
    AuthorizedTarget, BoxIo, CanonicalTarget, ConnectError, ControlError, ControlFailureCode,
    CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery, CredentialRecord,
    EgressGrant, Gateway, GatewayConfig, GatewayControlClient, GatewayError, GatewayLimits,
    GatewayTimeouts, HostPattern, MirrorProtocol, MirrorRoute, UpstreamConnector, UpstreamHealth,
    UpstreamPurpose, WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession,
    WorkspaceToken,
};
use http::HeaderName;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
#[cfg(target_os = "linux")]
use tokio::net::UnixStream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::TlsConnector;
use zeroize::Zeroizing;
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
struct FailingCredentials;

#[async_trait]
impl CredentialProvider for FailingCredentials {
    async fn lookup(
        &self,
        _query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        Err(CredentialError::Unavailable("injected failure".to_owned()))
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

#[derive(Debug)]
struct FailingAudit;

#[async_trait]
impl AuditSink for FailingAudit {
    async fn record(&self, _event: AuditEvent) -> Result<(), AuditError> {
        Err(AuditError("injected writer failure".to_owned()))
    }

    async fn flush(&self) -> Result<(), AuditError> {
        Err(AuditError("injected writer failure".to_owned()))
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

    async fn connect(&self, target: &AuthorizedTarget) -> Result<BoxIo, ConnectError> {
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
        Ok(Box::new(stream))
    }
}

#[derive(Clone, Debug)]
struct CountingFailConnector {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl UpstreamConnector for CountingFailConnector {
    async fn health(&self, _target: &CanonicalTarget) -> UpstreamHealth {
        self.calls.fetch_add(1, Ordering::SeqCst);
        UpstreamHealth::Healthy
    }

    async fn connect(&self, _target: &AuthorizedTarget) -> Result<BoxIo, ConnectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ConnectError::Io(io::Error::other(
            "injected connect failure",
        )))
    }
}

struct FixedCredential {
    repo_id: String,
    origin: String,
    value: String,
}

#[async_trait]
impl CredentialProvider for FixedCredential {
    async fn lookup(
        &self,
        _query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        Ok(Some(CredentialRecord {
            repo_id: self.repo_id.clone(),
            protocol: CredentialProtocol::Generic,
            origin: self.origin.clone(),
            methods: BTreeSet::from(["GET".to_owned()]),
            path_prefixes: vec!["/allowed".to_owned()],
            header_name: HeaderName::from_static("authorization"),
            header_value: Zeroizing::new(self.value.clone()),
        }))
    }
}

struct ChannelAudit(mpsc::Sender<AuditEvent>);

#[async_trait]
impl AuditSink for ChannelAudit {
    async fn record(&self, event: AuditEvent) -> Result<(), AuditError> {
        self.0
            .send(event)
            .await
            .map_err(|_| AuditError("audit receiver closed".to_owned()))
    }

    async fn flush(&self) -> Result<(), AuditError> {
        Ok(())
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

fn free_endpoint() -> SocketAddr {
    static NEXT_PORT: AtomicU16 = AtomicU16::new(cowshed_gateway::MACOS_PORT_MIN);
    loop {
        let port = NEXT_PORT.fetch_add(cowshed_gateway::MACOS_PORT_BLOCK_SIZE, Ordering::Relaxed);
        assert!(
            port <= cowshed_gateway::MACOS_PORT_MAX,
            "no free macOS gateway port block"
        );
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        if std::net::TcpListener::bind(address).is_ok() {
            return address;
        }
    }
}

fn tls_client_hello(host: &str) -> Vec<u8> {
    let config = ClientConfig::builder()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    let server_name = ServerName::try_from(host.to_owned()).expect("valid fixture SNI");
    let mut connection =
        rustls::ClientConnection::new(Arc::new(config), server_name).expect("TLS client");
    let mut bytes = Vec::new();
    connection
        .write_tls(&mut bytes)
        .expect("serialize ClientHello");
    bytes
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
    GatewayConfig {
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
        ..GatewayConfig::default()
    }
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

async fn proxy_request(endpoint: SocketAddr, request: String) -> String {
    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write proxy request");
    let mut response = Vec::new();
    timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
        .await
        .expect("proxy response timeout")
        .expect("read proxy response");
    String::from_utf8(response).expect("HTTP response is UTF-8")
}

async fn await_reclaimed(gateway: &Gateway) {
    timeout(Duration::from_secs(1), async {
        loop {
            let status = gateway.handle().status().await.expect("gateway status");
            if status.active == 0 && status.queued == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gateway capacity was not reclaimed");
}

async fn opaque_payload(
    endpoint: SocketAddr,
    token: &str,
    authority: &str,
    port: u16,
    payload: &[u8],
) {
    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    let connect = format!(
        "CONNECT {authority}:{port} HTTP/1.1\r\nHost: {authority}:{port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
    );
    stream
        .write_all(connect.as_bytes())
        .await
        .expect("write CONNECT");
    assert!(
        read_response_head(&mut stream)
            .await
            .starts_with("HTTP/1.1 200")
    );
    stream
        .write_all(payload)
        .await
        .expect("write tunnel payload");
    stream.shutdown().await.expect("shutdown tunnel writer");
    let mut discarded = Vec::new();
    timeout(Duration::from_secs(1), stream.read_to_end(&mut discarded))
        .await
        .expect("opaque denial timeout")
        .expect("read opaque denial");
}

fn absolute_request(host: &str, port: u16, token: &str, path: &str) -> String {
    format!(
        "GET http://{host}:{port}{path} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    )
}

async fn read_response_head(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut byte = [0u8; 1];
    while bytes.len() < 64 * 1024 {
        stream
            .read_exact(&mut byte)
            .await
            .expect("read response head");
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).expect("response head UTF-8")
}

#[tokio::test]
async fn allow_deny_malformed_token_and_audit_fields() {
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let endpoint = free_endpoint();
    let (audit_tx, mut audit_rx) = mpsc::channel(8);
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(ChannelAudit(audit_tx)),
    )
    .await;
    let policy = WorkspacePolicy {
        grants: vec![grant("allowed.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let (session, token, _) = session(
        "raven",
        "repo-one",
        WorkspaceEndpoint::Tcp(endpoint),
        7,
        1,
        policy,
    );
    gateway
        .handle()
        .install(session)
        .await
        .expect("install session");

    let malformed = absolute_request(
        "allowed.test",
        upstream_port,
        "not-base64!",
        "/allowed/item",
    );
    let response = proxy_request(endpoint, malformed).await;
    assert!(response.starts_with("HTTP/1.1 401"), "{response}");

    let denied = absolute_request("denied.test", upstream_port, &token, "/allowed/item");
    let response = proxy_request(endpoint, denied).await;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    assert!(
        response.contains("cowshed grant &lt;ws&gt;") || response.contains("cowshed grant <ws>")
    );

    let allowed = absolute_request("allowed.test", upstream_port, &token, "/allowed/item");
    let response = proxy_request(endpoint, allowed).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(!response.to_ascii_lowercase().contains("set-cookie"));
    let forwarded = captured.recv().await.expect("forwarded request");
    assert!(forwarded.starts_with("GET /allowed/item HTTP/1.1"));
    assert!(
        !forwarded
            .to_ascii_lowercase()
            .contains("proxy-authorization")
    );

    let unauthorized = timeout(Duration::from_secs(1), audit_rx.recv())
        .await
        .expect("audit timeout")
        .expect("unauthorized audit");
    assert_eq!(unauthorized.workspace_id, "raven");
    assert_eq!(unauthorized.http_status, Some(401));
    assert_eq!(unauthorized.method.as_deref(), Some("GET"));
    assert_eq!(unauthorized.path.as_deref(), Some("/allowed/item"));

    let denied = audit_rx.recv().await.expect("denied audit");
    assert_eq!(denied.http_status, Some(403));
    assert!(
        denied
            .grant_hint
            .as_deref()
            .is_some_and(|hint| hint.contains("denied.test"))
    );

    let allowed = timeout(Duration::from_secs(1), audit_rx.recv())
        .await
        .expect("completion audit timeout")
        .expect("completion audit");
    assert_eq!(allowed.workspace_id, "raven");
    assert_eq!(allowed.revision, 1);
    assert_eq!(allowed.http_status, Some(200));
    assert_eq!(allowed.method.as_deref(), Some("GET"));
    assert_eq!(allowed.path.as_deref(), Some("/allowed/item"));
    assert_eq!(allowed.bytes, 2);
    assert!(allowed.trace_id.is_some());
    assert!(unauthorized.sequence < denied.sequence && denied.sequence < allowed.sequence);
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn endpoint_identity_precedes_token_authentication() {
    let (upstream_port, _captured, _upstream) = http_fixture(1, None).await;
    let endpoint_a = free_endpoint();
    let endpoint_b = free_endpoint();
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
    let policy_a = WorkspacePolicy {
        grants: vec![grant("isolated.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let policy_b = policy_a.clone();
    let (session_a, token_a, _) = session(
        "alpha",
        "repo-a",
        WorkspaceEndpoint::Tcp(endpoint_a),
        1,
        1,
        policy_a,
    );
    let (session_b, token_b, _) = session(
        "bravo",
        "repo-b",
        WorkspaceEndpoint::Tcp(endpoint_b),
        2,
        1,
        policy_b,
    );
    gateway
        .handle()
        .install(session_a)
        .await
        .expect("install alpha");
    gateway
        .handle()
        .install(session_b)
        .await
        .expect("install bravo");

    let wrong_endpoint = proxy_request(
        endpoint_b,
        absolute_request("isolated.test", upstream_port, &token_a, "/allowed").to_owned(),
    )
    .await;
    assert!(
        wrong_endpoint.starts_with("HTTP/1.1 401"),
        "{wrong_endpoint}"
    );

    let own_endpoint = proxy_request(
        endpoint_b,
        absolute_request("isolated.test", upstream_port, &token_b, "/allowed").to_owned(),
    )
    .await;
    assert!(own_endpoint.starts_with("HTTP/1.1 200"), "{own_endpoint}");
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn local_mirror_route_rewrites_only_the_admitted_scope() {
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let endpoint = free_endpoint();
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
    let policy = WorkspacePolicy {
        grants: Vec::new(),
        mirrors: vec![MirrorRoute {
            local_prefix: "/npm/".to_owned(),
            upstream_origin: format!("https://mirror.test:{upstream_port}"),
            protocol: MirrorProtocol::Npm,
            admitted_prefixes: vec!["/allowed".to_owned()],
            credentialed: false,
        }],
    };
    let (session, token, _) = session(
        "mirror",
        "repo-mirror",
        WorkspaceEndpoint::Tcp(endpoint),
        9,
        1,
        policy,
    );
    gateway
        .handle()
        .install(session)
        .await
        .expect("install mirror session");
    let request = format!(
        "GET /npm/allowed/pkg HTTP/1.1\r\nHost: {endpoint}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    let response = proxy_request(endpoint, request).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let forwarded = captured.recv().await.expect("captured mirror request");
    assert!(
        forwarded.starts_with("GET /allowed/pkg HTTP/1.1"),
        "{forwarded}"
    );

    let denied = format!(
        "GET /npm/private/pkg HTTP/1.1\r\nHost: {endpoint}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    let response = proxy_request(endpoint, denied).await;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn opaque_connect_preserves_bytes_exactly() {
    let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind echo fixture");
    let upstream_port = upstream.local_addr().expect("echo address").port();
    let payload = tls_client_hello("pinned.test");
    let expected = payload.clone();
    let echo = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.expect("accept opaque tunnel");
        let mut bytes = vec![0_u8; expected.len()];
        stream
            .read_exact(&mut bytes)
            .await
            .expect("read opaque ClientHello");
        assert_eq!(bytes, expected);
        stream
            .write_all(&bytes)
            .await
            .expect("echo opaque ClientHello");
    });
    let endpoint = free_endpoint();
    let (observed_tx, mut observed_rx) = mpsc::channel(1);
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: Some(observed_tx),
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let policy = WorkspacePolicy {
        grants: vec![EgressGrant::opaque("pinned.test", upstream_port).expect("opaque grant")],
        mirrors: Vec::new(),
    };
    let (session, token, _) = session(
        "opaque",
        "repo-opaque",
        WorkspaceEndpoint::Tcp(endpoint),
        3,
        1,
        policy,
    );
    gateway
        .handle()
        .install(session)
        .await
        .expect("install session");
    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    let connect = format!(
        "CONNECT pinned.test:{upstream_port} HTTP/1.1\r\nHost: pinned.test:{upstream_port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
    );
    stream
        .write_all(connect.as_bytes())
        .await
        .expect("write CONNECT");
    let head = read_response_head(&mut stream).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    stream
        .write_all(&payload)
        .await
        .expect("write opaque ClientHello");
    let mut echoed = vec![0_u8; payload.len()];
    stream
        .read_exact(&mut echoed)
        .await
        .expect("read echoed ClientHello");
    assert_eq!(echoed, payload);
    let observed = observed_rx.recv().await.expect("connector observation");
    assert_eq!(observed.purpose, UpstreamPurpose::OpaqueTcp);
    drop(stream);
    echo.await.expect("echo task");
    timeout(Duration::from_secs(1), async {
        loop {
            if gateway.handle().status().await.expect("status").active == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("opaque completion");
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn intercept_injects_only_gateway_headers_and_validates_sni() {
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let endpoint = free_endpoint();
    let origin = format!("https://secure.test:{upstream_port}");
    let credentials: Arc<dyn CredentialProvider> = Arc::new(FixedCredential {
        repo_id: "repo-secure".to_owned(),
        origin,
        value: "Bearer host-secret".to_owned(),
    });
    let gateway = gateway(
        test_config(),
        credentials,
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let policy = WorkspacePolicy {
        grants: vec![grant("secure.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let (session, token, ca_certificate) = session(
        "secure",
        "repo-secure",
        WorkspaceEndpoint::Tcp(endpoint),
        4,
        1,
        policy,
    );
    gateway
        .handle()
        .install(session)
        .await
        .expect("install session");

    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    let connect = format!(
        "CONNECT secure.test:{upstream_port} HTTP/1.1\r\nHost: secure.test:{upstream_port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
    );
    stream
        .write_all(connect.as_bytes())
        .await
        .expect("write CONNECT");
    let head = read_response_head(&mut stream).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");

    let mut roots = RootCertStore::empty();
    roots.add(ca_certificate).expect("trust fixture CA");
    let mut client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"http/1.1".to_vec()];
    let server_name = ServerName::try_from("secure.test".to_owned()).expect("server name");
    let mut tls = TlsConnector::from(Arc::new(client))
        .connect(server_name, stream)
        .await
        .expect("intercept TLS handshake");
    tls.write_all(
        format!(
            "GET /allowed/item HTTP/1.1\r\nHost: secure.test:{upstream_port}\r\nAuthorization: Bearer sandbox-secret\r\nCookie: sandbox=cookie\r\nProxy-Authorization: Bearer forged\r\nConnection: close\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .expect("write intercepted request");
    let mut response = Vec::new();
    tls.read_to_end(&mut response)
        .await
        .expect("read intercepted response");
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));

    let forwarded = captured.recv().await.expect("captured upstream request");
    let lowercase = forwarded.to_ascii_lowercase();
    assert!(lowercase.contains("authorization: bearer host-secret\r\n"));
    assert!(!lowercase.contains("sandbox-secret"));
    assert!(!lowercase.contains("sandbox=cookie"));
    assert!(!lowercase.contains("proxy-authorization"));
    assert!(lowercase.contains("traceparent: 00-"));
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn impersonation_suppresses_credentials_and_trace_headers() {
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let endpoint = free_endpoint();
    let origin = format!("http://plain.test:{upstream_port}");
    let gateway = gateway(
        test_config(),
        Arc::new(FixedCredential {
            repo_id: "repo-plain".to_owned(),
            origin,
            value: "Bearer must-not-appear".to_owned(),
        }),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let mut impersonated = grant("plain.test", upstream_port);
    impersonated.impersonate = true;
    let (session, token, _) = session(
        "plain",
        "repo-plain",
        WorkspaceEndpoint::Tcp(endpoint),
        5,
        1,
        WorkspacePolicy {
            grants: vec![impersonated],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(session)
        .await
        .expect("install session");
    let response = proxy_request(
        endpoint,
        absolute_request("plain.test", upstream_port, &token, "/allowed"),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let forwarded = captured
        .recv()
        .await
        .expect("captured request")
        .to_ascii_lowercase();
    assert!(!forwarded.contains("authorization:"));
    assert!(!forwarded.contains("traceparent:"));
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn dead_upstream_fails_fast_without_connecting() {
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Offline,
            observed: None,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (session, token, _) = session(
        "offline",
        "repo-offline",
        WorkspaceEndpoint::Tcp(endpoint),
        6,
        1,
        WorkspacePolicy {
            grants: vec![grant("offline.test", 443)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(session)
        .await
        .expect("install session");
    let started = Instant::now();
    let response = proxy_request(
        endpoint,
        absolute_request("offline.test", 443, &token, "/allowed"),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    assert!(response.contains("upstream is offline"));
    assert!(started.elapsed() < Duration::from_millis(500));
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn active_queue_and_overflow_limits_are_enforced() {
    let gate = Arc::new(Notify::new());
    let (upstream_port, mut captured, _upstream) = http_fixture(2, Some(Arc::clone(&gate))).await;
    let endpoint = free_endpoint();
    let mut config = test_config();
    config.limits = GatewayLimits {
        workspace_active: 1,
        workspace_queued: 1,
        global_active: 1,
        global_queued: 1,
        origin_active: 1,
        leaf_cache_workspace: 2,
        leaf_cache_global: 2,
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
    let (session, token, _) = session(
        "limited",
        "repo-limited",
        WorkspaceEndpoint::Tcp(endpoint),
        8,
        1,
        WorkspacePolicy {
            grants: vec![grant("queue.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(session)
        .await
        .expect("install session");

    let first_request = absolute_request("queue.test", upstream_port, &token, "/allowed/one");
    let first = tokio::spawn(proxy_request(endpoint, first_request));
    captured.recv().await.expect("first reached upstream");

    let second_request = absolute_request("queue.test", upstream_port, &token, "/allowed/two");
    let second = tokio::spawn(proxy_request(endpoint, second_request));
    timeout(Duration::from_secs(1), async {
        loop {
            let status = gateway.handle().status().await.expect("gateway status");
            if status.queued == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second request queued");

    let overflow = proxy_request(
        endpoint,
        absolute_request("queue.test", upstream_port, &token, "/allowed/three"),
    )
    .await;
    assert!(overflow.starts_with("HTTP/1.1 429"), "{overflow}");

    gate.notify_one();
    assert!(first.await.expect("first task").starts_with("HTTP/1.1 200"));
    captured.recv().await.expect("queued request promoted");
    gate.notify_one();
    assert!(
        second
            .await
            .expect("second task")
            .starts_with("HTTP/1.1 200")
    );
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn queued_request_timeout_cancels_without_leaking_a_slot() {
    let tunnel_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind held tunnel");
    let tunnel_port = tunnel_listener.local_addr().expect("tunnel address").port();
    let held = tokio::spawn(async move {
        let (mut stream, _) = tunnel_listener.accept().await.expect("accept held tunnel");
        let mut discarded = Vec::new();
        stream
            .read_to_end(&mut discarded)
            .await
            .expect("held tunnel closes");
    });
    let endpoint = free_endpoint();
    let mut config = test_config();
    config.limits = GatewayLimits {
        workspace_active: 1,
        workspace_queued: 1,
        global_active: 1,
        global_queued: 1,
        origin_active: 1,
        leaf_cache_workspace: 2,
        leaf_cache_global: 2,
    };
    config.timeouts.response_headers = Duration::from_millis(100);
    config.timeouts.request_total = Duration::from_millis(200);
    config.timeouts.tunnel_total = Duration::from_secs(2);
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
    let (session, token, _) = session(
        "queue-timeout",
        "repo-queue-timeout",
        WorkspaceEndpoint::Tcp(endpoint),
        10,
        1,
        WorkspacePolicy {
            grants: vec![
                EgressGrant::opaque("held.test", tunnel_port).expect("held grant"),
                grant("queued.test", 443),
            ],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(session)
        .await
        .expect("install session");
    let mut tunnel = TcpStream::connect(endpoint).await.expect("connect gateway");
    tunnel
        .write_all(
            format!(
                "CONNECT held.test:{tunnel_port} HTTP/1.1\r\nHost: held.test:{tunnel_port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("write held CONNECT");
    assert!(
        read_response_head(&mut tunnel)
            .await
            .starts_with("HTTP/1.1 200")
    );
    tunnel
        .write_all(&tls_client_hello("held.test"))
        .await
        .expect("write held ClientHello");
    let started = Instant::now();
    let response = proxy_request(
        endpoint,
        absolute_request("queued.test", 443, &token, "/allowed"),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 504"), "{response}");
    assert!(started.elapsed() >= Duration::from_millis(150));
    assert_eq!(gateway.handle().status().await.expect("status").queued, 0);
    drop(tunnel);
    held.await.expect("held tunnel task");
    timeout(Duration::from_secs(1), async {
        loop {
            if gateway.handle().status().await.expect("status").active == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("tunnel slot released");
    gateway.drain().await.expect("drain gateway");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn unix_socket_churn_unlinks_every_session() {
    let root = std::env::temp_dir().join(format!("cowshed-gateway-churn-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).expect("create fixture directory");
    let socket = root.join("workspace.sock");
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
            "repo-churn",
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
    std::fs::remove_dir_all(root).expect("remove fixture directory");
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
        revision: 3,
        endpoint: "127.0.0.1:40960".to_owned(),
        kind: AuditKind::Http,
        host: Some("example.test:443".to_owned()),
        method: Some("GET".to_owned()),
        path: Some("/v1".to_owned()),
        status: AuditStatus::Completed,
        http_status: Some(200),
        bytes: 12,
        trace_id: Some("00000000000000000000000000000001".to_owned()),
        grant_hint: None,
        classification: None,
    })
    .await
    .expect("record Arrow audit");
    sink.flush().await.expect("flush Arrow audit");
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
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema().field(0).name(), "sequence");
    assert!(reader.next().is_none());
    drop(sink);
    std::fs::remove_dir_all(root).expect("remove audit fixture");
}

#[tokio::test]
async fn control_socket_is_local_authenticated_and_reports_status() {
    let root = std::env::temp_dir().join(format!("cowshed-gateway-control-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).expect("create control fixture directory");
    let control = root.join("gateway.sock");
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
    let client = GatewayControlClient::new(control.clone()).expect("control client");
    let endpoint = free_endpoint();
    let (session, _token, _) = session(
        "controlled",
        "repo-controlled",
        WorkspaceEndpoint::Tcp(endpoint),
        42,
        1,
        WorkspacePolicy::default(),
    );
    client
        .install(&session)
        .await
        .expect("install through control socket");
    let status = client.status().await.expect("control status");
    assert_eq!(status.sessions.len(), 1);
    assert_eq!(status.sessions[0].workspace_id, "controlled");
    assert_eq!(status.sessions[0].revision, 1);
    let stale = client
        .remove("controlled", 2)
        .await
        .expect_err("revision fence");
    assert!(matches!(
        stale,
        ControlError::Rejected {
            code: ControlFailureCode::RevisionFence,
            ..
        }
    ));
    client
        .remove("controlled", 1)
        .await
        .expect("fenced removal through control socket");
    assert!(
        client
            .status()
            .await
            .expect("empty status")
            .sessions
            .is_empty()
    );
    gateway.drain().await.expect("drain gateway");
    assert!(!control.exists());
    std::fs::remove_dir_all(root).expect("remove control fixture directory");
}

#[tokio::test]
async fn revision_tombstone_and_rotation_preserve_authority() {
    let endpoint = free_endpoint();
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
    let make_session = |revision, endpoint| {
        session(
            "revision",
            "repo-revision",
            WorkspaceEndpoint::Tcp(endpoint),
            revision as u8,
            revision,
            WorkspacePolicy::default(),
        )
        .0
    };
    gateway
        .handle()
        .install(make_session(1, endpoint))
        .await
        .expect("install revision one");
    gateway
        .handle()
        .remove("revision", 1)
        .await
        .expect("remove revision one");
    assert!(matches!(
        gateway.handle().install(make_session(1, endpoint)).await,
        Err(GatewayError::StaleRevision)
    ));
    gateway
        .handle()
        .install(make_session(2, endpoint))
        .await
        .expect("install revision two");

    let occupied_endpoint = free_endpoint();
    let occupied = TcpListener::bind(occupied_endpoint)
        .await
        .expect("occupy replacement endpoint");
    assert!(matches!(
        gateway
            .handle()
            .install(make_session(3, occupied_endpoint))
            .await,
        Err(GatewayError::Io(_))
    ));
    let status = gateway.handle().status().await.expect("rotation status");
    assert_eq!(status.sessions[0].revision, 2);
    assert_eq!(status.sessions[0].endpoint, endpoint.to_string());
    let old_listener = TcpStream::connect(endpoint)
        .await
        .expect("old authority remains bound");
    drop(old_listener);
    drop(occupied);

    gateway
        .handle()
        .install(make_session(3, endpoint))
        .await
        .expect("same-endpoint rotation");
    assert_eq!(
        gateway.handle().status().await.expect("status").sessions[0].revision,
        3
    );
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn audit_failure_is_fail_closed_and_marks_gateway_draining() {
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(FailingAudit),
    )
    .await;
    let (installed, token, _) = session(
        "audit-failure",
        "repo-audit-failure",
        WorkspaceEndpoint::Tcp(endpoint),
        21,
        1,
        WorkspacePolicy::default(),
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install session");
    let denied = proxy_request(
        endpoint,
        absolute_request("denied.test", 443, &token, "/blocked"),
    )
    .await;
    assert!(denied.starts_with("HTTP/1.1 503"), "{denied}");
    assert!(gateway.handle().status().await.expect("status").draining);
    let replacement = session(
        "audit-failure",
        "repo-audit-failure",
        WorkspaceEndpoint::Tcp(endpoint),
        22,
        2,
        WorkspacePolicy::default(),
    )
    .0;
    assert!(matches!(
        gateway.handle().install(replacement).await,
        Err(GatewayError::Draining)
    ));
    drop(gateway);
}

#[tokio::test]
async fn opaque_rejects_non_tls_missing_and_mismatched_sni_without_connector_calls() {
    let endpoint = free_endpoint();
    let calls = Arc::new(AtomicUsize::new(0));
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(CountingFailConnector {
            calls: Arc::clone(&calls),
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (installed, token, _) = session(
        "opaque-validation",
        "repo-opaque-validation",
        WorkspaceEndpoint::Tcp(endpoint),
        23,
        1,
        WorkspacePolicy {
            grants: vec![EgressGrant::opaque("expected.test", 443).expect("opaque grant")],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install session");

    opaque_payload(endpoint, &token, "expected.test", 443, b"not tls").await;
    await_reclaimed(&gateway).await;
    opaque_payload(
        endpoint,
        &token,
        "expected.test",
        443,
        &tls_client_hello("other.test"),
    )
    .await;
    await_reclaimed(&gateway).await;
    opaque_payload(
        endpoint,
        &token,
        "expected.test",
        443,
        &tls_client_hello("127.0.0.1"),
    )
    .await;
    await_reclaimed(&gateway).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn active_error_and_disconnect_paths_reclaim_single_permit() {
    let single_permit_config = || {
        let mut config = test_config();
        config.limits = GatewayLimits {
            workspace_active: 1,
            workspace_queued: 1,
            global_active: 1,
            global_queued: 1,
            origin_active: 1,
            leaf_cache_workspace: 2,
            leaf_cache_global: 2,
        };
        config
    };

    {
        let endpoint = free_endpoint();
        let calls = Arc::new(AtomicUsize::new(0));
        let gateway = gateway(
            single_permit_config(),
            Arc::new(NoCredentials),
            Arc::new(CountingFailConnector {
                calls: Arc::clone(&calls),
            }),
            Arc::new(DiscardAudit),
        )
        .await;
        let (installed, token, _) = session(
            "connect-failure",
            "repo-connect-failure",
            WorkspaceEndpoint::Tcp(endpoint),
            24,
            1,
            WorkspacePolicy {
                grants: vec![grant("connect-failure.test", 443)],
                mirrors: Vec::new(),
            },
        );
        gateway.handle().install(installed).await.expect("install");
        for _ in 0..2 {
            let response = proxy_request(
                endpoint,
                absolute_request("connect-failure.test", 443, &token, "/allowed"),
            )
            .await;
            assert!(response.starts_with("HTTP/1.1 502"), "{response}");
            await_reclaimed(&gateway).await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        gateway
            .drain()
            .await
            .expect("drain connect-failure gateway");
    }

    {
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind credential upstream");
        let upstream_port = upstream.local_addr().expect("upstream address").port();
        let accepts = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = upstream
                    .accept()
                    .await
                    .expect("accept credential connection");
                drop(stream);
            }
        });
        let endpoint = free_endpoint();
        let gateway = gateway(
            single_permit_config(),
            Arc::new(FailingCredentials),
            Arc::new(LocalConnector {
                health: UpstreamHealth::Healthy,
                observed: None,
            }),
            Arc::new(DiscardAudit),
        )
        .await;
        let (installed, token, _) = session(
            "credential-failure",
            "repo-credential-failure",
            WorkspaceEndpoint::Tcp(endpoint),
            25,
            1,
            WorkspacePolicy {
                grants: vec![grant("credential-failure.test", upstream_port)],
                mirrors: Vec::new(),
            },
        );
        gateway.handle().install(installed).await.expect("install");
        for _ in 0..2 {
            let response = proxy_request(
                endpoint,
                absolute_request("credential-failure.test", upstream_port, &token, "/allowed"),
            )
            .await;
            assert!(response.starts_with("HTTP/1.1 502"), "{response}");
            await_reclaimed(&gateway).await;
        }
        timeout(Duration::from_secs(1), accepts)
            .await
            .expect("credential accepts timeout")
            .expect("credential accepts task");
        gateway.drain().await.expect("drain credential gateway");
    }

    {
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind header upstream");
        let upstream_port = upstream.local_addr().expect("upstream address").port();
        let accepts = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = upstream.accept().await.expect("accept header connection");
                let _ = read_headers(&mut stream).await;
            }
        });
        let endpoint = free_endpoint();
        let gateway = gateway(
            single_permit_config(),
            Arc::new(NoCredentials),
            Arc::new(LocalConnector {
                health: UpstreamHealth::Healthy,
                observed: None,
            }),
            Arc::new(DiscardAudit),
        )
        .await;
        let (installed, token, _) = session(
            "header-failure",
            "repo-header-failure",
            WorkspaceEndpoint::Tcp(endpoint),
            26,
            1,
            WorkspacePolicy {
                grants: vec![grant("header-failure.test", upstream_port)],
                mirrors: Vec::new(),
            },
        );
        gateway.handle().install(installed).await.expect("install");
        for _ in 0..2 {
            let response = proxy_request(
                endpoint,
                absolute_request("header-failure.test", upstream_port, &token, "/allowed"),
            )
            .await;
            assert!(response.starts_with("HTTP/1.1 502"), "{response}");
            await_reclaimed(&gateway).await;
        }
        timeout(Duration::from_secs(1), accepts)
            .await
            .expect("header accepts timeout")
            .expect("header accepts task");
        gateway.drain().await.expect("drain header gateway");
    }

    {
        let gate = Arc::new(Notify::new());
        let (upstream_port, mut captured, _upstream) =
            http_fixture(1, Some(Arc::clone(&gate))).await;
        let endpoint = free_endpoint();
        let gateway = gateway(
            single_permit_config(),
            Arc::new(NoCredentials),
            Arc::new(LocalConnector {
                health: UpstreamHealth::Healthy,
                observed: None,
            }),
            Arc::new(DiscardAudit),
        )
        .await;
        let (installed, token, _) = session(
            "disconnect",
            "repo-disconnect",
            WorkspaceEndpoint::Tcp(endpoint),
            27,
            1,
            WorkspacePolicy {
                grants: vec![grant("disconnect.test", upstream_port)],
                mirrors: Vec::new(),
            },
        );
        gateway.handle().install(installed).await.expect("install");
        let mut client = TcpStream::connect(endpoint).await.expect("connect client");
        client
            .write_all(
                absolute_request("disconnect.test", upstream_port, &token, "/allowed").as_bytes(),
            )
            .await
            .expect("write request");
        timeout(Duration::from_secs(1), captured.recv())
            .await
            .expect("upstream capture timeout")
            .expect("upstream capture");
        drop(client);
        gate.notify_one();
        await_reclaimed(&gateway).await;
        gateway.drain().await.expect("drain disconnect gateway");
    }
}

#[tokio::test]
async fn queued_disconnect_and_drain_reclaim_all_capacity() {
    let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind held tunnel");
    let upstream_port = upstream.local_addr().expect("upstream address").port();
    let held = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.expect("accept held tunnel");
        let mut discarded = Vec::new();
        stream
            .read_to_end(&mut discarded)
            .await
            .expect("held tunnel closes");
    });
    let mut config = test_config();
    config.limits = GatewayLimits {
        workspace_active: 1,
        workspace_queued: 1,
        global_active: 1,
        global_queued: 1,
        origin_active: 1,
        leaf_cache_workspace: 2,
        leaf_cache_global: 2,
    };
    let endpoint = free_endpoint();
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
    let (installed, token, _) = session(
        "queue-cancel",
        "repo-queue-cancel",
        WorkspaceEndpoint::Tcp(endpoint),
        28,
        1,
        WorkspacePolicy {
            grants: vec![
                EgressGrant::opaque("held-cancel.test", upstream_port).expect("opaque grant"),
                grant("queued-cancel.test", 443),
            ],
            mirrors: Vec::new(),
        },
    );
    gateway.handle().install(installed).await.expect("install");

    let mut tunnel = TcpStream::connect(endpoint).await.expect("connect tunnel");
    let connect = format!(
        "CONNECT held-cancel.test:{upstream_port} HTTP/1.1\r\nHost: held-cancel.test:{upstream_port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
    );
    tunnel
        .write_all(connect.as_bytes())
        .await
        .expect("write CONNECT");
    assert!(
        read_response_head(&mut tunnel)
            .await
            .starts_with("HTTP/1.1 200")
    );
    tunnel
        .write_all(&tls_client_hello("held-cancel.test"))
        .await
        .expect("write ClientHello");

    let mut queued = TcpStream::connect(endpoint).await.expect("connect queued");
    queued
        .write_all(absolute_request("queued-cancel.test", 443, &token, "/allowed").as_bytes())
        .await
        .expect("write queued request");
    timeout(Duration::from_secs(1), async {
        loop {
            if gateway.handle().status().await.expect("status").queued == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request did not queue");
    drop(queued);
    timeout(Duration::from_secs(1), async {
        loop {
            let status = gateway.handle().status().await.expect("status");
            if status.active == 1 && status.queued == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued cancellation was not reclaimed");

    timeout(Duration::from_secs(2), gateway.drain())
        .await
        .expect("drain timeout")
        .expect("drain gateway");
    let mut discarded = Vec::new();
    timeout(Duration::from_secs(1), tunnel.read_to_end(&mut discarded))
        .await
        .expect("tunnel close timeout")
        .expect("read tunnel close");
    timeout(Duration::from_secs(1), held)
        .await
        .expect("held upstream timeout")
        .expect("held upstream task");
}

#[tokio::test]
async fn client_tls_failures_reclaim_permits_and_pre_admission_denials_are_audited() {
    let mut config = test_config();
    config.limits = GatewayLimits {
        workspace_active: 1,
        workspace_queued: 1,
        global_active: 1,
        global_queued: 1,
        origin_active: 1,
        leaf_cache_workspace: 2,
        leaf_cache_global: 2,
    };
    let endpoint = free_endpoint();
    let (audit_tx, mut audit_rx) = mpsc::channel(8);
    let gateway = gateway(
        config,
        Arc::new(NoCredentials),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(ChannelAudit(audit_tx)),
    )
    .await;
    let (installed, token, _) = session(
        "client-tls",
        "repo-client-tls",
        WorkspaceEndpoint::Tcp(endpoint),
        29,
        1,
        WorkspacePolicy {
            grants: vec![grant("client-tls.test", 443)],
            mirrors: Vec::new(),
        },
    );
    gateway.handle().install(installed).await.expect("install");

    let malformed = format!(
        "CONNECT client-tls.test:443 HTTP/1.1\r\nHost: wrong.test:443\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    let response = proxy_request(endpoint, malformed).await;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    let denial = timeout(Duration::from_secs(1), audit_rx.recv())
        .await
        .expect("denial audit timeout")
        .expect("denial audit");
    assert_eq!(denial.status, AuditStatus::Denied);
    assert_eq!(
        denial.classification.as_deref(),
        Some("connect-host-mismatch")
    );

    for _ in 0..2 {
        let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
        let connect = format!(
            "CONNECT client-tls.test:443 HTTP/1.1\r\nHost: client-tls.test:443\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
        );
        stream
            .write_all(connect.as_bytes())
            .await
            .expect("write CONNECT");
        assert!(
            read_response_head(&mut stream)
                .await
                .starts_with("HTTP/1.1 200")
        );
        stream
            .write_all(b"not a TLS record")
            .await
            .expect("write invalid TLS");
        stream.shutdown().await.expect("shutdown client");
        let mut discarded = Vec::new();
        timeout(Duration::from_secs(1), stream.read_to_end(&mut discarded))
            .await
            .expect("TLS rejection timeout")
            .expect("read TLS rejection");
        await_reclaimed(&gateway).await;
    }
    gateway.drain().await.expect("drain gateway");
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
