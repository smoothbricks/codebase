use std::{
    collections::BTreeSet,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use cowshed_gateway::{
    ArrowAuditConfig, ArrowAuditSink, AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus,
    AuthorizedTarget, BoxIo, CanonicalTarget, ConnectError, ControlError, ControlFailureCode,
    CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery, CredentialRecord,
    EgressGrant, Gateway, GatewayConfig, GatewayControlClient, GatewayLimits, GatewayTimeouts,
    HostPattern, MirrorProtocol, MirrorRoute, UpstreamConnector, UpstreamHealth, UpstreamPurpose,
    WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession, WorkspaceToken,
};
use http::HeaderName;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
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
    let listener =
        std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve loopback endpoint");
    listener.local_addr().expect("fixture local address")
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
    let echo = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.expect("accept opaque tunnel");
        let mut bytes = [0u8; 10];
        stream
            .read_exact(&mut bytes)
            .await
            .expect("read opaque bytes");
        stream.write_all(&bytes).await.expect("echo opaque bytes");
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
    let payload = b"\x00TLS\xffbytes";
    stream
        .write_all(payload)
        .await
        .expect("write opaque payload");
    let mut echoed = [0u8; 10];
    stream
        .read_exact(&mut echoed)
        .await
        .expect("read echoed payload");
    assert_eq!(&echoed, payload);
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

#[test]
fn validation_rejects_ambiguous_and_overbroad_policy() {
    assert!(WorkspaceToken::parse("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").is_err());
    assert!(HostPattern::parse("*.*.example.com").is_err());
    assert!(cowshed_gateway::normalize_path("/a/%2f/b").is_err());
    assert!(cowshed_gateway::normalize_path("/a/../b").is_err());
    assert!(
        WorkspaceEndpoint::Tcp(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
            40960,
        ))
        .validate()
        .is_err()
    );
}
