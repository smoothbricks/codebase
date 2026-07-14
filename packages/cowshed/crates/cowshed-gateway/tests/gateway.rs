use std::{
    collections::BTreeSet,
    convert::Infallible,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use cowshed_gateway::{
    ArrowAuditConfig, ArrowAuditSink, AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus,
    AuthorizedTarget, CanonicalTarget, ConnectError, ControlError, ControlFailureCode,
    CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery, CredentialRecord,
    EgressGrant, Gateway, GatewayConfig, GatewayControlClient, GatewayError, GatewayLimits,
    GatewayTimeouts, HostPattern, MirrorCacheConfig, MirrorCacheStatus, MirrorProtocol,
    MirrorRoute, NegotiatedTransport, UpstreamConnection, UpstreamConnector, UpstreamHealth,
    UpstreamPurpose, WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession,
    WorkspaceToken,
};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode, Version, header};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{BodyExt as _, Empty, Full};
use hyper::{
    client::conn::http2 as client_http2,
    server::conn::{http1 as server_http1, http2 as server_http2},
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
};
#[cfg(target_os = "linux")]
use std::{path::PathBuf, sync::LazyLock};
#[cfg(target_os = "linux")]
use tokio::net::UnixStream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use zeroize::Zeroizing;

#[cfg(target_os = "linux")]
static LINUX_SOCKET_ROOT: LazyLock<PathBuf> = LazyLock::new(|| {
    use std::os::unix::fs::PermissionsExt as _;

    let root = std::env::temp_dir().join(format!("cowshed-gateway-data-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).expect("create Linux data socket root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure Linux data socket root");
    root
});

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

#[cfg(target_os = "macos")]
#[derive(Clone)]
struct VerifiedTlsConnector {
    tls: Arc<ClientConfig>,
    negotiated: mpsc::Sender<Option<Vec<u8>>>,
}

#[cfg(target_os = "macos")]
#[async_trait]
impl UpstreamConnector for VerifiedTlsConnector {
    async fn health(&self, _target: &CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Healthy
    }

    async fn connect(
        &self,
        authorized: &AuthorizedTarget,
    ) -> Result<UpstreamConnection, ConnectError> {
        if authorized.purpose != UpstreamPurpose::TlsHttp {
            return Err(ConnectError::Io(io::Error::other(
                "TLS fixture received non-TLS purpose",
            )));
        }
        let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, authorized.target.port))
            .await
            .map_err(ConnectError::Io)?;
        let server_name = ServerName::try_from(authorized.target.host.as_str().to_owned())
            .map_err(|_| ConnectError::InvalidServerName)?;
        let tls = TlsConnector::from(Arc::clone(&self.tls))
            .connect(server_name, stream)
            .await
            .map_err(|error| ConnectError::Tls(error.to_string()))?;
        let alpn = tls.get_ref().1.alpn_protocol().map(<[u8]>::to_vec);
        let _ = self.negotiated.send(alpn.clone()).await;
        let transport = match alpn.as_deref() {
            Some(b"h2") => NegotiatedTransport::Http2,
            Some(b"http/1.1") => NegotiatedTransport::Http1,
            Some(_) => return Err(ConnectError::UnsupportedAlpn),
            None => return Err(ConnectError::MissingAlpn),
        };
        Ok(UpstreamConnection {
            io: Box::new(tls),
            transport,
        })
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

    async fn connect(
        &self,
        _target: &AuthorizedTarget,
    ) -> Result<UpstreamConnection, ConnectError> {
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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
        data_socket_root: Some(LINUX_SOCKET_ROOT.clone()),
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

#[cfg(target_os = "macos")]
struct ChannelBody {
    receiver: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
}

#[cfg(target_os = "macos")]
impl Body for ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.receiver.poll_recv(context)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

#[cfg(target_os = "macos")]
fn fixture_tls_configs(
    host: &str,
    server_alpn: Vec<Vec<u8>>,
) -> (Arc<ServerConfig>, Arc<ClientConfig>) {
    let ca_key = KeyPair::generate().expect("upstream CA key");
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_certificate = ca_params
        .self_signed(&ca_key)
        .expect("upstream CA certificate");
    let issuer =
        Issuer::from_ca_cert_pem(&ca_certificate.pem(), ca_key).expect("upstream CA issuer");
    let leaf_key = KeyPair::generate().expect("upstream leaf key");
    let leaf = CertificateParams::new(vec![host.to_owned()])
        .expect("upstream leaf params")
        .signed_by(&leaf_key, &issuer)
        .expect("upstream leaf certificate");
    let chain = vec![
        CertificateDer::from(leaf.der().to_vec()),
        CertificateDer::from(ca_certificate.der().to_vec()),
    ];
    let key = PrivatePkcs8KeyDer::from(leaf_key.serialize_der()).into();
    let mut server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .expect("upstream server TLS");
    server.alpn_protocols = server_alpn;

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(ca_certificate.der().to_vec()))
        .expect("trust upstream CA");
    let mut client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    (Arc::new(server), Arc::new(client))
}

#[cfg(target_os = "macos")]
async fn h2_tls_fixture(
    host: &str,
) -> (
    u16,
    Arc<ClientConfig>,
    Arc<Notify>,
    mpsc::Receiver<String>,
    JoinHandle<()>,
) {
    let (server, client) = fixture_tls_configs(host, vec![b"h2".to_vec()]);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind h2 TLS fixture");
    let port = listener.local_addr().expect("h2 fixture address").port();
    let gate = Arc::new(Notify::new());
    let producer_gate = Arc::clone(&gate);
    let (captured, receiver) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept h2 TLS");
        let tls = TlsAcceptor::from(server)
            .accept(stream)
            .await
            .expect("h2 TLS handshake");
        assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
        let service = service_fn(move |request: Request<hyper::body::Incoming>| {
            let gate = Arc::clone(&producer_gate);
            let captured = captured.clone();
            async move {
                captured
                    .send(request.uri().to_string())
                    .await
                    .expect("capture h2 request");
                let (frames, receiver) = mpsc::channel(1);
                tokio::spawn(async move {
                    frames
                        .send(Ok(Frame::data(Bytes::from(vec![b'a'; 24 * 1024]))))
                        .await
                        .expect("send first h2 body frame");
                    gate.notified().await;
                    frames
                        .send(Ok(Frame::data(Bytes::from(vec![b'b'; 48 * 1024]))))
                        .await
                        .expect("send second h2 body frame");
                    let mut trailers = HeaderMap::new();
                    trailers.insert(
                        HeaderName::from_static("x-fixture-trailer"),
                        HeaderValue::from_static("complete"),
                    );
                    frames
                        .send(Ok(Frame::trailers(trailers)))
                        .await
                        .expect("send h2 trailers");
                });
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "text/event-stream")
                        .body(ChannelBody { receiver })
                        .expect("h2 fixture response"),
                )
            }
        });
        let mut builder = server_http2::Builder::new(TokioExecutor::new());
        builder
            .max_concurrent_streams(8)
            .max_header_list_size(64 * 1024)
            .max_send_buf_size(64 * 1024);
        let _ = builder.serve_connection(TokioIo::new(tls), service).await;
    });
    (port, client, gate, receiver, task)
}

#[cfg(target_os = "macos")]
async fn h1_tls_fixture(host: &str) -> (u16, Arc<ClientConfig>, JoinHandle<()>) {
    let (server, client) = fixture_tls_configs(host, vec![b"http/1.1".to_vec()]);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind h1 TLS fixture");
    let port = listener.local_addr().expect("h1 fixture address").port();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept h1 TLS");
        let tls = TlsAcceptor::from(server)
            .accept(stream)
            .await
            .expect("h1 TLS handshake");
        assert_eq!(
            tls.get_ref().1.alpn_protocol(),
            Some(b"http/1.1".as_slice())
        );
        let service = service_fn(|_request| async {
            Ok::<_, Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from_static(b"h1-fallback")))
                    .expect("h1 fixture response"),
            )
        });
        let _ = server_http1::Builder::new()
            .serve_connection(TokioIo::new(tls), service)
            .await;
    });
    (port, client, task)
}

#[cfg(target_os = "macos")]
async fn no_alpn_tls_fixture(
    host: &str,
) -> (u16, Arc<ClientConfig>, mpsc::Receiver<bool>, JoinHandle<()>) {
    let (server, client) = fixture_tls_configs(host, Vec::new());
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind no-ALPN TLS fixture");
    let port = listener
        .local_addr()
        .expect("no-ALPN fixture address")
        .port();
    let (observed, receiver) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept no-ALPN TLS");
        let mut tls = TlsAcceptor::from(server)
            .accept(stream)
            .await
            .expect("no-ALPN TLS handshake");
        assert_eq!(tls.get_ref().1.alpn_protocol(), None);
        let mut byte = [0_u8; 1];
        let received_http = matches!(
            timeout(Duration::from_secs(1), tls.read(&mut byte)).await,
            Ok(Ok(count)) if count > 0
        );
        observed
            .send(received_http)
            .await
            .expect("report no-ALPN bytes");
    });
    (port, client, receiver, task)
}

#[cfg(target_os = "macos")]
async fn h2_intercept_client(
    endpoint: SocketAddr,
    token: &str,
    host: &str,
    port: u16,
    ca_certificate: CertificateDer<'static>,
) -> (
    client_http2::SendRequest<Empty<Bytes>>,
    JoinHandle<Result<(), hyper::Error>>,
) {
    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    stream
        .write_all(
            format!(
                "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("write CONNECT");
    let head = read_response_head(&mut stream).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");

    let mut roots = RootCertStore::empty();
    roots.add(ca_certificate).expect("trust workspace CA");
    let mut client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let server_name = ServerName::try_from(host.to_owned()).expect("fixture server name");
    let tls = TlsConnector::from(Arc::new(client))
        .connect(server_name, stream)
        .await
        .expect("intercept TLS handshake");
    assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
    let (sender, connection) = client_http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(tls))
        .await
        .expect("downstream h2 handshake");
    (sender, tokio::spawn(connection))
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn allow_deny_malformed_token_and_audit_fields() {
    let (upstream_port, mut captured, _upstream) = http_fixture(2, None).await;
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

    let allowed = absolute_request("allowed.test", upstream_port, &token, "/allowed/item")
        .replace(
            "\r\n\r\n",
            "\r\ntraceparent: 00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-03\r\ntracestate: vendor=opaque\r\n\r\n",
        );
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
    assert!(forwarded.contains(
        "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-"
    ));
    assert!(!forwarded.contains("00F067AA0BA902B7"));
    assert!(forwarded.contains("tracestate: vendor=opaque"));
    let invalid_trace =
        absolute_request("allowed.test", upstream_port, &token, "/allowed/invalid")
            .replace(
                "\r\n\r\n",
                "\r\ntraceparent: 00-00000000000000000000000000000000-00f067aa0ba902b7-01\r\ntracestate: vendor=opaque\r\n\r\n",
            );
    let invalid_response = proxy_request(endpoint, invalid_trace).await;
    assert!(
        invalid_response.starts_with("HTTP/1.1 200"),
        "{invalid_response}"
    );
    let invalid_forwarded = captured.recv().await.expect("invalid trace forwarded");
    assert!(!invalid_forwarded.contains("00000000000000000000000000000000"));
    assert!(!invalid_forwarded.contains("tracestate:"));

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
    assert_eq!(
        allowed.trace_id.as_deref(),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
    assert_eq!(allowed.parent_span_id, Some(0x00f0_67aa_0ba9_02b7));
    assert_ne!(allowed.upstream_span_id, Some(allowed.span_id));
    assert_eq!(allowed.tracestate.as_deref(), Some("vendor=opaque"));
    let invalid = timeout(Duration::from_secs(1), audit_rx.recv())
        .await
        .expect("invalid trace audit timeout")
        .expect("invalid trace audit");
    assert_eq!(invalid.classification.as_deref(), Some("invalid-trace-context"));
    assert!(invalid.parent_span_id.is_none());
    assert!(invalid.tracestate.is_none());
    assert!(
        unauthorized.sequence < denied.sequence
            && denied.sequence < allowed.sequence
            && allowed.sequence < invalid.sequence
    );
    gateway.drain().await.expect("drain gateway");
}

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn local_mirror_route_rewrites_only_the_admitted_scope() {
    let (upstream_port, mut captured, _upstream) = http_fixture(2, None).await;
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
            admitted_prefixes: vec!["/allowed".to_owned(), "/@scope/".to_owned()],
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

    let scoped = format!(
        "GET /npm/@scope%2fpkg HTTP/1.1\r\nHost: {endpoint}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    let response = proxy_request(endpoint, scoped).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    let forwarded = captured.recv().await.expect("captured scoped npm request");
    assert!(
        forwarded.starts_with("GET /@scope%2fpkg HTTP/1.1"),
        "{forwarded}"
    );

    let public_baseline = format!(
        "GET /npm/private/pkg HTTP/1.1\r\nHost: {endpoint}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    let response = proxy_request(endpoint, public_baseline).await;
    assert!(
        response.starts_with("HTTP/1.1 502"),
        "unmatched private scope must fall through to the fixed public baseline: {response}"
    );
    gateway.drain().await.expect("drain gateway");
}

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn active_queue_and_overflow_limits_are_enforced() {
    let gate = Arc::new(Notify::new());
    let (upstream_port, mut captured, _upstream) = http_fixture(2, Some(Arc::clone(&gate))).await;
    let endpoint = free_endpoint();
    let mut config = test_config();
    config.limits = GatewayLimits {
        max_sessions: 2,
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

#[cfg(target_os = "macos")]
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
        max_sessions: 2,
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
    assert!(LINUX_SOCKET_ROOT.exists());
}

#[cfg(target_os = "linux")]
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
        "repo-linux-request",
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
        "repo-outside",
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
        "repo-regular",
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
        "repo-control",
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
        repo_id: "repo-audit".to_owned(),
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
            repo_id: "repo-audit".to_owned(),
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn control_start_failure_stops_and_joins_gateway_actor() {
    let mut config = test_config();
    let missing_parent = std::env::temp_dir().join(format!(
        "cowshed-missing-control-parent-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&missing_parent);
    config.control_socket = Some(missing_parent.join("gateway.sock"));
    let credentials = Arc::new(NoCredentials);
    let connector = Arc::new(LocalConnector {
        health: UpstreamHealth::Healthy,
        observed: None,
    });
    let audit = Arc::new(DiscardAudit);
    let result = Gateway::start(
        config,
        credentials.clone(),
        connector.clone(),
        audit.clone(),
    )
    .await;
    assert!(matches!(result, Err(GatewayError::Io(_))));
    assert_eq!(Arc::strong_count(&credentials), 1);
    assert_eq!(Arc::strong_count(&connector), 1);
    assert_eq!(Arc::strong_count(&audit), 1);
}

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn audit_failure_is_fail_closed_and_marks_gateway_draining() {
    let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind active HTTP upstream");
    let upstream_port = upstream.local_addr().expect("upstream address").port();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let active_upstream = tokio::spawn(async move {
        let (mut stream, _) = upstream.accept().await.expect("accept HTTP stream");
        let request = read_headers(&mut stream).await;
        started_tx.send(()).expect("signal in-flight request");
        let mut trailing = Vec::new();
        stream
            .read_to_end(&mut trailing)
            .await
            .expect("audit failure closes active stream");
        assert!(request.contains("GET /allowed"));
        assert!(trailing.is_empty(), "bytes arrived after audit hard-stop");
    });
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
        WorkspacePolicy {
            grants: vec![grant("audit-active.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install session");
    let mut active = TcpStream::connect(endpoint).await.expect("connect gateway");
    active
        .write_all(
            absolute_request("audit-active.test", upstream_port, &token, "/allowed").as_bytes(),
        )
        .await
        .expect("write in-flight request");
    timeout(Duration::from_secs(1), started_rx)
        .await
        .expect("in-flight request timeout")
        .expect("in-flight request signal");
    let denied = proxy_request(
        endpoint,
        absolute_request("denied.test", 443, &token, "/blocked"),
    )
    .await;
    assert!(
        denied.is_empty() || denied.starts_with("HTTP/1.1 503"),
        "{denied}"
    );
    assert!(gateway.handle().status().await.expect("status").draining);
    timeout(Duration::from_secs(1), active_upstream)
        .await
        .expect("active stream did not close")
        .expect("active upstream task");
    await_reclaimed(&gateway).await;
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

#[cfg(target_os = "macos")]
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
            grants: vec![
                EgressGrant::opaque("expected.test", 443).expect("opaque DNS grant"),
                EgressGrant::opaque("127.0.0.1", 444).expect("opaque IP grant"),
            ],
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
    opaque_payload(
        endpoint,
        &token,
        "127.0.0.1",
        444,
        &tls_client_hello("127.0.0.1"),
    )
    .await;
    await_reclaimed(&gateway).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    opaque_payload(
        endpoint,
        &token,
        "127.0.0.1",
        444,
        &tls_client_hello("conflict.test"),
    )
    .await;
    await_reclaimed(&gateway).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    gateway.drain().await.expect("drain gateway");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn active_error_and_disconnect_paths_reclaim_single_permit() {
    let single_permit_config = || {
        let mut config = test_config();
        config.limits = GatewayLimits {
            max_sessions: 2,
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

#[cfg(target_os = "macos")]
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
        max_sessions: 2,
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn client_tls_failures_reclaim_permits_and_pre_admission_denials_are_audited() {
    let mut config = test_config();
    config.limits = GatewayLimits {
        max_sessions: 2,
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn h2_intercept_and_upstream_preserve_streaming_trailers_and_authority() {
    let (upstream_port, upstream_tls, gate, mut captured, upstream_task) =
        h2_tls_fixture("secure-h2.test").await;
    let (negotiated, mut negotiated_rx) = mpsc::channel(2);
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(VerifiedTlsConnector {
            tls: upstream_tls,
            negotiated,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (installed, token, ca_certificate) = session(
        "h2-streaming",
        "repo-h2-streaming",
        WorkspaceEndpoint::Tcp(endpoint),
        31,
        1,
        WorkspacePolicy {
            grants: vec![grant("secure-h2.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install h2 session");

    let (mut sender, downstream_connection) = h2_intercept_client(
        endpoint,
        &token,
        "secure-h2.test",
        upstream_port,
        ca_certificate,
    )
    .await;
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!(
            "https://secure-h2.test:{upstream_port}/allowed/events"
        ))
        .header(header::HOST, format!("secure-h2.test:{upstream_port}"))
        .body(Empty::<Bytes>::new())
        .expect("h2 request");
    let response = sender.send_request(request).await.expect("h2 response");
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE),
        Some(&HeaderValue::from_static("text/event-stream"))
    );
    assert_eq!(
        negotiated_rx.recv().await.expect("upstream ALPN"),
        Some(b"h2".to_vec())
    );
    assert_eq!(
        captured.recv().await.expect("captured h2 request"),
        format!("https://secure-h2.test:{upstream_port}/allowed/events")
    );

    let mut body = response.into_body();
    let first = timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("first frame arrived before remainder was released")
        .expect("first frame exists")
        .expect("first frame succeeds");
    let mut bytes = first.data_ref().map_or(0, Bytes::len);
    assert!(bytes > 0, "first frame must contain streaming data");
    gate.notify_waiters();
    let mut saw_trailers = false;
    let mut frames = 1usize;
    while let Some(frame) = body.frame().await {
        let frame = frame.expect("streamed h2 frame");
        if let Some(data) = frame.data_ref() {
            bytes += data.len();
            frames += 1;
        }
        if let Some(trailers) = frame.trailers_ref() {
            saw_trailers =
                trailers.get("x-fixture-trailer") == Some(&HeaderValue::from_static("complete"));
        }
    }
    assert_eq!(bytes, 72 * 1024);
    assert!(
        frames > 1,
        "body was not transported across multiple frames"
    );
    assert!(saw_trailers, "response trailers were not preserved");

    let mismatch = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("https://other.test:{upstream_port}/allowed"))
        .header(header::HOST, format!("secure-h2.test:{upstream_port}"))
        .body(Empty::<Bytes>::new())
        .expect("mismatched h2 request");
    let denied = sender
        .send_request(mismatch)
        .await
        .expect("authority mismatch response");
    assert_eq!(denied.status(), StatusCode::BAD_REQUEST);

    drop(sender);
    gateway.drain().await.expect("drain h2 gateway");
    let _ = timeout(Duration::from_secs(1), downstream_connection).await;
    timeout(Duration::from_secs(1), upstream_task)
        .await
        .expect("h2 upstream task timeout")
        .expect("h2 upstream task");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn upstream_tls_alpn_selects_h1_fallback_without_downgrading_h2() {
    let (upstream_port, upstream_tls, upstream_task) = h1_tls_fixture("fallback.test").await;
    let (negotiated, mut negotiated_rx) = mpsc::channel(1);
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(VerifiedTlsConnector {
            tls: upstream_tls,
            negotiated,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (installed, token, ca_certificate) = session(
        "h2-h1-fallback",
        "repo-h2-h1-fallback",
        WorkspaceEndpoint::Tcp(endpoint),
        32,
        1,
        WorkspacePolicy {
            grants: vec![grant("fallback.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install fallback session");
    let (mut sender, downstream_connection) = h2_intercept_client(
        endpoint,
        &token,
        "fallback.test",
        upstream_port,
        ca_certificate,
    )
    .await;
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("https://fallback.test:{upstream_port}/allowed"))
        .header(header::HOST, format!("fallback.test:{upstream_port}"))
        .body(Empty::<Bytes>::new())
        .expect("fallback request");
    let response = sender
        .send_request(request)
        .await
        .expect("fallback response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        negotiated_rx.recv().await.expect("fallback ALPN"),
        Some(b"http/1.1".to_vec())
    );
    assert_eq!(
        response
            .into_body()
            .collect()
            .await
            .expect("fallback body")
            .to_bytes(),
        Bytes::from_static(b"h1-fallback")
    );
    drop(sender);
    gateway.drain().await.expect("drain fallback gateway");
    let _ = timeout(Duration::from_secs(1), downstream_connection).await;
    timeout(Duration::from_secs(1), upstream_task)
        .await
        .expect("h1 upstream task timeout")
        .expect("h1 upstream task");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn missing_upstream_alpn_fails_without_sending_http1_bytes() {
    let (upstream_port, upstream_tls, mut received_http, upstream_task) =
        no_alpn_tls_fixture("no-alpn.test").await;
    let (negotiated, mut negotiated_rx) = mpsc::channel(1);
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(VerifiedTlsConnector {
            tls: upstream_tls,
            negotiated,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (installed, token, ca_certificate) = session(
        "no-upstream-alpn",
        "repo-no-upstream-alpn",
        WorkspaceEndpoint::Tcp(endpoint),
        33,
        1,
        WorkspacePolicy {
            grants: vec![grant("no-alpn.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install no-ALPN session");
    let (mut sender, downstream_connection) = h2_intercept_client(
        endpoint,
        &token,
        "no-alpn.test",
        upstream_port,
        ca_certificate,
    )
    .await;
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("https://no-alpn.test:{upstream_port}/allowed"))
        .header(header::HOST, format!("no-alpn.test:{upstream_port}"))
        .body(Empty::<Bytes>::new())
        .expect("no-ALPN request");
    let response = sender
        .send_request(request)
        .await
        .expect("no-ALPN gateway response");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(negotiated_rx.recv().await.expect("no-ALPN result"), None);
    assert!(
        !received_http.recv().await.expect("no-ALPN byte report"),
        "gateway sent HTTP/1.1 after TLS selected no ALPN"
    );
    drop(sender);
    gateway.drain().await.expect("drain no-ALPN gateway");
    let _ = timeout(Duration::from_secs(1), downstream_connection).await;
    timeout(Duration::from_secs(1), upstream_task)
        .await
        .expect("no-ALPN upstream task timeout")
        .expect("no-ALPN upstream task");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn missing_downstream_alpn_is_not_silently_treated_as_http1() {
    let calls = Arc::new(AtomicUsize::new(0));
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(CountingFailConnector {
            calls: Arc::clone(&calls),
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let port = 443;
    let (installed, token, ca_certificate) = session(
        "no-downstream-alpn",
        "repo-no-downstream-alpn",
        WorkspaceEndpoint::Tcp(endpoint),
        34,
        1,
        WorkspacePolicy {
            grants: vec![grant("downstream.test", port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install downstream no-ALPN session");
    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    stream
        .write_all(
            format!(
                "CONNECT downstream.test:{port} HTTP/1.1\r\nHost: downstream.test:{port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("write CONNECT");
    let head = read_response_head(&mut stream).await;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    let mut roots = RootCertStore::empty();
    roots.add(ca_certificate).expect("trust workspace CA");
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = ServerName::try_from("downstream.test".to_owned()).expect("server name");
    let mut tls = TlsConnector::from(Arc::new(client))
        .connect(server_name, stream)
        .await
        .expect("TLS handshake without ALPN");
    assert_eq!(tls.get_ref().1.alpn_protocol(), None);
    let _ = tls
        .write_all(
            format!(
                "GET /allowed HTTP/1.1\r\nHost: downstream.test:{port}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await;
    let mut response = Vec::new();
    let result = timeout(Duration::from_secs(1), tls.read_to_end(&mut response))
        .await
        .expect("gateway closes missing-ALPN transport");
    assert!(
        result.is_ok()
            || matches!(
                &result,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                    )
            ),
        "unexpected missing-ALPN close result: {result:?}"
    );
    assert!(response.is_empty(), "gateway silently selected HTTP/1.1");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "upstream connector ran despite rejected downstream ALPN"
    );
    gateway
        .drain()
        .await
        .expect("drain downstream no-ALPN gateway");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn h2_session_cancellation_closes_stream_and_is_audited() {
    let (upstream_port, upstream_tls, gate, _captured, upstream_task) =
        h2_tls_fixture("cancel-h2.test").await;
    let (negotiated, _negotiated_rx) = mpsc::channel(1);
    let (audit_tx, mut audit_rx) = mpsc::channel(16);
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(VerifiedTlsConnector {
            tls: upstream_tls,
            negotiated,
        }),
        Arc::new(ChannelAudit(audit_tx)),
    )
    .await;
    let (installed, token, ca_certificate) = session(
        "cancel-h2",
        "repo-cancel-h2",
        WorkspaceEndpoint::Tcp(endpoint),
        35,
        1,
        WorkspacePolicy {
            grants: vec![grant("cancel-h2.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install cancellation session");
    let (mut sender, downstream_connection) = h2_intercept_client(
        endpoint,
        &token,
        "cancel-h2.test",
        upstream_port,
        ca_certificate,
    )
    .await;
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("https://cancel-h2.test:{upstream_port}/allowed"))
        .header(header::HOST, format!("cancel-h2.test:{upstream_port}"))
        .body(Empty::<Bytes>::new())
        .expect("cancellation request");
    let response = sender
        .send_request(request)
        .await
        .expect("cancellation response");
    let mut body = response.into_body();
    let mut received = 0usize;
    while received < 24 * 1024 {
        let frame = timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("first cancellation chunk timeout")
            .expect("first cancellation chunk ended early")
            .expect("first cancellation data");
        received += frame.data_ref().map_or(0, Bytes::len);
    }
    assert_eq!(received, 24 * 1024);
    gateway
        .handle()
        .remove("cancel-h2", 1)
        .await
        .expect("remove h2 session");
    let terminal = timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("cancelled h2 body did not terminate");
    assert!(
        matches!(terminal, None | Some(Err(_))),
        "cancelled h2 body produced more data"
    );
    let saw_cancelled_connect = timeout(Duration::from_secs(2), async {
        while let Some(event) = audit_rx.recv().await {
            if event.kind == AuditKind::Connect
                && event.status == AuditStatus::Cancelled
                && event.classification.as_deref() == Some("session-rotated")
            {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    assert!(saw_cancelled_connect, "missing cancelled h2 CONNECT audit");
    gate.notify_waiters();
    drop(sender);
    gateway.drain().await.expect("drain cancelled h2 gateway");
    let _ = timeout(Duration::from_secs(1), downstream_connection).await;
    let _ = timeout(Duration::from_secs(1), upstream_task).await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn h2_audit_failure_hard_stops_the_negotiated_connection() {
    let calls = Arc::new(AtomicUsize::new(0));
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(CountingFailConnector {
            calls: Arc::clone(&calls),
        }),
        Arc::new(FailingAudit),
    )
    .await;
    let port = 443;
    let (installed, token, ca_certificate) = session(
        "h2-audit-stop",
        "repo-h2-audit-stop",
        WorkspaceEndpoint::Tcp(endpoint),
        36,
        1,
        WorkspacePolicy {
            grants: vec![grant("audit-h2.test", port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install h2 audit session");
    let (mut sender, connection) =
        h2_intercept_client(endpoint, &token, "audit-h2.test", port, ca_certificate).await;
    let malformed = Request::builder()
        .version(Version::HTTP_2)
        .uri("https://other.test/allowed")
        .header(header::HOST, "audit-h2.test")
        .body(Empty::<Bytes>::new())
        .expect("mismatched audit request");
    if let Ok(response) = sender.send_request(malformed).await {
        assert!(
            response.status() == StatusCode::BAD_REQUEST
                || response.status() == StatusCode::SERVICE_UNAVAILABLE
        );
    }
    timeout(Duration::from_secs(1), connection)
        .await
        .expect("audit hard-stop left h2 connection running")
        .expect("h2 connection task join")
        .expect_err("audit hard-stop unexpectedly completed h2 cleanly");
    assert!(gateway.handle().status().await.expect("status").draining);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "upstream connector ran before malformed h2 request was rejected"
    );
    await_reclaimed(&gateway).await;
    drop(gateway);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn direct_https_proxy_uses_negotiated_upstream_h2() {
    let (upstream_port, upstream_tls, gate, mut captured, upstream_task) =
        h2_tls_fixture("direct-h2.test").await;
    let (negotiated, mut negotiated_rx) = mpsc::channel(1);
    let endpoint = free_endpoint();
    let gateway = gateway(
        test_config(),
        Arc::new(NoCredentials),
        Arc::new(VerifiedTlsConnector {
            tls: upstream_tls,
            negotiated,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (installed, token, _) = session(
        "direct-h2",
        "repo-direct-h2",
        WorkspaceEndpoint::Tcp(endpoint),
        37,
        1,
        WorkspacePolicy {
            grants: vec![grant("direct-h2.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    gateway
        .handle()
        .install(installed)
        .await
        .expect("install direct h2 session");
    gate.notify_one();
    let response = proxy_request(
        endpoint,
        format!(
            "GET https://direct-h2.test:{upstream_port}/allowed HTTP/1.1\r\nHost: direct-h2.test:{upstream_port}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
        ),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert_eq!(
        negotiated_rx.recv().await.expect("direct upstream ALPN"),
        Some(b"h2".to_vec())
    );
    assert_eq!(
        captured.recv().await.expect("direct h2 request"),
        format!("https://direct-h2.test:{upstream_port}/allowed")
    );
    gateway.drain().await.expect("drain direct h2 gateway");
    timeout(Duration::from_secs(1), upstream_task)
        .await
        .expect("direct h2 upstream timeout")
        .expect("direct h2 upstream task");
}
