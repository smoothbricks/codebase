use super::*;

use bytes::Bytes;
use cowshed_gateway::{
    ControlError, ControlFailureCode, CredentialProtocol, GatewayControlClient, GatewayHandle,
    GatewayLimits, MirrorRoute,
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
use rcgen::Issuer;
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{PrivatePkcs8KeyDer, ServerName},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::{Mutex, atomic::AtomicU16},
    task::{Context, Poll},
    time::Instant,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use zeroize::Zeroizing;
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
struct VerifiedTlsConnector {
    tls: Arc<ClientConfig>,
    negotiated: mpsc::Sender<Option<Vec<u8>>>,
}

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

/// A registry credential scoped to one namespace path, the shape `cowshed credential add` writes.
///
/// It answers for its origin whatever the request path is, exactly as the platform store does:
/// the store is keyed by origin, and whether the path is in scope is the record's own business
/// through `validate_for`. That is what makes the refusal cases below meaningful.
struct ScopedRegistryCredential {
    repo_id: String,
    origin: String,
    path_prefix: String,
    value: String,
}

#[async_trait]
impl CredentialProvider for ScopedRegistryCredential {
    async fn lookup(
        &self,
        _query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError> {
        Ok(Some(CredentialRecord {
            repo_id: self.repo_id.clone(),
            protocol: CredentialProtocol::Generic,
            origin: self.origin.clone(),
            methods: BTreeSet::from(["GET".to_owned(), "HEAD".to_owned()]),
            path_prefixes: vec![self.path_prefix.clone()],
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

const PORT_BLOCKS: u16 = (cowshed_gateway::MACOS_PORT_MAX - cowshed_gateway::MACOS_PORT_MIN + 1)
    / cowshed_gateway::NEW_PORT_BLOCK_SIZE;

/// The blocks this process holds, by base port: a listener on each block's base+1. A static is
/// never dropped, so a claim outlives every session its test installs and goes back to the
/// kernel when the process exits, after a panic too, unless [`release_claim`] returns it first.
static CLAIMS: Mutex<BTreeMap<u16, std::net::TcpListener>> = Mutex::new(BTreeMap::new());

/// Claim a gateway block in the kernel and return its base as the endpoint to try.
///
/// A PID modulo the block count is not unique, and file locks under TMPDIR are not shared
/// across test runners with different temp roots. The claim is a listener on the block's next
/// port instead, held for this process's lifetime: every runner sees it, independent of its
/// filesystem, and a second claimant skips the block.
///
/// The base itself is not probed. A probe has to let go of the port before the gateway binds
/// it, and every allocator on the host binds the lowest free block's base first, so a released
/// base can be anyone's by then. [`install_claimed`] makes the gateway's own bind the probe.
fn free_endpoint() -> SocketAddr {
    static NEXT_BLOCK: AtomicU16 = AtomicU16::new(0);
    let seed = (std::process::id() % u32::from(PORT_BLOCKS)) as u16;
    for _ in 0..PORT_BLOCKS {
        let step = NEXT_BLOCK.fetch_add(1, Ordering::Relaxed);
        let index = ((u32::from(seed) + u32::from(step)) % u32::from(PORT_BLOCKS)) as u16;
        let base = cowshed_gateway::MACOS_PORT_MIN + index * cowshed_gateway::NEW_PORT_BLOCK_SIZE;
        let Ok(claim) = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, base + 1)) else {
            continue;
        };
        CLAIMS.lock().expect("port claims").insert(base, claim);
        return SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), base);
    }
    panic!("no free macOS gateway port block in {PORT_BLOCKS} candidates");
}

/// A claimed block whose base this process binds too, for a test that needs an endpoint the
/// gateway must find occupied. The listener is the probe itself, kept rather than released.
fn occupied_endpoint() -> (SocketAddr, std::net::TcpListener) {
    for _ in 0..PORT_BLOCKS {
        let endpoint = free_endpoint();
        match std::net::TcpListener::bind(endpoint) {
            Ok(listener) => return (endpoint, listener),
            Err(_) => release_claim(endpoint),
        }
    }
    panic!("no macOS gateway port block whose base this test could bind");
}

fn release_claim(endpoint: SocketAddr) {
    CLAIMS.lock().expect("port claims").remove(&endpoint.port());
}

/// The session endpoint for a block [`free_endpoint`] claimed.
fn block_endpoint(address: SocketAddr) -> WorkspaceEndpoint {
    WorkspaceEndpoint::Tcp {
        address,
        block_size: cowshed_gateway::NEW_PORT_BLOCK_SIZE,
    }
}

/// An install refusal that is the kernel declining to bind the session's endpoint.
trait EndpointRefusal: std::fmt::Debug {
    fn address_in_use(&self) -> bool;
}

impl EndpointRefusal for GatewayError {
    fn address_in_use(&self) -> bool {
        matches!(self, GatewayError::Io(error) if error.kind() == io::ErrorKind::AddrInUse)
    }
}

impl EndpointRefusal for ControlError {
    fn address_in_use(&self) -> bool {
        matches!(
            self,
            ControlError::Rejected {
                code: ControlFailureCode::AddressInUse,
                ..
            }
        )
    }
}

/// `session` at another endpoint. Installing consumes a session, so [`install_claimed`] keeps
/// the caller's and installs copies of it.
fn session_at(session: &WorkspaceSession, endpoint: WorkspaceEndpoint) -> WorkspaceSession {
    WorkspaceSession {
        workspace_id: session.workspace_id.clone(),
        repo_id: session.repo_id.clone(),
        revision: session.revision,
        endpoint,
        token: session.token.clone(),
        ca: WorkspaceCa {
            certificate_pem: session.ca.certificate_pem.clone(),
            private_key_pem: session.ca.private_key_pem.clone(),
        },
        policy: session.policy.clone(),
    }
}

/// Install `session`, starting at the claimed block its endpoint names, and return the endpoint
/// it serves. The gateway's own bind is the probe, so no window separates the check from the
/// use: when the kernel refuses the base (a live workspace's gateway holds it, or an allocator
/// is passing over it), the claim goes back and the session moves to the next claimed block,
/// as the runtime's allocator walks past a block it cannot bind.
async fn install_claimed<F, E>(
    session: WorkspaceSession,
    mut install: impl FnMut(WorkspaceSession) -> F,
) -> SocketAddr
where
    F: Future<Output = Result<(), E>>,
    E: EndpointRefusal,
{
    let WorkspaceEndpoint::Tcp { mut address, .. } = session.endpoint else {
        panic!("a macOS gateway session listens on a TCP port block");
    };
    for _ in 0..PORT_BLOCKS {
        match install(session_at(&session, block_endpoint(address))).await {
            Ok(()) => return address,
            Err(error) if error.address_in_use() => {
                release_claim(address);
                address = free_endpoint();
            }
            Err(error) => panic!("install session at {address}: {error:?}"),
        }
    }
    panic!("no claimed macOS gateway port block whose base the gateway could bind");
}

/// [`install_claimed`] through the gateway's own handle.
async fn install_in_free_block(gateway: &GatewayHandle, session: WorkspaceSession) -> SocketAddr {
    install_claimed(session, move |session| gateway.install(session)).await
}

/// The claim and the gateway's bind arbitrate one block between them: a second claimant skips
/// it, and a base the kernel refuses moves the session on and returns that block's claim.
#[tokio::test]
async fn gateway_port_claim_is_a_live_kernel_reservation() {
    let endpoint = free_endpoint();
    let error = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, endpoint.port() + 1))
        .expect_err("a second claimant cannot acquire the live block");
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    release_claim(endpoint);

    let (occupied, _held) = occupied_endpoint();
    // A connection queued, unaccepted, on the occupied block's claim is reset when that claim
    // closes: the kernel's own word that it was released, whoever binds the port afterwards.
    let mut queued = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, occupied.port() + 1))
        .expect("connect to the occupied block's claim");
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
    let (session, _token, _) = session(
        "claimed",
        "owner/repo-claimed",
        block_endpoint(occupied),
        3,
        1,
        WorkspacePolicy::default(),
    );
    let served = install_in_free_block(&gateway.handle(), session).await;
    assert_ne!(served, occupied, "the session moved past the refused base");
    let status = gateway.handle().status().await.expect("gateway status");
    assert_eq!(status.sessions[0].endpoint, served.to_string());
    let reset = std::io::Read::read(&mut queued, &mut [0])
        .expect_err("a connection queued on a closed claim is reset");
    assert_eq!(reset.kind(), io::ErrorKind::ConnectionReset);
    gateway.drain().await.expect("drain gateway");
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
struct ChannelBody {
    receiver: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
}

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

/// A gateway an audit failure closed is either still draining its in-flight work — and says so,
/// naming the failure — or has already stopped. It never answers as if it were serving.
async fn assert_refusing_after_audit_failure(gateway: &Gateway) {
    match gateway.handle().status().await {
        Ok(status) => {
            assert!(status.draining, "a fail-closed gateway reports draining");
            assert!(
                status
                    .drain_cause
                    .as_deref()
                    .is_some_and(|cause| cause.contains("audit")),
                "a draining gateway names its cause: {:?}",
                status.drain_cause
            );
        }
        Err(GatewayError::Stopped) => {}
        Err(other) => panic!("unexpected gateway status failure: {other}"),
    }
}

/// Once its drain completes, a fail-closed gateway stops with the audit failure, so the daemon
/// exits and is restarted instead of draining forever.
async fn assert_stops_with_audit_failure(gateway: &mut Gateway) {
    let stopped = timeout(Duration::from_secs(5), gateway.stopped())
        .await
        .expect("a fail-closed gateway stops once its drain completes");
    assert!(
        matches!(stopped, Err(GatewayError::Audit(_))),
        "the gateway stops with the audit failure that closed it: {stopped:?}"
    );
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
    let (upstream_port, mut captured, _upstream) = http_fixture(2, None).await;
    let endpoint = free_endpoint();
    let (audit_tx, mut audit_rx) = mpsc::channel(8);
    let (config, _cache) = test_config();
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
    let policy = WorkspacePolicy {
        grants: vec![grant("allowed.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let (session, token, _) = session(
        "raven",
        "owner/repo-one",
        block_endpoint(endpoint),
        7,
        1,
        policy,
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;

    let malformed = absolute_request(
        "allowed.test",
        upstream_port,
        "not-base64!",
        "/allowed/item",
    );
    let response = proxy_request(endpoint, malformed).await;
    assert!(response.starts_with("HTTP/1.1 407"), "{response}");
    assert!(
        response
            .to_ascii_lowercase()
            .contains("proxy-authenticate: basic realm=\"cowshed\""),
        "{response}"
    );

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
    assert!(forwarded.contains("traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-"));
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
    assert_eq!(unauthorized.http_status, Some(407));
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
    assert_eq!(
        invalid.classification.as_deref(),
        Some("invalid-trace-context")
    );
    assert!(invalid.parent_span_id.is_none());
    assert!(invalid.tracestate.is_none());
    assert!(
        unauthorized.sequence < denied.sequence
            && denied.sequence < allowed.sequence
            && allowed.sequence < invalid.sequence
    );
    gateway.drain().await.expect("drain gateway");
}

/// The `HTTP_PROXY` userinfo path a standard client actually takes: `Proxy-Authorization: Basic`
/// on the first CONNECT, no challenge round trip, and a terminal, immediate 407 when it is absent.
#[tokio::test]
async fn connect_accepts_basic_proxy_credentials_and_challenges_without_them() {
    let (upstream_port, _captured, _upstream) = http_fixture(0, None).await;
    let endpoint = free_endpoint();
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
    let policy = WorkspacePolicy {
        grants: vec![grant("basic.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let (session, token, _) = session(
        "raven",
        "owner/repo-basic",
        block_endpoint(endpoint),
        11,
        1,
        policy,
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;

    let connect = |credential: Option<String>| {
        let header = credential
            .map(|value| format!("Proxy-Authorization: {value}\r\n"))
            .unwrap_or_default();
        format!(
            "CONNECT basic.test:{upstream_port} HTTP/1.1\r\nHost: basic.test:{upstream_port}\r\n{header}\r\n"
        )
    };

    for user in ["cowshed", "anything"] {
        let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
        stream
            .write_all(connect(Some(basic_credential(user, &token))).as_bytes())
            .await
            .expect("write authenticated CONNECT");
        let head = read_response_head(&mut stream).await;
        assert!(head.starts_with("HTTP/1.1 200"), "{user}: {head}");
    }

    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    stream
        .write_all(connect(Some(basic_credential("cowshed", "wrong-token"))).as_bytes())
        .await
        .expect("write wrong-token CONNECT");
    let head = read_response_head(&mut stream).await;
    assert!(head.starts_with("HTTP/1.1 407"), "{head}");

    // Fail fast: the rejection is one round trip, so a client aborts instead of waiting out a
    // tunnel that will never open.
    let mut stream = TcpStream::connect(endpoint).await.expect("connect gateway");
    stream
        .write_all(connect(None).as_bytes())
        .await
        .expect("write unauthenticated CONNECT");
    let head = timeout(Duration::from_secs(1), read_response_head(&mut stream))
        .await
        .expect("unauthenticated CONNECT must answer immediately");
    assert!(head.starts_with("HTTP/1.1 407"), "{head}");
    assert!(
        head.to_ascii_lowercase()
            .contains("proxy-authenticate: basic realm=\"cowshed\""),
        "{head}"
    );
    gateway.drain().await.expect("drain gateway");
}

/// The contract with the clients cowshed cannot configure, exercised by one of them.
///
/// libcurl is what cargo's registry downloads run on, so what curl does here is what cargo does:
/// take `user:password` out of the proxy URL, send `Proxy-Authorization: Basic` on the first
/// CONNECT, and tunnel — no challenge round trip, and no way to be told cowshed's `Bearer`
/// spelling. The tunnel it opens is the intercepted path a sandbox walks: CONNECT, minted leaf,
/// request authorization, upstream fetch.
#[tokio::test(start_paused = true)]
async fn curl_tunnels_with_proxy_userinfo_and_fails_fast_without_it() {
    let (upstream_port, _captured, _upstream) = http_fixture(1, None).await;
    let endpoint = free_endpoint();
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
    let policy = WorkspacePolicy {
        grants: vec![grant("secure.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let (session, token, _) = session(
        "raven",
        "owner/repo-curl",
        block_endpoint(endpoint),
        13,
        1,
        policy,
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;
    let target = format!("https://secure.test:{upstream_port}/allowed/item");
    // curl's latency belongs to the host scheduler, so it must not race the gateway's deadlines:
    // a loaded host once held curl's TLS 1.3 Finished past `test_config()`'s 2s handshake bound,
    // the gateway dropped the handshake, and curl, already done with its side, met a reset.
    // The gateway's deadlines all run on this runtime's paused clock, and a running blocking task
    // inhibits auto-advance, so the clock stands still while curl runs and curl reports only what
    // the gateway answered. A hung curl is the harness's slow-timeout to bound, not ours.
    let curl = |proxy: String| {
        let target = target.clone();
        async move {
            tokio::task::spawn_blocking(move || {
                std::process::Command::new("/usr/bin/curl")
                    .args([
                        "-sS",
                        // The system curl's TLS backend is Secure Transport, which takes trust
                        // anchors only from a keychain, never from `--cacert`. The workspace CA
                        // chain is covered by the rustls intercept tests; what only a real client
                        // can prove is the credential on the wire, and that happens before any TLS.
                        "--insecure",
                        "-o",
                        "/dev/null",
                        "-w",
                        "%{http_code}",
                        "-x",
                        &proxy,
                        &target,
                    ])
                    .output()
            })
            .await
            .expect("join curl")
            .expect("run curl")
        }
    };

    let port = endpoint.port();
    let authenticated = curl(format!("http://cowshed:{token}@127.0.0.1:{port}")).await;
    assert!(
        authenticated.status.success(),
        "curl failed: {}",
        String::from_utf8_lossy(&authenticated.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&authenticated.stdout), "200");

    // No credential is a terminal answer, not a stall: curl reports the proxy's status and exits
    // rather than waiting out a tunnel that will never open. With the gateway's clock frozen, no
    // deadline can produce that answer either: a 407 here comes from the CONNECT itself.
    let anonymous = curl(format!("http://127.0.0.1:{port}")).await;
    assert!(!anonymous.status.success());
    let reported = String::from_utf8_lossy(&anonymous.stderr);
    assert!(reported.contains("407"), "{reported}");
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn node_native_fetch_uses_the_workspace_proxy_environment() {
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let endpoint = free_endpoint();
    let (audit_tx, mut audit_rx) = mpsc::channel(8);
    let (config, _cache) = test_config();
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
    let (installed, token, ca_certificate) = session(
        "node-fetch",
        "owner/repo-node-fetch",
        block_endpoint(endpoint),
        35,
        1,
        WorkspacePolicy {
            grants: vec![grant("node-fetch.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
    let root = secure_fixture_dir(&format!("cowshed-node-fetch-{}", std::process::id()));
    let ca_path = root.path().join("ca.pem");
    std::fs::write(
        &ca_path,
        format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
            base64::engine::general_purpose::STANDARD.encode(ca_certificate.as_ref())
        ),
    )
    .expect("write workspace trust anchor");
    let proxy = format!("http://cowshed:{token}@127.0.0.1:{}", endpoint.port());
    // This hostname does not resolve. Native fetch can reach it only through the authorized
    // workspace proxy, without a custom Agent, TLS bypass, or a package-specific patch.
    let mut command = tokio::process::Command::new("node");
    command
        .args([
            "--input-type=module",
            "-e",
            "const response = await fetch(process.argv[1], {headers: {connection: 'close'}}); \
             if (response.status !== 200) throw new Error(`HTTP ${response.status}`); \
             console.log(await response.text());",
            &format!("https://node-fetch.test:{upstream_port}/allowed/node"),
        ])
        .current_dir(root.path())
        .env("HTTP_PROXY", &proxy)
        .env("HTTPS_PROXY", &proxy)
        .env("http_proxy", &proxy)
        .env("https_proxy", &proxy)
        .env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1")
        .env("NODE_USE_ENV_PROXY", "1")
        .env("NODE_EXTRA_CA_CERTS", &ca_path)
        .env_remove("NODE_TLS_REJECT_UNAUTHORIZED")
        .env_remove("NODE_OPTIONS")
        .kill_on_drop(true);
    let output = command.output().await.expect("run real Node");
    assert!(
        output.status.success(),
        "native Node fetch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "ok");
    let forwarded = captured.recv().await.expect("capture Node request");
    assert!(
        forwarded.starts_with("GET /allowed/node HTTP/1.1"),
        "{forwarded}"
    );
    assert!(
        !forwarded
            .to_ascii_lowercase()
            .contains("proxy-authorization")
    );
    assert!(!forwarded.contains(&token));
    gateway.drain().await.expect("drain Node workspace");
    let mut saw_request = false;
    while let Ok(event) = audit_rx.try_recv() {
        if event.method.as_deref() == Some("GET") && event.path.as_deref() == Some("/allowed/node")
        {
            assert_eq!(event.kind, AuditKind::Intercept);
            assert_eq!(event.http_status, Some(200));
            saw_request = true;
        }
    }
    assert!(saw_request, "the workspace gateway audits native fetch");
}

#[tokio::test]
async fn endpoint_identity_precedes_token_authentication() {
    let (upstream_port, _captured, _upstream) = http_fixture(1, None).await;
    let endpoint_a = free_endpoint();
    let endpoint_b = free_endpoint();
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
    let policy_a = WorkspacePolicy {
        grants: vec![grant("isolated.test", upstream_port)],
        mirrors: Vec::new(),
    };
    let policy_b = policy_a.clone();
    let (session_a, token_a, _) = session(
        "alpha",
        "owner/repo-a",
        block_endpoint(endpoint_a),
        1,
        1,
        policy_a,
    );
    let (session_b, token_b, _) = session(
        "bravo",
        "owner/repo-b",
        block_endpoint(endpoint_b),
        2,
        1,
        policy_b,
    );
    install_in_free_block(&gateway.handle(), session_a).await;
    let endpoint_b = install_in_free_block(&gateway.handle(), session_b).await;

    let wrong_endpoint = proxy_request(
        endpoint_b,
        absolute_request("isolated.test", upstream_port, &token_a, "/allowed").to_owned(),
    )
    .await;
    assert!(
        wrong_endpoint.starts_with("HTTP/1.1 407"),
        "{wrong_endpoint}"
    );

    // The Basic spelling is the same credential, not a weaker one: another workspace's token is
    // rejected through it exactly as it is through Bearer.
    let wrong_basic = proxy_request(
        endpoint_b,
        basic_absolute_request(
            "isolated.test",
            upstream_port,
            &basic_credential("cowshed", &token_a),
            "/allowed",
        ),
    )
    .await;
    assert!(wrong_basic.starts_with("HTTP/1.1 407"), "{wrong_basic}");

    let own_endpoint = proxy_request(
        endpoint_b,
        absolute_request("isolated.test", upstream_port, &token_b, "/allowed").to_owned(),
    )
    .await;
    assert!(own_endpoint.starts_with("HTTP/1.1 200"), "{own_endpoint}");
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn eight_intercept_tunnels_do_not_starve_their_own_registry_requests() {
    let (upstream_port, mut captured, _upstream) = http_fixture(8, None).await;
    let endpoint = free_endpoint();
    let (mut config, _cache) = test_config();
    config.limits.workspace_active = 8;
    config.limits.global_active = 8;
    config.limits.origin_active = 8;
    let (audit_tx, mut audit_rx) = mpsc::channel(64);
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
    let (installed, token, certificate) = session(
        "concurrent-registry",
        "owner/concurrent-registry",
        block_endpoint(endpoint),
        9,
        1,
        WorkspacePolicy {
            grants: vec![grant("registry.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
    let mut roots = RootCertStore::empty();
    roots.add(certificate).expect("trust workspace CA");
    let client = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let mut tunnels = Vec::new();
    // Establish every CONNECT before sending a GET: the old single permit pool deadlocked here.
    for _ in 0..8 {
        let mut stream = TcpStream::connect(endpoint)
            .await
            .expect("connect registry proxy");
        stream.write_all(format!(
            "CONNECT registry.test:{upstream_port} HTTP/1.1\r\nHost: registry.test:{upstream_port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
        ).as_bytes()).await.expect("write registry CONNECT");
        let head = timeout(Duration::from_secs(2), read_response_head(&mut stream))
            .await
            .expect("bounded CONNECT admission");
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        tunnels.push(
            TlsConnector::from(Arc::clone(&client))
                .connect(
                    ServerName::try_from("registry.test").expect("registry name"),
                    stream,
                )
                .await
                .expect("intercepted registry TLS"),
        );
    }
    let mut requests = tokio::task::JoinSet::new();
    for (index, mut tls) in tunnels.into_iter().enumerate() {
        requests.spawn(async move {
            tls.write_all(format!(
                "GET /allowed/{index} HTTP/1.1\r\nHost: registry.test:{upstream_port}\r\nConnection: close\r\n\r\n"
            ).as_bytes()).await.expect("write inner registry request");
            let mut response = Vec::new();
            tls.read_to_end(&mut response).await.expect("read inner registry response");
            assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));
        });
    }
    timeout(Duration::from_secs(2), async {
        while let Some(result) = requests.join_next().await {
            result.expect("inner registry request finishes without client retry");
        }
    })
    .await
    .expect("inner requests never wait for the containing CONNECT to close");
    for _ in 0..8 {
        assert!(
            captured
                .recv()
                .await
                .expect("captured inner GET")
                .starts_with("GET /allowed/")
        );
    }
    gateway.drain().await.expect("drain both capacity classes");
    let mut completed_tunnels = 0;
    let mut completed_requests = 0;
    while let Ok(event) = audit_rx.try_recv() {
        if event.status == AuditStatus::Completed {
            match event.kind {
                AuditKind::Connect => completed_tunnels += 1,
                AuditKind::Intercept => completed_requests += 1,
                _ => {}
            }
        }
    }
    assert_eq!(completed_tunnels, 8);
    assert_eq!(completed_requests, 8);
}

#[tokio::test]
async fn native_registry_requests_use_one_admitted_proxy_path_and_cache() {
    let (upstream_port, mut captured, _upstream) = http_fixture(2, None).await;
    let (audit_tx, mut audit_rx) = mpsc::channel(32);
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
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
    let policy = WorkspacePolicy {
        grants: vec![grant("mirror.test", upstream_port)],
        mirrors: vec![
            MirrorRoute::new(
                &format!("https://mirror.test:{upstream_port}"),
                vec!["/allowed".to_owned(), "/@scope/".to_owned()],
                false,
            )
            .expect("registry route"),
        ],
    };
    let (installed, token, _) = session(
        "registry",
        "owner/repo-registry",
        block_endpoint(endpoint),
        9,
        1,
        policy,
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
    for path in ["/allowed", "/@scope%2fpkg"] {
        let request = format!(
            "GET https://mirror.test:{upstream_port}{path} HTTP/1.1\r\nHost: mirror.test:{upstream_port}\r\nAccept: application/vnd.npm.install-v1+json\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
        );
        let response = proxy_request(endpoint, request.clone()).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let forwarded = captured.recv().await.expect("captured registry request");
        assert!(
            forwarded.starts_with(&format!("GET {path} HTTP/1.1")),
            "{forwarded}"
        );
        assert!(
            !forwarded.contains(&token),
            "workspace proxy token reached registry"
        );
        let warm = proxy_request(endpoint, request).await;
        assert!(warm.starts_with("HTTP/1.1 200"), "{warm}");
    }
    for path in [
        "/npm/allowed",
        "/cargo/config.json",
        "/go/example.com/@v/list",
    ] {
        let response = proxy_request(endpoint, format!(
            "GET {path} HTTP/1.1\r\nHost: {endpoint}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
        )).await;
        assert!(
            response.starts_with("HTTP/1.1 400"),
            "retired reverse endpoint is not served: {response}"
        );
    }
    gateway.drain().await.expect("drain gateway");
    let mut filled = 0;
    let mut hit = 0;
    while let Ok(event) = audit_rx.try_recv() {
        if event.kind == AuditKind::Npm {
            match event.mirror_cache_status {
                Some(MirrorCacheStatus::Filled) => filled += 1,
                Some(MirrorCacheStatus::Hit) => hit += 1,
                _ => {}
            }
        }
    }
    assert_eq!(filled, 2);
    assert_eq!(hit, 2);
}

/// A client holding the registry's manifest revalidates it with the ETag it stored, weak when the
/// registry compressed it for that client. The gateway answers `304` itself, from the fill it just
/// published and then from its cache, and never forwards the validator: the registry would answer
/// the validator with a `304` the gateway has no body for. Hyper never polls a `304`'s empty body,
/// so each answer must still be audited as completed, not as a dropped response.
#[tokio::test]
async fn a_conditional_registry_request_is_answered_304_by_the_gateway_and_audited_completed() {
    let registry = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind registry fixture");
    let upstream_port = registry.local_addr().expect("registry address").port();
    // One connection: everything after the fill is served from the cache.
    let registry = tokio::spawn(async move {
        let (mut stream, _) = registry.accept().await.expect("accept registry request");
        let request = read_headers(&mut stream).await;
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nETag: \"v1\"\r\nCache-Control: public, max-age=300\r\nConnection: close\r\n\r\nok",
            )
            .await
            .expect("write registry response");
        request
    });
    let (audit_tx, mut audit_rx) = mpsc::channel(32);
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
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
    let policy = WorkspacePolicy {
        grants: vec![grant("mirror.test", upstream_port)],
        mirrors: vec![
            MirrorRoute::new(
                &format!("https://mirror.test:{upstream_port}"),
                vec!["/allowed".to_owned()],
                false,
            )
            .expect("registry route"),
        ],
    };
    let (installed, token, _) = session(
        "conditional",
        "owner/repo-conditional",
        block_endpoint(endpoint),
        9,
        1,
        policy,
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
    let conditional = |validator: &str| {
        format!(
            "GET https://mirror.test:{upstream_port}/allowed HTTP/1.1\r\nHost: mirror.test:{upstream_port}\r\nAccept: application/vnd.npm.install-v1+json\r\nIf-None-Match: {validator}\r\nProxy-Authorization: Bearer {token}\r\nConnection: close\r\n\r\n"
        )
    };

    for validator in ["W/\"v1\"", "\"v1\""] {
        let response = proxy_request(endpoint, conditional(validator)).await;
        assert!(
            response.starts_with("HTTP/1.1 304"),
            "{validator}: {response}"
        );
        assert!(
            response.to_ascii_lowercase().contains("etag: \"v1\""),
            "{validator}: {response}"
        );
        assert!(response.ends_with("\r\n\r\n"), "{validator}: {response}");
    }
    let changed = proxy_request(endpoint, conditional("W/\"v0\"")).await;
    assert!(changed.starts_with("HTTP/1.1 200"), "{changed}");
    assert!(changed.ends_with("\r\n\r\nok"), "{changed}");

    let forwarded = registry
        .await
        .expect("registry fixture")
        .to_ascii_lowercase();
    assert!(
        !forwarded.contains("if-none-match"),
        "the client's validator reached the registry: {forwarded}"
    );
    gateway.drain().await.expect("drain gateway");
    let mut answers = Vec::new();
    while let Ok(event) = audit_rx.try_recv() {
        if event.kind == AuditKind::Npm {
            answers.push((event.status, event.http_status, event.mirror_cache_status));
        }
    }
    assert_eq!(
        answers,
        [
            (
                AuditStatus::Completed,
                Some(304),
                Some(MirrorCacheStatus::Filled)
            ),
            (
                AuditStatus::Completed,
                Some(304),
                Some(MirrorCacheStatus::Hit)
            ),
            (
                AuditStatus::Completed,
                Some(200),
                Some(MirrorCacheStatus::Hit)
            ),
        ]
    );
}

#[tokio::test]
async fn intercepted_tarball_is_refused_before_its_last_bytes_escape_on_digest_mismatch() {
    use sha2::{Digest as _, Sha512};

    let expected = b"published tarball bytes";
    let tampered = b"tampered! tarball bytes";
    assert_eq!(expected.len(), tampered.len());
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind registry fixture");
    let upstream_port = listener.local_addr().expect("registry address").port();
    let packument = serde_json::to_vec(&serde_json::json!({
        "versions": {"1.0.0": {"dist": {
            "tarball": format!("https://mirror.test:{upstream_port}/pkg/-/pkg-1.0.0.tgz"),
            "integrity": format!("sha512-{}", base64::engine::general_purpose::STANDARD.encode(Sha512::digest(expected))),
            "size": expected.len()
        }}}
    })).expect("published packument");
    let (paths_tx, mut paths_rx) = mpsc::channel(2);
    let upstream = tokio::spawn(async move {
        for (path, content_type, bytes) in [
            (
                "/pkg",
                "application/vnd.npm.install-v1+json",
                packument.as_slice(),
            ),
            (
                "/pkg/-/pkg-1.0.0.tgz",
                "application/octet-stream",
                tampered.as_slice(),
            ),
        ] {
            let (mut stream, _) = listener.accept().await.expect("accept registry request");
            let request = read_headers(&mut stream).await;
            assert!(
                request.starts_with(&format!("GET {path} HTTP/1.1")),
                "{request}"
            );
            paths_tx.send(path).await.expect("record registry path");
            stream.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            ).as_bytes()).await.expect("write registry headers");
            stream.write_all(bytes).await.expect("write registry body");
        }
    });
    let (audit_tx, mut audit_rx) = mpsc::channel(16);
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
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
    let policy = WorkspacePolicy {
        grants: vec![grant("mirror.test", upstream_port)],
        mirrors: vec![
            MirrorRoute::new(
                &format!("https://mirror.test:{upstream_port}"),
                vec!["/pkg".to_owned()],
                false,
            )
            .expect("registry route"),
        ],
    };
    let (installed, token, certificate) = session(
        "tampered-registry",
        "owner/tampered-registry",
        block_endpoint(endpoint),
        9,
        1,
        policy,
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
    let mut stream = TcpStream::connect(endpoint).await.expect("connect proxy");
    stream.write_all(format!(
        "CONNECT mirror.test:{upstream_port} HTTP/1.1\r\nHost: mirror.test:{upstream_port}\r\nProxy-Authorization: Bearer {token}\r\n\r\n"
    ).as_bytes()).await.expect("write CONNECT");
    assert!(
        read_response_head(&mut stream)
            .await
            .starts_with("HTTP/1.1 200")
    );
    let mut roots = RootCertStore::empty();
    roots.add(certificate).expect("trust workspace CA");
    // Node's native registry client omits ALPN; the standard HTTP/1.1 fallback must work.
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut tls = TlsConnector::from(Arc::new(client))
        .connect(
            ServerName::try_from("mirror.test").expect("registry name"),
            stream,
        )
        .await
        .expect("intercept client TLS");
    tls.write_all(format!(
        "GET /pkg/-/pkg-1.0.0.tgz HTTP/1.1\r\nHost: mirror.test:{upstream_port}\r\nConnection: close\r\n\r\n"
    ).as_bytes()).await.expect("request frozen-install tarball");
    let mut received = Vec::new();
    let _closed_stream = timeout(Duration::from_secs(3), tls.read_to_end(&mut received))
        .await
        .expect("failed integrity closes stream without waiting for client timeout");
    assert!(
        !received
            .windows(tampered.len())
            .any(|bytes| bytes == tampered),
        "complete tampered content must never reach the client: {received:?}"
    );
    assert_eq!(paths_rx.recv().await, Some("/pkg"));
    assert_eq!(paths_rx.recv().await, Some("/pkg/-/pkg-1.0.0.tgz"));
    upstream.await.expect("registry fixture completes");
    gateway.drain().await.expect("drain gateway");
    let mut integrity_failure = false;
    while let Ok(event) = audit_rx.try_recv() {
        if event.kind == AuditKind::Npm && event.path.as_deref() == Some("/pkg/-/pkg-1.0.0.tgz") {
            assert_eq!(event.status, AuditStatus::Failed);
            integrity_failure = true;
        }
    }
    assert!(
        integrity_failure,
        "tampered intercepted download is auditable"
    );
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
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
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
        "owner/repo-opaque",
        block_endpoint(endpoint),
        3,
        1,
        policy,
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;
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
        repo_id: "owner/repo-secure".to_owned(),
        origin,
        value: "Bearer host-secret".to_owned(),
    });
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
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
        "owner/repo-secure",
        block_endpoint(endpoint),
        4,
        1,
        policy,
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;

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

/// The private-registry case end to end: bun's own URL shape reaches the registry with the
/// host-held credential attached, and the workspace never sees the secret.
#[tokio::test]
async fn a_scoped_packument_carries_the_held_credential_and_forwards_its_bytes_unchanged() {
    let (upstream_port, mut captured, _upstream) = http_fixture(1, None).await;
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
        Arc::new(ScopedRegistryCredential {
            repo_id: "owner/repo-registry".to_owned(),
            origin: format!("http://registry.test:{upstream_port}"),
            path_prefix: "/api/packages/owner/npm/".to_owned(),
            value: "Bearer host-held-registry-token".to_owned(),
        }),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (session, token, _ca) = session(
        "registry",
        "owner/repo-registry",
        block_endpoint(endpoint),
        9,
        1,
        WorkspacePolicy {
            grants: vec![grant("registry.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;

    // Exactly what bun 1.4 sends for a scoped packument: one encoded slash inside the package
    // segment, after the registry's own namespace path.
    let scoped = "/api/packages/owner/npm/@scope%2fpkg";
    let response = proxy_request(
        endpoint,
        absolute_request("registry.test", upstream_port, &token, scoped),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    let forwarded = captured.recv().await.expect("captured upstream request");
    assert!(
        forwarded.starts_with(&format!("GET {scoped} HTTP/1.1")),
        "upstream must see the client's own bytes, encoded slash included: {forwarded}"
    );
    assert!(
        forwarded
            .to_ascii_lowercase()
            .contains("authorization: bearer host-held-registry-token\r\n"),
        "{forwarded}"
    );
    gateway.drain().await.expect("drain gateway");
}

/// The refusals that make the admission above safe. Each one must end the request rather than
/// forward it without the credential: the upstream fixture accepts nothing here.
#[tokio::test]
async fn a_request_outside_the_credential_scope_is_refused_and_carries_no_credential() {
    let (upstream_port, mut captured, _upstream) = http_fixture(0, None).await;
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
        Arc::new(ScopedRegistryCredential {
            repo_id: "owner/repo-scope".to_owned(),
            origin: format!("http://scope.test:{upstream_port}"),
            path_prefix: "/api/packages/owner/npm/".to_owned(),
            value: "Bearer host-held-registry-token".to_owned(),
        }),
        Arc::new(LocalConnector {
            health: UpstreamHealth::Healthy,
            observed: None,
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (session, token, _ca) = session(
        "scope",
        "owner/repo-scope",
        block_endpoint(endpoint),
        10,
        1,
        WorkspacePolicy {
            grants: vec![grant("scope.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;

    // An encoded slash that would read as the admitted namespace once decoded. Upstream would
    // read a different resource, so the prefix is matched on raw bytes and this is out of scope.
    let disguised = "/api%2fpackages/owner/npm/pkg";
    let response = proxy_request(
        endpoint,
        absolute_request("scope.test", upstream_port, &token, disguised),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 502"), "{response}");

    // A neighbouring namespace on the same origin, plus traversal out of the admitted one.
    for refused in [
        "/api/packages/other/npm/pkg",
        "/api/packages/owner/npm/%2e%2e%2f%2e%2e%2fadmin",
    ] {
        let response = proxy_request(
            endpoint,
            absolute_request("scope.test", upstream_port, &token, refused),
        )
        .await;
        assert!(
            response.starts_with("HTTP/1.1 4") || response.starts_with("HTTP/1.1 5"),
            "{refused} must be refused: {response}"
        );
    }

    assert!(
        captured.try_recv().is_err(),
        "a refused request must never reach the registry"
    );
    gateway.drain().await.expect("drain gateway");
}

#[tokio::test]
async fn dead_upstream_fails_fast_without_connecting() {
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
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
        "owner/repo-offline",
        block_endpoint(endpoint),
        6,
        1,
        WorkspacePolicy {
            grants: vec![grant("offline.test", 443)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;
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
    let (mut config, _cache) = test_config();
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
        "owner/repo-limited",
        block_endpoint(endpoint),
        8,
        1,
        WorkspacePolicy {
            grants: vec![grant("queue.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), session).await;

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
    let (mut config, _cache) = test_config();
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
        "owner/repo-queue-timeout",
        block_endpoint(endpoint),
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
    let endpoint = install_in_free_block(&gateway.handle(), session).await;
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
#[tokio::test]
async fn control_start_failure_stops_and_joins_gateway_actor() {
    let (mut config, _cache) = test_config();
    let missing_parent = crate::fixture_dir::scratch_parent().join(format!(
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

/// The control socket parent is what a stranger would have to own in order to swap the socket out
/// from under the gateway, so both ways of losing that exclusivity are pinned here.
///
/// A group-writable parent lets another member of the group unlink the socket and bind their own;
/// a symlinked parent lets whoever owns the link retarget it after the check and before the bind.
/// Neither refusal may be traded away to make a fixture pass — a fixture that trips this check is
/// a fixture creating a directory no production parent is allowed to look like.
#[tokio::test]
async fn control_socket_parent_must_be_a_private_real_directory() {
    use std::os::unix::fs::PermissionsExt as _;

    async fn refusal(control: &std::path::Path) -> io::Error {
        let (mut config, _cache) = test_config();
        config.control_socket = Some(control.to_path_buf());
        let Err(error) = Gateway::start(
            config,
            Arc::new(NoCredentials),
            Arc::new(LocalConnector {
                health: UpstreamHealth::Healthy,
                observed: None,
            }),
            Arc::new(DiscardAudit),
        )
        .await
        else {
            panic!("insecure control socket parent must refuse the gateway");
        };
        let GatewayError::Io(error) = error else {
            panic!("expected an I/O refusal, got {error:?}");
        };
        assert!(
            !control.exists(),
            "the parent is rejected before the socket is bound, so {} must not exist",
            control.display()
        );
        error
    }

    // Deliberately terse: `sockaddr_un.sun_path` is 104 bytes on macOS. A descriptive name pushes
    // the socket past `SUN_LEN`, and then the bind refuses before the parent check is reached —
    // which would let this test keep passing for the wrong reason if the check under test were
    // ever removed.
    let root = secure_fixture_dir(&format!("cowshed-ctl-parent-{}", std::process::id()));

    let shared = root.path().join("shared");
    std::fs::create_dir(&shared).expect("create group-writable parent");
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o770))
        .expect("relax group-writable parent");
    let error = refusal(&shared.join("gateway.sock")).await;
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "control socket parent must be an owned, non-writable real directory"
    );

    let target = root.path().join("real");
    std::fs::create_dir(&target).expect("create symlink target");
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700))
        .expect("secure symlink target");
    let link = root.path().join("link");
    std::os::unix::fs::symlink(&target, &link).expect("link a private parent");
    let error = refusal(&link.join("gateway.sock")).await;
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "control socket parent must be an owned, non-writable real directory"
    );
    assert!(
        !target.join("gateway.sock").exists(),
        "the refusal must not bind through the link either"
    );
}

#[tokio::test]
async fn control_socket_is_local_authenticated_and_reports_status() {
    let root = secure_fixture_dir(&format!("cowshed-gateway-control-{}", std::process::id()));
    let control = root.path().join("gateway.sock");
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
    let client = GatewayControlClient::new(control.clone()).expect("control client");
    let endpoint = free_endpoint();
    let (session, _token, _) = session(
        "controlled",
        "owner/repo-controlled",
        block_endpoint(endpoint),
        42,
        1,
        WorkspacePolicy::default(),
    );
    let control_client = &client;
    install_claimed(session, move |session| async move {
        control_client.install(&session).await
    })
    .await;
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
}

#[tokio::test]
async fn revision_tombstone_and_rotation_preserve_authority() {
    let endpoint = free_endpoint();
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
    let make_session = |revision, endpoint| {
        session(
            "revision",
            "owner/repo-revision",
            block_endpoint(endpoint),
            revision as u8,
            revision,
            WorkspacePolicy::default(),
        )
        .0
    };
    let endpoint = install_in_free_block(&gateway.handle(), make_session(1, endpoint)).await;
    gateway
        .handle()
        .remove("revision", 1)
        .await
        .expect("remove revision one");
    assert!(matches!(
        gateway.handle().install(make_session(1, endpoint)).await,
        Err(GatewayError::StaleRevision)
    ));
    let endpoint = install_in_free_block(&gateway.handle(), make_session(2, endpoint)).await;

    let (occupied_endpoint, occupied) = occupied_endpoint();
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

/// An audit sink that fails closes the gateway: in-flight work is cut, new work is refused, the
/// status says why it is draining, and once the drain completes the gateway stops with the audit
/// error instead of draining forever behind a healthy-looking control socket.
#[tokio::test]
async fn audit_failure_is_fail_closed_drains_and_stops_the_gateway() {
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
    let (config, _cache) = test_config();
    let mut gateway = gateway(
        config,
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
        "owner/repo-audit-failure",
        block_endpoint(endpoint),
        21,
        1,
        WorkspacePolicy {
            grants: vec![grant("audit-active.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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
    assert_refusing_after_audit_failure(&gateway).await;
    timeout(Duration::from_secs(1), active_upstream)
        .await
        .expect("active stream did not close")
        .expect("active upstream task");
    assert_stops_with_audit_failure(&mut gateway).await;
    let replacement = session(
        "audit-failure",
        "owner/repo-audit-failure",
        block_endpoint(endpoint),
        22,
        2,
        WorkspacePolicy::default(),
    )
    .0;
    assert!(matches!(
        gateway.handle().install(replacement).await,
        Err(GatewayError::Stopped)
    ));
    drop(gateway);
}

#[tokio::test]
async fn opaque_rejects_non_tls_missing_and_mismatched_sni_without_connector_calls() {
    let endpoint = free_endpoint();
    let calls = Arc::new(AtomicUsize::new(0));
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
        Arc::new(NoCredentials),
        Arc::new(CountingFailConnector {
            calls: Arc::clone(&calls),
        }),
        Arc::new(DiscardAudit),
    )
    .await;
    let (installed, token, _) = session(
        "opaque-validation",
        "owner/repo-opaque-validation",
        block_endpoint(endpoint),
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
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;

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

#[tokio::test]
async fn active_error_and_disconnect_paths_reclaim_single_permit() {
    let single_permit_config = || {
        let (mut config, cache) = test_config();
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
        (config, cache)
    };

    {
        let endpoint = free_endpoint();
        let calls = Arc::new(AtomicUsize::new(0));
        let (config, _cache) = single_permit_config();
        let gateway = gateway(
            config,
            Arc::new(NoCredentials),
            Arc::new(CountingFailConnector {
                calls: Arc::clone(&calls),
            }),
            Arc::new(DiscardAudit),
        )
        .await;
        let (installed, token, _) = session(
            "connect-failure",
            "owner/repo-connect-failure",
            block_endpoint(endpoint),
            24,
            1,
            WorkspacePolicy {
                grants: vec![grant("connect-failure.test", 443)],
                mirrors: Vec::new(),
            },
        );
        let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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
        // Each scenario's gateway is gone once drained; its block goes back before the next.
        release_claim(endpoint);
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
        let (config, _cache) = single_permit_config();
        let gateway = gateway(
            config,
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
            "owner/repo-credential-failure",
            block_endpoint(endpoint),
            25,
            1,
            WorkspacePolicy {
                grants: vec![grant("credential-failure.test", upstream_port)],
                mirrors: Vec::new(),
            },
        );
        let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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
        release_claim(endpoint);
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
        let (config, _cache) = single_permit_config();
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
            "header-failure",
            "owner/repo-header-failure",
            block_endpoint(endpoint),
            26,
            1,
            WorkspacePolicy {
                grants: vec![grant("header-failure.test", upstream_port)],
                mirrors: Vec::new(),
            },
        );
        let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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
        release_claim(endpoint);
    }

    {
        let gate = Arc::new(Notify::new());
        let (upstream_port, mut captured, _upstream) =
            http_fixture(1, Some(Arc::clone(&gate))).await;
        let endpoint = free_endpoint();
        let (config, _cache) = single_permit_config();
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
            "disconnect",
            "owner/repo-disconnect",
            block_endpoint(endpoint),
            27,
            1,
            WorkspacePolicy {
                grants: vec![grant("disconnect.test", upstream_port)],
                mirrors: Vec::new(),
            },
        );
        let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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
    let (mut config, _cache) = test_config();
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
        "owner/repo-queue-cancel",
        block_endpoint(endpoint),
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
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;

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
    let (mut config, _cache) = test_config();
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
        "owner/repo-client-tls",
        block_endpoint(endpoint),
        29,
        1,
        WorkspacePolicy {
            grants: vec![grant("client-tls.test", 443)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;

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
#[tokio::test]
async fn h2_intercept_and_upstream_preserve_streaming_trailers_and_authority() {
    let (upstream_port, upstream_tls, gate, mut captured, upstream_task) =
        h2_tls_fixture("secure-h2.test").await;
    let (negotiated, mut negotiated_rx) = mpsc::channel(2);
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
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
        "owner/repo-h2-streaming",
        block_endpoint(endpoint),
        31,
        1,
        WorkspacePolicy {
            grants: vec![grant("secure-h2.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;

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

#[tokio::test]
async fn upstream_tls_alpn_selects_h1_fallback_without_downgrading_h2() {
    let (upstream_port, upstream_tls, upstream_task) = h1_tls_fixture("fallback.test").await;
    let (negotiated, mut negotiated_rx) = mpsc::channel(1);
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
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
        "owner/repo-h2-h1-fallback",
        block_endpoint(endpoint),
        32,
        1,
        WorkspacePolicy {
            grants: vec![grant("fallback.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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

#[tokio::test]
async fn missing_upstream_alpn_fails_without_sending_http1_bytes() {
    let (upstream_port, upstream_tls, mut received_http, upstream_task) =
        no_alpn_tls_fixture("no-alpn.test").await;
    let (negotiated, mut negotiated_rx) = mpsc::channel(1);
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
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
        "owner/repo-no-upstream-alpn",
        block_endpoint(endpoint),
        33,
        1,
        WorkspacePolicy {
            grants: vec![grant("no-alpn.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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

#[tokio::test]
async fn missing_downstream_alpn_serves_http1_for_registry_clients() {
    let (port, mut captured, _upstream) = http_fixture(1, None).await;
    let endpoint = free_endpoint();
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
    let (installed, token, ca_certificate) = session(
        "no-downstream-alpn",
        "owner/repo-no-downstream-alpn",
        block_endpoint(endpoint),
        34,
        1,
        WorkspacePolicy {
            grants: vec![grant("downstream.test", port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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
    tls.write_all(
        format!(
            "GET /allowed HTTP/1.1\r\nHost: downstream.test:{port}\r\nConnection: close\r\n\r\n"
        )
        .as_bytes(),
    )
    .await
    .expect("write HTTP/1.1 without ALPN");
    let mut response = Vec::new();
    timeout(Duration::from_secs(2), tls.read_to_end(&mut response))
        .await
        .expect("HTTP/1.1 response deadline")
        .expect("read HTTP/1.1 response");
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));
    let forwarded = captured.recv().await.expect("captured HTTP/1.1 request");
    assert!(
        forwarded.starts_with("GET /allowed HTTP/1.1"),
        "{forwarded}"
    );
    gateway
        .drain()
        .await
        .expect("drain downstream no-ALPN gateway");
}

#[tokio::test]
async fn h2_session_cancellation_closes_stream_and_is_audited() {
    let (upstream_port, upstream_tls, gate, _captured, upstream_task) =
        h2_tls_fixture("cancel-h2.test").await;
    let (negotiated, _negotiated_rx) = mpsc::channel(1);
    let (audit_tx, mut audit_rx) = mpsc::channel(16);
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
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
        "owner/repo-cancel-h2",
        block_endpoint(endpoint),
        35,
        1,
        WorkspacePolicy {
            grants: vec![grant("cancel-h2.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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

#[tokio::test]
async fn h2_audit_failure_hard_stops_the_negotiated_connection() {
    let calls = Arc::new(AtomicUsize::new(0));
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let mut gateway = gateway(
        config,
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
        "owner/repo-h2-audit-stop",
        block_endpoint(endpoint),
        36,
        1,
        WorkspacePolicy {
            grants: vec![grant("audit-h2.test", port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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
    assert_refusing_after_audit_failure(&gateway).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "upstream connector ran before malformed h2 request was rejected"
    );
    assert_stops_with_audit_failure(&mut gateway).await;
    drop(gateway);
}

#[tokio::test]
async fn direct_https_proxy_uses_negotiated_upstream_h2() {
    let (upstream_port, upstream_tls, gate, mut captured, upstream_task) =
        h2_tls_fixture("direct-h2.test").await;
    let (negotiated, mut negotiated_rx) = mpsc::channel(1);
    let endpoint = free_endpoint();
    let (config, _cache) = test_config();
    let gateway = gateway(
        config,
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
        "owner/repo-direct-h2",
        block_endpoint(endpoint),
        37,
        1,
        WorkspacePolicy {
            grants: vec![grant("direct-h2.test", upstream_port)],
            mirrors: Vec::new(),
        },
    );
    let endpoint = install_in_free_block(&gateway.handle(), installed).await;
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
