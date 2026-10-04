use std::{
    collections::VecDeque,
    fs::OpenOptions as StdOpenOptions,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use cowshed_gateway::{
    Cache, CacheBodyError, CacheConfig, CacheError, CanonicalTarget, ConfigError, GatewayConfig,
    MirrorBody, MirrorCacheConfig, MirrorCacheScope, MirrorCacheStatus, MirrorError,
    MirrorFetchRequest, MirrorOutcome, MirrorProtocol, MirrorRequest, MirrorResourceKind,
    MirrorRoute, MirrorService, MirrorUpstream, ObjectDigest, ObjectExpectation, TargetScheme,
    UpstreamHealth, WorkspacePolicy,
};
use http::{HeaderMap, Method, Response, StatusCode, header};
use http_body::{Body, Frame};
use http_body_util::{BodyExt as _, Full};
use sha2::{Digest as _, Sha256, Sha512};
use uuid::Uuid;

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("cowshed-mirror-test-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).expect("create test cache root");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                .expect("secure test cache root");
        }
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn cache_config(&self) -> CacheConfig {
        CacheConfig {
            root: self.0.clone(),
            high_water_bytes: 512 * 1024 * 1024,
            low_water_bytes: 256 * 1024 * 1024,
            metadata_ttl: Duration::from_secs(300),
            fill_wait_timeout: Duration::from_secs(1),
        }
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct QueueUpstream {
    calls: AtomicUsize,
    responses: Mutex<VecDeque<Response<MirrorBody>>>,
    requests: Mutex<Vec<MirrorFetchRequest>>,
}

impl QueueUpstream {
    fn new(responses: impl IntoIterator<Item = Response<MirrorBody>>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            responses: Mutex::new(responses.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<MirrorFetchRequest> {
        self.requests.lock().expect("request lock").clone()
    }
}

#[async_trait]
impl MirrorUpstream for QueueUpstream {
    async fn fetch(
        &self,
        request: MirrorFetchRequest,
    ) -> Result<Response<MirrorBody>, CacheBodyError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().expect("request lock").push(request);
        self.responses
            .lock()
            .expect("response lock")
            .pop_front()
            .ok_or_else(|| "fixture upstream response queue exhausted".into())
    }
}

struct FailingUpstream {
    calls: AtomicUsize,
}

#[async_trait]
impl MirrorUpstream for FailingUpstream {
    async fn fetch(
        &self,
        _request: MirrorFetchRequest,
    ) -> Result<Response<MirrorBody>, CacheBodyError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err("fixture connector offline".into())
    }
}

struct LargePackumentBody {
    prefix: Option<Bytes>,
    padding: u64,
    complete: bool,
    polls: Arc<AtomicUsize>,
}

impl Body for LargePackumentBody {
    type Data = Bytes;
    type Error = CacheBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, CacheBodyError>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(prefix) = self.prefix.take() {
            return Poll::Ready(Some(Ok(Frame::data(prefix))));
        }
        if self.padding > 0 {
            const CHUNK: &[u8] = &[b' '; 64 * 1024];
            let length = usize::try_from(self.padding.min(CHUNK.len() as u64))
                .expect("chunk length fits usize");
            self.padding -= length as u64;
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(&CHUNK[..length])))));
        }
        if !self.complete {
            self.complete = true;
            return Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"\"}")))));
        }
        Poll::Ready(None)
    }
}

fn body(bytes: impl Into<Bytes>) -> MirrorBody {
    Full::new(bytes.into())
        .map_err(|never| -> CacheBodyError { match never {} })
        .boxed()
}

fn ok_response(bytes: &[u8]) -> Response<MirrorBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(body(Bytes::copy_from_slice(bytes)))
        .expect("fixture response")
}

fn declared_digest_response(bytes: &[u8]) -> Response<MirrorBody> {
    let digest = STANDARD.encode(Sha256::digest(bytes));
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, bytes.len())
        .header("digest", format!("sha-256={digest}"))
        .body(body(Bytes::copy_from_slice(bytes)))
        .expect("fixture response")
}

/// The registry's install representation varies by Accept. Each representation is its own cache
/// entry, so `Vary: accept` never conflates it with the full packument.
fn registry_packument_response(bytes: &[u8]) -> Response<MirrorBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/vnd.npm.install-v1+json")
        .header(header::VARY, "accept-encoding, accept")
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(body(Bytes::copy_from_slice(bytes)))
        .expect("registry packument response")
}

/// What an npm client sends for `fullMetadata`, and for an install.
const FULL_ACCEPT: &str = "application/json";
const INSTALL_V1_ACCEPT: &str =
    "application/vnd.npm.install-v1+json; q=1.0, application/json; q=0.8, */*";

/// A registry's packument in one representation; content type and ETag tell them apart.
fn representation_response(content_type: &str, etag: &str, bytes: &[u8]) -> Response<MirrorBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ETAG, etag)
        .header(header::VARY, "accept-encoding, accept")
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(body(Bytes::copy_from_slice(bytes)))
        .expect("representation response")
}

fn full_response(bytes: &[u8]) -> Response<MirrorBody> {
    representation_response("application/json", "\"full\"", bytes)
}

fn install_response(bytes: &[u8]) -> Response<MirrorBody> {
    representation_response("application/vnd.npm.install-v1+json", "\"install\"", bytes)
}

/// `@scope/pkg@1.0.0` publishing `tarball`'s integrity. The full document adds fields only the
/// full representation carries, so the two representations never share bytes.
fn published(tarball: &[u8], full: bool) -> Vec<u8> {
    let mut document = serde_json::json!({
        "name": "@scope/pkg",
        "versions": {"1.0.0": {"dist": {
            "tarball": "https://registry.npmjs.org/@scope/pkg/-/pkg-1.0.0.tgz",
            "integrity": format!("sha512-{}", STANDARD.encode(Sha512::digest(tarball))),
            "size": tarball.len()
        }}}
    });
    if full {
        document["dist-tags"] = serde_json::json!({"latest": "1.0.0"});
        document["readme"] = serde_json::json!("only the full packument carries a readme");
    }
    serde_json::to_vec(&document).expect("encode packument")
}

fn accepting(
    target: CanonicalTarget,
    path: &str,
    scope: MirrorCacheScope,
    credentialed: bool,
    accept: &str,
) -> MirrorRequest {
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT, accept.parse().expect("accept header"));
    MirrorRequest::new(
        MirrorProtocol::Npm,
        target,
        Method::GET,
        path.to_owned(),
        headers,
        scope,
        credentialed,
        None,
    )
    .expect("valid fixture mirror request")
}

/// An anonymous packument request to the public registry.
fn packument(path: &str, accept: &str) -> MirrorRequest {
    accepting(
        target("registry.npmjs.org"),
        path,
        MirrorCacheScope::Anonymous,
        false,
        accept,
    )
}

fn header_str(headers: &HeaderMap, name: header::HeaderName) -> &str {
    headers
        .get(name)
        .expect("fixture header")
        .to_str()
        .expect("visible header")
}

async fn collect_response(outcome: MirrorOutcome) -> (MirrorCacheStatus, HeaderMap, Bytes) {
    let MirrorOutcome::Response(response) = outcome else {
        panic!("expected mirror response");
    };
    let status = response.cache_status;
    let (parts, body) = response.response.into_parts();
    let bytes = body
        .collect()
        .await
        .expect("collect mirror response")
        .to_bytes();
    (status, parts.headers, bytes)
}

fn metadata_response(bytes: &[u8], etag: &str) -> Response<MirrorBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, bytes.len())
        .header(header::ETAG, etag)
        .body(body(Bytes::copy_from_slice(bytes)))
        .expect("fixture response")
}

fn json_response(bytes: &[u8]) -> Response<MirrorBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(body(Bytes::copy_from_slice(bytes)))
        .expect("JSON fixture response")
}

fn not_modified() -> Response<MirrorBody> {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .body(body(Bytes::new()))
        .expect("fixture response")
}

fn expectation(bytes: &[u8]) -> ObjectExpectation {
    ObjectExpectation {
        length: bytes.len() as u64,
        digest: ObjectDigest::Sha256(Sha256::digest(bytes).into()),
    }
}

fn target(host: &str) -> CanonicalTarget {
    CanonicalTarget::from_authority(&format!("{host}:443"), TargetScheme::Https)
        .expect("canonical fixture target")
}

fn request(
    protocol: MirrorProtocol,
    target: CanonicalTarget,
    path: &str,
    scope: MirrorCacheScope,
    credentialed: bool,
    expected: Option<ObjectExpectation>,
) -> MirrorRequest {
    MirrorRequest::new(
        protocol,
        target,
        Method::GET,
        path.to_owned(),
        HeaderMap::new(),
        scope,
        credentialed,
        expected,
    )
    .expect("valid fixture mirror request")
}

async fn collect(outcome: MirrorOutcome) -> (MirrorCacheStatus, Bytes) {
    let MirrorOutcome::Response(response) = outcome else {
        panic!("expected mirror response");
    };
    let status = response.cache_status;
    let bytes = response
        .response
        .into_body()
        .collect()
        .await
        .expect("collect mirror response")
        .to_bytes();
    (status, bytes)
}

async fn open_service(root: &TestRoot) -> MirrorService {
    MirrorService::new(Cache::open(root.cache_config()).await.expect("open cache"))
}

#[tokio::test]
async fn immutable_fill_hit_offline_and_corruption_refusal() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let artifact = b"immutable registry object";
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/thing/-/thing-1.0.0.tgz",
        MirrorCacheScope::Anonymous,
        false,
        Some(expectation(artifact)),
    );
    let upstream = QueueUpstream::new([ok_response(artifact)]);

    let (status, first) = collect(
        service
            .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
            .await
            .expect("fill immutable object"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(first.as_ref(), artifact);

    let offline = QueueUpstream::new([]);
    let (status, cached) = collect(
        service
            .execute(request.clone(), UpstreamHealth::Offline, &offline)
            .await
            .expect("serve verified offline hit"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::OfflineHit);
    assert_eq!(cached.as_ref(), artifact);
    assert_eq!(offline.call_count(), 0);

    drop(service);
    tokio::task::yield_now().await;
    let restarted = open_service(&root).await;
    let (status, persisted) = collect(
        restarted
            .execute(request.clone(), UpstreamHealth::Offline, &offline)
            .await
            .expect("serve verified offline hit after actor restart"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::OfflineHit);
    assert_eq!(persisted.as_ref(), artifact);

    let object = std::fs::read_dir(root.path())
        .expect("list cache")
        .map(|entry| entry.expect("cache entry").path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("obj-"))
        })
        .expect("cached object");
    let file = StdOpenOptions::new()
        .write(true)
        .open(&object)
        .expect("open cached object for truncation fixture");
    let truncated = file.metadata().expect("cached object metadata").len() - 1;
    file.set_len(truncated).expect("truncate cached object");
    file.sync_all().expect("sync truncation fixture");

    drop(file);
    let error = restarted
        .execute(request, UpstreamHealth::Offline, &offline)
        .await
        .expect_err("corrupt offline object must become a miss");
    assert!(matches!(error, MirrorError::OfflineMiss));
    assert_eq!(offline.call_count(), 0);
    assert!(!object.exists(), "corruption must delete the cache entry");
}

#[tokio::test]
async fn same_length_cached_tarball_corruption_withholds_the_last_chunk() {
    use std::io::{Seek as _, SeekFrom, Write as _};

    let root = TestRoot::new();
    let service = open_service(&root).await;
    let artifact = b"verified cached tarball";
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/pkg/-/pkg-1.0.0.tgz",
        MirrorCacheScope::Anonymous,
        false,
        Some(expectation(artifact)),
    );
    let upstream = QueueUpstream::new([ok_response(artifact)]);
    collect(
        service
            .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
            .await
            .expect("fill verified tarball"),
    )
    .await;
    let object = std::fs::read_dir(root.path())
        .expect("list cache")
        .map(|entry| entry.expect("cache entry").path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("obj-"))
        })
        .expect("cached tarball");
    let mut file = StdOpenOptions::new()
        .write(true)
        .open(&object)
        .expect("open corruption fixture");
    file.seek(SeekFrom::Start(64 * 1024))
        .expect("seek unchanged body geometry");
    file.write_all(b"!")
        .expect("change one cached byte without changing length");
    file.sync_all().expect("sync corrupted byte");
    drop(file);
    let offline = QueueUpstream::new([]);
    let MirrorOutcome::Response(response) = service
        .execute(request, UpstreamHealth::Offline, &offline)
        .await
        .expect("headers admitted before streaming integrity verification")
    else {
        panic!("expected cached response");
    };
    let mut body = response.response.into_body();
    let error = body
        .frame()
        .await
        .expect("reader verifies final chunk")
        .expect_err("same-length digest corruption cannot emit the complete tarball");
    assert!(error.to_string().contains("digest"), "{error}");
    assert_eq!(offline.call_count(), 0);
}

/// An HTTP client reads a response to its declared `Content-Length` and closes; it never polls
/// for the end of a body it has already received. The fill must publish on the strength of the
/// declared length, not on a final poll the client never makes, or every fetch refills.
#[tokio::test]
async fn a_fill_read_to_its_declared_length_publishes_though_the_client_stops_there() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let artifact = b"tarball a client reads to its content length";
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/thing/-/thing-2.0.0.tgz",
        MirrorCacheScope::Anonymous,
        false,
        Some(expectation(artifact)),
    );
    let upstream = QueueUpstream::new([ok_response(artifact)]);
    let MirrorOutcome::Response(response) = service
        .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
        .await
        .expect("fill immutable object")
    else {
        panic!("expected a streamed fill");
    };
    assert_eq!(response.cache_status, MirrorCacheStatus::Filled);
    let mut body = response.response.into_body();
    let mut received = Vec::new();
    while received.len() < artifact.len() {
        let frame = body
            .frame()
            .await
            .expect("the declared bytes arrive")
            .expect("the fill streams the upstream bytes");
        received.extend_from_slice(&frame.into_data().expect("data frame"));
    }
    assert_eq!(received, artifact);
    drop(body);

    let untouched = QueueUpstream::new([]);
    let (status, cached) = collect(
        service
            .execute(request, UpstreamHealth::Healthy, &untouched)
            .await
            .expect("the second fetch is served from the published fill"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Hit);
    assert_eq!(cached.as_ref(), artifact);
    assert_eq!(untouched.call_count(), 0);
}

#[tokio::test]
async fn immutable_digest_mismatch_never_publishes() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let expected = expectation(b"right bytes");
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/demo/-/demo-1.0.0.tgz",
        MirrorCacheScope::Anonymous,
        false,
        Some(expected),
    );
    let upstream = QueueUpstream::new([ok_response(b"wrong bytes")]);
    let MirrorOutcome::Response(response) = service
        .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
        .await
        .expect("start rejected fill")
    else {
        panic!("expected streaming response");
    };
    let error = response
        .response
        .into_body()
        .collect()
        .await
        .expect_err("digest mismatch must fail the stream");
    assert!(error.to_string().contains("digest"));

    let offline = QueueUpstream::new([]);
    assert!(matches!(
        service
            .execute(request, UpstreamHealth::Offline, &offline)
            .await,
        Err(MirrorError::OfflineMiss)
    ));
}

#[tokio::test]
async fn synthetic_digest_header_cannot_supply_protocol_integrity() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let artifact = b"header-declared immutable object";
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/declared/-/declared-1.0.0.tgz",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    // The packument publishes the version with only a SHA-1 `shasum`, so the only digest on
    // offer is the tarball's own header — which never stands in for protocol integrity.
    let packument = serde_json::to_vec(&serde_json::json!({
        "name": "declared",
        "versions": { "1.0.0": { "dist": {
            "tarball": "https://registry.npmjs.org/declared/-/declared-1.0.0.tgz",
            "shasum": "0000000000000000000000000000000000000000"
        }}}
    }))
    .expect("encode packument");
    let upstream = QueueUpstream::new([
        registry_packument_response(&packument),
        declared_digest_response(artifact),
    ]);
    assert!(matches!(
        service
            .execute(request, UpstreamHealth::Healthy, &upstream)
            .await,
        Err(MirrorError::MissingIntegrity)
    ));
    assert_eq!(upstream.call_count(), 1);
}

#[tokio::test]
async fn metadata_over_the_old_cap_supplies_tarball_integrity() {
    let artifact = b"verified tarball";
    let mut packument = serde_json::to_vec(&serde_json::json!({
        "name": "typescript",
        "versions": { "1.0.0": { "dist": {
            "tarball": "https://registry.npmjs.org/typescript/-/typescript-1.0.0.tgz",
            "integrity": format!("sha512-{}", STANDARD.encode(Sha512::digest(artifact))),
            "size": artifact.len()
        }}}
    }))
    .expect("encode packument");
    packument.resize(8 * 1024 * 1024 + 1, b' ');

    for response in [
        registry_packument_response(&packument),
        json_response(&packument),
    ] {
        let root = TestRoot::new();
        let service = open_service(&root).await;
        let request = request(
            MirrorProtocol::Npm,
            target("registry.npmjs.org"),
            "/typescript/-/typescript-1.0.0.tgz",
            MirrorCacheScope::Anonymous,
            false,
            None,
        );
        let upstream = QueueUpstream::new([response, ok_response(artifact)]);
        let (status, bytes) = collect(
            service
                .execute(request, UpstreamHealth::Healthy, &upstream)
                .await
                .expect("large metadata supplies published integrity"),
        )
        .await;
        assert_eq!(status, MirrorCacheStatus::Filled);
        assert_eq!(bytes.as_ref(), artifact);
        assert_eq!(upstream.call_count(), 2);
        assert_eq!(upstream.requests()[0].path, "/typescript");
    }
}

#[tokio::test]
async fn packument_larger_than_128_mib_streams_and_indexes_without_buffering() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let artifact = b"verified large-packument tarball";
    let encoded = serde_json::to_vec(&serde_json::json!({
        "versions": {"1.0.0": {"dist": {
            "tarball": "https://registry.npmjs.org/huge/-/huge-1.0.0.tgz",
            "integrity": format!("sha512-{}", STANDARD.encode(Sha512::digest(artifact))),
            "size": artifact.len()
        }}}
    }))
    .expect("packument prefix");
    let mut prefix = encoded;
    prefix.pop();
    prefix.extend_from_slice(b",\"padding\":\"");
    let prefix_length = prefix.len() as u64;
    let padding = 129 * 1024 * 1024;
    let polls = Arc::new(AtomicUsize::new(0));
    let upstream_body = LargePackumentBody {
        prefix: Some(Bytes::from(prefix)),
        padding,
        complete: false,
        polls: polls.clone(),
    }
    .boxed();
    let upstream = QueueUpstream::new([Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/vnd.npm.install-v1+json")
        .header(header::VARY, "accept")
        .body(upstream_body)
        .expect("large stream")]);
    let MirrorOutcome::Response(response) = service
        .execute(
            request(
                MirrorProtocol::Npm,
                target("registry.npmjs.org"),
                "/huge",
                MirrorCacheScope::Anonymous,
                false,
                None,
            ),
            UpstreamHealth::Healthy,
            &upstream,
        )
        .await
        .expect("start streaming packument")
    else {
        panic!("expected streamed packument");
    };
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "headers arrive before reading metadata"
    );
    let mut body = response.response.into_body();
    let mut total = 0u64;
    while let Some(frame) = body.frame().await {
        if let Ok(bytes) = frame.expect("stream large packument").into_data() {
            total += bytes.len() as u64;
            assert!(
                bytes.len() <= 64 * 1024,
                "gateway never emits a buffered packument"
            );
        }
    }
    assert_eq!(total, prefix_length + padding + 2);
    let artifact_upstream = QueueUpstream::new([ok_response(artifact)]);
    let (_, bytes) = collect(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/huge/-/huge-1.0.0.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &artifact_upstream,
            )
            .await
            .expect("indexed integrity"),
    )
    .await;
    assert_eq!(bytes.as_ref(), artifact);
    assert_eq!(
        artifact_upstream.call_count(),
        1,
        "tarball uses index, never fetches metadata again"
    );
}

#[tokio::test]
async fn simultaneous_misses_coalesce_until_atomic_publish() {
    let root = TestRoot::new();
    let service = Arc::new(open_service(&root).await);
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/react",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    let upstream = Arc::new(QueueUpstream::new([metadata_response(
        b"packument",
        "\"one\"",
    )]));

    let first = service
        .execute(request.clone(), UpstreamHealth::Healthy, upstream.as_ref())
        .await
        .expect("first miss starts fill");
    let second_service = Arc::clone(&service);
    let second_upstream = Arc::clone(&upstream);
    let second_request = request.clone();
    let second = tokio::spawn(async move {
        second_service
            .execute(
                second_request,
                UpstreamHealth::Healthy,
                second_upstream.as_ref(),
            )
            .await
            .expect("coalesced waiter")
    });
    tokio::task::yield_now().await;
    assert_eq!(upstream.call_count(), 1);

    assert_eq!(collect(first).await.1.as_ref(), b"packument");
    let (status, bytes) = collect(second.await.expect("join waiter")).await;
    assert_eq!(status, MirrorCacheStatus::Hit);
    assert_eq!(bytes.as_ref(), b"packument");
    assert_eq!(upstream.call_count(), 1);
}

#[tokio::test]
async fn stale_metadata_304_refreshes_and_200_replaces_atomically() {
    let root = TestRoot::new();
    let mut config = root.cache_config();
    config.metadata_ttl = Duration::from_millis(15);
    let service = MirrorService::new(Cache::open(config).await.expect("open cache"));
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/react",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    let upstream = QueueUpstream::new([
        metadata_response(b"v1", "\"v1\""),
        not_modified(),
        metadata_response(b"v2", "\"v2\""),
    ]);

    assert_eq!(
        collect(
            service
                .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
                .await
                .expect("fill v1")
        )
        .await
        .1
        .as_ref(),
        b"v1"
    );
    tokio::time::sleep(Duration::from_millis(25)).await;
    let (status, bytes) = collect(
        service
            .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
            .await
            .expect("304 revalidation"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Revalidated);
    assert_eq!(bytes.as_ref(), b"v1");
    assert_eq!(
        upstream.requests()[1]
            .headers
            .get(header::IF_NONE_MATCH)
            .expect("conditional etag"),
        "\"v1\""
    );

    tokio::time::sleep(Duration::from_millis(25)).await;
    let (status, bytes) = collect(
        service
            .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
            .await
            .expect("200 replacement"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), b"v2");

    let offline = QueueUpstream::new([]);
    assert_eq!(
        collect(
            service
                .execute(request, UpstreamHealth::Offline, &offline)
                .await
                .expect("offline replacement hit")
        )
        .await
        .1
        .as_ref(),
        b"v2"
    );
}

#[tokio::test]
async fn coalesced_fill_waits_have_a_hard_timeout() {
    let root = TestRoot::new();
    let mut config = root.cache_config();
    config.fill_wait_timeout = Duration::from_millis(10);
    let service = MirrorService::new(Cache::open(config).await.expect("open cache"));
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/timeout",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    let upstream = QueueUpstream::new([metadata_response(b"held fill", "\"held\"")]);
    let leader = service
        .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
        .await
        .expect("start leader fill without consuming it");

    let error = service
        .execute(request, UpstreamHealth::Healthy, &upstream)
        .await
        .expect_err("coalesced wait must time out");
    assert!(matches!(
        error,
        MirrorError::Cache(CacheError::FillWaitTimeout)
    ));
    drop(leader);
}

#[tokio::test]
async fn scoped_cache_entries_never_cross_project_or_anonymous_boundaries() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let project_a = request(
        MirrorProtocol::Npm,
        target("npm.private.test"),
        "/@team/pkg",
        MirrorCacheScope::Project("repo-a".to_owned()),
        true,
        None,
    );
    let upstream = QueueUpstream::new([metadata_response(b"private-a", "\"a\"")]);
    assert_eq!(
        collect(
            service
                .execute(project_a, UpstreamHealth::Healthy, &upstream)
                .await
                .expect("fill project cache")
        )
        .await
        .1
        .as_ref(),
        b"private-a"
    );

    let project_b = request(
        MirrorProtocol::Npm,
        target("npm.private.test"),
        "/@team/pkg",
        MirrorCacheScope::Project("repo-b".to_owned()),
        true,
        None,
    );
    let offline = QueueUpstream::new([]);
    assert!(matches!(
        service
            .execute(project_b, UpstreamHealth::Offline, &offline)
            .await,
        Err(MirrorError::OfflineMiss)
    ));
    assert!(matches!(
        MirrorRequest::new(
            MirrorProtocol::Npm,
            target("npm.private.test"),
            Method::GET,
            "/@team/pkg".to_owned(),
            HeaderMap::new(),
            MirrorCacheScope::Anonymous,
            true,
            None,
        ),
        Err(MirrorError::UnscopedCredential)
    ));
}

#[tokio::test]
async fn inactive_lru_eviction_respects_an_active_reader_pin() {
    let root = TestRoot::new();
    let config = CacheConfig {
        root: root.path().to_owned(),
        high_water_bytes: 140_000,
        low_water_bytes: 131_200,
        metadata_ttl: Duration::from_secs(300),
        fill_wait_timeout: Duration::from_secs(1),
    };
    let service = MirrorService::new(Cache::open(config).await.expect("open small cache"));
    let upstream = QueueUpstream::new([
        ok_response(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        ok_response(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ok_response(b"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
    ]);
    let make = |name: &str, byte: u8| {
        let bytes = vec![byte; 64];
        request(
            MirrorProtocol::Npm,
            target("registry.npmjs.org"),
            &format!("/{name}/-/{name}-1.tgz"),
            MirrorCacheScope::Anonymous,
            false,
            Some(expectation(&bytes)),
        )
    };
    let a = make("a", b'a');
    let b = make("b", b'b');
    let c = make("c", b'c');
    collect(
        service
            .execute(a.clone(), UpstreamHealth::Healthy, &upstream)
            .await
            .expect("fill a"),
    )
    .await;
    collect(
        service
            .execute(b.clone(), UpstreamHealth::Healthy, &upstream)
            .await
            .expect("fill b"),
    )
    .await;

    let MirrorOutcome::Response(pinned) = service
        .execute(a.clone(), UpstreamHealth::Offline, &QueueUpstream::new([]))
        .await
        .expect("pin a")
    else {
        panic!("expected pinned cache response");
    };
    collect(
        service
            .execute(c.clone(), UpstreamHealth::Healthy, &upstream)
            .await
            .expect("fill c"),
    )
    .await;

    let offline = QueueUpstream::new([]);
    assert!(matches!(
        service.execute(b, UpstreamHealth::Offline, &offline).await,
        Err(MirrorError::OfflineMiss)
    ));
    assert_eq!(
        collect(
            service
                .execute(c, UpstreamHealth::Offline, &offline)
                .await
                .expect("c retained")
        )
        .await
        .1,
        Bytes::from_static(b"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
    );
    assert_eq!(
        pinned
            .response
            .into_body()
            .collect()
            .await
            .expect("read pinned a")
            .to_bytes(),
        Bytes::from_static(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
}

#[tokio::test]
async fn startup_removes_crash_temps_and_rejects_symlink_roots() {
    let root = TestRoot::new();
    let temp = root.path().join(".tmp-crashed-fill");
    std::fs::write(&temp, b"partial").expect("write crash fixture");
    Cache::open(root.cache_config())
        .await
        .expect("open cache and clean temps");
    assert!(!temp.exists());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let link = root.path().with_extension("link");
        symlink(root.path(), &link).expect("create root symlink fixture");
        let result = Cache::open(CacheConfig::production(link.clone())).await;
        assert!(result.is_err());
        std::fs::remove_file(link).expect("remove symlink fixture");
    }
}

#[test]
fn npm_fixtures_have_exact_protocol_metadata() {
    let npm = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/@scope%2fpkg",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    assert_eq!(npm.metadata.identity, "@scope/pkg");
    assert_eq!(npm.metadata.kind, MirrorResourceKind::Metadata);
}

#[test]
fn native_registry_policy_is_typed_exact_and_scope_bound() {
    let policy = WorkspacePolicy {
        grants: Vec::new(),
        mirrors: vec![
            MirrorRoute::new(
                "https://registry.npmjs.org:443",
                vec![
                    "/react".to_owned(),
                    "/react/-/".to_owned(),
                    "/@scope/".to_owned(),
                ],
                false,
            )
            .expect("valid npm route"),
        ],
    };
    policy.validate().expect("valid typed route");
    let resolved = policy
        .resolve_npm_registry(&target("registry.npmjs.org"), "/react/-/react-1.0.0.tgz")
        .expect("admitted route");
    assert_eq!(resolved.target, target("registry.npmjs.org"));
    assert_eq!(resolved.path, "/react/-/react-1.0.0.tgz");
    assert_eq!(resolved.protocol, MirrorProtocol::Npm);
    assert_eq!(resolved.admitted_prefix, "/react/-/");
    let baseline = policy
        .resolve_npm_registry(&target("registry.npmjs.org"), "/lodash")
        .expect("unmatched private scope falls through to public baseline");
    assert_eq!(baseline.target, target("registry.npmjs.org"));
    assert_eq!(baseline.admitted_prefix, "/");
    let scoped = policy
        .resolve_npm_registry(&target("registry.npmjs.org"), "/@scope%2fpkg")
        .expect("encoded npm scope is admitted without weakening generic paths");
    assert_eq!(scoped.path, "/@scope%2fpkg");
    assert_eq!(scoped.admitted_prefix, "/@scope/");

    assert!(
        MirrorRoute::new("http://registry.npmjs.org:80", vec!["/".to_owned()], false,).is_err()
    );
}

#[test]
fn disjoint_private_scopes_coexist_without_shadowing_public_baselines() {
    let policy = WorkspacePolicy {
        grants: Vec::new(),
        mirrors: vec![
            MirrorRoute::new(
                "https://npm.company.test:443",
                vec!["/@company/".to_owned()],
                true,
            )
            .expect("company npm route"),
            MirrorRoute::new(
                "https://npm.other.test:443",
                vec!["/@other/".to_owned()],
                true,
            )
            .expect("other npm route"),
        ],
    };
    policy.validate().expect("disjoint private scopes");

    assert_eq!(
        policy
            .resolve_npm_registry(&target("registry.npmjs.org"), "/react")
            .expect("public npm baseline")
            .target,
        target("registry.npmjs.org")
    );
    assert_eq!(
        policy
            .resolve_npm_registry(&target("npm.company.test"), "/@company%2fpkg")
            .expect("company npm scope")
            .target,
        target("npm.company.test")
    );
    assert_eq!(
        policy
            .resolve_npm_registry(&target("npm.other.test"), "/@other%2fpkg")
            .expect("other npm scope")
            .target,
        target("npm.other.test")
    );

    let mut overlapping = policy.clone();
    overlapping.mirrors.push(
        MirrorRoute::new(
            "https://npm.company.test:443",
            vec!["/@company/pkg".to_owned()],
            true,
        )
        .expect("overlapping npm route"),
    );
    assert!(matches!(
        overlapping.validate(),
        Err(cowshed_gateway::PolicyError::OverlappingMirrorScope)
    ));
}

#[tokio::test]
async fn redirect_is_bounded_same_origin_typed_and_never_followed() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/react",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    let redirected = Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header(header::LOCATION, "/react?write=true")
        .body(body(Bytes::new()))
        .expect("redirect response");
    let upstream = QueueUpstream::new([redirected]);
    let MirrorOutcome::Redirect(redirect) = service
        .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
        .await
        .expect("typed redirect")
    else {
        panic!("expected redirect outcome");
    };
    assert_eq!(redirect.request.path, "/react?write=true");
    assert_eq!(redirect.request.redirects_remaining, 4);
    assert_eq!(upstream.call_count(), 1);
    let mut next =
        MirrorRequest::from_redirect(redirect.request, MirrorCacheScope::Anonymous, false)
            .expect("re-admitted redirect request");
    for expected_remaining in [3, 2, 1, 0] {
        let response = Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header(header::LOCATION, "/react?write=true")
            .body(body(Bytes::new()))
            .expect("redirect response");
        let hop = QueueUpstream::new([response]);
        let MirrorOutcome::Redirect(redirect) = service
            .execute(next, UpstreamHealth::Healthy, &hop)
            .await
            .expect("bounded redirect hop")
        else {
            panic!("expected redirect hop");
        };
        assert_eq!(redirect.request.redirects_remaining, expected_remaining);
        next = MirrorRequest::from_redirect(redirect.request, MirrorCacheScope::Anonymous, false)
            .expect("re-admitted redirect request");
    }
    let sixth = Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header(header::LOCATION, "/react?write=true")
        .body(body(Bytes::new()))
        .expect("redirect response");
    assert!(matches!(
        service
            .execute(next, UpstreamHealth::Healthy, &QueueUpstream::new([sixth]),)
            .await,
        Err(MirrorError::TooManyRedirects)
    ));

    let cross_origin = Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header(header::LOCATION, "https://evil.example/react")
        .body(body(Bytes::new()))
        .expect("redirect response");
    let upstream = QueueUpstream::new([cross_origin]);
    assert!(matches!(
        service
            .execute(request.clone(), UpstreamHealth::Healthy, &upstream)
            .await,
        Err(MirrorError::UnsafeRedirect)
    ));
    assert_eq!(upstream.call_count(), 1);

    let downgrade = Response::builder()
        .status(StatusCode::TEMPORARY_REDIRECT)
        .header(header::LOCATION, "http://registry.npmjs.org/react")
        .body(body(Bytes::new()))
        .expect("downgrade response");
    assert!(matches!(
        service
            .execute(
                request.clone(),
                UpstreamHealth::Healthy,
                &QueueUpstream::new([downgrade]),
            )
            .await,
        Err(MirrorError::UnsafeRedirect)
    ));

    let method_rewrite = Response::builder()
        .status(StatusCode::SEE_OTHER)
        .header(header::LOCATION, "/react")
        .body(body(Bytes::new()))
        .expect("method rewrite response");
    assert!(matches!(
        service
            .execute(
                request,
                UpstreamHealth::Healthy,
                &QueueUpstream::new([method_rewrite]),
            )
            .await,
        Err(MirrorError::UnsafeRedirect)
    ));
}

#[test]
fn native_registry_baseline_is_only_the_exact_public_npm_origin() {
    let policy = WorkspacePolicy::default();
    let npm = policy
        .resolve_npm_registry(&target("registry.npmjs.org"), "/@scope%2fpkg")
        .expect("baseline npm packument");
    assert_eq!(npm.target, target("registry.npmjs.org"));
    assert_eq!(npm.path, "/@scope%2fpkg");
    assert!(!npm.credentialed);

    assert!(
        policy
            .resolve_npm_registry(&target("npm.other.test"), "/@scope%2fpkg")
            .is_none()
    );
    assert!(
        policy
            .resolve_npm_registry(&target("index.crates.io"), "/config.json")
            .is_none()
    );
    assert!(
        policy
            .resolve_npm_registry(&target("proxy.golang.org"), "/example.com/@v/list")
            .is_none()
    );
}

#[tokio::test]
async fn npm_packument_passes_through_and_index_verifies_tarball() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let tarball = b"real npm tarball bytes";
    let integrity = format!("sha512-{}", STANDARD.encode(Sha512::digest(tarball)));
    let packument = serde_json::json!({
        "name": "@scope/pkg",
        "versions": {
            "1.2.3": {
                "dist": {
                    "tarball": "https://registry.npmjs.org/@scope/pkg/-/pkg-1.2.3.tgz",
                    "integrity": integrity,
                    "size": tarball.len()
                }
            }
        }
    });
    let encoded = serde_json::to_vec(&packument).expect("encode packument");
    let metadata = QueueUpstream::new([json_response(&encoded)]);
    let (_, streamed) = collect(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope%2fpkg",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &metadata,
            )
            .await
            .expect("stream packument"),
    )
    .await;
    assert_eq!(streamed.as_ref(), encoded, "packument bytes are unchanged");
    let artifact_path = "/@scope/pkg/-/pkg-1.2.3.tgz";
    let artifact = QueueUpstream::new([ok_response(tarball)]);
    let (_, bytes) = collect(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    artifact_path,
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &artifact,
            )
            .await
            .expect("stream verified sha512 tarball"),
    )
    .await;
    assert_eq!(bytes.as_ref(), tarball);

    let bad_path = artifact_path.replace("pkg-1.2.3.tgz", "pkg-1.2.4.tgz");
    let mut tampered = tarball.to_vec();
    tampered[0] ^= 1;
    let mismatch = QueueUpstream::new([ok_response(&tampered)]);
    let error = service
        .execute(
            request(
                MirrorProtocol::Npm,
                target("registry.npmjs.org"),
                &bad_path,
                MirrorCacheScope::Anonymous,
                false,
                None,
            ),
            UpstreamHealth::Healthy,
            &mismatch,
        )
        .await
        .expect_err("unpublished tarball is refused");
    assert!(matches!(error, MirrorError::MissingIntegrity));
    assert_eq!(mismatch.call_count(), 0);
}

/// Lockfile installs can fetch a tarball before reading metadata. The first request fills
/// and indexes the packument; later requests use only its attached published expectations.
#[tokio::test]
async fn a_lockfile_tarball_is_verified_against_the_integrity_its_packument_publishes() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let tarball = b"lockfile-driven npm tarball bytes";
    let integrity = format!("sha512-{}", STANDARD.encode(Sha512::digest(tarball)));
    let packument = serde_json::to_vec(&serde_json::json!({
        "name": "@scope/pkg",
        "versions": {
            "1.2.3": { "dist": {
                "tarball": "https://registry.npmjs.org/@scope/pkg/-/pkg-1.2.3.tgz",
                "integrity": integrity,
                "size": tarball.len()
            }},
            "1.2.4": { "dist": {
                "tarball": "https://registry.npmjs.org/@scope/pkg/-/pkg-1.2.4.tgz",
                "integrity": integrity,
                "size": tarball.len()
            }}
        }
    }))
    .expect("encode packument");
    let upstream = QueueUpstream::new([json_response(&packument), ok_response(tarball)]);
    let (status, bytes) = collect(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope/pkg/-/pkg-1.2.3.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await
            .expect("a lockfile tarball is served"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), tarball);
    assert_eq!(
        upstream
            .requests()
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        ["/@scope%2fpkg", "/@scope/pkg/-/pkg-1.2.3.tgz"]
    );

    // Bytes that do not match the published integrity abort before publication.
    let mut tampered = tarball.to_vec();
    tampered[0] ^= 1;
    let mismatch = QueueUpstream::new([ok_response(&tampered)]);
    let MirrorOutcome::Response(response) = service
        .execute(
            request(
                MirrorProtocol::Npm,
                target("registry.npmjs.org"),
                "/@scope/pkg/-/pkg-1.2.4.tgz",
                MirrorCacheScope::Anonymous,
                false,
                None,
            ),
            UpstreamHealth::Healthy,
            &mismatch,
        )
        .await
        .expect("the cached packument supplies the expectation")
    else {
        panic!("expected streaming mismatch response");
    };
    assert!(
        response.response.into_body().collect().await.is_err(),
        "a tarball that does not match its packument integrity must never be served"
    );
    assert_eq!(
        mismatch.call_count(),
        1,
        "the packument comes from the metadata cache, only the tarball is fetched"
    );

    // A version the packument does not publish has no integrity to verify against.
    let unknown = QueueUpstream::new([]);
    assert!(matches!(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope/pkg/-/pkg-9.9.9.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &unknown,
            )
            .await,
        Err(MirrorError::MissingIntegrity)
    ));
    assert_eq!(unknown.call_count(), 0);

    // The registry's Vary: accept representation is cached and indexed without changing bytes.
    let registry_root = TestRoot::new();
    let registry_service = open_service(&registry_root).await;
    let registry = QueueUpstream::new([
        registry_packument_response(&packument),
        ok_response(tarball),
    ]);
    let (status, bytes) = collect(
        registry_service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope/pkg/-/pkg-1.2.3.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &registry,
            )
            .await
            .expect("a lockfile tarball is served against the published packument"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), tarball);
}

#[tokio::test]
async fn real_fetch_failure_transitions_unknown_health_to_fast_offline_miss() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let request = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/health-transition",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    let upstream = FailingUpstream {
        calls: AtomicUsize::new(0),
    };
    assert!(matches!(
        service
            .execute(request.clone(), UpstreamHealth::Unknown, &upstream)
            .await,
        Err(MirrorError::Upstream(_))
    ));
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        service
            .execute(request, UpstreamHealth::Unknown, &upstream)
            .await,
        Err(MirrorError::OfflineMiss)
    ));
    assert_eq!(
        upstream.calls.load(Ordering::SeqCst),
        1,
        "offline state must fail a cache miss before reconnecting"
    );
}

#[tokio::test]
async fn canonical_identity_encoding_and_vary_star_never_share_cache() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT_ENCODING, "gzip, br".parse().expect("header"));
    let request = MirrorRequest::new(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        Method::GET,
        "/encoding".to_owned(),
        headers,
        MirrorCacheScope::Anonymous,
        false,
        None,
    )
    .expect("canonical mirror request");
    let varying = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, 2)
        .header(header::VARY, "*")
        .body(body(Bytes::from_static(b"ok")))
        .expect("varying response");
    let upstream = QueueUpstream::new([varying]);
    let (status, _) = collect(
        service
            .execute(request, UpstreamHealth::Healthy, &upstream)
            .await
            .expect("bypass unsafe varying response"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Bypassed);
    assert_eq!(
        upstream.requests()[0]
            .headers
            .get(header::ACCEPT_ENCODING)
            .expect("canonical encoding"),
        "identity"
    );
}

#[test]
fn gateway_mirror_cache_config_requires_a_preexisting_real_root() {
    assert!(matches!(
        GatewayConfig::default().validate(),
        Err(ConfigError::MissingMirrorCacheRoot)
    ));

    let root = TestRoot::new();
    let config = MirrorCacheConfig::new(root.path().to_owned());
    config.validate().expect("pre-existing real cache root");

    let missing = MirrorCacheConfig::new(root.path().join("not-created"));
    assert!(matches!(
        missing.validate(),
        Err(ConfigError::InsecureMirrorCacheRoot)
    ));
}

/// A git helper whose mode this fixture owns.
///
/// `current_exe()` cannot serve here: the linker writes the test binary as 0o777 masked by the
/// umask of whoever ran the build, so a fixture pointing at it inherits a mode from the builder's
/// environment and `validate_git_helper_executable` then accepts or refuses it depending on a shell
/// setting the test never mentions.
fn git_helper(path: PathBuf, mode: u32) -> PathBuf {
    std::fs::write(&path, b"#!/bin/sh\nexit 0\n").expect("write git helper");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
            .expect("set git helper mode");
    }
    path
}

/// A host layout `validate_host_cache_layout` accepts, so a test can vary one part and know that
/// part is what the verdict came from. Idempotent: callers reuse one root across cases.
fn production_layout(root: &Path, helper: PathBuf) -> GatewayConfig {
    let store = root.join("store");
    let cache_dir = root.join("dev.cowshed");
    let fixed = cache_dir.join("mirror");
    std::fs::create_dir_all(&store).expect("store root");
    std::fs::create_dir_all(&fixed).expect("create fixed cache root");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&fixed, std::fs::Permissions::from_mode(0o700))
            .expect("secure fixed cache root");
    }
    GatewayConfig {
        control_socket: Some(store.join("gateway.sock")),
        production_cache_dir: Some(cache_dir),
        git_helper_executable: Some(helper),
        mirror_cache: MirrorCacheConfig::new(fixed),
        ..GatewayConfig::default()
    }
}

#[test]
fn production_cache_root_is_fixed_beneath_the_cowshed_cache_directory() {
    let fixture = TestRoot::new();
    let root = std::fs::canonicalize(fixture.path()).expect("canonical fixture root");
    let mut config = production_layout(&root, git_helper(root.join("git-helper"), 0o700));
    config
        .validate_host_cache_layout()
        .expect("fixed production cache layout");

    config.mirror_cache = MirrorCacheConfig::new(root);
    assert!(matches!(
        config.validate_host_cache_layout(),
        Err(ConfigError::InvalidProductionCacheRoot)
    ));
}

/// The gateway spawns this binary, so whoever can rewrite it between validation and spawn chooses
/// what the gateway runs. Each way of losing that exclusivity is refused, and the refusal is
/// reached only because the surrounding layout is otherwise valid.
#[test]
fn git_helper_executable_must_be_private_and_owner_executable() {
    let fixture = TestRoot::new();
    let root = std::fs::canonicalize(fixture.path()).expect("canonical fixture root");

    // One arm each: 0o770 sets group write and nothing else the check objects to, 0o702 sets other
    // write, 0o600 is private but not owner-executable. A mode like 0o750 would pass, correctly --
    // group read and execute are not the hazard, group write is.
    for (case, helper) in [
        (
            "group-writable",
            git_helper(root.join("group-writable"), 0o770),
        ),
        (
            "other-writable",
            git_helper(root.join("other-writable"), 0o702),
        ),
        (
            "not executable",
            git_helper(root.join("not-executable"), 0o600),
        ),
        ("relative", PathBuf::from("git-helper")),
    ] {
        assert!(
            matches!(
                production_layout(&root, helper).validate_host_cache_layout(),
                Err(ConfigError::InvalidGitHelperExecutable)
            ),
            "a {case} git helper must be refused"
        );
    }

    let link = root.join("linked");
    std::os::unix::fs::symlink(git_helper(root.join("real"), 0o700), &link)
        .expect("link a private helper");
    assert!(matches!(
        production_layout(&root, link).validate_host_cache_layout(),
        Err(ConfigError::InvalidGitHelperExecutable)
    ));

    production_layout(&root, git_helper(root.join("git-helper"), 0o700))
        .validate_host_cache_layout()
        .expect("a private, owner-executable helper is accepted");
}

#[tokio::test]
async fn persisted_tarball_index_is_used_without_opening_or_reparsing_packument() {
    use std::io::{Seek as _, Write as _};
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let artifact = b"persisted verified tarball";
    let packument = serde_json::to_vec(&serde_json::json!({
        "versions": {"1.0.0": {"dist": {
            "tarball": "https://registry.npmjs.org/@scope/demo/-/demo-1.0.0.tgz",
            "integrity": format!("sha512-{}", STANDARD.encode(Sha512::digest(artifact))),
            "size": artifact.len()
        }}}
    }))
    .expect("encode packument");
    let metadata = QueueUpstream::new([registry_packument_response(&packument)]);
    let (_, bytes) = collect(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope%2fdemo",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &metadata,
            )
            .await
            .expect("fill metadata"),
    )
    .await;
    assert_eq!(bytes.as_ref(), packument);
    let object = std::fs::read_dir(root.path())
        .expect("cache files")
        .map(|entry| entry.expect("cache entry").path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("obj-"))
        })
        .expect("published metadata object");
    let mut file = StdOpenOptions::new()
        .write(true)
        .open(object)
        .expect("open sealed fixture");
    file.seek(std::io::SeekFrom::Start(64 * 1024))
        .expect("seek packument body");
    file.write_all(&vec![b'!'; packument.len()])
        .expect("replace body with invalid JSON");
    file.sync_all().expect("sync fixture");
    drop(file);
    drop(service);
    let reopened = open_service(&root).await;
    let upstream = QueueUpstream::new([ok_response(artifact)]);
    let (_, bytes) = collect(
        reopened
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope%2Fdemo/-/demo-1.0.0.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await
            .expect("persisted index supplies expectation"),
    )
    .await;
    assert_eq!(bytes.as_ref(), artifact);
    assert_eq!(upstream.call_count(), 1, "only the tarball is fetched");
    assert!(matches!(
        reopened
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope/demo/-/demo-unknown.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None
                ),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await,
        Err(MirrorError::MissingIntegrity)
    ));
    assert_eq!(
        upstream.call_count(),
        1,
        "unpublished path does not refetch metadata"
    );
}

#[tokio::test]
async fn project_packument_index_does_not_supply_anonymous_tarball_integrity() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let artifact = b"private tarball";
    let packument = serde_json::to_vec(&serde_json::json!({
        "versions": {"1.0.0": {"dist": {
            "tarball": "https://registry.npmjs.org/private/-/private-1.0.0.tgz",
            "integrity": format!("sha512-{}", STANDARD.encode(Sha512::digest(artifact))),
            "size": artifact.len()
        }}}
    }))
    .expect("encode packument");
    let upstream = QueueUpstream::new([registry_packument_response(&packument)]);
    collect(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/private",
                    MirrorCacheScope::Project("owner/private".to_owned()),
                    true,
                    None,
                ),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await
            .expect("fill private metadata"),
    )
    .await;
    assert!(matches!(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/private/-/private-1.0.0.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None
                ),
                UpstreamHealth::Offline,
                &upstream,
            )
            .await,
        Err(MirrorError::OfflineMiss)
    ));
    assert_eq!(upstream.call_count(), 1);
}

#[tokio::test]
async fn concurrent_lockfile_misses_fetch_and_index_the_packument_once() {
    let root = TestRoot::new();
    let service = Arc::new(open_service(&root).await);
    let artifact = b"one verified coalesced tarball";
    let path = "/coalesced/-/coalesced-1.0.0.tgz";
    let packument = serde_json::to_vec(&serde_json::json!({
        "versions": {"1.0.0": {"dist": {
            "tarball": format!("https://registry.npmjs.org{path}"),
            "integrity": format!("sha512-{}", STANDARD.encode(Sha512::digest(artifact))),
            "size": artifact.len()
        }}}
    }))
    .expect("coalesced packument");
    let upstream = Arc::new(QueueUpstream::new([
        registry_packument_response(&packument),
        ok_response(artifact),
    ]));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let service = Arc::clone(&service);
        let upstream = Arc::clone(&upstream);
        tasks.spawn(async move {
            collect(
                service
                    .execute(
                        request(
                            MirrorProtocol::Npm,
                            target("registry.npmjs.org"),
                            path,
                            MirrorCacheScope::Anonymous,
                            false,
                            None,
                        ),
                        UpstreamHealth::Healthy,
                        upstream.as_ref(),
                    )
                    .await
                    .expect("shared metadata index"),
            )
            .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        let (_, bytes) = result.expect("join concurrent tarball");
        assert_eq!(bytes.as_ref(), artifact);
    }
    assert_eq!(
        upstream.call_count(),
        2,
        "one packument and one artifact fill"
    );
    assert_eq!(upstream.requests()[0].path, "/coalesced");
    assert_eq!(upstream.requests()[1].path, path);
}

#[tokio::test]
async fn full_and_install_packuments_are_distinct_byte_exact_cache_entries() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let tarball = b"representation tarball";
    let full = published(tarball, true);
    let install = published(tarball, false);
    assert_ne!(full, install);
    let upstream = QueueUpstream::new([full_response(&full), install_response(&install)]);

    let (status, headers, bytes) = collect_response(
        service
            .execute(
                packument("/@scope%2fpkg", FULL_ACCEPT),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await
            .expect("fill the full packument"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), full);
    assert_eq!(header_str(&headers, header::ETAG), "\"full\"");
    // Same package, same namespace and origin: still a separate entry, not a hit.
    let (status, headers, bytes) = collect_response(
        service
            .execute(
                packument("/@scope/pkg", INSTALL_V1_ACCEPT),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await
            .expect("fill the install packument"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), install);
    assert_eq!(header_str(&headers, header::ETAG), "\"install\"");
    let sent = upstream.requests();
    assert_eq!(header_str(&sent[0].headers, header::ACCEPT), FULL_ACCEPT);
    assert_eq!(
        header_str(&sent[1].headers, header::ACCEPT),
        INSTALL_V1_ACCEPT
    );

    // Each representation now hits through every spelling of the name, with its own bytes and
    // headers; `Vary: accept` is stored and still never mixes them up.
    for (path, accept, expected, content_type, etag) in [
        (
            "/@scope%2Fpkg",
            FULL_ACCEPT,
            &full,
            "application/json",
            "\"full\"",
        ),
        (
            "/@scope/pkg",
            "application/vnd.npm.install-v1+json",
            &install,
            "application/vnd.npm.install-v1+json",
            "\"install\"",
        ),
        (
            "/@scope%2fpkg",
            INSTALL_V1_ACCEPT,
            &install,
            "application/vnd.npm.install-v1+json",
            "\"install\"",
        ),
        (
            "/@scope/pkg",
            "application/json, */*",
            &full,
            "application/json",
            "\"full\"",
        ),
    ] {
        let (status, headers, bytes) = collect_response(
            service
                .execute(packument(path, accept), UpstreamHealth::Healthy, &upstream)
                .await
                .expect("cached representation"),
        )
        .await;
        assert_eq!(status, MirrorCacheStatus::Hit, "{path} {accept}");
        assert_eq!(bytes.as_ref(), expected.as_slice(), "{path} {accept}");
        assert_eq!(header_str(&headers, header::CONTENT_TYPE), content_type);
        assert_eq!(header_str(&headers, header::ETAG), etag);
        assert_eq!(
            header_str(&headers, header::VARY),
            "accept-encoding, accept"
        );
    }
    assert_eq!(upstream.call_count(), 2, "hits never reach the registry");

    // Another private scope or another origin never shares the full packument.
    let isolated = QueueUpstream::new([full_response(&full), full_response(&full)]);
    for isolated_request in [
        accepting(
            target("registry.npmjs.org"),
            "/@scope%2fpkg",
            MirrorCacheScope::Project("owner/private".to_owned()),
            true,
            FULL_ACCEPT,
        ),
        accepting(
            target("registry.example.test"),
            "/@scope%2fpkg",
            MirrorCacheScope::Anonymous,
            false,
            FULL_ACCEPT,
        ),
    ] {
        let (status, _, bytes) = collect_response(
            service
                .execute(isolated_request, UpstreamHealth::Healthy, &isolated)
                .await
                .expect("fill the isolated full packument"),
        )
        .await;
        assert_eq!(status, MirrorCacheStatus::Filled);
        assert_eq!(bytes.as_ref(), full);
    }
    assert_eq!(isolated.call_count(), 2);
    let (status, _, _) = collect_response(
        service
            .execute(
                accepting(
                    target("registry.npmjs.org"),
                    "/@scope/pkg",
                    MirrorCacheScope::Project("owner/private".to_owned()),
                    true,
                    FULL_ACCEPT,
                ),
                UpstreamHealth::Healthy,
                &isolated,
            )
            .await
            .expect("private full packument"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Hit);
    assert_eq!(isolated.call_count(), 2);
}

#[tokio::test]
async fn a_tarball_reuses_the_index_of_a_cached_full_packument_without_a_metadata_fetch() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let tarball = b"tarball verified by the full packument";
    let full = published(tarball, true);
    let metadata = QueueUpstream::new([full_response(&full)]);
    collect_response(
        service
            .execute(
                packument("/@scope%2fpkg", FULL_ACCEPT),
                UpstreamHealth::Healthy,
                &metadata,
            )
            .await
            .expect("fill the full packument"),
    )
    .await;

    let artifact = QueueUpstream::new([ok_response(tarball)]);
    let (status, _, bytes) = collect_response(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope/pkg/-/pkg-1.0.0.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &artifact,
            )
            .await
            .expect("the full packument's index verifies the tarball"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), tarball);
    assert_eq!(
        artifact
            .requests()
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        ["/@scope/pkg/-/pkg-1.0.0.tgz"],
        "no packument is fetched for a tarball the full index publishes"
    );

    let unpublished = QueueUpstream::new([]);
    assert!(matches!(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope/pkg/-/pkg-9.9.9.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &unpublished,
            )
            .await,
        Err(MirrorError::MissingIntegrity)
    ));
    assert_eq!(unpublished.call_count(), 0);

    // No install packument was invented from the full one: that request is still a miss.
    let install = published(tarball, false);
    let compact = QueueUpstream::new([install_response(&install)]);
    let (status, _, bytes) = collect_response(
        service
            .execute(
                packument("/@scope%2fpkg", INSTALL_V1_ACCEPT),
                UpstreamHealth::Healthy,
                &compact,
            )
            .await
            .expect("fill the install packument"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), install);
}

#[tokio::test]
async fn a_tarball_fetches_the_canonical_install_packument_whatever_it_accepts() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let tarball = b"tarball whose accept names no packument";
    let install = published(tarball, false);
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT, "*/*".parse().expect("accept header"));
    let lockfile_tarball = MirrorRequest::new(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        Method::GET,
        "/@scope/pkg/-/pkg-1.0.0.tgz".to_owned(),
        headers,
        MirrorCacheScope::Anonymous,
        false,
        None,
    )
    .expect("a tarball request negotiates no representation");
    let upstream = QueueUpstream::new([install_response(&install), ok_response(tarball)]);
    let (status, _, bytes) = collect_response(
        service
            .execute(lockfile_tarball, UpstreamHealth::Healthy, &upstream)
            .await
            .expect("a lockfile tarball is served"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), tarball);
    let sent = upstream.requests();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].path, "/@scope%2fpkg");
    assert_eq!(
        header_str(&sent[0].headers, header::ACCEPT),
        INSTALL_V1_ACCEPT
    );
    assert_eq!(sent[1].path, "/@scope/pkg/-/pkg-1.0.0.tgz");

    // The fetched packument is the install entry an installing client now hits.
    let silent = QueueUpstream::new([]);
    let (status, _, bytes) = collect_response(
        service
            .execute(
                packument("/@scope%2fpkg", INSTALL_V1_ACCEPT),
                UpstreamHealth::Healthy,
                &silent,
            )
            .await
            .expect("cached install packument"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Hit);
    assert_eq!(bytes.as_ref(), install);
    assert_eq!(silent.call_count(), 0);
}

#[tokio::test]
async fn representations_that_publish_different_integrity_refuse_the_tarball() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let install_tarball = b"tarball the install packument publishes";
    let full_tarball = b"tarball the full packument publishes";
    let install = published(install_tarball, false);
    let full = published(full_tarball, true);
    for (accept, response) in [
        (INSTALL_V1_ACCEPT, install_response(&install)),
        (FULL_ACCEPT, full_response(&full)),
    ] {
        collect_response(
            service
                .execute(
                    packument("/@scope%2fpkg", accept),
                    UpstreamHealth::Healthy,
                    &QueueUpstream::new([response]),
                )
                .await
                .expect("fill representation"),
        )
        .await;
    }
    let upstream = QueueUpstream::new([ok_response(install_tarball)]);
    assert!(matches!(
        service
            .execute(
                request(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    "/@scope/pkg/-/pkg-1.0.0.tgz",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await,
        Err(MirrorError::MissingIntegrity)
    ));
    assert_eq!(upstream.call_count(), 0);
}

#[test]
fn a_packument_request_selects_one_representation_or_defaults_to_the_install_document() {
    for accept in [
        "*/*",
        "text/plain",
        "application/json, application/vnd.npm.install-v1+json",
        "application/json;q=0",
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(header::ACCEPT, accept.parse().expect("accept header"));
        assert!(
            matches!(
                MirrorRequest::new(
                    MirrorProtocol::Npm,
                    target("registry.npmjs.org"),
                    Method::GET,
                    "/pkg".to_owned(),
                    headers.clone(),
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                Err(MirrorError::UnsupportedMetadataAccept)
            ),
            "{accept}"
        );
        // A tarball negotiates no representation, so its Accept is never a refusal.
        MirrorRequest::new(
            MirrorProtocol::Npm,
            target("registry.npmjs.org"),
            Method::GET,
            "/pkg/-/pkg-1.0.0.tgz".to_owned(),
            headers,
            MirrorCacheScope::Anonymous,
            false,
            None,
        )
        .expect("tarball request");
    }
    let default = request(
        MirrorProtocol::Npm,
        target("registry.npmjs.org"),
        "/pkg",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    assert_eq!(
        header_str(&default.headers, header::ACCEPT),
        INSTALL_V1_ACCEPT,
        "trusted callers without Accept get the install document"
    );
    for (accept, upstream_accept) in [
        (FULL_ACCEPT, FULL_ACCEPT),
        ("application/vnd.npm.install-v1+json", INSTALL_V1_ACCEPT),
        (INSTALL_V1_ACCEPT, INSTALL_V1_ACCEPT),
    ] {
        assert_eq!(
            header_str(&packument("/pkg", accept).headers, header::ACCEPT),
            upstream_accept
        );
    }
}

#[tokio::test]
async fn a_redirect_hop_keeps_the_full_representation_and_its_cache_identity() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let tarball = b"redirected tarball";
    let full = published(tarball, true);
    let redirected = Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header(header::LOCATION, "/@scope%2fpkg")
        .body(body(Bytes::new()))
        .expect("redirect response");
    let MirrorOutcome::Redirect(redirect) = service
        .execute(
            packument("/@scope/pkg", FULL_ACCEPT),
            UpstreamHealth::Healthy,
            &QueueUpstream::new([redirected]),
        )
        .await
        .expect("typed redirect")
    else {
        panic!("expected redirect outcome");
    };
    assert_eq!(
        header_str(&redirect.request.headers, header::ACCEPT),
        FULL_ACCEPT
    );
    let hop = MirrorRequest::from_redirect(redirect.request, MirrorCacheScope::Anonymous, false)
        .expect("re-admitted redirect request");
    let upstream = QueueUpstream::new([full_response(&full)]);
    let (status, _, bytes) = collect_response(
        service
            .execute(hop, UpstreamHealth::Healthy, &upstream)
            .await
            .expect("fill through the redirect hop"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);
    assert_eq!(bytes.as_ref(), full);
    let (status, _, bytes) = collect_response(
        service
            .execute(
                packument("/@scope/pkg", FULL_ACCEPT),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await
            .expect("the hop filled the full entry"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Hit);
    assert_eq!(bytes.as_ref(), full);
    assert_eq!(upstream.call_count(), 1);
}
