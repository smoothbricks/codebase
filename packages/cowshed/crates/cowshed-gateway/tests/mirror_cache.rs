use std::{
    collections::VecDeque,
    fs::OpenOptions as StdOpenOptions,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
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
            high_water_bytes: 1024 * 1024,
            low_water_bytes: 512 * 1024,
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
    let mut file = StdOpenOptions::new()
        .write(true)
        .open(&object)
        .expect("open cached object for corruption fixture");
    file.seek(SeekFrom::End(-1)).expect("seek final byte");
    file.write_all(b"!").expect("corrupt cached byte");
    file.sync_all().expect("sync corruption fixture");

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
async fn immutable_digest_mismatch_never_publishes() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let expected = expectation(b"right bytes");
    let request = request(
        MirrorProtocol::Cargo,
        target("static.crates.io"),
        "/crates/demo/demo-1.0.0.crate",
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
    let upstream = QueueUpstream::new([declared_digest_response(artifact)]);
    assert!(matches!(
        service
            .execute(request, UpstreamHealth::Healthy, &upstream)
            .await,
        Err(MirrorError::MissingIntegrity)
    ));
    assert_eq!(upstream.call_count(), 1);
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
fn npm_cargo_go_fixtures_have_exact_protocol_metadata() {
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

    let crate_bytes = b"crate";
    let cargo = request(
        MirrorProtocol::Cargo,
        target("static.crates.io"),
        "/crates/serde/serde-1.0.0.crate",
        MirrorCacheScope::Anonymous,
        false,
        Some(expectation(crate_bytes)),
    );
    assert_eq!(cargo.metadata.identity, "serde");
    assert_eq!(cargo.metadata.kind, MirrorResourceKind::Immutable);

    let zip = b"module zip";
    let go = request(
        MirrorProtocol::Go,
        target("proxy.golang.org"),
        "/golang.org/x/text/@v/v0.3.0.zip",
        MirrorCacheScope::Anonymous,
        false,
        Some(expectation(zip)),
    );
    assert_eq!(go.metadata.identity, "golang.org/x/text");
    assert_eq!(go.metadata.kind, MirrorResourceKind::Immutable);

    let sumdb = request(
        MirrorProtocol::Go,
        target("sum.golang.org"),
        "/sumdb/sum.golang.org/supported",
        MirrorCacheScope::Anonymous,
        false,
        None,
    );
    assert_eq!(sumdb.metadata.kind, MirrorResourceKind::Metadata);
}

#[test]
fn policy_rewrite_is_typed_exact_and_scope_bound() {
    let policy = WorkspacePolicy {
        grants: Vec::new(),
        mirrors: vec![MirrorRoute {
            local_prefix: "/npm/".to_owned(),
            upstream_origin: "https://registry.npmjs.org:443".to_owned(),
            protocol: MirrorProtocol::Npm,
            admitted_prefixes: vec![
                "/react".to_owned(),
                "/react/-/".to_owned(),
                "/@scope/".to_owned(),
            ],
            credentialed: false,
        }],
    };
    policy.validate().expect("valid typed route");
    let resolved = policy
        .resolve_mirror("/npm/react/-/react-1.0.0.tgz")
        .expect("admitted route");
    assert_eq!(resolved.target, target("registry.npmjs.org"));
    assert_eq!(resolved.path, "/react/-/react-1.0.0.tgz");
    assert_eq!(resolved.protocol, MirrorProtocol::Npm);
    assert_eq!(resolved.admitted_prefix, "/react/-/");
    let baseline = policy
        .resolve_mirror("/npm/lodash")
        .expect("unmatched private scope falls through to public baseline");
    assert_eq!(baseline.target, target("registry.npmjs.org"));
    assert_eq!(baseline.admitted_prefix, "/");
    let scoped = policy
        .resolve_mirror("/npm/@scope%2fpkg")
        .expect("encoded npm scope is admitted without weakening generic paths");
    assert_eq!(scoped.path, "/@scope%2fpkg");
    assert_eq!(scoped.admitted_prefix, "/@scope/");

    let wrong = MirrorRoute {
        local_prefix: "/registry/".to_owned(),
        upstream_origin: "https://registry.npmjs.org:443".to_owned(),
        protocol: MirrorProtocol::Npm,
        admitted_prefixes: vec!["/".to_owned()],
        credentialed: false,
    };
    assert!(wrong.validate().is_err());
}

#[test]
fn disjoint_private_scopes_coexist_without_shadowing_public_baselines() {
    let policy = WorkspacePolicy {
        grants: Vec::new(),
        mirrors: vec![
            MirrorRoute {
                local_prefix: "/npm/".to_owned(),
                upstream_origin: "https://npm.company.test:443".to_owned(),
                protocol: MirrorProtocol::Npm,
                admitted_prefixes: vec!["/@company/".to_owned()],
                credentialed: true,
            },
            MirrorRoute {
                local_prefix: "/npm/".to_owned(),
                upstream_origin: "https://npm.other.test:443".to_owned(),
                protocol: MirrorProtocol::Npm,
                admitted_prefixes: vec!["/@other/".to_owned()],
                credentialed: true,
            },
            MirrorRoute {
                local_prefix: "/cargo/".to_owned(),
                upstream_origin: "https://cargo.company.test:443".to_owned(),
                protocol: MirrorProtocol::Cargo,
                admitted_prefixes: vec!["/privatecrate".to_owned()],
                credentialed: true,
            },
            MirrorRoute {
                local_prefix: "/go/".to_owned(),
                upstream_origin: "https://go.company.test:443".to_owned(),
                protocol: MirrorProtocol::Go,
                admitted_prefixes: vec!["/company.example/".to_owned()],
                credentialed: true,
            },
        ],
    };
    policy.validate().expect("disjoint private scopes");

    assert_eq!(
        policy
            .resolve_mirror("/npm/react")
            .expect("public npm baseline")
            .target,
        target("registry.npmjs.org")
    );
    assert_eq!(
        policy
            .resolve_mirror("/npm/@company%2fpkg")
            .expect("company npm scope")
            .target,
        target("npm.company.test")
    );
    assert_eq!(
        policy
            .resolve_mirror("/npm/@other%2fpkg")
            .expect("other npm scope")
            .target,
        target("npm.other.test")
    );
    assert_eq!(
        policy
            .resolve_mirror("/cargo/se/rd/serde")
            .expect("public Cargo baseline")
            .target,
        target("index.crates.io")
    );
    assert_eq!(
        policy
            .resolve_mirror("/cargo/privatecrate")
            .expect("private Cargo scope")
            .target,
        target("cargo.company.test")
    );
    assert_eq!(
        policy
            .resolve_mirror("/go/golang.org/x/text/@v/list")
            .expect("public Go baseline")
            .target,
        target("proxy.golang.org")
    );
    assert_eq!(
        policy
            .resolve_mirror("/go/company.example/mod/@v/list")
            .expect("private Go scope")
            .target,
        target("go.company.test")
    );

    let mut overlapping = policy.clone();
    overlapping.mirrors.push(MirrorRoute {
        local_prefix: "/npm/".to_owned(),
        upstream_origin: "https://shadow.company.test:443".to_owned(),
        protocol: MirrorProtocol::Npm,
        admitted_prefixes: vec!["/@company/pkg".to_owned()],
        credentialed: true,
    });
    assert!(matches!(
        overlapping.validate(),
        Err(cowshed_gateway::PolicyError::DuplicateMirrorPrefix)
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
fn zero_grant_policy_resolves_only_fixed_public_protocol_origins() {
    let policy = WorkspacePolicy::default();
    let npm = policy
        .resolve_mirror("/npm/@scope%2fpkg")
        .expect("baseline npm packument");
    assert_eq!(npm.target, target("registry.npmjs.org"));
    assert_eq!(npm.path, "/@scope%2fpkg");
    assert!(!npm.credentialed);

    let cargo = policy
        .resolve_mirror(
            "/cargo/crates/demo/1.2.3/download?cowshed-integrity=sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .expect("baseline Cargo download");
    assert_eq!(cargo.target, target("static.crates.io"));
    assert!(cargo.path.starts_with("/crates/demo/demo-1.2.3.crate?"));

    let go = policy
        .resolve_mirror("/go/sumdb/sum.golang.org/lookup/example.com/mod@v1.0.0")
        .expect("baseline Go checksum lookup");
    assert_eq!(go.target, target("sum.golang.org"));
    assert_eq!(go.path, "/lookup/example.com/mod@v1.0.0");
}

#[tokio::test]
async fn npm_packument_rewrites_sha512_content_address_and_verifies_tarball() {
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
    let (_, rewritten) = collect(
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
            .expect("rewrite packument"),
    )
    .await;
    let document: serde_json::Value =
        serde_json::from_slice(&rewritten).expect("rewritten packument JSON");
    let local = document["versions"]["1.2.3"]["dist"]["tarball"]
        .as_str()
        .expect("local tarball URL");
    assert!(local.starts_with("/npm/@scope/pkg/-/pkg-1.2.3.tgz?"));
    assert!(local.contains("cowshed-integrity=sha512-"));
    assert!(local.contains("cowshed-length="));

    let artifact_path = local.strip_prefix("/npm").expect("npm local prefix");
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
    let MirrorOutcome::Response(response) = service
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
        .expect("start sha512 mismatch stream")
    else {
        panic!("expected streaming mismatch response");
    };
    assert!(
        response.response.into_body().collect().await.is_err(),
        "sha512 mismatch must abort before publication"
    );
}

#[tokio::test]
async fn cargo_config_rewrites_download_template_to_content_addressed_local_route() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let upstream = QueueUpstream::new([json_response(
        br#"{"dl":"https://static.crates.io/crates","api":"https://crates.io"}"#,
    )]);
    let (_, bytes) = collect(
        service
            .execute(
                request(
                    MirrorProtocol::Cargo,
                    target("index.crates.io"),
                    "/config.json",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &upstream,
            )
            .await
            .expect("rewrite Cargo sparse config"),
    )
    .await;
    let document: serde_json::Value =
        serde_json::from_slice(&bytes).expect("rewritten config JSON");
    assert_eq!(
        document["dl"],
        "/cargo/crates/{crate}/{version}/download?cowshed-integrity=sha256-{sha256-checksum}"
    );
    assert!(document.get("api").is_none());
}

#[tokio::test]
async fn cargo_sparse_and_go_metadata_rules_reject_unverified_entries() {
    let root = TestRoot::new();
    let service = open_service(&root).await;
    let checksum = "11".repeat(32);
    let sparse = format!(
        "{{\"name\":\"demo\",\"vers\":\"1.0.0\",\"cksum\":\"{checksum}\",\"deps\":[],\"features\":{{}},\"yanked\":false}}\n"
    );
    let cargo = QueueUpstream::new([ok_response(sparse.as_bytes())]);
    let (status, _) = collect(
        service
            .execute(
                request(
                    MirrorProtocol::Cargo,
                    target("index.crates.io"),
                    "/de/mo/demo",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &cargo,
            )
            .await
            .expect("validate Cargo sparse entry"),
    )
    .await;
    assert_eq!(status, MirrorCacheStatus::Filled);

    let invalid = QueueUpstream::new([ok_response(
        br#"{"name":"demo","vers":"1.0.1","cksum":"not-a-checksum"}"#,
    )]);
    assert!(matches!(
        service
            .execute(
                request(
                    MirrorProtocol::Cargo,
                    target("index.crates.io"),
                    "/de/mo/demo-invalid",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &invalid,
            )
            .await,
        Err(MirrorError::InvalidMetadata)
    ));

    let go_list = QueueUpstream::new([ok_response(b"v1.0.0\nv1.1.0\n")]);
    collect(
        service
            .execute(
                request(
                    MirrorProtocol::Go,
                    target("proxy.golang.org"),
                    "/example.com/mod/@v/list",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &go_list,
            )
            .await
            .expect("validate Go version list"),
    )
    .await;

    let bad_sumdb = QueueUpstream::new([ok_response(b"123\nexample.com/mod v1.0.0 bad\n")]);
    assert!(matches!(
        service
            .execute(
                request(
                    MirrorProtocol::Go,
                    target("sum.golang.org"),
                    "/lookup/example.com/mod@v1.0.0",
                    MirrorCacheScope::Anonymous,
                    false,
                    None,
                ),
                UpstreamHealth::Healthy,
                &bad_sumdb,
            )
            .await,
        Err(MirrorError::MissingIntegrity)
    ));
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

#[test]
fn production_cache_root_is_fixed_beneath_control_socket_parent() {
    let fixture = TestRoot::new();
    let gateway_root = std::fs::canonicalize(fixture.path()).expect("canonical fixture root");
    let fixed = gateway_root.join("caches").join("mirror");
    std::fs::create_dir_all(&fixed).expect("create fixed cache root");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&fixed, std::fs::Permissions::from_mode(0o700))
            .expect("secure fixed cache root");
    }
    let mut config = GatewayConfig {
        control_socket: Some(gateway_root.join("gateway.sock")),
        mirror_cache: MirrorCacheConfig::new(fixed),
        ..GatewayConfig::default()
    };
    config
        .validate_host_cache_layout()
        .expect("fixed production cache layout");

    config.mirror_cache = MirrorCacheConfig::new(gateway_root);
    assert!(matches!(
        config.validate_host_cache_layout(),
        Err(ConfigError::InvalidProductionCacheRoot)
    ));
}
