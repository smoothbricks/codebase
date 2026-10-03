use std::{cmp::Ordering, collections::HashMap, fmt, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode, header};
use http_body_util::{BodyExt as _, Empty, combinators::BoxBody};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use url::Url;

use cowshed_gateway_types::{CanonicalTarget, MirrorProtocol, decode_percent};

use crate::{
    cache::{
        Cache, CacheAcquire, CacheBodyError, CacheError, CacheKey, CacheNamespace, CachedResponse,
        NpmExpectationLookup, ObjectExpectation, unix_ms,
    },
    interfaces::UpstreamHealth,
};

const MAX_REDIRECTS: u8 = 5;
pub(crate) const MAX_LOCATION_BYTES: usize = 8 * 1024;
const MAX_OBJECT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const HEALTH_COMMAND_CAPACITY: usize = 64;

pub type MirrorBody = BoxBody<Bytes, CacheBodyError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MirrorCacheScope {
    Anonymous,
    Project(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MirrorResourceKind {
    Metadata,
    Immutable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorProtocolMetadata {
    pub kind: MirrorResourceKind,
    pub identity: String,
    pub expected: Option<ObjectExpectation>,
}

#[derive(Clone, Debug)]
pub struct MirrorRequest {
    pub protocol: MirrorProtocol,
    pub target: CanonicalTarget,
    pub method: Method,
    pub upstream_path: String,
    pub headers: HeaderMap,
    pub metadata: MirrorProtocolMetadata,
    pub cache_scope: MirrorCacheScope,
    pub credentialed: bool,
    pub redirects_remaining: u8,
    /// The packument representation this request names: `Some` exactly for an npm packument.
    /// `headers` only carry what is sent upstream; the cache identity comes from here.
    representation: Option<PackumentRepresentation>,
}

impl MirrorRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "this constructor is the transport boundary and keeps every security-relevant field explicit"
    )]
    pub fn new(
        protocol: MirrorProtocol,
        target: CanonicalTarget,
        method: Method,
        upstream_path: String,
        mut headers: HeaderMap,
        cache_scope: MirrorCacheScope,
        credentialed: bool,
        expected: Option<ObjectExpectation>,
    ) -> Result<Self, MirrorError> {
        if method != Method::GET && method != Method::HEAD {
            return Err(MirrorError::MethodNotAllowed);
        }
        if credentialed && matches!(cache_scope, MirrorCacheScope::Anonymous) {
            return Err(MirrorError::UnscopedCredential);
        }
        strip_request_secrets(&mut headers);
        headers.remove(header::ACCEPT_ENCODING);
        headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );
        let metadata = classify(&upstream_path, expected)?;
        let representation = match (protocol, metadata.kind) {
            (MirrorProtocol::Npm, MirrorResourceKind::Metadata) => {
                let representation = PackumentRepresentation::requested(&headers)?;
                headers.insert(
                    header::ACCEPT,
                    HeaderValue::from_static(representation.upstream_accept()),
                );
                Some(representation)
            }
            _ => None,
        };
        if metadata
            .expected
            .is_some_and(|expected| expected.length > MAX_OBJECT_BYTES)
        {
            return Err(MirrorError::ObjectTooLarge);
        }
        Ok(Self {
            protocol,
            target,
            method,
            upstream_path,
            headers,
            metadata,
            cache_scope,
            credentialed,
            representation,
            redirects_remaining: MAX_REDIRECTS,
        })
    }

    pub fn to_fetch(&self) -> MirrorFetchRequest {
        MirrorFetchRequest {
            protocol: self.protocol,
            target: self.target.clone(),
            method: self.method.clone(),
            path: self.upstream_path.clone(),
            headers: self.headers.clone(),
            redirects_remaining: self.redirects_remaining,
        }
    }
    pub fn from_redirect(
        fetch: MirrorFetchRequest,
        cache_scope: MirrorCacheScope,
        credentialed: bool,
    ) -> Result<Self, MirrorError> {
        let redirects_remaining = fetch.redirects_remaining;
        let mut request = Self::new(
            fetch.protocol,
            fetch.target,
            fetch.method,
            fetch.path,
            fetch.headers,
            cache_scope,
            credentialed,
            None,
        )?;
        request.redirects_remaining = redirects_remaining;
        Ok(request)
    }

    fn cache_namespace(&self) -> CacheNamespace {
        match &self.cache_scope {
            MirrorCacheScope::Anonymous => CacheNamespace::Anonymous,
            MirrorCacheScope::Project(repo_id) => CacheNamespace::Project {
                repo_id: repo_id.clone(),
            },
        }
    }

    fn cache_key(&self) -> Result<CacheKey, CacheError> {
        let (protocol, path) = match self.representation {
            Some(representation) if !self.upstream_path.contains('?') => (
                representation.cache_protocol(),
                npm_packument_path(&self.metadata.identity),
            ),
            Some(representation) => (representation.cache_protocol(), self.upstream_path.clone()),
            None => (self.protocol.as_str(), self.upstream_path.clone()),
        };
        CacheKey::new(
            self.cache_namespace(),
            protocol,
            self.target.origin(),
            path,
            self.metadata.expected.map(|expected| expected.digest),
        )
    }

    /// Key of this package's query-free packument in one representation. Every spelling of the
    /// name (`/@scope/pkg`, `/@scope%2Fpkg`) is the same resource; each representation is its own
    /// entry in the request's namespace and origin.
    fn packument_key(
        &self,
        representation: PackumentRepresentation,
    ) -> Result<CacheKey, CacheError> {
        CacheKey::new(
            self.cache_namespace(),
            representation.cache_protocol(),
            self.target.origin(),
            npm_packument_path(&self.metadata.identity),
            None,
        )
    }
}

#[derive(Clone, Debug)]
pub struct MirrorFetchRequest {
    pub protocol: MirrorProtocol,
    pub target: CanonicalTarget,
    pub method: Method,
    pub path: String,
    pub headers: HeaderMap,
    pub redirects_remaining: u8,
}

#[async_trait]
pub trait MirrorUpstream: Send + Sync {
    async fn fetch(
        &self,
        request: MirrorFetchRequest,
    ) -> Result<Response<MirrorBody>, CacheBodyError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MirrorCacheStatus {
    Hit,
    OfflineHit,
    Filled,
    Revalidated,
    Bypassed,
}

pub struct MirrorResponse {
    pub response: Response<MirrorBody>,
    pub cache_status: MirrorCacheStatus,
}

impl fmt::Debug for MirrorResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MirrorResponse")
            .field("status", &self.response.status())
            .field("cache_status", &self.cache_status)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct MirrorRedirect {
    pub request: MirrorFetchRequest,
}

#[derive(Debug)]
pub enum MirrorOutcome {
    Response(MirrorResponse),
    Redirect(MirrorRedirect),
}

#[derive(Clone, Debug)]
pub struct MirrorService {
    cache: Cache,
    health: mpsc::Sender<HealthCommand>,
}

impl MirrorService {
    pub fn new(cache: Cache) -> Self {
        let (health, mut receiver) = mpsc::channel(HEALTH_COMMAND_CAPACITY);
        tokio::spawn(async move {
            let mut origins = HashMap::new();
            while let Some(command) = receiver.recv().await {
                match command {
                    HealthCommand::Get { origin, reply } => {
                        let _ = reply.send(
                            origins
                                .get(&origin)
                                .copied()
                                .unwrap_or(UpstreamHealth::Unknown),
                        );
                    }
                    HealthCommand::Record { origin, health } => {
                        origins.insert(origin, health);
                    }
                }
            }
        });
        Self { cache, health }
    }

    pub async fn execute<U>(
        &self,
        mut request: MirrorRequest,
        observed_health: UpstreamHealth,
        upstream: &U,
    ) -> Result<MirrorOutcome, MirrorError>
    where
        U: MirrorUpstream + ?Sized,
    {
        let health = self
            .effective_health(&request.target, observed_health)
            .await;
        if request.redirects_remaining > MAX_REDIRECTS {
            return Err(MirrorError::TooManyRedirects);
        }
        if request.protocol == MirrorProtocol::Npm
            && request.metadata.kind == MirrorResourceKind::Immutable
            && request.metadata.expected.is_none()
        {
            request.metadata.expected = Some(
                self.published_npm_expectation(&request, health, upstream)
                    .await?,
            );
        }
        self.execute_expected(request, health, upstream).await
    }

    /// Published integrity is indexed once, as the unchanged packument fills the cache. The index
    /// attached to either cached representation of the package supplies it; only when neither
    /// exists does a lockfile-driven tarball fetch fill the canonical install-v1 packument.
    async fn published_npm_expectation<U>(
        &self,
        tarball: &MirrorRequest,
        health: UpstreamHealth,
        upstream: &U,
    ) -> Result<ObjectExpectation, MirrorError>
    where
        U: MirrorUpstream + ?Sized,
    {
        let path = tarball
            .upstream_path
            .split_once('?')
            .map_or(tarball.upstream_path.as_str(), |(path, _)| path);
        let path = decode_percent(path, |_| false).map_err(|_| MirrorError::InvalidProtocolPath)?;
        let allow_stale = health == UpstreamHealth::Offline;
        let published = match self
            .cached_npm_expectation(tarball, &path, allow_stale)
            .await?
        {
            NpmExpectationLookup::Published(expected) => expected,
            NpmExpectationLookup::MissingPackument => {
                // What a tarball request accepts says nothing about a packument: ask for the
                // canonical install-v1 document, exactly as an installing client would.
                let mut headers = tarball.headers.clone();
                headers.insert(
                    header::ACCEPT,
                    HeaderValue::from_static(PackumentRepresentation::InstallV1.upstream_accept()),
                );
                let packument = MirrorRequest::new(
                    MirrorProtocol::Npm,
                    tarball.target.clone(),
                    Method::GET,
                    npm_packument_path(&tarball.metadata.identity),
                    headers,
                    tarball.cache_scope.clone(),
                    tarball.credentialed,
                    None,
                )?;
                let MirrorOutcome::Response(response) =
                    self.execute_expected(packument, health, upstream).await?
                else {
                    return Err(MirrorError::MissingIntegrity);
                };
                if response.response.status() != StatusCode::OK {
                    return Err(MirrorError::MissingIntegrity);
                }
                // Only the leader must drive the upstream stream to publish the index.
                // Coalesced waiters and a 304 reuse an already-published index, not its body.
                if response.cache_status == MirrorCacheStatus::Filled {
                    let mut body = response.response.into_body();
                    while let Some(frame) = body.frame().await {
                        frame.map_err(MirrorError::Upstream)?;
                    }
                }
                match self
                    .cached_npm_expectation(tarball, &path, allow_stale)
                    .await?
                {
                    NpmExpectationLookup::Published(expected) => expected,
                    NpmExpectationLookup::MissingPackument => None,
                }
            }
        };
        let expected = published.ok_or(MirrorError::MissingIntegrity)?;
        if expected.length > MAX_OBJECT_BYTES {
            return Err(MirrorError::ObjectTooLarge);
        }
        Ok(expected)
    }

    /// Looks the tarball path up in the index attached to each cached representation of its
    /// package; neither packument is opened. A fresh index that does not publish the path refuses
    /// it without a refetch, and so does a pair that publish different integrity for it.
    async fn cached_npm_expectation(
        &self,
        tarball: &MirrorRequest,
        path: &str,
        allow_stale: bool,
    ) -> Result<NpmExpectationLookup, MirrorError> {
        let install_v1 = self
            .cache
            .npm_expectation(
                &tarball.packument_key(PackumentRepresentation::InstallV1)?,
                path,
                allow_stale,
            )
            .await?;
        let full = self
            .cache
            .npm_expectation(
                &tarball.packument_key(PackumentRepresentation::Full)?,
                path,
                allow_stale,
            )
            .await?;
        Ok(match (install_v1, full) {
            (NpmExpectationLookup::MissingPackument, other)
            | (other, NpmExpectationLookup::MissingPackument) => other,
            (
                NpmExpectationLookup::Published(Some(install_v1)),
                NpmExpectationLookup::Published(Some(full)),
            ) => NpmExpectationLookup::Published((install_v1 == full).then_some(full)),
            (NpmExpectationLookup::Published(first), NpmExpectationLookup::Published(second)) => {
                NpmExpectationLookup::Published(first.or(second))
            }
        })
    }

    async fn execute_expected<U>(
        &self,
        mut request: MirrorRequest,
        health: UpstreamHealth,
        upstream: &U,
    ) -> Result<MirrorOutcome, MirrorError>
    where
        U: MirrorUpstream + ?Sized,
    {
        let key = request.cache_key()?;
        loop {
            match self
                .cache
                .acquire(key.clone(), health == UpstreamHealth::Offline)
                .await?
            {
                CacheAcquire::Hit(candidate) => match self.cache.open_candidate(candidate).await {
                    Ok(hit) => {
                        return Ok(MirrorOutcome::Response(MirrorResponse {
                            response: response_from_hit(hit, request.method == Method::HEAD)?,
                            cache_status: if health == UpstreamHealth::Offline {
                                MirrorCacheStatus::OfflineHit
                            } else {
                                MirrorCacheStatus::Hit
                            },
                        }));
                    }
                    Err(CacheError::DigestMismatch | CacheError::InvalidMetadata) => continue,
                    Err(error) => return Err(error.into()),
                },
                CacheAcquire::Wait(wait) => match self.cache.retry_after_wait(wait).await {
                    Ok(())
                    | Err(
                        CacheError::FillAborted
                        | CacheError::DigestMismatch
                        | CacheError::CacheMiss,
                    ) => continue,
                    Err(error) => return Err(error.into()),
                },
                CacheAcquire::Fill(permit) => {
                    if health == UpstreamHealth::Offline {
                        permit.bypass().await?;
                        return Err(MirrorError::OfflineMiss);
                    }
                    if request.method == Method::HEAD {
                        permit.bypass().await?;
                        return self.fetch_bypassed(&request, upstream).await;
                    }
                    let previous = match self.cache.validate_previous(&permit).await {
                        Ok(previous) => previous,
                        Err(CacheError::DigestMismatch | CacheError::InvalidMetadata) => None,
                        Err(error) => return Err(error.into()),
                    };
                    let mut fetch = request.to_fetch();
                    if let Some(previous) = &previous {
                        add_conditionals(&mut fetch.headers, previous);
                    }
                    let response = self
                        .fetch_observed(&request.target, fetch, upstream)
                        .await?;
                    if response.status() == StatusCode::NOT_MODIFIED {
                        if previous.is_none() {
                            permit.bypass().await?;
                            return Err(MirrorError::UnexpectedNotModified);
                        }
                        let candidate = permit.not_modified().await?;
                        let hit = self.cache.open_candidate(candidate).await?;
                        return Ok(MirrorOutcome::Response(MirrorResponse {
                            response: response_from_hit(hit, false)?,
                            cache_status: MirrorCacheStatus::Revalidated,
                        }));
                    }
                    if response.status().is_redirection() {
                        permit.bypass().await?;
                        return redirect_outcome(&request, &response);
                    }
                    validate_representation(&response)?;
                    if response.status() != StatusCode::OK || !cacheable(&request, &response) {
                        permit.bypass().await?;
                        return Ok(MirrorOutcome::Response(MirrorResponse {
                            response: sanitize_response(response),
                            cache_status: MirrorCacheStatus::Bypassed,
                        }));
                    }
                    if request.metadata.kind == MirrorResourceKind::Immutable
                        && request.metadata.expected.is_none()
                    {
                        permit.bypass().await?;
                        return Err(MirrorError::MissingIntegrity);
                    }
                    if let Some(expected) = &mut request.metadata.expected
                        && expected.length == 0
                    {
                        expected.length = response_content_length(&response)?;
                    }
                    let max_bytes = response_limit(&request, &response)?;
                    let (mut parts, body) = response.into_parts();
                    strip_response_secrets(&mut parts.headers);
                    let cached = CachedResponse {
                        status: parts.status,
                        headers: parts.headers.clone(),
                        content_length: 0,
                        content_sha256: [0; 32],
                        expected: request.metadata.expected,
                        stored_unix_ms: unix_ms(SystemTime::now())
                            .map_err(|_| MirrorError::Clock)?,
                        npm_tarballs: None,
                        npm_index_bytes: 0,
                    };
                    let mut body = self
                        .cache
                        .start_fill(permit, cached, max_bytes, body)
                        .await?;
                    if request.representation.is_some()
                        && parts
                            .headers
                            .get(header::CONTENT_TYPE)
                            .and_then(|value| value.to_str().ok())
                            .is_some_and(|value| {
                                value.starts_with("application/json")
                                    || value.starts_with("application/vnd.npm.install-v1+json")
                            })
                    {
                        body = body.with_npm_index(request.target.clone());
                    }
                    let body = body.boxed();
                    return Ok(MirrorOutcome::Response(MirrorResponse {
                        response: Response::from_parts(parts, body),
                        cache_status: MirrorCacheStatus::Filled,
                    }));
                }
            }
        }
    }

    async fn fetch_bypassed<U>(
        &self,
        request: &MirrorRequest,
        upstream: &U,
    ) -> Result<MirrorOutcome, MirrorError>
    where
        U: MirrorUpstream + ?Sized,
    {
        let response = self
            .fetch_observed(&request.target, request.to_fetch(), upstream)
            .await?;
        if response.status().is_redirection() {
            redirect_outcome(request, &response)
        } else {
            Ok(MirrorOutcome::Response(MirrorResponse {
                response: sanitize_response(response),
                cache_status: MirrorCacheStatus::Bypassed,
            }))
        }
    }

    async fn fetch_observed<U>(
        &self,
        target: &CanonicalTarget,
        request: MirrorFetchRequest,
        upstream: &U,
    ) -> Result<Response<MirrorBody>, MirrorError>
    where
        U: MirrorUpstream + ?Sized,
    {
        match upstream.fetch(request).await {
            Ok(response) => {
                self.record_health(target, UpstreamHealth::Healthy).await;
                Ok(response)
            }
            Err(error) => {
                self.record_health(target, UpstreamHealth::Offline).await;
                Err(MirrorError::Upstream(error))
            }
        }
    }

    async fn effective_health(
        &self,
        target: &CanonicalTarget,
        observed: UpstreamHealth,
    ) -> UpstreamHealth {
        if observed != UpstreamHealth::Unknown {
            self.record_health(target, observed).await;
            return observed;
        }
        let (reply, receive) = oneshot::channel();
        if self
            .health
            .send(HealthCommand::Get {
                origin: target.origin(),
                reply,
            })
            .await
            .is_err()
        {
            return UpstreamHealth::Unknown;
        }
        receive.await.unwrap_or(UpstreamHealth::Unknown)
    }

    async fn record_health(&self, target: &CanonicalTarget, health: UpstreamHealth) {
        let _ = self
            .health
            .send(HealthCommand::Record {
                origin: target.origin(),
                health,
            })
            .await;
    }
}

enum HealthCommand {
    Get {
        origin: String,
        reply: oneshot::Sender<UpstreamHealth>,
    },
    Record {
        origin: String,
        health: UpstreamHealth,
    },
}

fn response_from_hit(
    hit: crate::cache::CacheHit,
    head: bool,
) -> Result<Response<MirrorBody>, MirrorError> {
    let mut builder = Response::builder().status(hit.response.status);
    *builder
        .headers_mut()
        .expect("response builder accepts headers") = hit.response.headers;
    builder.headers_mut().expect("headers exist").insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&hit.response.content_length.to_string())
            .map_err(|_| MirrorError::InvalidCachedResponse)?,
    );
    if head {
        drop(hit.body);
        builder
            .body(empty_body())
            .map_err(|_| MirrorError::InvalidCachedResponse)
    } else {
        builder
            .body(hit.body.boxed())
            .map_err(|_| MirrorError::InvalidCachedResponse)
    }
}

/// The path npm clients request a package's packument at: `/@scope%2fname` or `/name`.
fn npm_packument_path(identity: &str) -> String {
    format!("/{}", identity.replacen('/', "%2f", 1))
}

pub(crate) fn npm_resource_kind(path: &str) -> Option<MirrorResourceKind> {
    if path.contains('?') || validate_mirror_path(path).is_err() {
        return None;
    }
    classify_npm(path).ok().map(|(kind, _)| kind)
}

/// The two logical npm packument representations. Each is its own upstream request, its own cache
/// entry, and is served as the registry's own bytes and headers; neither is derived from the
/// other.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PackumentRepresentation {
    /// `application/vnd.npm.install-v1+json`: the abbreviated document `npm install` reads.
    InstallV1,
    /// `application/json`: the full packument (`fullMetadata`, `npm view`).
    Full,
}

impl PackumentRepresentation {
    /// A request without `Accept` is a trusted recovery caller and wants the install document;
    /// one with `Accept` must select a single representation.
    fn requested(headers: &HeaderMap) -> Result<Self, MirrorError> {
        if !headers.contains_key(header::ACCEPT) {
            return Ok(Self::InstallV1);
        }
        Self::select(headers).ok_or(MirrorError::UnsupportedMetadataAccept)
    }

    /// The canonical media type naming this representation.
    const fn media(self) -> &'static str {
        match self {
            Self::InstallV1 => "application/vnd.npm.install-v1+json",
            Self::Full => "application/json",
        }
    }

    /// The `Accept` the mirror sends upstream: what npm itself sends for this representation.
    /// It always selects the same representation again, so redirect hops keep it.
    const fn upstream_accept(self) -> &'static str {
        match self {
            Self::InstallV1 => {
                "application/vnd.npm.install-v1+json; q=1.0, application/json; q=0.8, */*"
            }
            Self::Full => "application/json",
        }
    }

    /// The install document keeps the protocol name it was always cached under.
    const fn cache_protocol(self) -> &'static str {
        match self {
            Self::InstallV1 => MirrorProtocol::Npm.as_str(),
            Self::Full => "npm-json",
        }
    }

    /// The one representation an `Accept` header selects: the supported media type with the
    /// strictly highest quality, not outranked by a wildcard. Absent, unknown ranges or
    /// parameters, `q=0`, and ties select nothing.
    fn select(headers: &HeaderMap) -> Option<Self> {
        let (mut install_v1, mut full, mut wildcard) = (0_u16, 0_u16, 0_u16);
        for value in headers.get_all(header::ACCEPT) {
            let ranges = value
                .to_str()
                .ok()?
                .split(',')
                .map(str::trim)
                .filter(|range| !range.is_empty());
            for range in ranges {
                let (media, quality) = accept_range(range)?;
                let slot = if media.eq_ignore_ascii_case(Self::InstallV1.media()) {
                    &mut install_v1
                } else if media.eq_ignore_ascii_case(Self::Full.media()) {
                    &mut full
                } else if media == "*/*" || media.eq_ignore_ascii_case("application/*") {
                    &mut wildcard
                } else if quality == 0 {
                    continue;
                } else {
                    return None;
                };
                *slot = (*slot).max(quality);
            }
        }
        let (selected, quality) = match install_v1.cmp(&full) {
            Ordering::Greater => (Self::InstallV1, install_v1),
            Ordering::Less => (Self::Full, full),
            Ordering::Equal => return None,
        };
        (quality >= wildcard).then_some(selected)
    }
}

/// The canonical media type of the one packument representation `headers` unambiguously select.
/// `None` keeps the request off the packument cache.
pub(crate) fn npm_metadata_accept(headers: &HeaderMap) -> Option<&'static str> {
    PackumentRepresentation::select(headers).map(PackumentRepresentation::media)
}

/// One `Accept` media range and its quality in thousandths. Only a bare `q` parameter is
/// understood; any other parameter makes the range unsupported.
fn accept_range(range: &str) -> Option<(&str, u16)> {
    let mut parts = range.split(';');
    let media = parts.next()?.trim();
    let quality = match parts.next() {
        None => 1000,
        Some(parameter) => {
            let (name, value) = parameter.trim().split_once('=')?;
            if !name.eq_ignore_ascii_case("q") || parts.next().is_some() {
                return None;
            }
            parse_quality(value)?
        }
    };
    Some((media, quality))
}

/// RFC 9110 `qvalue` in thousandths: `0[.ddd]` or `1[.000]`.
fn parse_quality(value: &str) -> Option<u16> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > 3 || !fraction.bytes().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    let thousandths = fraction
        .bytes()
        .zip([100_u16, 10, 1])
        .map(|(digit, scale)| u16::from(digit - b'0') * scale)
        .sum::<u16>();
    match whole {
        "0" => Some(thousandths),
        "1" if thousandths == 0 => Some(1000),
        _ => None,
    }
}

fn response_content_length(response: &Response<MirrorBody>) -> Result<u64, MirrorError> {
    response_content_length_optional(response)?.ok_or(MirrorError::MissingContentLength)
}

fn response_content_length_optional(
    response: &Response<MirrorBody>,
) -> Result<Option<u64>, MirrorError> {
    response
        .headers()
        .get(header::CONTENT_LENGTH)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| MirrorError::InvalidContentLength)?
                .parse::<u64>()
                .map_err(|_| MirrorError::InvalidContentLength)
        })
        .transpose()
}

fn validate_representation(response: &Response<MirrorBody>) -> Result<(), MirrorError> {
    if response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|value| value.as_bytes() != b"identity")
    {
        return Err(MirrorError::UnsupportedEncoding);
    }
    Ok(())
}

fn empty_body() -> MirrorBody {
    Empty::<Bytes>::new()
        .map_err(|never| -> CacheBodyError { match never {} })
        .boxed()
}

fn add_conditionals(headers: &mut HeaderMap, response: &CachedResponse) {
    headers.remove(header::IF_NONE_MATCH);
    headers.remove(header::IF_MODIFIED_SINCE);
    if let Some(etag) = response.etag() {
        headers.insert(header::IF_NONE_MATCH, etag.clone());
    } else if let Some(last_modified) = response.last_modified() {
        headers.insert(header::IF_MODIFIED_SINCE, last_modified.clone());
    }
}

fn response_limit(
    request: &MirrorRequest,
    response: &Response<MirrorBody>,
) -> Result<u64, MirrorError> {
    let header_length = response_content_length_optional(response)?;
    if let Some(expected) = request.metadata.expected {
        let length = header_length.ok_or(MirrorError::MissingContentLength)?;
        if length != expected.length {
            return Err(MirrorError::LengthMismatch);
        }
        return Ok(expected.length);
    }
    Ok(header_length.unwrap_or(u64::MAX))
}

fn cacheable(request: &MirrorRequest, response: &Response<MirrorBody>) -> bool {
    if response.headers().contains_key(header::SET_COOKIE) {
        return false;
    }
    // `Vary: accept` is safe only because each packument representation is its own cache entry.
    let unsupported_vary = response
        .headers()
        .get_all(header::VARY)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|name| {
            !name.eq_ignore_ascii_case("accept-encoding")
                && !(request.representation.is_some() && name.eq_ignore_ascii_case("accept"))
        });
    if unsupported_vary {
        return false;
    }
    let cache_control = response
        .headers()
        .get_all(header::CACHE_CONTROL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect::<Vec<_>>()
        .join(",")
        .to_ascii_lowercase();
    if cache_control
        .split(',')
        .any(|token| token.trim() == "no-store")
    {
        return false;
    }
    if matches!(request.cache_scope, MirrorCacheScope::Anonymous)
        && cache_control
            .split(',')
            .any(|token| token.trim() == "private")
    {
        return false;
    }
    true
}

fn sanitize_response(mut response: Response<MirrorBody>) -> Response<MirrorBody> {
    strip_response_secrets(response.headers_mut());
    response
}

fn redirect_outcome(
    request: &MirrorRequest,
    response: &Response<MirrorBody>,
) -> Result<MirrorOutcome, MirrorError> {
    if request.redirects_remaining == 0 {
        return Err(MirrorError::TooManyRedirects);
    }
    if !matches!(
        response.status(),
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    ) {
        return Err(MirrorError::UnsafeRedirect);
    }
    let location = response
        .headers()
        .get(header::LOCATION)
        .ok_or(MirrorError::InvalidRedirect)?
        .to_str()
        .map_err(|_| MirrorError::InvalidRedirect)?;
    if location.len() > MAX_LOCATION_BYTES {
        return Err(MirrorError::InvalidRedirect);
    }
    let base = Url::parse(&format!(
        "{}{path}",
        request.target.origin(),
        path = request.upstream_path
    ))
    .map_err(|_| MirrorError::InvalidRedirect)?;
    let redirected = base
        .join(location)
        .map_err(|_| MirrorError::InvalidRedirect)?;
    let target = CanonicalTarget::from_url(&redirected).map_err(|_| MirrorError::UnsafeRedirect)?;
    if target != request.target {
        return Err(MirrorError::UnsafeRedirect);
    }
    let path = redirected
        .path_and_query()
        .map(|value| value.as_str().to_owned())
        .unwrap_or_else(|| redirected.path().to_owned());
    let metadata = classify(&path, request.metadata.expected)?;
    if metadata.identity != request.metadata.identity {
        return Err(MirrorError::UnsafeRedirect);
    }
    let mut headers = request.headers.clone();
    strip_request_secrets(&mut headers);
    headers.remove(header::IF_NONE_MATCH);
    headers.remove(header::IF_MODIFIED_SINCE);
    Ok(MirrorOutcome::Redirect(MirrorRedirect {
        request: MirrorFetchRequest {
            protocol: request.protocol,
            target,
            method: request.method.clone(),
            path,
            headers,
            redirects_remaining: request.redirects_remaining - 1,
        },
    }))
}

fn classify(
    path_and_query: &str,
    supplied: Option<ObjectExpectation>,
) -> Result<MirrorProtocolMetadata, MirrorError> {
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _)| path);
    validate_mirror_path(path)?;
    let (kind, identity) = classify_npm(path)?;
    Ok(MirrorProtocolMetadata {
        kind,
        identity,
        expected: supplied,
    })
}

fn classify_npm(path: &str) -> Result<(MirrorResourceKind, String), MirrorError> {
    let relative = path
        .strip_prefix('/')
        .ok_or(MirrorError::InvalidProtocolPath)?;
    let (package, artifact) = match relative.split_once("/-/") {
        Some((package, artifact)) => (package, Some(artifact)),
        None => (relative, None),
    };
    let decoded =
        decode_percent(package, |_| false).map_err(|_| MirrorError::InvalidProtocolPath)?;
    let mut segments = decoded.split('/');
    let first = segments.next().ok_or(MirrorError::InvalidProtocolPath)?;
    if first.is_empty() {
        return Err(MirrorError::InvalidProtocolPath);
    }
    if first.starts_with('@') {
        if first.len() < 2 || segments.next().is_none_or(str::is_empty) || segments.next().is_some()
        {
            return Err(MirrorError::InvalidProtocolPath);
        }
    } else if segments.next().is_some() {
        return Err(MirrorError::InvalidProtocolPath);
    }
    let kind = match artifact {
        Some(artifact) if artifact.ends_with(".tgz") && !artifact.contains('/') => {
            MirrorResourceKind::Immutable
        }
        Some(_) => return Err(MirrorError::InvalidProtocolPath),
        None => MirrorResourceKind::Metadata,
    };
    Ok((kind, decoded))
}

pub(crate) fn validate_mirror_path(path: &str) -> Result<(), MirrorError> {
    if path.len() > MAX_LOCATION_BYTES || !path.starts_with('/') {
        return Err(MirrorError::InvalidProtocolPath);
    }
    if path.contains('\\') || path.contains('\0') || path.contains("//") {
        return Err(MirrorError::InvalidProtocolPath);
    }
    let lower = path.to_ascii_lowercase();
    for forbidden in ["%00", "%5c", "%2e", "%252f", "%255c"] {
        if lower.contains(forbidden) {
            return Err(MirrorError::InvalidProtocolPath);
        }
    }
    if path
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err(MirrorError::InvalidProtocolPath);
    }
    Ok(())
}

fn strip_request_secrets(headers: &mut HeaderMap) {
    crate::proxy::strip_upstream_request_headers(headers);
}

fn strip_response_secrets(headers: &mut HeaderMap) {
    crate::proxy::strip_upstream_response_headers(headers);
}

trait UrlPathAndQuery {
    fn path_and_query(&self) -> Option<http::uri::PathAndQuery>;
}

impl UrlPathAndQuery for Url {
    fn path_and_query(&self) -> Option<http::uri::PathAndQuery> {
        let value = match self.query() {
            Some(query) => format!("{}?{query}", self.path()),
            None => self.path().to_owned(),
        };
        value.parse().ok()
    }
}

#[derive(Debug, Error)]
pub enum MirrorError {
    #[error("mirror only accepts GET and HEAD")]
    MethodNotAllowed,
    #[error("credential-bearing mirrors require a project cache scope")]
    UnscopedCredential,
    #[error("mirror immutable object lacks expected length and a supported digest")]
    MissingIntegrity,
    #[error("mirror object exceeds the 2 GiB maximum")]
    ObjectTooLarge,
    #[error("mirror upstream ignored canonical identity encoding")]
    UnsupportedEncoding,
    #[error("mirror protocol path is invalid")]
    InvalidProtocolPath,
    #[error("mirror npm metadata Accept does not select exactly one packument representation")]
    UnsupportedMetadataAccept,
    #[error("mirror cache miss while upstream is offline")]
    OfflineMiss,
    #[error("mirror upstream failed: {0}")]
    Upstream(CacheBodyError),
    #[error("mirror upstream returned 304 without a verified cached object")]
    UnexpectedNotModified,
    #[error("mirror immutable response lacks Content-Length")]
    MissingContentLength,
    #[error("mirror immutable response has an invalid Content-Length")]
    InvalidContentLength,
    #[error("mirror immutable response length differs from protocol metadata")]
    LengthMismatch,
    #[error("mirror redirect is invalid")]
    InvalidRedirect,
    #[error("mirror redirect crosses its admitted origin or scope")]
    UnsafeRedirect,
    #[error("mirror redirect limit exceeded")]
    TooManyRedirects,
    #[error("cached mirror response is invalid")]
    InvalidCachedResponse,
    #[error("system clock is before the Unix epoch")]
    Clock,
    #[error(transparent)]
    Cache(#[from] CacheError),
}

impl fmt::Debug for dyn MirrorUpstream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MirrorUpstream")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirror_and_generic_hops_apply_the_same_header_policy() {
        let mut request = HeaderMap::new();
        for name in [
            "authorization",
            "npm-auth-type",
            "npm-auth-token",
            "x-npm-token",
            "traceparent",
            "tracestate",
            "x-hop",
        ] {
            request.insert(name, HeaderValue::from_static("secret"));
        }
        request.insert(header::CONNECTION, HeaderValue::from_static("x-hop"));

        let mut mirror_request = request.clone();
        strip_request_secrets(&mut mirror_request);
        let mut generic_request = request;
        crate::proxy::strip_upstream_request_headers(&mut generic_request);
        assert_eq!(mirror_request, generic_request);
        assert!(mirror_request.is_empty());

        let mut response = HeaderMap::new();
        for name in [
            "set-cookie",
            "proxy-authenticate",
            "www-authenticate",
            "x-hop",
        ] {
            response.insert(name, HeaderValue::from_static("secret"));
        }
        response.insert(header::CONNECTION, HeaderValue::from_static("x-hop"));

        let mut mirror_response = response.clone();
        strip_response_secrets(&mut mirror_response);
        let mut generic_response = response;
        crate::proxy::strip_upstream_response_headers(&mut generic_response);
        assert_eq!(mirror_response, generic_response);
        assert!(mirror_response.is_empty());
    }

    fn accepting<'a>(ranges: impl IntoIterator<Item = &'a str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for range in ranges {
            headers.append(
                header::ACCEPT,
                HeaderValue::from_str(range).expect("accept fixture"),
            );
        }
        headers
    }

    #[test]
    fn accept_selects_only_one_unambiguous_packument_representation() {
        const INSTALL_V1: &str = "application/vnd.npm.install-v1+json";
        for accept in [
            "application/json",
            "Application/JSON",
            "application/json, */*",
            "application/json; q=0.9",
            "application/json,",
            "application/json, application/vnd.npm.install-v1+json; q=0.5",
            "application/vnd.npm.install-v1+json;q=0, application/json",
            "application/json, text/html;q=0",
        ] {
            assert_eq!(
                npm_metadata_accept(&accepting([accept])),
                Some("application/json"),
                "{accept}"
            );
        }
        for accept in [
            "application/vnd.npm.install-v1+json",
            "application/vnd.npm.install-v1+json; q=1.0, application/json; q=0.8, */*",
            "application/vnd.npm.install-v1+json;q=1.000",
            "application/vnd.npm.install-v1+json, application/json;q=0.5",
            "application/vnd.npm.install-v1+json;q=0.001, application/json;q=0.",
        ] {
            assert_eq!(
                npm_metadata_accept(&accepting([accept])),
                Some(INSTALL_V1),
                "{accept}"
            );
        }
        for accept in [
            "",
            "*/*",
            "application/*",
            "text/plain",
            "application/json, text/html",
            "application/json, application/vnd.npm.install-v1+json",
            "application/json;q=0",
            "application/json;q=0, application/vnd.npm.install-v1+json;q=0",
            "application/json;q=0.5, */*",
            "application/json;q=1.5",
            "application/json;q=1.001",
            "application/json;q=0.1234",
            "application/json;q=",
            "application/json;q=x",
            "application/json;q = 1",
            "application/json;charset=utf-8",
            "application/json;q=1;ext=1",
            "application/json;q=1;q=1",
        ] {
            assert_eq!(
                npm_metadata_accept(&accepting([accept])),
                None,
                "{accept:?}"
            );
        }
        assert_eq!(npm_metadata_accept(&HeaderMap::new()), None);
        assert_eq!(
            npm_metadata_accept(&accepting(["application/json", "*/*"])),
            Some("application/json"),
            "ranges split over header lines are one list"
        );
        assert_eq!(
            npm_metadata_accept(&accepting(["application/json", INSTALL_V1])),
            None
        );
        let mut opaque = HeaderMap::new();
        opaque.insert(
            header::ACCEPT,
            HeaderValue::from_bytes(b"application/json, \xff").expect("opaque bytes"),
        );
        assert_eq!(npm_metadata_accept(&opaque), None);
    }

    #[test]
    fn the_accept_sent_upstream_selects_its_own_representation_again() {
        for representation in [
            PackumentRepresentation::InstallV1,
            PackumentRepresentation::Full,
        ] {
            let headers = accepting([representation.upstream_accept()]);
            assert_eq!(
                PackumentRepresentation::select(&headers),
                Some(representation)
            );
            assert_eq!(npm_metadata_accept(&headers), Some(representation.media()));
        }
        assert_ne!(
            PackumentRepresentation::InstallV1.cache_protocol(),
            PackumentRepresentation::Full.cache_protocol()
        );
    }
}
