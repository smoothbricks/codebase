use std::{collections::HashMap, fmt, time::SystemTime};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Response, StatusCode, header};
use http_body_util::{BodyExt as _, Empty, combinators::BoxBody};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use url::Url;

use crate::{
    cache::{
        Cache, CacheAcquire, CacheBodyError, CacheError, CacheKey, CacheNamespace, CachedResponse,
        ObjectDigest, ObjectExpectation,
    },
    interfaces::UpstreamHealth,
    policy::{CanonicalTarget, MirrorProtocol, normalize_path},
};

const MAX_REDIRECTS: u8 = 5;
const MAX_LOCATION_BYTES: usize = 8 * 1024;
const MAX_OBJECT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_METADATA_BYTES: u64 = 8 * 1024 * 1024;
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
        let metadata = classify(protocol, &upstream_path, expected)?;
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

    fn cache_key(&self) -> Result<CacheKey, CacheError> {
        let namespace = match &self.cache_scope {
            MirrorCacheScope::Anonymous => CacheNamespace::Anonymous,
            MirrorCacheScope::Project(repo_id) => CacheNamespace::Project {
                repo_id: repo_id.clone(),
            },
        };
        CacheKey::new(
            namespace,
            self.protocol.as_str(),
            self.target.origin(),
            self.upstream_path.clone(),
            self.metadata.expected.map(|expected| expected.digest),
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
                    let mut response = self
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
                    if response.status() == StatusCode::OK
                        && request.metadata.kind == MirrorResourceKind::Metadata
                        && cacheable(&request, &response)
                    {
                        response = rewrite_metadata_response(&request, response).await?;
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
                        stored_unix_ms: unix_ms(SystemTime::now())?,
                    };
                    let body = self
                        .cache
                        .start_fill(permit, cached, max_bytes, body)
                        .await?
                        .boxed();
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
async fn rewrite_metadata_response(
    request: &MirrorRequest,
    response: Response<MirrorBody>,
) -> Result<Response<MirrorBody>, MirrorError> {
    if request.protocol == MirrorProtocol::Npm
        && !response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        return Ok(response);
    }
    if response_content_length_optional(&response)?
        .is_some_and(|length| length > MAX_METADATA_BYTES)
    {
        return Err(MirrorError::MetadataTooLarge);
    }
    let (mut parts, mut body) = response.into_parts();
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(MirrorError::Upstream)?;
        if let Ok(data) = frame.into_data() {
            if bytes
                .len()
                .checked_add(data.len())
                .is_none_or(|length| length > MAX_METADATA_BYTES as usize)
            {
                return Err(MirrorError::MetadataTooLarge);
            }
            bytes.extend_from_slice(&data);
        }
    }
    let rewritten = match request.protocol {
        MirrorProtocol::Npm => rewrite_npm_packument(request, &bytes)?,
        MirrorProtocol::Cargo if request.upstream_path == "/config.json" => {
            rewrite_cargo_config(&bytes)?
        }
        MirrorProtocol::Cargo => {
            validate_cargo_index(&bytes)?;
            bytes
        }
        MirrorProtocol::Go => {
            validate_go_metadata(&request.upstream_path, &bytes)?;
            bytes
        }
    };
    parts.headers.remove(header::ETAG);
    parts.headers.remove(header::LAST_MODIFIED);
    parts.headers.remove(header::CONTENT_ENCODING);
    parts.headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&rewritten.len().to_string())
            .map_err(|_| MirrorError::InvalidContentLength)?,
    );
    Ok(Response::from_parts(
        parts,
        http_body_util::Full::new(Bytes::from(rewritten))
            .map_err(|never| -> CacheBodyError { match never {} })
            .boxed(),
    ))
}

fn rewrite_npm_packument(request: &MirrorRequest, bytes: &[u8]) -> Result<Vec<u8>, MirrorError> {
    let mut document: Value =
        serde_json::from_slice(bytes).map_err(|_| MirrorError::InvalidMetadata)?;
    let versions = document
        .get_mut("versions")
        .and_then(Value::as_object_mut)
        .ok_or(MirrorError::InvalidMetadata)?;
    for version in versions.values_mut() {
        let dist = version
            .get_mut("dist")
            .and_then(Value::as_object_mut)
            .ok_or(MirrorError::InvalidMetadata)?;
        let tarball = dist
            .get("tarball")
            .and_then(Value::as_str)
            .ok_or(MirrorError::InvalidMetadata)?;
        let tarball_url = Url::parse(tarball).map_err(|_| MirrorError::InvalidMetadata)?;
        let target =
            CanonicalTarget::from_url(&tarball_url).map_err(|_| MirrorError::InvalidMetadata)?;
        if target != request.target {
            return Err(MirrorError::UnsafeMetadataOrigin);
        }
        let integrity = dist
            .get("integrity")
            .and_then(Value::as_str)
            .ok_or(MirrorError::MissingIntegrity)?;
        parse_sri(integrity).ok_or(MirrorError::MissingIntegrity)?;
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("cowshed-integrity", integrity);
        if let Some(length) = dist.get("size").and_then(Value::as_u64) {
            query.append_pair("cowshed-length", &length.to_string());
        }
        let query = query.finish();
        let local = format!("/npm{}?{query}", tarball_url.path());
        dist.insert("tarball".to_owned(), Value::String(local));
    }
    serde_json::to_vec(&document).map_err(|_| MirrorError::InvalidMetadata)
}

fn rewrite_cargo_config(bytes: &[u8]) -> Result<Vec<u8>, MirrorError> {
    let mut document: Value =
        serde_json::from_slice(bytes).map_err(|_| MirrorError::InvalidMetadata)?;
    let object = document
        .as_object_mut()
        .ok_or(MirrorError::InvalidMetadata)?;
    object.insert(
        "dl".to_owned(),
        Value::String(
            "/cargo/crates/{crate}/{version}/download?cowshed-integrity=sha256-{sha256-checksum}"
                .to_owned(),
        ),
    );
    object.remove("api");
    serde_json::to_vec(&document).map_err(|_| MirrorError::InvalidMetadata)
}

fn validate_cargo_index(bytes: &[u8]) -> Result<(), MirrorError> {
    let text = std::str::from_utf8(bytes).map_err(|_| MirrorError::InvalidMetadata)?;
    let mut entries = 0usize;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let entry: Value = serde_json::from_str(line).map_err(|_| MirrorError::InvalidMetadata)?;
        let object = entry.as_object().ok_or(MirrorError::InvalidMetadata)?;
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or(MirrorError::InvalidMetadata)?;
        let version = object
            .get("vers")
            .and_then(Value::as_str)
            .ok_or(MirrorError::InvalidMetadata)?;
        let checksum = object
            .get("cksum")
            .and_then(Value::as_str)
            .ok_or(MirrorError::MissingIntegrity)?;
        if name.is_empty() || version.is_empty() || decode_hex_32(checksum).is_none() {
            return Err(MirrorError::InvalidMetadata);
        }
        entries += 1;
    }
    if entries == 0 {
        return Err(MirrorError::InvalidMetadata);
    }
    Ok(())
}

fn validate_go_metadata(path: &str, bytes: &[u8]) -> Result<(), MirrorError> {
    if path.starts_with("/tile/") || path == "/latest" {
        return Ok(());
    }
    let text = std::str::from_utf8(bytes).map_err(|_| MirrorError::InvalidMetadata)?;
    if path.starts_with("/lookup/") {
        let has_checksum = text.lines().any(|line| {
            line.split_once(" h1:").is_some_and(|(_, encoded)| {
                STANDARD
                    .decode(encoded.trim())
                    .is_ok_and(|digest| digest.len() == 32)
            })
        });
        return has_checksum
            .then_some(())
            .ok_or(MirrorError::MissingIntegrity);
    }
    if path.ends_with("/@v/list") {
        return text
            .lines()
            .filter(|line| !line.is_empty())
            .all(|line| line.starts_with('v') && !line.contains(char::is_whitespace))
            .then_some(())
            .ok_or(MirrorError::InvalidMetadata);
    }
    if path.ends_with(".info") {
        let info: Value =
            serde_json::from_slice(bytes).map_err(|_| MirrorError::InvalidMetadata)?;
        let object = info.as_object().ok_or(MirrorError::InvalidMetadata)?;
        return (object.get("Version").and_then(Value::as_str).is_some()
            && object.get("Time").and_then(Value::as_str).is_some())
        .then_some(())
        .ok_or(MirrorError::InvalidMetadata);
    }
    if path.ends_with(".mod") {
        return text
            .lines()
            .map(str::trim)
            .any(|line| line.starts_with("module "))
            .then_some(())
            .ok_or(MirrorError::InvalidMetadata);
    }
    Err(MirrorError::InvalidMetadata)
}

fn parse_sri(value: &str) -> Option<ObjectDigest> {
    let mut sha256 = None;
    for token in value.split_ascii_whitespace() {
        let (algorithm, encoded) = token.split_once('-')?;
        let decoded = STANDARD
            .decode(encoded.split_once('?').map_or(encoded, |(hash, _)| hash))
            .ok()?;
        match algorithm {
            "sha512" => return decoded.try_into().ok().map(ObjectDigest::Sha512),
            "sha256" => sha256 = decoded.try_into().ok().map(ObjectDigest::Sha256),
            _ => {}
        }
    }
    sha256
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
    let header_length = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| MirrorError::InvalidContentLength)?
                .parse::<u64>()
                .map_err(|_| MirrorError::InvalidContentLength)
        })
        .transpose()?;
    if let Some(expected) = request.metadata.expected {
        let length = header_length.ok_or(MirrorError::MissingContentLength)?;
        if length != expected.length {
            return Err(MirrorError::LengthMismatch);
        }
        return Ok(expected.length);
    }
    if header_length.is_some_and(|length| length > MAX_METADATA_BYTES) {
        return Err(MirrorError::ObjectTooLarge);
    }
    Ok(MAX_METADATA_BYTES)
}

fn cacheable(request: &MirrorRequest, response: &Response<MirrorBody>) -> bool {
    if response.headers().contains_key(header::SET_COOKIE) {
        return false;
    }
    let unsupported_vary = response
        .headers()
        .get_all(header::VARY)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|name| !name.eq_ignore_ascii_case("accept-encoding"));
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
    let metadata = classify(request.protocol, &path, request.metadata.expected)?;
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
    protocol: MirrorProtocol,
    path_and_query: &str,
    supplied: Option<ObjectExpectation>,
) -> Result<MirrorProtocolMetadata, MirrorError> {
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _)| path);
    validate_mirror_path(path, protocol)?;
    let (kind, identity) = match protocol {
        MirrorProtocol::Npm => classify_npm(path)?,
        MirrorProtocol::Cargo => classify_cargo(path)?,
        MirrorProtocol::Go => classify_go(path)?,
    };
    let encoded = parse_protocol_expectation(path_and_query)?;
    let expected = match (supplied, encoded) {
        (Some(left), Some(right)) if left != right => return Err(MirrorError::IntegrityConflict),
        (Some(expected), _) | (_, Some(expected)) => Some(expected),
        (None, None) => None,
    };
    Ok(MirrorProtocolMetadata {
        kind,
        identity,
        expected,
    })
}

fn parse_protocol_expectation(
    path_and_query: &str,
) -> Result<Option<ObjectExpectation>, MirrorError> {
    let Some((_, query)) = path_and_query.split_once('?') else {
        return Ok(None);
    };
    let mut integrity = None;
    let mut length = 0;
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match name.as_ref() {
            "cowshed-integrity" if integrity.is_none() => integrity = Some(value.into_owned()),
            "cowshed-length" => {
                length = value
                    .parse::<u64>()
                    .map_err(|_| MirrorError::InvalidContentLength)?;
            }
            _ => {}
        }
    }
    let Some(integrity) = integrity else {
        return Ok(None);
    };
    let digest = if let Some(hex) = integrity.strip_prefix("sha256-") {
        ObjectDigest::Sha256(decode_hex_32(hex).ok_or(MirrorError::MissingIntegrity)?)
    } else {
        parse_sri(&integrity).ok_or(MirrorError::MissingIntegrity)?
    };
    Ok(Some(ObjectExpectation { length, digest }))
}

fn decode_hex_32(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut decoded = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex(pair[0]).ok()? << 4) | hex(pair[1]).ok()?;
    }
    Some(decoded)
}

fn classify_npm(path: &str) -> Result<(MirrorResourceKind, String), MirrorError> {
    let relative = path
        .strip_prefix('/')
        .ok_or(MirrorError::InvalidProtocolPath)?;
    if relative.is_empty() {
        return Err(MirrorError::InvalidProtocolPath);
    }
    let lower = relative.to_ascii_lowercase();
    let tarball = lower.ends_with(".tgz") && lower.contains("/-/");
    let package_path = relative.split("/-/").next().unwrap_or(relative);
    let decoded = percent_decode(package_path)?;
    let identity = if decoded.starts_with('@') {
        let mut parts = decoded.split('/');
        let scope = parts.next().ok_or(MirrorError::InvalidProtocolPath)?;
        let package = parts.next().ok_or(MirrorError::InvalidProtocolPath)?;
        if parts.next().is_some() || scope.len() < 2 || package.is_empty() {
            return Err(MirrorError::InvalidProtocolPath);
        }
        format!("{scope}/{package}")
    } else {
        let package = decoded
            .split('/')
            .next()
            .ok_or(MirrorError::InvalidProtocolPath)?;
        if package.is_empty() {
            return Err(MirrorError::InvalidProtocolPath);
        }
        package.to_owned()
    };
    Ok((
        if tarball {
            MirrorResourceKind::Immutable
        } else {
            MirrorResourceKind::Metadata
        },
        identity,
    ))
}

fn classify_cargo(path: &str) -> Result<(MirrorResourceKind, String), MirrorError> {
    if path == "/config.json" {
        return Ok((MirrorResourceKind::Metadata, "config.json".to_owned()));
    }
    let segments = path
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() >= 5
        && segments[0] == "api"
        && segments[1] == "v1"
        && segments[2] == "crates"
        && segments.last() == Some(&"download")
    {
        return Ok((MirrorResourceKind::Immutable, segments[3].to_owned()));
    }
    if segments.len() >= 3 && segments[0] == "crates" && path.ends_with(".crate") {
        return Ok((MirrorResourceKind::Immutable, segments[1].to_owned()));
    }
    let crate_name = segments.last().ok_or(MirrorError::InvalidProtocolPath)?;
    if crate_name.is_empty() {
        return Err(MirrorError::InvalidProtocolPath);
    }
    Ok((MirrorResourceKind::Metadata, (*crate_name).to_owned()))
}

fn classify_go(path: &str) -> Result<(MirrorResourceKind, String), MirrorError> {
    let relative = path
        .strip_prefix('/')
        .ok_or(MirrorError::InvalidProtocolPath)?;
    if relative.starts_with("lookup/")
        || relative.starts_with("tile/")
        || relative == "latest"
        || relative.starts_with("sumdb/")
    {
        return Ok((MirrorResourceKind::Metadata, relative.to_owned()));
    }
    let (module, resource) = relative
        .split_once("/@v/")
        .ok_or(MirrorError::InvalidProtocolPath)?;
    if module.is_empty() || resource.is_empty() {
        return Err(MirrorError::InvalidProtocolPath);
    }
    let kind = if resource.ends_with(".zip") {
        MirrorResourceKind::Immutable
    } else if resource == "list" || resource.ends_with(".info") || resource.ends_with(".mod") {
        MirrorResourceKind::Metadata
    } else {
        return Err(MirrorError::InvalidProtocolPath);
    };
    Ok((kind, module.to_owned()))
}

fn validate_mirror_path(path: &str, protocol: MirrorProtocol) -> Result<(), MirrorError> {
    if path.len() > MAX_LOCATION_BYTES || !path.starts_with('/') {
        return Err(MirrorError::InvalidProtocolPath);
    }
    if protocol != MirrorProtocol::Npm {
        normalize_path(path).map_err(|_| MirrorError::InvalidProtocolPath)?;
        return Ok(());
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

fn percent_decode(value: &str) -> Result<String, MirrorError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(MirrorError::InvalidProtocolPath);
            }
            let high = hex(bytes[index + 1])?;
            let low = hex(bytes[index + 2])?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| MirrorError::InvalidProtocolPath)
}

fn hex(value: u8) -> Result<u8, MirrorError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(MirrorError::InvalidProtocolPath),
    }
}

fn strip_request_secrets(headers: &mut HeaderMap) {
    for name in [
        header::AUTHORIZATION,
        header::PROXY_AUTHORIZATION,
        header::COOKIE,
        header::SET_COOKIE,
    ] {
        headers.remove(name);
    }
    for name in ["npm-auth-type", "npm-otp", "x-goog-api-key"] {
        headers.remove(name);
    }
}

fn strip_response_secrets(headers: &mut HeaderMap) {
    headers.remove(header::SET_COOKIE);
    headers.remove(header::PROXY_AUTHENTICATE);
    headers.remove(header::WWW_AUTHENTICATE);
    strip_hop_headers(headers);
}

fn strip_hop_headers(headers: &mut HeaderMap) {
    for name in [
        header::CONNECTION,
        HeaderName::from_static("keep-alive"),
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ] {
        headers.remove(name);
    }
}

fn unix_ms(time: SystemTime) -> Result<u64, MirrorError> {
    let duration = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| MirrorError::Clock)?;
    u64::try_from(duration.as_millis()).map_err(|_| MirrorError::Clock)
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
    #[error("mirror metadata exceeds the 8 MiB parser limit")]
    MetadataTooLarge,
    #[error("mirror metadata is malformed")]
    InvalidMetadata,
    #[error("mirror metadata points at an unadmitted origin")]
    UnsafeMetadataOrigin,
    #[error("mirror protocol integrity metadata conflicts")]
    IntegrityConflict,
    #[error("mirror upstream ignored canonical identity encoding")]
    UnsupportedEncoding,
    #[error("mirror protocol path is invalid")]
    InvalidProtocolPath,
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
