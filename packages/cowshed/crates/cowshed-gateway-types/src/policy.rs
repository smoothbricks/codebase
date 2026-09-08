use std::{collections::BTreeSet, fmt, net::IpAddr, str::FromStr};

use http::{Method, uri::Authority};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EgressMode {
    Intercept,
    Opaque,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostPattern {
    Exact(String),
    Wildcard(String),
    Ip(IpAddr),
}

impl HostPattern {
    pub fn parse(value: &str) -> Result<Self, PolicyError> {
        let value = value.trim();
        if value.is_empty() || value.ends_with('.') {
            return Err(PolicyError::InvalidHost);
        }
        if let Ok(ip) = value.trim_matches(['[', ']']).parse() {
            return Ok(Self::Ip(ip));
        }
        if let Some(suffix) = value.strip_prefix("*.") {
            let suffix = canonical_dns(suffix)?;
            if suffix.split('.').count() < 2 {
                return Err(PolicyError::InvalidWildcard);
            }
            return Ok(Self::Wildcard(suffix));
        }
        if value.contains('*') {
            return Err(PolicyError::InvalidWildcard);
        }
        Ok(Self::Exact(canonical_dns(value)?))
    }

    pub fn matches(&self, host: &CanonicalHost) -> bool {
        match (self, host) {
            (Self::Ip(expected), CanonicalHost::Ip(actual)) => expected == actual,
            (Self::Exact(expected), CanonicalHost::Dns(actual)) => expected == actual,
            (Self::Wildcard(suffix), CanonicalHost::Dns(actual)) => {
                actual.strip_suffix(suffix).is_some_and(|prefix| {
                    prefix.ends_with('.') && !prefix[..prefix.len() - 1].contains('.')
                })
            }
            _ => false,
        }
    }

    /// Whether this pattern names one host rather than a family of them.
    ///
    /// A wildcard grant admits hosts the operator never enumerated, so the daemon records that
    /// distinction in its audit trail: "granted to `*.crates.io`" and "granted to `crates.io`"
    /// are different claims about what was authorised.
    pub fn is_exact(&self) -> bool {
        !matches!(self, Self::Wildcard(_))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CanonicalHost {
    Dns(String),
    Ip(IpAddr),
}

impl CanonicalHost {
    pub fn parse(value: &str) -> Result<Self, PolicyError> {
        let unbracketed = value.trim_matches(['[', ']']);
        if let Ok(ip) = unbracketed.parse() {
            return Ok(Self::Ip(ip));
        }
        Ok(Self::Dns(canonical_dns(value)?))
    }

    pub fn as_str(&self) -> String {
        match self {
            Self::Dns(name) => name.clone(),
            Self::Ip(ip) => ip.to_string(),
        }
    }
}

impl fmt::Display for CanonicalHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dns(name) => formatter.write_str(name),
            Self::Ip(ip) => ip.fmt(formatter),
        }
    }
}

fn canonical_dns(value: &str) -> Result<String, PolicyError> {
    if value.is_empty() || value.len() > 253 || value.ends_with('.') {
        return Err(PolicyError::InvalidHost);
    }
    let ascii = idna::domain_to_ascii(value).map_err(|_| PolicyError::InvalidHost)?;
    let canonical = ascii.to_ascii_lowercase();
    if canonical.split('.').any(|label| {
        label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    }) {
        return Err(PolicyError::InvalidHost);
    }
    Ok(canonical)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CanonicalTarget {
    pub scheme: TargetScheme,
    pub host: CanonicalHost,
    pub port: u16,
}

impl CanonicalTarget {
    pub fn from_authority(authority: &str, scheme: TargetScheme) -> Result<Self, PolicyError> {
        let authority =
            Authority::from_str(authority).map_err(|_| PolicyError::InvalidAuthority)?;
        let port = authority
            .port_u16()
            .ok_or(PolicyError::ExplicitPortRequired)?;
        Ok(Self {
            scheme,
            host: CanonicalHost::parse(authority.host())?,
            port,
        })
    }

    pub fn from_url(url: &Url) -> Result<Self, PolicyError> {
        let scheme = TargetScheme::parse(url.scheme())?;
        let host = url.host_str().ok_or(PolicyError::InvalidAuthority)?;
        let port = url
            .port_or_known_default()
            .ok_or(PolicyError::ExplicitPortRequired)?;
        Ok(Self {
            scheme,
            host: CanonicalHost::parse(host)?,
            port,
        })
    }

    pub fn authority(&self) -> String {
        match &self.host {
            CanonicalHost::Ip(IpAddr::V6(ip)) => format!("[{ip}]:{}", self.port),
            _ => format!("{}:{}", self.host, self.port),
        }
    }

    pub fn origin(&self) -> String {
        format!("{}://{}", self.scheme.as_str(), self.authority())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TargetScheme {
    Http,
    Https,
}

impl TargetScheme {
    pub fn parse(value: &str) -> Result<Self, PolicyError> {
        match value {
            "http" => Ok(Self::Http),
            "https" => Ok(Self::Https),
            _ => Err(PolicyError::UnsupportedScheme),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

#[derive(Clone, Debug)]
pub struct EgressGrant {
    pub host: HostPattern,
    pub port: u16,
    pub mode: EgressMode,
    pub methods: BTreeSet<String>,
    pub path_prefixes: Vec<String>,
    pub impersonate: bool,
}

impl EgressGrant {
    pub fn intercept(host: &str, port: u16) -> Result<Self, PolicyError> {
        Ok(Self {
            host: HostPattern::parse(host)?,
            port,
            mode: EgressMode::Intercept,
            methods: ["GET", "HEAD"].into_iter().map(String::from).collect(),
            path_prefixes: vec!["/".to_owned()],
            impersonate: false,
        })
    }

    pub fn opaque(host: &str, port: u16) -> Result<Self, PolicyError> {
        Ok(Self {
            host: HostPattern::parse(host)?,
            port,
            mode: EgressMode::Opaque,
            methods: BTreeSet::new(),
            path_prefixes: Vec::new(),
            impersonate: false,
        })
    }

    pub fn allow_method(mut self, method: Method) -> Self {
        self.methods.insert(method.as_str().to_owned());
        self
    }

    pub fn allow_path(mut self, prefix: &str) -> Result<Self, PolicyError> {
        self.path_prefixes.push(normalize_path(prefix)?);
        self.path_prefixes.sort();
        self.path_prefixes.dedup();
        Ok(self)
    }

    fn validate(&self) -> Result<(), PolicyError> {
        if self.port == 0 {
            return Err(PolicyError::InvalidPort);
        }
        if self.mode == EgressMode::Opaque {
            if !self.methods.is_empty() || !self.path_prefixes.is_empty() || self.impersonate {
                return Err(PolicyError::OpaqueCannotInspect);
            }
            return Ok(());
        }
        if self.methods.is_empty() || self.path_prefixes.is_empty() {
            return Err(PolicyError::EmptyAdmission);
        }
        for method in &self.methods {
            Method::from_bytes(method.as_bytes()).map_err(|_| PolicyError::InvalidMethod)?;
        }
        for path in &self.path_prefixes {
            if normalize_path(path)? != *path {
                return Err(PolicyError::InvalidPath);
            }
        }
        Ok(())
    }

    /// Does this grant admit the request, reading the RAW request path?
    ///
    /// The path is not pre-normalized by the caller: prefix matching belongs to
    /// [`path_matches_prefix`], the one owner of that decision on this boundary, and handing it
    /// raw bytes is what keeps an encoded slash from helping a prefix match. `Err` is a
    /// structurally refused path rather than "no grant matched", so the caller can say which of
    /// the two happened.
    pub(crate) fn admits(
        &self,
        target: &CanonicalTarget,
        method: &Method,
        path: &str,
    ) -> Result<bool, PolicyError> {
        if self.port != target.port || !self.host.matches(&target.host) {
            return Ok(false);
        }
        if method == Method::CONNECT {
            return Ok(true);
        }
        if self.mode != EgressMode::Intercept || !self.methods.contains(method.as_str()) {
            return Ok(false);
        }
        for prefix in &self.path_prefixes {
            if path_matches_prefix(path, prefix)? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MirrorProtocol {
    Npm,
    Cargo,
    Go,
}

impl MirrorProtocol {
    pub const fn local_prefix(self) -> &'static str {
        match self {
            Self::Npm => "/npm/",
            Self::Cargo => "/cargo/",
            Self::Go => "/go/",
        }
    }

    pub fn matches_local_path(self, path: &str) -> bool {
        let prefix = self.local_prefix();
        path == &prefix[..prefix.len() - 1] || path.starts_with(prefix)
    }

    pub const fn baseline_origin(self) -> &'static str {
        match self {
            Self::Npm => "https://registry.npmjs.org:443",
            Self::Cargo => "https://index.crates.io:443",
            Self::Go => "https://proxy.golang.org:443",
        }
    }

    pub const fn artifact_origin(self) -> &'static str {
        match self {
            Self::Npm => "https://registry.npmjs.org:443",
            Self::Cargo => "https://static.crates.io:443",
            Self::Go => "https://proxy.golang.org:443",
        }
    }

    pub const fn checksum_origin(self) -> Option<&'static str> {
        match self {
            Self::Go => Some("https://sum.golang.org:443"),
            Self::Npm | Self::Cargo => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Npm => "npm",
            Self::Cargo => "cargo",
            Self::Go => "go",
        }
    }
}

#[derive(Clone, Debug)]
pub struct MirrorRoute {
    pub local_prefix: String,
    pub upstream_origin: String,
    pub protocol: MirrorProtocol,
    pub admitted_prefixes: Vec<String>,
    pub credentialed: bool,
}

impl MirrorRoute {
    pub fn validate(&self) -> Result<(), PolicyError> {
        if !self.local_prefix.starts_with('/') || !self.local_prefix.ends_with('/') {
            return Err(PolicyError::InvalidMirrorPrefix);
        }
        if self.local_prefix != self.protocol.local_prefix() {
            return Err(PolicyError::MirrorProtocolPrefixMismatch);
        }
        let url = Url::parse(&self.upstream_origin).map_err(|_| PolicyError::InvalidOrigin)?;
        if url.scheme() != "https"
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(PolicyError::InvalidOrigin);
        }
        CanonicalTarget::from_url(&url)?;
        if self.admitted_prefixes.is_empty() {
            return Err(PolicyError::EmptyAdmission);
        }
        for prefix in &self.admitted_prefixes {
            normalize_path(prefix)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct WorkspacePolicy {
    pub grants: Vec<EgressGrant>,
    pub mirrors: Vec<MirrorRoute>,
}

impl WorkspacePolicy {
    pub fn validate(&self) -> Result<(), PolicyError> {
        for grant in &self.grants {
            grant.validate()?;
        }
        for route in &self.mirrors {
            route.validate()?;
        }
        let scopes = self
            .mirrors
            .iter()
            .enumerate()
            .flat_map(|(route_index, route)| {
                route
                    .admitted_prefixes
                    .iter()
                    .map(move |prefix| (route_index, &route.local_prefix, prefix))
            })
            .collect::<Vec<_>>();
        for (index, (left_route, left_local, left_scope)) in scopes.iter().enumerate() {
            if scopes[index + 1..]
                .iter()
                .any(|(right_route, right_local, right_scope)| {
                    left_route != right_route
                        && left_local == right_local
                        && (mirror_scope_matches(left_scope, right_scope)
                            || mirror_scope_matches(right_scope, left_scope))
                })
            {
                return Err(PolicyError::DuplicateMirrorPrefix);
            }
        }
        Ok(())
    }

    /// Resolves one request against the grants, or names why it is refused.
    ///
    /// The single admission decision of the whole egress path: the proxy in `cowshed-gateway`
    /// asks, and the answer is either the grant whose credentials may be injected or a
    /// [`PolicyDenial`] carrying the operator hint to print.
    pub fn authorize<'a>(
        &'a self,
        target: &CanonicalTarget,
        method: &Method,
        path: &str,
    ) -> Result<&'a EgressGrant, PolicyDenial> {
        raw_path_admissible(path).map_err(|_| PolicyDenial::InvalidPath)?;
        for grant in &self.grants {
            match grant.admits(target, method, path) {
                Ok(true) => return Ok(grant),
                Ok(false) => {}
                Err(_) => return Err(PolicyDenial::InvalidPath),
            }
        }
        Err(PolicyDenial::NotGranted {
            hint: format!("cowshed grant <ws> --egress {}", target.authority()),
        })
    }

    pub fn resolve_mirror(&self, path: &str) -> Option<ResolvedMirrorRoute> {
        let configured = self
            .mirrors
            .iter()
            .filter_map(|route| {
                let suffix =
                    path.strip_prefix(&route.local_prefix[..route.local_prefix.len() - 1])?;
                let (normalized, admission_path) =
                    normalize_mirror_suffix(route.protocol, suffix).ok()?;
                let admitted_prefix = route
                    .admitted_prefixes
                    .iter()
                    .filter(|prefix| mirror_scope_matches(&admission_path, prefix))
                    .max_by_key(|prefix| prefix.len())?
                    .clone();
                Some((route, normalized, admitted_prefix))
            })
            .max_by_key(|(_, _, admitted_prefix)| admitted_prefix.len());
        if let Some((route, normalized, admitted_prefix)) = configured {
            let base = Url::parse(&route.upstream_origin).ok()?;
            let url = base.join(normalized.trim_start_matches('/')).ok()?;
            return Some(ResolvedMirrorRoute {
                target: CanonicalTarget::from_url(&url).ok()?,
                path: normalized,
                protocol: route.protocol,
                credentialed: route.credentialed,
                admitted_prefix,
            });
        }
        resolve_baseline_mirror(path)
    }
}

/// Whether an admitted prefix covers a normalized path, on label boundaries only.
///
/// `/pypi` must not admit `/pypilookalike`, so a bare prefix match is wrong; the daemon re-checks
/// this after resolving a mirror route, which is why it crosses the crate boundary.
pub fn mirror_scope_matches(path: &str, prefix: &str) -> bool {
    path == prefix
        || prefix == "/"
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| prefix.ends_with('/') || suffix.starts_with('/'))
}

fn resolve_baseline_mirror(path: &str) -> Option<ResolvedMirrorRoute> {
    let (protocol, suffix) = [
        MirrorProtocol::Npm,
        MirrorProtocol::Cargo,
        MirrorProtocol::Go,
    ]
    .into_iter()
    .find_map(|protocol| {
        path.strip_prefix(protocol.local_prefix())
            .map(|suffix| (protocol, suffix))
    })?;
    let local_suffix = format!("/{suffix}");
    let (mut upstream_path, _) = normalize_mirror_suffix(protocol, &local_suffix).ok()?;
    let origin = match protocol {
        MirrorProtocol::Npm => protocol.baseline_origin(),
        MirrorProtocol::Cargo if upstream_path.starts_with("/crates/") => {
            let (route_path, query) = upstream_path
                .split_once('?')
                .map_or((upstream_path.as_str(), None), |(path, query)| {
                    (path, Some(query))
                });
            let segments = route_path
                .trim_start_matches('/')
                .split('/')
                .collect::<Vec<_>>();
            if segments.len() != 4 || segments[0] != "crates" || segments[3] != "download" {
                return None;
            }
            let suffix = query.map_or(String::new(), |query| format!("?{query}"));
            upstream_path = format!(
                "/crates/{name}/{name}-{version}.crate{suffix}",
                name = segments[1],
                version = segments[2]
            );
            protocol.artifact_origin()
        }
        MirrorProtocol::Cargo => protocol.baseline_origin(),
        MirrorProtocol::Go if upstream_path.starts_with("/sumdb/sum.golang.org/") => {
            upstream_path = upstream_path
                .strip_prefix("/sumdb/sum.golang.org")
                .unwrap_or("/")
                .to_owned();
            protocol.checksum_origin()?
        }
        MirrorProtocol::Go => protocol.baseline_origin(),
    };
    let target = CanonicalTarget::from_url(&Url::parse(origin).ok()?).ok()?;
    Some(ResolvedMirrorRoute {
        target,
        path: upstream_path,
        protocol,
        credentialed: false,
        admitted_prefix: "/".to_owned(),
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedMirrorRoute {
    pub target: CanonicalTarget,
    pub path: String,
    pub protocol: MirrorProtocol,
    pub credentialed: bool,
    pub admitted_prefix: String,
}

fn normalize_mirror_suffix(
    protocol: MirrorProtocol,
    path_and_query: &str,
) -> Result<(String, String), PolicyError> {
    if protocol != MirrorProtocol::Npm {
        let normalized = normalize_path(path_and_query)?;
        return Ok((normalized.clone(), normalized));
    }
    let admission_path = decode_encoded_slashes(path_and_query)?;
    if admission_path.contains("//") {
        return Err(PolicyError::InvalidPath);
    }
    Ok((path_and_query.to_owned(), admission_path))
}

pub const MAX_PATH_BYTES: usize = 8192;

/// Is this raw request path well-formed, before any prefix is known?
///
/// The proxy's front door and [`WorkspacePolicy::authorize`] share this, so a request that
/// reaches prefix matching has already been proven well-formed once. A path strict
/// [`normalize_path`] accepts keeps exactly its former meaning; the relaxed reading exists for
/// the single encoded slash npm puts inside one package segment (`/@scope%2fname`), and it still
/// refuses malformed or truncated escapes, double encoding (`%25`), backslash, NUL, CR, LF and
/// decoded `.`/`..` segments.
pub fn raw_path_admissible(path: &str) -> Result<(), PolicyError> {
    if normalize_path(path).is_ok() {
        return Ok(());
    }
    decode_encoded_slashes(path).map(|_| ())
}

/// Does `raw_path` lie under `prefix`?
///
/// The one owner of that decision on this boundary: egress grants and scoped credential records
/// both ask here, so the network admission and the secret's scope cannot drift into two readings
/// of the same bytes. `prefix` is matched against the RAW bytes, so an encoded slash can never
/// help satisfy a prefix — only the remainder after a literal match is decoded, and that
/// remainder must still be free of duplicate slashes and dot segments. Callers forward the
/// original bytes upstream; nothing here re-encodes a path.
pub fn path_matches_prefix(raw_path: &str, prefix: &str) -> Result<bool, PolicyError> {
    if let Ok(strict) = normalize_path(raw_path) {
        return Ok(strict.starts_with(prefix));
    }
    decode_encoded_slashes(raw_path)?;
    let path = split_query(raw_path);
    let Some(suffix) = path.strip_prefix(prefix) else {
        return Ok(false);
    };
    let decoded = decode_percent(suffix, encoded_slash_forbidden)?;
    if decoded.contains("//") || has_dot_segment(&decoded) {
        return Err(PolicyError::InvalidPath);
    }
    Ok(true)
}

/// Bytes no escape may decode to even in the relaxed reading. `%` is forbidden so a decoded
/// path can never be decoded a second time downstream into something else.
const fn encoded_slash_forbidden(value: u8) -> bool {
    matches!(value, b'\\' | 0 | b'%')
}

fn split_query(path_and_query: &str) -> &str {
    path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _)| path)
}

fn has_dot_segment(path: &str) -> bool {
    path.split('/')
        .any(|segment| segment == "." || segment == "..")
}

/// The relaxed reading: strict structure, but an escaped `/` decodes instead of refusing.
fn decode_encoded_slashes(path_and_query: &str) -> Result<String, PolicyError> {
    if !path_and_query.starts_with('/')
        || path_and_query.len() > MAX_PATH_BYTES
        || path_and_query.contains(['\\', '\0', '\r', '\n'])
    {
        return Err(PolicyError::InvalidPath);
    }
    let decoded = decode_percent(split_query(path_and_query), encoded_slash_forbidden)?;
    if has_dot_segment(&decoded) {
        return Err(PolicyError::InvalidPath);
    }
    Ok(decoded)
}

pub fn normalize_path(path: &str) -> Result<String, PolicyError> {
    if !path.starts_with('/') || path.len() > MAX_PATH_BYTES || path.contains(['\\', '\0']) {
        return Err(PolicyError::InvalidPath);
    }
    let decoded = decode_percent(path, |value| matches!(value, b'/' | b'\\' | 0 | b'%'))?;
    if has_dot_segment(&decoded) {
        return Err(PolicyError::InvalidPath);
    }
    Ok(decoded)
}

/// Percent-decodes a path, refusing any escape that decodes to a `forbidden` byte.
///
/// Rejecting rather than re-encoding is the point: a path that smuggles `%2f` past a prefix check
/// has two readings, and a proxy that picks one is a confused deputy. The mirror front end in
/// `cowshed-gateway` decodes package paths through the same routine, so both sides of the
/// boundary agree on exactly one canonical form.
pub fn decode_percent(path: &str, forbidden: impl Fn(u8) -> bool) -> Result<String, PolicyError> {
    let bytes = path.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len() {
            return Err(PolicyError::InvalidPath);
        }
        let high = percent_nibble(bytes[index + 1]).ok_or(PolicyError::InvalidPath)?;
        let low = percent_nibble(bytes[index + 2]).ok_or(PolicyError::InvalidPath)?;
        let value = (high << 4) | low;
        if forbidden(value) {
            return Err(PolicyError::InvalidPath);
        }
        decoded.push(value);
        index += 3;
    }
    String::from_utf8(decoded).map_err(|_| PolicyError::InvalidPath)
}

fn percent_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Why one request was refused, in the operator's terms.
#[derive(Clone, Debug)]
pub enum PolicyDenial {
    InvalidPath,
    NotGranted { hint: String },
}

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("host is not a canonical DNS name or IP address")]
    InvalidHost,
    #[error("wildcards must be the entire leftmost label and match exactly one label")]
    InvalidWildcard,
    #[error("authority must be host plus explicit port")]
    InvalidAuthority,
    #[error("an explicit port is required")]
    ExplicitPortRequired,
    #[error("only HTTP and HTTPS origins are supported")]
    UnsupportedScheme,
    #[error("port must be non-zero")]
    InvalidPort,
    #[error("opaque grants cannot contain request policy or impersonation")]
    OpaqueCannotInspect,
    #[error("intercept grants require methods and path prefixes")]
    EmptyAdmission,
    #[error("invalid HTTP method")]
    InvalidMethod,
    #[error("path is ambiguous or unsafe")]
    InvalidPath,
    #[error("mirror local prefixes must start and end with slash")]
    InvalidMirrorPrefix,
    #[error("mirror route prefix must be the frozen endpoint for its protocol")]
    MirrorProtocolPrefixMismatch,
    #[error("mirror origins must be exact HTTPS origins")]
    InvalidOrigin,
    #[error("mirror local prefixes must be unique")]
    DuplicateMirrorPrefix,
}

#[cfg(test)]
mod tests {
    use super::*;

    const NPM_PREFIX: &str = "/api/packages/owner/npm/";

    fn matches(raw: &str, prefix: &str) -> Result<bool, PolicyError> {
        path_matches_prefix(raw, prefix)
    }

    #[test]
    fn a_scoped_packument_after_the_prefix_is_admitted_and_forwarded_unchanged() {
        let raw = "/api/packages/owner/npm/@scope%2fpkg";
        assert!(matches(raw, NPM_PREFIX).expect("well-formed path"));
        // Nothing here rewrites the request: the caller forwards the same bytes it was given.
        assert!(raw.contains("%2f"));
        assert!(
            matches(
                "/api/packages/owner/npm/@scope%2Fpkg/-/pkg-1.0.0.tgz",
                NPM_PREFIX
            )
            .expect("well-formed path")
        );
    }

    #[test]
    fn an_encoded_slash_cannot_help_satisfy_the_prefix() {
        // Decoded, this reads as if it were inside the admitted namespace; upstream would read a
        // different resource. The prefix is matched on raw bytes precisely so it cannot pass.
        assert!(!matches("/api%2fpackages/owner/npm/pkg", NPM_PREFIX).expect("well-formed path"));
        assert!(!matches("%2fapi/packages/owner/npm/pkg", NPM_PREFIX).is_ok_and(|matched| matched));
    }

    #[test]
    fn traversal_and_double_encoding_are_refused_rather_than_matched() {
        for raw in [
            "/api/packages/owner/npm/..%2f..%2fother",
            "/api/packages/owner/npm/@scope%2f..%2f..%2fetc",
        ] {
            assert!(
                matches(raw, NPM_PREFIX).is_err(),
                "{raw} decodes to a dot segment"
            );
        }
        // `%252f` would decode to `%2f` and invite a second decoding round downstream.
        assert!(matches("/api/packages/owner/npm/@scope%252fpkg", NPM_PREFIX).is_err());
        assert!(matches("/api/packages/owner/npm/@scope%2", NPM_PREFIX).is_err());
        assert!(matches("/api/packages/owner/npm/@scope%zz", NPM_PREFIX).is_err());
        assert!(matches("/api/packages/owner/npm/a%2f%2fb", NPM_PREFIX).is_err());
        assert!(matches("/api/packages/owner/npm/a%5cb", NPM_PREFIX).is_err());
        assert!(matches("/api/packages/owner/npm/a%00b", NPM_PREFIX).is_err());
        assert!(matches("api/packages/owner/npm/@scope%2fpkg", NPM_PREFIX).is_err());
    }

    #[test]
    fn paths_strict_normalization_accepts_keep_their_former_meaning() {
        // Non-slash escapes still decode before the prefix comparison, as they always did.
        assert!(matches("/api/packages/owner/npm/%41", NPM_PREFIX).expect("strict path"));
        assert!(matches("/%61pi/packages/owner/npm/pkg", NPM_PREFIX).expect("strict path"));
        assert!(!matches("/other/pkg", NPM_PREFIX).expect("strict path"));
        // A duplicate slash is not new grounds for refusal where strict normalization allowed it.
        assert!(matches("//api/packages", "/").expect("strict path"));
        assert!(normalize_path("/a/%2f/b").is_err());
        assert!(raw_path_admissible("/a/%2f/b").is_ok());
        assert!(raw_path_admissible("/a/%2e%2e/b").is_err());
        assert!(raw_path_admissible("/a/../b").is_err());
    }

    #[test]
    fn a_query_string_is_not_part_of_the_prefix_decision() {
        assert!(
            matches(
                "/api/packages/owner/npm/@scope%2fpkg?write=true",
                NPM_PREFIX
            )
            .expect("well-formed path")
        );
    }

    #[test]
    fn grant_admission_reads_the_raw_path_and_still_gates_host_method_and_port() {
        let target = CanonicalTarget::from_authority("registry.test:443", TargetScheme::Https)
            .expect("fixture target");
        let grant = EgressGrant::intercept("registry.test", 443)
            .expect("fixture grant")
            .allow_path(NPM_PREFIX)
            .expect("fixture prefix");
        let scoped = "/api/packages/owner/npm/@scope%2fpkg";
        assert!(
            grant
                .admits(&target, &Method::GET, scoped)
                .expect("well-formed path")
        );
        assert!(
            !grant
                .admits(&target, &Method::POST, scoped)
                .expect("well-formed path")
        );
        let other = CanonicalTarget::from_authority("elsewhere.test:443", TargetScheme::Https)
            .expect("fixture target");
        assert!(
            !grant
                .admits(&other, &Method::GET, scoped)
                .expect("well-formed path")
        );
        let other_port = CanonicalTarget::from_authority("registry.test:8443", TargetScheme::Https)
            .expect("fixture target");
        assert!(
            !grant
                .admits(&other_port, &Method::GET, scoped)
                .expect("well-formed path")
        );
        assert!(
            grant
                .admits(&target, &Method::CONNECT, "/")
                .expect("well-formed path")
        );
    }

    #[test]
    fn authorize_separates_a_malformed_path_from_an_ungranted_destination() {
        let policy = WorkspacePolicy {
            grants: vec![
                EgressGrant::intercept("registry.test", 443)
                    .expect("fixture grant")
                    .allow_path(NPM_PREFIX)
                    .expect("fixture prefix"),
            ],
            mirrors: Vec::new(),
        };
        let target = CanonicalTarget::from_authority("registry.test:443", TargetScheme::Https)
            .expect("fixture target");
        assert!(
            policy
                .authorize(
                    &target,
                    &Method::GET,
                    "/api/packages/owner/npm/@scope%2fpkg"
                )
                .is_ok()
        );
        assert!(matches!(
            policy.authorize(&target, &Method::GET, "/api/packages/owner/npm/%2e%2e%2fx"),
            Err(PolicyDenial::InvalidPath)
        ));
        // An egress grant is origin-wide by design (`intercept` admits `/`), so the ungranted
        // case is another destination; the exact namespace lives in the credential's own scope.
        let elsewhere = CanonicalTarget::from_authority("elsewhere.test:443", TargetScheme::Https)
            .expect("fixture target");
        assert!(matches!(
            policy.authorize(&elsewhere, &Method::GET, "/api/packages/owner/npm/pkg"),
            Err(PolicyDenial::NotGranted { .. })
        ));
    }
}
