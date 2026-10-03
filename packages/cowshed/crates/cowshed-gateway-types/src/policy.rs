use std::{collections::BTreeSet, fmt, net::IpAddr, str::FromStr, sync::LazyLock};

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
}

impl EgressGrant {
    pub fn intercept(host: &str, port: u16) -> Result<Self, PolicyError> {
        Ok(Self {
            host: HostPattern::parse(host)?,
            port,
            mode: EgressMode::Intercept,
            methods: ["GET", "HEAD"].into_iter().map(String::from).collect(),
            path_prefixes: vec!["/".to_owned()],
        })
    }

    pub fn opaque(host: &str, port: u16) -> Result<Self, PolicyError> {
        Ok(Self {
            host: HostPattern::parse(host)?,
            port,
            mode: EgressMode::Opaque,
            methods: BTreeSet::new(),
            path_prefixes: Vec::new(),
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
            if !self.methods.is_empty() || !self.path_prefixes.is_empty() {
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
        if self.mode != EgressMode::Intercept {
            return Ok(false);
        }
        if !self.methods.contains(method.as_str())
            && !(method == Method::POST && is_git_upload_pack(path))
        {
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

/// git's smart-HTTP fetch endpoint, `<repository>/git-upload-pack`. A fetch, clone or
/// `ls-remote` is a read, but git can only express its negotiation as a POST there (and
/// protocol v2 POSTs even the ref listing), so an intercept grant that admits reads must admit
/// this one POST or git through it cannot list a single ref. `git-receive-pack` — a push — is not
/// it, nor is any other POST. The raw path is compared: an encoded separator never matches.
fn is_git_upload_pack(path: &str) -> bool {
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    path.ends_with("/git-upload-pack")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MirrorProtocol {
    Npm,
}

impl MirrorProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Npm => "npm",
        }
    }
}

#[derive(Clone, Debug)]
pub struct MirrorRoute {
    target: CanonicalTarget,
    pub admitted_prefixes: Vec<String>,
    pub credentialed: bool,
}

impl MirrorRoute {
    pub fn new(
        origin: &str,
        admitted_prefixes: Vec<String>,
        credentialed: bool,
    ) -> Result<Self, PolicyError> {
        let url = Url::parse(origin).map_err(|_| PolicyError::InvalidOrigin)?;
        if url.scheme() != "https"
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(PolicyError::InvalidOrigin);
        }
        let route = Self {
            target: CanonicalTarget::from_url(&url)?,
            admitted_prefixes,
            credentialed,
        };
        route.validate()?;
        Ok(route)
    }

    pub fn target(&self) -> &CanonicalTarget {
        &self.target
    }

    pub fn validate(&self) -> Result<(), PolicyError> {
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
        for (index, left) in self.mirrors.iter().enumerate() {
            for right in &self.mirrors[index + 1..] {
                if left.target == right.target
                    && left.admitted_prefixes.iter().any(|left_scope| {
                        right.admitted_prefixes.iter().any(|right_scope| {
                            mirror_scope_matches(left_scope, right_scope)
                                || mirror_scope_matches(right_scope, left_scope)
                        })
                    })
                {
                    return Err(PolicyError::OverlappingMirrorScope);
                }
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
        // Both remedies, the standing one first: a host a project's builds need (a registry, a
        // forge) belongs in every workspace, and granting it one workspace at a time is how the
        // grant sets of a project's workspaces drift apart.
        let authority = target.authority();
        Err(PolicyDenial::NotGranted {
            hint: format!(
                "cowshed grant --project-wide --egress {authority} (every workspace of this project) \
                 or cowshed grant <ws> --egress {authority} (this workspace)"
            ),
        })
    }

    /// Resolves one request a native npm client sent straight to `target` against the trusted
    /// routes. `path` is the request path and query exactly as received, with no mirror prefix.
    ///
    /// A configured route claims the request only when its upstream origin is exactly `target`
    /// and one of its admitted prefixes covers the path on label boundaries; the longest such
    /// prefix decides, and that route's own `credentialed` flag travels with the answer. A
    /// private scope therefore never admits another origin's paths, and another origin's scope
    /// never admits this one's. When no configured route claims it, `target` being exactly the
    /// public registry makes it an anonymous read from the root; every other destination is no
    /// mirror at all (`None`), left to the generic egress grants. The path keeps npm's reading:
    /// an escaped `/` inside a segment (`@scope%2fname`) decodes only for the admission
    /// decision, and the resolved path is the received bytes, which is what goes upstream.
    pub fn resolve_npm_registry(
        &self,
        target: &CanonicalTarget,
        path: &str,
    ) -> Option<ResolvedMirrorRoute> {
        let admission_path = npm_admission_path(path).ok()?;
        let configured = self
            .mirrors
            .iter()
            .filter(|route| &route.target == target)
            .filter_map(|route| {
                let admitted_prefix = route
                    .admitted_prefixes
                    .iter()
                    .filter(|prefix| mirror_scope_matches(&admission_path, prefix))
                    .max_by_key(|prefix| prefix.len())?;
                Some((route, admitted_prefix))
            })
            .max_by_key(|(_, admitted_prefix)| admitted_prefix.len());
        let (credentialed, admitted_prefix) = match configured {
            Some((route, admitted_prefix)) => (route.credentialed, admitted_prefix.clone()),
            None if NPM_BASELINE.as_ref() == Some(target) => (false, "/".to_owned()),
            None => return None,
        };
        Some(ResolvedMirrorRoute {
            target: target.clone(),
            path: path.to_owned(),
            protocol: MirrorProtocol::Npm,
            credentialed,
            admitted_prefix,
        })
    }
}

/// The public npm registry, readable anonymously by every workspace.
static NPM_BASELINE: LazyLock<Option<CanonicalTarget>> = LazyLock::new(|| {
    CanonicalTarget::from_url(&Url::parse("https://registry.npmjs.org:443").ok()?).ok()
});

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedMirrorRoute {
    pub target: CanonicalTarget,
    pub path: String,
    pub protocol: MirrorProtocol,
    pub credentialed: bool,
    pub admitted_prefix: String,
}

/// The path an npm admission prefix is compared against: strict structure, an escaped `/` inside
/// a segment decoded, an empty segment refused.
fn npm_admission_path(path_and_query: &str) -> Result<String, PolicyError> {
    let admission_path = decode_encoded_slashes(path_and_query)?;
    if admission_path.contains("//") {
        return Err(PolicyError::InvalidPath);
    }
    Ok(admission_path)
}

pub const MAX_PATH_BYTES: usize = 8192;

/// Is this raw request path well-formed, before any prefix is known?
///
/// The proxy's front door asks this, and it is the same question [`path_matches_prefix`] answers
/// against the root prefix every intercept grant already carries — so there is one reading of a
/// path here, not a front-door reading and a policy reading that could disagree.
pub fn raw_path_admissible(path: &str) -> Result<(), PolicyError> {
    path_matches_prefix(path, "/").map(|_| ())
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
    #[error("opaque grants cannot contain request policy")]
    OpaqueCannotInspect,
    #[error("intercept grants require methods and path prefixes")]
    EmptyAdmission,
    #[error("invalid HTTP method")]
    InvalidMethod,
    #[error("path is ambiguous or unsafe")]
    InvalidPath,
    #[error("mirror origins must be exact HTTPS origins")]
    InvalidOrigin,
    #[error("npm mirror scopes must not overlap on the same origin")]
    OverlappingMirrorScope,
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
        // An escape that decodes to an empty segment stays refused: `/a/%2f/b` reads as `/a//b`,
        // which is two paths, not one. Only a slash INSIDE a segment (npm's `@scope%2fname`) is
        // what the relaxed reading exists for.
        assert!(raw_path_admissible("/a/%2f/b").is_err());
        assert!(raw_path_admissible("/api/packages/o/npm/@scope%2fname").is_ok());
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

    /// git's smart-HTTP fetch is a read that git can only say as a POST: `ls-remote`, `fetch`
    /// and `clone` POST their negotiation to `<repo>/git-upload-pack` (protocol v2 even for the
    /// ref listing). An intercept grant admits exactly that POST; a push (`git-receive-pack`) and
    /// every other POST stay refused.
    #[test]
    fn an_intercept_grant_admits_git_smart_http_fetch_and_refuses_push() {
        let grant = EgressGrant::intercept("github.com", 443).expect("fixture grant");
        let target = CanonicalTarget::from_authority("github.com:443", TargetScheme::Https)
            .expect("fixture target");
        let admitted = |method: Method, path: &str| {
            grant
                .admits(&target, &method, path)
                .expect("well-formed path")
        };
        assert!(admitted(
            Method::GET,
            "/octocat/Hello-World/info/refs?service=git-upload-pack"
        ));
        assert!(admitted(
            Method::POST,
            "/octocat/Hello-World/git-upload-pack"
        ));
        assert!(admitted(
            Method::POST,
            "/octocat/Hello-World.git/git-upload-pack"
        ));
        assert!(!admitted(
            Method::POST,
            "/octocat/Hello-World/git-receive-pack"
        ));
        assert!(!admitted(
            Method::POST,
            "/octocat/Hello-World/git-upload-pack/extra"
        ));
        assert!(!admitted(Method::POST, "/graphql"));
        assert!(!admitted(
            Method::PUT,
            "/octocat/Hello-World/git-upload-pack"
        ));
        let opaque = EgressGrant::opaque("github.com", 443).expect("fixture grant");
        assert!(
            !opaque
                .admits(
                    &target,
                    &Method::POST,
                    "/octocat/Hello-World/git-upload-pack"
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
        let Err(PolicyDenial::NotGranted { hint }) =
            policy.authorize(&elsewhere, &Method::GET, "/api/packages/owner/npm/pkg")
        else {
            panic!("an ungranted destination is NotGranted");
        };
        // The denial teaches both remedies: the standing grant every workspace of the project
        // holds, and the one-workspace grant.
        assert_eq!(
            hint,
            "cowshed grant --project-wide --egress elsewhere.test:443 (every workspace of this project) \
             or cowshed grant <ws> --egress elsewhere.test:443 (this workspace)"
        );
    }

    fn https(host: &str) -> CanonicalTarget {
        CanonicalTarget::from_authority(&format!("{host}:443"), TargetScheme::Https)
            .expect("fixture target")
    }

    fn npm_route(origin_host: &str, prefixes: &[&str], credentialed: bool) -> MirrorRoute {
        MirrorRoute::new(
            &format!("https://{origin_host}:443"),
            prefixes.iter().map(|prefix| (*prefix).to_owned()).collect(),
            credentialed,
        )
        .expect("fixture npm origin")
    }

    #[test]
    fn a_direct_npm_request_resolves_by_exact_origin_and_longest_admitted_prefix() {
        let policy = WorkspacePolicy {
            grants: Vec::new(),
            mirrors: vec![npm_route(
                "registry.npmjs.org",
                &["/react", "/react/-/", "/@scope/"],
                false,
            )],
        };
        policy.validate().expect("valid typed route");
        let registry = https("registry.npmjs.org");

        let tarball = policy
            .resolve_npm_registry(&registry, "/react/-/react-1.0.0.tgz")
            .expect("admitted route");
        assert_eq!(tarball.target, registry);
        assert_eq!(tarball.path, "/react/-/react-1.0.0.tgz");
        assert_eq!(tarball.protocol, MirrorProtocol::Npm);
        assert_eq!(tarball.admitted_prefix, "/react/-/");

        // The escaped slash decides admission only; the bytes forwarded upstream are the ones
        // the client sent.
        let scoped = policy
            .resolve_npm_registry(&registry, "/@scope%2fpkg?write=true")
            .expect("encoded npm scope is admitted without weakening generic paths");
        assert_eq!(scoped.path, "/@scope%2fpkg?write=true");
        assert_eq!(scoped.admitted_prefix, "/@scope/");
    }

    #[test]
    fn the_public_registry_is_read_anonymously_and_only_when_it_is_the_destination() {
        let policy = WorkspacePolicy {
            grants: Vec::new(),
            mirrors: vec![npm_route("registry.npmjs.org", &["/@scope/"], true)],
        };
        policy.validate().expect("valid typed route");
        let registry = https("registry.npmjs.org");

        // A path outside every configured prefix falls through to the anonymous baseline, and
        // the configured route's credential decision stays inside its own scope.
        let public = policy
            .resolve_npm_registry(&registry, "/lodash")
            .expect("public baseline");
        assert!(!public.credentialed);
        assert_eq!(public.admitted_prefix, "/");
        let private = policy
            .resolve_npm_registry(&registry, "/@scope%2fpkg")
            .expect("configured scope");
        assert!(private.credentialed);
        assert_eq!(private.admitted_prefix, "/@scope/");

        // With no configured route at all the public registry is still readable.
        let public = WorkspacePolicy::default()
            .resolve_npm_registry(&registry, "/lodash")
            .expect("baseline needs no route");
        assert!(!public.credentialed);

        // Any other destination — host, port or scheme — is no mirror: generic grants decide it.
        assert!(
            policy
                .resolve_npm_registry(&https("registry.example.test"), "/lodash")
                .is_none()
        );
        let other_port =
            CanonicalTarget::from_authority("registry.npmjs.org:8443", TargetScheme::Https)
                .expect("fixture target");
        assert!(
            policy
                .resolve_npm_registry(&other_port, "/lodash")
                .is_none()
        );
        let plain = CanonicalTarget::from_authority("registry.npmjs.org:80", TargetScheme::Http)
            .expect("fixture target");
        assert!(policy.resolve_npm_registry(&plain, "/lodash").is_none());
    }

    #[test]
    fn disjoint_private_scopes_resolve_only_on_their_own_origin() {
        let policy = WorkspacePolicy {
            grants: Vec::new(),
            mirrors: vec![
                npm_route("npm.company.test", &["/@company/"], true),
                npm_route("npm.other.test", &["/@other/"], true),
            ],
        };
        policy.validate().expect("disjoint private scopes");
        let company = policy
            .resolve_npm_registry(&https("npm.company.test"), "/@company%2fpkg")
            .expect("company scope on its own origin");
        assert_eq!(company.target, https("npm.company.test"));
        assert!(company.credentialed);
        assert_eq!(company.admitted_prefix, "/@company/");
        let other = policy
            .resolve_npm_registry(&https("npm.other.test"), "/@other%2fpkg")
            .expect("other scope on its own origin");
        assert_eq!(other.target, https("npm.other.test"));

        // A scope never crosses origins, and a private origin has no public baseline behind it.
        for (host, path) in [
            ("npm.other.test", "/@company%2fpkg"),
            ("npm.company.test", "/@other%2fpkg"),
            ("npm.company.test", "/react"),
            ("npm.company.test", "/@companyx%2fpkg"),
            ("cargo.company.test", "/@company%2fpkg"),
        ] {
            assert!(
                policy.resolve_npm_registry(&https(host), path).is_none(),
                "{host}{path}"
            );
        }
        assert_eq!(
            policy
                .resolve_npm_registry(&https("registry.npmjs.org"), "/react")
                .expect("public baseline")
                .target,
            https("registry.npmjs.org")
        );

        // Two routes may not claim overlapping scopes on the same origin.
        let mut overlapping = policy.clone();
        overlapping
            .mirrors
            .push(npm_route("npm.company.test", &["/@company/pkg"], true));
        assert!(matches!(
            overlapping.validate(),
            Err(PolicyError::OverlappingMirrorScope)
        ));
    }

    #[test]
    fn mirror_origins_are_canonical_once_and_aliases_cannot_claim_one_scope() {
        let left = MirrorRoute::new(
            "https://registry.npmjs.org",
            vec!["/@scope/".to_owned()],
            false,
        )
        .expect("implicit HTTPS port");
        let right = MirrorRoute::new(
            "https://REGISTRY.npmjs.org:443/",
            vec!["/@scope/pkg".to_owned()],
            true,
        )
        .expect("explicit HTTPS port and DNS case");
        assert_eq!(left.target(), right.target());
        assert!(matches!(
            WorkspacePolicy {
                grants: Vec::new(),
                mirrors: vec![left, right]
            }
            .validate(),
            Err(PolicyError::OverlappingMirrorScope)
        ));
        for origin in [
            "http://registry.npmjs.org",
            "https://user@registry.npmjs.org",
            "https://registry.npmjs.org/pkg",
            "https://registry.npmjs.org?query",
            "https://registry.npmjs.org#fragment",
        ] {
            assert!(
                MirrorRoute::new(origin, vec!["/".to_owned()], false).is_err(),
                "{origin}"
            );
        }
    }

    #[test]
    fn ambiguous_npm_paths_resolve_to_nothing_even_on_the_public_registry() {
        let policy = WorkspacePolicy::default();
        let registry = https("registry.npmjs.org");
        for path in [
            "/..%2fother",
            "/@scope%2f..%2fx",
            "//react",
            "/a%2f%2fb",
            "/a%5cb",
            "/a%00b",
            "/@scope%252fpkg",
            "/@scope%2",
            "/re\\act",
            "react",
        ] {
            assert!(
                policy.resolve_npm_registry(&registry, path).is_none(),
                "{path}"
            );
        }
        assert!(
            policy
                .resolve_npm_registry(&registry, "/@scope%2fpkg")
                .is_some()
        );
    }
}
