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

    pub(crate) fn is_exact(&self) -> bool {
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

    pub(crate) fn admits(&self, target: &CanonicalTarget, method: &Method, path: &str) -> bool {
        self.port == target.port
            && self.host.matches(&target.host)
            && (method == Method::CONNECT
                || (self.mode == EgressMode::Intercept
                    && self.methods.contains(method.as_str())
                    && self
                        .path_prefixes
                        .iter()
                        .any(|prefix| path.starts_with(prefix))))
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
        for (index, left) in self.mirrors.iter().enumerate() {
            if self.mirrors[index + 1..]
                .iter()
                .any(|right| left.local_prefix == right.local_prefix)
            {
                return Err(PolicyError::DuplicateMirrorPrefix);
            }
        }
        Ok(())
    }

    pub(crate) fn authorize<'a>(
        &'a self,
        target: &CanonicalTarget,
        method: &Method,
        path: &str,
    ) -> Result<&'a EgressGrant, PolicyDenial> {
        let normalized = normalize_path(path).map_err(|_| PolicyDenial::InvalidPath)?;
        self.grants
            .iter()
            .find(|grant| grant.admits(target, method, &normalized))
            .ok_or_else(|| PolicyDenial::NotGranted {
                hint: format!("cowshed grant <ws> --egress {}", target.authority()),
            })
    }

    pub fn resolve_mirror(&self, path: &str) -> Option<ResolvedMirrorRoute> {
        if let Some(route) = self
            .mirrors
            .iter()
            .filter(|route| path.starts_with(&route.local_prefix))
            .max_by_key(|route| route.local_prefix.len())
        {
            let suffix = &path[route.local_prefix.len() - 1..];
            let (normalized, admission_path) =
                normalize_mirror_suffix(route.protocol, suffix).ok()?;
            let admitted_prefix = route
                .admitted_prefixes
                .iter()
                .filter(|prefix| admission_path.starts_with(prefix.as_str()))
                .max_by_key(|prefix| prefix.len())?
                .clone();
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
    if !path_and_query.starts_with('/')
        || path_and_query.len() > 8192
        || path_and_query.contains(['\\', '\0', '\r', '\n'])
    {
        return Err(PolicyError::InvalidPath);
    }
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _)| path);
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
        let high = hex(bytes[index + 1]).ok_or(PolicyError::InvalidPath)?;
        let low = hex(bytes[index + 2]).ok_or(PolicyError::InvalidPath)?;
        let value = (high << 4) | low;
        if matches!(value, b'\\' | 0 | b'%') {
            return Err(PolicyError::InvalidPath);
        }
        decoded.push(value);
        index += 3;
    }
    let admission_path = String::from_utf8(decoded).map_err(|_| PolicyError::InvalidPath)?;
    if admission_path.contains("//")
        || admission_path
            .split('/')
            .any(|segment| segment == "." || segment == "..")
    {
        return Err(PolicyError::InvalidPath);
    }
    Ok((path_and_query.to_owned(), admission_path))
}

pub fn normalize_path(path: &str) -> Result<String, PolicyError> {
    if !path.starts_with('/') || path.len() > 8192 || path.contains(['\\', '\0']) {
        return Err(PolicyError::InvalidPath);
    }
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
        let high = hex(bytes[index + 1]).ok_or(PolicyError::InvalidPath)?;
        let low = hex(bytes[index + 2]).ok_or(PolicyError::InvalidPath)?;
        let value = (high << 4) | low;
        if matches!(value, b'/' | b'\\' | 0 | b'%') {
            return Err(PolicyError::InvalidPath);
        }
        decoded.push(value);
        index += 3;
    }
    let decoded = String::from_utf8(decoded).map_err(|_| PolicyError::InvalidPath)?;
    if decoded
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err(PolicyError::InvalidPath);
    }
    Ok(decoded)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub(crate) enum PolicyDenial {
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
