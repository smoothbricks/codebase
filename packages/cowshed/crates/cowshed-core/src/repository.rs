use std::collections::HashSet;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

const BINDING_VERSION: u32 = 1;
const RESERVED_LAYOUT_OWNERS: &[&str] = &[
    "gateway",
    "telemetry",
    "caches",
    "mnt",
    ".cowshed-volume.json",
];

/// A canonical, machine-independent repository identity in `owner/repo` form.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RepoId(String);

impl RepoId {
    pub fn parse(value: &str) -> Result<Self, RepoIdError> {
        if value.is_empty() {
            return Err(RepoIdError::Empty);
        }

        let mut components = value.split('/');
        let owner = components.next().unwrap_or_default();
        let repo = components.next().ok_or(RepoIdError::ComponentCount)?;
        if components.next().is_some() {
            return Err(RepoIdError::ComponentCount);
        }

        validate_identity_component(owner, RepoIdComponent::Owner)?;
        validate_identity_component(repo, RepoIdComponent::Repo)?;
        Ok(Self(format!("{owner}/{repo}")))
    }

    pub fn from_remote_url(value: &str) -> Result<Self, RemoteUrlError> {
        normalize_remote_url(value)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn owner(&self) -> &str {
        self.0.split_once('/').map_or("", |(owner, _)| owner)
    }

    pub fn repo(&self) -> &str {
        self.0.split_once('/').map_or("", |(_, repo)| repo)
    }
}

impl AsRef<str> for RepoId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RepoId {
    type Err = RepoIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl<'de> Deserialize<'de> for RepoId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepoIdComponent {
    Owner,
    Repo,
}

impl fmt::Display for RepoIdComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Owner => "owner",
            Self::Repo => "repo",
        })
    }
}

fn validate_identity_component(value: &str, component: RepoIdComponent) -> Result<(), RepoIdError> {
    if value.is_empty() {
        return Err(RepoIdError::EmptyComponent { component });
    }
    if value == "." {
        return Err(RepoIdError::TraversalComponent { component });
    }
    if value == ".." {
        return Err(RepoIdError::TraversalComponent { component });
    }

    let Some(first) = value.as_bytes().first() else {
        return Err(RepoIdError::EmptyComponent { component });
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return Err(RepoIdError::InvalidComponent { component });
    }
    if !value
        .as_bytes()
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(byte))
    {
        return Err(RepoIdError::InvalidComponent { component });
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RepoIdError {
    #[error("repository identity is empty")]
    Empty,
    #[error("repository identity must contain exactly one owner/repo separator")]
    ComponentCount,
    #[error("repository identity has an empty {component} component")]
    EmptyComponent { component: RepoIdComponent },
    #[error("repository identity {component} component cannot be `.` or `..`")]
    TraversalComponent { component: RepoIdComponent },
    #[error("repository identity {component} component must match [a-z0-9][a-z0-9._-]*")]
    InvalidComponent { component: RepoIdComponent },
}

/// Normalize a supported Git remote URL to its canonical lowercase `owner/repo` identity.
///
/// Supported forms are `ssh://`, `https://`, `http://`, `git://`, and the common
/// SCP-like SSH spelling `user@host:owner/repo.git`.
pub fn normalize_remote_url(value: &str) -> Result<RepoId, RemoteUrlError> {
    if value.is_empty() {
        return Err(RemoteUrlError::EmptyOrPadded);
    }
    if value.trim() != value {
        return Err(RemoteUrlError::EmptyOrPadded);
    }
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(RemoteUrlError::InvalidSyntax);
    }
    if value.contains('\\') {
        return Err(RemoteUrlError::InvalidSyntax);
    }

    let suffix_start = value
        .char_indices()
        .find_map(|(index, character)| matches!(character, '?' | '#').then_some(index));
    let without_suffix = suffix_start.map_or(value, |index| &value[..index]);
    let path = if let Some((scheme, remainder)) = without_suffix.split_once("://") {
        if !matches!(
            scheme.to_ascii_lowercase().as_str(),
            "ssh" | "https" | "http" | "git"
        ) {
            return Err(RemoteUrlError::UnsupportedTransport);
        }
        let (authority, path) = remainder
            .split_once('/')
            .ok_or(RemoteUrlError::MissingRepositoryPath)?;
        validate_authority(authority)?;
        path
    } else {
        parse_scp_like(without_suffix)?
    };

    let mut path_components = path.split('/').filter(|component| !component.is_empty());
    let owner = path_components
        .next()
        .ok_or(RemoteUrlError::MissingRepositoryPath)?;
    let repo = path_components
        .next()
        .ok_or(RemoteUrlError::MissingRepositoryPath)?;
    if path_components.next().is_some() {
        return Err(RemoteUrlError::AmbiguousRepositoryPath);
    }

    let repo = strip_dot_git(repo)?;
    let mut canonical = String::with_capacity(owner.len() + 1 + repo.len());
    canonical.extend(
        owner
            .chars()
            .map(|character| character.to_ascii_lowercase()),
    );
    canonical.push('/');
    canonical.extend(repo.chars().map(|character| character.to_ascii_lowercase()));
    RepoId::parse(&canonical).map_err(RemoteUrlError::InvalidIdentity)
}

fn validate_authority(authority: &str) -> Result<(), RemoteUrlError> {
    if authority.is_empty() {
        return Err(RemoteUrlError::MissingHost);
    }
    if authority.matches('@').count() > 1 {
        return Err(RemoteUrlError::MissingHost);
    }
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if host_port.is_empty() {
        return Err(RemoteUrlError::MissingHost);
    }
    if host_port.contains('@') {
        return Err(RemoteUrlError::MissingHost);
    }

    let host = if let Some(bracketed) = host_port.strip_prefix('[') {
        let (host, port) = bracketed
            .split_once(']')
            .ok_or(RemoteUrlError::InvalidSyntax)?;
        if !port.is_empty() {
            let digits = port
                .strip_prefix(':')
                .ok_or(RemoteUrlError::InvalidSyntax)?;
            if digits.is_empty() {
                return Err(RemoteUrlError::InvalidSyntax);
            }
            if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(RemoteUrlError::InvalidSyntax);
            }
        }
        host
    } else {
        let (host, port) = host_port.rsplit_once(':').unwrap_or((host_port, ""));
        if host_port.contains(':') {
            if port.is_empty() {
                return Err(RemoteUrlError::InvalidSyntax);
            }
            if !port.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(RemoteUrlError::InvalidSyntax);
            }
        }
        host
    };

    if host.is_empty() {
        return Err(RemoteUrlError::MissingHost);
    }
    if host == "." {
        return Err(RemoteUrlError::MissingHost);
    }
    if host == ".." {
        return Err(RemoteUrlError::MissingHost);
    }
    if host.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(RemoteUrlError::MissingHost);
    }
    Ok(())
}

fn parse_scp_like(value: &str) -> Result<&str, RemoteUrlError> {
    if value.starts_with('/') {
        return Err(RemoteUrlError::UnsupportedTransport);
    }
    if value.starts_with('.') {
        return Err(RemoteUrlError::UnsupportedTransport);
    }
    let (authority, path) = value
        .split_once(':')
        .ok_or(RemoteUrlError::UnsupportedTransport)?;
    if !authority.contains('@') {
        return Err(RemoteUrlError::UnsupportedTransport);
    }
    validate_authority(authority)?;
    if path.is_empty() {
        return Err(RemoteUrlError::MissingRepositoryPath);
    }
    Ok(path)
}

fn strip_dot_git(repo: &str) -> Result<&str, RemoteUrlError> {
    let stripped = match repo.len().checked_sub(4) {
        Some(suffix_index)
            if repo
                .get(suffix_index..)
                .is_some_and(|suffix| suffix.eq_ignore_ascii_case(".git")) =>
        {
            repo.get(..suffix_index)
                .ok_or(RemoteUrlError::InvalidSyntax)?
        }
        _ => repo,
    };
    if stripped.is_empty() {
        return Err(RemoteUrlError::MissingRepositoryPath);
    }
    Ok(stripped)
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RemoteUrlError {
    #[error("remote URL is empty or has surrounding whitespace")]
    EmptyOrPadded,
    #[error("remote URL uses an unsupported or missing transport")]
    UnsupportedTransport,
    #[error("remote URL has invalid syntax")]
    InvalidSyntax,
    #[error("remote URL is missing a host")]
    MissingHost,
    #[error("remote URL is missing an owner/repository path")]
    MissingRepositoryPath,
    #[error("remote URL path is ambiguous; expected exactly owner/repo")]
    AmbiguousRepositoryPath,
    #[error("remote URL does not produce a canonical repository identity: {0}")]
    InvalidIdentity(RepoIdError),
}

/// Encode arbitrary UTF-8 as one safe, non-empty filesystem component.
///
/// The encoding is deliberately one-way during path lookup: callers must never percent-decode it.
pub fn encode_component(value: &str) -> Result<String, ComponentEncodingError> {
    encode_component_with_role(value, false)
}

/// Encode an owner component at the store layout root, additionally escaping reserved namespaces.
pub fn encode_layout_owner(value: &str) -> Result<String, ComponentEncodingError> {
    encode_component_with_role(value, RESERVED_LAYOUT_OWNERS.contains(&value))
}

fn encode_component_with_role(
    value: &str,
    reserved_owner: bool,
) -> Result<String, ComponentEncodingError> {
    if value.is_empty() {
        return Err(ComponentEncodingError::Empty);
    }

    let escape_all_dots = matches!(value, "." | "..");
    let mut encoded = String::with_capacity(value.len());
    for (index, byte) in value.bytes().enumerate() {
        let allowed = byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || (index != 0 && matches!(byte, b'.' | b'_' | b'-'));
        if allowed && !escape_all_dots && !(reserved_owner && index == 0) {
            encoded.push(char::from(byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    Ok(encoded)
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ComponentEncodingError {
    #[error("filesystem component cannot be empty")]
    Empty,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundIdentity {
    pub repo_id: RepoId,
    pub remote_name: Option<String>,
    pub remote_url: Option<String>,
    pub primary: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepositoryBinding {
    pub version: u32,
    pub identities: Vec<BoundIdentity>,
}

impl RepositoryBinding {
    pub fn new(identities: Vec<BoundIdentity>) -> Result<Self, BindingError> {
        let binding = Self {
            version: BINDING_VERSION,
            identities,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub fn validate(&self) -> Result<(), BindingError> {
        if self.version != BINDING_VERSION {
            return Err(BindingError::UnsupportedVersion(self.version));
        }
        if self.identities.is_empty() {
            return Err(BindingError::NoIdentities);
        }

        let primary_count = self
            .identities
            .iter()
            .filter(|identity| identity.primary)
            .count();
        if primary_count != 1 {
            return Err(BindingError::PrimaryCount(primary_count));
        }

        let mut repo_ids: HashSet<&RepoId> = HashSet::with_capacity(self.identities.len());
        let mut remote_names: HashSet<&str> = HashSet::with_capacity(self.identities.len());
        for identity in &self.identities {
            if !repo_ids.insert(&identity.repo_id) {
                return Err(BindingError::DuplicateRepoId(identity.repo_id.clone()));
            }

            match (&identity.remote_name, &identity.remote_url) {
                (None, None) => {}
                (Some(name), Some(url)) => {
                    if name.is_empty() {
                        return Err(BindingError::InvalidRemoteName);
                    }
                    if name.trim() != name {
                        return Err(BindingError::InvalidRemoteName);
                    }
                    if name.bytes().any(|byte| byte.is_ascii_control()) {
                        return Err(BindingError::InvalidRemoteName);
                    }
                    if !remote_names.insert(name.as_str()) {
                        return Err(BindingError::DuplicateRemoteName(name.clone()));
                    }
                    let normalized = normalize_remote_url(url).map_err(|source| {
                        BindingError::InvalidRemoteUrl {
                            remote_name: name.clone(),
                            source,
                        }
                    })?;
                    if normalized != identity.repo_id {
                        return Err(BindingError::RemoteIdentityMismatch {
                            remote_name: name.clone(),
                            expected: identity.repo_id.clone(),
                            actual: normalized,
                        });
                    }
                }
                _ => return Err(BindingError::IncompleteRemote),
            }
        }
        Ok(())
    }

    pub fn primary(&self) -> Result<&BoundIdentity, BindingError> {
        self.validate()?;
        self.identities
            .iter()
            .find(|identity| identity.primary)
            .ok_or(BindingError::PrimaryCount(0))
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BindingError {
    #[error("unsupported repository binding version {0}")]
    UnsupportedVersion(u32),
    #[error("repository binding must contain at least one identity")]
    NoIdentities,
    #[error("repository binding must contain exactly one primary identity, found {0}")]
    PrimaryCount(usize),
    #[error("repository binding contains duplicate repository identity {0}")]
    DuplicateRepoId(RepoId),
    #[error("repository binding remote name is empty or invalid")]
    InvalidRemoteName,
    #[error("repository binding contains duplicate remote name {0}")]
    DuplicateRemoteName(String),
    #[error("repository binding identity must specify both remote name and remote URL, or neither")]
    IncompleteRemote,
    #[error("repository binding remote {remote_name} has an invalid URL: {source}")]
    InvalidRemoteUrl {
        remote_name: String,
        source: RemoteUrlError,
    },
    #[error(
        "repository binding remote {remote_name} resolves to {actual}, not recorded identity {expected}"
    )]
    RemoteIdentityMismatch {
        remote_name: String,
        expected: RepoId,
        actual: RepoId,
    },
}

impl<'de> Deserialize<'de> for RepositoryBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RawBinding {
            version: u32,
            identities: Vec<BoundIdentity>,
        }

        let raw = RawBinding::deserialize(deserializer)?;
        let binding = Self {
            version: raw.version,
            identities: raw.identities,
        };
        binding.validate().map_err(serde::de::Error::custom)?;
        Ok(binding)
    }
}

impl<'de> Deserialize<'de> for BoundIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RawIdentity {
            repo_id: RepoId,
            remote_name: Option<String>,
            remote_url: Option<String>,
            primary: bool,
        }

        let raw = RawIdentity::deserialize(deserializer)?;
        Ok(Self {
            repo_id: raw.repo_id,
            remote_name: raw.remote_name,
            remote_url: raw.remote_url,
            primary: raw.primary,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectPaths {
    pub store_root: PathBuf,
    pub project_root: PathBuf,
    pub repository_binding: PathBuf,
    pub policy: PathBuf,
    pub sessions: PathBuf,
    pub checkpoints: PathBuf,
    pub quarantine: PathBuf,
    pub waivers: PathBuf,
    pub mount_root: PathBuf,
}

impl ProjectPaths {
    pub fn new(store_root: impl AsRef<Path>, repo_id: &RepoId) -> Result<Self, PathLayoutError> {
        let store_root = validate_store_root(store_root.as_ref())?.to_path_buf();
        let owner = encode_layout_owner(repo_id.owner())?;
        let repo = encode_component(repo_id.repo())?;
        let project_root = checked_join(&store_root, [owner.as_str(), repo.as_str()])?;
        let mount_root = checked_join(&store_root, ["mnt", owner.as_str(), repo.as_str()])?;

        Ok(Self {
            repository_binding: checked_join(&project_root, ["repository.json"])?,
            policy: checked_join(&project_root, ["policy.json"])?,
            sessions: checked_join(&project_root, ["sessions"])?,
            checkpoints: checked_join(&project_root, ["checkpoints"])?,
            quarantine: checked_join(&project_root, ["quarantine"])?,
            waivers: checked_join(&project_root, ["waivers.json"])?,
            store_root,
            project_root,
            mount_root,
        })
    }

    /// Lexically checks containment without resolving or following filesystem links.
    /// Filesystem callers must additionally use no-follow component traversal.
    pub fn contains(&self, candidate: &Path) -> bool {
        is_lexically_contained(&self.store_root, candidate)
    }
}

fn validate_store_root(root: &Path) -> Result<&Path, PathLayoutError> {
    if !root.is_absolute() {
        return Err(PathLayoutError::StoreRootNotAbsolute);
    }
    if root
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(PathLayoutError::StoreRootNotNormalized);
    }
    Ok(root)
}

fn checked_join<'a>(
    root: &Path,
    components: impl IntoIterator<Item = &'a str>,
) -> Result<PathBuf, PathLayoutError> {
    let mut candidate = root.to_path_buf();
    for component in components {
        if component.is_empty() {
            return Err(PathLayoutError::UnsafeComponent);
        }
        let mut path_components = Path::new(component).components();
        if !matches!(path_components.next(), Some(Component::Normal(_))) {
            return Err(PathLayoutError::UnsafeComponent);
        }
        if path_components.next().is_some() {
            return Err(PathLayoutError::UnsafeComponent);
        }
        candidate.push(component);
    }
    if !is_lexically_contained(root, &candidate) {
        return Err(PathLayoutError::EscapesStoreRoot);
    }
    Ok(candidate)
}

fn is_lexically_contained(root: &Path, candidate: &Path) -> bool {
    if validate_store_root(root).is_err() {
        return false;
    }
    if !candidate.is_absolute() {
        return false;
    }
    let Ok(relative) = candidate.strip_prefix(root) else {
        return false;
    };
    if relative.components().next().is_none() {
        return false;
    }
    relative
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PathLayoutError {
    #[error("store root must be absolute")]
    StoreRootNotAbsolute,
    #[error("store root must be lexically normalized")]
    StoreRootNotNormalized,
    #[error("path layout component could not be encoded: {0}")]
    ComponentEncoding(#[from] ComponentEncodingError),
    #[error("path layout component is unsafe")]
    UnsafeComponent,
    #[error("derived path escapes the store root")]
    EscapesStoreRoot,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_id(value: &str) -> RepoId {
        RepoId::parse(value).expect("test repository identity should be valid")
    }

    #[test]
    fn repo_id_accepts_only_canonical_owner_repo() {
        let parsed = repo_id("acme/widget.rs");
        assert_eq!(parsed.owner(), "acme");
        assert_eq!(parsed.repo(), "widget.rs");

        for invalid in [
            "",
            "acme",
            "/repo",
            "owner/",
            "a/b/c",
            "Acme/widget",
            "acme/Widget",
            "acme/../widget",
            "./widget",
            "acme/%2f",
            "acme/repo name",
            "acme/repo\\x",
            "_acme/widget",
        ] {
            assert!(RepoId::parse(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn repository_identity_interfaces_and_typed_traversal_errors_are_exact() {
        let identity = repo_id("acme/widget");
        assert_eq!(identity.as_ref(), "acme/widget");
        assert_eq!(identity.to_string(), "acme/widget");
        assert_eq!(RepoIdComponent::Owner.to_string(), "owner");
        assert_eq!(RepoIdComponent::Repo.to_string(), "repo");
        assert_eq!(
            RepoId::parse("./widget"),
            Err(RepoIdError::TraversalComponent {
                component: RepoIdComponent::Owner
            })
        );
        assert_eq!(
            RepoId::parse("../widget"),
            Err(RepoIdError::TraversalComponent {
                component: RepoIdComponent::Owner
            })
        );
        assert_eq!(
            RepoId::parse("acme/."),
            Err(RepoIdError::TraversalComponent {
                component: RepoIdComponent::Repo
            })
        );
        assert_eq!(
            RepoId::parse("acme/.."),
            Err(RepoIdError::TraversalComponent {
                component: RepoIdComponent::Repo
            })
        );
    }

    #[test]
    fn normalizes_supported_remote_spellings() {
        for remote in [
            "https://github.com/Acme/Widget.git",
            "http://user:secret@github.com//Acme///Widget.git?ref=ignored#fragment",
            "ssh://git@github.com/Acme/Widget.git",
            "ssh://git@github.com:22/Acme/Widget.git",
            "git://github.com/Acme/Widget.GIT",
            "git@github.com:Acme/Widget.git",
            "git@github.com:Acme/Widget.Git",
            "https://github.com///Acme//Widget.git/",
        ] {
            assert_eq!(normalize_remote_url(remote), Ok(repo_id("acme/widget")));
        }
    }

    #[test]
    fn rejects_ambiguous_or_path_like_remotes() {
        for remote in [
            "github.com/acme/widget",
            "file:///acme/widget",
            "/tmp/acme/widget",
            "../acme/widget",
            "https:///acme/widget",
            "https://github.com/acme/widget/extra",
            "https://github.com/acme",
            "https://github.com/acme/../widget",
            "https://github.com/acme/%77idget",
            "git@github.com:acme/widget/extra.git",
            "git@github.com:",
            " ssh://git@github.com/acme/widget.git",
        ] {
            assert!(normalize_remote_url(remote).is_err(), "accepted {remote:?}");
        }
    }

    #[test]
    fn remote_rejection_branches_are_typed_and_dot_git_is_only_a_suffix() {
        for (remote, expected) in [
            ("", RemoteUrlError::EmptyOrPadded),
            (" git@github.com:acme/widget", RemoteUrlError::EmptyOrPadded),
            (
                "git@github.com:acme/widget\n",
                RemoteUrlError::EmptyOrPadded,
            ),
            ("git@github.com:acme\\widget", RemoteUrlError::InvalidSyntax),
            (
                "git@github.com:acme/wid\nget",
                RemoteUrlError::InvalidSyntax,
            ),
            (
                "ssh://git@@github.com/acme/widget",
                RemoteUrlError::MissingHost,
            ),
            ("ssh://git@/acme/widget", RemoteUrlError::MissingHost),
            ("ssh://[::1/acme/widget", RemoteUrlError::InvalidSyntax),
            ("ssh://[::1]:/acme/widget", RemoteUrlError::InvalidSyntax),
            ("ssh://[::1]:no/acme/widget", RemoteUrlError::InvalidSyntax),
            (
                "ssh://github.com:/acme/widget",
                RemoteUrlError::InvalidSyntax,
            ),
            (
                "ssh://github.com:no/acme/widget",
                RemoteUrlError::InvalidSyntax,
            ),
            ("ssh://./acme/widget", RemoteUrlError::MissingHost),
            ("ssh://../acme/widget", RemoteUrlError::MissingHost),
            ("ssh://bad host/acme/widget", RemoteUrlError::MissingHost),
        ] {
            assert_eq!(normalize_remote_url(remote), Err(expected), "{remote:?}");
        }
        assert_eq!(
            normalize_remote_url("ssh://[::1]:22/acme/widget"),
            Ok(repo_id("acme/widget"))
        );
        assert_eq!(
            normalize_remote_url("https://github.com/acme/widget.git-extra"),
            Ok(repo_id("acme/widget.git-extra"))
        );
    }

    #[test]
    fn component_encoding_is_independent_and_blocks_aliases() {
        assert_eq!(encode_component("acme"), Ok("acme".into()));
        assert_eq!(encode_component("a.b_c-d"), Ok("a.b_c-d".into()));
        assert_eq!(encode_component("."), Ok("%2E".into()));
        assert_eq!(encode_component(".."), Ok("%2E%2E".into()));
        assert_eq!(encode_component("%2f"), Ok("%252f".into()));
        assert_eq!(encode_component("A/é"), Ok("%41%2F%C3%A9".into()));
        assert_eq!(encode_component("_hidden"), Ok("%5Fhidden".into()));
        assert_eq!(encode_component(""), Err(ComponentEncodingError::Empty));
        assert_eq!(encode_layout_owner("gateway"), Ok("%67ateway".into()));
        assert_eq!(encode_layout_owner("telemetry"), Ok("%74elemetry".into()));
        assert_eq!(encode_layout_owner("caches"), Ok("%63aches".into()));
        assert_eq!(encode_layout_owner("mnt"), Ok("%6Dnt".into()));
        assert_eq!(
            encode_layout_owner(".cowshed-volume.json"),
            Ok("%2Ecowshed-volume.json".into())
        );
        assert_eq!(encode_component("gateway"), Ok("gateway".into()));
    }

    #[test]
    fn binding_requires_one_primary_and_matching_remote() {
        let primary = BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".into()),
            remote_url: Some("git@github.com:Acme/Widget.git".into()),
            primary: true,
        };
        let alternate = BoundIdentity {
            repo_id: repo_id("upstream/widget"),
            remote_name: Some("upstream".into()),
            remote_url: Some("https://github.com/upstream/widget.git".into()),
            primary: false,
        };
        let binding = RepositoryBinding::new(vec![primary.clone(), alternate]).unwrap();
        assert_eq!(binding.primary().unwrap(), &primary);

        let mut mismatch = primary.clone();
        mismatch.remote_url = Some("https://github.com/other/widget.git".into());
        assert!(matches!(
            RepositoryBinding::new(vec![mismatch]),
            Err(BindingError::RemoteIdentityMismatch { .. })
        ));

        let mut not_primary = primary;
        not_primary.primary = false;
        assert_eq!(
            RepositoryBinding::new(vec![not_primary]),
            Err(BindingError::PrimaryCount(0))
        );
    }

    #[test]
    fn binding_rejects_each_ambiguous_identity_shape() {
        assert_eq!(
            RepositoryBinding::new(vec![]),
            Err(BindingError::NoIdentities)
        );
        for invalid_name in ["", " origin", "origin\n"] {
            let identity = BoundIdentity {
                repo_id: repo_id("acme/widget"),
                remote_name: Some(invalid_name.into()),
                remote_url: Some("https://github.com/acme/widget".into()),
                primary: true,
            };
            assert_eq!(
                RepositoryBinding::new(vec![identity]),
                Err(BindingError::InvalidRemoteName)
            );
        }

        let local = BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: None,
            remote_url: None,
            primary: true,
        };
        assert!(matches!(
            RepositoryBinding::new(vec![local.clone(), local]),
            Err(BindingError::PrimaryCount(2) | BindingError::DuplicateRepoId(_))
        ));
        let incomplete = BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".into()),
            remote_url: None,
            primary: true,
        };
        assert_eq!(
            RepositoryBinding::new(vec![incomplete]),
            Err(BindingError::IncompleteRemote)
        );
        let invalid_url = BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".into()),
            remote_url: Some("file:///acme/widget".into()),
            primary: true,
        };
        assert!(matches!(
            RepositoryBinding::new(vec![invalid_url]),
            Err(BindingError::InvalidRemoteUrl { .. })
        ));
    }

    #[test]
    fn binding_allows_explicit_local_only_identity() {
        let local = BoundIdentity {
            repo_id: repo_id("local/tool"),
            remote_name: None,
            remote_url: None,
            primary: true,
        };
        assert!(RepositoryBinding::new(vec![local]).is_ok());
    }

    #[test]
    fn derives_contained_project_paths() {
        let paths = ProjectPaths::new("/Users/test/.cowshed", &repo_id("acme/widget")).unwrap();
        assert_eq!(
            paths.project_root,
            Path::new("/Users/test/.cowshed/acme/widget")
        );
        assert_eq!(
            paths.repository_binding,
            Path::new("/Users/test/.cowshed/acme/widget/repository.json")
        );
        assert_eq!(
            paths.mount_root,
            Path::new("/Users/test/.cowshed/mnt/acme/widget")
        );
        assert!(paths.contains(&paths.sessions));
        assert!(!paths.contains(Path::new("/Users/test/.cowshed")));
        assert!(!paths.contains(Path::new("/Users/test/.cowshed/acme/../escape")));
        assert!(!paths.contains(Path::new("/Users/test/.cowshed-other/acme/widget")));
    }

    #[test]
    fn low_level_path_guards_reject_every_escape_shape() {
        for component in ["", ".", "..", "../escape", "nested/child", "/absolute"] {
            assert_eq!(
                checked_join(Path::new("/store"), [component]),
                Err(PathLayoutError::UnsafeComponent),
                "{component:?}"
            );
        }
        assert!(!is_lexically_contained(
            Path::new("relative"),
            Path::new("/store/acme")
        ));
        assert!(!is_lexically_contained(
            Path::new("/store"),
            Path::new("relative")
        ));
        assert!(!is_lexically_contained(
            Path::new("/store"),
            Path::new("/other/acme")
        ));
        assert!(!is_lexically_contained(
            Path::new("/store"),
            Path::new("/store")
        ));
    }

    #[test]
    fn reserved_owner_cannot_alias_layout_root() {
        let paths = ProjectPaths::new("/store", &repo_id("gateway/widget")).unwrap();
        assert_eq!(paths.project_root, Path::new("/store/%67ateway/widget"));
        assert_ne!(paths.project_root, Path::new("/store/gateway/widget"));
    }

    #[test]
    fn rejects_unsafe_store_roots() {
        assert_eq!(
            ProjectPaths::new("relative/store", &repo_id("acme/widget")),
            Err(PathLayoutError::StoreRootNotAbsolute)
        );
        assert_eq!(
            ProjectPaths::new("/safe/../escape", &repo_id("acme/widget")),
            Err(PathLayoutError::StoreRootNotNormalized)
        );
    }
}
