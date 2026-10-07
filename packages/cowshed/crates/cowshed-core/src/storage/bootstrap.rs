use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, mpsc};

use async_trait::async_trait;
#[cfg(target_os = "macos")]
use plist::Value;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use toml::Spanned;

use crate::capabilities::{CapabilityId, CapabilityOverride, DeclaredState};
use crate::process::fmt_command_failure;

use super::fstab::FstabPin;

pub mod native;
pub use native::{
    CachesVolume, FstabOutcome, HostAction, HostActionOutcome, HostActionResult, HostSetupPlan,
    HostSetupReport, HostUninstallPlan, UninstallFstabOutcome, UninstallReport,
    UninstallServiceOutcome, VolumeOutcome, VolumeState, execute_host_setup,
    execute_host_uninstall, plan_host_setup, plan_host_uninstall,
};

pub const VOLUME_MARKER_FILE: &str = ".cowshed-volume.json";
pub const APFS_STORE_VOLUME: &str = "cowshed.store";
pub const APFS_CACHES_VOLUME: &str = "cowshed.caches";
/// Machine-global evidence roots shared by every user in the herd.
///
/// Bootstrap volumes cannot follow `$HOME`: doing so gives each user a different view of the
/// same machine-global herd and leaves the evidence layer outside the dedicated volumes.
pub const STORE_ROOT: &str = "/private/cowshed/store";
/// Where the retired `cowshed.caches` volume (or `<pool>/cowshed/caches` dataset) is mounted on a
/// host that still has one. Nothing is provisioned there any more: setup moves what it holds into
/// the host HOME and `setup --retire-caches-volume` deletes it (03_caches.md).
pub const RETIRED_CACHES_MOUNTPOINT: &str = "/private/cowshed/caches";
/// Where launchd reads the storage mount service from; owned here so the CLI's doctor names the
/// same file the macOS planner installs.
pub const MOUNT_SERVICE_PLIST: &str = "/Library/LaunchDaemons/dev.cowshed.storage.plist";
pub const ZFS_COWSHED_ROOT: &str = "cowshed";
pub const ZFS_STORE_CHILD: &str = "store";
pub const ZFS_CACHES_CHILD: &str = "caches";
pub const ZFS_PROJECTS_CHILD: &str = "projects";

/// The store-root names cowshed owns, so no walker mistakes one for a repository owner.
///
/// The store root is a mount point whose children are `<owner>/` project namespaces plus the
/// controller's own directories. Every enumeration has to skip the same set, and a walker holding
/// a shorter list reports a controller directory as an owner and then invents repositories under
/// it. Dotted names are reserved wholesale rather than listed: that covers the volume marker,
/// macOS's per-volume system directories (`.fseventsd`, `.Spotlight-V100`, `.Trashes`), and the
/// `.staging`/`.trash` object namespaces, none of which a name list could keep up with.
pub const RESERVED_STORE_NAMESPACES: &[&str] = &[
    "caches",
    "gateway",
    "mnt",
    "quarantine",
    "run",
    "telemetry",
    "tmp",
];

pub fn is_reserved_store_namespace(name: &str) -> bool {
    name.starts_with('.') || RESERVED_STORE_NAMESPACES.contains(&name)
}

pub(crate) use crate::device::DISKUTIL;
const ZFS: &str = "/usr/sbin/zfs";
const MARKER_VERSION: u32 = 1;

/// Repository-owned settings accepted from `.cowshed.toml`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CowshedConfig {
    substrate: Option<SubstrateConfig>,
    /// `[sandbox] deny`: workspace-relative paths no job may read or write. Trusted only from
    /// main's checkout, which the operator owns; a workspace's copy is the agent's to edit.
    sandbox_deny: Vec<PathBuf>,
    /// `[caches] home`: HOME-relative cache directories the repository's own tooling places,
    /// shared into every sandbox of the project. Trusted only from main's checkout, exactly like
    /// `[sandbox] deny`.
    caches_home: Vec<PathBuf>,
    capabilities: std::collections::BTreeMap<
        crate::capabilities::CapabilityId,
        crate::capabilities::CapabilityOverride,
    >,
    /// `[build] capacity`: the capacity of the build volumes this project creates from nothing
    /// (16_build_volumes.md, "Substrate"); a clone and a seed inherit their source's instead.
    build_capacity: Option<crate::metadata::ImageCapacity>,
    /// `[build] state`: build state the repository declares for tools no capability detects
    /// (16_build_volumes.md, "Declared build state"), sorted, no spelling inside another.
    build_state: Vec<crate::capabilities::DeclaredState>,
}

impl CowshedConfig {
    pub fn substrate(&self) -> Option<&SubstrateConfig> {
        self.substrate.as_ref()
    }

    pub fn sandbox_deny(&self) -> &[PathBuf] {
        &self.sandbox_deny
    }

    pub fn caches_home(&self) -> &[PathBuf] {
        &self.caches_home
    }

    pub fn capabilities(
        &self,
    ) -> &std::collections::BTreeMap<
        crate::capabilities::CapabilityId,
        crate::capabilities::CapabilityOverride,
    > {
        &self.capabilities
    }

    /// The capacity a build volume created from nothing gets: `[build] capacity`, else
    /// [`crate::build_volume::DEFAULT_BUILD_VOLUME_CAPACITY`].
    pub fn build_capacity(&self) -> crate::metadata::ImageCapacity {
        self.build_capacity
            .unwrap_or(crate::build_volume::DEFAULT_BUILD_VOLUME_CAPACITY)
    }

    /// The entries `[build] state` declares; empty without it. Patterns are expanded against the
    /// checkout at discovery.
    pub fn build_state(&self) -> &[crate::capabilities::DeclaredState] {
        &self.build_state
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubstrateConfig {
    pool: String,
}

impl SubstrateConfig {
    pub fn pool(&self) -> &str {
        &self.pool
    }
}

/// `.cowshed.toml` as TOML spells it: every section and key cowshed reads, and nothing else. The
/// TOML parser refuses a duplicated key or table and serde an unknown one, so a setting cannot
/// silently change project detection; the spans locate each value for the validation in
/// [`parse_cowshed_config`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    substrate: Option<SubstrateSection>,
    sandbox: Option<SandboxSection>,
    build: Option<BuildSection>,
    caches: Option<CachesSection>,
    #[serde(default)]
    capabilities: BTreeMap<CapabilityId, CapabilitySection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubstrateSection {
    kind: ExplicitSubstrate,
    pool: Spanned<String>,
}

/// `[substrate] kind`: only an explicit ZFS pool overrides the filesystem evidence.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum ExplicitSubstrate {
    Zfs,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SandboxSection {
    deny: Vec<Spanned<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BuildSection {
    capacity: Option<Spanned<String>>,
    state: Option<Vec<Spanned<String>>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CachesSection {
    home: Vec<Spanned<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilitySection {
    #[serde(default)]
    disabled: bool,
    directory: Option<Spanned<String>>,
}

/// Parse repository-owned storage, sandbox deny, build volume capacity and declared state,
/// repository cache and convention-only capability overrides. Unknown or duplicated settings fail
/// rather than silently changing project detection.
pub fn parse_cowshed_config(input: &str) -> Result<CowshedConfig, ConfigError> {
    let file: ConfigFile = toml::from_str(input).map_err(|error| ConfigError::Malformed {
        line: error.span().map(|span| line_at(input, span.start)),
        message: error.message().to_owned(),
    })?;
    let substrate = match file.substrate {
        Some(SubstrateSection {
            kind: ExplicitSubstrate::Zfs,
            pool,
        }) => {
            let pool = pool.into_inner();
            validate_pool_name(&pool).map_err(ConfigError::InvalidPool)?;
            Some(SubstrateConfig { pool })
        }
        None => None,
    };
    let sandbox_deny = match file.sandbox {
        Some(section) => relative_paths(input, section.deny, "sandbox", "deny", "workspace")?,
        None => Vec::new(),
    };
    let caches_home = match file.caches {
        Some(section) => relative_paths(input, section.home, "caches", "home", "HOME")?,
        None => Vec::new(),
    };
    let (build_capacity, build_state) = match file.build {
        None => (None, Vec::new()),
        Some(BuildSection {
            capacity: None,
            state: None,
        }) => return Err(ConfigError::EmptySection("build")),
        Some(BuildSection { capacity, state }) => (
            capacity
                .map(|capacity| {
                    crate::metadata::ImageCapacity::parse(capacity.get_ref()).map_err(|reason| {
                        ConfigError::InvalidBuildCapacity {
                            line: line_of(input, &capacity),
                            reason,
                        }
                    })
                })
                .transpose()?,
            match state {
                Some(entries) => build_state(input, entries)?,
                None => Vec::new(),
            },
        ),
    };
    let capabilities =
        file.capabilities
            .into_iter()
            .map(|(id, section)| {
                let directory = section
                    .directory
                    .map(|directory| {
                        crate::capabilities::validate_override_directory(directory.get_ref())
                            .map_err(|reason| ConfigError::InvalidCapabilityDirectory {
                                section: id.section_name(),
                                line: line_of(input, &directory),
                                reason,
                            })
                    })
                    .transpose()?;
                Ok((
                    id,
                    CapabilityOverride {
                        disabled: section.disabled,
                        directory,
                    },
                ))
            })
            .collect::<Result<_, ConfigError>>()?;
    Ok(CowshedConfig {
        substrate,
        sandbox_deny,
        caches_home,
        capabilities,
        build_capacity,
        build_state,
    })
}

/// Main's `.cowshed.toml`, the only copy whose `[sandbox]` and `[caches]` sections are trusted:
/// main's checkout is the operator's, while a workspace's copy is the agent's to edit. A missing
/// file declares nothing; an unreadable or invalid one refuses rather than drop a deny or a cache.
pub fn main_cowshed_config(main_mount: &Path) -> crate::Result<CowshedConfig> {
    let path = main_mount.join(".cowshed.toml");
    let input = match std::fs::read_to_string(&path) {
        Ok(input) => input,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(CowshedConfig::default());
        }
        Err(error) => {
            return Err(crate::CowshedError::environment_missing(
                format!("cannot read {}: {error}", path.display()),
                "make main's .cowshed.toml readable, then retry",
            ));
        }
    };
    parse_cowshed_config(&input).map_err(|error| {
        crate::CowshedError::usage(
            format!("invalid {}: {error}", path.display()),
            "fix main's .cowshed.toml, then retry",
        )
    })
}

/// The 1-based line `value` starts on in `input`.
fn line_of<T>(input: &str, value: &Spanned<T>) -> usize {
    line_at(input, value.span().start)
}

/// The 1-based line of byte `offset` in `input`.
fn line_at(input: &str, offset: usize) -> usize {
    input
        .bytes()
        .take(offset)
        .filter(|&byte| byte == b'\n')
        .count()
        + 1
}

/// `[sandbox] deny` and `[caches] home`: paths, each relative to `base` (the workspace, or HOME)
/// and a non-empty path of plain components — no root, no `.` or `..` — sorted and deduplicated.
fn relative_paths(
    input: &str,
    entries: Vec<Spanned<String>>,
    section: &'static str,
    key: &'static str,
    base: &'static str,
) -> Result<Vec<PathBuf>, ConfigError> {
    let mut paths = entries
        .into_iter()
        .map(|entry| {
            let line = line_of(input, &entry);
            let path = entry.into_inner();
            let relative = Path::new(&path);
            if path.is_empty()
                || relative
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
            {
                return Err(ConfigError::InvalidRelativePath {
                    section,
                    key,
                    base,
                    line,
                    path,
                });
            }
            Ok(relative.components().collect::<PathBuf>())
        })
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    paths.dedup();
    Ok(paths)
}

/// `[build] state`: checkout-relative paths or patterns, each a [`DeclaredState`], sorted and
/// deduplicated, no spelling inside another: one inside another would link state through state.
/// A pattern's expansion is checked again at discovery, where its matches are known.
fn build_state(
    input: &str,
    entries: Vec<Spanned<String>>,
) -> Result<Vec<DeclaredState>, ConfigError> {
    let mut state = entries
        .into_iter()
        .map(|entry| {
            let line = line_of(input, &entry);
            let path = entry.into_inner();
            DeclaredState::parse(&path)
                .map(|declared| (declared, line))
                .map_err(|reason| ConfigError::InvalidBuildState { line, path, reason })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Sorting by (entry, line) keeps a repeated entry's first line.
    state.sort();
    state.dedup_by(|later, earlier| later.0 == earlier.0);
    let spellings: Vec<(PathBuf, usize)> = state
        .iter()
        .map(|(entry, line)| (entry.spelling(), *line))
        .collect();
    for (index, (outer, _)) in spellings.iter().enumerate() {
        if let Some((inner, line)) = spellings
            .iter()
            .enumerate()
            .find(|(other, (inner, _))| *other != index && inner.starts_with(outer))
            .map(|(_, inner)| inner)
        {
            return Err(ConfigError::OverlappingBuildState {
                line: *line,
                outer: outer.clone(),
                inner: inner.clone(),
            });
        }
    }
    Ok(state.into_iter().map(|(entry, _)| entry).collect())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    /// Not TOML, or TOML that is not a `.cowshed.toml`: a syntax error, a duplicated or unknown
    /// section or key, a missing key, or a value of the wrong type. `message` is the TOML
    /// reader's own, which names what it expected.
    #[error(
        "malformed configuration{}: {message}",
        line.map_or_else(String::new, |line| format!(" at line {line}"))
    )]
    Malformed {
        line: Option<usize>,
        message: String,
    },
    #[error(
        "[{section}] {key} entry {path:?} at line {line} must be a non-empty {base}-relative path without `.` or `..`"
    )]
    InvalidRelativePath {
        section: &'static str,
        key: &'static str,
        base: &'static str,
        line: usize,
        path: String,
    },
    #[error("[{section}] directory at line {line}: {reason}")]
    InvalidCapabilityDirectory {
        section: &'static str,
        line: usize,
        reason: &'static str,
    },
    #[error("[build] capacity at line {line}: {reason}")]
    InvalidBuildCapacity {
        line: usize,
        reason: crate::metadata::ImageCapacityError,
    },
    #[error("[build] state entry {path:?} at line {line} {reason}")]
    InvalidBuildState {
        line: usize,
        path: String,
        reason: &'static str,
    },
    #[error(
        "[build] state at line {line} declares {} inside {}; declare only the outer path",
        inner.display(),
        outer.display()
    )]
    OverlappingBuildState {
        line: usize,
        outer: PathBuf,
        inner: PathBuf,
    },
    #[error("the [{0}] section sets nothing")]
    EmptySection(&'static str),
    #[error("invalid ZFS pool: {0}")]
    InvalidPool(PoolNameError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegatedZfsDataset {
    dataset: String,
    pool: String,
    cowshed_root_available: bool,
}

impl DelegatedZfsDataset {
    pub fn new(
        dataset: impl Into<String>,
        cowshed_root_available: bool,
    ) -> Result<Self, PoolNameError> {
        let dataset = dataset.into();
        let mut components = dataset.split('/');
        let pool = components.next().unwrap_or_default().to_owned();
        validate_pool_name(&pool)?;
        if components.any(|component| component.is_empty() || component == "." || component == "..")
        {
            return Err(PoolNameError::InvalidDataset(dataset));
        }
        Ok(Self {
            dataset,
            pool,
            cowshed_root_available,
        })
    }

    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    pub fn pool(&self) -> &str {
        &self.pool
    }

    pub fn cowshed_root_available(&self) -> bool {
        self.cowshed_root_available
    }
}

/// Filesystem evidence gathered for the project root. Gathering it is a platform concern;
/// selection itself remains pure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatFsEvidence {
    Apfs {
        mount_source: PathBuf,
        container: Option<String>,
    },
    Zfs {
        containing_dataset: Option<DelegatedZfsDataset>,
    },
    Other {
        fs_type: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectionEvidence {
    ApfsStatFs {
        mount_source: PathBuf,
        container: String,
    },
    DelegatedContainingDataset {
        dataset: String,
        pool: String,
    },
    ExplicitConfig {
        pool: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectedSubstrate {
    Apfs {
        container: String,
        evidence: SelectionEvidence,
    },
    Zfs {
        pool: String,
        evidence: Vec<SelectionEvidence>,
    },
}

impl SelectedSubstrate {
    pub fn kind(&self) -> SubstrateKind {
        match self {
            Self::Apfs { .. } => SubstrateKind::Apfs,
            Self::Zfs { .. } => SubstrateKind::Zfs,
        }
    }

    pub fn evidence(&self) -> &[SelectionEvidence] {
        match self {
            Self::Apfs { evidence, .. } => std::slice::from_ref(evidence),
            Self::Zfs { evidence, .. } => evidence,
        }
    }
}

/// Deterministically select from supplied evidence. This function performs no discovery and has no
/// API through which it could scan APFS containers or ZFS pools.
pub fn select_substrate(
    statfs: StatFsEvidence,
    configured: Option<&SubstrateConfig>,
) -> Result<SelectedSubstrate, SelectionError> {
    if let Some(configured) = configured {
        let pool = configured.pool().to_owned();
        let mut evidence = vec![SelectionEvidence::ExplicitConfig { pool: pool.clone() }];
        if let StatFsEvidence::Zfs {
            containing_dataset: Some(dataset),
        } = &statfs
            && dataset.cowshed_root_available()
        {
            if dataset.pool() != pool {
                return Err(SelectionError::AmbiguousPools {
                    configured: pool,
                    containing: dataset.pool().to_owned(),
                });
            }
            evidence.insert(
                0,
                SelectionEvidence::DelegatedContainingDataset {
                    dataset: dataset.dataset().to_owned(),
                    pool: dataset.pool().to_owned(),
                },
            );
        }
        return Ok(SelectedSubstrate::Zfs { pool, evidence });
    }

    match statfs {
        StatFsEvidence::Apfs {
            mount_source,
            container: Some(container),
        } if !container.trim().is_empty() => Ok(SelectedSubstrate::Apfs {
            evidence: SelectionEvidence::ApfsStatFs {
                mount_source,
                container: container.clone(),
            },
            container,
        }),
        StatFsEvidence::Apfs { .. } => Err(SelectionError::MissingApfsContainer),
        StatFsEvidence::Zfs {
            containing_dataset: Some(dataset),
        } if dataset.cowshed_root_available() => {
            let pool = dataset.pool().to_owned();
            Ok(SelectedSubstrate::Zfs {
                evidence: vec![SelectionEvidence::DelegatedContainingDataset {
                    dataset: dataset.dataset().to_owned(),
                    pool: pool.clone(),
                }],
                pool,
            })
        }
        StatFsEvidence::Zfs { .. } | StatFsEvidence::Other { .. } => {
            Err(SelectionError::ExplicitZfsRequired)
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SelectionError {
    #[error("APFS statfs evidence did not identify its container")]
    MissingApfsContainer,
    #[error("selection requires explicit [substrate] kind = \"zfs\" and pool")]
    ExplicitZfsRequired,
    #[error(
        "configured ZFS pool {configured:?} conflicts with containing delegated pool {containing:?}"
    )]
    AmbiguousPools {
        configured: String,
        containing: String,
    },
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PoolNameError {
    #[error("pool names must be one non-empty component without whitespace or traversal")]
    InvalidPool,
    #[error("invalid containing dataset {0:?}")]
    InvalidDataset(String),
}

fn validate_pool_name(pool: &str) -> Result<(), PoolNameError> {
    let mut chars = pool.chars();
    let first = chars.next().ok_or(PoolNameError::InvalidPool)?;
    if first.is_ascii_digit()
        || !matches!(first, 'A'..='Z' | 'a'..='z' | '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
    {
        return Err(PoolNameError::InvalidPool);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SubstrateKind {
    Apfs,
    Zfs,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum VolumeRole {
    Store,
    Caches,
    Projects,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeMarker {
    version: u32,
    role: VolumeRole,
    substrate: SubstrateKind,
}

impl VolumeMarker {
    pub fn new(role: VolumeRole, substrate: SubstrateKind) -> Self {
        Self {
            version: MARKER_VERSION,
            role,
            substrate,
        }
    }

    pub fn role(&self) -> VolumeRole {
        self.role
    }

    pub fn substrate(&self) -> SubstrateKind {
        self.substrate
    }

    pub fn to_json(&self) -> Result<Vec<u8>, MarkerError> {
        let mut bytes = serde_json::to_vec_pretty(self).map_err(MarkerError::Encode)?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, MarkerError> {
        let marker: Self = serde_json::from_slice(bytes).map_err(MarkerError::Decode)?;
        if marker.version != MARKER_VERSION {
            return Err(MarkerError::UnsupportedVersion(marker.version));
        }
        Ok(marker)
    }
}

#[derive(Debug, Error)]
pub enum MarkerError {
    #[error("cannot encode volume marker: {0}")]
    Encode(serde_json::Error),
    #[error("cannot decode volume marker: {0}")]
    Decode(serde_json::Error),
    #[error("unsupported volume marker version {0}")]
    UnsupportedVersion(u32),
}

/// Refuse access through a mountpoint unless its authoritative marker is present and exact.
pub fn require_mounted_marker(
    bytes: Option<&[u8]>,
    expected_role: VolumeRole,
    expected_substrate: SubstrateKind,
) -> Result<VolumeMarker, MountGuardError> {
    let bytes = bytes.ok_or(MountGuardError::MissingMarker)?;
    let marker = VolumeMarker::from_json(bytes).map_err(MountGuardError::InvalidMarker)?;
    if marker.role() != expected_role || marker.substrate() != expected_substrate {
        return Err(MountGuardError::WrongMarker {
            expected_role,
            expected_substrate,
            actual: marker,
        });
    }
    Ok(marker)
}

#[derive(Debug, Error)]
pub enum MountGuardError {
    #[error("mountpoint marker is absent; treat the volume or dataset as unmounted")]
    MissingMarker,
    #[error("mountpoint marker is invalid: {0}")]
    InvalidMarker(MarkerError),
    #[error("mountpoint marker identifies the wrong role or substrate")]
    WrongMarker {
        expected_role: VolumeRole,
        expected_substrate: SubstrateKind,
        actual: VolumeMarker,
    },
}

/// The store root one host serves: the machine-global volume ([`Self::global`]) in production, or
/// a root a caller provisioned itself ([`Self::at`]), such as a scratch store of real APFS images.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalRoots {
    store: Arc<Path>,
    telemetry: PathBuf,
}

impl CanonicalRoots {
    pub fn global() -> Self {
        Self::at(PathBuf::from(STORE_ROOT))
    }

    pub fn at(store: PathBuf) -> Self {
        let telemetry = store.join("telemetry");
        Self {
            store: Arc::from(store),
            telemetry,
        }
    }

    pub fn store(&self) -> &Path {
        &self.store
    }

    /// The store root shared, so a project's identity answer names it without copying it.
    pub fn shared_store(&self) -> &Arc<Path> {
        &self.store
    }

    pub fn telemetry(&self) -> &Path {
        &self.telemetry
    }
}

/// Host-storage roots a project runtime, gateway, or service opens on, with the home they were
/// resolved for.
///
/// In production this is constructed only by the native boundaries that validated (or
/// bootstrapped) the machine-global volumes in place, and every component downstream takes the
/// value rather than resolving `$HOME` again: one open, one storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedHostStorage {
    home: PathBuf,
    roots: CanonicalRoots,
}

impl ValidatedHostStorage {
    pub fn new(home: PathBuf, roots: CanonicalRoots) -> Self {
        Self { home, roots }
    }

    pub fn roots(&self) -> &CanonicalRoots {
        &self.roots
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn store(&self) -> &Path {
        self.roots.store()
    }

    pub fn telemetry(&self) -> &Path {
        self.roots.telemetry()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostCommand {
    program: &'static str,
    args: Vec<String>,
}

impl HostCommand {
    fn new(program: &'static str, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            program,
            args: args.into_iter().map(Into::into).collect(),
        }
    }

    pub fn program(&self) -> &str {
        self.program
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VolumeRef {
    ExistingExact(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExistingMarkerEvidence {
    Missing,
    Invalid,
    UnsupportedVersion(u32),
    Valid(VolumeMarker),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExistingStorage {
    Absent,
    MountedValid {
        exact_identifier: String,
    },
    ExistingUnmounted {
        exact_identifier: String,
        marker: ExistingMarkerEvidence,
    },
    /// Exact cowshed APFS volume mounted at its canonical path, but without a marker because a
    /// prior provisioning transaction stopped before ownership and publication completed.
    MountedIncomplete {
        exact_identifier: String,
    },
    /// Exact reserved-name cowshed APFS volume in the selected container, detached before its
    /// marker could become readable. Only explicit provisioning may re-attest and recover it.
    DetachedIncomplete {
        exact_identifier: String,
    },
    /// Exact reserved-name cowshed APFS volume mounted at a noncanonical path or without the
    /// canonical `nobrowse` flag. Only explicit provisioning may unmount and repair it.
    MisMountedIncomplete {
        exact_identifier: String,
        current_mountpoint: PathBuf,
    },
    /// A reserved-name volume exists, but not in the home volume's APFS container. It is never
    /// treated as absent because creating another volume with the same name could hide user data.
    FoundElsewhere {
        container: String,
        device: String,
        volume_uuid: String,
        size_bytes: u64,
        mounted_at: Option<PathBuf>,
    },
}

impl ExistingStorage {
    pub fn mounted_valid(exact_identifier: impl Into<String>) -> Self {
        Self::MountedValid {
            exact_identifier: exact_identifier.into(),
        }
    }

    pub fn existing_unmounted(exact_identifier: impl Into<String>, marker: VolumeMarker) -> Self {
        Self::ExistingUnmounted {
            exact_identifier: exact_identifier.into(),
            marker: ExistingMarkerEvidence::Valid(marker),
        }
    }

    pub fn mounted_incomplete(exact_identifier: impl Into<String>) -> Self {
        Self::MountedIncomplete {
            exact_identifier: exact_identifier.into(),
        }
    }

    pub fn detached_incomplete(exact_identifier: impl Into<String>) -> Self {
        Self::DetachedIncomplete {
            exact_identifier: exact_identifier.into(),
        }
    }

    pub fn mis_mounted_incomplete(
        exact_identifier: impl Into<String>,
        current_mountpoint: impl Into<PathBuf>,
    ) -> Self {
        Self::MisMountedIncomplete {
            exact_identifier: exact_identifier.into(),
            current_mountpoint: current_mountpoint.into(),
        }
    }

    fn exact_identifier(&self) -> Option<&str> {
        match self {
            Self::Absent => None,
            Self::MountedValid { exact_identifier }
            | Self::MountedIncomplete { exact_identifier }
            | Self::DetachedIncomplete { exact_identifier }
            | Self::MisMountedIncomplete {
                exact_identifier, ..
            }
            | Self::ExistingUnmounted {
                exact_identifier, ..
            } => Some(exact_identifier),
            Self::FoundElsewhere { device, .. } => Some(device),
        }
    }
}

/// Evidence for only cowshed's fixed storage objects. Callers obtain this by exact APFS volume or
/// ZFS dataset identity; the type intentionally cannot represent a pool scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapEvidence {
    Apfs {
        store: ExistingStorage,
        caches: ExistingStorage,
    },
    Zfs {
        root: ExistingStorage,
        store: ExistingStorage,
        caches: ExistingStorage,
        /// Boxed so four ZFS snapshots do not dwarf the two-snapshot APFS variant.
        projects: Box<ExistingStorage>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApfsProvisionKind {
    Create,
    RepairMounted {
        exact_identifier: String,
    },
    RecoverDetached {
        exact_identifier: String,
    },
    RepairMisMounted {
        exact_identifier: String,
        current_mountpoint: PathBuf,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApfsVolumeProvision {
    name: &'static str,
    mountpoint: PathBuf,
    role: VolumeRole,
    kind: ApfsProvisionKind,
}

impl ApfsVolumeProvision {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    pub fn role(&self) -> VolumeRole {
        self.role
    }

    pub fn kind(&self) -> &ApfsProvisionKind {
        &self.kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostOperation {
    VerifyZfsDelegation {
        pool: String,
        required_root: String,
    },
    GuardMountpoint {
        path: PathBuf,
        role: VolumeRole,
        substrate: SubstrateKind,
    },
    EnsureDirectory(PathBuf),
    /// Delete a launchd StandardErrorPath stub so an existing cowshed volume can remount.
    ReclaimMountpoint(PathBuf),
    MountApfsVolume {
        mountpoint: PathBuf,
        volume: VolumeRef,
    },
    /// Provision or recover every incomplete cowshed APFS volume under one explicit
    /// Authorization Services session.
    ProvisionApfsVolumes {
        container: String,
        volumes: Vec<ApfsVolumeProvision>,
    },
    RunCommand(HostCommand),
    WriteMarkerAtomic {
        path: PathBuf,
        marker: VolumeMarker,
    },
    PinVolumesInFstab {
        pins: Vec<FstabPin>,
    },
    ReportVolumeIssue {
        name: &'static str,
        detail: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapPlan {
    substrate: SelectedSubstrate,
    home: PathBuf,
    roots: CanonicalRoots,
    operations: Vec<HostOperation>,
}

impl BootstrapPlan {
    pub fn substrate(&self) -> &SelectedSubstrate {
        &self.substrate
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn roots(&self) -> &CanonicalRoots {
        &self.roots
    }

    pub fn operations(&self) -> &[HostOperation] {
        &self.operations
    }

    pub fn push_operation(&mut self, operation: HostOperation) {
        self.operations.push(operation);
    }
}

/// Build the complete immutable host bootstrap plan without invoking a command or filesystem API.
pub fn plan_bootstrap(
    substrate: SelectedSubstrate,
    home: &Path,
    evidence: BootstrapEvidence,
) -> Result<BootstrapPlan, PlanError> {
    let roots = CanonicalRoots::global();
    if !home.is_absolute()
        || home
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(PlanError::NonCanonicalHome(home.to_owned()));
    }
    let operations = match (&substrate, evidence) {
        (SelectedSubstrate::Apfs { container, .. }, BootstrapEvidence::Apfs { store, caches }) => {
            plan_apfs(container, &roots, &store, &caches)?
        }
        (
            SelectedSubstrate::Zfs { pool, .. },
            BootstrapEvidence::Zfs {
                root,
                store,
                caches,
                projects,
            },
        ) => plan_zfs(pool, &roots, &root, &store, &caches, &projects)?,
        (SelectedSubstrate::Apfs { .. }, BootstrapEvidence::Zfs { .. }) => {
            return Err(PlanError::EvidenceSubstrateMismatch {
                selected: SubstrateKind::Apfs,
                evidence: SubstrateKind::Zfs,
            });
        }
        (SelectedSubstrate::Zfs { .. }, BootstrapEvidence::Apfs { .. }) => {
            return Err(PlanError::EvidenceSubstrateMismatch {
                selected: SubstrateKind::Zfs,
                evidence: SubstrateKind::Apfs,
            });
        }
    };
    Ok(BootstrapPlan {
        substrate,
        home: home.to_owned(),
        roots,
        operations,
    })
}

fn plan_apfs(
    container: &str,
    roots: &CanonicalRoots,
    store: &ExistingStorage,
    caches: &ExistingStorage,
) -> Result<Vec<HostOperation>, PlanError> {
    validate_existing_marker(store, VolumeRole::Store, SubstrateKind::Apfs)?;
    validate_existing_marker(caches, VolumeRole::Caches, SubstrateKind::Apfs)?;

    let mut operations = Vec::new();
    let mut volumes = Vec::new();

    if !mounted_but_unpublished_at(store, roots.store())
        && !remounting_from_elsewhere(store, roots.store())
    {
        operations.push(guard(roots.store(), VolumeRole::Store, SubstrateKind::Apfs));
    }
    plan_apfs_volume(
        &mut operations,
        &mut volumes,
        roots.store(),
        APFS_STORE_VOLUME,
        VolumeRole::Store,
        store,
    );

    // The caches volume is retired: never created, and an existing one is kept mounted only
    // until `setup --retire-caches-volume` deletes it (03_caches.md).
    if !matches!(caches, ExistingStorage::Absent) {
        let mountpoint = Path::new(RETIRED_CACHES_MOUNTPOINT);
        if !mounted_but_unpublished_at(caches, mountpoint)
            && !remounting_from_elsewhere(caches, mountpoint)
        {
            operations.push(guard(mountpoint, VolumeRole::Caches, SubstrateKind::Apfs));
        }
        plan_apfs_volume(
            &mut operations,
            &mut volumes,
            mountpoint,
            APFS_CACHES_VOLUME,
            VolumeRole::Caches,
            caches,
        );
    }

    if !volumes.is_empty() {
        operations.push(HostOperation::ProvisionApfsVolumes {
            container: container.to_owned(),
            volumes,
        });
    }
    Ok(operations)
}

fn mounted_but_unpublished_at(state: &ExistingStorage, path: &Path) -> bool {
    matches!(state, ExistingStorage::MountedIncomplete { .. })
        || matches!(
            state,
            ExistingStorage::MisMountedIncomplete {
                current_mountpoint,
                ..
            } if current_mountpoint == path
        )
}

fn remounting_from_elsewhere(state: &ExistingStorage, path: &Path) -> bool {
    matches!(
        state,
        ExistingStorage::MisMountedIncomplete {
            current_mountpoint,
            ..
        } if current_mountpoint != path
    )
}

fn plan_apfs_volume(
    operations: &mut Vec<HostOperation>,
    volumes: &mut Vec<ApfsVolumeProvision>,
    mountpoint: &Path,
    volume_name: &'static str,
    role: VolumeRole,
    state: &ExistingStorage,
) {
    match state {
        ExistingStorage::MountedValid { .. } => {}
        ExistingStorage::Absent => volumes.push(ApfsVolumeProvision {
            name: volume_name,
            mountpoint: mountpoint.to_owned(),
            role,
            kind: ApfsProvisionKind::Create,
        }),
        ExistingStorage::MountedIncomplete { exact_identifier } => {
            volumes.push(ApfsVolumeProvision {
                name: volume_name,
                mountpoint: mountpoint.to_owned(),
                role,
                kind: ApfsProvisionKind::RepairMounted {
                    exact_identifier: exact_identifier.clone(),
                },
            });
        }
        ExistingStorage::DetachedIncomplete { exact_identifier } => {
            volumes.push(ApfsVolumeProvision {
                name: volume_name,
                mountpoint: mountpoint.to_owned(),
                role,
                kind: ApfsProvisionKind::RecoverDetached {
                    exact_identifier: exact_identifier.clone(),
                },
            });
        }
        ExistingStorage::MisMountedIncomplete {
            exact_identifier,
            current_mountpoint,
        } if current_mountpoint == mountpoint => {
            volumes.push(ApfsVolumeProvision {
                name: volume_name,
                mountpoint: mountpoint.to_owned(),
                role,
                kind: ApfsProvisionKind::RepairMisMounted {
                    exact_identifier: exact_identifier.clone(),
                    current_mountpoint: current_mountpoint.clone(),
                },
            });
        }
        ExistingStorage::MisMountedIncomplete {
            exact_identifier,
            current_mountpoint,
        } => {
            operations.push(HostOperation::ReportVolumeIssue {
                name: volume_name,
                detail: format!(
                    "{volume_name} is mounted at {} instead of {}; cowshed setup will remount it and rewrite its /etc/fstab pin",
                    current_mountpoint.display(),
                    mountpoint.display()
                ),
            });
            operations.push(HostOperation::ReclaimMountpoint(mountpoint.to_owned()));
            operations.push(HostOperation::RunCommand(HostCommand::new(
                DISKUTIL,
                ["unmount", "force", exact_identifier.as_str()],
            )));
            operations.push(apfs_mount(
                mountpoint,
                VolumeRef::ExistingExact(exact_identifier.clone()),
            ));
            operations.push(guard(mountpoint, role, SubstrateKind::Apfs));
        }
        ExistingStorage::ExistingUnmounted {
            exact_identifier, ..
        } => {
            operations.push(HostOperation::EnsureDirectory(mountpoint.to_owned()));
            operations.push(apfs_mount(
                mountpoint,
                VolumeRef::ExistingExact(exact_identifier.clone()),
            ));
            operations.push(guard(mountpoint, role, SubstrateKind::Apfs));
        }
        ExistingStorage::FoundElsewhere {
            device, mounted_at, ..
        } => match mounted_at {
            Some(current) if current == mountpoint => {
                operations.push(guard(mountpoint, role, SubstrateKind::Apfs));
            }
            Some(_) => {
                operations.push(HostOperation::ReclaimMountpoint(mountpoint.to_owned()));
                operations.push(HostOperation::RunCommand(HostCommand::new(
                    DISKUTIL,
                    ["unmount", "force", device.as_str()],
                )));
                operations.push(apfs_mount(
                    mountpoint,
                    VolumeRef::ExistingExact(device.clone()),
                ));
                operations.push(guard(mountpoint, role, SubstrateKind::Apfs));
            }
            None => {
                operations.push(HostOperation::EnsureDirectory(mountpoint.to_owned()));
                operations.push(apfs_mount(
                    mountpoint,
                    VolumeRef::ExistingExact(device.clone()),
                ));
                operations.push(guard(mountpoint, role, SubstrateKind::Apfs));
            }
        },
    }
}

fn apfs_mount(mountpoint: &Path, volume: VolumeRef) -> HostOperation {
    HostOperation::MountApfsVolume {
        mountpoint: mountpoint.to_owned(),
        volume,
    }
}

fn plan_zfs(
    pool: &str,
    roots: &CanonicalRoots,
    root_state: &ExistingStorage,
    store_state: &ExistingStorage,
    caches_state: &ExistingStorage,
    projects_state: &ExistingStorage,
) -> Result<Vec<HostOperation>, PlanError> {
    let root = zfs_name(pool, ZFS_COWSHED_ROOT);
    let store = zfs_name(&root, ZFS_STORE_CHILD);
    let caches = zfs_name(&root, ZFS_CACHES_CHILD);
    let projects = zfs_name(&root, ZFS_PROJECTS_CHILD);
    validate_zfs_evidence(root_state, &root)?;
    validate_zfs_evidence(store_state, &store)?;
    validate_zfs_evidence(caches_state, &caches)?;
    validate_zfs_evidence(projects_state, &projects)?;
    validate_zfs_topology(root_state, store_state, caches_state, projects_state)?;
    validate_existing_marker(store_state, VolumeRole::Store, SubstrateKind::Zfs)?;
    validate_existing_marker(caches_state, VolumeRole::Caches, SubstrateKind::Zfs)?;
    validate_existing_marker(projects_state, VolumeRole::Projects, SubstrateKind::Zfs)?;

    let mut operations = vec![
        HostOperation::VerifyZfsDelegation {
            pool: pool.to_owned(),
            required_root: root.clone(),
        },
        guard(roots.store(), VolumeRole::Store, SubstrateKind::Zfs),
    ];
    if matches!(root_state, ExistingStorage::Absent) {
        operations.push(command(ZFS, ["create", "-o", "mountpoint=none", &root]));
    }
    plan_zfs_mounted_dataset(
        &mut operations,
        &store,
        roots.store(),
        VolumeRole::Store,
        store_state,
    );
    // The caches dataset is retired: never created, and an existing one is kept mounted so the
    // migration can move what it holds (03_caches.md, "Retiring the caches volume").
    if !matches!(caches_state, ExistingStorage::Absent) {
        let mountpoint = Path::new(RETIRED_CACHES_MOUNTPOINT);
        operations.push(guard(mountpoint, VolumeRole::Caches, SubstrateKind::Zfs));
        plan_zfs_mounted_dataset(
            &mut operations,
            &caches,
            mountpoint,
            VolumeRole::Caches,
            caches_state,
        );
    }
    if matches!(projects_state, ExistingStorage::Absent) {
        operations.push(command(ZFS, ["create", "-o", "mountpoint=none", &projects]));
        operations.push(zfs_marker(&projects, VolumeRole::Projects));
    }
    Ok(operations)
}

fn validate_zfs_topology(
    root: &ExistingStorage,
    store: &ExistingStorage,
    caches: &ExistingStorage,
    projects: &ExistingStorage,
) -> Result<(), PlanError> {
    if matches!(root, ExistingStorage::Absent)
        && [store, caches, projects]
            .into_iter()
            .any(|state| !matches!(state, ExistingStorage::Absent))
    {
        return Err(PlanError::ImpossibleStorageTopology(
            "ZFS root cannot be absent when a child dataset exists",
        ));
    }
    Ok(())
}

fn validate_existing_marker(
    state: &ExistingStorage,
    expected_role: VolumeRole,
    expected_substrate: SubstrateKind,
) -> Result<(), PlanError> {
    let ExistingStorage::ExistingUnmounted {
        exact_identifier,
        marker,
    } = state
    else {
        return Ok(());
    };
    if !matches!(
        marker,
        ExistingMarkerEvidence::Valid(actual)
            if actual.role() == expected_role && actual.substrate() == expected_substrate
    ) {
        return Err(PlanError::InvalidExistingStorageMarker {
            exact_identifier: exact_identifier.clone(),
            expected_role,
            expected_substrate,
        });
    }
    Ok(())
}

fn validate_zfs_evidence(state: &ExistingStorage, expected: &str) -> Result<(), PlanError> {
    if matches!(
        state,
        ExistingStorage::MountedIncomplete { .. }
            | ExistingStorage::DetachedIncomplete { .. }
            | ExistingStorage::MisMountedIncomplete { .. }
            | ExistingStorage::FoundElsewhere { .. }
    ) {
        return Err(PlanError::ImpossibleStorageTopology(
            "an APFS volume state cannot describe a ZFS dataset",
        ));
    }
    if let Some(actual) = state.exact_identifier()
        && actual != expected
    {
        return Err(PlanError::UnexpectedStorageIdentifier {
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        });
    }
    Ok(())
}

fn plan_zfs_mounted_dataset(
    operations: &mut Vec<HostOperation>,
    dataset: &str,
    mountpoint: &Path,
    role: VolumeRole,
    state: &ExistingStorage,
) {
    match state {
        ExistingStorage::MountedValid { .. } => {}
        ExistingStorage::Absent => {
            operations.push(HostOperation::EnsureDirectory(mountpoint.to_owned()));
            operations.push(command(
                ZFS,
                [
                    "create",
                    "-o",
                    &format!("mountpoint={}", mountpoint.display()),
                    dataset,
                ],
            ));
            operations.push(zfs_marker(dataset, role));
            operations.push(marker(
                mountpoint,
                VolumeMarker::new(role, SubstrateKind::Zfs),
            ));
        }
        ExistingStorage::ExistingUnmounted { .. } => {
            operations.push(HostOperation::EnsureDirectory(mountpoint.to_owned()));
            operations.push(command(
                ZFS,
                [
                    "set".to_owned(),
                    format!("mountpoint={}", mountpoint.display()),
                    dataset.to_owned(),
                ],
            ));
            operations.push(command(ZFS, ["mount", dataset]));
            operations.push(guard(mountpoint, role, SubstrateKind::Zfs));
        }
        ExistingStorage::MountedIncomplete { .. } => {
            unreachable!("incomplete APFS evidence was rejected for ZFS planning")
        }
        ExistingStorage::DetachedIncomplete { .. } => {
            unreachable!("incomplete APFS evidence was rejected for ZFS planning")
        }
        ExistingStorage::MisMountedIncomplete { .. } => {
            unreachable!("incomplete APFS evidence was rejected for ZFS planning")
        }
        ExistingStorage::FoundElsewhere { .. } => {
            unreachable!("cross-container APFS evidence was rejected for ZFS planning")
        }
    }
}

fn guard(path: &Path, role: VolumeRole, substrate: SubstrateKind) -> HostOperation {
    HostOperation::GuardMountpoint {
        path: path.to_owned(),
        role,
        substrate,
    }
}

fn marker(root: &Path, marker: VolumeMarker) -> HostOperation {
    HostOperation::WriteMarkerAtomic {
        path: root.join(VOLUME_MARKER_FILE),
        marker,
    }
}

fn command(
    program: &'static str,
    args: impl IntoIterator<Item = impl Into<String>>,
) -> HostOperation {
    HostOperation::RunCommand(HostCommand::new(program, args))
}

fn zfs_name(parent: &str, child: &str) -> String {
    format!("{parent}/{child}")
}

fn zfs_marker(dataset: &str, role: VolumeRole) -> HostOperation {
    command(
        ZFS,
        [
            "set".to_owned(),
            format!("org.cowshed:version={MARKER_VERSION}"),
            format!("org.cowshed:role={role}"),
            dataset.to_owned(),
        ],
    )
}

impl fmt::Display for VolumeRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Store => "store",
            Self::Caches => "caches",
            Self::Projects => "projects",
        })
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PlanError {
    #[error("home path must be absolute and normalized: {0:?}")]
    NonCanonicalHome(PathBuf),
    #[error("selected {selected:?} substrate cannot use {evidence:?} bootstrap evidence")]
    EvidenceSubstrateMismatch {
        selected: SubstrateKind,
        evidence: SubstrateKind,
    },
    #[error("storage evidence names {actual:?}, expected exact object {expected:?}")]
    UnexpectedStorageIdentifier { expected: String, actual: String },
    #[error(
        "existing storage {exact_identifier:?} has no valid {expected_substrate:?} {expected_role:?} marker"
    )]
    InvalidExistingStorageMarker {
        exact_identifier: String,
        expected_role: VolumeRole,
        expected_substrate: SubstrateKind,
    },
    #[error("impossible existing-storage topology: {0}")]
    ImpossibleStorageTopology(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountpointState {
    Missing,
    EmptyDirectory,
    /// Enumerated Data-volume residue that cowshed itself may create before the store volume is
    /// remounted. Every path is proven safe to remove; arbitrary entries remain fatal evidence.
    ReclaimableStub {
        paths: Vec<PathBuf>,
    },
    NonEmptyDirectoryWithoutMount,
    Mounted {
        marker: Option<Vec<u8>>,
    },
}

pub type HostCommandOutput = crate::process::CommandOutput;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostCommandFailure {
    command: HostCommand,
    output: HostCommandOutput,
}

impl HostCommandFailure {
    pub fn new(command: HostCommand, output: HostCommandOutput) -> Self {
        Self { command, output }
    }
}

impl fmt::Display for HostCommandFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_command_failure(
            f,
            "command",
            OsStr::new(self.command.program()),
            self.command.args(),
            &self.output,
        )
    }
}

/// Narrow synchronous host boundary. Implementations may block and therefore must only be called
/// by a [`BlockingLane`]. No planning function accepts this capability.
pub trait BootstrapHost: Send + Sync {
    fn verify_zfs_delegation(&self, pool: &str, required_root: &str) -> Result<(), HostError>;
    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError>;
    fn create_dir_all(&self, path: &Path) -> Result<(), HostError>;
    fn reclaim_mountpoint(&self, path: &Path) -> Result<(), HostError>;
    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError>;
    fn run_command_with_input(
        &self,
        command: &HostCommand,
        _input: &[u8],
    ) -> Result<HostCommandOutput, HostError> {
        Err(HostError::new(format!(
            "host cannot provide standard input to {:?}",
            command.program()
        )))
    }
    fn provision_apfs_volumes(
        &self,
        container: &str,
        volumes: &[ApfsVolumeProvision],
    ) -> Result<(), HostError>;
    fn write_file_atomic(&self, path: &Path, contents: &[u8]) -> Result<(), HostError>;
    fn pin_volumes_in_fstab(&self, pins: &[FstabPin]) -> Result<(), HostError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HostErrorKind {
    Other,
    AuthorizationDenied,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("host operation failed: {message}")]
pub struct HostError {
    kind: HostErrorKind,
    message: String,
}

impl HostError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            kind: HostErrorKind::Other,
            message: message.into(),
        }
    }

    pub fn authorization_denied(message: impl Into<String>) -> Self {
        Self {
            kind: HostErrorKind::AuthorizationDenied,
            message: message.into(),
        }
    }

    pub(crate) fn is_authorization_denied(&self) -> bool {
        self.kind == HostErrorKind::AuthorizationDenied
    }
}

pub type BlockingJob = Box<dyn FnOnce() -> Result<(), BootstrapExecutionError> + Send + 'static>;

#[async_trait]
pub trait BlockingLane: Send + Sync {
    async fn dispatch(&self, job: BlockingJob) -> Result<(), BootstrapExecutionError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TokioBlockingLane;

#[async_trait]
impl BlockingLane for TokioBlockingLane {
    async fn dispatch(&self, job: BlockingJob) -> Result<(), BootstrapExecutionError> {
        tokio::task::spawn_blocking(job)
            .await
            .map_err(|error| BootstrapExecutionError::BlockingLane(error.to_string()))?
    }
}

/// Execute each potentially blocking host interaction through the injected blocking lane.
pub async fn execute_bootstrap<H, L>(
    plan: &BootstrapPlan,
    host: Arc<H>,
    lane: &L,
) -> Result<(), BootstrapExecutionError>
where
    H: BootstrapHost + ?Sized + 'static,
    L: BlockingLane,
{
    for operation in plan.operations() {
        execute_bootstrap_operation(operation, Arc::clone(&host), lane).await?;
    }
    Ok(())
}

pub(crate) async fn execute_bootstrap_operation<H, L>(
    operation: &HostOperation,
    host: Arc<H>,
    lane: &L,
) -> Result<(), BootstrapExecutionError>
where
    H: BootstrapHost + ?Sized + 'static,
    L: BlockingLane,
{
    let operation = resolve_operation(operation)?;
    let (sender, receiver) = mpsc::sync_channel(1);
    lane.dispatch(Box::new(move || {
        apply_operation(host.as_ref(), &operation)?;
        sender.send(()).map_err(|_| {
            BootstrapExecutionError::BlockingLane(
                "bootstrap operation result receiver closed".to_owned(),
            )
        })
    }))
    .await?;
    receiver.try_recv().map_err(|error| {
        BootstrapExecutionError::BlockingLane(format!(
            "bootstrap operation produced no result: {error}"
        ))
    })?;
    Ok(())
}

fn resolve_operation(operation: &HostOperation) -> Result<HostOperation, BootstrapExecutionError> {
    let HostOperation::MountApfsVolume { mountpoint, volume } = operation else {
        return Ok(operation.clone());
    };
    let VolumeRef::ExistingExact(exact_identifier) = volume;
    let mountpoint = mountpoint.to_str().ok_or_else(|| {
        BootstrapExecutionError::Host(HostError::new(format!(
            "APFS mountpoint is not UTF-8: {mountpoint:?}"
        )))
    })?;
    Ok(HostOperation::RunCommand(HostCommand::new(
        DISKUTIL,
        [
            "mount",
            "-nobrowse",
            "-mountPoint",
            mountpoint,
            exact_identifier,
        ],
    )))
}

fn apply_operation<H>(host: &H, operation: &HostOperation) -> Result<(), BootstrapExecutionError>
where
    H: BootstrapHost + ?Sized,
{
    match operation {
        HostOperation::VerifyZfsDelegation {
            pool,
            required_root,
        } => host
            .verify_zfs_delegation(pool, required_root)
            .map_err(BootstrapExecutionError::Host),
        HostOperation::GuardMountpoint {
            path,
            role,
            substrate,
        } => match host
            .inspect_mountpoint(path)
            .map_err(BootstrapExecutionError::Host)?
        {
            MountpointState::Missing
            | MountpointState::EmptyDirectory
            | MountpointState::ReclaimableStub { .. } => Ok(()),
            MountpointState::NonEmptyDirectoryWithoutMount => {
                Err(BootstrapExecutionError::MaskedData(path.clone()))
            }
            MountpointState::Mounted { marker } => {
                require_mounted_marker(marker.as_deref(), *role, *substrate)
                    .map(|_| ())
                    .map_err(|source| BootstrapExecutionError::MountGuard {
                        path: path.clone(),
                        source,
                    })
            }
        },
        HostOperation::EnsureDirectory(path) => host
            .create_dir_all(path)
            .map_err(BootstrapExecutionError::Host),
        HostOperation::ReclaimMountpoint(path) => host
            .reclaim_mountpoint(path)
            .map_err(BootstrapExecutionError::Host),
        HostOperation::MountApfsVolume { .. } => {
            Err(BootstrapExecutionError::UnresolvedMountOperation)
        }
        HostOperation::RunCommand(command) => run_host_command(host, command).map(|_| ()),
        HostOperation::ProvisionApfsVolumes { container, volumes } => host
            .provision_apfs_volumes(container, volumes)
            .map_err(BootstrapExecutionError::Host),
        HostOperation::PinVolumesInFstab { pins } => host
            .pin_volumes_in_fstab(pins)
            .map_err(BootstrapExecutionError::Host),
        HostOperation::ReportVolumeIssue { .. } => Ok(()),
        HostOperation::WriteMarkerAtomic { path, marker } => {
            let contents = marker.to_json().map_err(BootstrapExecutionError::Marker)?;
            host.write_file_atomic(path, &contents)
                .map_err(BootstrapExecutionError::Host)
        }
    }
}

fn run_host_command<H>(
    host: &H,
    command: &HostCommand,
) -> Result<HostCommandOutput, BootstrapExecutionError>
where
    H: BootstrapHost + ?Sized,
{
    let output = host
        .run_command(command)
        .map_err(BootstrapExecutionError::Host)?;
    if output.succeeded() {
        Ok(output)
    } else {
        Err(BootstrapExecutionError::CommandFailed(
            HostCommandFailure::new(command.clone(), output),
        ))
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn parse_created_apfs_identifier(
    stdout: &[u8],
) -> Result<String, BootstrapExecutionError> {
    let stdout = std::str::from_utf8(stdout).map_err(|_| {
        BootstrapExecutionError::CreatedVolumeOutput(
            "diskutil addVolume output is not UTF-8".to_owned(),
        )
    })?;
    let prefix = "Created new APFS Volume ";
    let identifiers: Vec<_> = stdout
        .lines()
        .filter_map(|line| line.strip_prefix(prefix))
        .collect();
    if identifiers.len() != 1 {
        return Err(BootstrapExecutionError::CreatedVolumeOutput(format!(
            "expected one created DeviceIdentifier, found {}",
            identifiers.len()
        )));
    }
    let identifier = identifiers[0];
    // `device` owns the `diskN[sM…]` grammar, and the inventory parser this identifier is later
    // compared against uses it. Depth one is an APFS volume: `disk3` is a container and
    // `disk3s1s4` is a snapshot of a slice, neither of which addVolume returns.
    if crate::device::identifier_depth(identifier) != Some(1) {
        return Err(BootstrapExecutionError::CreatedVolumeOutput(format!(
            "malformed created DeviceIdentifier {identifier:?}"
        )));
    }
    Ok(identifier.to_owned())
}

#[cfg(target_os = "macos")]
/// Whether a newly created volume is still detached, or was mounted at the
/// default `/Volumes/<name>` by the system before the attestation ran.
///
/// The caller unmounts the latter before mounting the volume at its private
/// mountpoint, so provisioning converges to the same state on either host.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreatedMountState {
    Unmounted,
    AutoMounted,
}

#[cfg(target_os = "macos")]
pub(crate) fn attest_created_apfs_info(
    bytes: &[u8],
    expected_identifier: &str,
    expected_container: &str,
    expected_name: &str,
) -> Result<CreatedMountState, BootstrapExecutionError> {
    let value = Value::from_reader(std::io::Cursor::new(bytes))
        .map_err(|error| BootstrapExecutionError::CreatedVolumeAttestation(error.to_string()))?;
    let dictionary = value.as_dictionary().ok_or_else(|| {
        BootstrapExecutionError::CreatedVolumeAttestation(
            "diskutil info plist root is not a dictionary".to_owned(),
        )
    })?;
    for (key, expected) in [
        ("DeviceIdentifier", expected_identifier),
        ("APFSContainerReference", expected_container),
        ("VolumeName", expected_name),
        ("FilesystemType", "apfs"),
    ] {
        let actual = dictionary.get(key).and_then(Value::as_string);
        if actual != Some(expected) {
            return Err(BootstrapExecutionError::CreatedVolumeAttestation(format!(
                "{key} is {actual:?}, expected {expected:?}"
            )));
        }
    }
    let mount_state = match dictionary.get("MountPoint") {
        None => CreatedMountState::Unmounted,
        Some(Value::String(value)) if value.is_empty() => CreatedMountState::Unmounted,
        // Some macOS releases mount a freshly created APFS volume at its default
        // location regardless of `-nomount`; both cowshed volumes on a macOS 26
        // host are observed there immediately after `diskutil apfs addVolume`.
        // Only that exact default is tolerated — any other mountpoint means the
        // volume is not the pristine object this attestation is vouching for.
        Some(Value::String(value)) if *value == format!("/Volumes/{expected_name}") => {
            CreatedMountState::AutoMounted
        }
        Some(other) => {
            return Err(BootstrapExecutionError::CreatedVolumeAttestation(format!(
                "new APFS volume is mounted at an unexpected location: {other:?}"
            )));
        }
    };
    if !matches!(dictionary.get("APFSSnapshot"), Some(Value::Boolean(false))) {
        return Err(BootstrapExecutionError::CreatedVolumeAttestation(
            "new APFS object is not an ordinary volume".to_owned(),
        ));
    }
    for role_key in ["APFSVolumeRole", "APFSVolumeRoles", "Roles"] {
        match dictionary.get(role_key) {
            None => {}
            Some(Value::Array(values)) if values.is_empty() => {}
            Some(Value::String(value)) if value.is_empty() => {}
            Some(_) => {
                return Err(BootstrapExecutionError::CreatedVolumeAttestation(format!(
                    "new APFS volume unexpectedly has {role_key} metadata"
                )));
            }
        }
    }
    Ok(mount_state)
}

#[derive(Debug, Error)]
pub enum BootstrapExecutionError {
    #[error(transparent)]
    Host(HostError),
    #[error("blocking lane failed: {0}")]
    BlockingLane(String),
    #[error("refusing markerless non-empty mountpoint {0:?}")]
    MaskedData(PathBuf),
    #[error("mountpoint guard failed for {path:?}: {source}")]
    MountGuard {
        path: PathBuf,
        source: MountGuardError,
    },
    #[error("{0}")]
    CommandFailed(HostCommandFailure),
    #[error(transparent)]
    Marker(MarkerError),
    #[error("APFS mount operation reached execution without an exact DeviceIdentifier")]
    UnresolvedMountOperation,
    #[error("cannot identify the newly created APFS volume: {0}")]
    CreatedVolumeOutput(String),
    #[error("new APFS volume failed exact diskutil info attestation: {0}")]
    CreatedVolumeAttestation(String),
}

#[cfg(test)]
mod tests {
    use super::{VOLUME_MARKER_FILE, is_reserved_store_namespace};

    /// One grammar for every store-root walker. `cache` and `temp` are the near-misses that have
    /// to stay usable as repository owners, and the empty name is not a namespace at all.
    #[test]
    fn reserved_store_namespaces_share_one_complete_grammar() {
        for name in [
            "caches",
            "telemetry",
            "gateway",
            "mnt",
            "run",
            "tmp",
            "quarantine",
            VOLUME_MARKER_FILE,
            ".staging",
            ".trash",
        ] {
            assert!(is_reserved_store_namespace(name), "{name}");
        }
        for name in ["acme", "widget", "cache", "temp", ""] {
            assert!(!is_reserved_store_namespace(name), "{name}");
        }
    }

    /// `device::identifier_depth` is the one device grammar, and it rejects leading zeros because
    /// the kernel never prints them and these identifiers gate mount and detach decisions by
    /// textual comparison. A volume attested as `disk01s1` would be recorded under a spelling the
    /// inventory parser refuses, so provisioning could never match it to the device again.
    #[cfg(target_os = "macos")]
    #[test]
    fn created_volume_identifier_shares_the_device_grammar() {
        use super::parse_created_apfs_identifier;

        assert_eq!(
            parse_created_apfs_identifier(b"Created new APFS Volume disk3s5\n").unwrap(),
            "disk3s5"
        );
        for rejected in [
            &b"Created new APFS Volume disk01s1\n"[..],
            b"Created new APFS Volume disk3s1s4\n",
            b"Created new APFS Volume disk3\n",
            b"Created new APFS Volume disk3sx\n",
        ] {
            assert!(
                parse_created_apfs_identifier(rejected).is_err(),
                "{}",
                String::from_utf8_lossy(rejected)
            );
        }
    }
}
