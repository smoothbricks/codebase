use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const VOLUME_MARKER_FILE: &str = ".cowshed-volume.json";
pub const APFS_STORE_VOLUME: &str = "cowshed.store";
pub const APFS_CACHES_VOLUME: &str = "cowshed.caches";
pub const ZFS_COWSHED_ROOT: &str = "cowshed";
pub const ZFS_STORE_CHILD: &str = "store";
pub const ZFS_CACHES_CHILD: &str = "caches";
pub const ZFS_PROJECTS_CHILD: &str = "projects";

const DISKUTIL: &str = "/usr/sbin/diskutil";
const ZFS: &str = "/usr/sbin/zfs";
const MARKER_VERSION: u32 = 1;

/// The only substrate override accepted from `.cowshed.toml`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubstrateConfig {
    pool: String,
}

impl SubstrateConfig {
    pub fn pool(&self) -> &str {
        &self.pool
    }
}

/// Parse only `[substrate] kind = "zfs"` and `pool = "..."`.
///
/// Unrelated sections are deliberately ignored. Unknown keys, duplicate keys, and incomplete or
/// unsupported substrate sections are rejected rather than guessed around.
pub fn parse_substrate_config(input: &str) -> Result<Option<SubstrateConfig>, ConfigError> {
    let mut in_substrate = false;
    let mut saw_substrate = false;
    let mut kind = None;
    let mut pool = None;

    for (index, original) in input.lines().enumerate() {
        let line_number = index + 1;
        let line = strip_comment(original).trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('[') {
            let section = line
                .strip_prefix('[')
                .and_then(|value| value.strip_suffix(']'))
                .ok_or(ConfigError::MalformedLine { line: line_number })?
                .trim();
            in_substrate = section == "substrate";
            if in_substrate {
                if saw_substrate {
                    return Err(ConfigError::DuplicateSection);
                }
                saw_substrate = true;
            }
            continue;
        }
        if !in_substrate {
            continue;
        }

        let (key, value) = line
            .split_once('=')
            .ok_or(ConfigError::MalformedLine { line: line_number })?;
        let key = key.trim();
        let value = parse_toml_string(value.trim(), line_number)?;
        match key {
            "kind" => {
                if kind.replace(value).is_some() {
                    return Err(ConfigError::DuplicateKey("kind"));
                }
            }
            "pool" => {
                if pool.replace(value).is_some() {
                    return Err(ConfigError::DuplicateKey("pool"));
                }
            }
            other => return Err(ConfigError::UnknownKey(other.to_owned())),
        }
    }

    if !saw_substrate {
        return Ok(None);
    }
    let kind = kind.ok_or(ConfigError::MissingKey("kind"))?;
    if kind != "zfs" {
        return Err(ConfigError::UnsupportedKind(kind));
    }
    let pool = pool.ok_or(ConfigError::MissingKey("pool"))?;
    validate_pool_name(&pool).map_err(ConfigError::InvalidPool)?;
    Ok(Some(SubstrateConfig { pool }))
}

fn strip_comment(line: &str) -> &str {
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in line.bytes().enumerate() {
        match byte {
            b'\\' if quoted => escaped = !escaped,
            b'"' if !escaped => quoted = !quoted,
            b'#' if !quoted => return &line[..index],
            _ => escaped = false,
        }
    }
    line
}

fn parse_toml_string(value: &str, line: usize) -> Result<String, ConfigError> {
    if !value.starts_with('"') || !value.ends_with('"') {
        return Err(ConfigError::ExpectedQuotedString { line });
    }
    serde_json::from_str(value).map_err(|_| ConfigError::ExpectedQuotedString { line })
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ConfigError {
    #[error("malformed configuration at line {line}")]
    MalformedLine { line: usize },
    #[error("the [substrate] section is duplicated")]
    DuplicateSection,
    #[error("the [substrate] key {0:?} is duplicated")]
    DuplicateKey(&'static str),
    #[error("unknown [substrate] key {0:?}")]
    UnknownKey(String),
    #[error("missing [substrate] key {0:?}")]
    MissingKey(&'static str),
    #[error("unsupported substrate kind {0:?}; only explicit zfs is accepted")]
    UnsupportedKind(String),
    #[error("[substrate] value at line {line} must be a quoted string")]
    ExpectedQuotedString { line: usize },
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
        {
            if dataset.cowshed_root_available() {
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

    pub fn version(&self) -> u32 {
        self.version
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalRoots {
    home: PathBuf,
    store: PathBuf,
    caches: PathBuf,
}

impl CanonicalRoots {
    pub fn for_home(home: &Path) -> Result<Self, PlanError> {
        if !home.is_absolute()
            || home
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(PlanError::NonCanonicalHome(home.to_owned()));
        }
        let store = home.join(".cowshed");
        let caches = store.join("caches");
        Ok(Self {
            home: home.to_owned(),
            store,
            caches,
        })
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    pub fn store(&self) -> &Path {
        &self.store
    }

    pub fn caches(&self) -> &Path {
        &self.caches
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
    RunCommand(HostCommand),
    WriteMarkerAtomic {
        path: PathBuf,
        marker: VolumeMarker,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapPlan {
    substrate: SelectedSubstrate,
    roots: CanonicalRoots,
    operations: Vec<HostOperation>,
}

impl BootstrapPlan {
    pub fn substrate(&self) -> &SelectedSubstrate {
        &self.substrate
    }

    pub fn roots(&self) -> &CanonicalRoots {
        &self.roots
    }

    pub fn operations(&self) -> &[HostOperation] {
        &self.operations
    }
}

/// Build the complete immutable host bootstrap plan without invoking a command or filesystem API.
pub fn plan_bootstrap(
    substrate: SelectedSubstrate,
    home: &Path,
) -> Result<BootstrapPlan, PlanError> {
    let roots = CanonicalRoots::for_home(home)?;
    let operations = match &substrate {
        SelectedSubstrate::Apfs { container, .. } => plan_apfs(container, &roots),
        SelectedSubstrate::Zfs { pool, .. } => plan_zfs(pool, &roots),
    };
    Ok(BootstrapPlan {
        substrate,
        roots,
        operations,
    })
}

fn plan_apfs(container: &str, roots: &CanonicalRoots) -> Vec<HostOperation> {
    let store_marker = VolumeMarker::new(VolumeRole::Store, SubstrateKind::Apfs);
    let caches_marker = VolumeMarker::new(VolumeRole::Caches, SubstrateKind::Apfs);
    vec![
        guard(roots.store(), VolumeRole::Store, SubstrateKind::Apfs),
        HostOperation::EnsureDirectory(roots.store().to_owned()),
        command(
            DISKUTIL,
            [
                "apfs",
                "addVolume",
                container,
                "APFS",
                APFS_STORE_VOLUME,
                "-nomount",
            ],
        ),
        command(
            DISKUTIL,
            [
                "mount",
                "-nobrowse",
                "-mountPoint",
                &roots.store().to_string_lossy(),
                APFS_STORE_VOLUME,
            ],
        ),
        marker(roots.store(), store_marker),
        guard(roots.caches(), VolumeRole::Caches, SubstrateKind::Apfs),
        HostOperation::EnsureDirectory(roots.caches().to_owned()),
        command(
            DISKUTIL,
            [
                "apfs",
                "addVolume",
                container,
                "APFS",
                APFS_CACHES_VOLUME,
                "-nomount",
            ],
        ),
        command(
            DISKUTIL,
            [
                "mount",
                "-nobrowse",
                "-mountPoint",
                &roots.caches().to_string_lossy(),
                APFS_CACHES_VOLUME,
            ],
        ),
        marker(roots.caches(), caches_marker),
    ]
}

fn plan_zfs(pool: &str, roots: &CanonicalRoots) -> Vec<HostOperation> {
    let root = zfs_name(pool, ZFS_COWSHED_ROOT);
    let store = zfs_name(&root, ZFS_STORE_CHILD);
    let caches = zfs_name(&root, ZFS_CACHES_CHILD);
    let projects = zfs_name(&root, ZFS_PROJECTS_CHILD);
    vec![
        HostOperation::VerifyZfsDelegation {
            pool: pool.to_owned(),
            required_root: root.clone(),
        },
        command(ZFS, ["create", "-o", "mountpoint=none", &root]),
        guard(roots.store(), VolumeRole::Store, SubstrateKind::Zfs),
        HostOperation::EnsureDirectory(roots.store().to_owned()),
        command(
            ZFS,
            [
                "create",
                "-o",
                &format!("mountpoint={}", roots.store().display()),
                &store,
            ],
        ),
        zfs_marker(&store, VolumeRole::Store),
        marker(
            roots.store(),
            VolumeMarker::new(VolumeRole::Store, SubstrateKind::Zfs),
        ),
        guard(roots.caches(), VolumeRole::Caches, SubstrateKind::Zfs),
        HostOperation::EnsureDirectory(roots.caches().to_owned()),
        command(
            ZFS,
            [
                "create",
                "-o",
                &format!("mountpoint={}", roots.caches().display()),
                &caches,
            ],
        ),
        zfs_marker(&caches, VolumeRole::Caches),
        marker(
            roots.caches(),
            VolumeMarker::new(VolumeRole::Caches, SubstrateKind::Zfs),
        ),
        command(ZFS, ["create", "-o", "mountpoint=none", &projects]),
        zfs_marker(&projects, VolumeRole::Projects),
    ]
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountpointState {
    Missing,
    EmptyDirectory,
    NonEmptyDirectoryWithoutMount,
    Mounted { marker: Option<Vec<u8>> },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HostCommandOutput {
    pub success: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Narrow synchronous host boundary. Implementations may block and therefore must only be called
/// by a [`BlockingLane`]. No planning function accepts this capability.
pub trait BootstrapHost: Send + Sync {
    fn verify_zfs_delegation(&self, pool: &str, required_root: &str) -> Result<(), HostError>;
    fn inspect_mountpoint(&self, path: &Path) -> Result<MountpointState, HostError>;
    fn create_dir_all(&self, path: &Path) -> Result<(), HostError>;
    fn run_command(&self, command: &HostCommand) -> Result<HostCommandOutput, HostError>;
    fn write_file_atomic(&self, path: &Path, contents: &[u8]) -> Result<(), HostError>;
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("host operation failed: {message}")]
pub struct HostError {
    message: String,
}

impl HostError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
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
    H: BootstrapHost + 'static,
    L: BlockingLane,
{
    for operation in plan.operations().iter().cloned() {
        let host = Arc::clone(&host);
        lane.dispatch(Box::new(move || apply_operation(host.as_ref(), &operation)))
            .await?;
    }
    Ok(())
}

fn apply_operation(
    host: &dyn BootstrapHost,
    operation: &HostOperation,
) -> Result<(), BootstrapExecutionError> {
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
            MountpointState::Missing | MountpointState::EmptyDirectory => Ok(()),
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
        HostOperation::RunCommand(command) => {
            let output = host
                .run_command(command)
                .map_err(BootstrapExecutionError::Host)?;
            if output.success {
                Ok(())
            } else {
                Err(BootstrapExecutionError::CommandFailed {
                    command: command.clone(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                })
            }
        }
        HostOperation::WriteMarkerAtomic { path, marker } => {
            let contents = marker.to_json().map_err(BootstrapExecutionError::Marker)?;
            host.write_file_atomic(path, &contents)
                .map_err(BootstrapExecutionError::Host)
        }
    }
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
    #[error("command {command:?} failed: {stderr}")]
    CommandFailed {
        command: HostCommand,
        stderr: String,
    },
    #[error(transparent)]
    Marker(MarkerError),
}
