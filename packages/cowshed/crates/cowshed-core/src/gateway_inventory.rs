use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::apfs::{ApfsCaseSensitivity, SystemCommandRunner};
use crate::metadata::{
    DetachedWorkspaceMetadata, GrantSet, ImageFormat, PortBlock, PublicationState,
    WorkspaceIncarnation, WorkspaceName, sidecar_path,
};
use crate::repository::{ProjectPaths, RepoId, RepositoryBinding};
use crate::storage::apfs::native::{
    KernelMountSnapshot, KernelMountSource, MacOsApfsExecutionHost, SystemKernelMountSource,
};
use crate::storage::apfs::{ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig};
use crate::storage::bootstrap::ValidatedHostStorage;
use crate::storage::lifecycle::{
    DerivationError, KernelMountFact, MountState, StorageFact, derive_workspaces,
};
use crate::storage::{StorageLayout, verify_no_symlinks};
use crate::workspace_credentials::{
    GatewayWorkspaceCredentials, WorkspaceCredentialError, read_gateway_workspace_credentials,
};

const MAX_BINDING_BYTES: u64 = 1024 * 1024;
const UNRESOLVED_MAIN_MOUNT: &str = ".unresolved-main-mount";

/// Complete controller-authoritative input for installing one gateway workspace session.
pub struct GatewaySessionFact {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub incarnation: WorkspaceIncarnation,
    pub revision: u64,
    pub mount_id: u64,
    pub mount: PathBuf,
    pub grants: GrantSet,
    pub port_block: PortBlock,
    pub credentials: GatewayWorkspaceCredentials,
}

impl fmt::Debug for GatewaySessionFact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewaySessionFact")
            .field("repo_id", &self.repo_id)
            .field("workspace", &self.workspace)
            .field("incarnation", &self.incarnation)
            .field("revision", &self.revision)
            .field("mount_id", &self.mount_id)
            .field("mount", &self.mount)
            .field("grants", &self.grants)
            .field("port_block", &self.port_block)
            .field("credentials", &self.credentials)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum GatewayInventoryError {
    #[error("gateway inventory I/O failed while {operation} at {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid repository binding at {path}: {message}")]
    InvalidBinding { path: PathBuf, message: String },
    #[error("repository binding at {path} names {actual}, not canonical path identity {expected}")]
    ForeignBinding {
        path: PathBuf,
        expected: RepoId,
        actual: RepoId,
    },
    #[error("repository identity {0} occurs more than once in the store hierarchy")]
    DuplicateRepository(RepoId),
    #[error("macOS port block base {0} is assigned to more than one workspace")]
    DuplicatePortBlock(u16),
    #[error("gateway inventory has duplicate or ambiguous mount fact for {0}")]
    AmbiguousMount(String),
    #[error("gateway inventory metadata is invalid at {path}: {message}")]
    InvalidMetadata { path: PathBuf, message: String },
    #[error("attached workspace {repo}/{workspace} has no canonical macOS port block")]
    MissingPortBlock {
        repo: RepoId,
        workspace: WorkspaceName,
    },
    #[error(transparent)]
    Apfs(#[from] ApfsStorageError),
    #[error(transparent)]
    Derivation(#[from] DerivationError),
    #[error(transparent)]
    Credentials(#[from] WorkspaceCredentialError),
    #[error("gateway inventory blocking task failed: {0}")]
    Blocking(String),
}

#[derive(Clone)]
struct ProjectInventoryFacts {
    storage: Vec<StorageFact>,
    mounts: Vec<KernelMountFact>,
    mount_paths: BTreeMap<String, PathBuf>,
}

trait InventorySource: Send + Sync {
    fn project_facts(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<ProjectInventoryFacts, GatewayInventoryError>;
}

#[derive(Clone, Copy, Debug, Default)]
struct NativeInventorySource;

#[derive(Clone)]
struct CapturedKernelMountSource {
    mounts: Vec<KernelMountSnapshot>,
}

impl KernelMountSource for CapturedKernelMountSource {
    fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
        Ok(self.mounts.clone())
    }
}

impl InventorySource for NativeInventorySource {
    fn project_facts(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<ProjectInventoryFacts, GatewayInventoryError> {
        let layout = StorageLayout::new(storage.store(), repo).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: storage.store().to_owned(),
                message: error.to_string(),
            }
        })?;
        let main_mount = authoritative_main_mount(&layout, repo)?
            .unwrap_or_else(|| storage.store().join("gateway").join(UNRESOLVED_MAIN_MOUNT));
        let config = ApfsSubstrateConfig::new(
            storage.store(),
            storage.caches(),
            main_mount,
            ApfsCaseSensitivity::Sensitive,
        );
        let captured = SystemKernelMountSource.mounts()?;
        let host = MacOsApfsExecutionHost::with_mount_source(
            SystemCommandRunner,
            config,
            CapturedKernelMountSource {
                mounts: captured.clone(),
            },
        )?;
        let storage_facts = host.list(repo)?;
        let mount_paths = expected_mount_paths(&layout, &storage_facts)?;
        reject_ambiguous_native_mounts(&captured, &mount_paths)?;
        let mounts = host.mounts(repo)?;
        Ok(ProjectInventoryFacts {
            storage: storage_facts,
            mounts,
            mount_paths,
        })
    }
}

/// Read-only native inventory rooted in an already existing-only validated host store.
#[derive(Clone)]
pub struct NativeGatewayInventory {
    storage: ValidatedHostStorage,
    source: Arc<dyn InventorySource>,
}

impl fmt::Debug for NativeGatewayInventory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeGatewayInventory")
            .field("storage", &self.storage)
            .finish_non_exhaustive()
    }
}

impl NativeGatewayInventory {
    pub fn new(storage: ValidatedHostStorage) -> Self {
        Self {
            storage,
            source: Arc::new(NativeInventorySource),
        }
    }

    #[cfg(test)]
    fn with_source(storage: ValidatedHostStorage, source: Arc<dyn InventorySource>) -> Self {
        Self { storage, source }
    }

    pub async fn all_attached(&self) -> Result<Vec<GatewaySessionFact>, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || inventory.all_attached_blocking())
            .await
            .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    pub async fn project_attached(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<GatewaySessionFact>, GatewayInventoryError> {
        let inventory = self.clone();
        let repo_id = repo_id.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            inventory.project_attached_blocking(&repo_id)
        })
        .await
        .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }
    pub async fn all_reserved_port_bases(&self) -> Result<BTreeSet<u16>, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            inventory.all_reserved_port_bases_blocking()
        })
        .await
        .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    fn all_reserved_port_bases_blocking(&self) -> Result<BTreeSet<u16>, GatewayInventoryError> {
        let repositories = discover_repositories(self.storage.store())?;
        let mut bases = BTreeSet::new();
        for repo in repositories {
            let authoritative = self.source.project_facts(&self.storage, &repo)?;
            let layout = StorageLayout::new(self.storage.store(), &repo).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: self.storage.store().to_owned(),
                    message: error.to_string(),
                }
            })?;
            for fact in authoritative.storage {
                let image = canonical_image_paths(&layout, &fact.workspace)?;
                let metadata =
                    read_current_metadata(self.storage.store(), image.image(), &fact.workspace)?;
                let base = metadata
                    .grants
                    .port_block
                    .ok_or_else(|| GatewayInventoryError::MissingPortBlock {
                        repo: repo.clone(),
                        workspace: fact.workspace.name().clone(),
                    })?
                    .base();
                if !bases.insert(base) {
                    return Err(GatewayInventoryError::DuplicatePortBlock(base));
                }
            }
        }
        Ok(bases)
    }

    fn all_attached_blocking(&self) -> Result<Vec<GatewaySessionFact>, GatewayInventoryError> {
        let repositories = discover_repositories(self.storage.store())?;
        let mut facts = Vec::new();
        for repo in repositories {
            facts.extend(self.load_project(&repo)?);
        }
        facts.sort_by(|left, right| {
            (&left.repo_id, &left.workspace).cmp(&(&right.repo_id, &right.workspace))
        });
        Ok(facts)
    }

    fn project_attached_blocking(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<GatewaySessionFact>, GatewayInventoryError> {
        if !validate_requested_repository(self.storage.store(), repo_id)? {
            return Ok(Vec::new());
        }
        let mut facts = self.load_project(repo_id)?;
        facts.sort_by(|left, right| left.workspace.cmp(&right.workspace));
        Ok(facts)
    }

    fn load_project(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<GatewaySessionFact>, GatewayInventoryError> {
        let authoritative = self.source.project_facts(&self.storage, repo_id)?;
        reject_duplicate_mount_facts(&authoritative.mounts)?;
        let derived = derive_workspaces(authoritative.storage, authoritative.mounts, [])?;
        let layout = StorageLayout::new(self.storage.store(), repo_id).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: self.storage.store().to_owned(),
                message: error.to_string(),
            }
        })?;
        let mut facts = Vec::new();
        for workspace in derived {
            let MountState::Mounted { mount_id } = workspace.mount_state else {
                continue;
            };
            let volume = crate::storage::apfs::volume_name(repo_id, workspace.workspace.name());
            let mount = authoritative.mount_paths.get(&volume).ok_or_else(|| {
                GatewayInventoryError::InvalidMetadata {
                    path: layout.project().project_root.clone(),
                    message: format!("missing canonical mount path for {volume}"),
                }
            })?;
            let image_paths = canonical_image_paths(&layout, &workspace.workspace)?;
            let metadata = read_current_metadata(
                self.storage.store(),
                image_paths.image(),
                &workspace.workspace,
            )?;
            let expected_mount = if workspace.workspace.name().is_main() {
                let Ok(snapshot) = metadata.require_info_snapshot() else {
                    continue;
                };
                snapshot.project_root.clone()
            } else {
                layout
                    .workspace_mount(workspace.workspace.name())
                    .map_err(|error| GatewayInventoryError::InvalidMetadata {
                        path: layout.project().mount_root.clone(),
                        message: error.to_string(),
                    })?
            };
            if *mount != expected_mount {
                return Err(GatewayInventoryError::InvalidMetadata {
                    path: mount.clone(),
                    message: format!(
                        "mounted workspace path does not equal canonical path {}",
                        expected_mount.display()
                    ),
                });
            }
            let port_block = metadata.grants.port_block.ok_or_else(|| {
                GatewayInventoryError::MissingPortBlock {
                    repo: repo_id.clone(),
                    workspace: workspace.workspace.name().clone(),
                }
            })?;
            let credentials = read_gateway_workspace_credentials(
                &workspace.workspace,
                mount,
                image_paths.ca_private_key(),
            )?;
            facts.push(GatewaySessionFact {
                repo_id: repo_id.clone(),
                workspace: workspace.workspace.name().clone(),
                incarnation: workspace.workspace.incarnation().clone(),
                revision: metadata.grants.revision,
                mount_id,
                mount: mount.clone(),
                grants: metadata.grants,
                port_block,
                credentials,
            });
        }
        Ok(facts)
    }
}

fn discover_repositories(store_root: &Path) -> Result<Vec<RepoId>, GatewayInventoryError> {
    ensure_directory(store_root, "opening validated store root")?;
    let mut repositories = BTreeSet::new();
    for owner in read_directory(store_root, "enumerating store owners")? {
        let Some(owner_name) = owner.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if owner_name.starts_with('.') || !is_directory(&owner)? {
            continue;
        }
        for project in read_directory(&owner, "enumerating owner repositories")? {
            if !is_directory(&project)? {
                continue;
            }
            let binding_path = project.join("repository.json");
            if !binding_path_exists(&binding_path)? {
                continue;
            }
            let repo = load_binding_candidate(store_root, &project, &binding_path)?;
            if !repositories.insert(repo.clone()) {
                return Err(GatewayInventoryError::DuplicateRepository(repo));
            }
        }
    }
    Ok(repositories.into_iter().collect())
}

fn validate_requested_repository(
    store_root: &Path,
    repo_id: &RepoId,
) -> Result<bool, GatewayInventoryError> {
    let paths = ProjectPaths::new(store_root, repo_id).map_err(|error| {
        GatewayInventoryError::InvalidBinding {
            path: store_root.to_owned(),
            message: error.to_string(),
        }
    })?;
    let binding_path = paths.repository_binding.clone();
    if !binding_path_exists(&binding_path)? {
        return Ok(false);
    }
    ensure_directory(&paths.project_root, "opening repository directory")?;
    verify_no_symlinks(store_root, &paths.project_root).map_err(|error| {
        GatewayInventoryError::InvalidBinding {
            path: paths.project_root.clone(),
            message: error.to_string(),
        }
    })?;
    let found = load_binding_candidate(store_root, &paths.project_root, &binding_path)?;
    if found != *repo_id {
        return Err(GatewayInventoryError::ForeignBinding {
            path: binding_path,
            expected: repo_id.clone(),
            actual: found,
        });
    }
    Ok(true)
}

fn load_binding_candidate(
    store_root: &Path,
    project_root: &Path,
    binding_path: &Path,
) -> Result<RepoId, GatewayInventoryError> {
    verify_no_symlinks(store_root, project_root).map_err(|error| {
        GatewayInventoryError::InvalidBinding {
            path: project_root.to_owned(),
            message: error.to_string(),
        }
    })?;
    let binding: RepositoryBinding = read_typed_json_nofollow(binding_path, MAX_BINDING_BYTES)
        .map_err(|message| GatewayInventoryError::InvalidBinding {
            path: binding_path.to_owned(),
            message,
        })?;
    binding
        .validate()
        .map_err(|error| GatewayInventoryError::InvalidBinding {
            path: binding_path.to_owned(),
            message: error.to_string(),
        })?;
    let actual = binding
        .primary()
        .map_err(|error| GatewayInventoryError::InvalidBinding {
            path: binding_path.to_owned(),
            message: error.to_string(),
        })?
        .repo_id
        .clone();
    let expected_paths = ProjectPaths::new(store_root, &actual).map_err(|error| {
        GatewayInventoryError::InvalidBinding {
            path: binding_path.to_owned(),
            message: error.to_string(),
        }
    })?;
    if expected_paths.project_root != project_root {
        let expected = project_root_identity(project_root).unwrap_or_else(|| actual.clone());
        return Err(GatewayInventoryError::ForeignBinding {
            path: binding_path.to_owned(),
            expected,
            actual,
        });
    }
    Ok(actual)
}

fn project_root_identity(project_root: &Path) -> Option<RepoId> {
    let repo = project_root.file_name()?.to_str()?;
    let owner = project_root.parent()?.file_name()?.to_str()?;
    RepoId::parse(&format!("{owner}/{repo}")).ok()
}

fn binding_path_exists(path: &Path) -> Result<bool, GatewayInventoryError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(GatewayInventoryError::InvalidBinding {
            path: path.to_owned(),
            message: "repository binding is not a regular file".to_owned(),
        }),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(io_error("inspecting repository binding", path, source)),
    }
}

fn read_typed_json_nofollow<T: serde::de::DeserializeOwned>(
    path: &Path,
    maximum: u64,
) -> Result<T, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path).map_err(|error| error.to_string())?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.file_type().is_file() || metadata.len() > maximum {
        return Err("typed JSON file is not regular or exceeds its size bound".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err("typed JSON file is not controller-owned mode 0600".to_owned());
        }
    }
    let capacity = usize::try_from(metadata.len()).map_err(|error| error.to_string())?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > maximum {
        return Err("typed JSON file exceeds its size bound".to_owned());
    }
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

fn authoritative_main_mount(
    layout: &StorageLayout,
    repo: &RepoId,
) -> Result<Option<PathBuf>, GatewayInventoryError> {
    let mut found = None;
    for format in [ImageFormat::Asif, ImageFormat::Sparse] {
        let paths =
            layout
                .main_image(format)
                .map_err(|error| GatewayInventoryError::InvalidMetadata {
                    path: layout.project().project_root.clone(),
                    message: error.to_string(),
                })?;
        if !paths
            .image()
            .try_exists()
            .map_err(|source| io_error("inspecting canonical main image", paths.image(), source))?
        {
            continue;
        }
        if found.is_some() {
            return Err(GatewayInventoryError::InvalidMetadata {
                path: layout.project().project_root.clone(),
                message: "duplicate canonical main image formats".to_owned(),
            });
        }
        let metadata =
            DetachedWorkspaceMetadata::read_for_image(paths.image()).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: sidecar_path(paths.image()),
                    message: error.to_string(),
                }
            })?;
        if metadata.repo_id != *repo || !metadata.workspace.is_main() {
            return Err(GatewayInventoryError::InvalidMetadata {
                path: sidecar_path(paths.image()),
                message: "canonical main metadata identity mismatch".to_owned(),
            });
        }
        if metadata.publication_state == PublicationState::Active {
            found = Some(
                metadata
                    .require_info_snapshot()
                    .ok()
                    .map(|snapshot| snapshot.project_root.clone()),
            );
        }
    }
    Ok(found.flatten())
}

fn expected_mount_paths(
    layout: &StorageLayout,
    storage: &[StorageFact],
) -> Result<BTreeMap<String, PathBuf>, GatewayInventoryError> {
    let mut paths = BTreeMap::new();
    for fact in storage {
        let mount = if fact.workspace.name().is_main() {
            let image = canonical_image_paths(layout, &fact.workspace)?;
            let metadata =
                DetachedWorkspaceMetadata::read_for_image(image.image()).map_err(|error| {
                    GatewayInventoryError::InvalidMetadata {
                        path: sidecar_path(image.image()),
                        message: error.to_string(),
                    }
                })?;
            let Ok(snapshot) = metadata.require_info_snapshot() else {
                continue;
            };
            snapshot.project_root.clone()
        } else {
            layout
                .workspace_mount(fact.workspace.name())
                .map_err(|error| GatewayInventoryError::InvalidMetadata {
                    path: layout.project().mount_root.clone(),
                    message: error.to_string(),
                })?
        };
        paths.insert(fact.volume_name.clone(), mount);
    }
    Ok(paths)
}

fn reject_ambiguous_native_mounts(
    mounts: &[KernelMountSnapshot],
    expected: &BTreeMap<String, PathBuf>,
) -> Result<(), GatewayInventoryError> {
    let expected_paths = expected.values().collect::<BTreeSet<_>>();
    let mut sources_at_canonical_path = BTreeSet::new();
    for mount in mounts {
        if expected_paths.contains(&mount.mount_point) {
            if !sources_at_canonical_path.insert(mount.source_device.as_str()) {
                return Err(GatewayInventoryError::AmbiguousMount(
                    mount.mount_point.display().to_string(),
                ));
            }
            let source_count = mounts
                .iter()
                .filter(|candidate| candidate.source_device == mount.source_device)
                .count();
            if source_count != 1 {
                return Err(GatewayInventoryError::AmbiguousMount(
                    mount.source_device.clone(),
                ));
            }
        }
    }
    for path in expected_paths {
        if mounts
            .iter()
            .filter(|mount| &mount.mount_point == path)
            .count()
            > 1
        {
            return Err(GatewayInventoryError::AmbiguousMount(
                path.display().to_string(),
            ));
        }
    }
    Ok(())
}

fn reject_duplicate_mount_facts(mounts: &[KernelMountFact]) -> Result<(), GatewayInventoryError> {
    let mut volumes = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for mount in mounts {
        if !volumes.insert(mount.volume_name.as_str()) || !ids.insert(mount.mount_id) {
            return Err(GatewayInventoryError::AmbiguousMount(
                mount.volume_name.clone(),
            ));
        }
    }
    Ok(())
}

fn canonical_image_paths(
    layout: &StorageLayout,
    workspace: &crate::storage::lifecycle::LifecycleWorkspace,
) -> Result<crate::storage::ImagePaths, GatewayInventoryError> {
    let result = if workspace.name().is_main() {
        layout.main_image(workspace.format())
    } else {
        layout.session_image(workspace.name(), workspace.format())
    };
    result.map_err(|error| GatewayInventoryError::InvalidMetadata {
        path: layout.project().project_root.clone(),
        message: error.to_string(),
    })
}

fn read_current_metadata(
    store_root: &Path,
    image: &Path,
    workspace: &crate::storage::lifecycle::LifecycleWorkspace,
) -> Result<DetachedWorkspaceMetadata, GatewayInventoryError> {
    verify_no_symlinks(store_root, image).map_err(|error| {
        GatewayInventoryError::InvalidMetadata {
            path: image.to_owned(),
            message: error.to_string(),
        }
    })?;
    verify_no_symlinks(store_root, &sidecar_path(image)).map_err(|error| {
        GatewayInventoryError::InvalidMetadata {
            path: sidecar_path(image),
            message: error.to_string(),
        }
    })?;
    let metadata = DetachedWorkspaceMetadata::read_for_image(image).map_err(|error| {
        GatewayInventoryError::InvalidMetadata {
            path: sidecar_path(image),
            message: error.to_string(),
        }
    })?;
    if metadata.publication_state != PublicationState::Active
        || metadata.repo_id != *workspace.repo()
        || metadata.workspace != *workspace.name()
        || metadata.workspace_incarnation != *workspace.incarnation()
        || metadata.image_format != workspace.format()
        || metadata.grants.revision != workspace.revision().get()
    {
        return Err(GatewayInventoryError::InvalidMetadata {
            path: sidecar_path(image),
            message: "metadata does not match the exact current workspace incarnation".to_owned(),
        });
    }
    Ok(metadata)
}

fn ensure_directory(path: &Path, operation: &'static str) -> Result<(), GatewayInventoryError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| io_error(operation, path, source))?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(GatewayInventoryError::InvalidBinding {
            path: path.to_owned(),
            message: "path is not a no-follow directory".to_owned(),
        })
    }
}

fn is_directory(path: &Path) -> Result<bool, GatewayInventoryError> {
    fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_dir())
        .map_err(|source| io_error("inspecting inventory directory", path, source))
}

fn read_directory(
    path: &Path,
    operation: &'static str,
) -> Result<Vec<PathBuf>, GatewayInventoryError> {
    let mut children = fs::read_dir(path)
        .map_err(|source| io_error(operation, path, source))?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|source| io_error(operation, path, source))
        })
        .collect::<Result<Vec<_>, _>>()?;
    children.sort();
    Ok(children)
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> GatewayInventoryError {
    GatewayInventoryError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::metadata::{
        METADATA_VERSION, Platform, WorkspaceInfoSnapshot, WorkspaceRole, write_json,
    };
    use crate::repository::{BoundIdentity, RepositoryBinding};
    use crate::storage::bootstrap::CanonicalRoots;
    use crate::storage::lifecycle::{LifecycleWorkspace, Revision};
    use crate::workspace_credentials::mint_workspace_credentials;

    use super::*;

    #[derive(Default)]
    struct FixtureSource {
        projects: Mutex<BTreeMap<RepoId, ProjectInventoryFacts>>,
    }

    impl InventorySource for FixtureSource {
        fn project_facts(
            &self,
            _storage: &ValidatedHostStorage,
            repo: &RepoId,
        ) -> Result<ProjectInventoryFacts, GatewayInventoryError> {
            self.projects
                .lock()
                .expect("fixture source")
                .get(repo)
                .cloned()
                .ok_or_else(|| GatewayInventoryError::InvalidMetadata {
                    path: PathBuf::from("/missing-fixture"),
                    message: format!("missing fixture for {repo}"),
                })
        }
    }

    struct Fixture {
        root: PathBuf,
        storage: ValidatedHostStorage,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "cowshed-gateway-inventory-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            let home = root.join("home");
            fs::create_dir_all(&home).expect("fixture home");
            let roots = CanonicalRoots::for_home(&home).expect("canonical roots");
            fs::create_dir_all(roots.store()).expect("fixture store");
            fs::create_dir_all(roots.caches()).expect("fixture caches");
            fs::create_dir_all(roots.telemetry()).expect("fixture telemetry");
            Self {
                root,
                storage: ValidatedHostStorage::new(roots),
            }
        }

        fn bind(&self, repo: &RepoId) {
            let paths = ProjectPaths::new(self.storage.store(), repo).expect("project paths");
            fs::create_dir_all(&paths.project_root).expect("project root");
            let binding = RepositoryBinding::new(vec![BoundIdentity {
                repo_id: repo.clone(),
                remote_name: None,
                remote_url: None,
                primary: true,
            }])
            .expect("binding");
            write_json(&paths.repository_binding, &binding).expect("binding file");
        }

        fn workspace(
            &self,
            repo: &RepoId,
            name: WorkspaceName,
            incarnation: &str,
            revision: u64,
            mounted: bool,
            persist_main_root: bool,
        ) -> (StorageFact, Option<(KernelMountFact, PathBuf)>) {
            let layout = StorageLayout::new(self.storage.store(), repo).expect("layout");
            let role = if name.is_main() {
                WorkspaceRole::Main
            } else {
                WorkspaceRole::Workspace
            };
            let workspace = LifecycleWorkspace::new(
                repo.clone(),
                name.clone(),
                WorkspaceIncarnation::new(incarnation).expect("incarnation"),
                Revision::new(revision),
                Revision::new(revision),
                role,
                ImageFormat::Sparse,
            )
            .expect("workspace");
            let image = canonical_image_paths(&layout, &workspace).expect("image paths");
            fs::create_dir_all(image.image().parent().expect("image parent"))
                .expect("image parent");
            fs::write(image.image(), b"fixture").expect("image");
            let mount = if name.is_main() {
                self.root.join(format!("checkout-{}", repo.repo()))
            } else {
                layout.workspace_mount(&name).expect("session mount")
            };
            let mut grants =
                GrantSet::closed_baseline(Some(PortBlock::new(40_960, 16).expect("port block")))
                    .expect("grants");
            grants.revision = revision;
            let info_snapshot =
                (!name.is_main() || persist_main_root).then(|| WorkspaceInfoSnapshot {
                    project_root: mount.clone(),
                    role,
                    base_commit: "0123456789abcdef".to_owned(),
                    branch: Some("main".to_owned()),
                    created_at: "2026-07-14T00:00:00Z".to_owned(),
                    forked_from: None,
                    captured_at: "2026-07-14T00:00:00Z".to_owned(),
                    stale: false,
                });
            DetachedWorkspaceMetadata {
                version: METADATA_VERSION,
                repo_id: repo.clone(),
                workspace: name.clone(),
                workspace_incarnation: workspace.incarnation().clone(),
                image_format: ImageFormat::Sparse,
                platform: Platform::Macos,
                publication_state: PublicationState::Active,
                updated_at: "2026-07-14T00:00:00Z".to_owned(),
                grants,
                info_snapshot,
            }
            .write_for_image(image.image())
            .expect("metadata");
            let volume_name = crate::storage::apfs::volume_name(repo, &name);
            let storage = StorageFact {
                workspace: workspace.clone(),
                volume_name: volume_name.clone(),
            };
            let mounted = mounted.then(|| {
                fs::create_dir_all(&mount).expect("mount");
                mint_workspace_credentials(&workspace, &mount, image.ca_private_key())
                    .expect("credentials");
                (
                    KernelMountFact {
                        mount_id: revision + 100,
                        volume_name,
                    },
                    mount,
                )
            });
            (storage, mounted)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[tokio::test]
    async fn attached_inventory_is_sorted_complete_and_secret_redacted() {
        let fixture = Fixture::new("attached");
        let repo_a = RepoId::parse("acme/alpha").expect("repo A");
        let repo_b = RepoId::parse("acme/beta").expect("repo B");
        fixture.bind(&repo_b);
        fixture.bind(&repo_a);
        let source = Arc::new(FixtureSource::default());
        for (repo, incarnation) in [
            (&repo_b, "00000000000000000000000000000002"),
            (&repo_a, "00000000000000000000000000000001"),
        ] {
            let (storage, mounted) = fixture.workspace(
                repo,
                WorkspaceName::new("main").expect("main"),
                incarnation,
                7,
                true,
                true,
            );
            let (mount, path) = mounted.expect("mounted fixture");
            source.projects.lock().expect("source").insert(
                repo.clone(),
                ProjectInventoryFacts {
                    storage: vec![storage],
                    mounts: vec![mount.clone()],
                    mount_paths: BTreeMap::from([(mount.volume_name, path)]),
                },
            );
        }
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        let facts = inventory.all_attached().await.expect("attached inventory");
        assert_eq!(
            facts
                .iter()
                .map(|fact| fact.repo_id.as_str())
                .collect::<Vec<_>>(),
            ["acme/alpha", "acme/beta"]
        );
        assert!(facts.iter().all(|fact| fact.revision == 7));
        let rendered = format!("{facts:?}");
        assert!(!rendered.contains(facts[0].credentials.token()));
        assert!(!rendered.contains("BEGIN PRIVATE KEY"));
        let error = inventory
            .all_reserved_port_bases()
            .await
            .expect_err("duplicate global port assignment");
        assert!(matches!(
            error,
            GatewayInventoryError::DuplicatePortBlock(40_960)
        ));
    }

    #[tokio::test]
    async fn detached_and_legacy_main_facts_never_become_sessions() {
        let fixture = Fixture::new("excluded");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let (legacy, mounted) = fixture.workspace(
            &repo,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000001",
            4,
            true,
            false,
        );
        let (detached, _) = fixture.workspace(
            &repo,
            WorkspaceName::session("raven").expect("session"),
            "00000000000000000000000000000002",
            5,
            false,
            true,
        );
        let (mount, path) = mounted.expect("legacy main mount");
        let source = Arc::new(FixtureSource {
            projects: Mutex::new(BTreeMap::from([(
                repo.clone(),
                ProjectInventoryFacts {
                    storage: vec![legacy, detached],
                    mounts: vec![mount.clone()],
                    mount_paths: BTreeMap::from([(mount.volume_name, path)]),
                },
            )])),
        });
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        assert!(
            inventory
                .project_attached(&repo)
                .await
                .expect("closed inventory")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn duplicate_mounts_and_foreign_bindings_fail_closed() {
        let fixture = Fixture::new("invalid");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let (storage, mounted) = fixture.workspace(
            &repo,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000001",
            9,
            true,
            true,
        );
        let (mount, path) = mounted.expect("mount");
        let duplicate = KernelMountFact {
            mount_id: mount.mount_id + 1,
            volume_name: mount.volume_name.clone(),
        };
        let source = Arc::new(FixtureSource {
            projects: Mutex::new(BTreeMap::from([(
                repo.clone(),
                ProjectInventoryFacts {
                    storage: vec![storage],
                    mounts: vec![mount.clone(), duplicate],
                    mount_paths: BTreeMap::from([(mount.volume_name, path)]),
                },
            )])),
        });
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );
        assert!(matches!(
            inventory.project_attached(&repo).await,
            Err(GatewayInventoryError::AmbiguousMount(_))
        ));

        let paths = ProjectPaths::new(fixture.storage.store(), &repo).expect("paths");
        let foreign = RepositoryBinding::new(vec![BoundIdentity {
            repo_id: RepoId::parse("other/widget").expect("foreign repo"),
            remote_name: None,
            remote_url: None,
            primary: true,
        }])
        .expect("foreign binding");
        write_json(&paths.repository_binding, &foreign).expect("replace binding");
        assert!(matches!(
            inventory.all_attached().await,
            Err(GatewayInventoryError::ForeignBinding { .. })
        ));
    }
}
