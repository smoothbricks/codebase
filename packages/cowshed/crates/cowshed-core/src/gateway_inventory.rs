use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::apfs::{ApfsCaseSensitivity, SystemCommandRunner};
use crate::metadata::{
    CheckoutLayout, DetachedWorkspaceMetadata, GrantSet, ImageFormat, PortBlock, PublicationState,
    WorkspaceIncarnation, WorkspaceName, sidecar_path,
};
use crate::repository::{RepoId, RepositoryBinding};
use crate::storage::apfs::native::{
    KernelMountSnapshot, KernelMountSource, MacOsApfsExecutionHost, SystemKernelMountSource,
};
use crate::storage::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrate, ApfsSubstrateConfig,
};
use crate::storage::bootstrap::ValidatedHostStorage;
use crate::storage::lifecycle::{
    DerivationError, KernelMountFact, LifecycleWorkspace, MountIntent, MountState, StorageFact,
    Substrate, derive_workspaces,
};
use crate::storage::{StorageLayout, verify_no_symlinks};
use crate::workspace_credentials::{
    GatewayWorkspaceCredentials, WorkspaceCredentialError, read_gateway_workspace_credentials,
};

const MAX_BINDING_BYTES: u64 = 1024 * 1024;
const UNRESOLVED_CHECKOUT_PATH: &str = ".unresolved-main-mount";

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

/// One validated adopted project discovered from `<store>/<owner>/<repo>/repository.json`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdoptedProject {
    pub repo_id: RepoId,
    pub project_root: PathBuf,
}

/// A project whose main workspace is not mounted where its checkout layout puts it.
///
/// Mains are always-mounted (02_workspaces.md): the gateway mounts every one across every adopted
/// project before it serves, so a main that is not mounted is a host defect rather than a state a
/// user chose — `doctor` reports it as critical and `setup` refuses to call the host set up over
/// it. Both paths are named because neither is guessable from the other: the image is what should
/// be mounted, the mountpoint is the directory the user's shell, editor, and Finder are looking at.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnreachableMain {
    pub repo_id: RepoId,
    pub image: PathBuf,
    pub mountpoint: PathBuf,
    /// What was observed instead, in the words a finding shows.
    pub reason: String,
}

/// What eager heal achieved for one project.
///
/// Main and sessions are reported apart because they are not equally load-bearing: main is the
/// user's own checkout and an unmounted one is critical, while a session that fails to mount costs
/// only that session. Each result is kept rather than counted so the failure names itself.
#[derive(Debug)]
pub struct ProjectHealOutcome {
    pub repo_id: RepoId,
    /// Main's mountpoint, or why the project's checkout is not reachable there.
    pub main: Result<PathBuf, GatewayInventoryError>,
    /// One entry per recorded session workspace, in inventory order. Empty when the project could
    /// not be opened at all — there was nothing to attempt.
    pub sessions: Vec<SessionHealOutcome>,
}

#[derive(Debug)]
pub struct SessionHealOutcome {
    pub workspace: WorkspaceName,
    pub mount: Result<PathBuf, GatewayInventoryError>,
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
    #[error("project root {0} is claimed by more than one repository binding")]
    AmbiguousProjectRoot(PathBuf),
    #[error("gateway inventory has duplicate or ambiguous mount fact for {0}")]
    AmbiguousMount(String),
    #[error("gateway inventory metadata is invalid at {path}: {message}")]
    InvalidMetadata { path: PathBuf, message: String },
    #[error("attached workspace {repo}/{workspace} has no canonical macOS port block")]
    MissingPortBlock {
        repo: RepoId,
        workspace: WorkspaceName,
    },
    #[error("adopted project {0} records no main workspace to mount")]
    MissingMainWorkspace(RepoId),
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
        let checkout_path = authoritative_checkout_path(&layout, repo)?.unwrap_or_else(|| {
            storage
                .store()
                .join("gateway")
                .join(UNRESOLVED_CHECKOUT_PATH)
        });
        let config = project_substrate_config(storage, &layout, checkout_path)?;
        let captured = SystemKernelMountSource.mounts()?;
        let host = MacOsApfsExecutionHost::with_mount_source(
            SystemCommandRunner,
            config.clone(),
            CapturedKernelMountSource {
                mounts: captured.clone(),
            },
        )?;
        let storage_facts = host.list(repo)?;
        let mount_paths = expected_mount_paths(&config, &layout, &storage_facts)?;
        reject_ambiguous_native_mounts(&captured, &mount_paths)?;
        let mounts = host.mounts(repo)?;
        Ok(ProjectInventoryFacts {
            storage: storage_facts,
            mounts,
            mount_paths,
        })
    }
}

/// The substrate configuration for one project, read from the project's own records.
///
/// One builder for both sides of the inventory: the read-only fact pass and eager heal have to
/// agree about where every workspace of a project mounts, and a second copy of this derivation is
/// how they would stop agreeing.
fn project_substrate_config(
    storage: &ValidatedHostStorage,
    layout: &StorageLayout,
    checkout_path: PathBuf,
) -> Result<ApfsSubstrateConfig, GatewayInventoryError> {
    let checkout_layout =
        layout
            .checkout_layout()
            .map_err(|error| GatewayInventoryError::InvalidMetadata {
                path: layout.project().project_root.clone(),
                message: error.to_string(),
            })?;
    Ok(ApfsSubstrateConfig::new(
        storage.store(),
        storage.caches(),
        checkout_path,
        checkout_layout,
        ApfsCaseSensitivity::Sensitive,
    ))
}

/// One project's mount side, opened once and mounting nothing on its own.
///
/// Separate from [`InventorySource`] because the two answer different questions — what the store
/// records versus what the kernel can be made to hold — and because opening a project starts a
/// mount registry thread, which the two-pass heal order would otherwise pay for twice per project.
#[async_trait]
trait ProjectMounts: Send + Sync {
    /// Every workspace the project records, main included, with nothing mounted.
    async fn workspaces(&self) -> Result<Vec<LifecycleWorkspace>, GatewayInventoryError>;
    /// Mount one workspace where this project's checkout layout puts it.
    async fn mount(&self, workspace: &LifecycleWorkspace)
    -> Result<PathBuf, GatewayInventoryError>;
}

#[async_trait]
trait HealSource: Send + Sync {
    async fn open(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<Arc<dyn ProjectMounts>, GatewayInventoryError>;
}

#[derive(Clone, Copy, Debug, Default)]
struct NativeHealSource;

#[async_trait]
impl HealSource for NativeHealSource {
    /// A project with no adopted checkout path is refused rather than defaulted.
    ///
    /// Heal exists to put main where the user's tree expects it; without that path there is no
    /// such place, and mounting main anywhere else would create the dangling checkout this pass is
    /// here to prevent.
    async fn open(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<Arc<dyn ProjectMounts>, GatewayInventoryError> {
        let layout = StorageLayout::new(storage.store(), repo).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: storage.store().to_owned(),
                message: error.to_string(),
            }
        })?;
        let checkout_path = authoritative_checkout_path(&layout, repo)?.ok_or_else(|| {
            GatewayInventoryError::InvalidMetadata {
                path: layout.project().project_root.clone(),
                message: "project records no adopted checkout path".to_owned(),
            }
        })?;
        let config = project_substrate_config(storage, &layout, checkout_path)?;
        let host = MacOsApfsExecutionHost::new(SystemCommandRunner, config.clone())?;
        Ok(Arc::new(NativeProjectMounts {
            repo: repo.clone(),
            substrate: ApfsSubstrate::new(config, host),
        }))
    }
}

struct NativeProjectMounts {
    repo: RepoId,
    substrate: ApfsSubstrate<MacOsApfsExecutionHost<SystemCommandRunner>>,
}

#[async_trait]
impl ProjectMounts for NativeProjectMounts {
    async fn workspaces(&self) -> Result<Vec<LifecycleWorkspace>, GatewayInventoryError> {
        Ok(self
            .substrate
            .list(&self.repo)
            .await?
            .into_iter()
            .map(|derived| derived.workspace)
            .collect())
    }

    async fn mount(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<PathBuf, GatewayInventoryError> {
        Ok(self
            .substrate
            .ensure_mounted(workspace, MountIntent { browse: false })
            .await?)
    }
}

/// One project prepared for heal, with nothing mounted yet.
struct OpenProject {
    mounts: Arc<dyn ProjectMounts>,
    /// The project's main. Absent only when its store records none, which for an adopted project
    /// means its main image was retired without a replacement.
    main: Option<LifecycleWorkspace>,
    sessions: Vec<LifecycleWorkspace>,
}

/// One project between the two heal passes: its main settled, its sessions still to mount.
struct HealedMain {
    repo_id: RepoId,
    /// Absent when the project could not be opened, so there is nothing left to attempt.
    project: Option<OpenProject>,
    main: Result<PathBuf, GatewayInventoryError>,
}

/// Read-only native inventory rooted in an already existing-only validated host store.
#[derive(Clone)]
pub struct NativeGatewayInventory {
    storage: ValidatedHostStorage,
    source: Arc<dyn InventorySource>,
    heal: Arc<dyn HealSource>,
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
            heal: Arc::new(NativeHealSource),
        }
    }

    #[cfg(test)]
    fn with_source(storage: ValidatedHostStorage, source: Arc<dyn InventorySource>) -> Self {
        Self {
            storage,
            source,
            heal: Arc::new(NativeHealSource),
        }
    }

    #[cfg(test)]
    fn with_heal_source(storage: ValidatedHostStorage, heal: Arc<dyn HealSource>) -> Self {
        Self {
            storage,
            source: Arc::new(NativeInventorySource),
            heal,
        }
    }

    pub async fn adopted_projects(&self) -> Result<Vec<AdoptedProject>, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || inventory.adopted_projects_blocking())
            .await
            .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    fn adopted_projects_blocking(&self) -> Result<Vec<AdoptedProject>, GatewayInventoryError> {
        let mut projects = Vec::new();
        for repo_id in discover_repositories(self.storage.store())? {
            let layout = match StorageLayout::new(self.storage.store(), &repo_id) {
                Ok(layout) => layout,
                Err(error) => {
                    eprintln!("cowshed: skipping unhealable project {repo_id}: {error}");
                    continue;
                }
            };
            match authoritative_checkout_path(&layout, &repo_id) {
                Ok(Some(project_root)) => projects.push(AdoptedProject {
                    repo_id,
                    project_root,
                }),
                Ok(None) | Err(_) => {
                    eprintln!(
                        "cowshed: skipping unhealable project {repo_id}: project records no adopted checkout path"
                    );
                }
            }
        }
        Ok(projects)
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

    /// Attach and mount every recorded project's workspaces, mains before sessions.
    ///
    /// This runs at gateway startup, before serving, because the gateway is `RunAtLoad` and a
    /// reboot is the one window adoption's "the checkout path is never absent and never dangling"
    /// guarantee cannot defend on its own. Healing on contact would leave a dangling symlink — or,
    /// under direct mount, a bare stub directory — visible in the user's shell, editor, and Finder
    /// until something happened to touch it.
    ///
    /// Mains go first across every project, not per project in inventory order: a main is the
    /// user's own checkout and is always-mounted (02_workspaces.md), so no project's session
    /// mount — which may attach, fsck, and mount a multi-gigabyte image — is allowed to stand
    /// between another project's checkout and the gateway serving.
    ///
    /// Failures are per-project and returned rather than raised: one project whose store or image
    /// cannot be healed must not cost every other project its gateway. A project that cannot even
    /// be opened reports that error as its main outcome, because an unopenable project is exactly a
    /// project whose main is unreachable.
    pub async fn heal_all(
        &self,
    ) -> Result<Vec<ProjectHealOutcome>, GatewayInventoryError> {
        let store = self.storage.store().to_owned();
        let repositories =
            crate::storage::lifecycle::dispatch_blocking(move || discover_repositories(&store))
                .await
                .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))??;
        let mut opened = Vec::with_capacity(repositories.len());
        for repo in repositories {
            let project = self.open_project(&repo).await;
            opened.push((repo, project));
        }
        let mut healed_mains = Vec::with_capacity(opened.len());
        for (repo_id, project) in opened {
            let (project, main) = match project {
                Ok(project) => {
                    let main = match &project.main {
                        Some(main) => project.mounts.mount(main).await,
                        None => Err(GatewayInventoryError::MissingMainWorkspace(repo_id.clone())),
                    };
                    (Some(project), main)
                }
                Err(error) => (None, Err(error)),
            };
            healed_mains.push(HealedMain {
                repo_id,
                project,
                main,
            });
        }
        let mut outcomes = Vec::with_capacity(healed_mains.len());
        for healed in healed_mains {
            let mut sessions = Vec::new();
            if let Some(project) = healed.project {
                sessions.reserve(project.sessions.len());
                for workspace in &project.sessions {
                    sessions.push(SessionHealOutcome {
                        workspace: workspace.name().clone(),
                        mount: project.mounts.mount(workspace).await,
                    });
                }
            }
            outcomes.push(ProjectHealOutcome {
                repo_id: healed.repo_id,
                main: healed.main,
                sessions,
            });
        }
        Ok(outcomes)
    }

    /// Open one project's mount side and split its workspaces by class.
    ///
    /// Opening is separated from mounting so the whole store is prepared before the first mount:
    /// the main pass is only "mains first" if no project's preparation happens between two other
    /// projects' mains.
    async fn open_project(&self, repo: &RepoId) -> Result<OpenProject, GatewayInventoryError> {
        let mounts = self.heal.open(&self.storage, repo).await?;
        let (mains, sessions): (Vec<_>, Vec<_>) = mounts
            .workspaces()
            .await?
            .into_iter()
            .partition(|workspace| workspace.name().is_main());
        Ok(OpenProject {
            mounts,
            main: mains.into_iter().next(),
            sessions,
        })
    }

    /// Every adopted project whose main is not mounted where its checkout layout puts it.
    ///
    /// Observation only — nothing is mounted, because `doctor` never mutates (06_cli.md) and
    /// `setup` reports the host it found rather than the host it wishes for. A project whose facts
    /// cannot be read at all is reported as unreachable with that failure as its reason: "cannot
    /// tell" is not "mounted", and an invariant nobody can check is not an invariant that holds.
    pub async fn unmounted_mains(&self) -> Result<Vec<UnreachableMain>, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || inventory.unmounted_mains_blocking())
            .await
            .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    fn unmounted_mains_blocking(&self) -> Result<Vec<UnreachableMain>, GatewayInventoryError> {
        let mut unreachable = Vec::new();
        for project in self.adopted_projects_blocking()? {
            let layout =
                StorageLayout::new(self.storage.store(), &project.repo_id).map_err(|error| {
                    GatewayInventoryError::InvalidMetadata {
                        path: self.storage.store().to_owned(),
                        message: error.to_string(),
                    }
                })?;
            // An adopted project holds exactly one main image — reading it is how its checkout
            // path was resolved. None means the image was retired between that read and this one,
            // which is a race rather than a defect and belongs to whoever is retiring it.
            let Some(image) = existing_main_image(&layout)? else {
                continue;
            };
            let main = WorkspaceName::new("main").expect("fixed main");
            // A project that never recorded its checkout layout cannot say where its main belongs,
            // and that unresolved record is itself the defect — not grounds to skip the project and
            // report the host as healthy. The adopted checkout is the path named for it, because
            // direct mount is what adopt writes and puts main exactly there.
            let (mountpoint, unresolved_layout) = match layout.checkout_layout() {
                Ok(checkout_layout) => (
                    workspace_mountpoint(&layout, checkout_layout, &project.project_root, &main)?,
                    None,
                ),
                Err(error) => (
                    project.project_root.clone(),
                    Some(format!("the project records no checkout layout: {error}")),
                ),
            };
            let reason = match unresolved_layout {
                Some(unresolved) => Some(unresolved),
                None => match self.source.project_facts(&self.storage, &project.repo_id) {
                    Ok(facts) => main_mount_defect(facts)?,
                    Err(error) => Some(error.to_string()),
                },
            };
            if let Some(reason) = reason {
                unreachable.push(UnreachableMain {
                    repo_id: project.repo_id,
                    image,
                    mountpoint,
                    reason,
                });
            }
        }
        Ok(unreachable)
    }

    pub async fn all_reserved_port_bases(&self) -> Result<BTreeSet<u16>, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            inventory.all_reserved_port_bases_blocking()
        })
        .await
        .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    pub async fn repository_for_project_root(
        &self,
        project_root: &Path,
    ) -> Result<Option<RepoId>, GatewayInventoryError> {
        let inventory = self.clone();
        let project_root = project_root.to_owned();
        crate::storage::lifecycle::dispatch_blocking(move || {
            inventory.repository_for_project_root_blocking(&project_root)
        })
        .await
        .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    fn repository_for_project_root_blocking(
        &self,
        project_root: &Path,
    ) -> Result<Option<RepoId>, GatewayInventoryError> {
        let expected = fs::canonicalize(project_root)
            .map_err(|source| io_error("resolving project root", project_root, source))?;
        let mut matched = None;
        for repo in discover_repositories(self.storage.store())? {
            let layout = StorageLayout::new(self.storage.store(), &repo).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: self.storage.store().to_owned(),
                    message: error.to_string(),
                }
            })?;
            let mut images = Vec::new();
            for format in [ImageFormat::Asif, ImageFormat::Sparse] {
                let image = layout
                    .main_image(format)
                    .map_err(|error| GatewayInventoryError::InvalidMetadata {
                        path: layout.project().project_root.clone(),
                        message: error.to_string(),
                    })?
                    .image()
                    .to_owned();
                if fs::symlink_metadata(&image).is_ok_and(|metadata| metadata.file_type().is_file())
                {
                    images.push(image);
                }
            }
            let trash = layout.project().sessions.join(".trash");
            match fs::read_dir(&trash) {
                Ok(entries) => {
                    for entry in entries {
                        let path = entry
                            .map_err(|source| io_error("reading retirement trash", &trash, source))?
                            .path();
                        if path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.starts_with("main-"))
                            && ImageFormat::from_image_path(&path).is_ok()
                            && fs::symlink_metadata(&path)
                                .is_ok_and(|metadata| metadata.file_type().is_file())
                        {
                            images.push(path);
                        }
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(io_error("enumerating retirement trash", &trash, source));
                }
            }
            let claims_root = images.into_iter().try_fold(false, |claimed, image| {
                verify_no_symlinks(self.storage.store(), &image).map_err(|error| {
                    GatewayInventoryError::InvalidMetadata {
                        path: image.clone(),
                        message: error.to_string(),
                    }
                })?;
                let metadata =
                    DetachedWorkspaceMetadata::read_for_image(&image).map_err(|error| {
                        GatewayInventoryError::InvalidMetadata {
                            path: sidecar_path(&image),
                            message: error.to_string(),
                        }
                    })?;
                if metadata.repo_id != repo || !metadata.workspace.is_main() {
                    return Err(GatewayInventoryError::InvalidMetadata {
                        path: sidecar_path(&image),
                        message: "main image metadata identity does not match its binding"
                            .to_owned(),
                    });
                }
                let matches = metadata
                    .info_snapshot
                    .as_ref()
                    .and_then(|info| fs::canonicalize(&info.project_root).ok())
                    .is_some_and(|root| root == expected);
                Ok::<_, GatewayInventoryError>(claimed || matches)
            })?;
            if claims_root && matched.replace(repo).is_some() {
                return Err(GatewayInventoryError::AmbiguousProjectRoot(
                    project_root.to_owned(),
                ));
            }
        }
        Ok(matched)
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
            match self.load_project(&repo) {
                Ok(project) => facts.extend(project),
                Err(error) => {
                    // Store-wide identity collisions stay fatal. A single project's
                    // mount/metadata mismatch is a doctor finding and must not take
                    // the RunAtLoad gateway down with it.
                    if matches!(
                        error,
                        GatewayInventoryError::DuplicateRepository(_)
                            | GatewayInventoryError::DuplicatePortBlock(_)
                            | GatewayInventoryError::AmbiguousProjectRoot(_)
                            | GatewayInventoryError::ForeignBinding { .. }
                    ) {
                        return Err(error);
                    }
                    eprintln!("cowshed: skipping unhealable project {repo}: {error}");
                }
            }
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
            let volume = crate::storage::apfs::volume_key(repo_id, workspace.workspace.name());
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
            // Main with no recorded info snapshot never finished adoption: its checkout was never
            // taken over, so nothing should be served from it. This is a deliberate exclusion, not
            // a side effect of failing to resolve a path — the mount path itself is derivable from
            // the layout for every workspace.
            if workspace.workspace.name().is_main() && metadata.require_info_snapshot().is_err() {
                continue;
            }
            // The mount path is not re-derived here. `mount_paths` is already the checkout-layout
            // aware expectation (`expected_mount_paths`), and `ApfsExecutionHost::mounts` only
            // reports a volume as mounted when the kernel has it at exactly that path. Deriving it
            // a second time from the layout alone would ignore `CheckoutLayout::DirectMount`, where
            // main mounts at the adopted checkout instead of `mnt/<owner>/<repo>/main`.
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

const RESERVED_STORE_NAMESPACES: &[&str] = &[
    "caches",
    "telemetry",
    "gateway",
    "mnt",
    "run",
    "tmp",
    "quarantine",
];

fn is_reserved_store_namespace(name: &str) -> bool {
    name.starts_with('.') || RESERVED_STORE_NAMESPACES.contains(&name)
}

/// Is this store entry something other than a project namespace?
///
/// Three kinds of neighbour share the store root with `<owner>/<repo>` projects and none of them is
/// one: the bootstrap volumes, which carry a `.cowshed-volume.json` role marker and are their own
/// namespace; macOS's per-volume system directories (`.fseventsd`, `.Spotlight-V100`, `.Trashes`),
/// which are root-owned and unreadable to us; and cowshed's own reserved namespaces. The volume
/// marker is the structural test and is checked first, because it identifies a volume root by what
/// it *is* rather than by what it is called — a name list cannot keep up with a mountpoint the user
/// relocates, and being wrong here costs the whole inventory.
fn is_not_a_project_namespace(path: &Path, name: &str) -> bool {
    is_reserved_store_namespace(name)
        || fs::symlink_metadata(path.join(crate::storage::bootstrap::VOLUME_MARKER_FILE)).is_ok()
}

/// Enumerate the projects in the store, skipping everything that is not one.
///
/// Discovery is per-entry isolated for the same reason healing is (see `heal_all`): the store root
/// is a mount point with neighbours cowshed does not own and cannot read, and an entry that cannot
/// be inspected is evidence that it is not a project — not grounds to fail the pass. Raising here
/// took down the entire gateway inventory over one root-owned system directory, which left launchd
/// showing a running process while status and doctor reported nothing started and eager heal never
/// ran. A candidate that cannot be read is skipped; a candidate that reads as a real but broken
/// project still raises, because that is cowshed's own state and hiding it would hide corruption.
fn discover_repositories(store_root: &Path) -> Result<Vec<RepoId>, GatewayInventoryError> {
    ensure_directory(store_root, "opening validated store root")?;
    let mut repositories = BTreeSet::new();
    for owner in read_directory(store_root, "enumerating store owners")? {
        let Some(owner_name) = owner.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if is_not_a_project_namespace(&owner, owner_name) || !is_directory(&owner).unwrap_or(false)
        {
            continue;
        }
        let Ok(projects) = read_directory(&owner, "enumerating owner repositories") else {
            continue;
        };
        for project in projects {
            let Some(project_name) = project.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if is_not_a_project_namespace(&project, project_name)
                || !is_directory(&project).unwrap_or(false)
            {
                continue;
            }
            let binding_path = project.join("repository.json");
            // Unreadable is "not a project"; only a binding that is present and readable is held
            // to the project contract.
            if !binding_path_exists(&binding_path).unwrap_or(false) {
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
    let paths = StorageLayout::new(store_root, repo_id)
        .map(|layout| layout.project().clone())
        .map_err(|error| GatewayInventoryError::InvalidBinding {
            path: store_root.to_owned(),
            message: error.to_string(),
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
    let expected_paths = StorageLayout::new(store_root, &actual)
        .map(|layout| layout.project().clone())
        .map_err(|error| GatewayInventoryError::InvalidBinding {
            path: binding_path.to_owned(),
            message: error.to_string(),
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

fn authoritative_checkout_path(
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
    config: &ApfsSubstrateConfig,
    layout: &StorageLayout,
    storage: &[StorageFact],
) -> Result<BTreeMap<String, PathBuf>, GatewayInventoryError> {
    let mut paths = BTreeMap::new();
    for fact in storage {
        let mount = workspace_mountpoint(
            layout,
            config.checkout_layout,
            &config.checkout_path,
            fact.workspace.name(),
        )?;
        paths.insert(fact.volume_key.clone(), mount);
    }
    Ok(paths)
}

/// Where one workspace of one project mounts.
///
/// Main's path follows the project's checkout layout — the adopted checkout itself under direct
/// mount, the uniform `mnt/` path under the symlink layout. Every other workspace mounts under
/// `mnt/` either way. One rule, because the read-only fact pass and the always-mounted check both
/// need it and a project whose two answers disagreed would be reported as broken by whichever
/// derivation ran second.
fn workspace_mountpoint(
    layout: &StorageLayout,
    checkout_layout: CheckoutLayout,
    checkout_path: &Path,
    workspace: &WorkspaceName,
) -> Result<PathBuf, GatewayInventoryError> {
    if workspace.is_main() && checkout_layout.mounts_at_checkout() {
        return Ok(checkout_path.to_owned());
    }
    layout
        .workspace_mount(workspace)
        .map_err(|error| GatewayInventoryError::InvalidMetadata {
            path: layout.project().mount_root.clone(),
            message: error.to_string(),
        })
}

/// The canonical main image this project actually holds, in whichever format it was written.
///
/// The store holds at most one: `authoritative_checkout_path` rejects a project carrying both, so
/// the first hit is the answer rather than a candidate.
fn existing_main_image(
    layout: &StorageLayout,
) -> Result<Option<PathBuf>, GatewayInventoryError> {
    for format in [ImageFormat::Asif, ImageFormat::Sparse] {
        let paths =
            layout
                .main_image(format)
                .map_err(|error| GatewayInventoryError::InvalidMetadata {
                    path: layout.project().project_root.clone(),
                    message: error.to_string(),
                })?;
        if paths
            .image()
            .try_exists()
            .map_err(|source| io_error("inspecting canonical main image", paths.image(), source))?
        {
            return Ok(Some(paths.image().to_owned()));
        }
    }
    Ok(None)
}

/// Why this project's main is not mounted, or `None` when it is.
///
/// A project whose facts hold no main at all is reported rather than passed over: the always-
/// mounted invariant is about the checkout the user sees, and a store that records no main for an
/// adopted project cannot be serving one.
fn main_mount_defect(
    facts: ProjectInventoryFacts,
) -> Result<Option<String>, GatewayInventoryError> {
    let derived = derive_workspaces(facts.storage, facts.mounts, [])?;
    let Some(main) = derived
        .into_iter()
        .find(|workspace| workspace.workspace.name().is_main())
    else {
        return Ok(Some(String::from(
            "the project's store records no main workspace",
        )));
    };
    Ok(match main.mount_state {
        MountState::Mounted { .. } => None,
        MountState::Detached => Some(String::from("main's volume is not mounted")),
    })
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
        if !volumes.insert(mount.volume_key.as_str()) || !ids.insert(mount.mount_id) {
            return Err(GatewayInventoryError::AmbiguousMount(
                mount.volume_key.clone(),
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
        CheckoutLayout, METADATA_VERSION, Platform, WorkspaceInfoSnapshot, WorkspaceRole,
        write_json,
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
        checkout_layout: CheckoutLayout,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            Self::with_checkout_layout(label, CheckoutLayout::Symlink)
        }

        fn with_checkout_layout(label: &str, checkout_layout: CheckoutLayout) -> Self {
            let root = std::env::temp_dir().join(format!(
                "cowshed-gateway-inventory-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            let home = root.join("home");
            fs::create_dir_all(&home).expect("fixture home");
            let roots = CanonicalRoots::for_test(root.join("store"), root.join("caches"));
            fs::create_dir_all(roots.store()).expect("fixture store");
            fs::create_dir_all(roots.caches()).expect("fixture caches");
            fs::create_dir_all(roots.telemetry()).expect("fixture telemetry");
            Self {
                root,
                storage: ValidatedHostStorage::new(home, roots),
                checkout_layout,
            }
        }

        fn bind(&self, repo: &RepoId) {
            let paths = StorageLayout::new(self.storage.store(), repo)
                .expect("project paths")
                .project()
                .clone();
            fs::create_dir_all(&paths.project_root).expect("project root");
            let binding = RepositoryBinding::new(vec![BoundIdentity {
                repo_id: repo.clone(),
                remote_name: None,
                remote_url: None,
                primary: true,
            }])
            .expect("binding");
            write_json(&paths.repository_binding, &binding).expect("binding file");
            // Adopt records the layout, so a fixture project that omitted it would be exercising a
            // corrupted project rather than a healthy one.
            StorageLayout::new(self.storage.store(), repo)
                .expect("layout")
                .record_checkout_layout(self.checkout_layout)
                .expect("checkout layout record");
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
            let checkout = self.root.join(format!("checkout-{}", repo.repo()));
            if name.is_main() && persist_main_root {
                fs::create_dir_all(&checkout).expect("adopted checkout");
            }
            // Same derivation production uses in `expected_mount_paths`: main follows the project's
            // checkout layout, every other workspace mounts under `mnt/` either way.
            let mount = if name.is_main() && self.checkout_layout.mounts_at_checkout() {
                checkout.clone()
            } else {
                layout.workspace_mount(&name).expect("workspace mount")
            };
            let mut grants =
                GrantSet::closed_baseline(Some(PortBlock::new(40_960, 16).expect("port block")))
                    .expect("grants");
            grants.revision = revision;
            let info_snapshot =
                (!name.is_main() || persist_main_root).then(|| WorkspaceInfoSnapshot {
                    project_root: if name.is_main() {
                        checkout.clone()
                    } else {
                        mount.clone()
                    },
                    role,
                    base_commit: "0123456789abcdef".to_owned(),
                    branch: Some("main".to_owned()),
                    created_at: "2026-07-14T00:00:00Z".to_owned(),
                    forked_from: None,
                    captured_at: "2026-07-14T00:00:00Z".to_owned(),
                    stale: false,
                    git_worktree: false,
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
            let volume_key = crate::storage::apfs::volume_key(repo, &name);
            let storage = StorageFact {
                workspace: workspace.clone(),
                volume_key: volume_key.clone(),
            };
            let mounted = mounted.then(|| {
                fs::create_dir_all(&mount).expect("mount");
                mint_workspace_credentials(
                    &workspace,
                    &mount,
                    &mount,
                    Platform::Macos,
                    Some(PortBlock::new(40_960, 16).expect("port block")),
                    image.ca_private_key(),
                )
                .expect("credentials");
                (
                    KernelMountFact {
                        mount_id: revision + 100,
                        volume_key,
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

    /// The store root is a volume mount point with neighbours cowshed does not own: the caches
    /// volume beside it, and macOS's root-owned per-volume system directories inside both. Probing
    /// one of those for a repository binding earns `EPERM`, and raising it took the whole inventory
    /// down — launchd showed a running gateway while status and doctor reported nothing started.
    #[test]
    fn store_neighbours_and_unreadable_entries_never_fail_the_discovery_pass() {
        use std::os::unix::fs::PermissionsExt;

        let fixture = Fixture::new("discovery-neighbours");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let store = fixture.storage.store().to_owned();

        // The caches volume, mounted inside the store root and carrying its role marker.
        let caches = store.join("caches");
        fs::create_dir_all(&caches).expect("caches volume");
        fs::write(
            caches.join(crate::storage::bootstrap::VOLUME_MARKER_FILE),
            b"{}",
        )
        .expect("volume marker");
        let caches_system = caches.join(".fseventsd");
        fs::create_dir_all(&caches_system).expect("caches system directory");

        // A volume root whose name is not on any reserved list — a relocated mount is identified by
        // its marker, never by what it happens to be called.
        // Its child carries a binding, so without the marker skip it would be discovered as the
        // bogus repository `scratch-volume/anything`.
        let relocated = store.join("scratch-volume");
        fs::create_dir_all(relocated.join("anything")).expect("relocated volume");
        fs::write(relocated.join("anything/repository.json"), b"{}").expect("decoy binding");
        fs::write(
            relocated.join(crate::storage::bootstrap::VOLUME_MARKER_FILE),
            b"{}",
        )
        .expect("relocated marker");

        // A genuinely unreadable owner-level directory: not a project, and not a reason to stop.
        let opaque = store.join("opaque-owner");
        fs::create_dir_all(opaque.join("child")).expect("opaque owner");
        fs::set_permissions(&opaque, fs::Permissions::from_mode(0o000)).expect("chmod opaque");

        let discovered = discover_repositories(&store);
        fs::set_permissions(&opaque, fs::Permissions::from_mode(0o755)).expect("restore opaque");

        assert_eq!(
            discovered.expect("discovery survives its neighbours"),
            vec![repo],
            "only real projects are discovered, and no neighbour fails the pass"
        );
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
                    mount_paths: BTreeMap::from([(mount.volume_key, path)]),
                },
            );
        }
        for namespace in RESERVED_STORE_NAMESPACES {
            let collision = fixture.storage.store().join(namespace).join("system-owned");
            fs::create_dir_all(&collision).expect("reserved namespace fixture");
            fs::write(
                collision.join("repository.json"),
                b"not a repository binding",
            )
            .expect("invalid reserved binding");
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
        assert_eq!(
            inventory
                .repository_for_project_root(&fixture.root.join("checkout-alpha"))
                .await
                .expect("recover current main binding"),
            Some(repo_a.clone())
        );
        let error = inventory
            .all_reserved_port_bases()
            .await
            .expect_err("duplicate global port assignment");
        assert!(matches!(
            error,
            GatewayInventoryError::DuplicatePortBlock(40_960)
        ));
    }

    /// A direct-mount project's main volume is mounted at the adopted checkout, not at
    /// `mnt/<owner>/<repo>/main`. Re-deriving the expected path from the layout alone rejected
    /// every such project — the whole default layout — with "mounted workspace path does not equal
    /// canonical path", so `cowshed adopt` finished with a gateway that never came up.
    #[tokio::test]
    async fn a_direct_mount_project_serves_main_from_its_adopted_checkout() {
        let fixture = Fixture::with_checkout_layout("direct-mount", CheckoutLayout::DirectMount);
        let repo = RepoId::parse("hyperide/axe").expect("repo");
        fixture.bind(&repo);
        let (storage, mounted) = fixture.workspace(
            &repo,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000001",
            3,
            true,
            true,
        );
        let (mount, path) = mounted.expect("mounted fixture");
        assert_eq!(path, fixture.root.join("checkout-axe"));
        let source = Arc::new(FixtureSource::default());
        source.projects.lock().expect("source").insert(
            repo.clone(),
            ProjectInventoryFacts {
                storage: vec![storage],
                mounts: vec![mount.clone()],
                mount_paths: BTreeMap::from([(mount.volume_key, path.clone())]),
            },
        );
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        // `project_attached` is the path adopt runs, and it propagates rather than skips: the
        // regression surfaced there as an error, not as an empty inventory.
        let facts = inventory
            .project_attached(&repo)
            .await
            .expect("direct-mount project inventory");
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].mount, path);
        assert_eq!(facts[0].mount_id, mount.mount_id);
    }

    #[tokio::test]
    async fn retired_main_snapshot_recovers_its_exact_project_root_binding() {
        let fixture = Fixture::new("retired-root");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let (storage, _) = fixture.workspace(
            &repo,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000001",
            4,
            false,
            true,
        );
        let layout = StorageLayout::new(fixture.storage.store(), &repo).expect("layout");
        let canonical =
            canonical_image_paths(&layout, &storage.workspace).expect("canonical image");
        let trash = layout.project().sessions.join(".trash");
        fs::create_dir_all(&trash).expect("trash");
        let retired = trash.join("main-retired.sparseimage");
        fs::rename(canonical.image(), &retired).expect("retire image");
        fs::rename(sidecar_path(canonical.image()), sidecar_path(&retired))
            .expect("retire metadata");

        fs::create_dir_all(fixture.root.join("checkout-widget")).expect("restored project root");
        let inventory = NativeGatewayInventory::new(fixture.storage.clone());
        assert_eq!(
            inventory
                .repository_for_project_root(&fixture.root.join("checkout-widget"))
                .await
                .expect("recover retired main binding"),
            Some(repo)
        );
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
                    mount_paths: BTreeMap::from([(mount.volume_key, path)]),
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
            volume_key: mount.volume_key.clone(),
        };
        let source = Arc::new(FixtureSource {
            projects: Mutex::new(BTreeMap::from([(
                repo.clone(),
                ProjectInventoryFacts {
                    storage: vec![storage],
                    mounts: vec![mount.clone(), duplicate],
                    mount_paths: BTreeMap::from([(mount.volume_key, path)]),
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

        let paths = StorageLayout::new(fixture.storage.store(), &repo)
            .expect("paths")
            .project()
            .clone();
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

    /// A heal source that mounts nothing and remembers the order it was asked in.
    struct FakeHealSource {
        projects: BTreeMap<RepoId, Vec<LifecycleWorkspace>>,
        unopenable: BTreeSet<RepoId>,
        refused: BTreeSet<String>,
        order: Arc<Mutex<Vec<String>>>,
    }

    impl FakeHealSource {
        fn new(projects: BTreeMap<RepoId, Vec<LifecycleWorkspace>>) -> Self {
            Self {
                projects,
                unopenable: BTreeSet::new(),
                refused: BTreeSet::new(),
                order: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn unopenable(mut self, repo: &RepoId) -> Self {
            self.unopenable.insert(repo.clone());
            self
        }

        fn refusing(mut self, workspace: &str) -> Self {
            self.refused.insert(workspace.to_owned());
            self
        }
    }

    #[async_trait]
    impl HealSource for FakeHealSource {
        async fn open(
            &self,
            _storage: &ValidatedHostStorage,
            repo: &RepoId,
        ) -> Result<Arc<dyn ProjectMounts>, GatewayInventoryError> {
            if self.unopenable.contains(repo) {
                return Err(GatewayInventoryError::InvalidMetadata {
                    path: PathBuf::from(repo.as_str()),
                    message: String::from("fixture cannot open this project"),
                });
            }
            Ok(Arc::new(FakeProjectMounts {
                workspaces: self.projects.get(repo).cloned().unwrap_or_default(),
                refused: self.refused.clone(),
                order: Arc::clone(&self.order),
            }))
        }
    }

    struct FakeProjectMounts {
        workspaces: Vec<LifecycleWorkspace>,
        refused: BTreeSet<String>,
        order: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ProjectMounts for FakeProjectMounts {
        async fn workspaces(&self) -> Result<Vec<LifecycleWorkspace>, GatewayInventoryError> {
            Ok(self.workspaces.clone())
        }

        async fn mount(
            &self,
            workspace: &LifecycleWorkspace,
        ) -> Result<PathBuf, GatewayInventoryError> {
            let key = format!("{}/{}", workspace.repo(), workspace.name());
            self.order.lock().expect("mount order").push(key.clone());
            if self.refused.contains(&key) {
                return Err(GatewayInventoryError::InvalidMetadata {
                    path: PathBuf::from(&key),
                    message: String::from("fixture cannot mount this workspace"),
                });
            }
            Ok(PathBuf::from("/mounted").join(key))
        }
    }

    fn heal_workspace(repo: &RepoId, name: &str) -> LifecycleWorkspace {
        let name = WorkspaceName::new(name).expect("workspace name");
        let role = if name.is_main() {
            WorkspaceRole::Main
        } else {
            WorkspaceRole::Workspace
        };
        LifecycleWorkspace::new(
            repo.clone(),
            name,
            WorkspaceIncarnation::new("00000000000000000000000000000001").expect("incarnation"),
            Revision::new(1),
            Revision::new(1),
            role,
            ImageFormat::Sparse,
        )
        .expect("workspace")
    }

    /// Every project's main is mounted before any project's session.
    ///
    /// Mains are always-mounted, so the checkout a user sees must not wait behind another
    /// project's session image — the fixture lists each project's session first precisely so
    /// inventory order cannot pass this by accident.
    #[tokio::test]
    async fn eager_heal_mounts_every_main_before_the_first_session() {
        let fixture = Fixture::new("heal-order");
        let alpha = RepoId::parse("acme/alpha").expect("repo alpha");
        let beta = RepoId::parse("acme/beta").expect("repo beta");
        let mut projects = BTreeMap::new();
        for repo in [&alpha, &beta] {
            fixture.bind(repo);
            projects.insert(
                repo.clone(),
                vec![heal_workspace(repo, "raven"), heal_workspace(repo, "main")],
            );
        }
        let heal = Arc::new(FakeHealSource::new(projects));
        let order = Arc::clone(&heal.order);
        let inventory = NativeGatewayInventory::with_heal_source(
            fixture.storage.clone(),
            heal as Arc<dyn HealSource>,
        );

        let outcomes = inventory.heal_all().await.expect("eager heal");

        assert_eq!(
            *order.lock().expect("mount order"),
            [
                "acme/alpha/main",
                "acme/beta/main",
                "acme/alpha/raven",
                "acme/beta/raven"
            ]
        );
        assert_eq!(
            outcomes
                .iter()
                .map(|outcome| outcome.repo_id.as_str())
                .collect::<Vec<_>>(),
            ["acme/alpha", "acme/beta"]
        );
        for outcome in &outcomes {
            assert!(outcome.main.is_ok(), "{} main healed", outcome.repo_id);
            assert_eq!(outcome.sessions.len(), 1);
            assert!(outcome.sessions[0].mount.is_ok());
        }
    }

    /// One unhealable project never costs another its mounts, and an unreachable main never costs
    /// its own project's sessions.
    ///
    /// A single broken checkout taking the `RunAtLoad` daemon down with it would convert one
    /// defect into a machine with no gateway at all (05_gateway.md).
    #[tokio::test]
    async fn one_unhealable_project_never_costs_another_its_mounts() {
        let fixture = Fixture::new("heal-isolation");
        let alpha = RepoId::parse("acme/alpha").expect("repo alpha");
        let beta = RepoId::parse("acme/beta").expect("repo beta");
        let gamma = RepoId::parse("acme/gamma").expect("repo gamma");
        let mut projects = BTreeMap::new();
        for repo in [&alpha, &beta, &gamma] {
            fixture.bind(repo);
            projects.insert(
                repo.clone(),
                vec![heal_workspace(repo, "main"), heal_workspace(repo, "raven")],
            );
        }
        // Alpha sorts first, so its failure is upstream of every other project's mount.
        let heal = Arc::new(
            FakeHealSource::new(projects)
                .unopenable(&alpha)
                .refusing("acme/beta/main"),
        );
        let order = Arc::clone(&heal.order);
        let inventory = NativeGatewayInventory::with_heal_source(
            fixture.storage.clone(),
            heal as Arc<dyn HealSource>,
        );

        let outcomes = inventory.heal_all().await.expect("eager heal");

        assert_eq!(
            *order.lock().expect("mount order"),
            [
                "acme/beta/main",
                "acme/gamma/main",
                "acme/beta/raven",
                "acme/gamma/raven"
            ]
        );
        let alpha_outcome = &outcomes[0];
        assert_eq!(alpha_outcome.repo_id, alpha);
        assert!(matches!(
            alpha_outcome.main,
            Err(GatewayInventoryError::InvalidMetadata { .. })
        ));
        assert!(
            alpha_outcome.sessions.is_empty(),
            "a project that never opened has nothing to attempt"
        );
        let beta_outcome = &outcomes[1];
        assert_eq!(beta_outcome.repo_id, beta);
        assert!(beta_outcome.main.is_err());
        assert!(
            beta_outcome.sessions[0].mount.is_ok(),
            "an unreachable main still leaves its project's sessions to mount"
        );
        let gamma_outcome = &outcomes[2];
        assert_eq!(gamma_outcome.repo_id, gamma);
        assert!(gamma_outcome.main.is_ok());
        assert!(gamma_outcome.sessions[0].mount.is_ok());
    }

    /// A project whose main records no main workspace at all is reported, not skipped.
    #[tokio::test]
    async fn a_project_with_no_main_workspace_reports_it_as_the_main_outcome() {
        let fixture = Fixture::new("heal-no-main");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let heal = Arc::new(FakeHealSource::new(BTreeMap::from([(
            repo.clone(),
            vec![heal_workspace(&repo, "raven")],
        )])));
        let inventory = NativeGatewayInventory::with_heal_source(
            fixture.storage.clone(),
            heal as Arc<dyn HealSource>,
        );

        let outcomes = inventory.heal_all().await.expect("eager heal");

        assert!(matches!(
            &outcomes[0].main,
            Err(GatewayInventoryError::MissingMainWorkspace(named)) if *named == repo
        ));
        assert!(outcomes[0].sessions[0].mount.is_ok());
    }

    /// The always-mounted check names main's image and its mountpoint, so a finding can point at
    /// both the volume that should be mounted and the directory the user is looking at.
    #[tokio::test]
    async fn unmounted_mains_name_their_image_and_mountpoint() {
        let fixture = Fixture::with_checkout_layout("unmounted-mains", CheckoutLayout::DirectMount);
        let detached = RepoId::parse("acme/alpha").expect("repo alpha");
        let served = RepoId::parse("acme/beta").expect("repo beta");
        let source = Arc::new(FixtureSource::default());
        for (repo, mounted) in [(&detached, false), (&served, true)] {
            fixture.bind(repo);
            let (storage, kernel) = fixture.workspace(
                repo,
                WorkspaceName::new("main").expect("main"),
                "00000000000000000000000000000001",
                3,
                mounted,
                true,
            );
            let (mounts, mount_paths) = match kernel {
                Some((mount, path)) => (
                    vec![mount.clone()],
                    BTreeMap::from([(mount.volume_key, path)]),
                ),
                None => (
                    Vec::new(),
                    BTreeMap::from([(
                        storage.volume_key.clone(),
                        fixture.root.join(format!("checkout-{}", repo.repo())),
                    )]),
                ),
            };
            source.projects.lock().expect("source").insert(
                repo.clone(),
                ProjectInventoryFacts {
                    storage: vec![storage],
                    mounts,
                    mount_paths,
                },
            );
        }
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        let unreachable = inventory.unmounted_mains().await.expect("main reachability");

        let layout = StorageLayout::new(fixture.storage.store(), &detached).expect("layout");
        let image = layout
            .main_image(ImageFormat::Sparse)
            .expect("main image")
            .image()
            .to_owned();
        assert_eq!(
            unreachable,
            vec![UnreachableMain {
                repo_id: detached,
                image,
                mountpoint: fixture.root.join("checkout-alpha"),
                reason: String::from("main's volume is not mounted"),
            }],
            "only the project whose main is detached is reported"
        );
    }
}
