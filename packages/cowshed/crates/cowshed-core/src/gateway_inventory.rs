use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cowshed_gateway_types::StartupHeal;
use thiserror::Error;

use crate::apfs::SystemCommandRunner;
#[cfg(test)]
use crate::api::dto::WorkspaceState;
use crate::api::dto::{ProjectWorkspaces, WorkspaceInfo};
use crate::metadata::{
    DetachedWorkspaceMetadata, GrantSet, PortBlock, PublicationState, ReservedPortBlocks,
    WorkspaceIncarnation, WorkspaceName, sidecar_path,
};
use crate::repository::{OwnedRepoIds, RepoId, RepositoryBinding};
use crate::storage::apfs::native::{
    KernelMountSnapshot, KernelMountSource, MacOsApfsExecutionHost, SystemKernelMountSource,
};
use crate::storage::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrate, ApfsSubstrateConfig,
};
use crate::storage::bootstrap::ValidatedHostStorage;
use crate::storage::lifecycle::{
    CheckpointFact, DerivationError, KernelMountFact, LifecycleWorkspace, MountIntent, MountState,
    StorageFact, Substrate, derive_workspaces,
};
use crate::storage::{StorageLayout, verify_no_symlinks};
use crate::workspace_credentials::{
    GatewayWorkspaceCredentials, WorkspaceCredentialError, read_gateway_workspace_credentials,
};

pub(crate) const MAX_BINDING_BYTES: u64 = 1024 * 1024;
const UNRESOLVED_CHECKOUT_PATH: &str = ".unresolved-main-mount";
/// The span every startup heal step reports under.
const HEAL_SCOPE: &str = "startup-heal";

/// How far the daemon's startup pass has got, as the requests it answers meanwhile see it
/// (05_gateway.md "Startup contract"). It only moves forward: fewer projects left to mount, then
/// restoring sessions, then done.
#[derive(Debug)]
pub struct StartupHealState(std::sync::Mutex<Option<StartupHeal>>);

impl StartupHealState {
    /// A pass with `projects` recorded projects to mount; with none, it only restores sessions.
    pub fn mounting(projects: usize) -> Self {
        Self(std::sync::Mutex::new(Some(
            match std::num::NonZeroUsize::new(projects) {
                Some(projects) => StartupHeal::Mounting { projects },
                None => StartupHeal::RestoringSessions,
            },
        )))
    }

    /// No pass in flight: everything a daemon serves is already healed.
    pub fn healed() -> Self {
        Self(std::sync::Mutex::new(None))
    }

    pub fn current(&self) -> Option<StartupHeal> {
        *self.lock()
    }

    /// One more project is mounted, or reported as unmountable. [`NativeGatewayInventory::heal`]
    /// calls this once per project it was given, which is the count [`Self::mounting`] started
    /// from, so it never runs past `Mounting`.
    fn project_mounted(&self) {
        let mut state = self.lock();
        if let Some(StartupHeal::Mounting { projects }) = *state {
            *state = Some(match std::num::NonZeroUsize::new(projects.get() - 1) {
                Some(projects) => StartupHeal::Mounting { projects },
                None => StartupHeal::RestoringSessions,
            });
        }
    }

    /// The sessions are restored from what the pass mounted: the pass is over.
    pub fn restored(&self) {
        *self.lock() = None;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<StartupHeal>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

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

/// One registry traversal classifies each project as adopted, checkout-less, or unreadable.
struct ProjectRegistryScan {
    projects: Vec<AdoptedProject>,
    checkoutless: Vec<RepoId>,
    issues: Vec<(RepoId, GatewayInventoryError)>,
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
    #[error(
        "repository identity {repo_id} is claimed by two adopted projects, {first} and {second}"
    )]
    AmbiguousIdentity {
        repo_id: RepoId,
        first: RepoId,
        second: RepoId,
    },
    #[error("macOS port block {claimed} shares ports with {held}, assigned to another workspace")]
    OverlappingPortBlocks { held: PortBlock, claimed: PortBlock },
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

/// One project's complete substrate reading, as one value.
///
/// Every field is a fact the derivation needs, and the derivation takes all of them. Checkpoints
/// are here rather than fetched at each use site because the store-wide listing and the
/// project-scoped listing report the same `checkpoints` field: sourcing them separately is exactly
/// how one of the two came to hand `derive_workspaces` an empty list and report "no checkpoint
/// exists" over a pinned one.
#[derive(Clone)]
struct ProjectInventoryFacts {
    storage: Vec<StorageFact>,
    mounts: Vec<KernelMountFact>,
    checkpoints: Vec<CheckpointFact>,
    mount_paths: BTreeMap<String, PathBuf>,
}

struct DerivedInventory {
    layout: StorageLayout,
    mount_paths: BTreeMap<String, PathBuf>,
    derived: Vec<crate::storage::lifecycle::DerivedWorkspace>,
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
        let config = project_substrate_config(storage, checkout_path);
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
        let checkpoints = host.checkpoints(repo)?;
        Ok(ProjectInventoryFacts {
            storage: storage_facts,
            mounts,
            checkpoints,
            mount_paths,
        })
    }
}

/// The substrate configuration for one project checked out at `checkout_path`.
///
/// One builder for both sides of the inventory: the read-only fact pass and eager heal have to
/// agree about where every workspace of a project mounts, and a second copy of this derivation is
/// how they would stop agreeing.
fn project_substrate_config(
    storage: &ValidatedHostStorage,
    checkout_path: PathBuf,
) -> ApfsSubstrateConfig {
    ApfsSubstrateConfig::new(storage.store(), checkout_path)
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
        let config = project_substrate_config(storage, checkout_path);
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

/// Holds `block` among the store's reserved blocks, refusing one that shares a port with a block
/// another workspace holds: blocks of different sizes are compared by the ports they cover.
fn reserve_port_block(
    blocks: &mut ReservedPortBlocks,
    block: PortBlock,
) -> Result<(), GatewayInventoryError> {
    blocks
        .insert(block)
        .map_err(|held| GatewayInventoryError::OverlappingPortBlocks {
            held,
            claimed: block,
        })
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

    /// Adopted projects, unmounted mains, bound projects without a checkout, and individual
    /// unreadable projects. One bad project's identity never hides another project's diagnosis.
    pub async fn doctor_projects(
        &self,
    ) -> Result<
        (
            Vec<AdoptedProject>,
            Vec<UnreachableMain>,
            Vec<RepoId>,
            Vec<(RepoId, GatewayInventoryError)>,
        ),
        GatewayInventoryError,
    > {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let scan = inventory.adopted_and_checkoutless_blocking()?;
            let unreachable = inventory.unmounted_mains_for(&scan.projects)?;
            Ok((scan.projects, unreachable, scan.checkoutless, scan.issues))
        })
        .await
        .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    /// Enumerate every current workspace directly from the host storage registry.
    ///
    /// Unlike a project runtime open, this never discovers a Git repository or reads remotes from
    /// the recorded checkout path. The image and detached sidecar identify each workspace; the
    /// captured kernel mount inventory supplies only its attached/detached state. That distinction
    /// is what lets a detached direct-mounted main remain listable after its old checkout path has
    /// disappeared.
    pub async fn all_projects(&self) -> Result<Vec<ProjectWorkspaces>, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || inventory.all_projects_blocking())
            .await
            .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    fn all_projects_blocking(&self) -> Result<Vec<ProjectWorkspaces>, GatewayInventoryError> {
        let repositories = discover_repositories(self.storage.store())?;
        let mut projects = Vec::with_capacity(repositories.len());
        for repo_id in repositories {
            match self.load_project_workspaces(&repo_id) {
                Ok(project) => projects.push(project),
                Err(
                    error @ (GatewayInventoryError::DuplicateRepository(_)
                    | GatewayInventoryError::OverlappingPortBlocks { .. }
                    | GatewayInventoryError::AmbiguousProjectRoot(_)
                    | GatewayInventoryError::ForeignBinding { .. }),
                ) => return Err(error),
                Err(error) => {
                    eprintln!(
                        "cowshed: skipping {repo_id}: its workspace records could not be read: {error}"
                    );
                }
            }
        }
        projects.sort_by(|left, right| left.repo_id.cmp(&right.repo_id));
        Ok(projects)
    }

    fn adopted_projects_blocking(&self) -> Result<Vec<AdoptedProject>, GatewayInventoryError> {
        let scan = self.adopted_and_checkoutless_blocking()?;
        for (repo_id, error) in scan.issues {
            eprintln!("cowshed: skipping project {repo_id}: {error}");
        }
        Ok(scan.projects)
    }

    /// One registry traversal for both the adopted list and the checkout-less remainder.
    ///
    /// Besides avoiding redundant I/O, a single pass guarantees one verdict per registry
    /// entry rather than one per derived view: a project is adopted, checkout-less, or an
    /// error, never two of those at once.
    fn adopted_and_checkoutless_blocking(
        &self,
    ) -> Result<ProjectRegistryScan, GatewayInventoryError> {
        let mut scan = ProjectRegistryScan {
            projects: Vec::new(),
            checkoutless: Vec::new(),
            issues: Vec::new(),
        };
        for repo_id in discover_repositories(self.storage.store())? {
            let layout = match StorageLayout::new(self.storage.store(), &repo_id) {
                Ok(layout) => layout,
                Err(error) => {
                    scan.issues.push((
                        repo_id,
                        GatewayInventoryError::InvalidMetadata {
                            path: self.storage.store().to_owned(),
                            message: error.to_string(),
                        },
                    ));
                    continue;
                }
            };
            match authoritative_checkout_path(&layout, &repo_id) {
                Ok(Some(project_root)) => scan.projects.push(AdoptedProject {
                    repo_id,
                    project_root,
                }),
                // No adopted checkout path means no adopted project, but the binding is still
                // real: the entry is reported for `doctor` rather than printed on stderr from
                // a library traversal, where it polluted command output.
                Ok(None) => scan.checkoutless.push(repo_id),
                Err(error) => scan.issues.push((repo_id, error)),
            }
        }
        Ok(scan)
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

    /// The recorded projects a startup heal works through, in inventory order.
    ///
    /// Taken apart from [`Self::heal`] so the daemon knows how many there are before it answers
    /// anyone: its first status already says how many it is still mounting.
    pub async fn recorded_projects(&self) -> Result<Vec<RepoId>, GatewayInventoryError> {
        let store = self.storage.store().to_owned();
        crate::timing::timed_async(HEAL_SCOPE, "discover", async move {
            crate::storage::lifecycle::dispatch_blocking(move || discover_repositories(&store))
                .await
                .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
        })
        .await
    }

    /// Attach and mount every one of `repositories`' workspaces, mains before sessions, telling
    /// `progress` as each project is done.
    ///
    /// This runs at gateway startup because the gateway is `RunAtLoad` and a reboot is the one
    /// window adoption's "the checkout path is never absent and never dangling" guarantee cannot
    /// defend on its own. Healing on contact would leave a dangling symlink — or, under direct
    /// mount, a bare stub directory — visible in the user's shell, editor, and Finder until
    /// something happened to touch it.
    ///
    /// Mains go first across every project, not per project in inventory order: a main is the
    /// user's own checkout and is always-mounted (02_workspaces.md), so no project's session
    /// mount — which may attach, fsck, and mount a multi-gigabyte image — is allowed to stand
    /// between another project's checkout and its mount. A project is done once its sessions
    /// are, so `progress` counts down in the session pass.
    ///
    /// Every step is a lifecycle span of its own (`startup-heal open|mount <repo>[/<workspace>]`),
    /// so a slow heal reads as the step that spent the time.
    ///
    /// Failures are per-project and returned rather than raised: one project whose store or image
    /// cannot be healed must not cost every other project its gateway. A project that cannot even
    /// be opened reports that error as its main outcome, because an unopenable project is exactly a
    /// project whose main is unreachable.
    pub async fn heal(
        &self,
        repositories: Vec<RepoId>,
        progress: &StartupHealState,
    ) -> Vec<ProjectHealOutcome> {
        // Deferred-first: images whose disk child hit the deadline on the last pass heal
        // before the rest, so a deferred workspace is retried first rather than in
        // inventory order. Empty most passes, in which case every order below is untouched.
        let deferred = crate::apfs::take_deferred_images();
        let store_root = self.storage.store().to_owned();
        let was_deferred = |repo: &RepoId, workspace: &LifecycleWorkspace| {
            let Ok(layout) = StorageLayout::new(&store_root, repo) else {
                return false;
            };
            let Ok(paths) = canonical_image_paths(&layout, workspace) else {
                return false;
            };
            deferred
                .iter()
                .any(|candidate| candidate.as_path() == paths.image())
        };
        let mut opened = Vec::with_capacity(repositories.len());
        for repo in repositories {
            let project = crate::timing::timed_async(
                HEAL_SCOPE,
                format!("open {repo}"),
                self.open_project(&repo),
            )
            .await;
            opened.push((repo, project));
        }
        let mut healed_mains = Vec::with_capacity(opened.len());
        if !deferred.is_empty() {
            opened.sort_by_cached_key(|(repo, project)| {
                !project
                    .as_ref()
                    .ok()
                    .and_then(|open| open.main.as_ref())
                    .is_some_and(|main| was_deferred(repo, main))
            });
        }
        for (repo_id, project) in opened {
            let (project, main) = match project {
                Ok(project) => {
                    let main = match &project.main {
                        Some(main) => {
                            crate::timing::timed_async(
                                HEAL_SCOPE,
                                format!("mount {repo_id}/{}", main.name()),
                                project.mounts.mount(main),
                            )
                            .await
                        }
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
                let mut ordered: Vec<&LifecycleWorkspace> = project.sessions.iter().collect();
                if !deferred.is_empty() {
                    ordered
                        .sort_by_cached_key(|&workspace| !was_deferred(&healed.repo_id, workspace));
                }
                for workspace in ordered {
                    sessions.push(SessionHealOutcome {
                        workspace: workspace.name().clone(),
                        mount: crate::timing::timed_async(
                            HEAL_SCOPE,
                            format!("mount {}/{}", healed.repo_id, workspace.name()),
                            project.mounts.mount(workspace),
                        )
                        .await,
                    });
                }
            }
            outcomes.push(ProjectHealOutcome {
                repo_id: healed.repo_id,
                main: healed.main,
                sessions,
            });
            progress.project_mounted();
        }
        outcomes
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
        let projects = self.adopted_projects_blocking()?;
        self.unmounted_mains_for(&projects)
    }

    fn unmounted_mains_for(
        &self,
        projects: &[AdoptedProject],
    ) -> Result<Vec<UnreachableMain>, GatewayInventoryError> {
        let mut unreachable = Vec::new();
        for project in projects {
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
            let mountpoint = workspace_mountpoint(&layout, &project.project_root, &main)?;
            let reason = match self.source.project_facts(&self.storage, &project.repo_id) {
                Ok(facts) => main_mount_defect(facts)?,
                Err(error) => Some(error.to_string()),
            };
            if let Some(reason) = reason {
                unreachable.push(UnreachableMain {
                    repo_id: project.repo_id.clone(),
                    image,
                    mountpoint,
                    reason,
                });
            }
        }
        Ok(unreachable)
    }

    pub async fn all_reserved_port_blocks(
        &self,
    ) -> Result<ReservedPortBlocks, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            inventory.all_reserved_port_blocks_blocking()
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
            let image = layout
                .main_image()
                .map_err(|error| GatewayInventoryError::InvalidMetadata {
                    path: layout.project().project_root.clone(),
                    message: error.to_string(),
                })?
                .image()
                .to_owned();
            if fs::symlink_metadata(&image).is_ok_and(|metadata| metadata.file_type().is_file()) {
                images.push(image);
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
                            && crate::metadata::is_image_path(&path)
                            && fs::symlink_metadata(&path)
                                .is_ok_and(|metadata| metadata.file_type().is_file())
                            // A retired image without its sidecar has no record left to name a
                            // checkout; it is bytes awaiting reclaim, not a claim on the root.
                            && !fs::symlink_metadata(sidecar_path(&path))
                                .is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
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
            // Main's images name the checkout while any exists. Removing main takes them away, and
            // it records the root in the store first, so a project still bound afterwards is found
            // from its checkout through that record — and only then, so the record never outvotes
            // an image.
            let claims_root = if images.is_empty() {
                layout
                    .recorded_checkout_root()
                    .map_err(|error| GatewayInventoryError::InvalidMetadata {
                        path: layout.project().checkout_root.clone(),
                        message: error.to_string(),
                    })?
                    .and_then(|root| fs::canonicalize(root).ok())
                    .is_some_and(|root| root == expected)
            } else {
                images.into_iter().try_fold(false, |claimed, image| {
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
                    let matches = fs::canonicalize(&metadata.info_snapshot.project_root)
                        .is_ok_and(|root| root == expected);
                    Ok::<_, GatewayInventoryError>(claimed || matches)
                })?
            };
            if claims_root && matched.replace(repo).is_some() {
                return Err(GatewayInventoryError::AmbiguousProjectRoot(
                    project_root.to_owned(),
                ));
            }
        }
        Ok(matched)
    }

    fn all_reserved_port_blocks_blocking(
        &self,
    ) -> Result<ReservedPortBlocks, GatewayInventoryError> {
        let repositories = discover_repositories(self.storage.store())?;
        let mut blocks = ReservedPortBlocks::default();
        for repo in repositories {
            let authoritative = self.source.project_facts(&self.storage, &repo)?;
            let layout = StorageLayout::new(self.storage.store(), &repo).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: self.storage.store().to_owned(),
                    message: error.to_string(),
                }
            })?;
            let active_names = authoritative
                .storage
                .iter()
                .map(|fact| fact.workspace.name())
                .collect::<BTreeSet<_>>();
            for fact in &authoritative.storage {
                let image = canonical_image_paths(&layout, &fact.workspace)?;
                let metadata =
                    read_current_metadata(self.storage.store(), image.image(), &fact.workspace)?;
                let block = metadata.grants.port_block.ok_or_else(|| {
                    GatewayInventoryError::MissingPortBlock {
                        repo: repo.clone(),
                        workspace: fact.workspace.name().clone(),
                    }
                })?;
                reserve_port_block(&mut blocks, block)?;
                for retained in metadata.grants.retained_port_blocks {
                    reserve_port_block(&mut blocks, retained)?;
                }
            }
            // Incomplete clones cannot be served, but a canonical pending payload owns its
            // stored grant even after its creator exits. Sidecar-only fences are reclaimed
            // during recovery; until the payload exists, the live PID marker protects the claim.
            if !active_names.contains(&WorkspaceName::main())
                && let Some(image) = existing_main_image(&layout)?
            {
                Self::reserve_pending_port_block(
                    self.storage.store(),
                    &repo,
                    &WorkspaceName::main(),
                    &image,
                    &mut blocks,
                )?;
            }
            let sessions = &layout.project().sessions;
            let entries = match fs::read_dir(sessions) {
                Ok(entries) => entries
                    .map(|entry| {
                        entry.map(|entry| entry.path()).map_err(|source| {
                            io_error("enumerating session images", sessions, source)
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
                Err(source) => {
                    return Err(io_error("enumerating session images", sessions, source));
                }
            };
            for image in crate::storage::discover_session_images(entries) {
                // Published images were already validated above. Only pending sidecars
                // need reading after the canonical directory enumeration.
                if !active_names.contains(image.workspace()) {
                    Self::reserve_pending_port_block(
                        self.storage.store(),
                        &repo,
                        image.workspace(),
                        image.path(),
                        &mut blocks,
                    )?;
                }
            }
        }
        Ok(blocks)
    }

    fn reserve_pending_port_block(
        store_root: &Path,
        repo: &RepoId,
        workspace: &WorkspaceName,
        image: &Path,
        blocks: &mut ReservedPortBlocks,
    ) -> Result<(), GatewayInventoryError> {
        verify_no_symlinks(store_root, image).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: image.to_owned(),
                message: error.to_string(),
            }
        })?;
        let sidecar = sidecar_path(image);
        verify_no_symlinks(store_root, &sidecar).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: sidecar.clone(),
                message: error.to_string(),
            }
        })?;
        if !workspace.is_main()
            && !sidecar
                .try_exists()
                .map_err(|source| io_error("inspect session metadata", &sidecar, source))?
        {
            eprintln!(
                "cowshed: skipping orphan session image {}: missing {}",
                image.display(),
                sidecar.display()
            );
            return Ok(());
        }
        let metadata = DetachedWorkspaceMetadata::read_for_image(image).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: sidecar.clone(),
                message: error.to_string(),
            }
        })?;
        if metadata.repo_id != *repo || metadata.workspace != *workspace {
            return Err(GatewayInventoryError::InvalidMetadata {
                path: sidecar,
                message: "canonical pending metadata identity does not match its image".to_owned(),
            });
        }
        if metadata.publication_state != PublicationState::PendingFence {
            return Err(GatewayInventoryError::InvalidMetadata {
                path: sidecar,
                message: "canonical image was not present in the published inventory".to_owned(),
            });
        }
        let block =
            metadata
                .grants
                .port_block
                .ok_or_else(|| GatewayInventoryError::MissingPortBlock {
                    repo: repo.clone(),
                    workspace: workspace.clone(),
                })?;
        reserve_port_block(blocks, block)?;
        for retained in metadata.grants.retained_port_blocks {
            reserve_port_block(blocks, retained)?;
        }
        Ok(())
    }

    fn all_attached_blocking(&self) -> Result<Vec<GatewaySessionFact>, GatewayInventoryError> {
        let repositories = discover_repositories(self.storage.store())?;
        let mut facts = Vec::new();
        let mut port_blocks = ReservedPortBlocks::default();
        for repo in repositories {
            match self.load_project(&repo) {
                Ok(project) => {
                    for fact in &project {
                        reserve_port_block(&mut port_blocks, fact.port_block)?;
                        for retained in &fact.grants.retained_port_blocks {
                            reserve_port_block(&mut port_blocks, *retained)?;
                        }
                    }
                    facts.extend(project);
                }
                Err(error) => {
                    // Store-wide identity collisions stay fatal. A single project's
                    // mount/metadata mismatch is a doctor finding and must not take
                    // the RunAtLoad gateway down with it.
                    if matches!(
                        error,
                        GatewayInventoryError::DuplicateRepository(_)
                            | GatewayInventoryError::OverlappingPortBlocks { .. }
                            | GatewayInventoryError::AmbiguousProjectRoot(_)
                            | GatewayInventoryError::ForeignBinding { .. }
                    ) {
                        return Err(error);
                    }
                    eprintln!(
                        "cowshed: skipping {repo}: its workspace records could not be read: {error}"
                    );
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

    fn load_derived(&self, repo_id: &RepoId) -> Result<DerivedInventory, GatewayInventoryError> {
        let authoritative = self.source.project_facts(&self.storage, repo_id)?;
        reject_duplicate_mount_facts(&authoritative.mounts)?;
        let derived = derive_workspaces(
            authoritative.storage,
            authoritative.mounts,
            authoritative.checkpoints,
        )?;
        let layout = StorageLayout::new(self.storage.store(), repo_id).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: self.storage.store().to_owned(),
                message: error.to_string(),
            }
        })?;
        Ok(DerivedInventory {
            layout,
            mount_paths: authoritative.mount_paths,
            derived,
        })
    }

    fn load_project_workspaces(
        &self,
        repo_id: &RepoId,
    ) -> Result<ProjectWorkspaces, GatewayInventoryError> {
        let DerivedInventory {
            layout,
            mount_paths,
            derived,
        } = self.load_derived(repo_id)?;
        let mut workspaces = Vec::with_capacity(derived.len());
        for workspace in derived {
            let volume = crate::storage::apfs::volume_key(repo_id, workspace.workspace.name());
            let mount =
                mount_paths
                    .get(&volume)
                    .ok_or_else(|| GatewayInventoryError::InvalidMetadata {
                        path: layout.project().project_root.clone(),
                        message: format!("missing canonical mount path for {volume}"),
                    })?;
            let image_paths = canonical_image_paths(&layout, &workspace.workspace)?;
            let metadata = read_current_metadata(
                self.storage.store(),
                image_paths.image(),
                &workspace.workspace,
            )?;
            let info = WorkspaceInfo::from_current_metadata(&workspace, mount.clone(), &metadata)
                .map_err(|error| GatewayInventoryError::InvalidMetadata {
                path: sidecar_path(image_paths.image()),
                message: error.to_string(),
            })?;
            workspaces.push(info);
        }
        workspaces.sort_by(|left, right| left.workspace.cmp(&right.workspace));
        Ok(ProjectWorkspaces {
            repo_id: repo_id.clone(),
            workspaces,
        })
    }
    fn load_project(
        &self,
        repo_id: &RepoId,
    ) -> Result<Vec<GatewaySessionFact>, GatewayInventoryError> {
        let DerivedInventory {
            layout,
            mount_paths,
            derived,
        } = self.load_derived(repo_id)?;
        // Read once per project: the certificate subject inside a workspace image names whichever
        // identity was current when the image was minted, which after an identity change is one of
        // the project's former identities.
        let owned_repo_ids = load_binding_candidate(
            self.storage.store(),
            &layout.project().project_root,
            &layout.project().repository_binding,
        )?;
        // The project's standing grants join every workspace's own, read once per project so all
        // of its sessions are derived from one policy. A policy that cannot be read completely
        // serves no session of this project rather than a session without it.
        let project_policy = &layout.project().policy;
        let project_grants = crate::project_policy::ProjectPolicy::read(project_policy)
            .map_err(|error| GatewayInventoryError::InvalidMetadata {
                path: project_policy.clone(),
                message: error.to_string(),
            })?
            .grants;
        let mut facts = Vec::new();
        for workspace in derived {
            let MountState::Mounted { mount_id } = workspace.mount_state else {
                continue;
            };
            let volume = crate::storage::apfs::volume_key(repo_id, workspace.workspace.name());
            let mount =
                mount_paths
                    .get(&volume)
                    .ok_or_else(|| GatewayInventoryError::InvalidMetadata {
                        path: layout.project().project_root.clone(),
                        message: format!("missing canonical mount path for {volume}"),
                    })?;
            let image_paths = canonical_image_paths(&layout, &workspace.workspace)?;
            let metadata = read_current_metadata(
                self.storage.store(),
                image_paths.image(),
                &workspace.workspace,
            )?;
            // The mount path is not re-derived here. `mount_paths` is already the expectation
            // (`expected_mount_paths`) — main at the adopted checkout, every other workspace under
            // `mnt/<owner>/<repo>/` — and `ApfsExecutionHost::mounts` only reports a volume as
            // mounted when the kernel has it at exactly that path.
            let port_block = metadata.grants.port_block.ok_or_else(|| {
                GatewayInventoryError::MissingPortBlock {
                    repo: repo_id.clone(),
                    workspace: workspace.workspace.name().clone(),
                }
            })?;
            let credentials = read_gateway_workspace_credentials(
                &owned_repo_ids,
                &workspace.workspace,
                mount,
                image_paths.ca_private_key(),
            )?;
            let grants = crate::project_policy::effective_grants(&metadata.grants, &project_grants)
                .map_err(|error| GatewayInventoryError::InvalidMetadata {
                    path: project_policy.clone(),
                    message: error.to_string(),
                })?;
            facts.push(GatewaySessionFact {
                repo_id: repo_id.clone(),
                workspace: workspace.workspace.name().clone(),
                incarnation: workspace.workspace.incarnation().clone(),
                revision: grants.revision,
                mount_id,
                mount: mount.clone(),
                grants,
                port_block,
                credentials,
            });
        }
        Ok(facts)
    }
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
    crate::storage::bootstrap::is_reserved_store_namespace(name)
        || fs::symlink_metadata(path.join(crate::storage::bootstrap::VOLUME_MARKER_FILE)).is_ok()
}

/// Enumerate the projects in the store, skipping everything that is not one.
///
/// Discovery is per-entry isolated for the same reason healing is (see `heal`): the store root
/// is a mount point with neighbours cowshed does not own and cannot read, and an entry that cannot
/// be inspected is evidence that it is not a project — not grounds to fail the pass. Raising here
/// took down the entire gateway inventory over one root-owned system directory, which left launchd
/// showing a running process while status and doctor reported nothing started and eager heal never
/// ran. A candidate that cannot be read is skipped; a candidate that reads as a real but broken
/// project still raises, because that is cowshed's own state and hiding it would hide corruption.
pub(crate) fn discover_repositories(
    store_root: &Path,
) -> Result<Vec<RepoId>, GatewayInventoryError> {
    Ok(discover_owned_identities(store_root)?
        .into_iter()
        .map(|owned| owned.current().clone())
        .collect())
}

/// The same scan, keeping every identity each live project owns rather than only its current one.
///
/// This is what "an identity is taken" means: a live project's binding either answers to it now or
/// records having answered to it. Retirement deletes `repository.json`, so a retired project drops
/// out of this scan entirely and its former identities stop being anybody's.
pub(crate) fn discover_owned_identities(
    store_root: &Path,
) -> Result<Vec<OwnedRepoIds>, GatewayInventoryError> {
    ensure_directory(store_root, "opening validated store root")?;
    let mut current: BTreeSet<RepoId> = BTreeSet::new();
    let mut owned = Vec::new();
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
            let binding_path = project.join(crate::repository::REPOSITORY_BINDING_FILE);
            // Unreadable is "not a project"; only a binding that is present and readable is held
            // to the project contract.
            if !binding_path_exists(&binding_path).unwrap_or(false) {
                continue;
            }
            let repo = load_binding_candidate(store_root, &project, &binding_path)?;
            if !current.insert(repo.current().clone()) {
                return Err(GatewayInventoryError::DuplicateRepository(
                    repo.current().clone(),
                ));
            }
            owned.push(repo);
        }
    }
    owned.sort_by(|left, right| left.current().cmp(right.current()));
    Ok(owned)
}

/// The live project that owns `repo_id` — as its current identity or as one it recorded holding.
///
/// `None` means the identity is free: no adopted project claims it, so adoption may take it and a
/// rename may move onto it. Two live projects claiming one identity is cowshed's own state gone
/// wrong, and it is raised here rather than from the store-wide scan so that the refusal lands on
/// the one operation that asked, instead of taking down the whole inventory.
///
/// The product caller is macOS-only, but the ownership rule is platform-neutral and its test
/// should keep running on every host. Test builds therefore retain the function while Linux
/// library builds omit a product path they cannot reach.
#[cfg(any(target_os = "macos", test))]
pub(crate) fn identity_owner(
    store_root: &Path,
    repo_id: &RepoId,
) -> Result<Option<OwnedRepoIds>, GatewayInventoryError> {
    let mut found: Option<OwnedRepoIds> = None;
    for owned in discover_owned_identities(store_root)? {
        if !owned.accepts(repo_id) {
            continue;
        }
        if let Some(first) = found {
            return Err(GatewayInventoryError::AmbiguousIdentity {
                repo_id: repo_id.clone(),
                first: first.current().clone(),
                second: owned.current().clone(),
            });
        }
        found = Some(owned);
    }
    Ok(found)
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
    if found.current() != repo_id {
        return Err(GatewayInventoryError::ForeignBinding {
            path: binding_path,
            expected: repo_id.clone(),
            actual: found.current().clone(),
        });
    }
    Ok(true)
}

fn load_binding_candidate(
    store_root: &Path,
    project_root: &Path,
    binding_path: &Path,
) -> Result<OwnedRepoIds, GatewayInventoryError> {
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
    let owned =
        binding
            .owned_repo_ids()
            .map_err(|error| GatewayInventoryError::InvalidBinding {
                path: binding_path.to_owned(),
                message: error.to_string(),
            })?;
    let actual = owned.current().clone();
    let expected_layout = StorageLayout::new(store_root, &actual).map_err(|error| {
        GatewayInventoryError::InvalidBinding {
            path: binding_path.to_owned(),
            message: error.to_string(),
        }
    })?;
    let expected_paths = expected_layout.project();
    if expected_paths.project_root != project_root {
        let expected = project_root_identity(project_root).unwrap_or_else(|| actual.clone());
        return Err(GatewayInventoryError::ForeignBinding {
            path: binding_path.to_owned(),
            expected,
            actual,
        });
    }
    Ok(owned)
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

pub(crate) fn read_typed_json_nofollow<T: serde::de::DeserializeOwned>(
    path: &Path,
    maximum: u64,
) -> Result<T, String> {
    let bytes = read_bytes_nofollow(path, maximum)?;
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

/// Bounded private-file admission shared by typed controller metadata and generated Git policy.
pub(crate) fn read_bytes_nofollow(path: &Path, maximum: u64) -> Result<Vec<u8>, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        // Regular files ignore O_NONBLOCK; a substituted FIFO must not block before fstat.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|error| error.to_string())?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.file_type().is_file() || metadata.len() > maximum {
        return Err("private file is not regular or exceeds its size bound".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.uid() != crate::gateway_sessions::effective_uid()
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err("private file is not controller-owned mode 0600".to_owned());
        }
    }
    let capacity = usize::try_from(metadata.len()).map_err(|error| error.to_string())?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(maximum + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > maximum {
        return Err("private file exceeds its size bound".to_owned());
    }
    Ok(bytes)
}

pub(crate) fn authoritative_checkout_path(
    layout: &StorageLayout,
    repo: &RepoId,
) -> Result<Option<PathBuf>, GatewayInventoryError> {
    let Some(image) = existing_main_image(layout)? else {
        return Ok(None);
    };
    let metadata = DetachedWorkspaceMetadata::read_for_image(&image).map_err(|error| {
        GatewayInventoryError::InvalidMetadata {
            path: sidecar_path(&image),
            message: error.to_string(),
        }
    })?;
    if metadata.repo_id != *repo || !metadata.workspace.is_main() {
        return Err(GatewayInventoryError::InvalidMetadata {
            path: sidecar_path(&image),
            message: "canonical main metadata identity mismatch".to_owned(),
        });
    }
    if metadata.publication_state != PublicationState::Active {
        return Ok(None);
    }
    Ok(Some(metadata.info_snapshot.project_root))
}

fn expected_mount_paths(
    config: &ApfsSubstrateConfig,
    layout: &StorageLayout,
    storage: &[StorageFact],
) -> Result<BTreeMap<String, PathBuf>, GatewayInventoryError> {
    let mut paths = BTreeMap::new();
    for fact in storage {
        let mount = workspace_mountpoint(layout, &config.checkout_path, fact.workspace.name())?;
        paths.insert(fact.volume_key.clone(), mount);
    }
    Ok(paths)
}

/// Where one workspace of one project mounts: main at the adopted checkout, every other workspace
/// under `mnt/`. One rule, because the read-only fact pass and the always-mounted check both need
/// it and a project whose two answers disagreed would be reported as broken by whichever
/// derivation ran second.
fn workspace_mountpoint(
    layout: &StorageLayout,
    checkout_path: &Path,
    workspace: &WorkspaceName,
) -> Result<PathBuf, GatewayInventoryError> {
    layout
        .main_aware_workspace_mount(checkout_path, workspace)
        .map_err(|error| GatewayInventoryError::InvalidMetadata {
            path: layout.project().mount_root.clone(),
            message: error.to_string(),
        })
}

/// The canonical main image this project holds, if it holds one.
fn existing_main_image(layout: &StorageLayout) -> Result<Option<PathBuf>, GatewayInventoryError> {
    let paths = layout
        .main_image()
        .map_err(|error| GatewayInventoryError::InvalidMetadata {
            path: layout.project().project_root.clone(),
            message: error.to_string(),
        })?;
    if paths
        .image()
        .try_exists()
        .map_err(|source| io_error("inspecting canonical main image", paths.image(), source))?
    {
        Ok(Some(paths.image().to_owned()))
    } else {
        Ok(None)
    }
}

/// Why this project's main is not mounted, or `None` when it is.
///
/// A project whose facts hold no main at all is reported rather than passed over: the always-
/// mounted invariant is about the checkout the user sees, and a store that records no main for an
/// adopted project cannot be serving one.
fn main_mount_defect(
    facts: ProjectInventoryFacts,
) -> Result<Option<String>, GatewayInventoryError> {
    let derived = derive_workspaces(facts.storage, facts.mounts, facts.checkpoints)?;
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
    let result = layout.canonical_image(workspace.name());
    result.map_err(|error| GatewayInventoryError::InvalidMetadata {
        path: layout.project().project_root.clone(),
        message: error.to_string(),
    })
}

/// Read one sidecar without following links and prove it describes the exact active lifecycle fact.
///
/// This is the only admission path for projecting store metadata into `WorkspaceInfo`; callers
/// outside inventory use it rather than weakening the identity or publication-state checks.
pub fn read_current_metadata(
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
    use std::cell::Cell;
    use std::sync::Mutex;

    use crate::metadata::{
        MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, Platform, SIDECAR_VERSION, WorkspaceInfoSnapshot,
        WorkspaceRole, write_json,
    };
    use crate::repository::{BoundIdentity, RepositoryBinding};
    use crate::storage::CheckpointLabel;
    use crate::storage::bootstrap::CanonicalRoots;
    use crate::storage::lifecycle::{LifecycleWorkspace, Pin, Revision};
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

    /// How the fixture assigns macOS port blocks.
    ///
    /// A fixture that gave every workspace base 40960 made the store-wide disjointness invariant
    /// unobservable: the second workspace in any store was already a collision, so no test could
    /// tell a healthy multi-project store from a broken one. `Grid` is the healthy host, walking
    /// the new-size grid the allocator walks; `Planted` hands the next workspace a chosen block
    /// and then resumes the grid, the fault on purpose.
    #[derive(Clone, Copy)]
    enum FixturePortBlocks {
        Grid(u16),
        Planted { block: PortBlock, then: u16 },
    }

    struct Fixture {
        root: PathBuf,
        storage: ValidatedHostStorage,
        port_blocks: Cell<FixturePortBlocks>,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "cowshed-gateway-inventory-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            let home = root.join("home");
            fs::create_dir_all(&home).expect("fixture home");
            let roots = CanonicalRoots::at(root.join("store"));
            fs::create_dir_all(roots.store()).expect("fixture store");
            fs::create_dir_all(roots.telemetry()).expect("fixture telemetry");
            Self {
                root,
                storage: ValidatedHostStorage::new(home, roots),
                port_blocks: Cell::new(FixturePortBlocks::Grid(MACOS_PORT_MIN)),
            }
        }

        /// This workspace's port block.
        fn take_port_block(&self) -> PortBlock {
            match self.port_blocks.get() {
                FixturePortBlocks::Grid(base) => {
                    self.port_blocks
                        .set(FixturePortBlocks::Grid(base + NEW_PORT_BLOCK_SIZE));
                    PortBlock::new(base, NEW_PORT_BLOCK_SIZE).expect("port block")
                }
                FixturePortBlocks::Planted { block, then } => {
                    self.port_blocks.set(FixturePortBlocks::Grid(then));
                    block
                }
            }
        }

        /// Give the next workspace `block`; the ones after it resume the grid.
        fn plant_port_block(&self, block: PortBlock) {
            let then = match self.port_blocks.get() {
                FixturePortBlocks::Grid(base) | FixturePortBlocks::Planted { then: base, .. } => {
                    base
                }
            };
            self.port_blocks
                .set(FixturePortBlocks::Planted { block, then });
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
        }

        fn workspace(
            &self,
            repo: &RepoId,
            name: WorkspaceName,
            incarnation: &str,
            revision: u64,
            mounted: bool,
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
            )
            .expect("workspace");
            let image = canonical_image_paths(&layout, &workspace).expect("image paths");
            fs::create_dir_all(image.image().parent().expect("image parent"))
                .expect("image parent");
            fs::write(image.image(), b"fixture").expect("image");
            let checkout = self.root.join(format!("checkout-{}", repo.repo()));
            if name.is_main() {
                fs::create_dir_all(&checkout).expect("adopted checkout");
            }
            // Same derivation production uses in `expected_mount_paths`: main mounts at the
            // checkout, every other workspace under `mnt/`.
            let mount = if name.is_main() {
                checkout.clone()
            } else {
                layout.workspace_mount(&name).expect("workspace mount")
            };
            let port_block = self.take_port_block();
            let mut grants = GrantSet::closed_baseline(Some(port_block)).expect("grants");
            grants.revision = revision;
            let info_snapshot = WorkspaceInfoSnapshot {
                project_root: if name.is_main() {
                    checkout.clone()
                } else {
                    mount.clone()
                },
                role,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: Some("main".to_owned()),
                created_at: "2026-07-14T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-14T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            };
            DetachedWorkspaceMetadata {
                version: SIDECAR_VERSION,
                repo_id: repo.clone(),
                workspace: name.clone(),
                workspace_incarnation: workspace.incarnation().clone(),
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
                    Platform::Macos,
                    Some(port_block),
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

    /// Identity uniqueness is scoped to *live* projects, which is the case a careless
    /// implementation breaks.
    ///
    /// The two halves pin opposite failures. Drop former identities from the membership check and
    /// the live half fails at the `merged` lookup:
    ///
    ///     panicked at gateway_inventory.rs: live project owns it
    ///
    /// Keep the identities in a registry that outlives the binding — anything other than reading
    /// them back out of `repository.json` — and the retired half fails instead, refusing to adopt a
    /// name whose only claimant no longer exists.
    #[test]
    fn a_retired_projects_former_identity_stops_blocking_adoption() {
        let fixture = Fixture::new("retired-former-identity");
        let monorepo = RepoId::parse("acme/monorepo").expect("monorepo");
        let merged = RepoId::parse("acme/widget").expect("merged repository");
        fixture.bind(&monorepo);
        let paths = StorageLayout::new(fixture.storage.store(), &monorepo)
            .expect("layout")
            .project()
            .clone();
        let binding: RepositoryBinding =
            crate::metadata::read_json(&paths.repository_binding).expect("binding");
        let merged_in = binding
            .rename_primary(merged.clone())
            .expect("adopt as the merged identity")
            .rename_primary(monorepo.clone())
            .expect("rename onto the monorepo identity");
        assert_eq!(merged_in.former_identities, vec![merged.clone()]);
        write_json(&paths.repository_binding, &merged_in).expect("republish binding");

        // While the monorepo is live it owns both names, so neither is free.
        for identity in [&monorepo, &merged] {
            let owner = identity_owner(fixture.storage.store(), identity)
                .expect("owner lookup")
                .expect("live project owns it");
            assert_eq!(owner.current(), &monorepo);
        }

        // Retirement deletes the binding, which is what makes the project stop existing. Its former
        // identities go with it.
        fs::remove_file(&paths.repository_binding).expect("retire the project");
        for identity in [&monorepo, &merged] {
            assert!(
                identity_owner(fixture.storage.store(), identity)
                    .expect("owner lookup")
                    .is_none(),
                "{identity} is free once the project holding it is retired"
            );
        }
    }

    /// Two adopted projects, each with one mounted main, plus a repository binding planted in
    /// every reserved store namespace so discovery has to skip them by name rather than by luck.
    fn two_project_store(fixture: &Fixture) -> (RepoId, RepoId, Arc<FixtureSource>) {
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
            );
            let (mount, path) = mounted.expect("mounted fixture");
            source.projects.lock().expect("source").insert(
                repo.clone(),
                ProjectInventoryFacts {
                    storage: vec![storage],
                    mounts: vec![mount.clone()],
                    checkpoints: Vec::new(),
                    mount_paths: BTreeMap::from([(mount.volume_key, path)]),
                },
            );
        }
        for namespace in crate::storage::bootstrap::RESERVED_STORE_NAMESPACES {
            let collision = fixture.storage.store().join(namespace).join("system-owned");
            fs::create_dir_all(&collision).expect("reserved namespace fixture");
            fs::write(
                collision.join("repository.json"),
                b"not a repository binding",
            )
            .expect("invalid reserved binding");
        }
        (repo_a, repo_b, source)
    }

    #[tokio::test]
    async fn attached_inventory_is_sorted_complete_and_secret_redacted() {
        let fixture = Fixture::new("attached");
        let (repo_a, _repo_b, source) = two_project_store(&fixture);
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
        // A healthy store reserves one block per workspace, so the scan reports both rather than
        // reporting a collision. This is the positive half of the disjointness invariant.
        assert_eq!(
            inventory
                .all_reserved_port_blocks()
                .await
                .expect("reserved port blocks")
                .blocks()
                .collect::<Vec<_>>(),
            [
                PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).expect("first block"),
                PortBlock::new(MACOS_PORT_MIN + NEW_PORT_BLOCK_SIZE, NEW_PORT_BLOCK_SIZE)
                    .expect("second block"),
            ]
        );
    }

    #[tokio::test]
    async fn orphan_session_image_does_not_block_store_wide_port_reservations() {
        let fixture = Fixture::new("orphan-ports");
        let (repo_a, _repo_b, source) = two_project_store(&fixture);
        let sessions = StorageLayout::new(fixture.storage.store(), &repo_a)
            .expect("layout")
            .project()
            .sessions
            .clone();
        fs::create_dir_all(&sessions).expect("sessions directory");
        fs::write(sessions.join("scratch.asif"), b"unpublished image").expect("orphan image");
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );
        assert_eq!(
            inventory
                .all_reserved_port_blocks()
                .await
                .expect("one orphan must not hide healthy projects")
                .blocks()
                .count(),
            2
        );
        fs::write(
            sidecar_path(&sessions.join("scratch.asif")),
            b"{\"invalid\": true}",
        )
        .expect("malformed sidecar");
        assert!(
            inventory.all_reserved_port_blocks().await.is_err(),
            "present but unreadable grants cannot silently free a reserved port block"
        );
    }

    #[tokio::test]
    async fn one_unreadable_project_does_not_hide_others_in_store_wide_listing() {
        let fixture = Fixture::new("unreadable-list");
        let (repo_a, repo_b, source) = two_project_store(&fixture);
        let main = StorageLayout::new(fixture.storage.store(), &repo_a)
            .expect("layout")
            .main_image()
            .expect("main paths");
        fs::write(main.sidecar(), b"{\"invalid\": true}").expect("unreadable main metadata");
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );
        let listed = inventory
            .all_projects()
            .await
            .expect("healthy project remains listable");
        assert_eq!(
            listed
                .iter()
                .map(|project| &project.repo_id)
                .collect::<Vec<_>>(),
            [&repo_b]
        );
        let adopted = inventory
            .adopted_projects()
            .await
            .expect("healthy project remains attachable");
        assert_eq!(
            adopted
                .iter()
                .map(|project| &project.repo_id)
                .collect::<Vec<_>>(),
            [&repo_b]
        );
        let (diagnosed, _, _, issues) = inventory
            .doctor_projects()
            .await
            .expect("doctor continues past a corrupt project");
        assert_eq!(
            diagnosed
                .iter()
                .map(|project| &project.repo_id)
                .collect::<Vec<_>>(),
            [&repo_b]
        );
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].0, repo_a);
        assert!(issues[0].1.to_string().contains("main.asif.grants.json"));
    }

    /// A project's standing grants reach every workspace's gateway session, on top of its own,
    /// and advance the session revision the gateway requires to grow; a sibling project's
    /// sessions are untouched.
    #[tokio::test]
    async fn project_grants_join_every_session_of_their_project_and_advance_its_revision() {
        let fixture = Fixture::new("project-grants");
        let (repo_a, _repo_b, source) = two_project_store(&fixture);
        let policy_path = StorageLayout::new(fixture.storage.store(), &repo_a)
            .expect("layout")
            .project()
            .policy
            .clone();
        crate::project_policy::ProjectPolicy {
            grants: crate::project_policy::ProjectGrants {
                revision: 2,
                read: vec![PathBuf::from("/opt/shared")],
                deny_write: Vec::new(),
                deny: Vec::new(),
                egress: vec![crate::metadata::EgressRule {
                    host: "registry.example.test".to_owned(),
                    ports: Vec::new(),
                    mode: crate::metadata::EgressMode::Intercept,
                }],
            },
            ..crate::project_policy::ProjectPolicy::default()
        }
        .write(&policy_path)
        .expect("project policy");
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        let facts = inventory.all_attached().await.expect("attached inventory");
        let alpha = facts
            .iter()
            .find(|fact| fact.repo_id == repo_a)
            .expect("alpha session");
        assert_eq!(alpha.revision, 9);
        assert_eq!(alpha.grants.revision, 9);
        assert_eq!(alpha.grants.read, [PathBuf::from("/opt/shared")]);
        assert_eq!(
            alpha
                .grants
                .egress
                .iter()
                .map(|rule| rule.host.as_str())
                .collect::<Vec<_>>(),
            ["registry.example.test"]
        );
        let beta = facts
            .iter()
            .find(|fact| fact.repo_id != repo_a)
            .expect("beta session");
        assert_eq!(beta.revision, 7);
        assert!(beta.grants.egress.is_empty());

        // A policy the controller cannot read completely serves no session of its project, and
        // costs no other project its sessions.
        fs::write(
            &policy_path,
            br#"{ "grants": { "write": ["/opt/shared"] } }"#,
        )
        .expect("bad");
        assert!(inventory.project_attached(&repo_a).await.is_err());
        let facts = inventory
            .all_attached()
            .await
            .expect("other projects still served");
        assert!(facts.iter().all(|fact| fact.repo_id != repo_a));
        assert_eq!(facts.len(), 1);
    }

    /// A macOS port belongs to at most one workspace host-wide: a block's base is the workspace's
    /// gateway endpoint and its other ports are the workspace's own, so two workspaces whose
    /// blocks share a port — the same block, or a live 16-port block inside another's 64 —
    /// answer on one port. Both readers of the store refuse — the allocator's scan, and the
    /// session listing the `RunAtLoad` gateway builds — because installing the second session is
    /// the harm.
    #[tokio::test]
    async fn one_port_claimed_twice_is_refused_by_every_store_wide_reader() {
        let fixture = Fixture::new("duplicate-port");
        let live = PortBlock::new(MACOS_PORT_MIN + 16, 16).expect("live 16-port block");
        fixture.plant_port_block(live);
        let (_repo_a, _repo_b, source) = two_project_store(&fixture);
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        // The fixture hands out disjoint blocks, so the collision has to be planted: one main
        // holds a live 16-port block inside the 64-port block the other main is issued.
        let fresh = PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).expect("new block");
        let collision = |error: GatewayInventoryError| match error {
            GatewayInventoryError::OverlappingPortBlocks { held, claimed } => {
                let mut pair = [held, claimed];
                pair.sort_by_key(|block| block.size());
                pair == [live, fresh]
            }
            _ => false,
        };
        assert!(collision(
            inventory
                .all_reserved_port_blocks()
                .await
                .expect_err("overlapping global port assignment"),
        ));
        assert!(collision(
            inventory
                .all_attached()
                .await
                .expect_err("a colliding endpoint must not be installed"),
        ));
    }

    #[tokio::test]
    async fn store_wide_listing_includes_a_project_whose_direct_mounted_main_path_is_gone() {
        let fixture = Fixture::new("list-stale-main");
        let valid_repo = RepoId::parse("acme/valid").expect("valid repo");
        let stale_repo = RepoId::parse("acme/stale").expect("stale repo");
        fixture.bind(&valid_repo);
        fixture.bind(&stale_repo);

        let (valid_storage, valid_mounted) = fixture.workspace(
            &valid_repo,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000001",
            3,
            true,
        );
        let (valid_mount, valid_path) = valid_mounted.expect("valid main mounted");
        let (stale_storage, stale_mounted) = fixture.workspace(
            &stale_repo,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000002",
            4,
            false,
        );
        assert!(stale_mounted.is_none());
        let stale_path = fixture.root.join("checkout-stale");
        fs::remove_dir_all(&stale_path).expect("remove stale direct mount path");

        let source = Arc::new(FixtureSource {
            projects: Mutex::new(BTreeMap::from([
                (
                    valid_repo.clone(),
                    ProjectInventoryFacts {
                        storage: vec![valid_storage],
                        mounts: vec![valid_mount.clone()],
                        checkpoints: Vec::new(),
                        mount_paths: BTreeMap::from([(valid_mount.volume_key, valid_path)]),
                    },
                ),
                (
                    stale_repo.clone(),
                    ProjectInventoryFacts {
                        checkpoints: Vec::new(),
                        mount_paths: BTreeMap::from([(
                            stale_storage.volume_key.clone(),
                            stale_path.clone(),
                        )]),
                        storage: vec![stale_storage],
                        mounts: Vec::new(),
                    },
                ),
            ])),
        });
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        let projects = inventory
            .all_projects()
            .await
            .expect("store-wide list does not open checkout Git repositories");
        assert_eq!(
            projects
                .iter()
                .map(|project| project.repo_id.as_str())
                .collect::<Vec<_>>(),
            ["acme/stale", "acme/valid"]
        );
        let stale = projects
            .iter()
            .find(|project| project.repo_id == stale_repo)
            .expect("stale project listed");
        assert_eq!(stale.workspaces.len(), 1);
        assert_eq!(stale.workspaces[0].state, WorkspaceState::Detached);
        assert_eq!(stale.workspaces[0].mount, stale_path);
        assert!(!stale.workspaces[0].mount.exists());
        let valid = projects
            .iter()
            .find(|project| project.repo_id == valid_repo)
            .expect("valid project listed");
        assert_eq!(valid.workspaces[0].state, WorkspaceState::Attached);
    }

    /// `checkpoints` is a safety interlock: triage reads it to decide whether a preserved copy of a
    /// workspace exists before destroying the workspace. The store-wide listing used to hand the
    /// derivation an empty checkpoint list and then serialize the result as fact, so a `--keep`
    /// pinned checkpoint present on disk was reported as no checkpoint at all — a false negative on
    /// the interlock, in the direction that destroys the only copy.
    #[tokio::test]
    async fn store_wide_listing_reports_a_pinned_checkpoint_rather_than_an_empty_list() {
        let fixture = Fixture::new("checkpoint-visibility");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let workspace = WorkspaceName::new("raven").expect("workspace");
        let (storage, mounted) = fixture.workspace(
            &repo,
            workspace.clone(),
            "00000000000000000000000000000001",
            4,
            true,
        );
        let (mount, path) = mounted.expect("mounted fixture");
        let source = Arc::new(FixtureSource::default());
        source.projects.lock().expect("source").insert(
            repo.clone(),
            ProjectInventoryFacts {
                storage: vec![storage],
                mounts: vec![mount.clone()],
                checkpoints: vec![
                    CheckpointFact {
                        repo: repo.clone(),
                        workspace: workspace.clone(),
                        label: CheckpointLabel::new("release-snapshot").expect("label"),
                        revision: Revision::new(3),
                        pin: Pin::Pinned,
                    },
                    CheckpointFact {
                        repo: repo.clone(),
                        workspace: workspace.clone(),
                        label: CheckpointLabel::new("autosave").expect("label"),
                        revision: Revision::new(2),
                        pin: Pin::Automatic,
                    },
                ],
                mount_paths: BTreeMap::from([(mount.volume_key, path)]),
            },
        );
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        let projects = inventory.all_projects().await.expect("store-wide list");
        let listed = projects
            .iter()
            .find(|project| project.repo_id == repo)
            .expect("project listed")
            .workspaces
            .iter()
            .find(|info| info.workspace == workspace)
            .expect("workspace listed");
        assert_eq!(
            listed
                .checkpoints
                .iter()
                .map(|checkpoint| (checkpoint.label.as_str(), checkpoint.pinned))
                .collect::<Vec<_>>(),
            [("autosave", false), ("release-snapshot", true)],
            "the store-wide listing reports the checkpoints that exist, pin state included"
        );
    }

    /// A direct-mount project's main volume is mounted at the adopted checkout rather than under
    /// `mnt/`, and the recorded layout is what says so. Deriving mount paths from the record is the
    /// whole point of writing it at adopt time.
    #[tokio::test]
    async fn a_direct_mount_project_serves_main_at_the_adopted_checkout() {
        let fixture = Fixture::new("direct-mount");
        let repo = RepoId::parse("example-org/example-app").expect("repo");
        fixture.bind(&repo);
        let (storage, mounted) = fixture.workspace(
            &repo,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000001",
            3,
            true,
        );
        let (mount, path) = mounted.expect("mounted fixture");
        assert_eq!(path, fixture.root.join("checkout-example-app"));
        let source = Arc::new(FixtureSource::default());
        source.projects.lock().expect("source").insert(
            repo.clone(),
            ProjectInventoryFacts {
                storage: vec![storage],
                mounts: vec![mount.clone()],
                checkpoints: Vec::new(),
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
        );
        let layout = StorageLayout::new(fixture.storage.store(), &repo).expect("layout");
        let canonical =
            canonical_image_paths(&layout, &storage.workspace).expect("canonical image");
        let trash = layout.project().sessions.join(".trash");
        fs::create_dir_all(&trash).expect("trash");
        let retired = trash.join("main-retired.asif");
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
    async fn a_bound_project_whose_main_images_are_gone_is_found_by_its_recorded_checkout_root() {
        let fixture = Fixture::new("reclaimed-main");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let checkout = fixture.root.join("checkout-widget");
        fs::create_dir_all(&checkout).expect("restored checkout");
        StorageLayout::new(fixture.storage.store(), &repo)
            .expect("layout")
            .record_checkout_root(&checkout)
            .expect("checkout root record");
        let inventory = NativeGatewayInventory::new(fixture.storage.clone());

        assert_eq!(
            inventory
                .repository_for_project_root(&checkout)
                .await
                .expect("resolve a project with no main image"),
            Some(repo)
        );
    }

    #[tokio::test]
    async fn a_recorded_checkout_root_never_overrides_what_main_images_record() {
        let fixture = Fixture::new("image-precedence");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        fixture.workspace(
            &repo,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000001",
            4,
            false,
        );
        let stale = fixture.root.join("checkout-before-move");
        fs::create_dir_all(&stale).expect("stale checkout");
        StorageLayout::new(fixture.storage.store(), &repo)
            .expect("layout")
            .record_checkout_root(&stale)
            .expect("checkout root record");
        let inventory = NativeGatewayInventory::new(fixture.storage.clone());

        assert_eq!(
            inventory
                .repository_for_project_root(&stale)
                .await
                .expect("resolve the stale root"),
            None,
            "main's image names the checkout while it exists"
        );
        assert_eq!(
            inventory
                .repository_for_project_root(&fixture.root.join("checkout-widget"))
                .await
                .expect("resolve the imaged root"),
            Some(repo)
        );
    }

    #[tokio::test]
    async fn detached_facts_never_become_sessions() {
        let fixture = Fixture::new("excluded");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let (detached, _) = fixture.workspace(
            &repo,
            WorkspaceName::session("raven").expect("session"),
            "00000000000000000000000000000002",
            5,
            false,
        );
        let source = Arc::new(FixtureSource {
            projects: Mutex::new(BTreeMap::from([(
                repo.clone(),
                ProjectInventoryFacts {
                    storage: vec![detached],
                    mounts: Vec::new(),
                    checkpoints: Vec::new(),
                    mount_paths: BTreeMap::new(),
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
                    checkpoints: Vec::new(),
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
        gate: Option<MountGate>,
    }

    /// Holds every mount until the test lets it through, saying which mount is waiting.
    #[derive(Clone)]
    struct MountGate {
        permits: Arc<tokio::sync::Semaphore>,
        waiting: tokio::sync::mpsc::UnboundedSender<String>,
    }

    impl FakeHealSource {
        fn new(projects: BTreeMap<RepoId, Vec<LifecycleWorkspace>>) -> Self {
            Self {
                projects,
                unopenable: BTreeSet::new(),
                refused: BTreeSet::new(),
                order: Arc::new(Mutex::new(Vec::new())),
                gate: None,
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

        fn gated(mut self, gate: MountGate) -> Self {
            self.gate = Some(gate);
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
                gate: self.gate.clone(),
            }))
        }
    }

    struct FakeProjectMounts {
        workspaces: Vec<LifecycleWorkspace>,
        refused: BTreeSet<String>,
        order: Arc<Mutex<Vec<String>>>,
        gate: Option<MountGate>,
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
            if let Some(gate) = &self.gate {
                gate.waiting.send(key.clone()).expect("the test listens");
                gate.permits
                    .acquire()
                    .await
                    .expect("the gate opens")
                    .forget();
            }
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
        )
        .expect("workspace")
    }

    /// Heal every recorded project as the daemon does, and check the pass counted each one.
    async fn heal_recorded(inventory: &NativeGatewayInventory) -> Vec<ProjectHealOutcome> {
        let repositories = inventory
            .recorded_projects()
            .await
            .expect("recorded projects");
        let progress = StartupHealState::mounting(repositories.len());
        let outcomes = inventory.heal(repositories, &progress).await;
        assert_eq!(
            progress.current(),
            Some(StartupHeal::RestoringSessions),
            "every project was counted as mounted"
        );
        outcomes
    }

    /// While a mount is held, the pass says how many projects it still has to mount: a project
    /// counts as mounted only once its sessions are, so the count holds through the mains and
    /// drops as each project's sessions finish, then turns to restoring sessions.
    #[tokio::test]
    async fn the_startup_heal_counts_down_the_projects_it_still_mounts() {
        let fixture = Fixture::new("heal-progress");
        let alpha = RepoId::parse("acme/alpha").expect("repo alpha");
        let beta = RepoId::parse("acme/beta").expect("repo beta");
        fixture.bind(&alpha);
        fixture.bind(&beta);
        let permits = Arc::new(tokio::sync::Semaphore::new(0));
        let (waiting, mut mounts) = tokio::sync::mpsc::unbounded_channel();
        let heal = Arc::new(
            FakeHealSource::new(BTreeMap::from([
                (alpha.clone(), vec![heal_workspace(&alpha, "main")]),
                (
                    beta.clone(),
                    vec![
                        heal_workspace(&beta, "main"),
                        heal_workspace(&beta, "raven"),
                    ],
                ),
            ]))
            .gated(MountGate {
                permits: Arc::clone(&permits),
                waiting,
            }),
        );
        let inventory = NativeGatewayInventory::with_heal_source(
            fixture.storage.clone(),
            heal as Arc<dyn HealSource>,
        );
        let repositories = inventory
            .recorded_projects()
            .await
            .expect("recorded projects");
        let progress = Arc::new(StartupHealState::mounting(repositories.len()));
        let mounting = |projects| {
            Some(StartupHeal::Mounting {
                projects: std::num::NonZeroUsize::new(projects).expect("non-zero"),
            })
        };
        let healing = tokio::spawn({
            let progress = Arc::clone(&progress);
            async move { inventory.heal(repositories, &progress).await }
        });

        for (held, left) in [
            ("acme/alpha/main", 2),
            ("acme/beta/main", 2),
            ("acme/beta/raven", 1),
        ] {
            assert_eq!(mounts.recv().await.as_deref(), Some(held));
            assert_eq!(progress.current(), mounting(left), "while {held} is held");
            permits.add_permits(1);
        }
        let outcomes = healing.await.expect("the heal ends");
        assert_eq!(outcomes.len(), 2);
        assert_eq!(progress.current(), Some(StartupHeal::RestoringSessions));
        progress.restored();
        assert_eq!(progress.current(), None);
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

        let outcomes = heal_recorded(&inventory).await;

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

        let outcomes = heal_recorded(&inventory).await;

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

        let outcomes = heal_recorded(&inventory).await;

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
        let fixture = Fixture::new("unmounted-mains");
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
                    checkpoints: Vec::new(),
                    mount_paths,
                },
            );
        }
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        let unreachable = inventory
            .unmounted_mains()
            .await
            .expect("main reachability");

        let layout = StorageLayout::new(fixture.storage.store(), &detached).expect("layout");
        let image = layout.main_image().expect("main image").image().to_owned();
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

    /// A bound project whose main image records no adopted checkout path is omitted from the
    /// adopted list but still reported, so `doctor` can name it instead of the inventory
    /// swallowing it on stderr.
    #[tokio::test]
    async fn checkoutless_projects_are_reported_not_just_skipped() {
        let fixture = Fixture::new("checkoutless-reported");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);

        let inventory = NativeGatewayInventory::new(fixture.storage.clone());
        assert!(
            inventory
                .adopted_projects()
                .await
                .expect("adopted projects")
                .is_empty(),
            "no checkout path means no adopted project"
        );

        let (projects, _, checkoutless, issues) =
            inventory.doctor_projects().await.expect("doctor projects");
        assert!(projects.is_empty());
        assert_eq!(checkoutless, vec![repo]);
        assert!(issues.is_empty());
    }
}
