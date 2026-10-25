use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use cowshed_gateway_types::StartupHeal;
use thiserror::Error;

#[cfg(test)]
use crate::api::dto::WorkspaceState;
use crate::api::dto::{ProjectWorkspaces, UnprovenWorkspace, WorkspaceInfo};
use crate::metadata::{
    DetachedWorkspaceMetadata, GrantSet, PortBlock, PublicationState, ReservedPortBlocks,
    WorkspaceIncarnation, WorkspaceName, sidecar_path,
};
use crate::repository::{OwnedRepoIds, RepoId, RepositoryBinding};
use crate::storage::apfs::native::{
    KernelMountSource, MacOsApfsExecutionHost, SystemKernelMountSource,
};
use crate::storage::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrate, ApfsSubstrateConfig,
};
use crate::storage::bootstrap::ValidatedHostStorage;
use crate::storage::directory::DirectoryExecutionHost;
use crate::storage::lifecycle::{
    CheckpointFact, DerivationError, DerivedWorkspace, LifecycleWorkspace, MountIntent, MountJoin,
    MountState, StorageFact, Substrate, derive_workspaces,
};
use crate::storage::{StorageLayout, verify_no_symlinks};
use crate::workspace_credentials::{
    GatewayWorkspaceCredentials, WorkspaceCredentialError, read_gateway_workspace_credentials,
};
use cowshed_apfs::SystemCommandRunner;
use cowshed_apfs::mounts::KernelMountSnapshot;

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

    /// One more project's main is mounted, or reported as unmountable. [`NativeGatewayInventory::heal`]
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

/// Controller-authoritative macOS port-block inventory for one gateway session.
/// This pure inventory can be inspected off-host; installation still validates the host platform.
pub struct GatewaySessionFact {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
    pub incarnation: WorkspaceIncarnation,
    pub revision: u64,
    /// When the workspace was created, as its metadata records it: of two sessions sharing a
    /// port, the older is served.
    pub created_at: String,
    pub mount_id: u64,
    pub mount: PathBuf,
    pub grants: GrantSet,
    pub port_block: PortBlock,
    pub credentials: GatewayWorkspaceCredentials,
}

/// The sessions one inventory pass found attached, and the workspaces it holds.
#[derive(Debug, Default)]
pub struct AttachedInventory {
    pub attached: Vec<GatewaySessionFact>,
    pub held: Vec<HeldWorkspace>,
}

/// A workspace whose mount is unproven (`MountState::Unproven`): something is mounted at its
/// mountpoint that does not prove itself the workspace's, so it is neither attached nor detached.
/// It is reported by name; nothing installs a session for it, and nothing removes the one it has,
/// on the strength of this inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeldWorkspace {
    pub repo_id: RepoId,
    pub workspace: WorkspaceName,
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

/// What the startup heal achieved for one project: its main, the user's own checkout. Session
/// workspaces are not attached by the startup pass; each attaches on use (05_gateway.md "Startup
/// contract"), so a reboot attaches only the checkouts someone can see and the sheds someone uses.
#[derive(Debug)]
pub struct ProjectHealOutcome {
    pub repo_id: RepoId,
    /// Main's mountpoint, or why the project's checkout is not reachable there.
    pub main: Result<PathBuf, GatewayInventoryError>,
}

impl fmt::Debug for GatewaySessionFact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewaySessionFact")
            .field("repo_id", &self.repo_id)
            .field("workspace", &self.workspace)
            .field("incarnation", &self.incarnation)
            .field("revision", &self.revision)
            .field("created_at", &self.created_at)
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
    OwnerScan(#[from] crate::storage::bootstrap::OwnerScanError),
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
    mounts: Vec<MountJoin>,
    checkpoints: Vec<CheckpointFact>,
    mount_paths: BTreeMap<String, PathBuf>,
}

struct DerivedInventory {
    layout: StorageLayout,
    mount_paths: BTreeMap<String, PathBuf>,
    derived: Vec<crate::storage::lifecycle::DerivedWorkspace>,
}

trait InventorySource: Send + Sync {
    /// The workspaces one project's records publish, read without the kernel mount table: what a
    /// workspace holds attached or detached (its port blocks) is decided from these alone.
    fn project_metadata(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<Vec<DetachedWorkspaceMetadata>, GatewayInventoryError>;
    fn project_facts(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<ProjectInventoryFacts, GatewayInventoryError>;
    fn workspace_snapshot(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata, PathBuf)>, GatewayInventoryError>;
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
    fn project_metadata(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<Vec<DetachedWorkspaceMetadata>, GatewayInventoryError> {
        let config = project_substrate_config(
            storage,
            storage
                .store()
                .join("gateway")
                .join(UNRESOLVED_CHECKOUT_PATH),
        );
        // One host for the complete pass, not a mount-registry thread per captured image.
        let host = MacOsApfsExecutionHost::new(SystemCommandRunner::new(config.namespace), config)?;
        let published = host.list(repo)?;
        let mut records = Vec::with_capacity(published.len());
        for fact in published {
            // The listing is not authority after retirement; a recreated name owns its current
            // grants, and an image already retired owns no ports.
            if let Some(metadata) = host.canonical_metadata(repo, fact.workspace.name())? {
                records.push(metadata);
            }
        }
        Ok(records)
    }

    fn project_facts(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<ProjectInventoryFacts, GatewayInventoryError> {
        let (layout, config) = project_layout_and_config(storage, repo)?;
        let captured = SystemKernelMountSource.mounts()?;
        let host = MacOsApfsExecutionHost::with_mount_source(
            SystemCommandRunner::new(config.namespace),
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

    fn workspace_snapshot(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata, PathBuf)>, GatewayInventoryError>
    {
        let layout = StorageLayout::new(storage.store(), repo).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: storage.store().to_owned(),
                message: error.to_string(),
            }
        })?;
        // A session workspace's mount is independent of main. Only a request for main reads
        // main's recorded checkout; a job must never inspect a sibling's detached metadata.
        let checkout = if workspace.is_main() {
            authoritative_checkout_path(&layout, repo)?
        } else {
            None
        }
        .unwrap_or_else(|| {
            storage
                .store()
                .join("gateway")
                .join(UNRESOLVED_CHECKOUT_PATH)
        });
        let mount = workspace_mountpoint(&layout, &checkout, workspace)?;
        let host = MacOsApfsExecutionHost::new(
            SystemCommandRunner::new(storage.namespace()),
            project_substrate_config(storage, checkout),
        )?;
        Ok(host
            .workspace_snapshot(repo, workspace)?
            .map(|(derived, metadata)| (derived, metadata, mount)))
    }
}

/// Inventory over a caller-owned directory store, selected explicitly by the directory
/// runtime's caller rather than inferred from path shapes. A mounted directory payload
/// produces no kernel mount: its mount fact is the image path's mount record, proven by
/// the payload marker the directory host checks, so the kernel table is never consulted.
#[derive(Clone, Copy, Debug, Default)]
struct DirectoryInventorySource;

impl InventorySource for DirectoryInventorySource {
    fn project_metadata(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<Vec<DetachedWorkspaceMetadata>, GatewayInventoryError> {
        let config = project_substrate_config(
            storage,
            storage
                .store()
                .join("gateway")
                .join(UNRESOLVED_CHECKOUT_PATH),
        );
        let host = DirectoryExecutionHost::new(config)?;
        let published = host.list(repo)?;
        let mut records = Vec::with_capacity(published.len());
        for fact in published {
            if let Some(metadata) = host.canonical_metadata(repo, fact.workspace.name())? {
                records.push(metadata);
            }
        }
        Ok(records)
    }

    fn project_facts(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
    ) -> Result<ProjectInventoryFacts, GatewayInventoryError> {
        let (layout, config) = project_layout_and_config(storage, repo)?;
        let host = DirectoryExecutionHost::new(config.clone())?;
        let storage_facts = host.list(repo)?;
        let mount_paths = expected_mount_paths(&config, &layout, &storage_facts)?;
        let mounts = host.mounts(repo)?;
        let checkpoints = host.checkpoints(repo)?;
        Ok(ProjectInventoryFacts {
            storage: storage_facts,
            mounts,
            checkpoints,
            mount_paths,
        })
    }

    fn workspace_snapshot(
        &self,
        storage: &ValidatedHostStorage,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata, PathBuf)>, GatewayInventoryError>
    {
        let layout = StorageLayout::new(storage.store(), repo).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: storage.store().to_owned(),
                message: error.to_string(),
            }
        })?;
        let checkout = if workspace.is_main() {
            authoritative_checkout_path(&layout, repo)?
        } else {
            None
        }
        .unwrap_or_else(|| {
            storage
                .store()
                .join("gateway")
                .join(UNRESOLVED_CHECKOUT_PATH)
        });
        let mount = workspace_mountpoint(&layout, &checkout, workspace)?;
        let host = DirectoryExecutionHost::new(project_substrate_config(storage, checkout))?;
        Ok(host
            .workspace_snapshot(repo, workspace)?
            .map(|(derived, metadata)| (derived, metadata, mount)))
    }
}

/// One project's layout and the substrate configuration for the checkout its records name.
fn project_layout_and_config(
    storage: &ValidatedHostStorage,
    repo: &RepoId,
) -> Result<(StorageLayout, ApfsSubstrateConfig), GatewayInventoryError> {
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
    Ok((layout, config))
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
    ApfsSubstrateConfig::for_storage(storage, checkout_path)
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
        let retirement_lease =
            crate::project_retirement::acquire_project_reader(storage.store(), repo).map_err(
                |error| GatewayInventoryError::InvalidMetadata {
                    path: storage.store().to_owned(),
                    message: error.to_string(),
                },
            )?;
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
        let host = MacOsApfsExecutionHost::new(
            SystemCommandRunner::new(config.namespace),
            config.clone(),
        )?;
        Ok(Arc::new(NativeProjectMounts {
            repo: repo.clone(),
            substrate: ApfsSubstrate::new(config, host),
            _retirement_lease: retirement_lease,
        }))
    }
}

struct NativeProjectMounts {
    repo: RepoId,
    substrate: ApfsSubstrate<MacOsApfsExecutionHost<SystemCommandRunner>>,
    _retirement_lease: crate::project_retirement::ProjectReadLease,
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
        crate::project_retirement::require_project_active(
            &self.substrate.config().store_root,
            &self.repo,
        )
        .map_err(|error| GatewayInventoryError::InvalidMetadata {
            path: self.substrate.config().store_root.clone(),
            message: error.to_string(),
        })?;
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

/// One macOS port block the store names, the workspace that names it, and the record it is in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortClaim {
    pub repo: RepoId,
    pub workspace: WorkspaceName,
    /// When the workspace was created, as its metadata records it (RFC 3339 UTC): of two
    /// workspaces claiming one port, the older keeps it.
    pub created_at: String,
    pub block: PortBlock,
    pub record: ClaimRecord,
}

impl PortClaim {
    fn same_workspace(&self, other: &Self) -> bool {
        self.repo == other.repo && self.workspace == other.workspace
    }
}

/// The record a claim is read from. Only a published grant is the workspace's own to rewrite;
/// every other record changes when the operation that wrote it finishes or is retired.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimRecord {
    /// The grants of a published workspace.
    Published,
    /// The grants fenced into an unpublished clone's canonical image, at this sidecar.
    PendingFence(PathBuf),
    /// The grants an unfinished lifecycle intent in this journal carries.
    Intent(PathBuf),
    /// A grants sidecar whose image is outside the canonical inventory.
    Sidecar(PathBuf),
}

impl fmt::Display for ClaimRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Published => formatter.write_str("its published grants"),
            Self::PendingFence(path) => {
                write!(
                    formatter,
                    "the unpublished clone's grants {}",
                    path.display()
                )
            }
            Self::Intent(path) => {
                write!(
                    formatter,
                    "an unfinished lifecycle intent in {}",
                    path.display()
                )
            }
            Self::Sidecar(path) => write!(formatter, "the grants sidecar {}", path.display()),
        }
    }
}

/// Two workspaces whose blocks share a port. `held` belongs to the older workspace, which keeps
/// the port; `claimed` belongs to the newer one, which `cowshed doctor --repair` moves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortCollision {
    pub held: PortClaim,
    pub claimed: PortClaim,
}

impl PortCollision {
    /// Whether `workspace` of `repo` is the newer member, the one not served.
    pub fn claims(&self, repo: &RepoId, workspace: &WorkspaceName) -> bool {
        self.claimed.repo == *repo && self.claimed.workspace == *workspace
    }
}

impl fmt::Display for PortCollision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { held, claimed } = self;
        write!(
            formatter,
            "macOS port block {} of {}/{} ({}) shares ports with block {} of {}/{} ({}), an older workspace",
            claimed.block,
            claimed.repo,
            claimed.workspace,
            claimed.record,
            held.block,
            held.repo,
            held.workspace,
            held.record
        )
    }
}

/// What the store's records name: every claim, and every record that could not be read, which
/// names a block nobody can know. Allocation proceeds without it and says so.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PortClaims {
    pub claims: Vec<PortClaim>,
    pub unreadable: Vec<(PathBuf, String)>,
}

/// Every collision among the store's claims, and the records that could not be read.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PortCollisions {
    pub collisions: Vec<PortCollision>,
    pub unreadable: Vec<(PathBuf, String)>,
}

/// Every block `metadata` grants — the current one and each retained one — claimed by its
/// workspace in `repo`, as `record` names them.
fn port_claims<'a>(
    repo: &'a RepoId,
    metadata: &'a DetachedWorkspaceMetadata,
    record: &'a ClaimRecord,
) -> impl Iterator<Item = PortClaim> + 'a {
    metadata.grants.port_blocks().map(move |block| PortClaim {
        repo: repo.clone(),
        workspace: metadata.workspace.clone(),
        created_at: metadata.info_snapshot.created_at.clone(),
        block,
        record: record.clone(),
    })
}

/// The attached sessions the gateway serves, and the collisions whose newer member it does not
/// serve (04_sandbox.md, `portBlock`; 05_gateway.md). `attached` is every attached session on
/// the host: two sessions whose blocks share a port would answer on one endpoint, so the older
/// workspace's is served and the newer one's waits for `cowshed doctor --repair` to move it.
pub fn served_sessions(
    attached: Vec<GatewaySessionFact>,
) -> (Vec<GatewaySessionFact>, Vec<PortCollision>) {
    let refused = port_collisions(attached.iter().flat_map(|fact| {
        std::iter::once(fact.port_block)
            .chain(fact.grants.retained_port_blocks.iter().copied())
            .map(|block| PortClaim {
                repo: fact.repo_id.clone(),
                workspace: fact.workspace.clone(),
                created_at: fact.created_at.clone(),
                block,
                record: ClaimRecord::Published,
            })
    }));
    let served = attached
        .into_iter()
        .filter(|fact| {
            !refused
                .iter()
                .any(|collision| collision.claims(&fact.repo_id, &fact.workspace))
        })
        .collect();
    (served, refused)
}

/// The claims among `claims` that share a port with an older workspace's claim. Claims are held
/// oldest first — by recorded creation time, then by repository and workspace so a tie still has
/// one answer — and a claim reaching into a held claim of another workspace is that workspace's
/// collision instead of a holding. One workspace's own blocks never collide: a block it outgrew
/// nests inside the one that replaced it, and a block its intent carries is the one it holds.
pub fn port_collisions(claims: impl IntoIterator<Item = PortClaim>) -> Vec<PortCollision> {
    let mut claims = claims.into_iter().collect::<Vec<_>>();
    claims.sort_by(|left, right| {
        (
            &left.created_at,
            &left.repo,
            &left.workspace,
            left.block.base(),
        )
            .cmp(&(
                &right.created_at,
                &right.repo,
                &right.workspace,
                right.block.base(),
            ))
    });
    let mut held: Vec<PortClaim> = Vec::with_capacity(claims.len());
    let mut collisions = Vec::new();
    for claim in claims {
        match held
            .iter()
            .find(|holder| holder.block.overlaps(claim.block) && !holder.same_workspace(&claim))
        {
            Some(holder) => collisions.push(PortCollision {
                held: holder.clone(),
                claimed: claim,
            }),
            None => held.push(claim),
        }
    }
    collisions.dedup();
    collisions
}

/// The images whose grants sidecars sit directly in `directory`; none when it does not exist.
fn sidecar_images(directory: &Path) -> Result<Vec<PathBuf>, GatewayInventoryError> {
    match fs::symlink_metadata(directory) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(io_error("inspecting grants sidecars", directory, source)),
    }
    Ok(read_directory(directory, "enumerating grants sidecars")?
        .iter()
        .filter_map(|path| crate::metadata::image_from_sidecar_path(path))
        .collect())
}

/// Claims `metadata`'s current and retained blocks for its workspace, refusing a canonical grant
/// that holds no current block.
fn claim_granted_blocks(
    claims: &mut Vec<PortClaim>,
    repo: &RepoId,
    metadata: &DetachedWorkspaceMetadata,
    record: &ClaimRecord,
) -> Result<(), GatewayInventoryError> {
    if metadata.grants.port_block.is_none() {
        return Err(GatewayInventoryError::MissingPortBlock {
            repo: repo.clone(),
            workspace: metadata.workspace.clone(),
        });
    }
    claims.extend(port_claims(repo, metadata, record));
    Ok(())
}

/// Which store backend an inventory reads, chosen by the caller that opened the store:
/// native APFS volumes, whose mounts the kernel reports, or directory payloads, whose
/// mounts are the records the directory host proves. Never inferred from path shapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayStoreBackend {
    Native,
    Directory,
}
impl NativeGatewayInventory {
    pub fn new(storage: ValidatedHostStorage) -> Self {
        Self {
            storage,
            source: Arc::new(NativeInventorySource),
            heal: Arc::new(NativeHealSource),
        }
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn port_allocation(&self) -> crate::metadata::PortAllocation {
        self.storage.port_allocation()
    }

    pub(crate) fn fixture_port_claims(
        &self,
    ) -> Option<&crate::storage::bootstrap::FixturePortClaims> {
        self.storage.roots().fixture_port_claims()
    }

    /// Inventory over a caller-owned directory store: mount facts come from the mount
    /// records the directory host proves, never from the kernel mount table. Only a
    /// caller that opened a directory runtime selects this; native stores keep [`Self::new`].
    pub fn for_directory_store(storage: ValidatedHostStorage) -> Self {
        Self {
            storage,
            source: Arc::new(DirectoryInventorySource),
            heal: Arc::new(NativeHealSource),
        }
    }

    pub fn with_backend(storage: ValidatedHostStorage, backend: GatewayStoreBackend) -> Self {
        match backend {
            GatewayStoreBackend::Native => Self::new(storage),
            GatewayStoreBackend::Directory => Self::for_directory_store(storage),
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
                    | GatewayInventoryError::AmbiguousProjectRoot(_)
                    | GatewayInventoryError::ForeignBinding { .. }),
                ) => return Err(error),
                Err(error) => {
                    cowshed_os::eprintln!(
                        "cowshed: skipping {repo_id}: its workspace records could not be read: {error}"
                    );
                }
            }
        }
        projects.sort_by(|left, right| left.repo_id.cmp(&right.repo_id));
        Ok(projects)
    }

    /// What each adopted project's trash keeps of `rm --abandon`, for `ls --all` to name: no
    /// listing shows `sessions/.trash`, and no plain gc deletes those bundles.
    pub async fn kept_abandon_bundles(
        &self,
    ) -> Result<Vec<(RepoId, crate::storage::recovery::KeptAbandonBundles)>, GatewayInventoryError>
    {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let store = inventory.storage.store();
            let mut kept = Vec::new();
            for repo_id in discover_repositories(store)? {
                let layout = StorageLayout::new(store, &repo_id).map_err(|error| {
                    GatewayInventoryError::InvalidMetadata {
                        path: store.to_owned(),
                        message: error.to_string(),
                    }
                })?;
                let trash = layout
                    .project()
                    .sessions
                    .join(crate::storage::recovery::TRASH_NAMESPACE);
                match crate::storage::recovery::KeptAbandonBundles::in_trash(&trash) {
                    Ok(Some(bundles)) => kept.push((repo_id, bundles)),
                    Ok(None) => {}
                    Err(source) => {
                        return Err(GatewayInventoryError::Io {
                            operation: "reading abandon bundles",
                            path: trash,
                            source,
                        });
                    }
                }
            }
            Ok(kept)
        })
        .await
        .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    fn adopted_projects_blocking(&self) -> Result<Vec<AdoptedProject>, GatewayInventoryError> {
        let scan = self.adopted_and_checkoutless_blocking()?;
        for (repo_id, error) in scan.issues {
            cowshed_os::eprintln!("cowshed: skipping project {repo_id}: {error}");
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

    pub async fn all_attached(&self) -> Result<AttachedInventory, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || inventory.all_attached_blocking())
            .await
            .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    pub async fn project_attached(
        &self,
        repo_id: &RepoId,
    ) -> Result<AttachedInventory, GatewayInventoryError> {
        let inventory = self.clone();
        let repo_id = repo_id.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            inventory.project_attached_blocking(&repo_id)
        })
        .await
        .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    /// Read only the selected workspace, including its exact detached grant and mount identity.
    pub async fn workspace_attached(
        &self,
        repo_id: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<GatewaySessionFact>, GatewayInventoryError> {
        let inventory = self.clone();
        let repo_id = repo_id.clone();
        let workspace = workspace.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            inventory.workspace_attached_blocking(&repo_id, &workspace)
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
        cowshed_os::timing::timed_async(HEAL_SCOPE, "discover", async move {
            crate::storage::lifecycle::dispatch_blocking(move || {
                let mut repositories = discover_repositories(&store)?;
                // Only a positive pending authorization removes a project from ordinary heal.
                // An unreadable record remains a per-project open failure, not a silent skip or
                // a reason to stop every other project's startup.
                repositories.retain(|repo| {
                    !matches!(
                        crate::project_retirement::retirement_blocks_open(&store, repo),
                        Ok(true)
                    )
                });
                Ok(repositories)
            })
            .await
            .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
        })
        .await
    }

    /// Attach and mount every one of `repositories`' mains, telling `progress` as each project is
    /// done.
    ///
    /// This runs at gateway startup because the gateway is `RunAtLoad` and a reboot is the one
    /// window adoption's "the checkout path is never absent and never dangling" guarantee cannot
    /// defend on its own. Healing on contact would leave a dangling symlink — or, under direct
    /// mount, a bare stub directory — visible in the user's shell, editor, and Finder until
    /// something happened to touch it.
    ///
    /// Session workspaces are not attached here. Each costs two images (its own and its build
    /// volume) and six registered IOMedia under IOKit's 1024 match-notification cap, and most sheds
    /// a reboot finds are idle: attaching all of them spent the cap and the image driver's pool on
    /// sheds nobody used. A session attaches on use instead (`path`, `exec`, `attach` and every
    /// verb that opens it), which is the same heal-on-contact a detached session already takes.
    ///
    /// Every step is a lifecycle span of its own (`startup-heal open|mount <repo>/main`), so a slow
    /// heal reads as the step that spent the time.
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
        let mut opened = Vec::with_capacity(repositories.len());
        for repo in repositories {
            let project = cowshed_os::timing::timed_async(
                HEAL_SCOPE,
                format!("open {repo}"),
                self.open_project(&repo),
            )
            .await;
            opened.push((repo, project));
        }
        let mut outcomes = Vec::with_capacity(opened.len());
        for (repo_id, project) in opened {
            let main = match project {
                Ok(OpenProject {
                    mounts,
                    main: Some(main),
                }) => {
                    cowshed_os::timing::timed_async(
                        HEAL_SCOPE,
                        format!("mount {repo_id}/{}", main.name()),
                        mounts.mount(&main),
                    )
                    .await
                }
                Ok(OpenProject { main: None, .. }) => {
                    Err(GatewayInventoryError::MissingMainWorkspace(repo_id.clone()))
                }
                Err(error) => Err(error),
            };
            outcomes.push(ProjectHealOutcome { repo_id, main });
            progress.project_mounted();
        }
        outcomes
    }

    /// Open one project's mount side and find its main.
    ///
    /// Opening is separated from mounting so the whole store is prepared before the first mount:
    /// the main pass is only "mains first" if no project's preparation happens between two other
    /// projects' mains.
    async fn open_project(&self, repo: &RepoId) -> Result<OpenProject, GatewayInventoryError> {
        let mounts = self.heal.open(&self.storage, repo).await?;
        let main = mounts
            .workspaces()
            .await?
            .into_iter()
            .find(|workspace| workspace.name().is_main());
        Ok(OpenProject { mounts, main })
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

    /// Every pair of workspaces whose recorded blocks share a port, the newer one claiming.
    pub async fn port_collisions(&self) -> Result<PortCollisions, GatewayInventoryError> {
        let inventory = self.clone();
        crate::storage::lifecycle::dispatch_blocking(move || inventory.port_collisions_blocking())
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

    /// The primary identity of the bound project whose main sits at `checkout`, for an explicit
    /// `adopt --re-adopt` that names no repository. Read-only: nothing is repaired or written.
    ///
    /// Four facts must agree before a project is named for a mounted main: the in-image marker at
    /// the checkout is the main workspace's and records that checkout; the controller's main sidecar
    /// for the marker's repo is the Active main, at the same incarnation, recording the same checkout
    /// (a RAW `main.dmg`); a binding owns that repo; and no other project's RAW main claims the
    /// checkout. Without a marker the main is unmounted, and the store alone names the owner: the
    /// project it recorded for this checkout root, or the one whose main sidecar names it. The
    /// adoption path proves that the checkout's remotes name that repo. `Ok(None)` means nothing in
    /// the store claims `checkout`, or its marker's owner holds no RAW `main.dmg`; adopt step 1 then
    /// names the identity.
    pub async fn re_adoption_primary_for_checkout(
        &self,
        checkout: &Path,
    ) -> Result<Option<RepoId>, GatewayInventoryError> {
        let inventory = self.clone();
        let checkout = checkout.to_owned();
        crate::storage::lifecycle::dispatch_blocking(move || {
            inventory.re_adoption_primary_for_checkout_blocking(&checkout)
        })
        .await
        .map_err(|error| GatewayInventoryError::Blocking(error.to_string()))?
    }

    /// The exact checkout an unfinished, validated re-adoption owns, even before its new
    /// main image is published. A recorded checkout alone never authorizes this recovery.
    pub async fn forming_re_adoption_root(
        &self,
        project_root: &Path,
    ) -> Result<Option<PathBuf>, GatewayInventoryError> {
        let inventory = self.clone();
        let project_root = project_root.to_owned();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let Some(repo) = inventory.repository_for_project_root_blocking(&project_root)? else {
                return Ok(None);
            };
            let checkout = fs::canonicalize(&project_root).map_err(|source| {
                io_error("resolving re-adoption checkout", &project_root, source)
            })?;
            let layout = StorageLayout::new(inventory.storage.store(), &repo).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: inventory.storage.store().to_owned(),
                    message: error.to_string(),
                }
            })?;
            forming_re_adoption_checkout(&layout, &repo, &checkout)
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
                            && cowshed_apfs::image::is_image_path(&path)
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
                    verify_image_path(self.storage.store(), &image)?;
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
        if matched.is_some() {
            return Ok(matched);
        }
        pending_restore_for_checkout(self.storage.store(), &expected)
    }

    fn re_adoption_primary_for_checkout_blocking(
        &self,
        checkout: &Path,
    ) -> Result<Option<RepoId>, GatewayInventoryError> {
        let marker_path = checkout.join(crate::storage::WORKSPACE_MARKER_PATH);
        let marker = match crate::metadata::WorkspaceMarker::read_from(&marker_path) {
            Ok(marker) => marker,
            Err(crate::metadata::MetadataError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                // An unmounted main has no in-image marker. The store names its owner by the root it
                // recorded for this checkout or by the main sidecar that names it. The remote identity
                // proof is the adoption path's job, because it owns the async Git handle.
                return self.repository_for_project_root_blocking(checkout);
            }
            Err(error) => {
                return Err(GatewayInventoryError::InvalidMetadata {
                    path: marker_path,
                    message: error.to_string(),
                });
            }
        };
        let refused = |message: String| GatewayInventoryError::InvalidMetadata {
            path: marker_path.clone(),
            message,
        };
        if marker.role != crate::metadata::WorkspaceRole::Main || !marker.workspace.is_main() {
            return Err(refused(format!(
                "in-image marker names {} with role {:?}, not the main workspace",
                marker.workspace, marker.role
            )));
        }
        let canonical = fs::canonicalize(checkout)
            .map_err(|source| io_error("resolving re-adoption checkout", checkout, source))?;
        if fs::canonicalize(&marker.project_root).ok().as_deref() != Some(canonical.as_path()) {
            return Err(refused(format!(
                "in-image marker records {}, not the checkout it sits in",
                marker.project_root.display()
            )));
        }
        let mut owner: Option<OwnedRepoIds> = None;
        for owned in discover_owned_identities(self.storage.store())? {
            if !owned.accepts(&marker.repo_id) {
                continue;
            }
            if let Some(first) = owner {
                return Err(GatewayInventoryError::AmbiguousIdentity {
                    repo_id: marker.repo_id.clone(),
                    first: first.current().clone(),
                    second: owned.current().clone(),
                });
            }
            owner = Some(owned);
        }
        let Some(owner) = owner else {
            return Err(refused(format!(
                "in-image marker names {}, which no adopted project owns",
                marker.repo_id
            )));
        };
        let layout =
            StorageLayout::new(self.storage.store(), owner.current()).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: self.storage.store().to_owned(),
                    message: error.to_string(),
                }
            })?;
        let image = layout
            .main_image()
            .map_err(|error| refused(error.to_string()))?
            .image()
            .to_owned();
        match fs::symlink_metadata(&image) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            // No RAW main to bind through: adopt step 1 names the identity from the remotes.
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Ok(_) => {
                return Err(refused(format!(
                    "main image {} is not a regular file",
                    image.display()
                )));
            }
            Err(source) => return Err(io_error("inspecting main image", &image, source)),
        }
        verify_image_path(self.storage.store(), &image)?;
        let sidecar = sidecar_path(&image);
        let metadata = DetachedWorkspaceMetadata::read_for_image(&image).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: sidecar.clone(),
                message: error.to_string(),
            }
        })?;
        if metadata.repo_id != marker.repo_id
            || !metadata.workspace.is_main()
            || metadata.publication_state != PublicationState::Active
        {
            return Err(refused(format!(
                "main sidecar {} is not the active main of {}",
                sidecar.display(),
                marker.repo_id
            )));
        }
        if metadata.workspace_incarnation != marker.workspace_incarnation {
            return Err(refused(format!(
                "main sidecar records incarnation {}, but the in-image marker records {}",
                metadata.workspace_incarnation, marker.workspace_incarnation
            )));
        }
        if fs::canonicalize(&metadata.info_snapshot.project_root)
            .ok()
            .as_deref()
            != Some(canonical.as_path())
        {
            return Err(refused(format!(
                "main sidecar {} records {}, not this checkout",
                sidecar.display(),
                metadata.info_snapshot.project_root.display()
            )));
        }
        // The RAW enumeration is the only other claimant this controller reads. A different project
        // it names for this root is an ambiguity, never a tie to break.
        if let Some(claimant) = self.repository_for_project_root_blocking(&canonical)?
            && claimant != *owner.current()
        {
            return Err(GatewayInventoryError::AmbiguousProjectRoot(
                checkout.to_owned(),
            ));
        }
        Ok(Some(owner.current().clone()))
    }

    fn all_reserved_port_blocks_blocking(
        &self,
    ) -> Result<ReservedPortBlocks, GatewayInventoryError> {
        let PortClaims { claims, unreadable } = self.port_claims_blocking()?;
        for (path, error) in unreadable {
            cowshed_os::eprintln!(
                "cowshed: {} cannot be used ({error}); a port block it names may be handed out again",
                path.display()
            );
        }
        let mut blocks = ReservedPortBlocks::default();
        for claim in claims {
            blocks.insert(claim.block);
        }
        Ok(blocks)
    }

    fn port_collisions_blocking(&self) -> Result<PortCollisions, GatewayInventoryError> {
        let PortClaims { claims, unreadable } = self.port_claims_blocking()?;
        Ok(PortCollisions {
            collisions: port_collisions(claims),
            unreadable,
        })
    }

    /// Every macOS port block the store names, with the workspace naming it (04_sandbox.md,
    /// `portBlock`): the grant every unfinished lifecycle intent carries; each published grant;
    /// the grant fenced into a pending clone, which owns it even after its creator exits; and
    /// every grants sidecar whose image is outside the canonical inventory — retained, staged, or
    /// not yet published from staging. A port is in use while any record names it, so no
    /// allocation is handed a block before that record is gone. The journal is read first:
    /// an image leaves the canonical inventory only after its intent is journaled, and is
    /// published before that intent completes, so a block moving between the two is seen in one
    /// of them. A sidecar outside the canonical inventory that cannot be read is reported rather
    /// than failing every allocation on the host.
    fn port_claims_blocking(&self) -> Result<PortClaims, GatewayInventoryError> {
        let store = self.storage.store();
        let mut claims = Vec::new();
        let mut unreadable = Vec::new();
        for repo in discover_repositories(store)? {
            let layout = StorageLayout::new(store, &repo).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: store.to_owned(),
                    message: error.to_string(),
                }
            })?;
            let project = &layout.project().project_root;
            let sessions = &layout.project().sessions;
            // An unfinished lifecycle intent carries the grant of the workspace it rebuilds: a
            // re-adoption's main. Its image may be anywhere until the intent completes —
            // retained, staged, or gone with the grant recorded only here.
            let journal_path = project.join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE);
            let journal = crate::storage::recovery::LifecycleIntentJournal::load(&journal_path)
                .map_err(|error| GatewayInventoryError::InvalidMetadata {
                    path: journal_path.clone(),
                    message: error.to_string(),
                })?;
            let intent = ClaimRecord::Intent(journal_path);
            for (_, record) in journal.records() {
                if record.completion.is_none()
                    && let Some(metadata) = record.operation.carried_metadata()
                {
                    claims.extend(port_claims(&repo, metadata, &intent));
                }
            }
            // A block is held attached or detached: kernel mount faults do not refuse every new.
            let published = self.source.project_metadata(&self.storage, &repo)?;
            let active_names = published
                .iter()
                .map(|metadata| &metadata.workspace)
                .collect::<BTreeSet<_>>();
            for metadata in &published {
                claim_granted_blocks(&mut claims, &repo, metadata, &ClaimRecord::Published)?;
            }
            // Incomplete clones cannot be served, but a canonical pending payload owns its
            // stored grant even after its creator exits.
            if !active_names.contains(&WorkspaceName::main())
                && let Some(image) = existing_main_image(&layout)?
                && let Some(metadata) =
                    Self::pending_metadata(store, &repo, &WorkspaceName::main(), &image)?
            {
                let record = ClaimRecord::PendingFence(sidecar_path(&image));
                claim_granted_blocks(&mut claims, &repo, &metadata, &record)?;
            }
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
                if !active_names.contains(image.workspace())
                    && let Some(metadata) =
                        Self::pending_metadata(store, &repo, image.workspace(), image.path())?
                {
                    let record = ClaimRecord::PendingFence(sidecar_path(image.path()));
                    claim_granted_blocks(&mut claims, &repo, &metadata, &record)?;
                }
            }
            // A sidecar names its workspace's grant wherever its image is: a fence whose payload
            // is still staged, a legacy session image, an image re-adoption retained or staged.
            // The canonical images above are read already.
            for (directory, canonical) in [
                (project.clone(), true),
                (sessions.clone(), true),
                (
                    project.join(crate::storage::recovery::RETAINED_NAMESPACE),
                    false,
                ),
                (
                    project.join(crate::storage::recovery::STAGING_NAMESPACE),
                    false,
                ),
            ] {
                for image in sidecar_images(&directory)? {
                    if canonical
                        && cowshed_apfs::image::is_image_path(&image)
                        && fs::symlink_metadata(&image).is_ok()
                    {
                        continue;
                    }
                    let sidecar = sidecar_path(&image);
                    // Read as the sidecar it is: `read_for_image` refuses a sidecar beside a file
                    // that is not a RAW image.
                    let read = verify_no_symlinks(store, &sidecar)
                        .map_err(|error| error.to_string())
                        .and_then(|()| {
                            crate::metadata::read_json::<DetachedWorkspaceMetadata>(&sidecar)
                                .map_err(|error| error.to_string())
                        });
                    match read {
                        Ok(metadata) if metadata.repo_id == repo => {
                            let record = ClaimRecord::Sidecar(sidecar);
                            claims.extend(port_claims(&repo, &metadata, &record));
                        }
                        // A record naming another repository cannot name a party of this
                        // project's collision: it is reported, not trusted.
                        Ok(metadata) => unreadable.push((
                            sidecar,
                            format!(
                                "it names repository {}, not the project it sits in",
                                metadata.repo_id
                            ),
                        )),
                        Err(error) => unreadable.push((sidecar, error)),
                    }
                }
            }
        }
        Ok(PortClaims { claims, unreadable })
    }

    /// The metadata of a canonical image nothing has published: its pending fence, or `None` for
    /// an orphan session image that has no sidecar.
    fn pending_metadata(
        store_root: &Path,
        repo: &RepoId,
        workspace: &WorkspaceName,
        image: &Path,
    ) -> Result<Option<DetachedWorkspaceMetadata>, GatewayInventoryError> {
        verify_image_path(store_root, image)?;
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
            cowshed_os::eprintln!(
                "cowshed: skipping orphan session image {}: missing {}",
                image.display(),
                sidecar.display()
            );
            return Ok(None);
        }
        // Read the record before checking publication, just like canonical storage inventory.
        // Retirement commits at image removal; leftover or concurrently removed companions
        // cannot reserve a port block once that canonical image has vanished.
        let metadata = DetachedWorkspaceMetadata::read_for_image(image);
        if !image
            .try_exists()
            .map_err(|source| io_error("inspecting pending image publication", image, source))?
        {
            return Ok(None);
        }
        let metadata = metadata.map_err(|error| GatewayInventoryError::InvalidMetadata {
            path: sidecar.clone(),
            message: error.to_string(),
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
        Ok(Some(metadata))
    }

    fn all_attached_blocking(&self) -> Result<AttachedInventory, GatewayInventoryError> {
        let repositories = discover_repositories(self.storage.store())?;
        let mut inventory = AttachedInventory::default();
        for repo in repositories {
            match self.load_project(&repo) {
                Ok(project) => {
                    inventory.attached.extend(project.attached);
                    inventory.held.extend(project.held);
                }
                Err(error) => {
                    // Store-wide identity collisions stay fatal. A single project's
                    // mount/metadata mismatch is a doctor finding and must not take
                    // the RunAtLoad gateway down with it.
                    if matches!(
                        error,
                        GatewayInventoryError::DuplicateRepository(_)
                            | GatewayInventoryError::AmbiguousProjectRoot(_)
                            | GatewayInventoryError::ForeignBinding { .. }
                    ) {
                        return Err(error);
                    }
                    cowshed_os::eprintln!(
                        "cowshed: skipping {repo}: its workspace records could not be read: {error}"
                    );
                }
            }
        }
        inventory.attached.sort_by(|left, right| {
            (&left.repo_id, &left.workspace).cmp(&(&right.repo_id, &right.workspace))
        });
        Ok(inventory)
    }

    fn project_attached_blocking(
        &self,
        repo_id: &RepoId,
    ) -> Result<AttachedInventory, GatewayInventoryError> {
        if !validate_requested_repository(self.storage.store(), repo_id)? {
            return Ok(AttachedInventory::default());
        }
        let mut inventory = self.load_project(repo_id)?;
        inventory
            .attached
            .sort_by(|left, right| left.workspace.cmp(&right.workspace));
        Ok(inventory)
    }

    fn workspace_attached_blocking(
        &self,
        repo_id: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<GatewaySessionFact>, GatewayInventoryError> {
        if !validate_requested_repository(self.storage.store(), repo_id)? {
            return Ok(None);
        }
        let Some((derived, metadata, mount)) =
            self.source
                .workspace_snapshot(&self.storage, repo_id, workspace)?
        else {
            return Ok(None);
        };
        if !matches!(derived.mount_state, MountState::Mounted { .. }) {
            return Ok(None);
        }
        let layout = StorageLayout::new(self.storage.store(), repo_id).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path: self.storage.store().to_owned(),
                message: error.to_string(),
            }
        })?;
        let owned_repo_ids = load_binding_candidate(
            self.storage.store(),
            &layout.project().project_root,
            &layout.project().repository_binding,
        )?;
        let project_policy = &layout.project().policy;
        let project_grants = crate::project_policy::ProjectPolicy::read(project_policy)
            .map_err(|error| GatewayInventoryError::InvalidMetadata {
                path: project_policy.clone(),
                message: error.to_string(),
            })?
            .grants;
        self.load_session_fact(
            &layout,
            &owned_repo_ids,
            &project_grants,
            derived,
            metadata,
            &mount,
        )
        .map(Some)
    }

    fn load_derived(&self, repo_id: &RepoId) -> Result<DerivedInventory, GatewayInventoryError> {
        let authoritative = self.source.project_facts(&self.storage, repo_id)?;
        reject_duplicate_mount_facts(&authoritative.mounts)?;
        let derived = derive_workspaces(
            authoritative.storage,
            authoritative.mounts,
            authoritative.checkpoints,
        )?;
        // Every reader of a project's inventory says each unproven workspace by name; the
        // derivation keeps it, so each decision below meets it as its own state.
        for workspace in &derived {
            if let MountState::Unproven(refusal) = &workspace.mount_state {
                cowshed_os::eprintln!(
                    "cowshed: {repo_id}/{} is neither attached nor detached: {refusal}; next: cowshed doctor",
                    workspace.workspace.name()
                );
            }
        }
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
        let mut unproven = Vec::new();
        for workspace in derived {
            let volume = crate::storage::apfs::volume_key(repo_id, workspace.workspace.name());
            let mount =
                mount_paths
                    .get(&volume)
                    .ok_or_else(|| GatewayInventoryError::InvalidMetadata {
                        path: layout.project().project_root.clone(),
                        message: format!("missing canonical mount path for {volume}"),
                    })?;
            // Neither attached nor detached, so no row: named, and said by `load_derived`.
            match &workspace.mount_state {
                MountState::Unproven(refusal) => {
                    unproven.push(UnprovenWorkspace::of(refusal, mount.clone()));
                    continue;
                }
                MountState::Mounted { .. } | MountState::Detached => {}
            }
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
        unproven.sort_by(|left, right| left.workspace.cmp(&right.workspace));
        Ok(ProjectWorkspaces {
            repo_id: repo_id.clone(),
            workspaces,
            unproven,
        })
    }
    fn load_project(&self, repo_id: &RepoId) -> Result<AttachedInventory, GatewayInventoryError> {
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
        let mut inventory = AttachedInventory::default();
        for workspace in derived {
            match workspace.mount_state {
                MountState::Mounted { .. } => {}
                MountState::Detached => continue,
                MountState::Unproven(_) => {
                    inventory.held.push(HeldWorkspace {
                        repo_id: repo_id.clone(),
                        workspace: workspace.workspace.name().clone(),
                    });
                    continue;
                }
            }
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
            inventory.attached.push(self.load_session_fact(
                &layout,
                &owned_repo_ids,
                &project_grants,
                workspace,
                metadata,
                mount,
            )?);
        }
        Ok(inventory)
    }

    fn load_session_fact(
        &self,
        layout: &StorageLayout,
        owned_repo_ids: &OwnedRepoIds,
        project_grants: &crate::project_policy::ProjectGrants,
        workspace: DerivedWorkspace,
        metadata: DetachedWorkspaceMetadata,
        mount: &Path,
    ) -> Result<GatewaySessionFact, GatewayInventoryError> {
        let MountState::Mounted { mount_id } = workspace.mount_state else {
            return Err(GatewayInventoryError::InvalidMetadata {
                path: mount.to_owned(),
                message: "gateway session workspace is not mounted".to_owned(),
            });
        };
        let image_paths = canonical_image_paths(layout, &workspace.workspace)?;
        let port_block =
            metadata
                .grants
                .port_block
                .ok_or_else(|| GatewayInventoryError::MissingPortBlock {
                    repo: workspace.workspace.repo().clone(),
                    workspace: workspace.workspace.name().clone(),
                })?;
        let credentials = read_gateway_workspace_credentials(
            owned_repo_ids,
            &workspace.workspace,
            mount,
            image_paths.ca_private_key(),
        )?;
        let project_policy = &layout.project().policy;
        let grants = crate::project_policy::effective_grants(&metadata.grants, project_grants)
            .map_err(|error| GatewayInventoryError::InvalidMetadata {
                path: project_policy.clone(),
                message: error.to_string(),
            })?;
        Ok(GatewaySessionFact {
            repo_id: workspace.workspace.repo().clone(),
            workspace: workspace.workspace.name().clone(),
            incarnation: workspace.workspace.incarnation().clone(),
            revision: grants.revision,
            created_at: metadata.info_snapshot.created_at.clone(),
            mount_id,
            mount: mount.to_owned(),
            grants,
            port_block,
            credentials,
        })
    }
}

/// Enumerate the projects in the store, skipping everything that is not one.
///
/// Only the owners cowshed marked are read (`bootstrap::project_owner_directories`): whatever
/// else sits in the store root — a volume, a macOS system directory, a directory someone put
/// there for safe-keeping — costs one `lstat` and is never listed. Measured 2026-10-20: six
/// telemetry archives moved into the store root, one holding 3,070,019 files, made every command
/// that discovers projects probe each of them for a volume marker and a binding; `cowshed path
/// main` waited more than 18 minutes, and with them moved under a dotted name it answered in 0.19 s.
/// Within an owner, discovery is per-entry isolated for the same reason healing is (see `heal`):
/// a candidate that cannot be read is skipped; a candidate that reads as a real but broken project
/// still raises, because that is cowshed's own state and hiding it would hide corruption.
pub(crate) fn discover_repositories(
    store_root: &Path,
) -> Result<Vec<RepoId>, GatewayInventoryError> {
    Ok(discover_owned_identities(store_root)?
        .into_iter()
        .map(|owned| owned.current().clone())
        .collect())
}

/// Read-only recovery admission, not active inventory or repository identity ownership.
/// A pending mutating main restore and its exact recorded checkout jointly authorize discovery.
fn pending_restore_for_checkout(
    store_root: &Path,
    checkout: &Path,
) -> Result<Option<RepoId>, GatewayInventoryError> {
    use crate::storage::recovery::{
        LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentJournal, LifecycleIntentPhase,
    };
    let mut matched = None;
    for owner in crate::storage::bootstrap::project_owner_directories(store_root)? {
        let Some(owner_name) = owner.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        for project in read_directory(&owner, "enumerating restore continuations")? {
            let Some(project_name) = project.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if crate::storage::bootstrap::is_reserved_store_namespace(project_name)
                || !is_directory(&project)?
            {
                continue;
            }
            let Ok(repo) = RepoId::parse(&format!("{owner_name}/{project_name}")) else {
                continue;
            };
            let layout = StorageLayout::new(store_root, &repo).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: project.clone(),
                    message: error.to_string(),
                }
            })?;
            if layout.project().project_root != project
                || binding_path_exists(&layout.project().repository_binding)?
            {
                continue;
            }
            let recorded = layout.recorded_checkout_root().map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: layout.project().checkout_root.clone(),
                    message: error.to_string(),
                }
            })?;
            if !recorded
                .and_then(|root| fs::canonicalize(root).ok())
                .is_some_and(|root| root == checkout)
            {
                continue;
            }
            let journal_path = project.join(LIFECYCLE_INTENTS_FILE);
            let journal = LifecycleIntentJournal::load(&journal_path).map_err(|error| {
                GatewayInventoryError::InvalidMetadata {
                    path: journal_path.clone(),
                    message: error.to_string(),
                }
            })?;
            let pending_restore = journal.get(&WorkspaceName::main()).is_some_and(|record| {
                record.phase == LifecycleIntentPhase::Mutating && record.completion.is_none()
                    && matches!(&record.operation, LifecycleIntent::Retire { workspace, options, .. }
                        if workspace.is_main() && options.restore)
            });
            if pending_restore && matched.replace(repo).is_some() {
                return Err(GatewayInventoryError::AmbiguousProjectRoot(
                    checkout.to_owned(),
                ));
            }
        }
    }
    Ok(matched)
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
    for owner in crate::storage::bootstrap::project_owner_directories(store_root)? {
        let Ok(projects) = read_directory(&owner, "enumerating owner repositories") else {
            continue;
        };
        for project in projects {
            let Some(project_name) = project.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if crate::storage::bootstrap::is_reserved_store_namespace(project_name)
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

/// Resolve only the exact checkout owned by a strict unfinished replacement record.
/// A checkout-root record without this journal authority is not a mount/recovery grant.
pub fn forming_re_adoption_checkout(
    layout: &StorageLayout,
    repo: &RepoId,
    checkout: &Path,
) -> Result<Option<PathBuf>, GatewayInventoryError> {
    let path = layout
        .project()
        .project_root
        .join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE);
    let journal =
        crate::storage::recovery::LifecycleIntentJournal::load(&path).map_err(|error| {
            GatewayInventoryError::InvalidMetadata {
                path,
                message: error.to_string(),
            }
        })?;
    Ok(journal
        .forming_re_adoptions()
        .find(|source| source.checkout == checkout && source.metadata.repo_id == *repo)
        .map(|source| source.checkout.clone()))
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
        MountState::Unproven(refusal) => Some(refusal.to_string()),
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

fn reject_duplicate_mount_facts(mounts: &[MountJoin]) -> Result<(), GatewayInventoryError> {
    let mut volumes = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for mount in mounts {
        let unique_id = match mount {
            MountJoin::Proven(fact) => ids.insert(fact.mount_id),
            MountJoin::Refused { .. } => true,
        };
        if !volumes.insert(mount.volume_key()) || !unique_id {
            return Err(GatewayInventoryError::AmbiguousMount(
                mount.volume_key().to_owned(),
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

/// Verify one inventory image path. The native rule stands: every component, the image
/// included, must be reached through no symlink. The one explicit exception is a directory
/// store's mount record: while a directory payload is mounted, its image path is a symlink
/// naming the mountpoint, and inventory may follow it only when the binding proves itself —
/// the record names an absolute directory that resolves to itself, and the workspace marker
/// inside that directory names the same repo, workspace and incarnation as the sidecar beside
/// the image. A symlink that proves none of this is refused exactly as a native one is.
fn verify_image_path(store_root: &Path, image: &Path) -> Result<(), GatewayInventoryError> {
    let invalid = |path: &Path, message: String| GatewayInventoryError::InvalidMetadata {
        path: path.to_owned(),
        message,
    };
    match fs::symlink_metadata(image) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            if let Some(parent) = image.parent() {
                verify_no_symlinks(store_root, parent)
                    .map_err(|error| invalid(image, error.to_string()))?;
            }
            let target = fs::read_link(image)
                .map_err(|source| io_error("read image mount record", image, source))?;
            if !target.is_absolute() {
                return Err(invalid(
                    image,
                    format!(
                        "image mount record names the relative path {}",
                        target.display()
                    ),
                ));
            }
            let resolved = fs::canonicalize(&target)
                .map_err(|source| io_error("resolve image mount target", &target, source))?;
            if resolved != target
                || !fs::symlink_metadata(&target)
                    .is_ok_and(|metadata| metadata.file_type().is_dir())
            {
                return Err(invalid(
                    image,
                    format!(
                        "image mount record names {}, which is not the directory it resolves to",
                        target.display()
                    ),
                ));
            }
            let sidecar = sidecar_path(image);
            verify_no_symlinks(store_root, &sidecar)
                .map_err(|error| invalid(&sidecar, error.to_string()))?;
            let metadata = DetachedWorkspaceMetadata::read_for_image(image)
                .map_err(|error| invalid(&sidecar, error.to_string()))?;
            let marker = crate::metadata::WorkspaceMarker::read_from(
                &target.join(crate::storage::WORKSPACE_MARKER_PATH),
            )
            .map_err(|error| invalid(image, error.to_string()))?;
            if marker.repo_id != metadata.repo_id
                || marker.workspace != metadata.workspace
                || marker.workspace_incarnation != metadata.workspace_incarnation
            {
                return Err(invalid(
                    image,
                    "mounted payload marker does not match its image sidecar".to_owned(),
                ));
            }
            Ok(())
        }
        _ => {
            verify_no_symlinks(store_root, image).map_err(|error| invalid(image, error.to_string()))
        }
    }
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
    verify_image_path(store_root, image)?;
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
    use crate::storage::lifecycle::{KernelMountFact, LifecycleWorkspace, Pin, Revision};
    use crate::workspace_credentials::mint_workspace_credentials;
    use cowshed_os::namespace::TestNamespace;

    use super::*;

    #[derive(Default)]
    struct FixtureSource {
        projects: Mutex<BTreeMap<RepoId, ProjectInventoryFacts>>,
        /// Projects whose kernel mount reading fails the way a volume without the workspace's
        /// marker, mounted at its checkout, fails it. Their records stay readable.
        unidentifiable_mounts: Mutex<BTreeSet<RepoId>>,
    }

    impl FixtureSource {
        fn recorded(&self, repo: &RepoId) -> Result<ProjectInventoryFacts, GatewayInventoryError> {
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
        fn canonical_metadata(
            &self,
            storage: &ValidatedHostStorage,
            repo: &RepoId,
            workspace: &WorkspaceName,
        ) -> Result<Option<DetachedWorkspaceMetadata>, GatewayInventoryError> {
            let layout = StorageLayout::new(storage.store(), repo).expect("fixture layout");
            let image = layout
                .canonical_image(workspace)
                .expect("fixture canonical image");
            if !image.image().exists() {
                return Ok(None);
            }
            let metadata =
                DetachedWorkspaceMetadata::read_for_image(image.image()).map_err(|error| {
                    GatewayInventoryError::InvalidMetadata {
                        path: sidecar_path(image.image()),
                        message: error.to_string(),
                    }
                })?;
            if metadata.repo_id != *repo || metadata.workspace != *workspace {
                return Err(GatewayInventoryError::InvalidMetadata {
                    path: sidecar_path(image.image()),
                    message: "fixture metadata names another workspace".to_owned(),
                });
            }
            Ok(Some(metadata))
        }
    }

    impl InventorySource for FixtureSource {
        fn project_metadata(
            &self,
            storage: &ValidatedHostStorage,
            repo: &RepoId,
        ) -> Result<Vec<DetachedWorkspaceMetadata>, GatewayInventoryError> {
            let published = self.recorded(repo)?.storage;
            let mut records = Vec::with_capacity(published.len());
            for fact in published {
                if let Some(metadata) =
                    self.canonical_metadata(storage, repo, fact.workspace.name())?
                {
                    records.push(metadata);
                }
            }
            Ok(records)
        }

        fn project_facts(
            &self,
            _storage: &ValidatedHostStorage,
            repo: &RepoId,
        ) -> Result<ProjectInventoryFacts, GatewayInventoryError> {
            if self
                .unidentifiable_mounts
                .lock()
                .expect("fixture source")
                .contains(repo)
            {
                return Err(GatewayInventoryError::Apfs(ApfsStorageError::Host(
                    format!(
                        "workspace mount identity mismatch: {repo}'s checkout carries no marker"
                    ),
                )));
            }
            self.recorded(repo)
        }

        fn workspace_snapshot(
            &self,
            storage: &ValidatedHostStorage,
            repo: &RepoId,
            workspace: &WorkspaceName,
        ) -> Result<
            Option<(DerivedWorkspace, DetachedWorkspaceMetadata, PathBuf)>,
            GatewayInventoryError,
        > {
            let recorded = self.recorded(repo)?;
            let Some(fact) = recorded
                .storage
                .into_iter()
                .find(|fact| fact.workspace.name() == workspace)
            else {
                return Ok(None);
            };
            let layout = StorageLayout::new(storage.store(), repo).expect("fixture layout");
            let image = canonical_image_paths(&layout, &fact.workspace)?;
            if !image.image().exists() {
                return Ok(None);
            }
            let metadata = read_current_metadata(storage.store(), image.image(), &fact.workspace)?;
            let mounts: Vec<_> = recorded
                .mounts
                .into_iter()
                .filter(|mount| mount.volume_key() == fact.volume_key)
                .collect();
            let mount =
                workspace_mountpoint(&layout, &metadata.info_snapshot.project_root, workspace)?;
            let derived = derive_workspaces(vec![fact], mounts, Vec::new())?
                .pop()
                .expect("one selected workspace");
            Ok(Some((derived, metadata, mount)))
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
            let roots = CanonicalRoots::fixture(
                TestNamespace::mint().expect("a test namespace"),
                root.join("store"),
            );
            fs::create_dir_all(roots.store()).expect("fixture store");
            fs::create_dir_all(roots.telemetry()).expect("fixture telemetry");
            Self {
                root,
                storage: ValidatedHostStorage::new(home, Vec::new(), roots),
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
            let layout = StorageLayout::new(self.storage.store(), repo).expect("project paths");
            // Adoption's own provisioning, which marks the owner discovery reads.
            layout.provision_project().expect("project root");
            let paths = layout.project().clone();
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

        // A volume root whose name is not on any reserved list. Its child carries a binding, but
        // nothing marked the volume a project owner, so it is never read as the bogus repository
        // `scratch-volume/anything`.
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

    /// `re_adoption_primary_for_checkout` names the recorded primary through a RAW main and an
    /// ambiguous remote binding, and names nothing when the owner holds no RAW `main.dmg`. Each
    /// single mutation of the facts it reads fails on the check that reads that fact, every mutation
    /// starts from the restored world, and the read leaves every source byte as it found it.
    #[tokio::test]
    async fn re_adoption_primary_names_the_raw_main_and_refuses_each_mutation() {
        let fixture = Fixture::new("re-adoption-lifecycle");
        let repo = RepoId::parse("acme/alpha").expect("repo");
        let fork = RepoId::parse("acme/alpha-fork").expect("fork identity");
        let other = RepoId::parse("acme/beta").expect("other repo");
        let unowned = RepoId::parse("acme/gamma").expect("unowned repo");
        let incarnation = "00000000000000000000000000000001";
        fixture.bind(&repo);
        let (_storage, mounted) = fixture.workspace(
            &repo,
            WorkspaceName::new("main").expect("main"),
            incarnation,
            3,
            true,
        );
        let (_mount, checkout) = mounted.expect("mounted main");
        let layout = StorageLayout::new(fixture.storage.store(), &repo).expect("layout");
        let project = layout.project().clone();
        let raw = layout.main_image().expect("main image").image().to_owned();
        let raw_sidecar = crate::metadata::sidecar_path(&raw);
        let aside = project.project_root.join("main-aside.dmg");
        let aside_sidecar = crate::metadata::sidecar_path(&aside);

        let binding = RepositoryBinding::new(vec![
            BoundIdentity {
                repo_id: repo.clone(),
                remote_name: None,
                remote_url: None,
                primary: true,
            },
            BoundIdentity {
                repo_id: fork.clone(),
                remote_name: Some(String::from("origin")),
                remote_url: Some(String::from("https://example.invalid/acme/alpha-fork.git")),
                primary: false,
            },
        ])
        .expect("ambiguous binding");
        write_json(&project.repository_binding, &binding).expect("binding file");

        let marker = crate::metadata::WorkspaceMarker {
            version: crate::metadata::MARKER_VERSION,
            repo_id: repo.clone(),
            project_root: checkout.clone(),
            workspace: WorkspaceName::new("main").expect("main"),
            workspace_incarnation: crate::metadata::WorkspaceIncarnation::new(incarnation)
                .expect("incarnation"),
            role: WorkspaceRole::Main,
            base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            created_at: "2026-07-14T00:00:00Z".to_owned(),
            forked_from: None,
            created_trace: "re-adoption-lifecycle".to_owned(),
            lineage: Some(Vec::new()),
            owned_links_revision: 0,
        };
        let marker_path = checkout.join(crate::storage::WORKSPACE_MARKER_PATH);
        fs::create_dir_all(marker_path.parent().expect("marker parent")).expect("marker dir");
        write_json(&marker_path, &marker).expect("marker");

        let original_marker = fs::read(&marker_path).expect("marker bytes");
        let original_binding = fs::read(&project.repository_binding).expect("binding bytes");
        let original_image = fs::read(&raw).expect("image bytes");
        let original_sidecar = fs::read(&raw_sidecar).expect("sidecar bytes");
        let restore = || {
            fs::write(&marker_path, &original_marker).expect("restore marker");
            fs::write(&project.repository_binding, &original_binding).expect("restore binding");
            fs::write(&raw, &original_image).expect("restore image");
            fs::write(&raw_sidecar, &original_sidecar).expect("restore sidecar");
        };
        // Every refusal below is the check it names, reported against the marker it judged.
        let refusal = |result: Result<Option<RepoId>, GatewayInventoryError>| match result {
            Err(GatewayInventoryError::InvalidMetadata { path, message })
                if path == marker_path =>
            {
                message
            }
            other => panic!(
                "expected a refusal of {}, got {other:?}",
                marker_path.display()
            ),
        };
        let inventory = NativeGatewayInventory::new(fixture.storage.clone());

        // RAW main with an ambiguous remote binding names the recorded primary.
        assert_eq!(
            inventory
                .re_adoption_primary_for_checkout(&checkout)
                .await
                .expect("raw primary"),
            Some(repo.clone())
        );

        // With no RAW `main.dmg` the store binds nothing, so adopt step 1 names the identity.
        fs::rename(&raw, &aside).expect("move main image aside");
        fs::rename(&raw_sidecar, &aside_sidecar).expect("move main sidecar aside");
        assert_eq!(
            inventory
                .re_adoption_primary_for_checkout(&checkout)
                .await
                .expect("no raw main"),
            None
        );
        fs::rename(&aside, &raw).expect("return main image");
        fs::rename(&aside_sidecar, &raw_sidecar).expect("return main sidecar");
        assert_eq!(fs::read(&marker_path).expect("marker"), original_marker);
        assert_eq!(
            fs::read(&project.repository_binding).expect("binding"),
            original_binding
        );

        // `main.dmg` exists but is not a regular file.
        fs::remove_file(&raw).expect("remove main image");
        fs::create_dir(&raw).expect("main image directory");
        let message = refusal(inventory.re_adoption_primary_for_checkout(&checkout).await);
        assert!(message.ends_with("is not a regular file"), "{message}");
        fs::remove_dir(&raw).expect("remove main image directory");
        restore();

        // The marker names a workspace other than main.
        let mut demoted = marker.clone();
        demoted.workspace = WorkspaceName::new("feature").expect("workspace");
        demoted.role = WorkspaceRole::Workspace;
        write_json(&marker_path, &demoted).expect("demoted marker");
        let message = refusal(inventory.re_adoption_primary_for_checkout(&checkout).await);
        assert!(message.ends_with("not the main workspace"), "{message}");
        restore();

        // The marker records another checkout.
        let mut displaced = marker.clone();
        displaced.project_root = fixture.root.clone();
        write_json(&marker_path, &displaced).expect("displaced marker");
        let message = refusal(inventory.re_adoption_primary_for_checkout(&checkout).await);
        assert!(
            message.ends_with("not the checkout it sits in"),
            "{message}"
        );
        restore();

        // The marker names a repository no adopted project owns.
        let mut stray = marker.clone();
        stray.repo_id = unowned.clone();
        write_json(&marker_path, &stray).expect("stray marker");
        let message = refusal(inventory.re_adoption_primary_for_checkout(&checkout).await);
        assert!(
            message.ends_with("which no adopted project owns"),
            "{message}"
        );
        restore();

        // The main sidecar is not the Active main.
        let mut fenced = DetachedWorkspaceMetadata::read_for_image(&raw).expect("sidecar");
        fenced.publication_state = PublicationState::PendingFence;
        fenced.write_for_image(&raw).expect("fenced sidecar");
        let message = refusal(inventory.re_adoption_primary_for_checkout(&checkout).await);
        assert!(message.contains("is not the active main of"), "{message}");
        restore();

        // Incarnation drift between marker and sidecar.
        let mut drifted = marker.clone();
        drifted.workspace_incarnation =
            crate::metadata::WorkspaceIncarnation::new("00000000000000000000000000000002")
                .expect("other incarnation");
        write_json(&marker_path, &drifted).expect("drifted marker");
        let message = refusal(inventory.re_adoption_primary_for_checkout(&checkout).await);
        assert!(
            message.starts_with("main sidecar records incarnation"),
            "{message}"
        );
        restore();

        // The sidecar records a different checkout root.
        let mut moved = DetachedWorkspaceMetadata::read_for_image(&raw).expect("sidecar");
        moved.info_snapshot.project_root = fixture.root.clone();
        moved.write_for_image(&raw).expect("moved sidecar");
        let message = refusal(inventory.re_adoption_primary_for_checkout(&checkout).await);
        assert!(message.ends_with("not this checkout"), "{message}");
        restore();

        // A second project's RAW main claims the same checkout.
        fixture.bind(&other);
        let (_other_storage, _) = fixture.workspace(
            &other,
            WorkspaceName::new("main").expect("main"),
            "00000000000000000000000000000003",
            4,
            false,
        );
        let other_layout = StorageLayout::new(fixture.storage.store(), &other).expect("layout");
        let other_root = other_layout.project().project_root.clone();
        let other_raw = other_layout
            .main_image()
            .expect("main image")
            .image()
            .to_owned();
        let mut claiming = DetachedWorkspaceMetadata::read_for_image(&other_raw).expect("sidecar");
        claiming.info_snapshot.project_root = checkout.clone();
        claiming
            .write_for_image(&other_raw)
            .expect("claiming sidecar");
        let result = inventory.re_adoption_primary_for_checkout(&checkout).await;
        assert!(
            matches!(result, Err(GatewayInventoryError::AmbiguousProjectRoot(_))),
            "a second project's main claiming the checkout must be ambiguous, got {result:?}"
        );
        fs::remove_dir_all(&other_root).expect("remove second project");
        restore();

        // Restored, the primary is named again.
        assert_eq!(
            inventory
                .re_adoption_primary_for_checkout(&checkout)
                .await
                .expect("restored primary"),
            Some(repo.clone())
        );
        assert_eq!(fs::read(&raw).expect("image"), original_image);
        assert_eq!(fs::read(&raw_sidecar).expect("sidecar"), original_sidecar);
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
                    mounts: vec![mount.clone().into()],
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

    /// A workspace whose mount does not prove itself the workspace's is held by name (01a_apfs.md,
    /// "Ownership, identity, and the volume label"). Its project's inventory still serves every
    /// other workspace instead of failing whole, and the held workspace is not served.
    #[tokio::test]
    async fn an_unproven_workspace_is_held_and_never_fails_its_project() {
        let fixture = Fixture::new("unproven-held");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let unproven = WorkspaceName::new("ci-fhs-tools").expect("unproven");
        let source = Arc::new(FixtureSource::default());
        let mut facts = ProjectInventoryFacts {
            storage: Vec::new(),
            mounts: Vec::new(),
            checkpoints: Vec::new(),
            mount_paths: BTreeMap::new(),
        };
        // Each revision differs, so each fixture mount id (revision + 100) is the workspace's own.
        for (name, incarnation, revision) in [
            (WorkspaceName::main(), "11111111111111111111111111111111", 7),
            (
                WorkspaceName::new("raven").expect("raven"),
                "22222222222222222222222222222222",
                8,
            ),
            (unproven.clone(), "f221ac9777c546caba9b48a20f9e68b4", 9),
        ] {
            let (storage, mounted) =
                fixture.workspace(&repo, name.clone(), incarnation, revision, true);
            let (mount, path) = mounted.expect("mounted");
            facts
                .mount_paths
                .insert(mount.volume_key.clone(), path.clone());
            facts.mounts.push(if name == unproven {
                MountJoin::Refused {
                    volume_key: mount.volume_key,
                    refusal: Box::new(crate::storage::lifecycle::MountRefusal {
                        workspace: storage.workspace.clone(),
                        mount_point: path,
                        mounted_from: "/dev/disk41s1".to_owned(),
                        reason: crate::storage::lifecycle::MountRefusalReason::MarkerUnreadable(
                            "No such file or directory (os error 2)".to_owned(),
                        ),
                    }),
                }
            } else {
                mount.into()
            });
            facts.storage.push(storage);
        }
        source
            .projects
            .lock()
            .expect("source")
            .insert(repo.clone(), facts);
        let inventory = NativeGatewayInventory::with_source(fixture.storage.clone(), source);

        let project = inventory
            .project_attached(&repo)
            .await
            .expect("one unproven workspace never fails its project");
        assert_eq!(
            project
                .attached
                .iter()
                .map(|fact| fact.workspace.as_str())
                .collect::<Vec<_>>(),
            ["main", "raven"]
        );
        assert_eq!(
            project.held,
            [HeldWorkspace {
                repo_id: repo.clone(),
                workspace: unproven,
            }]
        );
        let host = inventory.all_attached().await.expect("host inventory");
        assert_eq!(host.held, project.held);

        // A row's state is attached or detached (07_api.md), so the listing gives the unproven
        // workspace no row, names it with its refusal, and still lists every other workspace.
        let listing = inventory
            .load_project_workspaces(&repo)
            .expect("one unproven workspace never fails its project's listing");
        assert_eq!(
            listing
                .workspaces
                .iter()
                .map(|info| info.workspace.as_str())
                .collect::<Vec<_>>(),
            ["main", "raven"]
        );
        let [held] = listing.unproven.as_slice() else {
            panic!(
                "exactly the unproven workspace is named: {:?}",
                listing.unproven
            );
        };
        assert_eq!(held.workspace.as_str(), "ci-fhs-tools");
        assert_eq!(
            held.workspace_incarnation.as_str(),
            "f221ac9777c546caba9b48a20f9e68b4"
        );
        assert!(
            held.reason
                .contains("No such file or directory (os error 2)"),
            "{}",
            held.reason
        );
    }

    #[tokio::test]
    async fn workspace_inventory_ignores_malformed_and_removed_sibling_grants() {
        let fixture = Fixture::new("named-session");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let selected = WorkspaceName::new("raven").expect("selected");
        let sibling = WorkspaceName::new("retired").expect("sibling");
        let source = Arc::new(FixtureSource::default());
        let mut facts = ProjectInventoryFacts {
            storage: Vec::new(),
            mounts: Vec::new(),
            checkpoints: Vec::new(),
            mount_paths: BTreeMap::new(),
        };
        for (name, incarnation) in [
            (WorkspaceName::main(), "11111111111111111111111111111111"),
            (selected.clone(), "22222222222222222222222222222222"),
            (sibling.clone(), "33333333333333333333333333333333"),
        ] {
            let (storage, mounted) = fixture.workspace(&repo, name, incarnation, 7, true);
            let (mount, path) = mounted.expect("mounted");
            facts.mount_paths.insert(mount.volume_key.clone(), path);
            facts.mounts.push(mount.into());
            facts.storage.push(storage);
        }
        source
            .projects
            .lock()
            .expect("source")
            .insert(repo.clone(), facts);
        let inventory = NativeGatewayInventory::with_source(fixture.storage.clone(), source);
        let layout = StorageLayout::new(fixture.storage.store(), &repo).expect("layout");
        let main = layout
            .canonical_image(&WorkspaceName::main())
            .expect("main");
        let removed = layout.canonical_image(&sibling).expect("sibling image");
        fs::write(sidecar_path(main.image()), b"malformed sibling grants").expect("bad main");
        fs::remove_file(sidecar_path(removed.image())).expect("remove sibling grants");
        assert!(inventory.project_attached(&repo).await.is_err());
        let fact = inventory
            .workspace_attached(&repo, &selected)
            .await
            .expect("siblings do not refuse the selected session")
            .expect("selected session is mounted");
        assert_eq!(fact.workspace, selected);
        assert_eq!(fact.revision, 7);
        fs::remove_file(removed.image()).expect("retire sibling image");
        assert!(
            inventory
                .workspace_attached(&repo, &selected)
                .await
                .expect("retired sibling is not read")
                .is_some()
        );

        // Scoped inventory still refuses corrupt authority and the selected workspace's grants.
        let policy = &layout.project().policy;
        fs::write(policy, b"malformed project policy").expect("bad policy");
        assert!(
            inventory
                .workspace_attached(&repo, &selected)
                .await
                .is_err()
        );
        fs::remove_file(policy).expect("remove bad policy");
        let binding = &layout.project().repository_binding;
        let original_binding = fs::read(binding).expect("binding bytes");
        fs::write(binding, b"malformed owned binding").expect("bad binding");
        assert!(
            inventory
                .workspace_attached(&repo, &selected)
                .await
                .is_err()
        );
        fs::write(binding, original_binding).expect("restore binding");
        let selected_image = layout.canonical_image(&selected).expect("selected image");
        fs::write(
            sidecar_path(selected_image.image()),
            b"malformed own grants",
        )
        .expect("bad own grants");
        assert!(
            inventory
                .workspace_attached(&repo, &selected)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn port_reservations_skip_retired_captured_facts_but_refuse_missing_live_grants() {
        let fixture = Fixture::new("retired-port-owner");
        let (repo, _, source) = two_project_store(&fixture);
        let inventory = NativeGatewayInventory::with_source(fixture.storage.clone(), source);
        let layout = StorageLayout::new(fixture.storage.store(), &repo).expect("layout");
        let image = layout
            .canonical_image(&WorkspaceName::main())
            .expect("main image");
        fs::remove_file(sidecar_path(image.image())).expect("remove grants");
        assert!(
            inventory.all_reserved_port_blocks().await.is_err(),
            "a live canonical image without grants is still an integrity error"
        );
        fs::remove_file(image.image()).expect("retire image");
        assert_eq!(
            inventory
                .all_reserved_port_blocks()
                .await
                .expect("retired captured owner no longer reserves ports")
                .blocks()
                .count(),
            1
        );
    }

    #[test]
    fn pending_port_scan_skips_captured_images_retired_before_the_record_read() {
        let fixture = Fixture::new("retired-pending-port-owner");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        for (name, state, incarnation) in [
            (
                "active",
                PublicationState::Active,
                "11111111111111111111111111111111",
            ),
            (
                "pending",
                PublicationState::PendingFence,
                "22222222222222222222222222222222",
            ),
        ] {
            let workspace = WorkspaceName::new(name).expect("workspace");
            let (fact, _) = fixture.workspace(&repo, workspace.clone(), incarnation, 7, false);
            let layout = StorageLayout::new(fixture.storage.store(), &repo).expect("layout");
            let image = canonical_image_paths(&layout, &fact.workspace).expect("captured image");
            let mut metadata =
                DetachedWorkspaceMetadata::read_for_image(image.image()).expect("metadata");
            metadata.publication_state = state;
            metadata.write_for_image(image.image()).expect("state");
            if state == PublicationState::PendingFence {
                let pending = NativeGatewayInventory::pending_metadata(
                    fixture.storage.store(),
                    &repo,
                    &workspace,
                    image.image(),
                )
                .expect("live pending owner reads its block")
                .expect("live pending metadata");
                let mut claims = Vec::new();
                claim_granted_blocks(
                    &mut claims,
                    &repo,
                    &pending,
                    &ClaimRecord::PendingFence(sidecar_path(image.image())),
                )
                .expect("live pending owner reserves its block");
                let mut reserved = ReservedPortBlocks::default();
                for claim in claims {
                    reserved.insert(claim.block);
                }
                assert_eq!(reserved.blocks().count(), 1);
            }
            fs::remove_file(image.image()).expect("commit retirement after directory capture");
            assert!(
                NativeGatewayInventory::pending_metadata(
                    fixture.storage.store(),
                    &repo,
                    &workspace,
                    image.image(),
                )
                .expect("retained sidecar cannot reserve a retired image's block")
                .is_none()
            );
            fs::write(sidecar_path(image.image()), b"record disappeared mid-read")
                .expect("bad retired record");
            assert!(
                NativeGatewayInventory::pending_metadata(
                    fixture.storage.store(),
                    &repo,
                    &workspace,
                    image.image(),
                )
                .expect("a failed read of a retired companion contributes no block")
                .is_none()
            );
            fs::write(image.image(), b"recreated image").expect("restore canonical publication");
            assert!(
                NativeGatewayInventory::pending_metadata(
                    fixture.storage.store(),
                    &repo,
                    &workspace,
                    image.image(),
                )
                .is_err(),
                "malformed metadata beside a present image remains an error"
            );
        }
    }

    #[tokio::test]
    async fn port_reservations_use_current_recreated_owner_not_captured_incarnation() {
        let fixture = Fixture::new("recreated-port-owner");
        let (repo, _, source) = two_project_store(&fixture);
        let inventory = NativeGatewayInventory::with_source(fixture.storage.clone(), source);
        let layout = StorageLayout::new(fixture.storage.store(), &repo).expect("layout");
        let image = layout
            .canonical_image(&WorkspaceName::main())
            .expect("main image");
        let mut metadata =
            DetachedWorkspaceMetadata::read_for_image(image.image()).expect("metadata");
        let old = metadata.grants.port_block.expect("old block");
        let replacement = PortBlock::new(40_960, NEW_PORT_BLOCK_SIZE).expect("replacement block");
        metadata.workspace_incarnation =
            WorkspaceIncarnation::new("ffffffffffffffffffffffffffffffff").expect("new incarnation");
        metadata.grants.port_block = Some(replacement);
        metadata
            .write_for_image(image.image())
            .expect("publish recreated metadata");
        let reserved = inventory
            .all_reserved_port_blocks()
            .await
            .expect("current port owners");
        assert_eq!(reserved.overlapping(replacement), Some(replacement));
        assert_eq!(reserved.overlapping(old), None);
    }

    #[tokio::test]
    async fn workspace_inventory_omits_absent_and_detached_targets() {
        let fixture = Fixture::new("detached-selected");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let workspace = WorkspaceName::new("raven").expect("workspace");
        let (fact, mounted) = fixture.workspace(
            &repo,
            workspace.clone(),
            "11111111111111111111111111111111",
            7,
            false,
        );
        assert!(mounted.is_none());
        let source = Arc::new(FixtureSource::default());
        source.projects.lock().expect("source").insert(
            repo.clone(),
            ProjectInventoryFacts {
                storage: vec![fact],
                mounts: Vec::new(),
                checkpoints: Vec::new(),
                mount_paths: BTreeMap::new(),
            },
        );
        let inventory = NativeGatewayInventory::with_source(fixture.storage.clone(), source);
        assert!(
            inventory
                .workspace_attached(&repo, &workspace)
                .await
                .expect("detached target")
                .is_none()
        );
        assert!(
            inventory
                .workspace_attached(&repo, &WorkspaceName::new("absent").expect("name"))
                .await
                .expect("absent target")
                .is_none()
        );
    }

    #[tokio::test]
    async fn attached_inventory_is_sorted_complete_and_secret_redacted() {
        let fixture = Fixture::new("attached");
        let (repo_a, _repo_b, source) = two_project_store(&fixture);
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        let facts = inventory
            .all_attached()
            .await
            .expect("attached inventory")
            .attached;
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
        fs::write(sessions.join("scratch.dmg"), b"unpublished image").expect("orphan image");
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
            sidecar_path(&sessions.join("scratch.dmg")),
            b"{\"invalid\": true}",
        )
        .expect("malformed sidecar");
        assert!(
            inventory.all_reserved_port_blocks().await.is_err(),
            "present but unreadable grants cannot silently free a reserved port block"
        );
    }

    /// A volume mounted at one project's checkout that carries no marker for it — another store's
    /// image a test left behind — faults that project's mount reading, not its records. A block is
    /// held attached or detached (04_sandbox.md, `portBlock`), so the store-wide reservation reads
    /// records alone: it still holds the faulted project's block, and refuses no other project's
    /// `new` over a mount it never needed to read.
    #[tokio::test]
    async fn an_unidentifiable_mount_does_not_block_store_wide_port_reservations() {
        let fixture = Fixture::new("unidentifiable-mount-ports");
        let (_repo_a, repo_b, source) = two_project_store(&fixture);
        source
            .unidentifiable_mounts
            .lock()
            .expect("fixture source")
            .insert(repo_b);
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );
        assert_eq!(
            inventory
                .all_reserved_port_blocks()
                .await
                .expect("one project's mount fault leaves every block readable")
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
        assert!(issues[0].1.to_string().contains("main.dmg.grants.json"));
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

        let facts = inventory
            .all_attached()
            .await
            .expect("attached inventory")
            .attached;
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
            .expect("other projects still served")
            .attached;
        assert!(facts.iter().all(|fact| fact.repo_id != repo_a));
        assert_eq!(facts.len(), 1);
    }

    /// A macOS port belongs to at most one workspace host-wide: a block's base is the workspace's
    /// gateway endpoint and its other ports are the workspace's own, so two workspaces whose
    /// blocks share a port — the same block, or a live 16-port block inside another's 64 —
    /// answer on one port. The store names that as a collision the newer workspace claims, and
    /// the allocator holds every port either names. The attached listing every other reader uses
    /// still lists both; only the gateway's session path
    /// serves the older one alone, at startup and per project, so the gateway stays up.
    #[tokio::test]
    async fn one_port_claimed_twice_is_served_only_to_the_older_workspace() {
        use crate::gateway_sessions::SessionInventory as _;

        let fixture = Fixture::new("duplicate-port");
        let live = PortBlock::new(MACOS_PORT_MIN + 16, 16).expect("live 16-port block");
        fixture.plant_port_block(live);
        let (repo_a, repo_b, source) = two_project_store(&fixture);
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );

        // The fixture hands out disjoint blocks, so the collision has to be planted: beta's main
        // holds a live 16-port block inside the 64-port block alpha's main is issued. Both record
        // one creation time, so the tie goes to the repository named first: alpha keeps it.
        let fresh = PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).expect("new block");
        let PortCollisions {
            collisions,
            unreadable,
        } = inventory.port_collisions().await.expect("collisions");
        assert!(unreadable.is_empty(), "{unreadable:?}");
        assert_eq!(collisions.len(), 1, "{collisions:?}");
        let collision = &collisions[0];
        assert_eq!(
            (&collision.held.repo, collision.held.block),
            (&repo_a, fresh)
        );
        assert_eq!(
            (&collision.claimed.repo, collision.claimed.block),
            (&repo_b, live)
        );
        assert_eq!(collision.claimed.record, ClaimRecord::Published);
        let message = collision.to_string();
        assert!(
            message.contains("acme/alpha/main") && message.contains("acme/beta/main"),
            "{message}"
        );
        assert_eq!(
            inventory
                .all_reserved_port_blocks()
                .await
                .expect("reserved blocks")
                .blocks()
                .collect::<Vec<_>>(),
            [fresh],
            "every port either workspace names stays out of allocation"
        );
        assert_eq!(
            inventory
                .all_attached()
                .await
                .expect("attached listing")
                .attached
                .iter()
                .map(|fact| (&fact.repo_id, fact.port_block))
                .collect::<Vec<_>>(),
            [(&repo_a, fresh), (&repo_b, live)],
            "every reader but the gateway's session path sees every attached workspace"
        );
        let alpha = crate::gateway_sessions::stable_workspace_id(
            &repo_a,
            "main",
            &WorkspaceIncarnation::new("00000000000000000000000000000001").expect("incarnation"),
        );
        let sessions = crate::gateway_sessions::NativeSessionInventory::from_inventory(inventory);
        let ids = |sessions: Vec<cowshed_gateway_types::WorkspaceSession>| {
            sessions
                .into_iter()
                .map(|session| {
                    session
                        .endpoint
                        .validate_macos_port_block()
                        .expect("macOS fixture endpoint");
                    #[cfg(target_os = "linux")]
                    assert!(
                        matches!(
                            session.validate(),
                            Err(cowshed_gateway_types::ConfigError::ExpectedUnixEndpoint)
                        ),
                        "a macOS inventory session must never install on Linux"
                    );
                    session.workspace_id
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ids(sessions
                .all_sessions()
                .await
                .expect("startup listing")
                .serve),
            std::slice::from_ref(&alpha),
            "the gateway starts serving the older workspace"
        );
        assert_eq!(
            ids(sessions
                .project_sessions(&repo_a)
                .await
                .expect("alpha")
                .serve),
            [alpha]
        );
        assert!(
            ids(sessions
                .project_sessions(&repo_b)
                .await
                .expect("beta")
                .serve)
            .is_empty()
        );
    }

    /// The gateway's control socket as a reconcile sees it: one status, then the writes it
    /// accepts, recorded.
    struct RecordingControl {
        status: Mutex<Option<cowshed_gateway_types::DaemonStatus>>,
        installs: Mutex<Vec<String>>,
        removes: Mutex<Vec<String>>,
    }

    impl RecordingControl {
        fn serving(sessions: Vec<cowshed_gateway_types::SessionStatus>) -> Self {
            Self {
                status: Mutex::new(Some(cowshed_gateway_types::DaemonStatus {
                    version: "test".to_owned(),
                    draining: false,
                    drain_cause: None,
                    executable_sha256: None,
                    healing: None,
                    recovering: None,
                    sessions,
                    active: 0,
                    queued: 0,
                })),
                installs: Mutex::default(),
                removes: Mutex::default(),
            }
        }
    }

    #[async_trait]
    impl crate::gateway_sessions::GatewayControl for RecordingControl {
        async fn status(
            &self,
        ) -> std::result::Result<
            cowshed_gateway_types::DaemonStatus,
            crate::gateway_sessions::GatewayStatusError,
        > {
            Ok(self
                .status
                .lock()
                .expect("status")
                .take()
                .expect("a reconcile reads status once"))
        }

        async fn install(
            &self,
            session: &cowshed_gateway_types::WorkspaceSession,
        ) -> std::result::Result<(), crate::gateway_sessions::ControlRefusal> {
            self.installs
                .lock()
                .expect("installs")
                .push(session.workspace_id.clone());
            Ok(())
        }

        async fn remove(
            &self,
            workspace_id: &str,
            _expected_revision: u64,
        ) -> std::result::Result<(), crate::gateway_sessions::ControlRefusal> {
            self.removes
                .lock()
                .expect("removes")
                .push(workspace_id.to_owned());
            Ok(())
        }
    }

    /// Two projects' mains on one block, the older one served: the newer one's project still
    /// reconciles, installing its other session, instead of refusing over an endpoint a live
    /// workspace of another project holds (05_gateway.md).
    #[tokio::test]
    async fn a_project_with_a_colliding_workspace_still_reconciles_its_other_sessions() {
        use crate::gateway_sessions::SessionInventory as _;

        let fixture = Fixture::new("collision-reconcile");
        let shared = PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).expect("shared block");
        fixture.plant_port_block(shared);
        let (repo_a, repo_b, source) = two_project_store(&fixture);
        let (topic, mounted) = fixture.workspace(
            &repo_b,
            WorkspaceName::session("topic").expect("topic"),
            "00000000000000000000000000000003",
            8,
            true,
        );
        let (mount, path) = mounted.expect("mounted topic");
        {
            let mut projects = source.projects.lock().expect("source");
            let beta = projects.get_mut(&repo_b).expect("beta facts");
            beta.storage.push(topic);
            beta.mounts.push(mount.clone().into());
            beta.mount_paths.insert(mount.volume_key, path);
        }
        let sessions = crate::gateway_sessions::NativeSessionInventory::from_inventory(
            NativeGatewayInventory::with_source(
                fixture.storage.clone(),
                source as Arc<dyn InventorySource>,
            ),
        );
        let alpha = sessions
            .project_sessions(&repo_a)
            .await
            .expect("alpha sessions")
            .serve;
        assert_eq!(alpha.len(), 1, "alpha serves its main");
        let control = RecordingControl::serving(vec![cowshed_gateway_types::SessionStatus {
            workspace_id: alpha[0].workspace_id.clone(),
            revision: alpha[0].revision,
            endpoint: alpha[0].endpoint.to_string(),
            active: 0,
            queued: 0,
        }]);
        let report = crate::gateway_sessions::reconcile_project(&control, &sessions, &repo_b, 501)
            .await
            .expect("beta reconciles around its colliding main");
        let topic = crate::gateway_sessions::stable_workspace_id(
            &repo_b,
            "topic",
            &WorkspaceIncarnation::new("00000000000000000000000000000003").expect("incarnation"),
        );
        assert_eq!(*control.installs.lock().expect("installs"), [topic]);
        assert!(control.removes.lock().expect("removes").is_empty());
        assert_eq!(report.installed, 1);
    }

    /// A newer workspace's block shares a port with an older one's whatever order the store
    /// lists them in; the older keeps it, a third disjoint workspace is no party to it, and one
    /// workspace's own nested current and retained blocks never collide with each other.
    #[test]
    fn collisions_name_the_newer_workspace_as_claimant() {
        let claim =
            |repo: &str, workspace: &str, created_at: &str, base: u16, size: u16| PortClaim {
                repo: RepoId::parse(repo).expect("repo"),
                workspace: WorkspaceName::new(workspace).expect("workspace"),
                created_at: created_at.to_owned(),
                block: PortBlock::new(base, size).expect("block"),
                record: ClaimRecord::Published,
            };
        let older = claim("acme/one", "main", "2026-07-01T00:00:00Z", 40_960, 64);
        let newer = claim("acme/two", "topic", "2026-07-02T00:00:00Z", 40_960, 64);
        let own_retained = claim("acme/two", "topic", "2026-07-02T00:00:00Z", 41_024, 16);
        let own_current = claim("acme/two", "topic", "2026-07-02T00:00:00Z", 41_024, 64);
        let other = claim("acme/three", "main", "2026-07-03T00:00:00Z", 41_088, 64);
        let collisions = port_collisions([
            other,
            own_current,
            newer.clone(),
            own_retained,
            older.clone(),
        ]);
        assert_eq!(
            collisions,
            [PortCollision {
                held: older,
                claimed: newer,
            }]
        );
    }

    /// A grants sidecar outside the canonical inventory that names another repository is not
    /// trusted as a claim of this project: it is reported with its reason, and claims nothing.
    #[tokio::test]
    async fn a_retained_sidecar_naming_another_repository_is_reported_not_claimed() {
        let fixture = Fixture::new("foreign-sidecar");
        let (repo_a, _repo_b, source) = two_project_store(&fixture);
        let inventory = NativeGatewayInventory::with_source(
            fixture.storage.clone(),
            source as Arc<dyn InventorySource>,
        );
        let layout = StorageLayout::new(fixture.storage.store(), &repo_a).expect("layout");
        let main = layout.main_image().expect("main image");
        let mut foreign =
            DetachedWorkspaceMetadata::read_for_image(main.image()).expect("alpha main sidecar");
        foreign.repo_id = RepoId::parse("acme/other").expect("foreign repo");
        let retained = layout
            .project()
            .project_root
            .join(crate::storage::recovery::RETAINED_NAMESPACE);
        fs::create_dir_all(&retained).expect("retained");
        let sidecar = sidecar_path(&retained.join("main-foreign.dmg"));
        write_json(&sidecar, &foreign).expect("foreign sidecar");
        let found = inventory.port_collisions().await.expect("collisions");
        assert!(found.collisions.is_empty(), "{:?}", found.collisions);
        assert!(
            matches!(
                found.unreadable.as_slice(),
                [(path, reason)] if *path == sidecar && reason.contains("acme/other")
            ),
            "{:?}",
            found.unreadable
        );
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
                        mounts: vec![valid_mount.clone().into()],
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
            ..Default::default()
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
                mounts: vec![mount.clone().into()],
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
                mounts: vec![mount.clone().into()],
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
            .expect("direct-mount project inventory")
            .attached;
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
        let retired = trash.join("main-retired.dmg");
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

    #[cfg(target_os = "macos")]
    fn interrupted_restore(fixture: &Fixture, repo: &RepoId, checkout: &Path) -> StorageLayout {
        use crate::storage::recovery::{
            LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentJournal,
        };
        fixture.bind(repo);
        let layout = StorageLayout::new(fixture.storage.store(), repo).unwrap();
        layout.record_checkout_root(checkout).unwrap();
        LifecycleIntentJournal::update(
            &layout.project().project_root.join(LIFECYCLE_INTENTS_FILE),
            |journal| {
                journal.begin(LifecycleIntent::Retire {
                    workspace: WorkspaceName::main(),
                    options: crate::api::dto::RemoveOptions {
                        restore: true,
                        ..Default::default()
                    },
                    origin: None,
                });
                journal.mark_mutating(&WorkspaceName::main())
            },
        )
        .unwrap();
        let binding: RepositoryBinding =
            crate::metadata::read_json(&layout.project().repository_binding).unwrap();
        let error = crate::runtime::project::finish_restored_project_unbinding(
            layout.project(),
            &binding,
            None,
            &mut |step| {
                if step == crate::runtime::project::UnbindCheckpoint::BindingRemoved {
                    return Err(crate::CowshedError::internal(
                        "interrupted after binding unlink",
                    ));
                }
                Ok(())
            },
        )
        .unwrap_err();
        assert!(error.message.contains("interrupted after binding unlink"));
        assert!(!layout.project().repository_binding.exists());
        layout
    }

    /// A new inventory instance follows the exact lookup used by the original restore command.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn a_fresh_controller_finds_an_interrupted_unbinding_without_rebinding_it() {
        use crate::storage::recovery::LIFECYCLE_INTENTS_FILE;
        let fixture = Fixture::new("unbinding-discovery");
        let repo = RepoId::parse("acme/widget").unwrap();
        let checkout = fixture.root.join("plain-checkout");
        fs::create_dir_all(&checkout).unwrap();
        let layout = interrupted_restore(&fixture, &repo, &checkout);
        let journal = layout.project().project_root.join(LIFECYCLE_INTENTS_FILE);
        let before = fs::read(&journal).unwrap();
        let inventory = NativeGatewayInventory::new(fixture.storage.clone());
        assert_eq!(
            inventory
                .repository_for_project_root(&checkout)
                .await
                .unwrap(),
            Some(repo.clone())
        );
        assert!(
            discover_repositories(fixture.storage.store())
                .unwrap()
                .is_empty(),
            "cleanup admission became active inventory"
        );
        assert!(
            !layout.project().repository_binding.exists(),
            "lookup recreated registration"
        );
        assert_eq!(
            fs::read(&journal).unwrap(),
            before,
            "lookup rewrote controller authority"
        );
        let other = fixture.root.join("other-checkout");
        fs::create_dir_all(&other).unwrap();
        assert_eq!(
            inventory.repository_for_project_root(&other).await.unwrap(),
            None
        );
        let binding = RepositoryBinding::new(vec![BoundIdentity {
            repo_id: repo,
            remote_name: None,
            remote_url: None,
            primary: true,
        }])
        .unwrap();
        crate::runtime::project::finish_restored_project_unbinding(
            layout.project(),
            &binding,
            None,
            &mut |_| Ok(()),
        )
        .unwrap();
        assert!(!layout.project().project_root.exists());
        assert_eq!(
            NativeGatewayInventory::new(fixture.storage.clone())
                .repository_for_project_root(&checkout)
                .await
                .unwrap(),
            None
        );
        assert!(
            !layout.project().project_root.exists(),
            "completed lookup recreated directories"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn unbinding_discovery_requires_mutating_restore_authority_and_refuses_ambiguity() {
        use crate::storage::recovery::{
            LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentJournal,
        };
        let fixture = Fixture::new("unbinding-authority");
        let checkout = fixture.root.join("plain-checkout");
        fs::create_dir_all(&checkout).unwrap();
        let first = RepoId::parse("acme/first").unwrap();
        let layout = interrupted_restore(&fixture, &first, &checkout);
        let path = layout.project().project_root.join(LIFECYCLE_INTENTS_FILE);
        let inventory = NativeGatewayInventory::new(fixture.storage.clone());
        for (workspace, restore, mutating) in [
            (WorkspaceName::main(), true, false),
            (WorkspaceName::main(), false, true),
            (WorkspaceName::new("topic").unwrap(), true, true),
        ] {
            LifecycleIntentJournal::update(&path, |journal| {
                journal.clear(&WorkspaceName::main());
                journal.begin(LifecycleIntent::Retire {
                    workspace: workspace.clone(),
                    options: crate::api::dto::RemoveOptions {
                        restore,
                        ..Default::default()
                    },
                    origin: None,
                });
                if mutating {
                    journal.mark_mutating(&workspace)?;
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(
                inventory
                    .repository_for_project_root(&checkout)
                    .await
                    .unwrap(),
                None
            );
        }
        LifecycleIntentJournal::update(&path, |journal| {
            journal.begin(LifecycleIntent::Retire {
                workspace: WorkspaceName::main(),
                options: crate::api::dto::RemoveOptions {
                    restore: true,
                    ..Default::default()
                },
                origin: None,
            });
            journal.mark_mutating(&WorkspaceName::main())
        })
        .unwrap();
        let second = RepoId::parse("acme/second").unwrap();
        let second_layout = interrupted_restore(&fixture, &second, &checkout);
        assert!(matches!(
            inventory.repository_for_project_root(&checkout).await,
            Err(GatewayInventoryError::AmbiguousProjectRoot(_))
        ));
        fixture.bind(&second);
        second_layout.record_checkout_root(&checkout).unwrap();
        assert_eq!(
            inventory
                .repository_for_project_root(&checkout)
                .await
                .unwrap(),
            Some(second),
            "pending cleanup must not override a live binding"
        );
        assert!(!layout.project().repository_binding.exists());
    }

    #[tokio::test]
    async fn an_unmounted_checkout_names_only_the_owner_its_store_records() {
        let fixture = Fixture::new("unmounted-primary");
        let repo = RepoId::parse("acme/widget").expect("repo");
        fixture.bind(&repo);
        let checkout = fixture.root.join("checkout-widget");
        fs::create_dir_all(&checkout).expect("unmounted checkout");
        StorageLayout::new(fixture.storage.store(), &repo)
            .expect("layout")
            .record_checkout_root(&checkout)
            .expect("checkout root record");
        let stranger = fixture.root.join("stranger");
        fs::create_dir_all(&stranger).expect("unrecorded checkout");
        let inventory = NativeGatewayInventory::new(fixture.storage.clone());

        assert_eq!(
            inventory
                .re_adoption_primary_for_checkout(&checkout)
                .await
                .expect("unmounted primary"),
            Some(repo)
        );
        assert_eq!(
            inventory
                .re_adoption_primary_for_checkout(&stranger)
                .await
                .expect("unrecorded primary"),
            None
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
            ..Default::default()
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
                .attached
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
                    mounts: vec![mount.clone().into(), duplicate.into()],
                    checkpoints: Vec::new(),
                    mount_paths: BTreeMap::from([(mount.volume_key, path)]),
                },
            )])),
            ..Default::default()
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
    /// counts as mounted once its main is, so the count drops as each main finishes, then turns to
    /// restoring sessions. A session is never mounted by the pass.
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

        for (held, left) in [("acme/alpha/main", 2), ("acme/beta/main", 1)] {
            assert_eq!(mounts.recv().await.as_deref(), Some(held));
            assert_eq!(progress.current(), mounting(left), "while {held} is held");
            permits.add_permits(1);
        }
        let outcomes = healing.await.expect("the heal ends");
        assert_eq!(outcomes.len(), 2);
        assert!(mounts.try_recv().is_err(), "no session was mounted");
        assert_eq!(progress.current(), Some(StartupHeal::RestoringSessions));
        progress.restored();
        assert_eq!(progress.current(), None);
    }

    /// The startup heal mounts every project's main and no session: a session attaches on use.
    ///
    /// The fixture lists each project's session first precisely so inventory order cannot pass this
    /// by accident.
    #[tokio::test]
    async fn the_startup_heal_mounts_every_main_and_no_session() {
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
            ["acme/alpha/main", "acme/beta/main"]
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
        }
    }
    /// One unhealable project never costs another its main.
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
            ["acme/beta/main", "acme/gamma/main"]
        );
        let alpha_outcome = &outcomes[0];
        assert_eq!(alpha_outcome.repo_id, alpha);
        assert!(matches!(
            alpha_outcome.main,
            Err(GatewayInventoryError::InvalidMetadata { .. })
        ));
        let beta_outcome = &outcomes[1];
        assert_eq!(beta_outcome.repo_id, beta);
        assert!(beta_outcome.main.is_err());
        let gamma_outcome = &outcomes[2];
        assert_eq!(gamma_outcome.repo_id, gamma);
        assert!(gamma_outcome.main.is_ok());
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
                    vec![MountJoin::Proven(mount.clone())],
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
