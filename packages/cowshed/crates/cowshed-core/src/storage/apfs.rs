pub mod deleter;
pub mod extents;
pub mod native;
pub mod rekey;
pub mod release;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::api::held_lock::{LockHolding, LockOperation};
use crate::metadata::{WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use crate::repository::{OwnedRepoIds, RepoId};
use cowshed_apfs::image::{IMAGE_EXTENSION, ImageCapacity};
use cowshed_apfs::{ApfsError, DetachIntent, MountAccess, timed_apfs_step};
use cowshed_os::timing::timed_async;

use super::lifecycle::{
    AdoptHandoff, AdoptPlan, AdoptRequest, CreatePlan, DefragmentOutcome, DerivedWorkspace,
    Destination, ExecuteError, ForkPlan, ImmutablePlan, LifecycleBackend, LifecycleFact,
    LifecyclePlanner, LifecycleReceipt, LifecycleWorkspace, MountIntent, MountJoin, MountRefusal,
    MountRefusalReason, MountState, Operation, OperationIdentity, PlanError, PurePlanner,
    ResizeOutcome, RetirePlan, RetiredRef, Revision, StorageFact, StorageGcOptions, StorageGcPlan,
    StorageGcReport, Substrate, SubstrateStats, execute_checked, revalidate,
};
use super::{StorageLayout, StorageLayoutError};

pub const DEFAULT_IMAGE_CAPACITY: ImageCapacity = ImageCapacity::from_gibibytes(100);
use super::recovery::{STAGING_NAMESPACE, TRASH_NAMESPACE};
use cowshed_os::namespace::HostNamespace;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApfsSubstrateConfig {
    /// Whose names the store's volumes carry (01_storage.md, "Host namespace").
    pub namespace: HostNamespace,
    pub store_root: PathBuf,
    /// The adopted checkout's path — the place in the user's source tree that adoption took over,
    /// and main's mountpoint.
    pub checkout_path: PathBuf,
    pub capacity: ImageCapacity,
    /// The gateway control socket a release nudges (05_gateway.md, "Deleting released images");
    /// with none, the entry waits in `.deleting` for a gateway's start.
    pub cleanup: Option<std::sync::Arc<Path>>,
}

impl ApfsSubstrateConfig {
    pub fn new(
        namespace: HostNamespace,
        store_root: impl Into<PathBuf>,
        checkout_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            namespace,
            store_root: store_root.into(),
            checkout_path: checkout_path.into(),
            capacity: DEFAULT_IMAGE_CAPACITY,
            cleanup: None,
        }
    }

    /// The config of a project in the validated store `storage`, whose releases go to the gateway
    /// that store names.
    pub fn for_storage(
        storage: &crate::storage::bootstrap::ValidatedHostStorage,
        checkout_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            cleanup: storage.roots().cleanup().cloned(),
            ..Self::new(storage.namespace(), storage.store(), checkout_path)
        }
    }

    /// The same project, checked out somewhere else.
    ///
    /// The checkout path is the only field a live project can change — that is what `cowshed mv
    /// main` does, and what `cowshed attach` converges onto after a checkout is respelt. Everything
    /// else (store root, capacity) is fixed for the project's lifetime.
    ///
    /// It is a whole-config clone rather than a mutable field because the config is shared by
    /// value: `ApfsSubstrate` holds it behind an `Arc` that every clone of the substrate shares,
    /// and `MacOsApfsExecutionHost` holds its own copy. Mutating it in place would let an
    /// outstanding clone observe a half-applied move — a mount point derived from the new checkout
    /// path against an execution host still validating against the old one. Rebinding instead
    /// builds a new config, a new host, and a new substrate, and the caller swaps all three at
    /// once, at the one point in the move transaction where nothing is mounted.
    pub fn rebind_checkout(&self, checkout_path: impl Into<PathBuf>) -> Self {
        Self {
            checkout_path: checkout_path.into(),
            ..self.clone()
        }
    }

    /// Every identity the project answering to `current` owns, read from its own binding.
    ///
    /// Read at the point of use rather than carried in the config, because the config is built in
    /// two places — the project runtime, which holds the binding, and the gateway inventory host,
    /// which does not — and a set that only one of them could populate is a set the other would
    /// silently narrow to nothing. The binding beside the project directory is the authority both
    /// can reach.
    ///
    /// A binding that is absent, unreadable, or does not answer to `current` yields the tightest
    /// possible set. Widening acceptance on no evidence is never the safe direction.
    pub fn owned_repo_ids(&self, current: &RepoId) -> OwnedRepoIds {
        let Ok(layout) = StorageLayout::new(&self.store_root, current) else {
            return OwnedRepoIds::sole(current.clone());
        };
        let Ok(binding) = crate::metadata::read_json::<crate::repository::RepositoryBinding>(
            &layout.project().repository_binding,
        ) else {
            return OwnedRepoIds::sole(current.clone());
        };
        match binding.owned_repo_ids() {
            Ok(owned) if owned.current() == current => owned,
            _ => OwnedRepoIds::sole(current.clone()),
        }
    }

    pub fn with_capacity(mut self, capacity: ImageCapacity) -> Self {
        self.capacity = capacity;
        self
    }
}
pub trait IncarnationSource: Send + Sync + 'static {
    fn mint(&self) -> Result<WorkspaceIncarnation, ApfsStorageError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UuidIncarnationSource;

impl IncarnationSource for UuidIncarnationSource {
    fn mint(&self) -> Result<WorkspaceIncarnation, ApfsStorageError> {
        WorkspaceIncarnation::new(uuid::Uuid::new_v4().simple().to_string())
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataPolicy {
    Fresh,
    FreshPendingFence,
    Preserve,
    PendingFence,
}

/// What a mounted volume's in-image marker has to say for the volume to be this workspace's.
///
/// The repository axis is a set rather than one identity: an in-place identity change cannot reach
/// the marker sealed inside a detached image, so a renamed project legitimately mounts an image
/// still stamped with an identity it used to hold. Every other field is exact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkerExpectation {
    pub repos: OwnedRepoIds,
    pub workspace: WorkspaceName,
    pub incarnation: WorkspaceIncarnation,
}

impl MarkerExpectation {
    /// For a marker written at some earlier time, which is every already-published image. The
    /// identity it carries may be one the project has since changed away from.
    fn owned(config: &ApfsSubstrateConfig, workspace: &LifecycleWorkspace) -> Self {
        Self {
            repos: config.owned_repo_ids(workspace.repo()),
            workspace: workspace.name().clone(),
            incarnation: workspace.incarnation().clone(),
        }
    }

    /// Whether the marker in the tree at `root` names this workspace, and what was read instead
    /// when it does not. A marker that cannot be read is not ours (01a_apfs.md, "Ownership,
    /// identity, and the volume label"); the read's own error says why it could not be read.
    pub(crate) fn judge(&self, root: &Path) -> Result<(), MountRefusalReason> {
        let marker =
            crate::metadata::WorkspaceMarker::read_from(&root.join(super::WORKSPACE_MARKER_PATH))
                .map_err(|error| MountRefusalReason::MarkerUnreadable(error.to_string()))?;
        if self.repos.accepts(&marker.repo_id)
            && marker.workspace == self.workspace
            && marker.workspace_incarnation == self.incarnation
        {
            Ok(())
        } else {
            Err(MountRefusalReason::MarkerNamesAnother {
                repo: marker.repo_id,
                workspace: marker.workspace,
                incarnation: marker.workspace_incarnation,
            })
        }
    }

    /// For a marker this same operation just stamped, where the current identity is the only one
    /// the marker can possibly carry, so the expectation stays exact.
    fn freshly_stamped(workspace: &LifecycleWorkspace) -> Self {
        Self {
            repos: OwnedRepoIds::sole(workspace.repo().clone()),
            workspace: workspace.name().clone(),
            incarnation: workspace.incarnation().clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedImage {
    pub workspace: LifecycleWorkspace,
    pub image: PathBuf,
    pub mount_point: PathBuf,
}

/// Mounted, controller-private workspace stage. It is not published into workspace enumeration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceStage {
    pub workspace: LifecycleWorkspace,
    pub mount_point: PathBuf,
    pub companion: PathBuf,
    /// True when this callback is continuing a durable pending clone rather than initializing
    /// bytes created by this invocation. Callers use this proof to admit only their own prior
    /// branch/worktree state; a fresh operation must still reject pre-existing state.
    pub resuming: bool,
}

pub type AdoptStage = WorkspaceStage;
pub type CreateStage = WorkspaceStage;
pub type ForkStage = WorkspaceStage;

/// Work a staged adopt runs beside staging main's image, under the plan's locks: it starts once
/// the locks are held and the plan's facts revalidated, and runs while the image is attached and
/// mounted; its output is answered with the receipt once main is committed. When the image cannot
/// be staged, initialized or committed, `abandon` gets that output instead, so nothing the work
/// made outlives the failure. An operation dropped before either drops the work and its output
/// with it, and nothing runs for them after: an output that owns a resource releases it when
/// dropped (main's first build volume does), and work dropped while it runs releases what it
/// makes once nobody is there to take it. A create or fork runs [`Beside`] instead.
///
/// It exists for a second image the operation attaches anyway (main's first build volume, a
/// fork's): every `diskutil image attach` waits in the host's one `storagekitd` queue
/// (01a_apfs.md, "How the APFS host degrades"), and two attaches in flight together wait in it
/// once, not twice.
pub struct Alongside<W, D> {
    pub work: W,
    pub abandon: D,
}

impl Alongside<std::future::Ready<()>, fn(()) -> std::future::Ready<()>> {
    /// No work beside the clone: the initializer gets `()`.
    pub fn none() -> Self {
        Self {
            work: std::future::ready(()),
            abandon: std::future::ready,
        }
    }
}

/// Work a staged create or fork runs beside staging its image, in two halves that straddle its
/// source's image lock (16_build_volumes.md, "Fork"). `capture` runs under that lock, beside the
/// capture of the source's image, and reads what the lock guards (a fork's seed); `ready` runs
/// once the lock is let go, beside the clone's attach and mount, with what `capture` answered
/// and the new workspace's incarnation, and its output goes to the initializer with the stage. When the source's
/// image cannot be captured `abandon_captured` gets `capture`'s output; when the clone cannot be
/// staged after that, `abandon_ready` gets `ready`'s. Dropping follows [`Alongside`]: an output
/// that owns a resource releases it, or leaves it registered for collection, when dropped.
pub struct Beside<C, R, AC, AR> {
    pub capture: C,
    pub ready: R,
    pub abandon_captured: AC,
    pub abandon_ready: AR,
}

type Unit = fn(()) -> std::future::Ready<()>;
type ReadyNone = fn((), WorkspaceIncarnation) -> std::future::Ready<()>;

impl Beside<std::future::Ready<()>, ReadyNone, Unit, Unit> {
    /// No work beside the clone: the initializer gets `()`.
    pub fn none() -> Self {
        Self {
            capture: std::future::ready(()),
            ready: |(), _| std::future::ready(()),
            abandon_captured: std::future::ready,
            abandon_ready: std::future::ready,
        }
    }
}

/// An adoption a crash interrupted: the canonical main image its `PendingFence` sidecar still
/// fences, the incarnation that sidecar was minted for, and what it records of the adoption.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingAdoption {
    pub incarnation: WorkspaceIncarnation,
    pub image: PathBuf,
    pub info: crate::metadata::WorkspaceInfoSnapshot,
    pub grants: crate::metadata::GrantSet,
}

impl PendingAdoption {
    /// The identity the interrupted attempt recorded, resumed under `trace`.
    pub fn identity(&self, trace: &str) -> OperationIdentity {
        OperationIdentity {
            project_root: self.info.project_root.clone(),
            base_commit: self.info.base_commit.clone(),
            created_at: self.info.created_at.clone(),
            branch: self.info.branch.clone(),
            forked_from: self.info.forked_from.clone(),
            created_trace: trace.to_owned(),
            grants: self.grants.clone(),
            git_worktree: self.info.git_worktree,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumableClone {
    pub workspace: LifecycleWorkspace,
    pub identity: OperationIdentity,
    pub image: PathBuf,
}

/// How [`ApfsExecutionHost::attach_and_mount_resumable`] found a pending image, so a caller that
/// ends without changing it can leave it exactly that way.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FoundMount {
    /// Its volume was already mounted at the mount point, and the call reused that mount.
    Mounted,
    /// It was attached without a mount, and the call mounted that attachment.
    Attached,
    /// It was detached, and the call attached and mounted it.
    Detached,
}

/// A pending image mounted at its canonical mount point, and how it was found.
pub struct ResumedMount<A> {
    pub attachment: A,
    pub found: FoundMount,
}

#[derive(Debug, Error)]
pub enum StagedExecutionError<E> {
    #[error("staged lifecycle execution failed: {0}")]
    Storage(#[source] ApfsStorageError),
    #[error("staged lifecycle initializer failed: {0}")]
    Initializer(E),
    /// The initializer of a fresh clone whose effects stay inside it failed, and the clone was
    /// reclaimed: nothing of it remains for a retry to resume (02_workspaces.md, "Publication
    /// and crash recovery").
    #[error("staged lifecycle initializer failed and its fresh clone was reclaimed: {0}")]
    InitializerReclaimed(E),
    #[error(
        "staged lifecycle initializer failed and cleanup also failed: initializer={initializer}; cleanup={cleanup}"
    )]
    InitializerCleanup {
        initializer: E,
        #[source]
        cleanup: ApfsStorageError,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetirementDisposition {
    /// Every source byte is rebuildable or recoverable outside the retiring image.
    Release,
    /// Keep the complete source payload as recovery evidence.
    Recovery,
}

#[derive(Debug, Error)]
pub enum RetireExecutionError<F> {
    #[error("workspace retirement failed: {0}")]
    Storage(#[source] ApfsStorageError),
    #[error("workspace retired but durable lifecycle publication failed: {source}")]
    Fence { source: F, retired: RetiredRef },
}

impl<F> From<ApfsStorageError> for RetireExecutionError<F> {
    fn from(error: ApfsStorageError) -> Self {
        Self::Storage(error)
    }
}

pub type AdoptExecutionError<E> = StagedExecutionError<E>;
pub type CreateExecutionError<E> = StagedExecutionError<E>;
pub type ForkExecutionError<E> = StagedExecutionError<E>;

impl<E> From<ApfsStorageError> for StagedExecutionError<E> {
    fn from(error: ApfsStorageError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetiredImage {
    pub retired: RetiredRef,
    pub image: PathBuf,
}
use cowshed_apfs::store_lock::LockMode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationDisposition {
    RolledBack,
    ForwardOnly,
}

#[derive(Debug, Error)]
#[error("{source}")]
pub struct PublicationError {
    disposition: PublicationDisposition,
    #[source]
    source: Box<ApfsStorageError>,
}

impl PublicationError {
    pub fn rolled_back(source: ApfsStorageError) -> Self {
        Self {
            disposition: PublicationDisposition::RolledBack,
            source: Box::new(source),
        }
    }

    pub fn forward_only(source: ApfsStorageError) -> Self {
        Self {
            disposition: PublicationDisposition::ForwardOnly,
            source: Box::new(source),
        }
    }

    pub fn disposition(&self) -> PublicationDisposition {
        self.disposition
    }

    pub fn into_source(self) -> ApfsStorageError {
        *self.source
    }
}

impl From<PublicationError> for ApfsStorageError {
    fn from(error: PublicationError) -> Self {
        error.into_source()
    }
}

/// How naming an image's volume went (01a_apfs.md, "Ownership, identity, and the volume
/// label"). The target is always the volume device the kernel's attachment inventory maps an
/// image under the store to, never a path; every case but `Renamed` renamed nothing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Labelled {
    /// The kernel holds no attachment of the image: a create that was refused, or a workspace
    /// detached. Whatever directory it would mount at belongs to another volume.
    NotAttached,
    /// The image is attached, but its volume is mounted nowhere, and APFS renames only mounted
    /// volumes.
    NotMounted,
    /// The file system already reports the label.
    Already,
    /// Renamed to the label through the image's pinned volume device.
    Renamed,
}

/// Synchronous macOS/filesystem boundary. Implementations must use the primitives in
/// `cowshed_apfs`; the storage executor calls this trait only through [`ApfsBlockingLane`].
pub trait ApfsExecutionHost: Send + Sync + 'static {
    /// Whether this host's workspaces are kernel volumes with physical identity: a label, a
    /// device, a capacity and block statistics. A directory store has none of them, so the
    /// executor and the runtime skip only the cosmetic steps (volume labels, volume sampling)
    /// that exist for physical volumes; every physical claim such a host is asked for anyway is
    /// refused with [`ApfsStorageError::Unsupported`].
    const HAS_PHYSICAL_VOLUMES: bool = true;
    type LockGuard: Send + 'static;
    /// Take the lifecycle locks at `images`, each recorded as held by this process for
    /// `holding`, the request the caller names (02_workspaces.md "Held locks").
    fn lock_images(
        &self,
        images: &[PathBuf],
        mode: LockMode,
        holding: &LockHolding,
    ) -> Result<Option<Self::LockGuard>, ApfsStorageError>;
    type Attachment: Send + 'static;

    fn observe(&self, expected: &[LifecycleFact]) -> Result<Vec<LifecycleFact>, ApfsStorageError>;
    /// Mint `image`, which must not exist, and hand it back checked and mounted at `mount_point`:
    /// one `clonefile` of the store's formatted blank template at `capacity`, then one exact-image
    /// `--noMount` attach, pinned `fsck_apfs` check and `mount_apfs` mount (01a_apfs.md, "Mounts").
    /// The canonical name
    /// only ever holds a complete, formatted volume; it carries the template's label until the
    /// workspace's supervisor relabels it. A failure leaves no image and no attachment.
    fn create_attached(
        &self,
        capacity: ImageCapacity,
        image: &Path,
        mount_point: &Path,
    ) -> Result<Self::Attachment, ApfsStorageError>;
    /// Clone `source` to `destination` with the source's latest writes in it. `source_mount` is
    /// where the source is mounted when it is a live workspace — its volume is what gets flushed
    /// — and `None` for an image nothing mounts (a staged image).
    fn clone_image(
        &self,
        source: &Path,
        source_mount: Option<&Path>,
        destination: &Path,
    ) -> Result<(), ApfsStorageError>;
    /// Prove this exact physical image generation within the standalone first-write bound.
    /// Unknown/stale images refuse; the native extent walk stops at the first over-limit run.
    fn require_forkable_image(
        &self,
        image: &Path,
        mount: Option<&Path>,
    ) -> Result<(), ApfsStorageError>;
    fn resumable_clone(
        &self,
        config: &ApfsSubstrateConfig,
        source: &LifecycleWorkspace,
        destination: &WorkspaceName,
        destination_topology: Revision,
        identity: &OperationIdentity,
    ) -> Result<Option<ResumableClone>, ApfsStorageError> {
        let _ = (config, source, destination, destination_topology, identity);
        Ok(None)
    }
    /// Revalidate the exact pending sidecar and payload under its image lock.
    fn validate_pending_clone(
        &self,
        image: &Path,
        expected: &crate::metadata::DetachedWorkspaceMetadata,
    ) -> Result<(), ApfsStorageError>;

    /// The unfinished adoption whose canonical main still carries `PendingFence`, if any. A main
    /// that is already published is not an adoption to resume and is refused.
    fn pending_adoption(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
    ) -> Result<Option<PendingAdoption>, ApfsStorageError>;
    /// Attach and mount an unfinished adoption's canonical image at its staging mountpoint the
    /// way [`Self::attach_and_mount_resumable`] does, or answer `None` when its legacy payload
    /// never became an APFS volume. Pinned filesystem-verification failures are returned;
    /// whatever a refused fresh attachment produced is released first.
    fn resume_pending_adopt(
        &self,
        image: &Path,
        mount_point: &Path,
        workspace: &LifecycleWorkspace,
    ) -> Result<Option<Self::Attachment>, ApfsStorageError> {
        self.attach_and_mount_resumable(image, mount_point, workspace)
            .map(|resumed| Some(resumed.attachment))
    }
    /// Release and reclaim an unpublished canonical image no attachment handle is held for:
    /// whatever the kernel still attaches from it, then the image, its companion and sidecar.
    fn discard_pending(&self, image: &Path) -> Result<(), ApfsStorageError>;
    /// Unmount the exact verified mounted volume with `diskutil unmount`, leaving the image
    /// attached for its next mount. An already-unmounted volume needs no command effect.
    fn unmount_attachment(&self, attachment: &Self::Attachment) -> Result<(), ApfsStorageError>;
    fn copy_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError>;
    fn attach_verified(&self, image: &Path) -> Result<Self::Attachment, ApfsStorageError>;
    /// Attach a formatted image once without mounting, check its pinned volume, then use `mount_apfs`.
    fn attach_mounted(
        &self,
        image: &Path,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<Self::Attachment, ApfsStorageError>;
    fn mount(
        &self,
        attachment: &Self::Attachment,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsStorageError>;
    /// Attach and mount a pending canonical image, reusing an exact surviving kernel mount or
    /// checking its pinned unmounted crash-left volume with `fsck_apfs`, then using `mount_apfs`.
    /// The answer says which of those it found, so a caller can put the image back as it was.
    /// Implementations own cleanup when attachment or mounting fails.
    fn attach_and_mount_resumable(
        &self,
        image: &Path,
        mount_point: &Path,
        _workspace: &LifecycleWorkspace,
    ) -> Result<ResumedMount<Self::Attachment>, ApfsStorageError> {
        Ok(ResumedMount {
            attachment: self.attach_mounted(image, mount_point, MountAccess::ReadWrite, false)?,
            found: FoundMount::Detached,
        })
    }
    /// Rename the volume of `attachment`, a verified attachment of an image under the store
    /// mounted at `mount_point`, through that mount root held open (`cowshed_apfs::OwnedVolumeRoot`):
    /// never a path or Disk Arbitration, which name whatever volume holds a path. `mount_point`
    /// is proven that device's mount root under the image's lease before the name is set.
    fn rename_volume(
        &self,
        attachment: &Self::Attachment,
        mount_point: &Path,
        volume_name: &str,
    ) -> Result<(), ApfsStorageError>;
    fn mint_workspace_credentials(
        &self,
        workspace: &LifecycleWorkspace,
        image_path: &Path,
        mount_point: &Path,
        private_key_path: &Path,
    ) -> Result<(), ApfsStorageError>;
    fn write_marker(
        &self,
        mount_point: &Path,
        workspace: &LifecycleWorkspace,
        forked_from: Option<&WorkspaceName>,
        identity: &OperationIdentity,
    ) -> Result<(), ApfsStorageError>;
    fn validate_marker(
        &self,
        mount_point: &Path,
        expected: &MarkerExpectation,
    ) -> Result<(), ApfsStorageError>;
    fn validate_staged_companion(&self, path: &Path) -> Result<(), ApfsStorageError>;
    fn detach(
        &self,
        attachment: Self::Attachment,
        intent: DetachIntent,
    ) -> Result<(), ApfsStorageError>;
    fn heal_mount(
        &self,
        workspace: &LifecycleWorkspace,
        mount_point: &Path,
    ) -> Result<(), ApfsStorageError>;
    /// Whether `workspace` already stands as [`Substrate::ensure_mounted`] leaves it, read without
    /// changing anything: mounted at `mount_point` from its own image with its owners honoured,
    /// carrying its marker, published as mounted, and its checkout's build link, when it has one,
    /// kept on a mounted volume of its own (16_build_volumes.md, "One link per checkout").
    /// `false` is anything the mutating path would heal, mount or relink under the lock.
    fn mount_settled(
        &self,
        workspace: &LifecycleWorkspace,
        mount_point: &Path,
    ) -> Result<bool, ApfsStorageError>;
    fn retain_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        attachment: Self::Attachment,
    ) -> Result<u64, ApfsStorageError>;
    fn detach_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        intent: DetachIntent,
    ) -> Result<(), ApfsStorageError>;
    /// Mount the build volume the checkout mounted at `checkout` links, when it links one
    /// (16_build_volumes.md, "One link per checkout"): a mounted checkout's build-state paths
    /// always resolve into a mounted volume, and a stale link is re-pointed at `workspace`'s own
    /// volume first (`BuildVolumeLayout::resolve_link`).
    fn ensure_linked_build_volume(
        &self,
        workspace: &LifecycleWorkspace,
        checkout: &Path,
    ) -> Result<(), ApfsStorageError>;
    /// Detach every build volume `workspace`'s checkout owns, kept whole for its next mount,
    /// once the checkout itself is detached: a detached workspace holds none of its images
    /// (02_workspaces.md, "Attach and detach"). Its next mount mounts the volume again
    /// ([`Self::ensure_linked_build_volume`]).
    fn detach_owned_build_volumes(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<(), ApfsStorageError>;
    /// Grow the workspace's image to `capacity` and restore the mount state it was found in.
    ///
    /// Refuses before touching the image when `capacity` does not exceed what the image already
    /// holds: resize only ever grows. A mounted workspace is detached non-forcibly first, so a
    /// volume with work in flight refuses the resize instead of being torn out from under it.
    fn resize(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
        mount_point: &Path,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError>;
    /// Rewrite the workspace's image contiguously and restore the mount state it was found in.
    ///
    /// Exactly as `resize` does, a mounted workspace is detached non-forcibly first, so a volume
    /// with work in flight refuses the rewrite before the image is touched. The data is copied
    /// with plain reads and writes into a sibling no enumeration reads as an image, flushed,
    /// renamed over the image, and verified by attaching it before it is mounted again. A copy
    /// that fails leaves the image untouched and puts the workspace back where it was.
    fn defragment(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
        mount_point: &Path,
    ) -> Result<DefragmentOutcome, ApfsStorageError>;
    /// Detach adopted main and atomically restore its exact retained host checkout.
    ///
    /// Implementations derive retry state solely from `source_checkout` — main's mountpoint — and
    /// its exact `pre_cowshed_checkout` sibling. They must never recursively copy or merge either
    /// tree.
    fn restore_adopted_checkout(
        &self,
        workspace: &LifecycleWorkspace,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), ApfsStorageError>;
    /// Hand the checkout path over to main's mountpoint.
    ///
    /// Builds an empty mountpoint directory under a staging sibling, exchanges it with the
    /// original checkout in one `renameatx_np(RENAME_SWAP)`, and renames the displaced original
    /// to `pre_cowshed_checkout`. The checkout path is never absent. Main mounts there afterwards;
    /// a failed publication is completed explicitly by the controller, not by a repository hook.
    fn vacate_adopted_checkout(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), PublicationError>;
    /// Hand the checkout path over to a plain re-adoption's replacement: the same atomic swap as
    /// `vacate_adopted_checkout`, retaining the displaced tree at `preserved`, a distinct sibling
    /// of `checkout` the caller has validated. Converges from an interrupted swap and never moves
    /// an already-mounted replacement.
    fn vacate_plain_checkout(
        &self,
        checkout: &Path,
        preserved: &Path,
    ) -> Result<(), PublicationError>;
    /// Refuse unless `checkout` is an empty directory nothing is mounted at: the mountpoint a
    /// replaced main left, which a re-adoption's replacement mounts on (02_workspaces.md,
    /// "Re-adoption" step 5). A volume still mounted there, or anything written into the
    /// directory, means the replaced main or something else still owns the path.
    fn require_vacant_checkout(&self, checkout: &Path) -> Result<(), ApfsStorageError>;
    fn publish_metadata(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        policy: MetadataPolicy,
        identity: Option<&OperationIdentity>,
        source_image: Option<&Path>,
    ) -> Result<(), ApfsStorageError>;
    /// Publish a pending image: one atomic sidecar rewrite from `PendingFence` to `Active`.
    fn activate_pending(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
    ) -> Result<(), ApfsStorageError> {
        let _ = (image, workspace);
        Ok(())
    }
    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError>;
    /// Release an exactly fenced canonical workspace, retaining its record until physical unlink.
    fn release_workspace(
        &self,
        config: &ApfsSubstrateConfig,
        retired: &RetiredRef,
    ) -> Result<(), ApfsStorageError>;
    fn reclaim_image(&self, image: &Path) -> Result<(), ApfsStorageError>;
    fn reclaim_retired(
        &self,
        config: &ApfsSubstrateConfig,
        retired: &RetiredRef,
    ) -> Result<(), ApfsStorageError>;
    fn list(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError>;
    /// Revalidate one published workspace's identity and mount without reading sibling records.
    fn workspace_mount_state(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<MountState, ApfsStorageError>;
    /// Every published workspace's mount, joined or refused workspace by workspace. A refusal is
    /// that workspace's fact; only reading the mount table or the records at all fails the call.
    fn mounts(&self, repo: &RepoId) -> Result<Vec<MountJoin>, ApfsStorageError>;
    fn recover_pending(
        &self,
        config: &ApfsSubstrateConfig,
        held_locks: &[PathBuf],
    ) -> Result<(), ApfsStorageError>;
    fn stats(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
    ) -> Result<SubstrateStats, ApfsStorageError>;
    fn preview_gc(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
        options: StorageGcOptions,
    ) -> Result<StorageGcPlan, ApfsStorageError>;
    fn execute_gc(
        &self,
        config: &ApfsSubstrateConfig,
        plan: StorageGcPlan,
    ) -> Result<StorageGcReport, ApfsStorageError>;
}

#[async_trait]
pub trait ApfsBlockingLane: Send + Sync + 'static {
    async fn dispatch<T, F>(&self, job: F) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ApfsStorageError> + Send + 'static;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TokioApfsBlockingLane;

#[async_trait]
impl ApfsBlockingLane for TokioApfsBlockingLane {
    async fn dispatch<T, F>(&self, job: F) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ApfsStorageError> + Send + 'static,
    {
        tokio::task::spawn_blocking(cowshed_os::timing::carried(job))
            .await
            .map_err(|error| ApfsStorageError::BlockingTask(error.to_string()))?
    }
}

#[derive(Debug, Error)]
pub enum ApfsStorageError {
    #[error("APFS operation failed: {0}")]
    Apfs(#[from] ApfsError),
    #[error("storage layout failed: {0}")]
    Layout(#[from] StorageLayoutError),
    #[error(transparent)]
    OwnerScan(#[from] super::bootstrap::OwnerScanError),
    #[error("{operation} {path} failed: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    /// The build volume is in use, so it cannot be claimed. `holds` names each process holding
    /// `hold` (a job admitted on the volume, through its supervisor), `open` each that has the
    /// volume open; either is empty when none of this user's processes is visible there. The
    /// message names whichever holds it ([`build_volume_held`]).
    #[error("{}", build_volume_held(image, hold, holds, open))]
    BuildVolumeHeld {
        image: PathBuf,
        hold: PathBuf,
        holds: Vec<cowshed_os::process::Holder>,
        open: Vec<cowshed_os::process::Holder>,
    },
    /// A lifecycle operation taking its image locks without waiting ([`LockMode::Try`]) found one
    /// held: a pass finishing another process's residue never waits while it holds an intent lease
    /// (01_storage.md, "Lock order"). `held` names each lock it asked for with who has it open.
    #[error("image locks are held by another operation: {}", held_locks(.held))]
    ImageLocksHeld { held: Vec<HeldImageLock> },
    #[error("lifecycle conflict: {0}")]
    Conflict(#[from] super::lifecycle::Conflict),
    #[error("derived APFS state is inconsistent: {0}")]
    Derivation(#[from] super::lifecycle::DerivationError),
    #[error("workspace publication is pending its controller fence: {0}")]
    PendingPublication(PathBuf),
    #[error("blocking APFS task failed: {0}")]
    BlockingTask(String),
    #[error("requested capacity {requested} does not exceed the image's current {current}")]
    CapacityNotGrowing {
        current: ImageCapacity,
        requested: ImageCapacity,
    },
    #[error("resized image reports {observed}, short of the requested {requested}")]
    ResizeNotObserved {
        requested: ImageCapacity,
        observed: ImageCapacity,
    },
    #[error(
        "rewriting {path} needs {needed} free bytes on its volume and {available} are available"
    )]
    InsufficientSpace {
        path: PathBuf,
        needed: u64,
        available: u64,
    },
    #[error("unexpected lifecycle operation result")]
    UnexpectedResult,
    /// A physical-volume operation asked of a backend whose workspaces are not kernel volumes.
    #[error("{backend} storage does not support {operation}")]
    Unsupported {
        backend: &'static str,
        operation: &'static str,
    },
    #[error("invalid APFS lifecycle plan: {0}")]
    InvalidPlan(&'static str),
    #[error("APFS host operation failed: {0}")]
    Host(String),
    #[error("garbage-collection plan is stale")]
    GcPlanStale,
    #[error("{layout} is missing its CA companion: image={image}, companion={companion}")]
    MissingCaCompanion {
        layout: &'static str,
        image: PathBuf,
        companion: PathBuf,
    },
    #[error("marker does not match detached APFS metadata: {0}")]
    MarkerMismatch(String),
    /// The workspace an operation names has a mount at its mountpoint that does not prove itself
    /// the workspace's.
    #[error(transparent)]
    MountIdentity(#[from] Box<MountRefusal>),
    #[error("workspace {workspace} quarantined ({reason}): {tombstone}")]
    Quarantined {
        workspace: WorkspaceName,
        reason: String,
        tombstone: PathBuf,
    },
    #[error("cleanup after {operation} failed: primary={primary}; cleanup={cleanup}")]
    Cleanup {
        operation: &'static str,
        primary: Box<ApfsStorageError>,
        cleanup: Box<ApfsStorageError>,
    },
}

impl From<cowshed_apfs::store_lock::StoreLockError> for ApfsStorageError {
    fn from(error: cowshed_apfs::store_lock::StoreLockError) -> Self {
        use cowshed_apfs::store_lock::StoreLockError;
        match error {
            StoreLockError::EscapesStoreRoot => Self::Layout(StorageLayoutError::EscapesStoreRoot),
            StoreLockError::InvalidPath(reason) => Self::InvalidPlan(reason),
            StoreLockError::Nul(path) => {
                Self::Host(format!("controller path contains NUL: {}", path.display()))
            }
            StoreLockError::SymlinkComponent(path) => {
                Self::Layout(StorageLayoutError::SymlinkComponent(path))
            }
            replaced @ StoreLockError::Replaced { .. } => Self::Host(replaced.to_string()),
            StoreLockError::Io {
                operation,
                path,
                source,
            } => Self::Io {
                operation,
                path,
                source,
            },
        }
    }
}

impl From<cowshed_apfs::template::TemplateError> for ApfsStorageError {
    fn from(error: cowshed_apfs::template::TemplateError) -> Self {
        use cowshed_apfs::template::TemplateError;
        match error {
            TemplateError::Apfs(error) => error.into(),
            TemplateError::Lock(error) => error.into(),
            TemplateError::Io {
                operation,
                path,
                source,
            } => Self::Io {
                operation,
                path,
                source,
            },
            TemplateError::InvalidPlan(reason) => Self::InvalidPlan(reason),
            TemplateError::Cleanup {
                operation,
                primary,
                cleanup,
            } => Self::Cleanup {
                operation,
                primary: Box::new((*primary).into()),
                cleanup: Box::new((*cleanup).into()),
            },
        }
    }
}

/// One lock of an [`ApfsStorageError::ImageLocksHeld`], with who has it open or why they cannot be
/// named.
#[derive(Debug)]
pub struct HeldImageLock {
    pub lock: PathBuf,
    pub holders: Result<Vec<cowshed_os::process::Holder>, String>,
}

impl ApfsStorageError {
    /// `locks`, which an acquisition without waiting found held, each named with who has it open.
    fn image_locks_held(locks: &[PathBuf]) -> Self {
        Self::ImageLocksHeld {
            held: locks
                .iter()
                .map(|lock| HeldImageLock {
                    lock: lock.clone(),
                    holders: crate::build_volume::nx::holders(lock)
                        .map_err(|error| error.to_string()),
                })
                .collect(),
        }
    }
}

/// [`ApfsStorageError::ImageLocksHeld`]'s locks, each with its holders.
fn held_locks(held: &[HeldImageLock]) -> String {
    held.iter()
        .map(|held| match &held.holders {
            Ok(holders) => format!("{} by {}", held.lock.display(), held_by(holders)),
            Err(error) => format!(
                "{} by processes that cannot be named ({error})",
                held.lock.display()
            ),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Take `locks` in `mode`. Under [`LockMode::Try`] a held one is
/// [`ApfsStorageError::ImageLocksHeld`], never a wait.
fn lock_images_in<H: ApfsExecutionHost>(
    host: &H,
    locks: &[PathBuf],
    mode: LockMode,
    holding: &LockHolding,
) -> Result<H::LockGuard, ApfsStorageError> {
    host.lock_images(locks, mode, holding)?
        .ok_or_else(|| match mode {
            LockMode::Try => ApfsStorageError::image_locks_held(locks),
            LockMode::Wait => {
                ApfsStorageError::InvalidPlan("blocking image lock unexpectedly unavailable")
            }
        })
}

/// [`ApfsStorageError::BuildVolumeHeld`]'s holders of the hold file.
fn held_by(holds: &[cowshed_os::process::Holder]) -> String {
    if holds.is_empty() {
        "nothing this user can see".to_owned()
    } else {
        crate::build_volume::nx::Holders(holds).to_string()
    }
}

/// [`ApfsStorageError::BuildVolumeHeld`] said by what holds the volume: a job admitted on it
/// (its hold file's holders), else the processes that have it open, else neither, visibly.
fn build_volume_held(
    image: &Path,
    hold: &Path,
    holds: &[cowshed_os::process::Holder],
    open: &[cowshed_os::process::Holder],
) -> String {
    let image = image.display();
    match (holds.is_empty(), open.is_empty()) {
        (false, _) => format!(
            "build volume {image} is held by a job admitted on it: {} is held by {}{}",
            hold.display(),
            crate::build_volume::nx::Holders(holds),
            open_in(open)
        ),
        (true, false) => format!(
            "build volume {image} is in use: it is open in {}",
            crate::build_volume::nx::Holders(open)
        ),
        (true, true) => format!(
            "build volume {image} is in use, but no process of this user holds {} or has the volume open",
            hold.display()
        ),
    }
}

/// What to do about a [`ApfsStorageError::BuildVolumeHeld`]: end the process that has it, or
/// wait for it. Nothing about the store needs repair.
#[cfg(target_os = "macos")]
pub(crate) fn build_volume_held_hint(
    holds: &[cowshed_os::process::Holder],
    open: &[cowshed_os::process::Holder],
) -> String {
    match (holds.is_empty(), open.is_empty()) {
        (false, _) => format!(
            "wait for the job holding it ({}) to finish, or end it, then retry",
            crate::build_volume::nx::Holders(holds)
        ),
        (true, false) => format!(
            "end {} or wait for it to exit, then retry",
            crate::build_volume::nx::Holders(open)
        ),
        (true, true) => "no process of this user holds it: another user's process (Spotlight's \
             `mds`) or the kernel does, and `sudo lsof +f -- <its mountpoint>` is the next look; \
             end it or wait, then retry"
            .to_owned(),
    }
}
/// [`ApfsStorageError::BuildVolumeHeld`]'s processes with the volume open, when there are any.
fn open_in(open: &[cowshed_os::process::Holder]) -> String {
    if open.is_empty() {
        String::new()
    } else {
        format!(
            "; its volume is open in {}",
            crate::build_volume::nx::Holders(open)
        )
    }
}

impl From<ExecuteError<ApfsStorageError>> for ApfsStorageError {
    fn from(error: ExecuteError<ApfsStorageError>) -> Self {
        match error {
            ExecuteError::Conflict(conflict) => Self::Conflict(conflict),
            ExecuteError::Backend(error) => error,
        }
    }
}

#[derive(Debug)]
enum Applied {
    Lifecycle(LifecycleReceipt),
    Retired(RetiredRef),
}

pub struct ApfsSubstrate<H, L = TokioApfsBlockingLane> {
    planner: PurePlanner,
    host: Arc<H>,
    lane: Arc<L>,
    config: Arc<ApfsSubstrateConfig>,
    incarnations: Arc<dyn IncarnationSource>,
    /// How lifecycle operations take image locks: [`LockMode::Wait`], except while a pass finishing
    /// another process's residue runs ([`Self::take_image_locks`]).
    image_lock_mode: LockMode,
}

impl<H, L> Clone for ApfsSubstrate<H, L> {
    fn clone(&self) -> Self {
        Self {
            planner: self.planner,
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            image_lock_mode: self.image_lock_mode,
        }
    }
}

impl<H> ApfsSubstrate<H, TokioApfsBlockingLane>
where
    H: ApfsExecutionHost,
{
    pub fn new(config: ApfsSubstrateConfig, host: H) -> Self {
        Self::with_lane(config, host, TokioApfsBlockingLane)
    }
}

impl<H, L> ApfsSubstrate<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    pub fn with_lane(config: ApfsSubstrateConfig, host: H, lane: L) -> Self {
        Self::with_lane_and_incarnations(config, host, lane, UuidIncarnationSource)
    }

    pub fn with_lane_and_incarnations(
        config: ApfsSubstrateConfig,
        host: H,
        lane: L,
        incarnations: impl IncarnationSource,
    ) -> Self {
        Self {
            planner: PurePlanner,
            host: Arc::new(host),
            lane: Arc::new(lane),
            config: Arc::new(config),
            incarnations: Arc::new(incarnations),
            image_lock_mode: LockMode::Wait,
        }
    }

    pub fn config(&self) -> &ApfsSubstrateConfig {
        &self.config
    }

    /// Take image locks in `mode` from now on, answering the mode it replaces. Under
    /// [`LockMode::Try`] a lifecycle operation that finds one held refuses with
    /// [`ApfsStorageError::ImageLocksHeld`] instead of waiting: a pass finishing another process's
    /// residue runs so, since it never waits while it holds an intent lease (01_storage.md,
    /// "Lock order").
    #[cfg(target_os = "macos")]
    pub(crate) fn take_image_locks(&mut self, mode: LockMode) -> LockMode {
        std::mem::replace(&mut self.image_lock_mode, mode)
    }

    /// Wait until no other process holds any of `locks`, then let go of them at once. Only a caller
    /// holding no intent lease waits (`_unleased`), so a verb on a name whose residue that caller
    /// would finish is never refused that name's lease meanwhile (01_storage.md, "Lock order").
    #[cfg(target_os = "macos")]
    pub(crate) async fn wait_for_image_locks(
        &self,
        _unleased: super::recovery::Unleased<'_>,
        locks: Box<[PathBuf]>,
        holding: LockHolding,
    ) -> Result<(), ApfsStorageError> {
        let host = Arc::clone(&self.host);
        self.lane
            .dispatch(move || {
                lock_images_in(host.as_ref(), &locks, LockMode::Wait, &holding).map(drop)
            })
            .await
    }
    pub fn host(&self) -> &H {
        &self.host
    }

    /// The host, for work that outlives the call that started it.
    pub fn shared_host(&self) -> Arc<H> {
        Arc::clone(&self.host)
    }

    /// Detach main and atomically restore the exact checkout retained by adoption.
    pub async fn restore_adopted_checkout(
        &self,
        workspace: &LifecycleWorkspace,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), ApfsStorageError> {
        if !workspace.name().is_main()
            || self.config.checkout_path == pre_cowshed_checkout
            || pre_cowshed_checkout.parent() != self.config.checkout_path.parent()
        {
            return Err(ApfsStorageError::InvalidPlan(
                "adoption rollback requires main and its exact pre-cowshed sibling",
            ));
        }
        let mut expected_pre = self.config.checkout_path.as_os_str().to_owned();
        expected_pre.push(".pre-cowshed");
        if Path::new(&expected_pre) != pre_cowshed_checkout {
            return Err(ApfsStorageError::InvalidPlan(
                "adoption rollback requires main and its exact pre-cowshed sibling",
            ));
        }

        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        let source_checkout = self.config.checkout_path.clone();
        let pre_cowshed_checkout = pre_cowshed_checkout.to_owned();
        let holding = LockHolding::of(LockOperation::RestoreCheckout, workspace.name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, _| {
            host.restore_adopted_checkout(&workspace, &source_checkout, &pre_cowshed_checkout)
        })
        .await
    }

    /// Complete an adoption whose main is published but whose checkout handoff or mount at the
    /// checkout was interrupted: hand the checkout over as `handoff` says if that has not
    /// happened, then mount main there, reusing a surviving attachment. Idempotent once main is
    /// mounted.
    pub async fn finish_adoption(
        &self,
        workspace: &LifecycleWorkspace,
        handoff: &AdoptHandoff,
    ) -> Result<(), ApfsStorageError> {
        if !workspace.name().is_main() {
            return Err(ApfsStorageError::InvalidPlan(
                "only main's adoption has a checkout to finish",
            ));
        }
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        let handoff = handoff.clone();
        let holding = LockHolding::of(LockOperation::FinishAdoption, workspace.name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, config| {
            finish_adoption(host.as_ref(), &config, &workspace, &handoff)
        })
        .await
    }

    /// Repair an unpublished clone in place without discarding its possibly only-copy contents
    /// or activating it. A retry of the original intent remains the publication authority.
    pub async fn defragment_pending_clone(
        &self,
        metadata: crate::metadata::DetachedWorkspaceMetadata,
    ) -> Result<DefragmentOutcome, ApfsStorageError> {
        if metadata.publication_state != crate::metadata::PublicationState::PendingFence
            || metadata.workspace.is_main()
        {
            return Err(ApfsStorageError::InvalidPlan(
                "defrag requires a pending session clone",
            ));
        }
        let workspace = native::metadata_workspace_ref(&metadata)?;
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let holding = LockHolding::of(LockOperation::RepairUnpublished, workspace.name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, config| {
            let image = canonical_image_path(&config, &workspace)?;
            host.validate_pending_clone(&image, &metadata)?;
            let mount = mount_point(&config, &workspace)?;
            host.defragment(&workspace, &image, &mount)
        })
        .await
    }

    /// Retire an unpublished main nothing will finish: release whatever the kernel still attaches
    /// from it and reclaim the image with its sidecar and companion. Only the exact `incarnation`
    /// the caller judged abandoned is touched, re-read under main's lifecycle lock; the checkout
    /// is never involved, because an unpublished adoption never changed it.
    pub async fn discard_pending_adoption(
        &self,
        repo: &RepoId,
        incarnation: &WorkspaceIncarnation,
    ) -> Result<(), ApfsStorageError> {
        let lock_paths = vec![workspace_lock_path(&self.config, repo, &main_name())?];
        let repo = repo.clone();
        let incarnation = incarnation.clone();
        let holding = LockHolding::of(LockOperation::DiscardAdoption, &main_name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, config| {
            match host.pending_adoption(&config, &repo)? {
                Some(pending) if pending.incarnation == incarnation => {
                    host.discard_pending(&pending.image)
                }
                Some(_) => Err(ApfsStorageError::InvalidPlan(
                    "unpublished main changed incarnation before its retirement",
                )),
                None => Ok(()),
            }
        })
        .await
    }

    /// Create main's canonical image behind its `PendingFence`, mount it at a staging mountpoint
    /// for the controller to initialize, then publish it and mount it at the checkout, with
    /// `alongside`'s work run beside the staging; its output is answered with the receipt.
    ///
    /// The lifecycle lock remains owned across the callback. The image stays unpublished and the
    /// checkout untouched until `initialize` returns success.
    pub async fn execute_adopt_staged<W, D, DFut, F, Fut, E>(
        &self,
        plan: AdoptPlan,
        alongside: Alongside<W, D>,
        initialize: F,
    ) -> Result<(LifecycleReceipt, W::Output), AdoptExecutionError<E>>
    where
        W: Future + Send,
        W::Output: Send,
        D: FnOnce(W::Output) -> DFut + Send,
        DFut: Future<Output = ()> + Send,
        F: FnOnce(AdoptStage) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send,
    {
        let backend = CheckedApfsBackend {
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            expected: plan.expected().to_vec(),
            image_lock_mode: self.image_lock_mode,
        };
        let mut guard = backend.acquire(plan.operation()).await?;
        let actual = backend
            .read_authoritative(&mut guard, plan.expected())
            .await?;
        revalidate(plan.expected(), &actual).map_err(ApfsStorageError::from)?;

        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let incarnations = Arc::clone(&self.incarnations);
        let expected = plan.expected().to_vec();
        let operation = plan.operation().clone();
        let staging = self.lane.dispatch(move || {
            let Operation::Adopt {
                repo,
                capacity,
                source_checkout,
                handoff,
                identity,
            } = &operation
            else {
                return Err(ApfsStorageError::InvalidPlan(
                    "staged adopt executor requires an adopt operation",
                ));
            };
            prepare_adopt_stage(
                host.as_ref(),
                &config,
                &expected,
                AdoptExecution {
                    repo,
                    capacity: *capacity,
                    source_checkout,
                    handoff,
                    identity,
                },
                incarnations.as_ref(),
            )
        });
        let Alongside { work, abandon } = alongside;
        let (prepared, beside) = tokio::join!(staging, work);
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                abandon(beside).await;
                return Err(error.into());
            }
        };
        let prepared =
            StagedCallbackGuard::new(Arc::clone(&self.host), prepared, abort_prepared_adopt::<H>);

        if let Err(initializer) = initialize(prepared.get().stage.clone()).await {
            let prepared = prepared.into_prepared();
            let host = Arc::clone(&self.host);
            let cleanup = self
                .lane
                .dispatch(move || abort_prepared_adopt(host.as_ref(), prepared))
                .await;
            abandon(beside).await;
            return Err(match cleanup {
                Ok(()) => StagedExecutionError::Initializer(initializer),
                Err(cleanup) => StagedExecutionError::InitializerCleanup {
                    initializer,
                    cleanup,
                },
            });
        }

        let prepared = prepared.into_prepared();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let applied = self
            .lane
            .dispatch(move || commit_prepared_adopt(host.as_ref(), &config, prepared))
            .await;
        match applied {
            Ok(Applied::Lifecycle(receipt)) => Ok((receipt, beside)),
            Ok(_) => {
                abandon(beside).await;
                Err(ApfsStorageError::UnexpectedResult.into())
            }
            Err(error) => {
                abandon(beside).await;
                Err(error.into())
            }
        }
    }

    pub async fn execute_create_staged<C, R, RF, AC, ACF, AR, ARF, F, Fut, E>(
        &self,
        plan: CreatePlan,
        beside: Beside<C, R, AC, AR>,
        initialize: F,
    ) -> Result<LifecycleReceipt, CreateExecutionError<E>>
    where
        C: Future + Send,
        C::Output: Send,
        R: FnOnce(C::Output, WorkspaceIncarnation) -> RF + Send,
        RF: Future + Send,
        RF::Output: Send,
        AC: FnOnce(C::Output) -> ACF + Send,
        ACF: Future<Output = ()> + Send,
        AR: FnOnce(RF::Output) -> ARF + Send,
        ARF: Future<Output = ()> + Send,
        F: FnOnce(CreateStage, RF::Output) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send + std::fmt::Display,
    {
        self.execute_clone_staged(plan, CloneKind::Create, beside, initialize)
            .await
    }

    pub async fn execute_fork_staged<C, R, RF, AC, ACF, AR, ARF, F, Fut, E>(
        &self,
        plan: ForkPlan,
        beside: Beside<C, R, AC, AR>,
        initialize: F,
    ) -> Result<LifecycleReceipt, ForkExecutionError<E>>
    where
        C: Future + Send,
        C::Output: Send,
        R: FnOnce(C::Output, WorkspaceIncarnation) -> RF + Send,
        RF: Future + Send,
        RF::Output: Send,
        AC: FnOnce(C::Output) -> ACF + Send,
        ACF: Future<Output = ()> + Send,
        AR: FnOnce(RF::Output) -> ARF + Send,
        ARF: Future<Output = ()> + Send,
        F: FnOnce(ForkStage, RF::Output) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send + std::fmt::Display,
    {
        self.execute_clone_staged(plan, CloneKind::Fork, beside, initialize)
            .await
    }

    /// A create or fork (02_workspaces.md, "`cowshed fork`"; 16_build_volumes.md, "Fork"). Its
    /// image locks are taken one at a time in path order, as one acquisition takes a set, so the
    /// source's can be let go alone: it is held while the source's image and `beside.capture`'s
    /// share of what it guards (a fork's seed) are captured, milliseconds of `clonefile`s, and let
    /// go before any attach, mount or initializer runs, which read only the clone.
    /// A git-worktree clone keeps it to the end: its initializer registers it in main's
    /// repository. The destination's lock is held to the end. The hold is said on stderr as
    /// `cowshed: apfs source-lock held=<duration>`.
    async fn execute_clone_staged<P, C, R, RF, AC, ACF, AR, ARF, F, Fut, E>(
        &self,
        plan: P,
        kind: CloneKind,
        beside: Beside<C, R, AC, AR>,
        initialize: F,
    ) -> Result<LifecycleReceipt, StagedExecutionError<E>>
    where
        P: ImmutablePlan,
        C: Future + Send,
        C::Output: Send,
        R: FnOnce(C::Output, WorkspaceIncarnation) -> RF + Send,
        RF: Future + Send,
        RF::Output: Send,
        AC: FnOnce(C::Output) -> ACF + Send,
        ACF: Future<Output = ()> + Send,
        AR: FnOnce(RF::Output) -> ARF + Send,
        ARF: Future<Output = ()> + Send,
        F: FnOnce(WorkspaceStage, RF::Output) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send + std::fmt::Display,
    {
        let (source, destination, identity) = match plan.operation() {
            Operation::Create {
                source,
                destination,
                identity,
            } if kind == CloneKind::Create => (source, destination, identity),
            Operation::Fork {
                source,
                destination,
                identity,
            } if kind == CloneKind::Fork => (source, destination, identity),
            Operation::Create { .. } | Operation::Fork { .. } => {
                return Err(ApfsStorageError::InvalidPlan(
                    "staged clone executor operation kind mismatch",
                )
                .into());
            }
            _ => {
                return Err(ApfsStorageError::InvalidPlan(
                    "staged clone executor requires a create or fork operation",
                )
                .into());
            }
        };
        let (source, destination, identity) =
            (source.clone(), destination.clone(), identity.clone());
        // A git-worktree clone's initializer registers it in main's repository; every other
        // clone's initializer changes only the clone and what its incarnation owns.
        let reaches_main = identity.git_worktree;
        let source_lock =
            workspace_lock_path(&self.config, expected_repo(plan.expected())?, &source)?;
        let paths = operation_lock_paths(&self.config, plan.expected(), plan.operation())?;
        let holding = operation_holding(plan.operation());
        let mode = self.image_lock_mode;
        let (mut locks, actual) = {
            let host = Arc::clone(&self.host);
            let config = Arc::clone(&self.config);
            let expected = plan.expected().to_vec();
            self.lane
                .dispatch(move || {
                    let mut locks = Vec::with_capacity(paths.len());
                    for path in &paths {
                        let guard = lock_images_in(
                            host.as_ref(),
                            std::slice::from_ref(path),
                            mode,
                            &holding,
                        )?;
                        locks.push((path.clone(), guard));
                    }
                    host.recover_pending(&config, &paths)?;
                    let actual = host.observe(&expected)?;
                    Ok((locks, actual))
                })
                .await?
        };
        let locked_at = std::time::Instant::now();
        revalidate(plan.expected(), &actual).map_err(ApfsStorageError::from)?;

        let capturing = {
            let host = Arc::clone(&self.host);
            let config = Arc::clone(&self.config);
            let incarnations = Arc::clone(&self.incarnations);
            let expected = plan.expected().to_vec();
            self.lane.dispatch(move || {
                capture_clone_stage(
                    host.as_ref(),
                    &config,
                    &expected,
                    CloneExecution {
                        source: &source,
                        destination: &destination,
                        fork: kind == CloneKind::Fork,
                        identity: &identity,
                    },
                    incarnations.as_ref(),
                )
            })
        };
        let Beside {
            capture,
            ready,
            abandon_captured,
            abandon_ready,
        } = beside;
        let (captured, beside_captured) = tokio::join!(capturing, capture);
        let captured = match captured {
            Ok(captured) => captured,
            Err(error) => {
                abandon_captured(beside_captured).await;
                return Err(error.into());
            }
        };
        if !reaches_main {
            locks.retain(|(path, _)| *path != source_lock);
            cowshed_os::timing::attribute(
                "apfs",
                format_args!("source-lock"),
                "held",
                format_args!("{:?}", locked_at.elapsed()),
            );
        }
        let incarnation = captured.clone.workspace.incarnation().clone();
        let staging = {
            let host = Arc::clone(&self.host);
            let config = Arc::clone(&self.config);
            self.lane
                .dispatch(move || stage_captured_clone(host.as_ref(), &config, captured))
        };
        let (prepared, beside) = tokio::join!(staging, ready(beside_captured, incarnation));
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                abandon_ready(beside).await;
                return Err(error.into());
            }
        };
        let prepared = StagedCallbackGuard::new(
            Arc::clone(&self.host),
            prepared,
            preserve_prepared_clone::<H>,
        );

        let initialized = timed_async(
            "apfs",
            "canonical/init",
            initialize(prepared.get().stage.clone(), beside),
        )
        .await;
        if let Err(initializer) = initialized {
            let prepared = prepared.into_prepared();
            if prepared.stage.resuming || reaches_main {
                // The callback can already have changed state outside the image (a worktree
                // registration in main), or an earlier attempt did. The PendingFence is the
                // rollback boundary: keep the exact mounted clone so the durable lifecycle intent
                // can re-enter the initializer with resume authority rather than orphaning
                // those effects.
                drop(prepared);
                return Err(StagedExecutionError::Initializer(initializer));
            }
            // A fresh clone whose initializer reaches nothing outside it holds nothing a retry
            // needs: it was never published, and its source still holds every byte it copied.
            // Kept, every retry would resume this copy of the state that refused it; reclaimed,
            // the retry clones the source as it is then.
            let host = Arc::clone(&self.host);
            let reclaimed = self
                .lane
                .dispatch(move || {
                    let PreparedClone {
                        attachment, image, ..
                    } = prepared;
                    detach_and_reclaim(host.as_ref(), attachment, &image, "refused clone detach")
                })
                .await;
            return Err(match reclaimed {
                Ok(()) => StagedExecutionError::InitializerReclaimed(initializer),
                Err(cleanup) => StagedExecutionError::InitializerCleanup {
                    initializer,
                    cleanup,
                },
            });
        }
        let prepared = prepared.into_prepared();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let applied = self
            .lane
            .dispatch(move || commit_prepared_clone(host.as_ref(), &config, prepared))
            .await?;
        match applied {
            Applied::Lifecycle(receipt) => Ok(receipt),
            _ => Err(ApfsStorageError::UnexpectedResult.into()),
        }
    }

    /// Retire an unfinished clone without ever publishing it. Inspection runs on its verified
    /// mount under the image lock; a refusal preserves the pending payload and leaves its mount
    /// state as it found it.
    pub async fn execute_pending_clone_retirement<F, Fut, R, E>(
        &self,
        metadata: crate::metadata::DetachedWorkspaceMetadata,
        identity: OperationIdentity,
        inspect: F,
    ) -> Result<(RetiredRef, R), StagedExecutionError<E>>
    where
        F: FnOnce(WorkspaceStage) -> Fut + Send,
        Fut: Future<Output = Result<(RetirementDisposition, R), E>> + Send,
        R: Send,
        E: Send,
    {
        if metadata.workspace.is_main()
            || metadata.publication_state != crate::metadata::PublicationState::PendingFence
        {
            return Err(ApfsStorageError::InvalidPlan(
                "pending retirement requires an unfinished session clone",
            )
            .into());
        }
        let workspace = native::metadata_workspace_ref(&metadata)?;
        let image = canonical_image_path(&self.config, &workspace)?;
        let lock_path = workspace_lock_path(&self.config, workspace.repo(), workspace.name())?;
        let holding = LockHolding::of(LockOperation::RetireUnpublished, workspace.name());
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let mode = self.image_lock_mode;
        let (lock, prepared) = self
            .lane
            .dispatch(move || {
                let lock = lock_images_in(host.as_ref(), &[lock_path], mode, &holding)?;
                host.validate_pending_clone(&image, &metadata)?;
                let prepared = mount_clone_stage(
                    host.as_ref(),
                    &config,
                    ResumableClone {
                        workspace,
                        identity,
                        image,
                    },
                    None,
                    true,
                )?;
                Ok((lock, prepared))
            })
            .await?;
        let prepared = StagedCallbackGuard::new(
            Arc::clone(&self.host),
            (lock, prepared),
            |host, (lock, prepared)| {
                let _lock = lock;
                restore_prepared_mount(host, prepared)
            },
        );
        let (disposition, value) = match inspect(prepared.get().1.stage.clone()).await {
            Ok((disposition, value)) => (disposition, value),
            Err(error) => {
                // Nothing was retired, so the image goes back as it was found: a removal that
                // mounted it to inspect it must not leave it mounted.
                let (lock, prepared) = prepared.into_prepared();
                let host = Arc::clone(&self.host);
                let restored = self
                    .lane
                    .dispatch(move || {
                        let _lock = lock;
                        restore_prepared_mount(host.as_ref(), prepared)
                    })
                    .await;
                return Err(match restored {
                    Ok(()) => StagedExecutionError::Initializer(error),
                    Err(cleanup) => StagedExecutionError::InitializerCleanup {
                        initializer: error,
                        cleanup,
                    },
                });
            }
        };
        let (lock, prepared) = prepared.into_prepared();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let retired = self
            .lane
            .dispatch(move || {
                // Cancellation drops the waiter, not release's image ownership.
                let _lock = lock;
                let PreparedClone {
                    stage,
                    attachment,
                    image,
                    found: _,
                } = prepared;
                let trash = retired_image_path(&config, &stage.workspace)?;
                let revision = stage.workspace.revision().get().checked_add(1).ok_or(
                    ApfsStorageError::InvalidPlan("pending retirement revision overflow"),
                )?;
                host.detach(attachment, DetachIntent::Release)?;
                let retired = RetiredRef::new(stage.workspace, Revision::new(revision));
                match disposition {
                    RetirementDisposition::Release => host.release_workspace(&config, &retired)?,
                    RetirementDisposition::Recovery => host.retire_image(&image, &trash)?,
                }
                Ok(retired)
            })
            .await?;
        Ok((retired, value))
    }

    /// Retire under the existing image fence, then publish the lifecycle commitment.
    /// Clean recoverable sources are physically released; recovery sources retain trash.
    pub async fn execute_retire_staged<F, Fut, E>(
        &self,
        plan: RetirePlan,
        disposition: RetirementDisposition,
        fence: F,
    ) -> Result<RetiredRef, RetireExecutionError<E>>
    where
        F: FnOnce(RetiredRef) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send,
    {
        let retired = match disposition {
            RetirementDisposition::Recovery => match self.execute(&plan).await? {
                Applied::Retired(retired) => retired,
                _ => return Err(ApfsStorageError::UnexpectedResult.into()),
            },
            RetirementDisposition::Release => {
                let locks = operation_lock_paths(&self.config, plan.expected(), plan.operation())?;
                let holding = operation_holding(plan.operation());
                self.dispatch_with_locks(locks, holding, true, move |host, config| {
                    revalidate(plan.expected(), &host.observe(plan.expected())?)?;
                    let Operation::Retire { workspace, .. } = plan.operation() else {
                        return Err(ApfsStorageError::UnexpectedResult);
                    };
                    let current = active_expected(plan.expected(), workspace)?;
                    let revision = current.revision().get().checked_add(1).ok_or(
                        ApfsStorageError::InvalidPlan("retirement revision overflow"),
                    )?;
                    host.detach_mounted(&current, DetachIntent::Release)?;
                    let retired = RetiredRef::new(current, Revision::new(revision));
                    host.release_workspace(&config, &retired)?;
                    Ok(retired)
                })
                .await?
            }
        };
        if let Err(source) = fence(retired.clone()).await {
            return Err(RetireExecutionError::Fence { source, retired });
        }
        Ok(retired)
    }

    /// Retire adopted main only after its exact pre-cowshed checkout has been restored.
    ///
    /// Ordinary lifecycle planning keeps main permanent. This narrow terminal path requires the
    /// main image to be detached and still match the exact current lifecycle identity, then moves
    /// its image, sidecar, and CA key to recoverable trash before publishing the retirement fence.
    pub async fn execute_restored_main_retirement<F, Fut, E>(
        &self,
        workspace: &LifecycleWorkspace,
        fence: F,
    ) -> Result<RetiredRef, RetireExecutionError<E>>
    where
        F: FnOnce(RetiredRef) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send,
    {
        if !workspace.name().is_main() {
            return Err(
                ApfsStorageError::InvalidPlan("restored-main retirement requires main").into(),
            );
        }
        let workspace = workspace.clone();
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let holding = LockHolding::of(LockOperation::RetireMain, workspace.name());
        let retired = self
            .dispatch_with_locks(lock_paths, holding, true, move |host, config| {
                let volume = volume_key(workspace.repo(), workspace.name());
                if host
                    .mounts(workspace.repo())?
                    .iter()
                    .any(|mount| mount.volume_key() == volume)
                {
                    return Err(ApfsStorageError::InvalidPlan(
                        "restored main image remains mounted",
                    ));
                }
                if !host
                    .list(workspace.repo())?
                    .into_iter()
                    .any(|fact| fact.workspace == workspace)
                {
                    return Err(ApfsStorageError::MarkerMismatch(
                        "restored main image no longer matches its lifecycle identity".to_owned(),
                    ));
                }
                let canonical = canonical_image_path(&config, &workspace)?;
                let trash = retired_image_path(&config, &workspace)?;
                host.retire_image(&canonical, &trash)?;
                let revision = workspace.revision().get().checked_add(1).ok_or(
                    ApfsStorageError::InvalidPlan("restored main retirement revision overflow"),
                )?;
                Ok(RetiredRef::new(workspace, Revision::new(revision)))
            })
            .await?;
        if let Err(source) = fence(retired.clone()).await {
            return Err(RetireExecutionError::Fence { source, retired });
        }
        Ok(retired)
    }

    /// Mount `workspace` without waiting for its image lock. With the lock free this is
    /// [`Substrate::ensure_mounted`]. A held lock is a live lifecycle operation's: a fork holds
    /// its source's while it clones it (16_build_volumes.md, "Fork" step 1), and an `rm` or a
    /// defragmentation holds the workspace's own; that process may itself be waiting on the caller,
    /// as a client waits on the gateway whose startup heals main. A workspace that already stands
    /// as `ensure_mounted` leaves it ([`ApfsExecutionHost::mount_settled`]) is served as it
    /// stands, its marker validated. One that needs healing, mounting or relinking, which only the
    /// lock authorizes, answers `None`: the caller leaves it, or waits.
    pub(crate) async fn mount_without_waiting(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<Option<PathBuf>, ApfsStorageError> {
        let lock = workspace_lock_path(&self.config, workspace.repo(), workspace.name())?;
        let holding = LockHolding::of(LockOperation::Mount, workspace.name());
        let workspace = workspace.clone();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        self.lane
            .dispatch(move || {
                let locks = std::slice::from_ref(&lock);
                match host.lock_images(locks, LockMode::Try, &holding)? {
                    Some(_guard) => {
                        host.recover_pending(&config, locks)?;
                        settle_mount(host.as_ref(), &config, &workspace, MountIntent::default())
                            .map(Some)
                    }
                    None => {
                        let mount_point = mount_point(&config, &workspace)?;
                        if !host.mount_settled(&workspace, &mount_point)? {
                            return Ok(None);
                        }
                        host.validate_marker(
                            &mount_point,
                            &MarkerExpectation::owned(&config, &workspace),
                        )?;
                        Ok(Some(mount_point))
                    }
                }
            })
            .await
    }

    /// Wait until no other process holds `workspace`'s image lock, then let go of it at once.
    /// A caller that [`Self::mount_without_waiting`] answered `None` waits so, holding no lease,
    /// before it takes its lease and mounts without waiting again (05_gateway.md, "Startup",
    /// step 3; 01_storage.md, "Lock order").
    pub(crate) async fn wait_for_mount_lock(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<(), ApfsStorageError> {
        let lock = workspace_lock_path(&self.config, workspace.repo(), workspace.name())?;
        let holding = LockHolding::of(LockOperation::Mount, workspace.name());
        let host = Arc::clone(&self.host);
        self.lane
            .dispatch(move || {
                lock_images_in(
                    host.as_ref(),
                    std::slice::from_ref(&lock),
                    LockMode::Wait,
                    &holding,
                )
                .map(drop)
            })
            .await
    }

    /// Mount `workspace` for the start of its supervisor (11_shell.md, "Supervisor"):
    /// [`Self::mount_without_waiting`], and a mount that needs the held lock waits for it in
    /// `flock` once it has said on stderr which processes hold it. The wait has no deadline of
    /// its own: the supervisor manager's start bound is its only one.
    #[cfg(target_os = "macos")]
    pub(crate) async fn mount_for_supervisor(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<PathBuf, ApfsStorageError> {
        if let Some(mount) = self.mount_without_waiting(workspace).await? {
            return Ok(mount);
        }
        let lock = workspace_lock_path(&self.config, workspace.repo(), workspace.name())?;
        let holding = LockHolding::of(LockOperation::Mount, workspace.name());
        let workspace = workspace.clone();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        self.lane
            .dispatch(move || {
                let locks = std::slice::from_ref(&lock);
                let holders = match crate::build_volume::nx::holders(&lock) {
                    Ok(holders) => held_by(&holders),
                    Err(error) => format!("processes that cannot be named ({error})"),
                };
                cowshed_os::eprintln!(
                    "cowshed: the supervisor of {} waits for its image lock {}, held by {holders}, to settle its mount",
                    workspace.name(),
                    lock.display()
                );
                let _guard = host.lock_images(locks, LockMode::Wait, &holding)?.ok_or(
                    ApfsStorageError::InvalidPlan("blocking image lock unexpectedly unavailable"),
                )?;
                host.recover_pending(&config, locks)?;
                settle_mount(host.as_ref(), &config, &workspace, MountIntent::default())
            })
            .await
    }

    async fn execute<P: ImmutablePlan>(&self, plan: &P) -> Result<Applied, ApfsStorageError> {
        let backend = CheckedApfsBackend {
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            expected: plan.expected().to_vec(),
            image_lock_mode: self.image_lock_mode,
        };
        execute_checked(&backend, plan).await.map_err(Into::into)
    }

    async fn dispatch_read<T, F>(&self, job: F) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<H>, Arc<ApfsSubstrateConfig>) -> Result<T, ApfsStorageError> + Send + 'static,
    {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        self.lane.dispatch(move || job(host, config)).await
    }

    async fn dispatch_with_locks<T, F>(
        &self,
        lock_paths: Vec<PathBuf>,
        holding: LockHolding,
        recover: bool,
        job: F,
    ) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<H>, Arc<ApfsSubstrateConfig>) -> Result<T, ApfsStorageError> + Send + 'static,
    {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let mode = self.image_lock_mode;
        self.lane
            .dispatch(move || {
                let _guard = lock_images_in(host.as_ref(), &lock_paths, mode, &holding)?;
                if recover {
                    host.recover_pending(&config, &lock_paths)?;
                }
                job(host, config)
            })
            .await
    }
    #[cfg(any(target_os = "macos", test))]
    /// Runs a non-lifecycle metadata mutation while holding the same hardened image lock used by
    /// APFS lifecycle operations. The job's result is deliberately opaque to this layer so callers
    /// can preserve their own error taxonomy while lock acquisition remains an APFS concern.
    pub(crate) async fn dispatch_with_image_lock<T, F>(
        &self,
        lock_path: PathBuf,
        holding: LockHolding,
        job: F,
    ) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.dispatch_with_locks(vec![lock_path], holding, false, move |_, _| Ok(job()))
            .await
    }

    /// Runs `job` against the execution host on the blocking lane while holding `lock_path`, the
    /// same image lock lifecycle operations take: a re-adoption's retention and its build
    /// replacement's publication happen under main's lock, so staging collection, which takes
    /// it too before it acts, never interleaves with them.
    #[cfg(target_os = "macos")]
    pub(crate) async fn dispatch_on_host_with_image_lock<T, F>(
        &self,
        lock_path: PathBuf,
        holding: LockHolding,
        job: F,
    ) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce(&H) -> Result<T, ApfsStorageError> + Send + 'static,
    {
        self.dispatch_with_locks(vec![lock_path], holding, false, move |host, _| {
            job(host.as_ref())
        })
        .await
    }
}

impl<H, L> LifecyclePlanner for ApfsSubstrate<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    fn plan_adopt(&self, request: AdoptRequest) -> Result<AdoptPlan, PlanError> {
        self.planner.plan_adopt(request)
    }

    fn plan_create(
        &self,
        from: &LifecycleWorkspace,
        destination: Destination,
    ) -> Result<CreatePlan, PlanError> {
        self.planner.plan_create(from, destination)
    }

    fn plan_fork(
        &self,
        from: &LifecycleWorkspace,
        destination: Destination,
    ) -> Result<ForkPlan, PlanError> {
        self.planner.plan_fork(from, destination)
    }

    fn plan_retire(&self, workspace: &LifecycleWorkspace) -> Result<RetirePlan, PlanError> {
        self.planner.plan_retire(workspace)
    }
}

#[async_trait]
impl<H, L> Substrate for ApfsSubstrate<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    type Error = ApfsStorageError;

    async fn execute_retire(&self, plan: RetirePlan) -> Result<RetiredRef, Self::Error> {
        match self.execute(&plan).await? {
            Applied::Retired(retired) => Ok(retired),
            _ => Err(ApfsStorageError::UnexpectedResult),
        }
    }

    async fn reclaim(&self, retired: RetiredRef) -> Result<(), Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            retired.workspace().repo(),
            retired.workspace().name(),
        )?];
        let holding = LockHolding::of(LockOperation::Reclaim, retired.workspace().name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, config| {
            host.reclaim_retired(&config, &retired)
        })
        .await
    }

    async fn list(&self, repo: &RepoId) -> Result<Vec<DerivedWorkspace>, Self::Error> {
        let repo = repo.clone();
        self.dispatch_read(move |host, _| {
            let storage = host.list(&repo)?;
            let mounts = host.mounts(&repo)?;
            Ok(super::lifecycle::derive_workspaces(storage, mounts)?)
        })
        .await
    }

    async fn mount_state(&self, workspace: &LifecycleWorkspace) -> Result<MountState, Self::Error> {
        let workspace = workspace.clone();
        self.dispatch_read(move |host, _| host.workspace_mount_state(&workspace))
            .await
    }

    async fn ensure_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        intent: MountIntent,
    ) -> Result<PathBuf, Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        let holding = LockHolding::of(LockOperation::Mount, workspace.name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, config| {
            settle_mount(host.as_ref(), &config, &workspace, intent)
        })
        .await
    }

    async fn unmount(&self, workspace: &LifecycleWorkspace) -> Result<(), Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        // The volume is the user's to be working in: an explicit unmount that cannot land is a
        // busy conflict for the caller to report, never grounds to force it out from under them.
        let holding = LockHolding::of(LockOperation::Unmount, workspace.name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, _| {
            host.detach_mounted(&workspace, DetachIntent::WhenIdle)?;
            host.detach_owned_build_volumes(&workspace)
        })
        .await
    }

    async fn resize(
        &self,
        workspace: &LifecycleWorkspace,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        let holding = LockHolding::of(LockOperation::Resize, workspace.name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, config| {
            let image = canonical_image_path(&config, &workspace)?;
            let mount_point = mount_point(&config, &workspace)?;
            host.resize(&workspace, &image, &mount_point, capacity)
        })
        .await
    }

    async fn defragment(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<DefragmentOutcome, Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        let holding = LockHolding::of(LockOperation::Defragment, workspace.name());
        self.dispatch_with_locks(lock_paths, holding, true, move |host, config| {
            let image = canonical_image_path(&config, &workspace)?;
            let mount_point = mount_point(&config, &workspace)?;
            host.defragment(&workspace, &image, &mount_point)
        })
        .await
    }

    async fn stats(&self, workspace: &LifecycleWorkspace) -> Result<SubstrateStats, Self::Error> {
        let workspace = workspace.clone();
        self.dispatch_read(move |host, config| {
            let image = canonical_image_path(&config, &workspace)?;
            host.stats(&workspace, &image)
        })
        .await
    }

    async fn preview_gc(
        &self,
        repo: &RepoId,
        options: StorageGcOptions,
    ) -> Result<StorageGcPlan, Self::Error> {
        let repo = repo.clone();
        self.dispatch_read(move |host, config| host.preview_gc(&config, &repo, options))
            .await
    }

    async fn execute_gc(&self, plan: StorageGcPlan) -> Result<StorageGcReport, Self::Error> {
        self.dispatch_read(move |host, config| host.execute_gc(&config, plan))
            .await
    }
}

struct StagedCallbackGuard<H, P>
where
    H: ApfsExecutionHost,
    P: Send,
{
    host: Arc<H>,
    prepared: Option<P>,
    abort: fn(&H, P) -> Result<(), ApfsStorageError>,
}

impl<H, P> StagedCallbackGuard<H, P>
where
    H: ApfsExecutionHost,
    P: Send,
{
    fn new(host: Arc<H>, prepared: P, abort: fn(&H, P) -> Result<(), ApfsStorageError>) -> Self {
        Self {
            host,
            prepared: Some(prepared),
            abort,
        }
    }

    fn get(&self) -> &P {
        self.prepared.as_ref().expect("armed staged callback guard")
    }

    fn into_prepared(mut self) -> P {
        self.prepared.take().expect("armed staged callback guard")
    }
}

impl<H, P> Drop for StagedCallbackGuard<H, P>
where
    H: ApfsExecutionHost,
    P: Send,
{
    fn drop(&mut self) {
        let Some(prepared) = self.prepared.take() else {
            return;
        };
        let host = Arc::clone(&self.host);
        let abort = self.abort;
        std::thread::scope(|scope| {
            let _ = scope.spawn(move || abort(host.as_ref(), prepared)).join();
        });
    }
}

struct CheckedGuard<G> {
    _lock: G,
    paths: Vec<PathBuf>,
}

struct CheckedApfsBackend<H, L> {
    host: Arc<H>,
    lane: Arc<L>,
    config: Arc<ApfsSubstrateConfig>,
    incarnations: Arc<dyn IncarnationSource>,
    expected: Vec<LifecycleFact>,
    image_lock_mode: LockMode,
}

#[async_trait]
impl<H, L> LifecycleBackend for CheckedApfsBackend<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    type Guard = CheckedGuard<H::LockGuard>;
    type Output = Applied;
    type Error = ApfsStorageError;

    async fn acquire(&self, operation: &Operation) -> Result<Self::Guard, Self::Error> {
        let paths = operation_lock_paths(&self.config, &self.expected, operation)?;
        let holding = operation_holding(operation);
        let host = Arc::clone(&self.host);
        let mode = self.image_lock_mode;
        self.lane
            .dispatch(move || {
                let lock = lock_images_in(host.as_ref(), &paths, mode, &holding)?;
                Ok(CheckedGuard { _lock: lock, paths })
            })
            .await
    }

    async fn read_authoritative(
        &self,
        guard: &mut Self::Guard,
        expected: &[LifecycleFact],
    ) -> Result<Vec<LifecycleFact>, Self::Error> {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let held_locks = guard.paths.clone();
        let expected = expected.to_vec();
        self.lane
            .dispatch(move || {
                host.recover_pending(&config, &held_locks)?;
                host.observe(&expected)
            })
            .await
    }

    async fn apply(
        &self,
        _: &mut Self::Guard,
        operation: &Operation,
    ) -> Result<Self::Output, Self::Error> {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let incarnations = Arc::clone(&self.incarnations);
        let expected = self.expected.clone();
        let operation = operation.clone();
        self.lane
            .dispatch(move || {
                apply_operation(
                    host.as_ref(),
                    &config,
                    &expected,
                    &operation,
                    incarnations.as_ref(),
                )
            })
            .await
    }
}

struct AdoptExecution<'a> {
    repo: &'a RepoId,
    capacity: ImageCapacity,
    source_checkout: &'a Path,
    handoff: &'a AdoptHandoff,
    identity: &'a OperationIdentity,
}

struct PreparedAdopt<A> {
    stage: AdoptStage,
    attachment: A,
    /// Main's canonical image, still behind its `PendingFence` sidecar.
    image: PathBuf,
    canonical_mount: PathBuf,
    source_checkout: PathBuf,
    handoff: AdoptHandoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloneKind {
    Create,
    Fork,
}

struct PreparedClone<A> {
    stage: WorkspaceStage,
    attachment: A,
    image: PathBuf,
    found: FoundMount,
}

pub(super) fn workspace_lock_path(
    config: &ApfsSubstrateConfig,
    repo: &RepoId,
    workspace: &WorkspaceName,
) -> Result<PathBuf, ApfsStorageError> {
    let storage = layout(config, repo)?;
    if workspace.is_main() {
        Ok(storage.main_image()?.lock().to_owned())
    } else {
        Ok(storage.session_image(workspace)?.lock().to_owned())
    }
}

/// The request a lifecycle operation holds its image locks for: its verb, and the workspace it
/// makes or changes (a fork's new workspace, though it holds its source's lock too).
fn operation_holding(operation: &Operation) -> LockHolding {
    match operation {
        Operation::Adopt { .. } => LockHolding::of(LockOperation::Adopt, &main_name()),
        Operation::Create { destination, .. } => {
            LockHolding::of(LockOperation::Create, destination)
        }
        Operation::Fork { destination, .. } => LockHolding::of(LockOperation::Fork, destination),
        Operation::Retire { workspace } => LockHolding::of(LockOperation::Retire, workspace),
    }
}

fn operation_lock_paths(
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    operation: &Operation,
) -> Result<Vec<PathBuf>, ApfsStorageError> {
    let repo = match operation {
        Operation::Adopt { repo, .. } => repo,
        _ => expected_repo(expected)?,
    };
    let mut locks = match operation {
        Operation::Adopt { .. } => vec![workspace_lock_path(config, repo, &main_name())?],
        Operation::Create {
            source,
            destination,
            ..
        }
        | Operation::Fork {
            source,
            destination,
            ..
        } => vec![
            workspace_lock_path(config, repo, source)?,
            workspace_lock_path(config, repo, destination)?,
        ],
        Operation::Retire { workspace, .. } => {
            vec![workspace_lock_path(config, repo, workspace)?]
        }
    };
    locks.sort();
    locks.dedup();
    Ok(locks)
}

struct CloneExecution<'a> {
    source: &'a WorkspaceName,
    destination: &'a WorkspaceName,
    fork: bool,
    identity: &'a OperationIdentity,
}

fn apply_operation<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    operation: &Operation,
    _incarnations: &dyn IncarnationSource,
) -> Result<Applied, ApfsStorageError> {
    match operation {
        Operation::Adopt { .. } => Err(ApfsStorageError::InvalidPlan(
            "adopt operations require the staged controller executor",
        )),
        Operation::Create { .. } | Operation::Fork { .. } => Err(ApfsStorageError::InvalidPlan(
            "create and fork operations require the staged controller executor",
        )),
        Operation::Retire { workspace, .. } => apply_retire(host, config, expected, workspace),
    }
}

fn adopted_workspace(
    repo: &RepoId,
    incarnation: WorkspaceIncarnation,
    topology: Revision,
) -> Result<LifecycleWorkspace, ApfsStorageError> {
    LifecycleWorkspace::new(
        repo.clone(),
        main_name(),
        incarnation,
        Revision::new(1),
        Revision::new(topology.get() + 1),
        WorkspaceRole::Main,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid adopted workspace identity"))
}

/// Main's image is created at its canonical path, behind a `PendingFence` sidecar, and attached
/// exactly once: the copy runs on a staging mount of that attachment, and publication is the
/// sidecar's activation followed by a remount of the same attachment at the checkout. Nothing
/// is renamed under an attachment, whose identity is the backing path it was opened with.
fn prepare_adopt_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    execution: AdoptExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<PreparedAdopt<H::Attachment>, ApfsStorageError> {
    let AdoptExecution {
        repo,
        capacity,
        source_checkout,
        handoff,
        identity: requested,
    } = execution;
    if requested.project_root != source_checkout || config.checkout_path != source_checkout {
        return Err(ApfsStorageError::InvalidPlan(
            "adopt source must equal operation project root and the configured checkout path",
        ));
    }
    match handoff {
        AdoptHandoff::Checkout {
            pre_cowshed_checkout,
        } => {
            if pre_cowshed_checkout.exists() {
                return Err(ApfsStorageError::InvalidPlan(
                    "pre-cowshed checkout already exists",
                ));
            }
        }
        // A plain replacement copies from the checkout the user still owns, which stays in place
        // until the verified replacement is published, so only its retained name must be free.
        AdoptHandoff::PlainReplacement { preserved, .. } => {
            if preserved.exists() {
                return Err(ApfsStorageError::InvalidPlan(
                    "plain replacement's preserved checkout already exists",
                ));
            }
        }
        // The replaced main is gone from the checkout: nothing may be mounted there, or main
        // would mount over it.
        AdoptHandoff::Replacement { .. } => host.require_vacant_checkout(source_checkout)?,
    }
    let topology = absent_expected(expected)?;
    let image = layout(config, repo)?.main_image()?.image().to_owned();

    // An adoption a crash interrupted resumes in place: the delta copier skips every leaf the
    // last attempt finished, so the repository copy never starts over. Its sidecar names the
    // source it was copying; a different checkout root or commit is a different adoption, and
    // the unpublished image is replaced — the checkout itself was never touched.
    let mut resumed = None;
    if let Some(pending) = host.pending_adoption(config, repo)? {
        let workspace = adopted_workspace(repo, pending.incarnation.clone(), topology)?;
        let staging = staging_mount(config, &workspace)?;
        let same_source = pending.info.project_root == requested.project_root
            && pending.info.base_commit == requested.base_commit
            && match handoff {
                AdoptHandoff::Checkout { .. } => true,
                AdoptHandoff::Replacement { incarnation, .. }
                | AdoptHandoff::PlainReplacement { incarnation, .. } => {
                    pending.incarnation == *incarnation
                }
            };
        let attachment = if same_source {
            timed_apfs_step("staging", "resume", || {
                host.resume_pending_adopt(&pending.image, &staging, &workspace)
            })?
        } else {
            None
        };
        match attachment {
            Some(attachment) => {
                // The recorded identity, not the retry's: the marker the last attempt may have
                // written then validates as already current instead of growing its own lineage.
                let identity = pending.identity(&requested.created_trace);
                resumed = Some((workspace, identity, attachment, staging));
            }
            None => {
                cowshed_os::eprintln!(
                    "cowshed: replacing unpublished main image {} ({}); the checkout was never touched",
                    pending.image.display(),
                    if same_source {
                        "its payload never became a verified APFS volume"
                    } else {
                        "it was copying a different checkout or commit"
                    }
                );
                host.discard_pending(&pending.image)?;
            }
        }
    }
    let (workspace, identity, attachment, staging, resuming) = match resumed {
        Some((workspace, identity, attachment, staging)) => {
            (workspace, identity, attachment, staging, true)
        }
        None => {
            let incarnation = match handoff {
                AdoptHandoff::Checkout { .. } => incarnations.mint()?,
                AdoptHandoff::Replacement { incarnation, .. }
                | AdoptHandoff::PlainReplacement { incarnation, .. } => incarnation.clone(),
            };
            let workspace = adopted_workspace(repo, incarnation, topology)?;
            let staging = staging_mount(config, &workspace)?;
            // The sidecar is the publication fence and is durable before the payload name
            // appears. A crash in the gap leaves a sidecar-only record that recover_pending
            // removes; once the payload exists, PendingFence keeps it out of enumeration.
            if let Err(primary) = timed_apfs_step("canonical", "metadata-pending", || {
                host.publish_metadata(
                    &image,
                    &workspace,
                    workspace.revision(),
                    MetadataPolicy::FreshPendingFence,
                    Some(requested),
                    None,
                )
            }) {
                return combine_cleanup(
                    "adopt metadata preparation",
                    primary,
                    host.reclaim_image(&image),
                );
            }
            let attachment = match host.create_attached(capacity, &image, &staging) {
                Ok(attachment) => attachment,
                // Creation releases what it attached; anything it could not is released here,
                // before the image beneath it is removed.
                Err(primary) => {
                    return combine_cleanup(
                        "adopt image creation",
                        primary,
                        host.discard_pending(&image),
                    );
                }
            };
            // Creation already completed the checked staging mount in its attachment transaction.
            (workspace, requested.clone(), attachment, staging, false)
        }
    };
    let canonical_mount = mount_point(config, &workspace)?;
    let companion = companion_path(&image);
    let prepared = timed_apfs_step("staging", "copy", || {
        host.copy_tree(handoff.copy_source(source_checkout), &staging)
    })
    .and_then(|()| {
        timed_apfs_step("staging", "creds", || {
            host.mint_workspace_credentials(&workspace, &image, &staging, &companion)
        })
    })
    .and_then(|()| {
        timed_apfs_step("staging", "marker", || {
            host.write_marker(&staging, &workspace, None, &identity)
        })
    })
    .and_then(|()| {
        timed_apfs_step("staging", "validate", || {
            host.validate_marker(&staging, &MarkerExpectation::freshly_stamped(&workspace))
        })
    });
    let adopt = PreparedAdopt {
        stage: WorkspaceStage {
            workspace,
            mount_point: staging,
            companion,
            resuming,
        },
        attachment,
        image,
        canonical_mount,
        source_checkout: source_checkout.to_owned(),
        handoff: handoff.clone(),
    };
    match prepared {
        Ok(()) => Ok(adopt),
        Err(primary) => combine_cleanup(
            "adopt preparation",
            primary,
            abort_prepared_adopt(host, adopt),
        ),
    }
}

fn abort_prepared_adopt<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedAdopt<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    release_unpublished_adopt(
        host,
        prepared.attachment,
        &prepared.image,
        prepared.stage.resuming,
    )
}

/// Let go of an adoption that will not be published. A fresh image is reclaimed with its sidecar
/// and companion. A resumed one keeps them: it holds the copy the next attempt continues from.
fn release_unpublished_adopt<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    image: &Path,
    resuming: bool,
) -> Result<(), ApfsStorageError> {
    if resuming {
        host.detach(attachment, DetachIntent::Release)
    } else {
        detach_and_reclaim(host, attachment, image, "adopt detach")
    }
}

fn detach_and_reclaim<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    image: &Path,
    operation: &'static str,
) -> Result<(), ApfsStorageError> {
    let detached = host.detach(attachment, DetachIntent::Release);
    let reclaimed = host.reclaim_image(image);
    match detached {
        Ok(()) => reclaimed,
        Err(primary) => combine_cleanup(operation, primary, reclaimed),
    }
}

fn commit_prepared_adopt<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    prepared: PreparedAdopt<H::Attachment>,
) -> Result<Applied, ApfsStorageError> {
    if let Err(primary) = host
        .validate_staged_companion(&prepared.stage.companion)
        .and_then(|()| {
            host.validate_marker(
                &prepared.stage.mount_point,
                &MarkerExpectation::freshly_stamped(&prepared.stage.workspace),
            )
        })
    {
        return combine_cleanup(
            "adopt post-initialization validation",
            primary,
            abort_prepared_adopt(host, prepared),
        );
    }
    let PreparedAdopt {
        stage,
        attachment,
        image,
        canonical_mount,
        source_checkout,
        handoff,
    } = prepared;
    // The staging mount comes down through the kernel's unmount and the image stays attached, so
    // the checkout mount below needs no detach, second attach or second fsck.
    if let Err(primary) = timed_apfs_step("staging", "unmount", || {
        host.unmount_attachment(&attachment)
    }) {
        return combine_cleanup(
            "adopt staging unmount",
            primary,
            release_unpublished_adopt(host, attachment, &image, stage.resuming),
        );
    }
    // Publication is the sidecar's one atomic rewrite from PendingFence to Active; until it lands
    // the image is invisible to every verb and the user's tree is untouched.
    if let Err(primary) = timed_apfs_step("canonical", "activate", || {
        host.activate_pending(&image, &stage.workspace)
    }) {
        return combine_cleanup(
            "adopt activation",
            primary,
            release_unpublished_adopt(host, attachment, &image, stage.resuming),
        );
    }
    // Only now does the checkout path change hands: for a first adoption in one atomic swap (the
    // mountpoint *is* the checkout path and cannot exist until the swap creates it, so the swap
    // comes first and the mount follows); a plain re-adoption swaps the user's checkout aside to
    // its preserved sibling the same way; a replacement of an adopted main finds the checkout an
    // empty mountpoint, which must still be one. A failure leaves a published main that
    // `ApfsSubstrate::finish_adoption` completes.
    hand_over_checkout(host, &source_checkout, &handoff)?;
    if let Err(primary) = host
        .mount(&attachment, &canonical_mount, MountAccess::ReadWrite, false)
        .and_then(|()| {
            timed_apfs_step("canonical", "validate", || {
                host.validate_marker(
                    &canonical_mount,
                    &MarkerExpectation::owned(config, &stage.workspace),
                )
            })
        })
    {
        return detach_after_failure(host, attachment, primary, "canonical validation");
    }
    timed_apfs_step("canonical", "retain", || {
        host.retain_mounted(&stage.workspace, attachment)
    })?;
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: stage.workspace.revision(),
        workspace: stage.workspace,
    }))
}

/// Give main the checkout path as `handoff` says.
fn hand_over_checkout<H: ApfsExecutionHost>(
    host: &H,
    checkout: &Path,
    handoff: &AdoptHandoff,
) -> Result<(), ApfsStorageError> {
    match handoff {
        AdoptHandoff::Checkout {
            pre_cowshed_checkout,
        } => host
            .vacate_adopted_checkout(checkout, pre_cowshed_checkout)
            .map_err(PublicationError::into_source),
        AdoptHandoff::PlainReplacement { preserved, .. } => host
            .vacate_plain_checkout(checkout, preserved)
            .map_err(PublicationError::into_source),
        AdoptHandoff::Replacement { .. } => host.require_vacant_checkout(checkout),
    }
}

/// Finish an adoption whose image is published but whose checkout handoff or mount a crash or
/// failure interrupted: every step converges from what is already on disk and in the kernel.
fn finish_adoption<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
    handoff: &AdoptHandoff,
) -> Result<(), ApfsStorageError> {
    let checkout = mount_point(config, workspace)?;
    // A first adoption's swap and a plain re-adoption's vacate converge whether or not main is
    // mounted yet, a leftover staging name included. A replacement's handoff follows the mount
    // check below.
    match handoff {
        AdoptHandoff::Checkout { .. } | AdoptHandoff::PlainReplacement { .. } => {
            hand_over_checkout(host, &checkout, handoff)?;
        }
        AdoptHandoff::Replacement { .. } => {}
    }
    let derived = super::lifecycle::derive_workspaces(
        host.list(workspace.repo())?,
        host.mounts(workspace.repo())?,
    )?;
    // The workspace adoption names is read strictly: a mount at its checkout that does not prove
    // itself main's refuses, rather than reading as absent and being mounted over.
    let mut mounted = false;
    for candidate in derived {
        if candidate.workspace != *workspace {
            continue;
        }
        match candidate.mount_state {
            MountState::Mounted { .. } => mounted = true,
            MountState::Detached => {}
            MountState::Unproven(refusal) => return Err(refusal.into()),
        }
    }
    let expected = MarkerExpectation::owned(config, workspace);
    if mounted {
        return host.validate_marker(&checkout, &expected);
    }
    // A replacement mounts only on the empty mountpoint the replaced main left.
    if let AdoptHandoff::Replacement { .. } = handoff {
        hand_over_checkout(host, &checkout, handoff)?;
    }
    let image = canonical_image_path(config, workspace)?;
    let attachment = host
        .attach_and_mount_resumable(&image, &checkout, workspace)?
        .attachment;
    if let Err(primary) = host.validate_marker(&checkout, &expected) {
        return detach_after_failure(host, attachment, primary, "canonical validation");
    }
    host.retain_mounted(workspace, attachment).map(|_| ())
}

/// A create's or fork's image as its source's image lock leaves it: a `clonefile` of the source
/// behind its `PendingFence` sidecar (or a crash-left pending clone it resumes), not yet proven
/// forkable or attached. [`stage_captured_clone`] does that once the lock is let
/// go: none of it reads the source.
struct CapturedClone {
    clone: ResumableClone,
    source: WorkspaceName,
    source_image: PathBuf,
    fork: bool,
    resuming: bool,
}

fn capture_clone_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    execution: CloneExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<CapturedClone, ApfsStorageError> {
    let CloneExecution {
        source: source_name,
        destination: destination_name,
        fork,
        identity: requested_identity,
    } = execution;
    let source = active_expected(expected, source_name)?;
    let destination_topology = absent_expected(expected)?;
    let resumed = host.resumable_clone(
        config,
        &source,
        destination_name,
        destination_topology,
        requested_identity,
    )?;
    let (workspace, identity, canonical_image, resuming) = match resumed {
        Some(resumed) => (resumed.workspace, resumed.identity, resumed.image, true),
        None => {
            let workspace = LifecycleWorkspace::new(
                source.repo().clone(),
                destination_name.clone(),
                incarnations.mint()?,
                Revision::new(source.revision().get() + 1),
                Revision::new(destination_topology.get() + 1),
                WorkspaceRole::Workspace,
            )
            .map_err(|_| ApfsStorageError::InvalidPlan("invalid cloned workspace identity"))?;
            let canonical_image = canonical_image_path(config, &workspace)?;
            (
                workspace,
                requested_identity.clone(),
                canonical_image,
                false,
            )
        }
    };
    let source_image = canonical_image_path(config, &source)?;

    if !resuming {
        // The sidecar is the publication fence and is durable before the canonical payload name
        // appears. A crash in the gap leaves a sidecar-only record that recover_pending removes;
        // once the payload exists, PendingFence keeps it out of enumeration until init completes.
        timed_apfs_step("canonical", "metadata-pending", || {
            host.publish_metadata(
                &canonical_image,
                &workspace,
                workspace.revision(),
                MetadataPolicy::FreshPendingFence,
                Some(&identity),
                Some(&source_image),
            )
        })?;
        let source_mount = mount_point(config, &source)?;
        if let Err(primary) = host.clone_image(&source_image, Some(&source_mount), &canonical_image)
        {
            return combine_cleanup(
                "clone canonical payload",
                primary,
                host.reclaim_image(&canonical_image),
            );
        }
    }
    Ok(CapturedClone {
        clone: ResumableClone {
            workspace,
            identity,
            image: canonical_image,
        },
        source: source.name().clone(),
        source_image,
        fork,
        resuming,
    })
}

fn stage_captured_clone<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    captured: CapturedClone,
) -> Result<PreparedClone<H::Attachment>, ApfsStorageError> {
    let CapturedClone {
        clone,
        source,
        source_image,
        fork,
        resuming,
    } = captured;
    let canonical_image = &clone.image;
    // Only an image clone shares an extent map with its source; a host without physical volumes
    // has none. Measure the actual detached clone, not HEAD or the still-writing source. Stop at
    // 7141 runs: admission must never hide an unbounded recount in fork. Nothing writes the clone
    // here: its first mutation, whatever and wherever it is, makes APFS copy the whole extent list
    // it shares with its source, and that copy lands where the clone is first written, never under
    // its source's image lock.
    if H::HAS_PHYSICAL_VOLUMES && !resuming {
        let proven = timed_apfs_step("canonical", "extent-proof", || {
            host.require_forkable_image(canonical_image, None)
                .map_err(|error| {
                    ApfsStorageError::Host(format!(
                        "{error}; source image {}; next: cowshed defrag {source}",
                        source_image.display(),
                    ))
                })
        });
        if let Err(primary) = proven {
            return combine_cleanup(
                "clone canonical payload",
                primary,
                host.reclaim_image(canonical_image),
            );
        }
    }
    if resuming && H::HAS_PHYSICAL_VOLUMES {
        // A crash-left payload is not a retained proof of its current write generation.
        host.require_forkable_image(canonical_image, None)
            .map_err(|error| {
                ApfsStorageError::Host(format!(
                    "{error}; pending clone retained; next: cowshed defrag {}, then retry its original fork",
                    clone.workspace.name()
                ))
            })?;
    }
    mount_clone_stage(host, config, clone, fork.then_some(&source), resuming)
}

fn mount_clone_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    clone: ResumableClone,
    forked_from: Option<&WorkspaceName>,
    resuming: bool,
) -> Result<PreparedClone<H::Attachment>, ApfsStorageError> {
    let ResumableClone {
        workspace,
        identity,
        image: canonical_image,
    } = clone;
    let canonical_mount = mount_point(config, &workspace)?;
    let canonical_companion = companion_path(&canonical_image);
    // An attach refusal preserves the pending payload: it can be reporting a crash-left mount.
    let ResumedMount { attachment, found } =
        host.attach_and_mount_resumable(&canonical_image, &canonical_mount, &workspace)?;
    let prepared = timed_apfs_step("canonical", "creds", || {
        host.mint_workspace_credentials(
            &workspace,
            &canonical_image,
            &canonical_mount,
            &canonical_companion,
        )
    })
    .and_then(|()| {
        timed_apfs_step("canonical", "marker", || {
            host.write_marker(
                &canonical_mount,
                &workspace,
                forked_from.or(identity.forked_from.as_ref()),
                &identity,
            )
        })
    })
    .and_then(|()| {
        timed_apfs_step("canonical", "validate", || {
            host.validate_marker(
                &canonical_mount,
                &MarkerExpectation::freshly_stamped(&workspace),
            )
        })
    });
    if resuming {
        // Nothing ran on the stage yet, so a refused preparation leaves the pending image as it
        // was found; the next verb naming it mounts it again.
        if let Err(primary) = prepared {
            return combine_cleanup(
                "pending clone preparation",
                primary,
                restore_found_mount(host, attachment, found),
            );
        }
        return Ok(PreparedClone {
            stage: WorkspaceStage {
                workspace,
                mount_point: canonical_mount,
                companion: canonical_companion,
                resuming,
            },
            attachment,
            image: canonical_image,
            found,
        });
    }
    if let Err(primary) = prepared {
        return combine_cleanup(
            "clone preparation",
            primary,
            detach_and_reclaim(host, attachment, &canonical_image, "clone canonical detach"),
        );
    }
    Ok(PreparedClone {
        stage: WorkspaceStage {
            workspace,
            mount_point: canonical_mount,
            companion: canonical_companion,
            resuming,
        },
        attachment,
        image: canonical_image,
        found,
    })
}

fn preserve_prepared_clone<H: ApfsExecutionHost>(
    _host: &H,
    _prepared: PreparedClone<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    // Cancellation can occur after the initializer changed Git state outside this image. Dropping
    // the userspace attachment handle leaves the kernel mount and PendingFence intact for the
    // durable lifecycle intent to resume; detaching and reclaiming here would orphan those effects.
    Ok(())
}

/// Leave a pending image as [`ApfsExecutionHost::attach_and_mount_resumable`] found it: a mount
/// it reused stays, a mount it made on a surviving attachment is undone, and an image it
/// attached is detached.
fn restore_found_mount<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    found: FoundMount,
) -> Result<(), ApfsStorageError> {
    match found {
        FoundMount::Mounted => Ok(()),
        FoundMount::Attached => host.unmount_attachment(&attachment),
        FoundMount::Detached => host.detach(attachment, DetachIntent::Release),
    }
}

/// A pending retirement that ends without retiring — refused or cancelled — changed nothing the
/// image must stay mounted for, so it puts the mount back as it found it.
fn restore_prepared_mount<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedClone<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    restore_found_mount(host, prepared.attachment, prepared.found)
}

fn commit_prepared_clone<H: ApfsExecutionHost>(
    host: &H,
    _config: &ApfsSubstrateConfig,
    prepared: PreparedClone<H::Attachment>,
) -> Result<Applied, ApfsStorageError> {
    let PreparedClone {
        stage,
        attachment,
        image,
        found: _,
    } = prepared;
    // The callback has already run and may have changed external Git state. On failure, `?`
    // leaves the exact PendingFence clone mounted so recovery can rerun initialization and
    // validation.
    timed_apfs_step("canonical", "validate-companion", || {
        host.validate_staged_companion(&stage.companion)
    })
    .and_then(|()| {
        timed_apfs_step("canonical", "validate", || {
            host.validate_marker(
                &stage.mount_point,
                &MarkerExpectation::freshly_stamped(&stage.workspace),
            )
        })
    })?;
    if H::HAS_PHYSICAL_VOLUMES {
        timed_apfs_step("canonical", "publication-extent-proof", || {
            host.require_forkable_image(&image, Some(&stage.mount_point)).map_err(|error| {
                ApfsStorageError::Host(format!(
                    "{error}; pending clone retained; next: cowshed defrag {}, then retry its original fork",
                    stage.workspace.name()
                ))
            })
        })?;
    }
    timed_apfs_step("canonical", "activate", || {
        host.activate_pending(&image, &stage.workspace)
    })?;
    timed_apfs_step("canonical", "retain", || {
        host.retain_mounted(&stage.workspace, attachment)
    })
    .map(|_| ())?;
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: stage.workspace.revision(),
        workspace: stage.workspace,
    }))
}

fn apply_retire<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    workspace_name: &WorkspaceName,
) -> Result<Applied, ApfsStorageError> {
    let current = active_expected(expected, workspace_name)?;
    let canonical = canonical_image_path(config, &current)?;
    let trash = retired_image_path(config, &current)?;
    host.detach_mounted(&current, DetachIntent::Release)?;
    host.retire_image(&canonical, &trash)?;
    Ok(Applied::Retired(RetiredRef::new(
        current.clone(),
        Revision::new(current.revision().get() + 1),
    )))
}

/// Settle `workspace`'s mount under its image lock, which the caller holds: heal a mount the
/// kernel no longer honours, mount a detached image, validate the marker, and settle the
/// checkout's build link. Answers the mount point.
fn settle_mount<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
    intent: MountIntent,
) -> Result<PathBuf, ApfsStorageError> {
    let mount_point = mount_point(config, workspace)?;
    host.heal_mount(workspace, &mount_point)?;
    let state = host.workspace_mount_state(workspace)?;
    if matches!(state, MountState::Mounted { .. }) {
        host.validate_marker(&mount_point, &MarkerExpectation::owned(config, workspace))?;
        host.ensure_linked_build_volume(workspace, &mount_point)?;
        return Ok(mount_point);
    }
    let canonical = canonical_image_path(config, workspace)?;
    let attachment = host.attach_mounted(
        &canonical,
        &mount_point,
        MountAccess::ReadWrite,
        intent.browse,
    )?;
    if let Err(primary) =
        host.validate_marker(&mount_point, &MarkerExpectation::owned(config, workspace))
    {
        return detach_after_failure(host, attachment, primary, "mount workspace");
    }
    host.retain_mounted(workspace, attachment)?;
    host.ensure_linked_build_volume(workspace, &mount_point)?;
    Ok(mount_point)
}

fn detach_after_failure<H: ApfsExecutionHost, T>(
    host: &H,
    attachment: H::Attachment,
    primary: ApfsStorageError,
    operation: &'static str,
) -> Result<T, ApfsStorageError> {
    match host.detach(attachment, DetachIntent::Release) {
        Ok(()) => Err(primary),
        Err(cleanup) => Err(ApfsStorageError::Cleanup {
            operation,
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

pub(super) fn combine_cleanup<T>(
    operation: &'static str,
    primary: ApfsStorageError,
    cleanup: Result<(), ApfsStorageError>,
) -> Result<T, ApfsStorageError> {
    match cleanup {
        Ok(()) => Err(primary),
        Err(cleanup) => Err(ApfsStorageError::Cleanup {
            operation,
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn expected_repo(expected: &[LifecycleFact]) -> Result<&RepoId, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            LifecycleFact::Exists {
                repo,
                retired: false,
                ..
            } => Some(repo),
            _ => None,
        })
        .ok_or(ApfsStorageError::InvalidPlan(
            "active workspace expectation is missing",
        ))
}

fn active_expected(
    expected: &[LifecycleFact],
    name: &WorkspaceName,
) -> Result<LifecycleWorkspace, ApfsStorageError> {
    let Some(LifecycleFact::Exists {
        repo,
        name: expected_name,
        incarnation,
        revision,
        topology_revision,
        retired: false,
    }) = expected.iter().find(
        |fact| matches!(fact, LifecycleFact::Exists { name: candidate, .. } if candidate == name),
    )
    else {
        return Err(ApfsStorageError::InvalidPlan(
            "active workspace expectation is missing",
        ));
    };
    let role = WorkspaceRole::for_name(expected_name);
    LifecycleWorkspace::new(
        repo.clone(),
        expected_name.clone(),
        incarnation.clone(),
        *revision,
        *topology_revision,
        role,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid active workspace identity"))
}

/// The suffix of the CA private key that rides beside a workspace image.
pub(crate) const COMPANION_SUFFIX: &str = ".ca.key";

pub(crate) fn companion_path(image: &Path) -> PathBuf {
    crate::metadata::append_suffix(image, COMPANION_SUFFIX)
}

fn absent_expected(expected: &[LifecycleFact]) -> Result<Revision, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            LifecycleFact::Absent {
                topology_revision, ..
            } => Some(*topology_revision),
            _ => None,
        })
        .ok_or(ApfsStorageError::InvalidPlan(
            "absent destination expectation is missing",
        ))
}

fn main_name() -> WorkspaceName {
    WorkspaceName::main()
}

pub(super) fn layout(
    config: &ApfsSubstrateConfig,
    repo: &RepoId,
) -> Result<StorageLayout, ApfsStorageError> {
    StorageLayout::new(&config.store_root, repo).map_err(Into::into)
}

fn canonical_image_path(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    let layout = layout(config, workspace.repo())?;
    Ok(layout.canonical_image(workspace.name())?.image().to_owned())
}

pub(super) fn staging_mount(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    Ok(layout(config, workspace.repo())?
        .project()
        .mount_root
        .join(STAGING_NAMESPACE)
        .join(format!(
            "{}-{}",
            workspace.name().as_str(),
            workspace.incarnation().as_str()
        )))
}

/// `<sessions>/<TRASH_NAMESPACE>/<name>-<incarnation>.dmg`. `sessions` is
/// [`crate::repository::ProjectPaths::sessions`]; this helper does not name that directory.
///
/// The `-` between name and incarnation is the separator [`split_retired_stem`] reverses; the two
/// live side by side so the pair cannot drift. Incarnations are fixed-width lowercase hex, which
/// is what keeps `rsplit_once` unambiguous even though workspace names contain hyphens.
pub(crate) fn retired_image_below(
    sessions: &Path,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
) -> PathBuf {
    sessions
        .join(TRASH_NAMESPACE)
        .join(retired_file_name(workspace, incarnation))
}

/// `<sessions>/.deleting/<name>-<incarnation>.dmg`: where a workspace image's release is committed
/// and waits for the gateway's cleanup (02_workspaces.md, `rm` step 5;
/// [`release::DELETING_DIRECTORY`]). Named like its trash entry, so an incarnation names one.
pub(super) fn deleting_image_below(
    sessions: &Path,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
) -> PathBuf {
    sessions
        .join(release::DELETING_DIRECTORY)
        .join(retired_file_name(workspace, incarnation))
}

fn retired_file_name(workspace: &WorkspaceName, incarnation: &WorkspaceIncarnation) -> String {
    format!(
        "{}-{}.{IMAGE_EXTENSION}",
        workspace.as_str(),
        incarnation.as_str()
    )
}

/// Whether `name` is a released workspace image's name, `<name>-<incarnation>.dmg`.
pub(crate) fn is_deleting_image_name(name: &str) -> bool {
    name.strip_suffix(&format!(".{IMAGE_EXTENSION}"))
        .and_then(split_retired_stem)
        .is_some()
}

/// The released workspace images of every project in the store at `store`, as the gateway's
/// cleanup lists them at its start (05_gateway.md, "Deleting released images"): each project's
/// `sessions/.deleting`.
pub fn released_sessions(store: &Path) -> release::Listing {
    release::released_in_store(
        store,
        crate::repository::SESSIONS_DIRECTORY,
        release::DELETING_DIRECTORY,
        is_deleting_image_name,
    )
}

/// Splits a `<name>-<incarnation>` trash stem: the exact inverse of the stem written by
/// [`retired_image_below`], kept adjacent so neither side can change the separator alone.
pub(crate) fn split_retired_stem(stem: &str) -> Option<(WorkspaceName, WorkspaceIncarnation)> {
    let (name, incarnation) = stem.rsplit_once('-')?;
    Some((
        WorkspaceName::new(name).ok()?,
        WorkspaceIncarnation::new(incarnation).ok()?,
    ))
}

#[cfg(test)]
std::thread_local! {
    static RETIRED_TRASH_OBSERVATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(in crate::storage) fn record_retired_trash_observation() {
    RETIRED_TRASH_OBSERVATIONS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
pub(in crate::storage) fn retired_trash_observations() -> usize {
    RETIRED_TRASH_OBSERVATIONS.with(std::cell::Cell::get)
}

/// The remaining trash incarnations from one observation. Preview and its locked revalidation
/// share this owner; execution removes only an entry whose exact payload and record are gone.
#[derive(Default)]
pub(in crate::storage) struct RetiredNameOwners {
    remaining:
        std::collections::BTreeMap<WorkspaceName, std::collections::BTreeSet<WorkspaceIncarnation>>,
}

impl RetiredNameOwners {
    pub(in crate::storage) fn from_images(images: &[PathBuf]) -> Self {
        let mut owners = Self::default();
        for image in images {
            let Some((name, incarnation)) = image
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(split_retired_stem)
            else {
                continue;
            };
            owners
                .remaining
                .entry(name)
                .or_default()
                .insert(incarnation);
        }
        owners
    }

    pub(in crate::storage) fn get(&self, name: &WorkspaceName) -> Option<&WorkspaceIncarnation> {
        self.remaining
            .get(name)
            .and_then(|remaining| remaining.last())
    }

    fn remove(&mut self, name: &WorkspaceName, incarnation: &WorkspaceIncarnation) {
        if let Some(remaining) = self.remaining.get_mut(name) {
            remaining.remove(incarnation);
            if remaining.is_empty() {
                self.remaining.remove(name);
            }
        }
    }

    /// A failed cleanup can already have unlinked its last authority before a directory sync
    /// fails. Inspect only that entry, not the whole trash, and preserve both failures if this
    /// observation also fails. A surviving payload or grants record still owns its name.
    pub(in crate::storage) fn after_reclaim<T>(
        &mut self,
        retired: &RetiredRef,
        image: &Path,
        payload: impl Fn(std::fs::FileType) -> bool,
        result: Result<T, ApfsStorageError>,
    ) -> Result<T, ApfsStorageError> {
        fn present(
            path: &Path,
            keep: impl Fn(std::fs::FileType) -> bool,
        ) -> Result<bool, ApfsStorageError> {
            match std::fs::symlink_metadata(path) {
                Ok(metadata) => Ok(keep(metadata.file_type())),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(source) => Err(ApfsStorageError::Io {
                    operation: "inspect remaining retired authority",
                    path: path.to_owned(),
                    source,
                }),
            }
        }
        let observation = present(image, payload).and_then(|remains| {
            if remains {
                Ok(true)
            } else {
                present(&crate::metadata::sidecar_path(image), |kind| kind.is_file())
            }
        });
        let update = observation.map(|remains| {
            if !remains {
                self.remove(
                    retired.workspace().name(),
                    retired.workspace().incarnation(),
                );
            }
        });
        match result {
            Ok(value) => update.map(|()| value),
            Err(primary) => combine_cleanup(
                "observe a retired entry after its cleanup failed",
                primary,
                update,
            ),
        }
    }
}

fn retired_image_path(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    let sessions = layout(config, workspace.repo())?.project().sessions.clone();
    Ok(retired_image_below(
        &sessions,
        workspace.name(),
        workspace.incarnation(),
    ))
}

/// Main mounts at the user's checkout path; every other workspace under the fixed per-user
/// `<mount-root>/<owner>/<repo>/`.
fn mount_point(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    main_aware_mount_point(config, workspace.repo(), workspace.name())
}

pub(super) fn main_aware_mount_point(
    config: &ApfsSubstrateConfig,
    repo: &RepoId,
    workspace: &WorkspaceName,
) -> Result<PathBuf, ApfsStorageError> {
    layout(config, repo)?
        .main_aware_workspace_mount(&config.checkout_path, workspace)
        .map_err(Into::into)
}

/// Internal join key for a workspace's volume, derived from metadata and never read back off a
/// volume. Enumeration is keyed by image location and mount identity by the in-image marker, so
/// this key exists only to pair a `StorageFact` with a `KernelMountFact` inside one project.
pub fn volume_key(repo: &RepoId, workspace: &WorkspaceName) -> String {
    format!(
        "cowshed.{}--{}.{}",
        repo.owner(),
        repo.repo(),
        workspace.as_str()
    )
}

pub fn volume_label(namespace: HostNamespace, repo: &RepoId, workspace: &WorkspaceName) -> String {
    namespace.volume_label(format_args!(
        "{} · {} — {}",
        repo.owner(),
        repo.repo(),
        workspace.as_str()
    ))
}

pub fn build_volume_label(
    namespace: HostNamespace,
    repo: &RepoId,
    checkout: &WorkspaceName,
) -> String {
    namespace.volume_label(format_args!(
        "{} · {} — build {}",
        repo.owner(),
        repo.repo(),
        checkout.as_str()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::GrantSet;

    #[test]
    fn retired_name_owners_follow_remaining_incarnations_in_every_cleanup_order() {
        let name = WorkspaceName::session("topic").unwrap();
        let other = WorkspaceName::session("retained").unwrap();
        let incarnation = |value: u128| WorkspaceIncarnation::new(format!("{value:032x}")).unwrap();
        for input in [[1, 2, 3], [3, 1, 2], [2, 3, 1]] {
            for (cleanup, expected) in [
                ([1, 2, 3], [Some(3), Some(3), None]),
                ([3, 2, 1], [Some(2), Some(1), None]),
                ([2, 3, 1], [Some(3), Some(1), None]),
            ] {
                let mut images: Vec<_> = input
                    .into_iter()
                    .map(|value| {
                        retired_image_below(Path::new("sessions"), &name, &incarnation(value))
                    })
                    .collect();
                images.push(images[0].clone());
                images.push(PathBuf::from("sessions/.trash/not-an-incarnation.dmg"));
                images.push(retired_image_below(
                    Path::new("sessions"),
                    &other,
                    &incarnation(9),
                ));
                let mut owners = RetiredNameOwners::from_images(&images);
                assert_eq!(owners.get(&name), Some(&incarnation(3)));
                owners.remove(&name, &incarnation(7));
                assert_eq!(
                    owners.get(&name),
                    Some(&incarnation(3)),
                    "an absent entry changes nothing"
                );
                for (value, expected) in cleanup.into_iter().zip(expected) {
                    owners.remove(&name, &incarnation(value));
                    assert_eq!(
                        owners.get(&name).cloned(),
                        expected.map(incarnation),
                        "input={input:?} cleanup={cleanup:?} removed={value}"
                    );
                    assert_eq!(
                        owners.get(&other),
                        Some(&incarnation(9)),
                        "retained evidence keeps its own name"
                    );
                }
            }
        }
    }

    /// A busy build volume names what holds it: the job when its hold file is held, the process
    /// with the volume open otherwise (live: a host `cat` with its cwd on the mount, pid 64266,
    /// once read "held by a job admitted on it"), and says so when no holder is visible.
    #[test]
    fn a_busy_build_volume_names_its_actual_holder() {
        let holder = |pid, command: &str| cowshed_os::process::Holder {
            pid,
            command: command.to_owned(),
        };
        let held = |holds: Vec<cowshed_os::process::Holder>, open| {
            ApfsStorageError::BuildVolumeHeld {
                image: PathBuf::from("/store/acme/widget/build/v.dmg"),
                hold: PathBuf::from("/store/acme/widget/build/v.dmg.hold"),
                holds,
                open,
            }
            .to_string()
        };
        let open_only = held(Vec::new(), vec![holder(64266, "cat /tmp/fifo")]);
        assert_eq!(
            open_only,
            "build volume /store/acme/widget/build/v.dmg is in use: it is open in pid 64266 (cat /tmp/fifo)"
        );
        assert!(!open_only.contains("job"), "{open_only}");
        let by_job = held(vec![holder(7, "cowshed supervisor")], Vec::new());
        assert!(
            by_job.starts_with("build volume /store/acme/widget/build/v.dmg is held by a job admitted on it: /store/acme/widget/build/v.dmg.hold is held by pid 7 (cowshed supervisor)"),
            "{by_job}"
        );
        let unseen = held(Vec::new(), Vec::new());
        assert!(unseen.contains("no process of this user holds"), "{unseen}");
        #[cfg(target_os = "macos")]
        {
            let hint = build_volume_held_hint(&[], &[holder(64266, "cat /tmp/fifo")]);
            assert_eq!(
                hint,
                "end pid 64266 (cat /tmp/fifo) or wait for it to exit, then retry"
            );
            assert!(!hint.contains("repair"), "{hint}");
        }
    }

    fn identity() -> OperationIdentity {
        OperationIdentity {
            project_root: PathBuf::from("/project"),
            base_commit: "0123456789abcdef".to_owned(),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            branch: Some("main".to_owned()),
            forked_from: None,
            created_trace: "lock-table".to_owned(),
            git_worktree: false,
            grants: GrantSet::default(),
        }
    }

    #[test]
    fn every_mutating_operation_maps_to_its_exact_canonical_lock_set() {
        use cowshed_os::namespace::{HostNamespace, TestNamespace};
        let config = ApfsSubstrateConfig::new(
            HostNamespace::Test(TestNamespace::mint().expect("a test namespace")),
            "/tmp/cowshed-lock-table/store",
            "/tmp/cowshed-lock-table/main",
        );
        let repo = RepoId::parse("acme/widget").expect("repo");
        let main = main_name();
        let source = WorkspaceName::session("source").expect("source");
        let destination = WorkspaceName::session("destination").expect("destination");
        let expected = vec![LifecycleFact::Exists {
            repo: repo.clone(),
            name: source.clone(),
            incarnation: WorkspaceIncarnation::new("00000000000000000000000000000001")
                .expect("incarnation"),
            revision: Revision::new(1),
            topology_revision: Revision::new(1),
            retired: false,
        }];
        let main_lock = workspace_lock_path(&config, &repo, &main).expect("main");
        assert_eq!(
            main_lock,
            Path::new("/tmp/cowshed-lock-table/store/acme/widget/main.dmg.lock")
        );
        let source_lock = workspace_lock_path(&config, &repo, &source).expect("source");
        let destination_lock =
            workspace_lock_path(&config, &repo, &destination).expect("destination");
        let mut clone_locks = vec![source_lock.clone(), destination_lock.clone()];
        clone_locks.sort();
        let cases = [
            (
                Operation::Adopt {
                    repo: repo.clone(),
                    capacity: DEFAULT_IMAGE_CAPACITY,
                    source_checkout: PathBuf::from("/project"),
                    handoff: AdoptHandoff::Checkout {
                        pre_cowshed_checkout: PathBuf::from("/project.pre-cowshed"),
                    },
                    identity: identity(),
                },
                vec![main_lock],
            ),
            (
                Operation::Create {
                    source: source.clone(),
                    destination: destination.clone(),
                    identity: identity(),
                },
                clone_locks.clone(),
            ),
            (
                Operation::Fork {
                    source: source.clone(),
                    destination: destination.clone(),
                    identity: identity(),
                },
                clone_locks,
            ),
            (Operation::Retire { workspace: source }, vec![source_lock]),
        ];

        for (operation, mut wanted) in cases {
            wanted.sort();
            assert_eq!(
                operation_lock_paths(&config, &expected, &operation).expect("lock mapping"),
                wanted,
                "{operation:?}"
            );
        }
    }

    #[tokio::test]
    async fn image_lock_serializes_metadata_read_modify_write() {
        use cowshed_os::namespace::{HostNamespace, TestNamespace};
        let root = std::env::temp_dir().join(format!(
            "cowshed-grant-lock-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let store = root.join("store");
        std::fs::create_dir_all(&store).expect("store");
        let counter = root.join("grant-revision");
        std::fs::write(&counter, "0").expect("initial revision");
        let lock = store.join("sessions/raven.dmg.lock");
        let config = ApfsSubstrateConfig::new(
            HostNamespace::Test(TestNamespace::mint().expect("a test namespace")),
            &store,
            root.join("checkout"),
        );
        let host = native::MacOsApfsExecutionHost::new(
            cowshed_apfs::SystemCommandRunner::new(config.namespace),
            config.clone(),
        )
        .expect("native host");
        let substrate = ApfsSubstrate::new(config, host);

        let (first_entered_tx, first_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_first_tx, release_first_rx) = std::sync::mpsc::sync_channel(1);
        let first_substrate = substrate.clone();
        let first_lock = lock.clone();
        let first_counter = counter.clone();
        let first = tokio::spawn(async move {
            first_substrate
                .dispatch_with_image_lock(
                    first_lock,
                    LockHolding::of(LockOperation::Grant, &WorkspaceName::new("raven").unwrap()),
                    move || {
                        let observed = std::fs::read_to_string(&first_counter)?
                            .parse::<u64>()
                            .expect("numeric revision");
                        first_entered_tx.send(()).expect("announce first lock");
                        release_first_rx.recv().expect("release first mutation");
                        std::fs::write(first_counter, (observed + 1).to_string())
                    },
                )
                .await
        });
        tokio::task::yield_now().await;
        first_entered_rx
            .recv()
            .expect("first mutation entered its lock");

        let (second_attempt_tx, second_attempt_rx) = std::sync::mpsc::sync_channel(1);
        let (second_entered_tx, second_entered_rx) = std::sync::mpsc::sync_channel(1);
        let second_substrate = substrate.clone();
        let second_counter = counter.clone();
        let second = tokio::spawn(async move {
            second_attempt_tx.send(()).expect("announce second attempt");
            second_substrate
                .dispatch_with_image_lock(
                    lock,
                    LockHolding::of(LockOperation::Grant, &WorkspaceName::new("raven").unwrap()),
                    move || {
                        second_entered_tx.send(()).expect("announce second lock");
                        let observed = std::fs::read_to_string(&second_counter)?
                            .parse::<u64>()
                            .expect("numeric revision");
                        std::fs::write(second_counter, (observed + 1).to_string())
                    },
                )
                .await
        });
        tokio::task::yield_now().await;
        second_attempt_rx
            .recv()
            .expect("second mutation attempted the lock");
        let entered_before_release =
            second_entered_rx.recv_timeout(std::time::Duration::from_millis(100));

        release_first_tx.send(()).expect("release first mutation");
        first
            .await
            .expect("first task")
            .expect("first lock")
            .expect("first mutation");
        if entered_before_release.is_err() {
            second_entered_rx
                .recv()
                .expect("second mutation entered after release");
        }
        second
            .await
            .expect("second task")
            .expect("second lock")
            .expect("second mutation");

        assert!(
            matches!(
                entered_before_release,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "the second mutation entered while the first held the image lock"
        );
        assert_eq!(
            std::fs::read_to_string(&counter).expect("final revision"),
            "2",
            "both serialized read-modify-write operations must survive"
        );
        drop(substrate);
        std::fs::remove_dir_all(root).expect("fixture");
    }
}
