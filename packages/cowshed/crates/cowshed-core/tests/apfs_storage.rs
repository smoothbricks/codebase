use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cowshed_apfs::image::{ImageCapacity, is_image_path};
use cowshed_apfs::store_lock::LockMode;
use cowshed_apfs::{DetachIntent, MountAccess};
use cowshed_core::metadata::{
    GrantSet, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, PortBlock, WorkspaceIncarnation,
    WorkspaceInfoSnapshot, WorkspaceName, WorkspaceRole,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::apfs::{
    AdoptExecutionError, Alongside, ApfsBlockingLane, ApfsExecutionHost, ApfsStorageError,
    ApfsSubstrate, ApfsSubstrateConfig, Beside, DEFAULT_IMAGE_CAPACITY, FoundMount,
    IncarnationSource, MarkerExpectation, MetadataPolicy, PendingAdoption, PublicationError,
    ResumableClone, ResumedMount, RetireExecutionError, StagedExecutionError, volume_key,
};
use cowshed_core::storage::lifecycle::{
    AdoptHandoff, AdoptRequest, DefragmentOutcome, Destination, ExtentCount, KernelMountFact,
    LifecycleFact, LifecyclePlanner, LifecycleWorkspace, MountIntent, MountJoin, MountState,
    OperationIdentity, PlanError, ResizeOutcome, RetiredRef, Revision, StorageFact,
    StorageGcOptions, StorageGcPlan, StorageGcReport, Substrate, SubstrateStats,
};
use cowshed_os::namespace::{HostNamespace, TestNamespace};
use proptest::prelude::*;

#[derive(Clone, Debug)]
struct FakeAttachment;

#[derive(Default)]
struct FakeState {
    events: Vec<String>,
    published: BTreeMap<(RepoId, WorkspaceName), StorageFact>,
    mounted: BTreeMap<(RepoId, WorkspaceName), KernelMountFact>,
    staged: BTreeMap<PathBuf, StorageFact>,
    pending: BTreeMap<PathBuf, StorageFact>,
    next_mount_id: u64,
    paths: Vec<PathBuf>,
    mount_paths: Vec<PathBuf>,
    resumable_adopt: Option<PendingAdoption>,
    /// The pending main's payload never became a verified volume.
    unformatted_adopt: bool,
    resumable_clone: Option<ResumableClone>,
}

#[derive(Clone)]
struct FakeHost {
    state: Arc<Mutex<FakeState>>,
    marker_validations_before_failure: Arc<AtomicUsize>,
    fail_reclaim_once: Arc<AtomicBool>,
    fail_credentials_once: Arc<AtomicBool>,
    fail_extent_proof_once: Arc<AtomicBool>,
    mounted_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
    census: cowshed_core::census::CensusScope,
}

impl Default for FakeHost {
    fn default() -> Self {
        Self {
            state: Arc::default(),
            marker_validations_before_failure: Arc::new(AtomicUsize::new(usize::MAX)),
            fail_credentials_once: Arc::default(),
            fail_reclaim_once: Arc::default(),
            fail_extent_proof_once: Arc::default(),
            mounted_paths: Arc::default(),
            census: cowshed_core::census::CensusScope::new(
                HostNamespace::Test(TestNamespace::mint().expect("a test namespace")),
                Arc::from(Path::new("/fake/store")),
            ),
        }
    }
}

impl FakeHost {
    fn events(&self) -> Vec<String> {
        self.state.lock().expect("fake state").events.clone()
    }

    fn clear_events(&self) {
        self.state.lock().expect("fake state").events.clear();
    }

    fn record(&self, event: impl Into<String>) {
        self.state
            .lock()
            .expect("fake state")
            .events
            .push(event.into());
    }

    fn paths(&self) -> Vec<PathBuf> {
        self.state.lock().expect("fake state").paths.clone()
    }

    fn mount_paths(&self) -> Vec<PathBuf> {
        self.state.lock().expect("fake state").mount_paths.clone()
    }

    fn record_path(&self, path: &Path) {
        self.state
            .lock()
            .expect("fake state")
            .paths
            .push(path.to_owned());
    }

    fn seed(&self, workspace: &LifecycleWorkspace) {
        let key = (workspace.repo().clone(), workspace.name().clone());
        let mut state = self.state.lock().expect("fake state");
        state.published.insert(
            key,
            StorageFact {
                workspace: workspace.clone(),
                volume_key: volume_key(workspace.repo(), workspace.name()),
            },
        );
    }
    fn resume_adopt_from(&self, adoption: PendingAdoption) {
        let workspace = LifecycleWorkspace::new(
            repo(),
            WorkspaceName::new("main").expect("main"),
            adoption.incarnation.clone(),
            Revision::new(1),
            Revision::new(1),
            WorkspaceRole::Main,
        )
        .expect("pending main");
        let mut state = self.state.lock().expect("fake state");
        state.pending.insert(
            adoption.image.clone(),
            StorageFact {
                volume_key: volume_key(workspace.repo(), workspace.name()),
                workspace,
            },
        );
        state.resumable_adopt = Some(adoption);
    }

    fn leave_pending_adopt_unformatted(&self) {
        self.state.lock().expect("fake state").unformatted_adopt = true;
    }

    /// The canonical images still waiting to be published, as the fake holds them.
    fn pending_clone_count(&self) -> usize {
        self.state.lock().expect("fake state").pending.len()
    }

    fn pending_adopt(&self) -> Option<PendingAdoption> {
        self.state
            .lock()
            .expect("fake state")
            .resumable_adopt
            .clone()
    }

    fn resume_clone_from(
        &self,
        workspace: LifecycleWorkspace,
        identity: OperationIdentity,
        image: impl Into<PathBuf>,
    ) {
        let image = image.into();
        let fact = StorageFact {
            workspace: workspace.clone(),
            volume_key: volume_key(workspace.repo(), workspace.name()),
        };
        let mut state = self.state.lock().expect("fake state");
        state.pending.insert(image.clone(), fact);
        state.resumable_clone = Some(ResumableClone {
            workspace,
            identity,
            image,
        });
    }
    fn fail_next_marker(&self) {
        self.marker_validations_before_failure
            .store(0, Ordering::SeqCst);
    }

    fn fail_next_credentials(&self) {
        self.fail_credentials_once.store(true, Ordering::SeqCst);
    }
    fn fail_next_reclaim(&self) {
        self.fail_reclaim_once.store(true, Ordering::SeqCst);
    }

    fn mounted_paths_now(&self) -> BTreeSet<PathBuf> {
        self.mounted_paths.lock().expect("mounted paths").clone()
    }

    fn staged_paths_now(&self) -> BTreeSet<PathBuf> {
        self.state
            .lock()
            .expect("fake state")
            .staged
            .keys()
            .cloned()
            .collect()
    }
}

impl ApfsExecutionHost for FakeHost {
    type LockGuard = ();
    fn census(&self) -> &cowshed_core::census::CensusScope {
        &self.census
    }
    fn lock_images(
        &self,
        paths: &[PathBuf],
        _: LockMode,
        _: &cowshed_core::api::held_lock::LockHolding,
    ) -> Result<Option<Self::LockGuard>, ApfsStorageError> {
        self.record(format!("lock:{}", paths.len()));
        Ok(Some(()))
    }

    type Attachment = FakeAttachment;

    fn observe(&self, expected: &[LifecycleFact]) -> Result<Vec<LifecycleFact>, ApfsStorageError> {
        self.record("observe");
        Ok(expected
            .iter()
            .map(|fact| match fact {
                LifecycleFact::Exists {
                    repo,
                    name,
                    incarnation,
                    revision,
                    topology_revision,
                    retired,
                } => LifecycleFact::Exists {
                    repo: repo.clone(),
                    name: name.clone(),
                    incarnation: incarnation.clone(),
                    revision: *revision,
                    topology_revision: *topology_revision,
                    retired: *retired,
                },
                LifecycleFact::Absent {
                    repo,
                    name,
                    topology_revision,
                } => LifecycleFact::Absent {
                    repo: repo.clone(),
                    name: name.clone(),
                    topology_revision: *topology_revision,
                },
            })
            .collect())
    }

    fn create_attached(
        &self,
        capacity: ImageCapacity,
        image: &Path,
        mount_point: &Path,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        if !is_image_path(image) {
            return Err(ApfsStorageError::Host(
                "a mint lands on an image path".to_owned(),
            ));
        }
        self.record_path(image);
        self.record(format!("create-mounted+checked:{capacity}"));
        self.mount(&FakeAttachment, mount_point, MountAccess::ReadWrite, false)?;
        Ok(FakeAttachment)
    }

    fn clone_image(
        &self,
        source: &Path,
        _: Option<&Path>,
        destination: &Path,
    ) -> Result<(), ApfsStorageError> {
        if !is_image_path(source) || !is_image_path(destination) {
            return Err(ApfsStorageError::Host(
                "clone between paths that are not images".to_owned(),
            ));
        }
        self.record("clone");
        Ok(())
    }
    fn require_forkable_image(
        &self,
        image: &Path,
        _mount: Option<&Path>,
    ) -> Result<(), ApfsStorageError> {
        self.record_path(image);
        self.record("extent-proof");
        if self.fail_extent_proof_once.swap(false, Ordering::SeqCst) {
            return Err(ApfsStorageError::Host(
                "physical generation is over budget or unknown".into(),
            ));
        }
        Ok(())
    }
    fn pending_adoption(
        &self,
        _: &ApfsSubstrateConfig,
        _: &RepoId,
    ) -> Result<Option<PendingAdoption>, ApfsStorageError> {
        Ok(self
            .state
            .lock()
            .expect("fake state")
            .resumable_adopt
            .clone())
    }

    fn resume_pending_adopt(
        &self,
        image: &Path,
        mount_point: &Path,
        _: &LifecycleWorkspace,
    ) -> Result<Option<Self::Attachment>, ApfsStorageError> {
        self.record_path(image);
        if self.state.lock().expect("fake state").unformatted_adopt {
            self.record("resume-unformatted");
            return Ok(None);
        }
        self.record("resume-attach-or-reuse");
        self.mount(&FakeAttachment, mount_point, MountAccess::ReadWrite, false)?;
        Ok(Some(FakeAttachment))
    }

    fn discard_pending(&self, image: &Path) -> Result<(), ApfsStorageError> {
        self.record("discard-pending");
        let mut state = self.state.lock().expect("fake state");
        state.pending.remove(image);
        state.unformatted_adopt = false;
        if state
            .resumable_adopt
            .as_ref()
            .is_some_and(|pending| pending.image == image)
        {
            state.resumable_adopt = None;
        }
        Ok(())
    }

    fn unmount_attachment(&self, _: &Self::Attachment) -> Result<(), ApfsStorageError> {
        self.record("unmount");
        self.mounted_paths.lock().expect("mounted paths").clear();
        Ok(())
    }

    fn validate_pending_clone(
        &self,
        image: &Path,
        expected: &cowshed_core::metadata::DetachedWorkspaceMetadata,
    ) -> Result<(), ApfsStorageError> {
        let state = self.state.lock().expect("fake state");
        let pending = state
            .resumable_clone
            .as_ref()
            .ok_or(ApfsStorageError::InvalidPlan("missing pending clone"))?;
        if pending.image != image
            || pending.workspace.incarnation() != &expected.workspace_incarnation
            || pending.workspace.revision().get() != expected.grants.revision
        {
            return Err(ApfsStorageError::InvalidPlan(
                "pending clone identity changed",
            ));
        }
        Ok(())
    }

    fn resumable_clone(
        &self,
        _: &ApfsSubstrateConfig,
        _: &LifecycleWorkspace,
        _: &WorkspaceName,
        _: Revision,
        _: &OperationIdentity,
    ) -> Result<Option<ResumableClone>, ApfsStorageError> {
        Ok(self
            .state
            .lock()
            .expect("fake state")
            .resumable_clone
            .clone())
    }

    fn attach_verified(&self, image: &Path) -> Result<Self::Attachment, ApfsStorageError> {
        if !is_image_path(image) {
            return Err(ApfsStorageError::Host(
                "attach of a path that is not an image".to_owned(),
            ));
        }
        self.record("attach-no-mount+fsck");
        Ok(FakeAttachment)
    }

    fn attach_mounted(
        &self,
        image: &Path,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        if !is_image_path(image) {
            return Err(ApfsStorageError::Host(
                "attach of a path that is not an image".to_owned(),
            ));
        }
        self.record("attach-mounted+checked");
        self.mount(&FakeAttachment, mount_point, access, browse)?;
        Ok(FakeAttachment)
    }

    /// Reuses a mount the fake already holds at `mount_point`, as the native host reuses a
    /// surviving kernel mount; otherwise attaches and mounts.
    fn attach_and_mount_resumable(
        &self,
        image: &Path,
        mount_point: &Path,
        _: &LifecycleWorkspace,
    ) -> Result<ResumedMount<Self::Attachment>, ApfsStorageError> {
        if self.mounted_paths_now().contains(mount_point) {
            self.record("reuse-mount");
            return Ok(ResumedMount {
                attachment: FakeAttachment,
                found: FoundMount::Mounted,
                attach_mount_work: None,
            });
        }
        Ok(ResumedMount {
            attachment: self.attach_mounted(image, mount_point, MountAccess::ReadWrite, false)?,
            found: FoundMount::Detached,
            attach_mount_work: None,
        })
    }

    fn copy_tree(&self, source: &Path, _: &Path) -> Result<(), ApfsStorageError> {
        self.record("copy-until-quiescent");
        self.record_path(source);
        Ok(())
    }
    fn mount(
        &self,
        _: &Self::Attachment,
        mount_point: &Path,
        _: MountAccess,
        _: bool,
    ) -> Result<(), ApfsStorageError> {
        self.record_path(mount_point);
        self.state
            .lock()
            .expect("fake state")
            .mount_paths
            .push(mount_point.to_owned());
        self.mounted_paths
            .lock()
            .expect("mounted paths")
            .insert(mount_point.to_owned());
        self.record("mount");
        Ok(())
    }

    fn rename_volume(
        &self,
        _: &Self::Attachment,
        _: &Path,
        volume_key: &str,
    ) -> Result<(), ApfsStorageError> {
        self.record(format!("rename-volume:{volume_key}"));
        Ok(())
    }

    fn mint_workspace_credentials(
        &self,
        _: &LifecycleWorkspace,
        _: &Path,
        _: &Path,
        private_key_path: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.record_path(private_key_path);
        self.record("mint-workspace-credentials");
        if self.fail_credentials_once.swap(false, Ordering::SeqCst) {
            Err(ApfsStorageError::Host(
                "injected credential mint failure".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn write_marker(
        &self,
        _: &Path,
        _: &LifecycleWorkspace,
        forked_from: Option<&WorkspaceName>,
        identity: &OperationIdentity,
    ) -> Result<(), ApfsStorageError> {
        self.record(format!("marker-identity:{}", identity.created_trace));
        self.record(if forked_from.is_some() {
            "write-marker:fork"
        } else {
            "write-marker"
        });
        Ok(())
    }

    fn validate_marker(&self, _: &Path, _: &MarkerExpectation) -> Result<(), ApfsStorageError> {
        self.record("validate-marker");
        let remaining = self
            .marker_validations_before_failure
            .load(Ordering::SeqCst);
        if remaining != usize::MAX
            && self
                .marker_validations_before_failure
                .compare_exchange(
                    remaining,
                    remaining.saturating_sub(1),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            && remaining == 0
        {
            self.marker_validations_before_failure
                .store(usize::MAX, Ordering::SeqCst);
            Err(ApfsStorageError::MarkerMismatch("injected".to_owned()))
        } else {
            Ok(())
        }
    }

    fn validate_staged_companion(&self, path: &Path) -> Result<(), ApfsStorageError> {
        self.record_path(path);
        self.record("validate-staged-companion");
        Ok(())
    }
    fn detach(&self, _: Self::Attachment, intent: DetachIntent) -> Result<(), ApfsStorageError> {
        self.record(format!("detach:{intent:?}"));
        self.mounted_paths.lock().expect("mounted paths").clear();
        Ok(())
    }

    fn heal_mount(&self, _: &LifecycleWorkspace, _: &Path) -> Result<(), ApfsStorageError> {
        Ok(())
    }

    fn mount_settled(
        &self,
        workspace: &LifecycleWorkspace,
        _: &Path,
    ) -> Result<bool, ApfsStorageError> {
        self.record("mount-settled");
        Ok(matches!(
            self.workspace_mount_state(workspace)?,
            MountState::Mounted { .. }
        ))
    }

    fn retain_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        _: Self::Attachment,
    ) -> Result<u64, ApfsStorageError> {
        self.record("retain-mounted");
        let mut state = self.state.lock().expect("fake state");
        state.next_mount_id += 1;
        let mount_id = state.next_mount_id;
        state.mounted.insert(
            (workspace.repo().clone(), workspace.name().clone()),
            KernelMountFact {
                mount_id,
                volume_key: volume_key(workspace.repo(), workspace.name()),
            },
        );
        Ok(mount_id)
    }

    fn detach_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        intent: DetachIntent,
    ) -> Result<(), ApfsStorageError> {
        self.record(format!("detach-mounted:{intent:?}"));
        self.state
            .lock()
            .expect("fake state")
            .mounted
            .remove(&(workspace.repo().clone(), workspace.name().clone()));
        Ok(())
    }

    fn ensure_linked_build_volume(
        &self,
        _: &LifecycleWorkspace,
        checkout: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.record(format!("ensure-build-volume:{}", checkout.display()));
        Ok(())
    }

    fn detach_owned_build_volumes(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<(), ApfsStorageError> {
        self.record(format!("detach-build-volumes:{}", workspace.name()));
        Ok(())
    }

    fn resize(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
        _: &Path,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError> {
        self.record_path(image);
        self.record(format!("resize:{}:{capacity}", workspace.name()));
        Ok(ResizeOutcome {
            previous: DEFAULT_IMAGE_CAPACITY,
            capacity,
        })
    }

    fn defragment(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
        _: &Path,
    ) -> Result<DefragmentOutcome, ApfsStorageError> {
        self.record_path(image);
        self.record(format!("defragment:{}", workspace.name()));
        Ok(DefragmentOutcome {
            previous: ExtentCount::new(2),
            extents: ExtentCount::new(1),
            bytes: 0,
        })
    }

    fn restore_adopted_checkout(
        &self,
        workspace: &LifecycleWorkspace,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.record_path(source_checkout);
        self.record_path(pre_cowshed_checkout);
        self.detach_mounted(workspace, DetachIntent::WhenIdle)?;
        self.record("atomic-restore-checkout");
        Ok(())
    }
    fn vacate_adopted_checkout(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), PublicationError> {
        self.record("atomic-adopt-checkout-vacate");
        self.record_path(source_checkout);
        self.record_path(pre_cowshed_checkout);
        Ok(())
    }
    fn vacate_plain_checkout(
        &self,
        checkout: &Path,
        preserved: &Path,
    ) -> Result<(), PublicationError> {
        self.record("atomic-plain-checkout-vacate");
        self.record_path(checkout);
        self.record_path(preserved);
        Ok(())
    }
    fn require_vacant_checkout(&self, checkout: &Path) -> Result<(), ApfsStorageError> {
        self.record("replacement-checkout-vacant");
        self.record_path(checkout);
        Ok(())
    }
    fn publish_metadata(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        policy: MetadataPolicy,
        identity: Option<&OperationIdentity>,
        _: Option<&Path>,
    ) -> Result<(), ApfsStorageError> {
        self.record(format!("atomic-metadata+parent-fsync:{policy:?}"));
        if revision != workspace.revision()
            && matches!(
                policy,
                MetadataPolicy::Fresh | MetadataPolicy::FreshPendingFence
            )
        {
            return Err(ApfsStorageError::Host("fresh revision mismatch".to_owned()));
        }
        let fact = StorageFact {
            workspace: workspace.clone(),
            volume_key: volume_key(workspace.repo(), workspace.name()),
        };
        if matches!(
            policy,
            MetadataPolicy::PendingFence | MetadataPolicy::FreshPendingFence
        ) {
            let key = (workspace.repo().clone(), workspace.name().clone());
            let mut state = self.state.lock().expect("fake state");
            state.published.remove(&key);
            state.pending.insert(image.to_owned(), fact);
            if policy == MetadataPolicy::FreshPendingFence {
                let identity = identity.ok_or(ApfsStorageError::InvalidPlan(
                    "fake pending publication requires operation identity",
                ))?;
                if workspace.name().is_main() {
                    state.resumable_adopt = Some(PendingAdoption {
                        incarnation: workspace.incarnation().clone(),
                        image: image.to_owned(),
                        info: WorkspaceInfoSnapshot {
                            project_root: identity.project_root.clone(),
                            role: workspace.role(),
                            base_commit: identity.base_commit.clone(),
                            branch: identity.branch.clone(),
                            created_at: identity.created_at.clone(),
                            forked_from: identity.forked_from.clone(),
                            captured_at: identity.created_at.clone(),
                            stale: false,
                            git_worktree: identity.git_worktree,
                        },
                        grants: identity.grants.clone(),
                    });
                } else {
                    state.resumable_clone = Some(ResumableClone {
                        workspace: workspace.clone(),
                        identity: identity.clone(),
                        image: image.to_owned(),
                    });
                }
            }
        } else if image
            .components()
            .any(|component| component.as_os_str() == ".staging")
        {
            self.state
                .lock()
                .expect("fake state")
                .staged
                .insert(image.to_owned(), fact);
        } else {
            self.seed(workspace);
        }
        Ok(())
    }

    fn activate_pending(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
    ) -> Result<(), ApfsStorageError> {
        self.record("activate-pending");
        let mut state = self.state.lock().expect("fake state");
        let fact = state
            .pending
            .remove(image)
            .ok_or_else(|| ApfsStorageError::Host("missing fake pending metadata".to_owned()))?;
        if fact.workspace != *workspace {
            return Err(ApfsStorageError::Host(
                "fake pending identity mismatch".to_owned(),
            ));
        }
        let key = (workspace.repo().clone(), workspace.name().clone());
        state.published.insert(key, fact);
        state.resumable_clone = None;
        state.resumable_adopt = None;
        Ok(())
    }

    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError> {
        self.record("atomic-retire-to-trash");
        self.record_path(canonical);
        self.record_path(trash);
        Ok(())
    }

    fn release_workspace(
        &self,
        _: &ApfsSubstrateConfig,
        _: &RetiredRef,
    ) -> Result<(), ApfsStorageError> {
        self.record("physical-release");
        if self.fail_reclaim_once.swap(false, Ordering::SeqCst) {
            return Err(ApfsStorageError::Host(
                "injected physical unlink failure".to_owned(),
            ));
        }
        Ok(())
    }

    fn reclaim_image(&self, image: &Path) -> Result<(), ApfsStorageError> {
        self.record("idempotent-reclaim");
        let mut state = self.state.lock().expect("fake state");
        state.staged.remove(image);
        state.pending.remove(image);
        if state
            .resumable_clone
            .as_ref()
            .is_some_and(|pending| pending.image == image)
        {
            state.resumable_clone = None;
        }
        if state
            .resumable_adopt
            .as_ref()
            .is_some_and(|pending| pending.image == image)
        {
            state.resumable_adopt = None;
        }
        if self.fail_reclaim_once.swap(false, Ordering::SeqCst) {
            Err(ApfsStorageError::Host(
                "injected reclaim failure".to_owned(),
            ))
        } else {
            Ok(())
        }
    }
    fn reclaim_retired(
        &self,
        _: &ApfsSubstrateConfig,
        _: &RetiredRef,
    ) -> Result<(), ApfsStorageError> {
        self.record("idempotent-reclaim");
        if self.fail_reclaim_once.swap(false, Ordering::SeqCst) {
            Err(ApfsStorageError::Host(
                "injected reclaim failure".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn list(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError> {
        Ok(self
            .state
            .lock()
            .expect("fake state")
            .published
            .iter()
            .filter(|((published_repo, _), _)| published_repo == repo)
            .map(|(_, fact)| fact.clone())
            .collect())
    }

    fn workspace_mount_state(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<MountState, ApfsStorageError> {
        let state = self.state.lock().expect("fake state");
        let key = (workspace.repo().clone(), workspace.name().clone());
        if !state
            .published
            .get(&key)
            .is_some_and(|fact| &fact.workspace == workspace)
        {
            return Err(ApfsStorageError::InvalidPlan("workspace is not published"));
        }
        Ok(match state.mounted.get(&key) {
            Some(mount) => MountState::Mounted {
                mount_id: mount.mount_id,
            },
            None => MountState::Detached,
        })
    }

    fn mounts(&self, repo: &RepoId) -> Result<Vec<MountJoin>, ApfsStorageError> {
        Ok(self
            .state
            .lock()
            .expect("fake state")
            .mounted
            .iter()
            .filter(|((mounted_repo, _), _)| mounted_repo == repo)
            .map(|(_, fact)| MountJoin::Proven(fact.clone()))
            .collect())
    }

    fn recover_pending(
        &self,
        _: &ApfsSubstrateConfig,
        _: &[PathBuf],
    ) -> Result<(), ApfsStorageError> {
        let mut state = self.state.lock().expect("fake state");
        let resumable = [
            state
                .resumable_clone
                .as_ref()
                .map(|clone| clone.image.clone()),
            state
                .resumable_adopt
                .as_ref()
                .map(|adoption| adoption.image.clone()),
        ];
        let pending = std::mem::take(&mut state.pending);
        for (image, fact) in pending {
            if resumable.contains(&Some(image.clone())) {
                state.pending.insert(image, fact);
                continue;
            }
            state.events.push("recover-pending-publication".to_owned());
            let key = (fact.workspace.repo().clone(), fact.workspace.name().clone());
            state.published.insert(key, fact);
        }
        Ok(())
    }

    fn stats(&self, _: &LifecycleWorkspace, _: &Path) -> Result<SubstrateStats, ApfsStorageError> {
        Ok(SubstrateStats {
            logical_bytes: 4096,
            allocated_bytes: 1024,
        })
    }

    fn preview_gc(
        &self,
        _: &ApfsSubstrateConfig,
        repo: &RepoId,
        _: StorageGcOptions,
    ) -> Result<StorageGcPlan, ApfsStorageError> {
        self.record("preview-gc");
        Ok(StorageGcPlan::empty(repo.clone()))
    }

    fn execute_gc(
        &self,
        _: &ApfsSubstrateConfig,
        _: StorageGcPlan,
    ) -> Result<StorageGcReport, ApfsStorageError> {
        self.record("execute-gc");
        Ok(StorageGcReport {
            examined: 2,
            reclaimed: 1,
            retained_active: 0,
            released_bytes: 1024,
            deferred: Vec::new(),
            root_held_mountpoints: Vec::new(),
        })
    }
}

#[derive(Clone, Default)]
struct CountingLane(Arc<AtomicUsize>);

impl CountingLane {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ApfsBlockingLane for CountingLane {
    async fn dispatch<T, F>(&self, job: F) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ApfsStorageError> + Send + 'static,
    {
        self.0.fetch_add(1, Ordering::SeqCst);
        job()
    }
}

#[derive(Clone, Default)]
struct FixedIncarnations(Arc<AtomicU64>);

impl IncarnationSource for FixedIncarnations {
    fn mint(&self) -> Result<WorkspaceIncarnation, ApfsStorageError> {
        let value = self.0.fetch_add(1, Ordering::SeqCst);
        WorkspaceIncarnation::new(format!("{value:032x}"))
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }
}

fn repo() -> RepoId {
    RepoId::parse("acme/widget").expect("repo")
}

fn incarnation(value: u8) -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(format!("{value:032x}")).expect("incarnation")
}

fn identity() -> OperationIdentity {
    OperationIdentity {
        project_root: PathBuf::from("/project"),
        base_commit: "0123456789abcdef".to_owned(),
        created_at: "2026-07-13T00:00:00Z".to_owned(),
        branch: Some("main".to_owned()),
        forked_from: None,
        created_trace: "apfs-storage".to_owned(),
        git_worktree: false,
        grants: GrantSet::closed_baseline(Some(
            PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).expect("port block"),
        ))
        .expect("grants"),
    }
}

fn adopt_request() -> AdoptRequest {
    AdoptRequest {
        repo: repo(),
        capacity: DEFAULT_IMAGE_CAPACITY,
        topology_revision: Revision::new(0),
        source_checkout: PathBuf::from("/project"),
        handoff: AdoptHandoff::Checkout {
            pre_cowshed_checkout: PathBuf::from("/project.pre-cowshed"),
        },
        identity: identity(),
    }
}

fn workspace(name: &str, revision: u64) -> LifecycleWorkspace {
    let name = WorkspaceName::new(name).expect("workspace name");
    LifecycleWorkspace::new(
        repo(),
        name.clone(),
        incarnation(revision as u8),
        Revision::new(revision),
        Revision::new(revision),
        if name.is_main() {
            WorkspaceRole::Main
        } else {
            WorkspaceRole::Workspace
        },
    )
    .expect("workspace ref")
}

fn substrate(host: FakeHost, lane: CountingLane) -> ApfsSubstrate<FakeHost, CountingLane> {
    ApfsSubstrate::with_lane_and_incarnations(
        ApfsSubstrateConfig::new(
            HostNamespace::Test(TestNamespace::mint().expect("a test namespace")),
            "/store",
            "/project",
        ),
        host,
        lane,
        FixedIncarnations::default(),
    )
}

async fn abort_at_callback<T>(task: tokio::task::JoinHandle<T>, entered: Arc<AtomicBool>) {
    for _ in 0..1_000 {
        if entered.load(Ordering::SeqCst) {
            task.abort();
            match task.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("aborted staged execution completed"),
            }
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("staged callback was not entered");
}

fn assert_no_orphan_stage(host: &FakeHost) {
    assert!(
        host.mounted_paths_now().is_empty(),
        "cancellation left a staging mount attached"
    );
    assert!(
        host.staged_paths_now().is_empty(),
        "cancellation left staged metadata or an orphan image"
    );
}

/// Adopt creates main at its canonical path behind a PendingFence sidecar and attaches it exactly
/// once: the staging mount and the checkout mount are two mounts of that one attachment, the
/// staging one comes down through the kernel's unmount, and publication is the sidecar's
/// activation. No detach, no second attach and no rename sit on the normal path.
#[tokio::test]
async fn adopt_publishes_main_by_activating_its_fence_on_one_attachment() {
    let host = FakeHost::default();
    let lane = CountingLane::default();
    let substrate = substrate(host.clone(), lane.clone());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    let callback_host = host.clone();
    let (receipt, ()) = substrate
        .execute_adopt_staged(plan, Alongside::none(), move |stage| async move {
            assert!(stage.workspace.name().is_main());
            assert!(
                stage
                    .mount_point
                    .components()
                    .any(|component| component.as_os_str() == ".staging")
            );
            assert_eq!(
                callback_host.mounted_paths_now(),
                BTreeSet::from([stage.mount_point])
            );
            assert!(
                callback_host
                    .list(&repo())
                    .expect("controller listing")
                    .is_empty(),
                "the fenced main must not be visible in canonical enumeration"
            );
            assert!(
                !callback_host
                    .events()
                    .contains(&"activate-pending".to_owned())
            );
            callback_host.record("controller-initialize");
            Ok::<(), &'static str>(())
        })
        .await
        .expect("adopt");

    assert_eq!(receipt.workspace.revision(), Revision::new(1));
    assert_eq!(receipt.workspace.incarnation(), &incarnation(0));
    assert_eq!(receipt.workspace.topology_revision(), Revision::new(1));
    assert_eq!(
        lane.count(),
        4,
        "lock, authoritative read, staged preparation, and post-callback commit"
    );
    let events = host.events();
    assert_eq!(
        events,
        [
            "lock:1",
            "observe",
            // The fence is durable before the payload name appears.
            "atomic-metadata+parent-fsync:FreshPendingFence",
            "create-mounted+checked:100g",
            "mount",
            "copy-until-quiescent",
            "mint-workspace-credentials",
            "marker-identity:apfs-storage",
            "write-marker",
            "validate-marker",
            "controller-initialize",
            "validate-staged-companion",
            "validate-marker",
            "unmount",
            // Publication: the user's tree is untouched until the image is published...
            "activate-pending",
            // ...then the swap plants the mountpoint at the checkout path, and the same
            // attachment mounts there.
            "atomic-adopt-checkout-vacate",
            "mount",
            "validate-marker",
            "retain-mounted",
        ]
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("create-mounted") || event.starts_with("attach-"))
            .count(),
        1,
        "main is attached exactly once"
    );
    assert!(
        !events.iter().any(|event| event.starts_with("detach")),
        "the normal path never detaches main"
    );
    assert_eq!(
        host.paths()[0],
        PathBuf::from("/store/acme/widget/main.dmg"),
        "the image is created at its canonical path"
    );
    assert_eq!(
        host.mount_paths(),
        [
            PathBuf::from(format!(
                "/store/mnt/acme/widget/.staging/main-{}",
                incarnation(0)
            )),
            PathBuf::from("/project"),
        ]
    );
    assert_eq!(
        substrate
            .mount_state(&receipt.workspace)
            .await
            .expect("mount state"),
        MountState::Mounted { mount_id: 1 }
    );
}

/// A re-adoption's replacement goes through the same staged copy, fence and activation as an
/// adoption (02_workspaces.md, "Re-adoption" steps 3 and 5), with three differences: it copies
/// from the replaced main's retained read-only mount, it is published as the incarnation the
/// re-adoption recorded, and it mounts on the empty mountpoint the replaced main left instead
/// of swapping the user's tree aside.
#[tokio::test]
async fn a_replacement_copies_its_retained_source_and_mounts_on_the_vacant_checkout() {
    let host = FakeHost::default();
    let substrate = substrate(host.clone(), CountingLane::default());
    let replacement = incarnation(42);
    let copy_source = PathBuf::from("/store/mnt/acme/widget/.staging/readopt-old-main");
    let plan = substrate
        .plan_adopt(AdoptRequest {
            handoff: AdoptHandoff::Replacement {
                copy_source: copy_source.clone(),
                incarnation: replacement.clone(),
            },
            ..adopt_request()
        })
        .expect("replacement plan");
    let (receipt, ()) = substrate
        .execute_adopt_staged(plan, Alongside::none(), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect("replacement");

    assert_eq!(receipt.workspace.incarnation(), &replacement);
    let events = host.events();
    assert!(
        !events
            .iter()
            .any(|event| event == "atomic-adopt-checkout-vacate"),
        "a replacement never swaps the checkout: {events:?}"
    );
    let vacant = events
        .iter()
        .rposition(|event| event == "replacement-checkout-vacant")
        .expect("the checkout is proven vacant before main mounts there");
    let activated = events
        .iter()
        .position(|event| event == "activate-pending")
        .expect("published");
    assert!(activated < vacant, "{events:?}");
    assert_eq!(
        events[vacant + 1..].first().map(String::as_str),
        Some("mount"),
        "{events:?}"
    );
    assert!(
        host.paths().contains(&copy_source),
        "main is copied from the retained source"
    );
    assert!(
        !host
            .paths()
            .contains(&PathBuf::from("/project.pre-cowshed")),
        "the user's .pre-cowshed tree is never named"
    );
    assert_eq!(
        host.mount_paths(),
        [
            PathBuf::from(format!(
                "/store/mnt/acme/widget/.staging/main-{replacement}"
            )),
            PathBuf::from("/project"),
        ]
    );
}

/// A replacement plan names a copy source outside the checkout it replaces.
#[test]
fn a_replacement_copying_from_inside_its_checkout_is_not_planned() {
    let host = FakeHost::default();
    let substrate = substrate(host, CountingLane::default());
    let error = substrate
        .plan_adopt(AdoptRequest {
            handoff: AdoptHandoff::Replacement {
                copy_source: PathBuf::from("/project/inner"),
                incarnation: incarnation(42),
            },
            ..adopt_request()
        })
        .expect_err("a source inside the checkout");
    assert!(matches!(error, PlanError::InvalidAdoptHandoff), "{error:?}");
}

fn pending_adoption(identity: &OperationIdentity) -> PendingAdoption {
    PendingAdoption {
        incarnation: incarnation(7),
        image: PathBuf::from("/store/acme/widget/main.dmg"),
        info: WorkspaceInfoSnapshot {
            project_root: identity.project_root.clone(),
            role: WorkspaceRole::Main,
            base_commit: identity.base_commit.clone(),
            branch: identity.branch.clone(),
            created_at: "2026-07-12T00:00:00Z".to_owned(),
            forked_from: None,
            captured_at: "2026-07-12T00:00:00Z".to_owned(),
            stale: false,
            git_worktree: false,
        },
        grants: identity.grants.clone(),
    }
}

/// Killed during the tree copy (or after attach, or after the marker), adopt resumes the same
/// canonical image in place under its recorded identity: nothing is created, the delta copier
/// runs over what the last attempt left, and the marker validates instead of growing a lineage.
#[tokio::test]
async fn interrupted_adopt_resumes_its_canonical_image_in_place() {
    let host = FakeHost::default();
    let pending = pending_adoption(&identity());
    host.resume_adopt_from(pending.clone());
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    let (receipt, ()) = substrate
        .execute_adopt_staged(plan, Alongside::none(), |stage| async move {
            assert!(
                stage.resuming,
                "the initializer reruns with resume authority"
            );
            Ok::<(), &'static str>(())
        })
        .await
        .expect("resumed adopt");

    assert_eq!(receipt.workspace.incarnation(), &pending.incarnation);
    let events = host.events();
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("create-attached"))
    );
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("atomic-metadata+parent-fsync"))
    );
    assert!(!events.contains(&"discard-pending".to_owned()));
    let resumed = events
        .iter()
        .position(|event| event == "resume-attach-or-reuse")
        .expect("pending image resumed");
    let copy = events
        .iter()
        .position(|event| event == "copy-until-quiescent")
        .expect("convergent tree copy");
    assert!(resumed < copy, "the resumed image seeds the tree copy");
    assert!(events.contains(&"activate-pending".to_owned()));
    assert!(!events.iter().any(|event| event.starts_with("detach")));
    assert_eq!(host.pending_adopt(), None);
}

/// An unpublished main recording a different commit is a different adoption: it is replaced, and
/// the checkout it was copying was never touched.
#[tokio::test]
async fn adopt_replaces_an_unpublished_main_copied_from_another_commit() {
    let host = FakeHost::default();
    let mut stale = identity();
    stale.base_commit = "fedcba9876543210".to_owned();
    host.resume_adopt_from(pending_adoption(&stale));
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    let (receipt, ()) = substrate
        .execute_adopt_staged(plan, Alongside::none(), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect("fresh adopt");

    assert_eq!(receipt.workspace.incarnation(), &incarnation(0));
    let events = host.events();
    assert!(!events.iter().any(|event| event.starts_with("resume-")));
    let discarded = events
        .iter()
        .position(|event| event == "discard-pending")
        .expect("stale main discarded");
    let created = events
        .iter()
        .position(|event| event.starts_with("create-mounted"))
        .expect("fresh main created");
    assert!(discarded < created);
}

/// Killed while the image was being created — before its volume was formatted — the pending main
/// never held a copy: it is discarded and adoption starts over.
#[tokio::test]
async fn adopt_replaces_a_pending_main_whose_volume_was_never_formatted() {
    let host = FakeHost::default();
    host.resume_adopt_from(pending_adoption(&identity()));
    host.leave_pending_adopt_unformatted();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    let (receipt, ()) = substrate
        .execute_adopt_staged(plan, Alongside::none(), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect("fresh adopt");

    assert_eq!(receipt.workspace.incarnation(), &incarnation(0));
    let events = host.events();
    let order = [
        "resume-unformatted",
        "discard-pending",
        "create-mounted+checked:100g",
    ]
    .map(|step| {
        events
            .iter()
            .position(|event| event == step)
            .unwrap_or_else(|| panic!("missing {step}: {events:?}"))
    });
    assert!(order[0] < order[1] && order[1] < order[2], "{events:?}");
}

/// A resumed adoption that fails again keeps its image: it holds the copy the next attempt
/// continues from, so the repository copy never restarts from zero.
#[tokio::test]
async fn failed_resumed_adopt_keeps_the_pending_image_for_the_next_attempt() {
    let host = FakeHost::default();
    let pending = pending_adoption(&identity());
    host.resume_adopt_from(pending.clone());
    host.fail_next_credentials();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    substrate
        .execute_adopt_staged(plan, Alongside::none(), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect_err("credential mint failure");

    let events = host.events();
    assert!(events.contains(&"detach:Release".to_owned()));
    assert!(!events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"activate-pending".to_owned()));
    assert_eq!(host.pending_adopt(), Some(pending));
    assert!(host.list(&repo()).expect("listing").is_empty());
}

/// Side work an adopt runs beside staging main (main's first build volume, minted beside it):
/// the value it answers, which says when it is dropped.
#[derive(Debug)]
struct SideWork {
    dropped: Arc<AtomicBool>,
}

impl Drop for SideWork {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// A staged adopt answers its side work's output with the receipt once main is committed, and
/// hands it to `abandon` when main is not: a staging failure or an initializer failure. Nothing
/// of it is dropped unanswered on the way.
#[tokio::test]
async fn a_staged_adopt_answers_its_side_work_or_abandons_it() {
    let (abandoning, abandoned) = std::sync::mpsc::channel();
    let side = |dropped: &Arc<AtomicBool>, label: &'static str| Alongside {
        work: {
            let dropped = Arc::clone(dropped);
            async move { (label, SideWork { dropped }) }
        },
        abandon: {
            let abandoning = abandoning.clone();
            move |(label, work): (&'static str, SideWork)| async move {
                abandoning.send(label).expect("the test holds the receiver");
                drop(work);
            }
        },
    };

    let dropped = Arc::new(AtomicBool::new(false));
    let host = FakeHost::default();
    let committing = substrate(host.clone(), CountingLane::default());
    let plan = committing.plan_adopt(adopt_request()).expect("adopt plan");
    let (_, (label, work)) = committing
        .execute_adopt_staged(plan, side(&dropped, "committed"), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect("adopt");
    assert_eq!(label, "committed");
    assert!(!dropped.load(Ordering::SeqCst), "answered, not dropped");
    drop(work);

    let dropped = Arc::new(AtomicBool::new(false));
    let host = FakeHost::default();
    host.resume_adopt_from(pending_adoption(&identity()));
    host.fail_next_credentials();
    let failing = substrate(host.clone(), CountingLane::default());
    let plan = failing.plan_adopt(adopt_request()).expect("adopt plan");
    failing
        .execute_adopt_staged(plan, side(&dropped, "unstaged"), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect_err("credential mint failure");
    assert!(dropped.load(Ordering::SeqCst));

    let dropped = Arc::new(AtomicBool::new(false));
    let host = FakeHost::default();
    let initializing = substrate(host.clone(), CountingLane::default());
    let plan = initializing
        .plan_adopt(adopt_request())
        .expect("adopt plan");
    initializing
        .execute_adopt_staged(plan, side(&dropped, "uninitialized"), |_| async {
            Err::<(), &'static str>("initializer failure")
        })
        .await
        .expect_err("initializer failure");
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(
        abandoned.try_iter().collect::<Vec<_>>(),
        ["unstaged", "uninitialized"]
    );
}

/// An adopt dropped while its initializer runs drops its side work's output with it, unanswered
/// and unabandoned: what owns a resource there releases it when dropped (main's first build
/// volume does), since nothing after the drop runs for it.
#[tokio::test]
async fn a_dropped_staged_adopt_drops_its_side_work_output() {
    let dropped = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicBool::new(false));
    let host = FakeHost::default();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");
    let (started, initializing) = tokio::sync::oneshot::channel();
    let adopt = substrate.execute_adopt_staged(
        plan,
        Alongside {
            work: {
                let dropped = Arc::clone(&dropped);
                async move { SideWork { dropped } }
            },
            abandon: {
                let abandoned = Arc::clone(&abandoned);
                move |_: SideWork| async move { abandoned.store(true, Ordering::SeqCst) }
            },
        },
        move |_| async move {
            started
                .send(())
                .expect("the test waits for the initializer");
            std::future::pending::<Result<(), &'static str>>().await
        },
    );
    tokio::select! {
        _ = adopt => panic!("the initializer never returns"),
        started = initializing => started.expect("the initializer ran"),
    }
    assert!(dropped.load(Ordering::SeqCst), "dropped with the adopt");
    assert!(!abandoned.load(Ordering::SeqCst));
}

/// The mountpoint *is* the checkout path and cannot exist until the swap creates it, so the swap
/// comes first and the mount follows it — of the attachment the copy ran on.
#[tokio::test]
async fn direct_mount_adopt_swaps_the_checkout_before_mounting_it() {
    let host = FakeHost::default();
    let lane = CountingLane::default();
    let substrate = substrate(host.clone(), lane.clone());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    substrate
        .execute_adopt_staged(plan, Alongside::none(), |_stage| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect("adopt");

    let events = host.events();
    let tail: Vec<_> = events
        .iter()
        .skip_while(|event| event.as_str() != "unmount")
        .cloned()
        .collect();
    assert_eq!(
        tail,
        [
            "unmount",
            // The image is published on its own: there is no mountpoint to create under the store,
            // because main's mountpoint is the user's checkout path.
            "activate-pending",
            // The swap plants the mountpoint and the self-healing stub at the checkout path...
            "atomic-adopt-checkout-vacate",
            // ...and only then can main be mounted there.
            "mount",
            "validate-marker",
            "retain-mounted",
        ]
    );
}

/// Killed after activation — before the checkout swap, or after it and before the mount — main is
/// published and the adoption is finished from durable facts: the swap is idempotent, and main is
/// mounted at the checkout. A second pass finds it mounted and only validates it.
#[tokio::test]
async fn finishing_a_published_adoption_swaps_and_mounts_once() {
    let host = FakeHost::default();
    let substrate = substrate(host.clone(), CountingLane::default());
    let main = workspace("main", 1);
    host.seed(&main);
    let pre_cowshed = AdoptHandoff::Checkout {
        pre_cowshed_checkout: PathBuf::from("/project.pre-cowshed"),
    };

    substrate
        .finish_adoption(&main, &pre_cowshed)
        .await
        .expect("finish adoption");
    let first = host.events();
    let tail: Vec<_> = first
        .iter()
        .skip_while(|event| event.as_str() != "atomic-adopt-checkout-vacate")
        .cloned()
        .collect();
    assert_eq!(
        tail,
        [
            "atomic-adopt-checkout-vacate",
            "attach-mounted+checked",
            "mount",
            "validate-marker",
            "retain-mounted",
        ]
    );
    assert_eq!(host.mount_paths(), [PathBuf::from("/project")]);
    assert_eq!(
        substrate.mount_state(&main).await.expect("mount state"),
        MountState::Mounted { mount_id: 1 }
    );

    host.clear_events();
    substrate
        .finish_adoption(&main, &pre_cowshed)
        .await
        .expect("finishing again is idempotent");
    let second: Vec<_> = host
        .events()
        .into_iter()
        .filter(|event| !event.starts_with("lock:"))
        .collect();
    assert_eq!(second, ["atomic-adopt-checkout-vacate", "validate-marker"]);
}

#[tokio::test]
async fn initializer_failure_detaches_reclaims_and_never_publishes() {
    let host = FakeHost::default();
    let lane = CountingLane::default();
    let substrate = substrate(host.clone(), lane.clone());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");
    let callback_host = host.clone();

    let error = substrate
        .execute_adopt_staged(plan, Alongside::none(), move |stage| async move {
            assert_eq!(
                callback_host.mounted_paths_now(),
                BTreeSet::from([stage.mount_point])
            );
            assert!(
                callback_host
                    .list(&repo())
                    .expect("controller listing")
                    .is_empty()
            );
            callback_host.record("controller-rejected");
            Err("identity commitment rejected")
        })
        .await
        .expect_err("initializer rejection");

    assert!(matches!(
        error,
        AdoptExecutionError::Initializer("identity commitment rejected")
    ));
    assert!(host.mounted_paths_now().is_empty());
    assert!(host.list(&repo()).expect("post-abort listing").is_empty());
    assert_eq!(lane.count(), 4, "abort cleanup uses the blocking lane");
    let events = host.events();
    assert!(events.contains(&"controller-rejected".to_owned()));
    assert!(events.contains(&"detach:Release".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"activate-pending".to_owned()));
    assert!(!events.contains(&"retain-mounted".to_owned()));
}

#[tokio::test]
async fn credential_mint_failure_reclaims_adopt_stage_before_publication() {
    let host = FakeHost::default();
    host.fail_next_credentials();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    substrate
        .execute_adopt_staged(plan, Alongside::none(), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect_err("credential mint failure");

    let events = host.events();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.as_str() == "mint-workspace-credentials")
            .count(),
        1
    );
    assert!(events.contains(&"detach:Release".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"write-marker".to_owned()));
    assert!(!events.contains(&"activate-pending".to_owned()));
    assert!(host.list(&repo()).expect("post-failure listing").is_empty());
}

#[tokio::test]
async fn initializer_and_cleanup_errors_are_both_preserved() {
    let host = FakeHost::default();
    host.fail_next_reclaim();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    let error = substrate
        .execute_adopt_staged(plan, Alongside::none(), |_| async {
            Err("tool wiring rejected")
        })
        .await
        .expect_err("initializer and cleanup fail");

    match error {
        AdoptExecutionError::InitializerCleanup {
            initializer,
            cleanup: ApfsStorageError::Host(cleanup),
        } => {
            assert_eq!(initializer, "tool wiring rejected");
            assert_eq!(cleanup, "injected reclaim failure");
        }
        other => panic!("unexpected compound adopt error: {other:?}"),
    }
    assert!(host.mounted_paths_now().is_empty());
    assert!(!host.events().contains(&"activate-pending".to_owned()));
}

#[tokio::test]
async fn adopt_rejects_each_source_identity_mismatch_before_mutation() {
    for (checkout_path, project_root) in [
        ("/project", "/different-project"),
        ("/different-main-mount", "/project"),
    ] {
        let host = FakeHost::default();
        let lane = CountingLane::default();
        let substrate = ApfsSubstrate::with_lane_and_incarnations(
            ApfsSubstrateConfig::new(
                HostNamespace::Test(TestNamespace::mint().expect("a test namespace")),
                "/store",
                checkout_path,
            ),
            host.clone(),
            lane.clone(),
            FixedIncarnations::default(),
        );
        let mut request = adopt_request();
        request.identity.project_root = PathBuf::from(project_root);
        let plan = substrate.plan_adopt(request).expect("adopt plan");

        let error = substrate
            .execute_adopt_staged(plan, Alongside::none(), |_| async {
                Ok::<(), &'static str>(())
            })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AdoptExecutionError::Storage(ApfsStorageError::InvalidPlan(
                "adopt source must equal operation project root and the configured checkout path"
            ))
        ));
        assert_eq!(host.events(), ["lock:1", "observe"]);
        assert_eq!(
            lane.count(),
            3,
            "validation may lock and observe, but must not execute a host mutation"
        );
    }
}

#[tokio::test]
async fn ensure_mounted_is_idempotent_for_an_already_mounted_workspace() {
    let host = FakeHost::default();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");
    let (receipt, ()) = substrate
        .execute_adopt_staged(plan, Alongside::none(), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect("adopt");
    let workspace = receipt.workspace;
    host.clear_events();

    let path = substrate
        .ensure_mounted(&workspace, MountIntent { browse: false })
        .await
        .expect("already mounted");

    assert_eq!(path, PathBuf::from("/project"));
    assert!(
        !host
            .events()
            .iter()
            .any(|event| event.starts_with("attach-")),
        "an already-mounted image must not be attached a second time"
    );
}
#[tokio::test]
async fn marker_mismatch_detaches_and_reclaims_staging_before_publication() {
    let host = FakeHost::default();
    host.fail_next_marker();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");

    let error = substrate
        .execute_adopt_staged(plan, Alongside::none(), |_| async {
            Ok::<(), &'static str>(())
        })
        .await
        .expect_err("marker mismatch");
    assert!(matches!(
        error,
        AdoptExecutionError::Storage(ApfsStorageError::MarkerMismatch(_))
    ));
    let events = host.events();
    assert!(events.iter().any(|event| event.starts_with("detach:")));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"activate-pending".to_owned()));
    assert!(!events.contains(&"retain-mounted".to_owned()));
}

#[tokio::test]
async fn lifecycle_receipts_preserve_exact_revisions_and_topology_for_create_and_fork() {
    let host = FakeHost::default();
    let source = workspace("main", 5);
    host.seed(&source);
    let substrate = substrate(host.clone(), CountingLane::default());

    let created = substrate
        .execute_create_staged(
            substrate
                .plan_create(
                    &source,
                    Destination {
                        repo: repo(),
                        name: WorkspaceName::session("created").expect("created"),
                        topology_revision: Revision::new(8),
                        identity: identity(),
                    },
                )
                .expect("create plan"),
            Beside::none(),
            |_, ()| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("create");
    assert_eq!(created.workspace.revision(), Revision::new(6));
    assert_eq!(created.workspace.topology_revision(), Revision::new(9));

    let forked = substrate
        .execute_fork_staged(
            substrate
                .plan_fork(
                    &source,
                    Destination {
                        repo: repo(),
                        name: WorkspaceName::session("forked").expect("forked"),
                        topology_revision: Revision::new(10),
                        identity: identity(),
                    },
                )
                .expect("fork plan"),
            Beside::none(),
            |_, ()| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("fork");
    assert_eq!(forked.workspace.revision(), Revision::new(6));
    assert_eq!(forked.workspace.topology_revision(), Revision::new(11));

    assert_eq!(
        host.events()
            .iter()
            .filter(|event| event.as_str() == "mint-workspace-credentials")
            .count(),
        2,
        "create and fork each mint one fresh authority"
    );
    let relabels: Vec<_> = host
        .events()
        .into_iter()
        .filter(|event| event.starts_with("rename-volume:"))
        .collect();
    assert!(
        relabels.is_empty(),
        "create and fork leave the inherited label to the workspace's supervisor, off the provisioning path"
    );

    let listed = substrate.list(&repo()).await.expect("list");
    assert!(
        listed
            .iter()
            .any(|observed| observed.workspace == created.workspace)
    );
    assert!(
        listed
            .iter()
            .any(|observed| observed.workspace == forked.workspace)
    );

    host.clear_events();
    substrate
        .unmount(&created.workspace)
        .await
        .expect("unmount");
    assert_eq!(
        host.events(),
        [
            "lock:1".to_owned(),
            "detach-mounted:WhenIdle".to_owned(),
            format!("detach-build-volumes:{}", created.workspace.name()),
        ]
    );
}

/// A new workspace's clone proves its extents between the clone and the attach, and nothing writes
/// it before its attach: its read-write mount pays the extent-map copy. A refused proof attaches
/// nothing.
#[tokio::test]
async fn a_new_workspace_is_proven_after_its_clone_and_before_its_attach() {
    let host = FakeHost::default();
    let source = workspace("main", 5);
    host.seed(&source);
    let substrate = substrate(host.clone(), CountingLane::default());
    let destination = |name: &str, topology| Destination {
        repo: repo(),
        name: WorkspaceName::session(name).expect("name"),
        topology_revision: Revision::new(topology),
        identity: identity(),
    };
    let around_the_clone = |events: Vec<String>| -> Vec<String> {
        events
            .into_iter()
            .skip_while(|event| event != "clone")
            .take(4)
            .collect()
    };

    host.clear_events();
    substrate
        .execute_create_staged(
            substrate
                .plan_create(&source, destination("created", 8))
                .expect("create plan"),
            Beside::none(),
            |_, ()| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("create");
    assert_eq!(
        around_the_clone(host.events()),
        ["clone", "extent-proof", "attach-mounted+checked", "mount"]
    );

    host.clear_events();
    substrate
        .execute_fork_staged(
            substrate
                .plan_fork(&source, destination("forked", 10))
                .expect("fork plan"),
            Beside::none(),
            |_, ()| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("fork");
    assert_eq!(
        around_the_clone(host.events()),
        ["clone", "extent-proof", "attach-mounted+checked", "mount"]
    );

    host.clear_events();
    host.fail_extent_proof_once.store(true, Ordering::SeqCst);
    substrate
        .execute_fork_staged(
            substrate
                .plan_fork(&source, destination("over-budget", 12))
                .expect("plan"),
            Beside::none(),
            |_, ()| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect_err("an unproven physical generation cannot publish");
    let refused = host.events();
    assert!(refused.iter().any(|event| event == "extent-proof"));
    assert!(
        !refused
            .iter()
            .any(|event| event == "attach-mounted+checked")
    );

    host.clear_events();
    let changed = host.clone();
    substrate
        .execute_fork_staged(
            substrate
                .plan_fork(&source, destination("publication-budget", 14))
                .expect("plan"),
            Beside::none(),
            move |_, ()| async move {
                changed.fail_extent_proof_once.store(true, Ordering::SeqCst);
                Ok::<(), &'static str>(())
            },
        )
        .await
        .expect_err("initialization cannot publish a now-unproven generation");
    {
        let state = host.state.lock().expect("state");
        assert!(
            !state
                .published
                .keys()
                .any(|(_, name)| name.as_str() == "publication-budget")
        );
        assert!(
            state
                .pending
                .values()
                .any(|fact| fact.workspace.name().as_str() == "publication-budget"),
            "post-initializer refusal preserves the exact pending clone"
        );
    }
}

#[tokio::test]
async fn staged_retire_releases_before_its_fence_and_never_keeps_a_trash_image() {
    let host = FakeHost::default();
    let current = workspace("raven", 4);
    host.seed(&current);
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_retire(&current).expect("retire plan");
    let callback_host = host.clone();

    let error = substrate
        .execute_retire_staged(plan, move |retired| async move {
            assert_eq!(retired.workspace().name().as_str(), "raven");
            assert!(
                callback_host
                    .events()
                    .contains(&"physical-release".to_owned()),
                "the release must be durable before its lifecycle fence"
            );
            Err::<(), _>("injected commitment failure")
        })
        .await
        .expect_err("fence failure is reported");
    let RetireExecutionError::Fence { source, .. } = error else {
        panic!("expected forward-only retirement fence failure");
    };
    assert_eq!(source, "injected commitment failure");
    assert!(
        !host.events().contains(&"atomic-retire-to-trash".to_owned()),
        "a retirement keeps no recovery image"
    );
}

#[tokio::test]
async fn failed_direct_source_unlink_does_not_publish_retirement_or_fall_back_to_trash() {
    let host = FakeHost::default();
    let current = workspace("raven", 4);
    host.seed(&current);
    host.fail_reclaim_once.store(true, Ordering::SeqCst);
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_retire(&current).unwrap();
    let callback_host = host.clone();
    let failed = substrate
        .execute_retire_staged(plan, move |_| async move {
            callback_host.record("retirement-commitment");
            Ok::<(), &'static str>(())
        })
        .await;
    assert!(matches!(failed, Err(RetireExecutionError::Storage(_))));
    assert!(!host.events().contains(&"retirement-commitment".to_owned()));
    assert!(!host.events().contains(&"atomic-retire-to-trash".to_owned()));
    assert!(
        host.list(&repo())
            .unwrap()
            .iter()
            .any(|fact| fact.workspace == current)
    );
    let retry = substrate.plan_retire(&current).unwrap();
    substrate
        .execute_retire_staged(retry, |_| async { Ok::<(), &'static str>(()) })
        .await
        .expect("same release retries");
}

#[tokio::test]
async fn cancelling_the_retirement_commitment_never_recreates_released_source_trash() {
    let host = FakeHost::default();
    let current = workspace("raven", 4);
    host.seed(&current);
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_retire(&current).unwrap();
    let entered = Arc::new(AtomicBool::new(false));
    let callback_entered = Arc::clone(&entered);
    let callback_host = host.clone();
    let task = tokio::spawn(async move {
        substrate
            .execute_retire_staged(plan, move |_| async move {
                assert!(
                    callback_host
                        .events()
                        .contains(&"physical-release".to_owned())
                );
                callback_entered.store(true, Ordering::SeqCst);
                std::future::pending::<Result<(), &'static str>>().await
            })
            .await
    });
    abort_at_callback(task, entered).await;
    assert!(!host.events().contains(&"atomic-retire-to-trash".to_owned()));
    assert!(!host.events().contains(&"idempotent-reclaim".to_owned()));
}

#[tokio::test]
async fn restored_main_retirement_is_a_distinct_fenced_terminal_path() {
    let host = FakeHost::default();
    let main = workspace("main", 4);
    host.seed(&main);
    let substrate = substrate(host.clone(), CountingLane::default());
    let callback_host = host.clone();

    let retired = substrate
        .execute_restored_main_retirement(&main, move |retired| async move {
            assert_eq!(retired.workspace().name().as_str(), "main");
            assert!(
                callback_host
                    .events()
                    .contains(&"atomic-retire-to-trash".to_owned()),
                "main image must be durably undiscoverable before the lifecycle fence"
            );
            Ok::<(), &'static str>(())
        })
        .await
        .expect("retire restored main");
    assert_eq!(retired.resulting_revision(), Revision::new(5));
    assert!(
        !host.events().contains(&"idempotent-reclaim".to_owned()),
        "reclamation remains a post-fence action"
    );
    substrate
        .reclaim(retired)
        .await
        .expect("reclaim main trash");
    assert!(host.events().contains(&"idempotent-reclaim".to_owned()));
}

#[tokio::test]
async fn retire_reclaim_stats_and_gc_cross_only_the_blocking_lane() {
    let host = FakeHost::default();
    let current = workspace("raven", 4);
    host.seed(&current);
    let lane = CountingLane::default();
    let substrate = substrate(host.clone(), lane.clone());
    let plan = substrate.plan_retire(&current).expect("retire plan");

    let retired = substrate.execute_retire(plan).await.expect("retire");
    assert_eq!(retired.resulting_revision(), Revision::new(5));
    assert!(
        host.paths().iter().any(|path| {
            path.components()
                .any(|component| component.as_os_str() == ".trash")
        }),
        "retirement must publish into sessions/.trash"
    );
    substrate.reclaim(retired).await.expect("reclaim");
    assert_eq!(
        substrate.stats(&current).await.expect("stats"),
        SubstrateStats {
            logical_bytes: 4096,
            allocated_bytes: 1024,
        }
    );
    let gc_plan = substrate
        .preview_gc(&repo(), StorageGcOptions::default())
        .await
        .expect("GC preview");
    assert_eq!(
        substrate
            .execute_gc(gc_plan)
            .await
            .expect("GC execution")
            .reclaimed,
        1
    );
    assert!(lane.count() >= 5);
    let events = host.events();
    assert!(events.contains(&"atomic-retire-to-trash".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(events.contains(&"preview-gc".to_owned()));
    assert!(events.contains(&"execute-gc".to_owned()));
}

#[tokio::test]
async fn aborting_adopt_callback_detaches_and_reclaims_the_stage() {
    let host = FakeHost::default();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate.plan_adopt(adopt_request()).expect("adopt plan");
    let entered = Arc::new(AtomicBool::new(false));
    let callback_entered = Arc::clone(&entered);
    let task = tokio::spawn(async move {
        substrate
            .execute_adopt_staged(plan, Alongside::none(), move |_| async move {
                callback_entered.store(true, Ordering::SeqCst);
                std::future::pending::<Result<(), &'static str>>().await
            })
            .await
    });

    abort_at_callback(task, entered).await;

    assert_no_orphan_stage(&host);
    assert!(host.list(&repo()).expect("post-cancel listing").is_empty());
    let events = host.events();
    assert!(events.contains(&"detach:Release".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"activate-pending".to_owned()));
}

#[tokio::test]
async fn aborting_create_and_fork_callbacks_preserves_each_pending_clone_for_resume() {
    for fork in [false, true] {
        let host = FakeHost::default();
        let source = workspace("main", 5);
        host.seed(&source);
        let initial_substrate = substrate(host.clone(), CountingLane::default());
        let destination_name = WorkspaceName::session(if fork {
            "cancelled-fork"
        } else {
            "cancelled-create"
        })
        .expect("destination");
        let operation_identity = identity();
        let destination = Destination {
            repo: repo(),
            name: destination_name.clone(),
            topology_revision: Revision::new(8),
            identity: operation_identity.clone(),
        };
        let entered = Arc::new(AtomicBool::new(false));
        let callback_entered = Arc::clone(&entered);
        let task = if fork {
            let plan = initial_substrate
                .plan_fork(&source, destination)
                .expect("fork plan");
            tokio::spawn(async move {
                initial_substrate
                    .execute_fork_staged(plan, Beside::none(), move |_, ()| async move {
                        callback_entered.store(true, Ordering::SeqCst);
                        std::future::pending::<Result<(), &'static str>>().await
                    })
                    .await
            })
        } else {
            let plan = initial_substrate
                .plan_create(&source, destination)
                .expect("create plan");
            tokio::spawn(async move {
                initial_substrate
                    .execute_create_staged(plan, Beside::none(), move |_, ()| async move {
                        callback_entered.store(true, Ordering::SeqCst);
                        std::future::pending::<Result<(), &'static str>>().await
                    })
                    .await
            })
        };

        abort_at_callback(task, entered).await;

        assert_eq!(
            host.mounted_paths_now().len(),
            1,
            "the crash-left canonical mount must survive for resume"
        );
        assert_eq!(host.pending_clone_count(), 1);
        let events = host.events();
        assert!(!events.iter().any(|event| event.starts_with("detach:")));
        assert!(!events.contains(&"idempotent-reclaim".to_owned()));

        let retry_substrate = substrate(host.clone(), CountingLane::default());
        let destination = Destination {
            repo: repo(),
            name: destination_name.clone(),
            topology_revision: Revision::new(8),
            identity: operation_identity,
        };
        let resumed = if fork {
            let plan = retry_substrate
                .plan_fork(&source, destination)
                .expect("resume fork plan");
            retry_substrate
                .execute_fork_staged(plan, Beside::none(), |stage, ()| async move {
                    assert!(stage.resuming);
                    Ok::<(), &'static str>(())
                })
                .await
                .expect("resume fork")
        } else {
            let plan = retry_substrate
                .plan_create(&source, destination)
                .expect("resume create plan");
            retry_substrate
                .execute_create_staged(plan, Beside::none(), |stage, ()| async move {
                    assert!(stage.resuming);
                    Ok::<(), &'static str>(())
                })
                .await
                .expect("resume create")
        };
        assert_eq!(resumed.workspace.name(), &destination_name);
        assert_eq!(host.pending_clone_count(), 0);
    }
}

/// A fresh clone whose initializer refuses is reclaimed when nothing the initializer does
/// reaches past the clone, so the retry clones the source again rather than resuming a copy of
/// the state that refused. A git-worktree clone's initializer registers it in main's
/// repository, so its refused clone stays pending for the retry to resume and finish.
#[tokio::test]
async fn a_refused_initializer_reclaims_a_fresh_clone_unless_it_reaches_main() {
    for git_worktree in [false, true] {
        let host = FakeHost::default();
        let source = workspace("main", 5);
        host.seed(&source);
        let substrate = substrate(host.clone(), CountingLane::default());
        let destination_name = WorkspaceName::session("refused").expect("destination");
        let operation_identity = OperationIdentity {
            git_worktree,
            ..identity()
        };
        let destination = || Destination {
            repo: repo(),
            name: destination_name.clone(),
            topology_revision: Revision::new(8),
            identity: operation_identity.clone(),
        };
        let refusal = substrate
            .execute_fork_staged(
                substrate
                    .plan_fork(&source, destination())
                    .expect("fork plan"),
                Beside::none(),
                |stage, ()| async move {
                    assert!(!stage.resuming);
                    Err::<(), _>("escaping link")
                },
            )
            .await;
        let pending = host.pending_clone_count();
        let events = host.events();
        if git_worktree {
            assert!(matches!(
                refusal,
                Err(StagedExecutionError::Initializer("escaping link"))
            ));
            assert_eq!(pending, 1, "the registration's clone stays to resume");
            assert_eq!(host.mounted_paths_now().len(), 1);
            assert!(!events.contains(&"idempotent-reclaim".to_owned()));
        } else {
            assert!(matches!(
                refusal,
                Err(StagedExecutionError::InitializerReclaimed("escaping link"))
            ));
            assert_eq!(pending, 0, "the refused clone is reclaimed");
            assert!(host.mounted_paths_now().is_empty());
            assert!(events.contains(&"detach:Release".to_owned()));
            assert!(events.contains(&"idempotent-reclaim".to_owned()));
        }

        host.clear_events();
        substrate
            .execute_fork_staged(
                substrate
                    .plan_fork(&source, destination())
                    .expect("retry plan"),
                Beside::none(),
                move |stage, ()| async move {
                    assert_eq!(stage.resuming, git_worktree);
                    Ok::<(), &'static str>(())
                },
            )
            .await
            .expect("retry");
        assert_eq!(
            host.events().iter().any(|event| event.as_str() == "clone"),
            !git_worktree,
            "only a reclaimed clone is cloned again"
        );
        assert_eq!(host.pending_clone_count(), 0);
    }
}

#[tokio::test]
async fn adoption_rollback_detaches_before_atomic_restore_and_never_copies() {
    let host = FakeHost::default();
    let main = workspace("main", 1);
    host.seed(&main);
    let substrate = substrate(host.clone(), CountingLane::default());

    substrate
        .restore_adopted_checkout(&main, Path::new("/project.pre-cowshed"))
        .await
        .expect("restore retained checkout");

    let events = host.events();
    let detach = events
        .iter()
        .position(|event| event == "detach-mounted:WhenIdle")
        .expect("detach event");
    let restore = events
        .iter()
        .position(|event| event == "atomic-restore-checkout")
        .expect("atomic restore event");
    assert!(detach < restore);
    assert!(
        !events.iter().any(|event| event == "copy-tree"),
        "rollback must never recursively copy"
    );
    assert!(
        host.paths()
            .windows(2)
            .any(|paths| paths == [Path::new("/project"), Path::new("/project.pre-cowshed")])
    );

    let session = workspace("raven", 2);
    let before = host.events();
    let error = substrate
        .restore_adopted_checkout(&session, Path::new("/project.pre-cowshed"))
        .await
        .expect_err("session is not an adoption rollback target");
    assert!(matches!(error, ApfsStorageError::InvalidPlan(_)));
    assert_eq!(host.events(), before, "invalid target mutates nothing");
}

proptest! {
    #[test]
    fn clone_create_and_fork_each_clone_one_image(
        operations in prop::collection::vec(0_u8..=1, 1..20),
    ) {
        let count = operations.len();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let host = FakeHost::default();
            let source = workspace("main", 1);
            host.seed(&source);
            let substrate = substrate(host.clone(), CountingLane::default());
            for (index, operation) in operations.into_iter().enumerate() {
                match operation {
                    0 => {
                        let destination = WorkspaceName::session(format!("create-{index}"))
                            .expect("destination");
                        let plan = substrate.plan_create(
                            &source,
                            Destination {
                                repo: repo(),
                                name: destination,
                                topology_revision: Revision::new(index as u64 + 2),
                                identity: identity(),
                            },
                        ).expect("create plan");
                        substrate
                            .execute_create_staged(plan, Beside::none(), |_, ()| async {
                                Ok::<(), &'static str>(())
                            })
                            .await
                            .expect("create");
                    }
                    _ => {
                        let destination = WorkspaceName::session(format!("fork-{index}"))
                            .expect("destination");
                        let plan = substrate.plan_fork(
                            &source,
                            Destination {
                                repo: repo(),
                                name: destination,
                                topology_revision: Revision::new(index as u64 + 2),
                                identity: identity(),
                            },
                        ).expect("fork plan");
                        substrate
                            .execute_fork_staged(plan, Beside::none(), |_, ()| async {
                                Ok::<(), &'static str>(())
                            })
                            .await
                            .expect("fork");
                    }
                }
            }
            // The fake refuses a clone between paths that are not `<name>.dmg`, so each operation
            // above succeeding is the proof that it cloned an image to an image.
            let clones = host.events().iter().filter(|event| event.as_str() == "clone").count();
            prop_assert_eq!(clones, count);
            Ok(())
        })?;
    }
}

/// Create/fork's normal path mounts the canonical image once. There is no staging detach,
/// publication rename, or second attach for Disk Arbitration to serialize.
#[tokio::test]
async fn create_uses_one_canonical_attach_and_mount_without_detach_churn() {
    let host = FakeHost::default();
    let source = workspace("main", 1);
    host.seed(&source);
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate
        .plan_create(
            &source,
            Destination {
                repo: repo(),
                name: WorkspaceName::new("hedge").expect("destination"),
                topology_revision: Revision::new(2),
                identity: identity(),
            },
        )
        .expect("create plan");
    substrate
        .execute_create_staged(plan, Beside::none(), |stage, ()| async move {
            assert!(!stage.resuming);
            assert!(
                !stage
                    .mount_point
                    .components()
                    .any(|component| component.as_os_str() == ".staging")
            );
            Ok::<(), &'static str>(())
        })
        .await
        .expect("create");
    let events = host.events();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.as_str() == "attach-mounted+checked")
            .count(),
        1,
        "one canonical attachment, got {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.as_str() == "mount")
            .count(),
        1,
        "one canonical mount, got {events:?}"
    );
    assert!(!events.iter().any(|event| event.starts_with("detach:")));
    let activate = events
        .iter()
        .position(|event| event == "activate-pending")
        .expect("activation fence");
    let retain = events
        .iter()
        .position(|event| event == "retain-mounted")
        .expect("mount retention");
    assert!(
        activate < retain,
        "activation precedes retention: {events:?}"
    );
}

/// A crash-left PendingFence supplies the original incarnation and identity. The retry must not
/// clone or rewrite pending metadata; it remounts, re-enters the initializer with explicit resume
/// authority, then activates exactly that workspace.
#[tokio::test]
async fn pending_canonical_clone_resumes_without_reclone_and_activates_after_init() {
    let host = FakeHost::default();
    let source = LifecycleWorkspace::new(
        repo(),
        WorkspaceName::main(),
        incarnation(5),
        Revision::new(5),
        Revision::new(10),
        WorkspaceRole::Main,
    )
    .expect("source");
    host.seed(&source);
    let destination = WorkspaceName::new("hedge").expect("destination");
    let resumed = LifecycleWorkspace::new(
        repo(),
        destination.clone(),
        incarnation(9),
        Revision::new(6),
        Revision::new(11),
        WorkspaceRole::Workspace,
    )
    .expect("resumed workspace");
    let original_identity = identity();
    host.resume_clone_from(
        resumed.clone(),
        original_identity.clone(),
        "/store/acme--widget/sessions/hedge.dmg",
    );
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate
        .plan_create(
            &source,
            Destination {
                repo: repo(),
                name: destination,
                topology_revision: Revision::new(11),
                identity: original_identity,
            },
        )
        .expect("resume plan");
    let callback_seen = Arc::new(AtomicBool::new(false));
    let seen = Arc::clone(&callback_seen);
    let receipt = substrate
        .execute_create_staged(plan, Beside::none(), move |stage, ()| async move {
            assert!(stage.resuming);
            assert_eq!(stage.workspace, resumed);
            seen.store(true, Ordering::SeqCst);
            Ok::<(), &'static str>(())
        })
        .await
        .expect("resume pending clone");
    assert!(callback_seen.load(Ordering::SeqCst));
    assert_eq!(receipt.workspace.incarnation(), &incarnation(9));
    assert_eq!(receipt.workspace.revision(), Revision::new(6));
    assert_eq!(receipt.workspace.topology_revision(), Revision::new(11));
    let events = host.events();
    assert!(
        !events.iter().any(|event| event.as_str() == "clone"),
        "resume must not clone over the pending payload: {events:?}"
    );
    assert!(
        !events
            .iter()
            .any(|event| event == "atomic-metadata+parent-fsync:FreshPendingFence"),
        "resume must preserve the original metadata identity: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.as_str() == "activate-pending")
            .count(),
        1
    );
}

#[tokio::test]
async fn pending_clone_retirement_inspects_before_trash_without_activation() {
    let host = FakeHost::default();
    let name = WorkspaceName::new("unfinished").expect("destination");
    let pending = LifecycleWorkspace::new(
        repo(),
        name.clone(),
        incarnation(9),
        Revision::new(6),
        Revision::new(6),
        WorkspaceRole::Workspace,
    )
    .expect("pending");
    let mut original = identity();
    original.grants.revision = 6;
    host.resume_clone_from(
        pending.clone(),
        original.clone(),
        "/store/acme/widget/sessions/unfinished.dmg",
    );
    let substrate = substrate(host.clone(), CountingLane::default());
    let metadata = cowshed_core::metadata::DetachedWorkspaceMetadata {
        version: cowshed_core::metadata::SIDECAR_VERSION,
        repo_id: repo(),
        workspace: name,
        workspace_incarnation: pending.incarnation().clone(),
        platform: cowshed_core::metadata::Platform::Macos,
        publication_state: cowshed_core::metadata::PublicationState::PendingFence,
        updated_at: original.created_at.clone(),
        grants: original.grants.clone(),
        info_snapshot: WorkspaceInfoSnapshot {
            project_root: original.project_root.clone(),
            role: WorkspaceRole::Workspace,
            base_commit: original.base_commit.clone(),
            created_at: original.created_at.clone(),
            branch: original.branch.clone(),
            forked_from: original.forked_from.clone(),
            captured_at: original.created_at.clone(),
            stale: true,
            git_worktree: false,
        },
    };
    let outcome = substrate
        .defragment_pending_clone(metadata.clone())
        .await
        .expect("rewrite a pending clone without publishing or discarding it");
    assert_eq!(outcome.extents, ExtentCount::new(1));
    {
        let state = host.state.lock().expect("state");
        assert_eq!(
            state.resumable_clone.as_ref().expect("retained").workspace,
            pending
        );
        assert!(
            state.published.is_empty(),
            "defrag grants no publication authority"
        );
    }
    let mut stale = metadata.clone();
    stale.workspace_incarnation = incarnation(8);
    host.clear_events();
    substrate
        .defragment_pending_clone(stale)
        .await
        .expect_err("a stale pending owner cannot rewrite another generation");
    assert!(
        !host
            .events()
            .iter()
            .any(|event| event.starts_with("defragment:"))
    );
    host.clear_events();
    let refusal = substrate
        .execute_pending_clone_retirement(metadata.clone(), original.clone(), |stage| async move {
            assert!(stage.resuming);
            Err::<(), _>(stage.mount_point)
        })
        .await;
    let Err(StagedExecutionError::Initializer(mount)) = refusal else {
        panic!("a Git safety refusal keeps the pending clone");
    };
    assert!(
        !host
            .events()
            .iter()
            .any(|event| event == "atomic-retire-to-trash")
    );
    // The refusal changed nothing, so the image it attached to inspect is detached again.
    assert!(
        host.mounted_paths_now().is_empty(),
        "a refused retirement leaves no mount it made"
    );
    assert!(host.events().iter().any(|event| event == "detach:Release"));

    // Found mounted, a refused retirement keeps that mount and detaches nothing.
    host.mount(&FakeAttachment, &mount, MountAccess::ReadWrite, false)
        .expect("mount as an earlier verb left it");
    host.clear_events();
    let refusal = substrate
        .execute_pending_clone_retirement(metadata.clone(), original.clone(), |_| async move {
            Err::<(), _>("unlanded commits")
        })
        .await;
    assert!(matches!(
        refusal,
        Err(StagedExecutionError::Initializer("unlanded commits"))
    ));
    assert_eq!(host.mounted_paths_now(), BTreeSet::from([mount.clone()]));
    let events = host.events();
    assert!(events.iter().any(|event| event == "reuse-mount"));
    assert!(
        !events
            .iter()
            .any(|event| event.starts_with("detach") || event == "unmount"),
        "{events:?}"
    );
    let entered = Arc::new(AtomicBool::new(false));
    let callback_entered = Arc::clone(&entered);
    let cancelling_substrate = substrate.clone();
    let cancelling_metadata = metadata.clone();
    let cancelling_identity = original.clone();
    let cancelled = tokio::spawn(async move {
        cancelling_substrate
            .execute_pending_clone_retirement(
                cancelling_metadata,
                cancelling_identity,
                |_| async move {
                    callback_entered.store(true, Ordering::SeqCst);
                    std::future::pending::<Result<(), &'static str>>().await
                },
            )
            .await
    });
    abort_at_callback(cancelled, entered).await;
    assert_eq!(host.mounted_paths_now(), BTreeSet::from([mount.clone()]));
    assert!(
        !host
            .events()
            .iter()
            .any(|event| event == "atomic-retire-to-trash"
                || event == "physical-release"
                || event == "idempotent-reclaim"),
        "cancelled inspection must preserve the pending source"
    );
    host.clear_events();
    let (retired, proof) = substrate
        .execute_pending_clone_retirement(metadata, original, |stage| async move {
            assert_eq!(stage.workspace, pending);
            Ok::<_, &'static str>("landed")
        })
        .await
        .expect("retire verified pending clone");
    assert_eq!(proof, "landed");
    assert_eq!(retired.workspace().revision(), Revision::new(6));
    let events = host.events();
    assert!(events.iter().any(|event| event == "lock:1"));
    assert!(!events.iter().any(|event| event == "lock:2"));
    assert!(events.iter().any(|event| event == "physical-release"));
    assert!(
        !events.iter().any(|event| event == "atomic-retire-to-trash"),
        "an unfinished clone's retirement keeps no recovery image"
    );
    assert!(!events.iter().any(|event| event == "activate-pending"));
    assert!(!events.iter().any(|event| event.as_str() == "clone"));
}
