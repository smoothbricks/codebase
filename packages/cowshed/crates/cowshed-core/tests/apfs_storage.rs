use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cowshed_core::apfs::{CreateImageRequest, DetachIntent, MountAccess};
use cowshed_core::metadata::{
    GrantSet, IMAGE_EXTENSION, ImageCapacity, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, PortBlock,
    WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceName, WorkspaceRole, is_image_path,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::CheckpointLabel;
use cowshed_core::storage::apfs::{
    AdoptExecutionError, ApfsBlockingLane, ApfsExecutionHost, ApfsStorageError, ApfsSubstrate,
    ApfsSubstrateConfig, DEFAULT_IMAGE_CAPACITY, IncarnationSource, LockMode, MarkerExpectation,
    MetadataPolicy, PendingAdoption, PublicationError, RestoreStage, ResumableClone,
    RetireExecutionError, volume_key,
};
use cowshed_core::storage::lifecycle::{
    AdoptRequest, CheckpointFact, DefragmentOutcome, Destination, ExtentCount, KernelMountFact,
    LifecycleFact, LifecyclePlanner, LifecycleWorkspace, MountIntent, MountState,
    OperationIdentity, Pin, ResizeOutcome, RestoreMode, RetiredRef, Revision, StorageFact,
    StorageGcPlan, StorageGcReport, Substrate, SubstrateStats,
};
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
    checkpoints: Vec<CheckpointFact>,
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
    fail_metadata_once: Arc<AtomicBool>,
    fail_restored_metadata_once: Arc<AtomicBool>,
    fail_reclaim_once: Arc<AtomicBool>,
    fail_credentials_once: Arc<AtomicBool>,
    mounted_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
}

impl Default for FakeHost {
    fn default() -> Self {
        Self {
            state: Arc::default(),
            marker_validations_before_failure: Arc::new(AtomicUsize::new(usize::MAX)),
            fail_metadata_once: Arc::default(),
            fail_restored_metadata_once: Arc::default(),
            fail_credentials_once: Arc::default(),
            fail_reclaim_once: Arc::default(),
            mounted_paths: Arc::default(),
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

    fn fail_marker_after(&self, successful_validations: usize) {
        self.marker_validations_before_failure
            .store(successful_validations, Ordering::SeqCst);
    }

    fn fail_next_metadata(&self) {
        self.fail_metadata_once.store(true, Ordering::SeqCst);
    }

    fn fail_next_credentials(&self) {
        self.fail_credentials_once.store(true, Ordering::SeqCst);
    }
    fn fail_next_restored_metadata(&self) {
        self.fail_restored_metadata_once
            .store(true, Ordering::SeqCst);
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
    fn lock_images(
        &self,
        paths: &[PathBuf],
        _: LockMode,
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
                LifecycleFact::Checkpoint {
                    repo,
                    workspace,
                    label,
                    revision,
                } => LifecycleFact::Checkpoint {
                    repo: repo.clone(),
                    workspace: workspace.clone(),
                    label: label.clone(),
                    revision: *revision,
                },
            })
            .collect())
    }

    fn create_attached(
        &self,
        request: &CreateImageRequest,
        image: &Path,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        if !is_image_path(image)
            || !request
                .staged_stem
                .with_extension(IMAGE_EXTENSION)
                .components()
                .any(|component| component.as_os_str() == ".staging")
        {
            return Err(ApfsStorageError::Host(
                "the blank is staged and the attached image is canonical".to_owned(),
            ));
        }
        self.record_path(image);
        self.record(format!("create-attached+fsck:{}", request.capacity));
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
    fn write_first(&self, image: &Path) -> Result<(), ApfsStorageError> {
        if !is_image_path(image) {
            return Err(ApfsStorageError::Host(
                "first write into a path that is not an image".to_owned(),
            ));
        }
        self.record("first-write");
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

    fn copy_tree(&self, _: &Path, _: &Path) -> Result<(), ApfsStorageError> {
        self.record("copy-until-quiescent");
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

    fn rename_volume(&self, _: &Path, volume_key: &str) -> Result<(), ApfsStorageError> {
        self.record(format!("rename-volume:{volume_key}"));
        Ok(())
    }

    fn mint_workspace_credentials(
        &self,
        _: &LifecycleWorkspace,
        _: &Path,
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
        if self.fail_metadata_once.swap(false, Ordering::SeqCst) {
            return Err(ApfsStorageError::Host(
                "injected metadata failure".to_owned(),
            ));
        }
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

    fn publish_checkpoint_fact(
        &self,
        _: &Path,
        label: &CheckpointLabel,
        revision: Revision,
        pin: Pin,
    ) -> Result<(), ApfsStorageError> {
        let workspace = self
            .state
            .lock()
            .expect("fake state")
            .published
            .values()
            .find(|fact| fact.workspace.name().is_main())
            .expect("published workspace")
            .workspace
            .clone();
        self.state
            .lock()
            .expect("fake state")
            .checkpoints
            .push(CheckpointFact {
                repo: workspace.repo().clone(),
                workspace: workspace.name().clone(),
                label: label.clone(),
                revision,
                pin,
            });
        self.record(format!("checkpoint-fact:{pin:?}"));
        Ok(())
    }

    fn restore_swap(
        &self,
        staged: &Path,
        canonical: &Path,
        undo: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.record("atomic-restore-swap+undo");
        self.record_path(staged);
        self.record_path(canonical);
        self.record_path(undo);
        Ok(())
    }

    fn publish_restored_metadata(
        &self,
        _: &Path,
        canonical: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        source_image: &Path,
        replaced_incarnation: &WorkspaceIncarnation,
    ) -> Result<cowshed_core::storage::apfs::PendingPublicationFact, ApfsStorageError> {
        self.record("publish-restored-metadata-after-mount");
        if self
            .fail_restored_metadata_once
            .swap(false, Ordering::SeqCst)
        {
            return Err(ApfsStorageError::Host(
                "injected restored metadata failure".to_owned(),
            ));
        }
        self.publish_metadata(
            canonical,
            workspace,
            revision,
            MetadataPolicy::PendingFence,
            None,
            Some(source_image),
        )?;
        Ok(cowshed_core::storage::apfs::PendingPublicationFact {
            workspace: workspace.clone(),
            image: canonical.to_owned(),
            mount_point: if workspace.name().is_main() {
                PathBuf::from("/project")
            } else {
                PathBuf::from(format!(
                    "/store/mnt/{}/{}",
                    workspace.repo(),
                    workspace.name()
                ))
            },
            source_checkpoint: source_image
                .file_stem()
                .and_then(|stem| stem.to_str())
                .expect("checkpoint name")
                .to_owned(),
            source_incarnation: WorkspaceIncarnation::new("ffffffffffffffffffffffffffffffff")
                .expect("source incarnation"),
            replaced_incarnation: replaced_incarnation.clone(),
            destination_incarnation: workspace.incarnation().clone(),
        })
    }

    fn activate_restored_metadata(&self, canonical: &Path) -> Result<(), ApfsStorageError> {
        self.record("activate-restored-metadata");
        let mut state = self.state.lock().expect("fake state");
        let fact = state
            .pending
            .remove(canonical)
            .ok_or_else(|| ApfsStorageError::PendingPublication(canonical.to_owned()))?;
        let key = (fact.workspace.repo().clone(), fact.workspace.name().clone());
        state.published.insert(key, fact);
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

    fn rollback_restore(&self, _: &Path, _: &Path, _: &Path) -> Result<(), ApfsStorageError> {
        self.record("rollback-before-publication");
        Ok(())
    }

    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError> {
        self.record("atomic-retire-to-trash");
        self.record_path(canonical);
        self.record_path(trash);
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
    fn reclaim_unrecorded_retired(
        &self,
        _: &ApfsSubstrateConfig,
        _: &RepoId,
        _: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.record("reclaim-unrecorded");
        Ok(())
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

    fn pending_publications(
        &self,
        repo: &RepoId,
    ) -> Result<Vec<cowshed_core::storage::apfs::PendingPublicationFact>, ApfsStorageError> {
        Ok(self
            .state
            .lock()
            .expect("fake state")
            .pending
            .iter()
            .filter(|(_, fact)| fact.workspace.repo() == repo)
            .map(
                |(image, fact)| cowshed_core::storage::apfs::PendingPublicationFact {
                    workspace: fact.workspace.clone(),
                    image: image.clone(),
                    mount_point: if fact.workspace.name().is_main() {
                        PathBuf::from("/project")
                    } else {
                        PathBuf::from(format!(
                            "/store/mnt/{}/{}",
                            fact.workspace.repo(),
                            fact.workspace.name()
                        ))
                    },
                    source_checkpoint: "checkpoint-source".to_owned(),
                    source_incarnation: WorkspaceIncarnation::new(
                        "ffffffffffffffffffffffffffffffff",
                    )
                    .expect("source incarnation"),
                    replaced_incarnation: WorkspaceIncarnation::new(
                        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    )
                    .expect("replaced incarnation"),
                    destination_incarnation: fact.workspace.incarnation().clone(),
                },
            )
            .collect())
    }

    fn mounts(&self, repo: &RepoId) -> Result<Vec<KernelMountFact>, ApfsStorageError> {
        Ok(self
            .state
            .lock()
            .expect("fake state")
            .mounted
            .iter()
            .filter(|((mounted_repo, _), _)| mounted_repo == repo)
            .map(|(_, fact)| fact.clone())
            .collect())
    }

    fn checkpoints(&self, repo: &RepoId) -> Result<Vec<CheckpointFact>, ApfsStorageError> {
        Ok(self
            .state
            .lock()
            .expect("fake state")
            .checkpoints
            .iter()
            .filter(|checkpoint| &checkpoint.repo == repo)
            .cloned()
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
            checkpoint_count: 3,
            checkpoint_bytes: 3072,
            pinned_checkpoint_bytes: 2048,
        })
    }

    fn preview_gc(
        &self,
        _: &ApfsSubstrateConfig,
        repo: &RepoId,
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
            retained_pinned: 1,
            retained_active: 0,
            retained_recent: 0,
            freed_bytes: 1024,
            deferred: Vec::new(),
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
        pre_cowshed_checkout: PathBuf::from("/project.pre-cowshed"),
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
        ApfsSubstrateConfig::new("/store", "/store/caches", "/project"),
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
    let receipt = substrate
        .execute_adopt_staged(plan, move |stage| async move {
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
            "create-attached+fsck:100g",
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
            .filter(|event| event.starts_with("create-attached")
                || event.starts_with("attach-no-mount"))
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
        PathBuf::from("/store/acme/widget/main.asif"),
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
    assert_eq!(
        substrate.caches_root().await.expect("caches"),
        PathBuf::from("/store/caches")
    );
}

fn pending_adoption(identity: &OperationIdentity) -> PendingAdoption {
    PendingAdoption {
        incarnation: incarnation(7),
        image: PathBuf::from("/store/acme/widget/main.asif"),
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

    let receipt = substrate
        .execute_adopt_staged(plan, |stage| async move {
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

    let receipt = substrate
        .execute_adopt_staged(plan, |_| async { Ok::<(), &'static str>(()) })
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
        .position(|event| event.starts_with("create-attached"))
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

    let receipt = substrate
        .execute_adopt_staged(plan, |_| async { Ok::<(), &'static str>(()) })
        .await
        .expect("fresh adopt");

    assert_eq!(receipt.workspace.incarnation(), &incarnation(0));
    let events = host.events();
    let order = [
        "resume-unformatted",
        "discard-pending",
        "create-attached+fsck:100g",
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
        .execute_adopt_staged(plan, |_| async { Ok::<(), &'static str>(()) })
        .await
        .expect_err("credential mint failure");

    let events = host.events();
    assert!(events.contains(&"detach:Release".to_owned()));
    assert!(!events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"activate-pending".to_owned()));
    assert_eq!(host.pending_adopt(), Some(pending));
    assert!(host.list(&repo()).expect("listing").is_empty());
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
        .execute_adopt_staged(plan, |_stage| async { Ok::<(), &'static str>(()) })
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
    let pre_cowshed = PathBuf::from("/project.pre-cowshed");

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
            "attach-no-mount+fsck",
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
        .execute_adopt_staged(plan, move |stage| async move {
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
        .execute_adopt_staged(plan, |_| async { Ok::<(), &'static str>(()) })
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
        .execute_adopt_staged(plan, |_| async { Err("tool wiring rejected") })
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
            ApfsSubstrateConfig::new("/store", "/store/caches", checkout_path),
            host.clone(),
            lane.clone(),
            FixedIncarnations::default(),
        );
        let mut request = adopt_request();
        request.identity.project_root = PathBuf::from(project_root);
        let plan = substrate.plan_adopt(request).expect("adopt plan");

        let error = substrate
            .execute_adopt_staged(plan, |_| async { Ok::<(), &'static str>(()) })
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
    let workspace = substrate
        .execute_adopt_staged(plan, |_| async { Ok::<(), &'static str>(()) })
        .await
        .expect("adopt")
        .workspace;
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
            .any(|event| event.starts_with("attach-no-mount")),
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
        .execute_adopt_staged(plan, |_| async { Ok::<(), &'static str>(()) })
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
async fn restore_staging_failure_leaves_the_old_workspace_untouched() {
    let host = FakeHost::default();
    let current = workspace("raven", 7);
    host.seed(&current);
    let substrate = substrate(host.clone(), CountingLane::default());
    let checkpoint = cowshed_core::storage::lifecycle::CheckpointRef::new(
        current.clone(),
        CheckpointLabel::new("ready").expect("label"),
        Revision::new(8),
        true,
    );
    let plan = substrate
        .plan_restore(
            &current,
            &checkpoint,
            cowshed_core::storage::lifecycle::RestoreMode::Replace,
            identity(),
        )
        .expect("restore plan");
    host.fail_next_metadata();
    host.clear_events();

    substrate
        .execute_restore_staged(
            plan,
            |_| async { Ok::<(), &'static str>(()) },
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect_err("staging metadata failure");

    let events = host.events();
    assert!(!events.contains(&"atomic-restore-swap+undo".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"detach-mounted:Release".to_owned()));
    assert!(!events.contains(&"attach-no-mount+fsck".to_owned()));
    assert_eq!(
        host.list(&repo()).expect("active workspace"),
        vec![StorageFact {
            workspace: current,
            volume_key: volume_key(&repo(), &WorkspaceName::new("raven").expect("workspace")),
        }]
    );
}

#[tokio::test]
async fn restore_post_swap_marker_failure_rolls_back_and_remounts_old_image() {
    let host = FakeHost::default();
    let current = workspace("raven", 7);
    host.seed(&current);
    let substrate = substrate(host.clone(), CountingLane::default());
    let checkpoint = cowshed_core::storage::lifecycle::CheckpointRef::new(
        current.clone(),
        CheckpointLabel::new("ready").expect("label"),
        Revision::new(8),
        true,
    );
    let plan = substrate
        .plan_restore(
            &current,
            &checkpoint,
            cowshed_core::storage::lifecycle::RestoreMode::Replace,
            identity(),
        )
        .expect("restore plan");
    host.fail_marker_after(2);
    host.clear_events();

    substrate
        .execute_restore_staged(
            plan,
            |_| async { Ok::<(), &'static str>(()) },
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect_err("canonical marker failure");

    let events = host.events();
    let swap = events
        .iter()
        .position(|event| event == "atomic-restore-swap+undo")
        .expect("swap");
    let rollback = events
        .iter()
        .position(|event| event == "rollback-before-publication")
        .expect("rollback");
    assert!(swap < rollback);
    assert!(
        events[rollback + 1..]
            .iter()
            .any(|event| event == "attach-no-mount+fsck")
    );
    assert_eq!(events.last().map(String::as_str), Some("retain-mounted"));
}

#[tokio::test]
async fn restore_metadata_publication_failure_rolls_back_after_verified_mount() {
    let host = FakeHost::default();
    let current = workspace("raven", 7);
    host.seed(&current);
    let substrate = substrate(host.clone(), CountingLane::default());
    let checkpoint = cowshed_core::storage::lifecycle::CheckpointRef::new(
        current.clone(),
        CheckpointLabel::new("ready").expect("label"),
        Revision::new(8),
        true,
    );
    let plan = substrate
        .plan_restore(&current, &checkpoint, RestoreMode::Replace, identity())
        .expect("restore plan");
    host.fail_next_restored_metadata();
    host.clear_events();

    substrate
        .execute_restore_staged(
            plan,
            |_| async { Ok::<(), &'static str>(()) },
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect_err("restored metadata publication failure");

    let events = host.events();
    let publication = events
        .iter()
        .position(|event| event == "publish-restored-metadata-after-mount")
        .expect("publication attempt");
    let marker = events[..publication]
        .iter()
        .rposition(|event| event == "validate-marker")
        .expect("canonical marker");
    let rollback = events
        .iter()
        .position(|event| event == "rollback-before-publication")
        .expect("rollback");
    assert!(marker < publication && publication < rollback);
    assert_eq!(events.last().map(String::as_str), Some("retain-mounted"));
}

#[tokio::test]
async fn lifecycle_receipts_preserve_exact_revisions_topology_and_checkpoint_pin() {
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
            |_| async { Ok::<(), &'static str>(()) },
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
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("fork");
    assert_eq!(forked.workspace.revision(), Revision::new(6));
    assert_eq!(forked.workspace.topology_revision(), Revision::new(11));

    let exact_label = CheckpointLabel::new("exact").expect("label");
    assert_eq!(exact_label.as_str(), "exact");
    assert_eq!(format!("{exact_label}"), "exact");
    let checkpoint = substrate
        .execute_checkpoint_staged(
            substrate
                .plan_checkpoint(&source, exact_label, Pin::Pinned)
                .expect("checkpoint plan"),
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("checkpoint");
    assert_eq!(checkpoint.revision(), Revision::new(6));
    assert!(checkpoint.pinned());

    let restored = substrate
        .execute_restore_staged(
            substrate
                .plan_restore(&source, &checkpoint, RestoreMode::Replace, identity())
                .expect("restore plan"),
            |_| async { Ok::<(), &'static str>(()) },
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("restore");
    assert_eq!(restored.previous_incarnation, *source.incarnation());
    assert_eq!(restored.workspace.revision(), Revision::new(6));
    assert_eq!(
        restored.workspace.topology_revision(),
        source.topology_revision()
    );
    assert_eq!(
        host.events()
            .iter()
            .filter(|event| event.as_str() == "mint-workspace-credentials")
            .count(),
        3,
        "create, fork, and replacement restore each mint one fresh authority"
    );
    let relabels: Vec<_> = host
        .events()
        .into_iter()
        .filter(|event| event.starts_with("rename-volume:"))
        .collect();
    assert_eq!(
        relabels,
        ["rename-volume:[cowshed] acme · widget — main"],
        "a replacement restore relabels its staging volume before publication; create and fork \
         leave the inherited label to the workspace's supervisor, off the provisioning path, \
         because relabelling waits in Disk Arbitration's host-wide queue"
    );

    let restore_events = host.events();
    let restore_swap = restore_events
        .iter()
        .position(|event| event == "atomic-restore-swap+undo")
        .expect("restore swap");
    let restore_marker = restore_events
        .iter()
        .rposition(|event| event == "validate-marker")
        .expect("canonical marker validation");
    let metadata_publication = restore_events
        .iter()
        .position(|event| event == "publish-restored-metadata-after-mount")
        .expect("restored metadata publication");
    assert!(
        restore_swap < restore_marker && restore_marker < metadata_publication,
        "replacement metadata must publish only after canonical mount and marker verification"
    );
    assert_eq!(
        host.mount_paths()
            .iter()
            .filter(|path| path
                .components()
                .any(|component| component.as_os_str() == ".staging"))
            .count(),
        1,
        "only replacement restore uses a hidden staging mount; create and fork mount canonical once"
    );
    let paths = host.paths();
    assert!(
        paths.iter().any(|path| path
            .components()
            .any(|component| component.as_os_str() == ".staging")),
        "replacement restore preparation must use a hidden staging mount"
    );
    assert!(
        paths.iter().any(|path| path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("pre-restore-"))),
        "restore must retain a pre-restore undo image"
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
    let restored_observation = listed
        .iter()
        .find(|observed| observed.workspace == restored.workspace)
        .expect("restored observation");
    assert_eq!(
        restored_observation.mount_state,
        MountState::Mounted { mount_id: 3 }
    );
    assert_eq!(restored_observation.checkpoints.len(), 1);
    assert_eq!(restored_observation.checkpoints[0].pin, Pin::Pinned);

    host.clear_events();
    substrate
        .unmount(&restored.workspace)
        .await
        .expect("unmount");
    assert_eq!(host.events(), ["lock:1", "detach-mounted:WhenIdle"]);
}

/// A new workspace's clone takes its own extent map as a step of its own, between the clone and
/// the attach: never on an attached image, whose driver could have just written the block being
/// rewritten. A checkpoint is never written, so its clone keeps sharing the map.
#[tokio::test]
async fn a_new_workspace_is_first_written_after_its_clone_and_before_its_attach() {
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
            .take(3)
            .collect()
    };

    host.clear_events();
    substrate
        .execute_create_staged(
            substrate
                .plan_create(&source, destination("created", 8))
                .expect("create plan"),
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("create");
    assert_eq!(
        around_the_clone(host.events()),
        ["clone", "first-write", "attach-no-mount+fsck"]
    );

    host.clear_events();
    substrate
        .execute_fork_staged(
            substrate
                .plan_fork(&source, destination("forked", 10))
                .expect("fork plan"),
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("fork");
    assert_eq!(
        around_the_clone(host.events()),
        ["clone", "first-write", "attach-no-mount+fsck"]
    );

    host.clear_events();
    substrate
        .execute_checkpoint_staged(
            substrate
                .plan_checkpoint(
                    &source,
                    CheckpointLabel::new("kept").expect("label"),
                    Pin::Pinned,
                )
                .expect("checkpoint plan"),
            |_| async { Ok::<(), &'static str>(()) },
        )
        .await
        .expect("checkpoint");
    let events = host.events();
    assert!(events.iter().any(|event| event == "clone"), "{events:?}");
    assert!(
        !events.iter().any(|event| event == "first-write"),
        "{events:?}"
    );
}

#[tokio::test]
async fn repeated_restore_preserves_checkpoint_replaced_and_destination_identities() {
    let host = FakeHost::default();
    let original = workspace("main", 7);
    host.seed(&original);
    let substrate = substrate(host.clone(), CountingLane::default());
    let label = CheckpointLabel::new("retained-origin").expect("checkpoint label");
    let checkpoint = cowshed_core::storage::lifecycle::CheckpointRef::new(
        original.clone(),
        label.clone(),
        Revision::new(8),
        true,
    );
    let checkpoint_source =
        WorkspaceIncarnation::new("ffffffffffffffffffffffffffffffff").expect("checkpoint source");

    let expected_source = checkpoint_source.clone();
    let expected_replaced = original.incarnation().clone();
    let first = substrate
        .execute_restore_staged(
            substrate
                .plan_restore(&original, &checkpoint, RestoreMode::Replace, identity())
                .expect("first restore plan"),
            |_| async { Ok::<(), &'static str>(()) },
            move |fence| async move {
                assert_eq!(fence.pending.source_checkpoint, "retained-origin");
                assert_eq!(fence.pending.source_incarnation, expected_source);
                assert_eq!(fence.pending.replaced_incarnation, expected_replaced);
                assert_eq!(
                    fence.pending.destination_incarnation,
                    *fence.pending.workspace.incarnation()
                );
                Ok::<(), &'static str>(())
            },
        )
        .await
        .expect("first restore");

    let repeated_checkpoint = cowshed_core::storage::lifecycle::CheckpointRef::new(
        first.workspace.clone(),
        label,
        Revision::new(8),
        true,
    );
    let first_destination = first.workspace.incarnation().clone();
    let expected_source = checkpoint_source.clone();
    let expected_replaced = first_destination.clone();
    let second = substrate
        .execute_restore_staged(
            substrate
                .plan_restore(
                    &first.workspace,
                    &repeated_checkpoint,
                    RestoreMode::Replace,
                    identity(),
                )
                .expect("second restore plan"),
            |_| async { Ok::<(), &'static str>(()) },
            move |fence| async move {
                assert_eq!(fence.pending.source_checkpoint, "retained-origin");
                assert_eq!(fence.pending.source_incarnation, expected_source);
                assert_eq!(fence.pending.replaced_incarnation, expected_replaced);
                assert_eq!(
                    fence.pending.destination_incarnation,
                    *fence.pending.workspace.incarnation()
                );
                Ok::<(), &'static str>(())
            },
        )
        .await
        .expect("second restore");

    assert_eq!(first.previous_incarnation, *original.incarnation());
    assert_eq!(second.previous_incarnation, first_destination);
    assert_ne!(checkpoint_source, second.previous_incarnation);
    assert_ne!(second.previous_incarnation, *second.workspace.incarnation());
}
#[tokio::test]
async fn staged_retire_fences_after_durable_undiscovery_and_preserves_trash_on_failure() {
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
                    .contains(&"atomic-retire-to-trash".to_owned()),
                "retirement must be durable before its lifecycle fence"
            );
            Err::<(), _>("injected commitment failure")
        })
        .await
        .expect_err("fence failure must preserve a recoverable retired reference");
    let RetireExecutionError::Fence { source, retired } = error else {
        panic!("expected forward-only retirement fence failure");
    };
    assert_eq!(source, "injected commitment failure");
    assert!(
        !host.events().contains(&"idempotent-reclaim".to_owned()),
        "fence failure must not reclaim unpublished retirement trash"
    );

    substrate.reclaim(retired).await.expect("recovery reclaim");
    assert!(host.events().contains(&"idempotent-reclaim".to_owned()));
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
            checkpoint_count: 3,
            checkpoint_bytes: 3072,
            pinned_checkpoint_bytes: 2048,
        }
    );
    let gc_plan = substrate.preview_gc(&repo()).await.expect("GC preview");
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
            .execute_adopt_staged(plan, move |_| async move {
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
                    .execute_fork_staged(plan, move |_| async move {
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
                    .execute_create_staged(plan, move |_| async move {
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
        assert_eq!(
            host.pending_publications(&repo())
                .expect("pending clone")
                .len(),
            1
        );
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
                .execute_fork_staged(plan, |stage| async move {
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
                .execute_create_staged(plan, |stage| async move {
                    assert!(stage.resuming);
                    Ok::<(), &'static str>(())
                })
                .await
                .expect("resume create")
        };
        assert_eq!(resumed.workspace.name(), &destination_name);
        assert!(
            host.pending_publications(&repo())
                .expect("pending clone after resume")
                .is_empty()
        );
    }
}

#[tokio::test]
async fn checkpoint_barrier_runs_under_lock_before_snapshot_clone() {
    let host = FakeHost::default();
    let source = workspace("main", 5);
    host.seed(&source);
    let substrate = substrate(host.clone(), CountingLane::default());
    let callback_host = host.clone();

    substrate
        .execute_checkpoint_staged(
            substrate
                .plan_checkpoint(
                    &source,
                    CheckpointLabel::new("durable-prefix").expect("label"),
                    Pin::Pinned,
                )
                .expect("checkpoint plan"),
            move |stage| async move {
                assert!(
                    stage
                        .image
                        .components()
                        .any(|component| component.as_os_str() == "checkpoints")
                );
                assert!(
                    !callback_host
                        .events()
                        .iter()
                        .any(|event| event.as_str() == "clone"),
                    "snapshot clone started before the artifact barrier completed"
                );
                callback_host.record("artifact-barrier+manifest-fsync");
                Ok::<(), &'static str>(())
            },
        )
        .await
        .expect("checkpoint");

    let events = host.events();
    let barrier = events
        .iter()
        .position(|event| event == "artifact-barrier+manifest-fsync")
        .expect("barrier event");
    let clone = events
        .iter()
        .position(|event| event == "clone")
        .expect("snapshot clone");
    let publication = events
        .iter()
        .position(|event| event == "checkpoint-fact:Pinned")
        .expect("checkpoint publication");
    assert!(barrier < clone && clone < publication);
    assert_eq!(events[0], "lock:1");
}

#[tokio::test]
async fn aborting_checkpoint_barrier_creates_no_snapshot_or_fact() {
    let host = FakeHost::default();
    let source = workspace("main", 5);
    host.seed(&source);
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate
        .plan_checkpoint(
            &source,
            CheckpointLabel::new("cancelled").expect("label"),
            Pin::Automatic,
        )
        .expect("checkpoint plan");
    let entered = Arc::new(AtomicBool::new(false));
    let callback_entered = Arc::clone(&entered);
    let task = tokio::spawn(async move {
        substrate
            .execute_checkpoint_staged(plan, move |_| async move {
                callback_entered.store(true, Ordering::SeqCst);
                std::future::pending::<Result<(), &'static str>>().await
            })
            .await
    });

    abort_at_callback(task, entered).await;

    assert_no_orphan_stage(&host);
    assert!(!host.events().iter().any(|event| event.as_str() == "clone"));
    assert!(host.checkpoints(&repo()).expect("checkpoints").is_empty());
}

#[tokio::test]
async fn aborting_restore_prepare_callback_cleans_replace_and_verify_mounts() {
    for mode in [RestoreMode::Replace, RestoreMode::VerifyOnly] {
        let host = FakeHost::default();
        let current = workspace("raven", 7);
        host.seed(&current);
        let substrate = substrate(host.clone(), CountingLane::default());
        let checkpoint = cowshed_core::storage::lifecycle::CheckpointRef::new(
            current.clone(),
            CheckpointLabel::new("ready").expect("label"),
            Revision::new(8),
            true,
        );
        let plan = substrate
            .plan_restore(&current, &checkpoint, mode, identity())
            .expect("restore plan");
        let entered = Arc::new(AtomicBool::new(false));
        let callback_entered = Arc::clone(&entered);
        let task = tokio::spawn(async move {
            substrate
                .execute_restore_staged(
                    plan,
                    move |stage| async move {
                        assert_eq!(
                            matches!(stage, RestoreStage::Replace(_)),
                            mode == RestoreMode::Replace
                        );
                        callback_entered.store(true, Ordering::SeqCst);
                        std::future::pending::<Result<(), &'static str>>().await
                    },
                    |_| async { Ok::<(), &'static str>(()) },
                )
                .await
        });

        abort_at_callback(task, entered).await;

        assert_no_orphan_stage(&host);
        assert_eq!(
            host.list(&repo()).expect("post-cancel listing"),
            vec![StorageFact {
                workspace: current,
                volume_key: volume_key(&repo(), &WorkspaceName::new("raven").expect("workspace"),),
            }]
        );
        let events = host.events();
        assert!(events.contains(&"detach:Release".to_owned()));
        if mode == RestoreMode::Replace {
            assert!(events.contains(&"idempotent-reclaim".to_owned()));
        }
        assert!(!events.contains(&"atomic-restore-swap+undo".to_owned()));
    }
}

#[tokio::test]
async fn aborting_restore_fence_leaves_recoverable_pending_publication() {
    let host = FakeHost::default();
    let current = workspace("raven", 7);
    host.seed(&current);
    let substrate = substrate(host.clone(), CountingLane::default());
    let checkpoint = cowshed_core::storage::lifecycle::CheckpointRef::new(
        current.clone(),
        CheckpointLabel::new("ready").expect("label"),
        Revision::new(8),
        true,
    );
    let plan = substrate
        .plan_restore(&current, &checkpoint, RestoreMode::Replace, identity())
        .expect("restore plan");
    let entered = Arc::new(AtomicBool::new(false));
    let callback_entered = Arc::clone(&entered);
    let pending_workspace = Arc::new(Mutex::new(None));
    let callback_workspace = Arc::clone(&pending_workspace);
    let task = {
        let substrate = substrate.clone();
        tokio::spawn(async move {
            substrate
                .execute_restore_staged(
                    plan,
                    |_| async { Ok::<(), &'static str>(()) },
                    move |fence| async move {
                        *callback_workspace.lock().expect("pending workspace") =
                            Some(fence.pending.workspace);
                        callback_entered.store(true, Ordering::SeqCst);
                        std::future::pending::<Result<(), &'static str>>().await
                    },
                )
                .await
        })
    };

    abort_at_callback(task, entered).await;

    assert_eq!(
        host.pending_publications(&repo())
            .expect("pending publication")
            .len(),
        1
    );
    assert!(
        host.mounted_paths_now().iter().all(|path| !path
            .components()
            .any(|component| component.as_os_str() == ".staging")),
        "the persisted pending publication must not retain a staging mount"
    );
    let replacement = pending_workspace
        .lock()
        .expect("pending workspace")
        .clone()
        .expect("replacement workspace");
    substrate
        .ensure_mounted(&replacement, MountIntent { browse: false })
        .await
        .expect("ensure recovers pending publication");
    assert!(
        host.pending_publications(&repo())
            .expect("pending publication after recovery")
            .is_empty()
    );
    assert_eq!(
        host.list(&repo()).expect("recovered publication"),
        vec![StorageFact {
            workspace: replacement,
            volume_key: volume_key(&repo(), &WorkspaceName::new("raven").expect("workspace"),),
        }]
    );
    assert!(
        host.events()
            .contains(&"recover-pending-publication".to_owned())
    );
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
    fn clone_checkpoint_and_fork_each_clone_one_image(
        operations in prop::collection::vec(0_u8..=2, 1..20),
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
                            .execute_create_staged(plan, |_| async {
                                Ok::<(), &'static str>(())
                            })
                            .await
                            .expect("create");
                    }
                    1 => {
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
                            .execute_fork_staged(plan, |_| async {
                                Ok::<(), &'static str>(())
                            })
                            .await
                            .expect("fork");
                    }
                    _ => {
                        let plan = substrate.plan_checkpoint(
                            &source,
                            CheckpointLabel::new(format!("checkpoint-{index}"))
                                .expect("label"),
                            Pin::Automatic,
                        ).expect("checkpoint plan");
                        substrate
                            .execute_checkpoint_staged(plan, |_| async {
                                Ok::<(), &'static str>(())
                            })
                            .await
                            .expect("checkpoint");
                    }
                }
            }
            // The fake refuses a clone between paths that are not `<name>.asif`, so each operation
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
        .execute_create_staged(plan, |stage| async move {
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
            .filter(|event| event.as_str() == "attach-no-mount+fsck")
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
        "/store/acme--widget/sessions/hedge.asif",
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
        .execute_create_staged(plan, move |stage| async move {
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
    let name = WorkspaceName::new("unfinished").expect("destination");
    let pending = LifecycleWorkspace::new(
        repo(),
        name.clone(),
        incarnation(9),
        Revision::new(6),
        Revision::new(11),
        WorkspaceRole::Workspace,
    )
    .expect("pending");
    let original = identity();
    host.resume_clone_from(
        pending.clone(),
        original.clone(),
        "/store/acme--widget/sessions/unfinished.asif",
    );
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate
        .plan_create(
            &source,
            Destination {
                repo: repo(),
                name,
                topology_revision: Revision::new(11),
                identity: original,
            },
        )
        .expect("retirement plan");
    let refusal = substrate
        .execute_pending_clone_retirement(plan.clone(), |stage| async move {
            assert!(stage.resuming);
            Err::<(), _>("unlanded commits")
        })
        .await
        .expect_err("Git safety refusal keeps the pending clone");
    assert!(matches!(
        refusal,
        cowshed_core::storage::apfs::StagedExecutionError::Initializer("unlanded commits")
    ));
    assert!(
        !host
            .events()
            .iter()
            .any(|event| event == "atomic-retire-to-trash")
    );
    let (retired, proof) = substrate
        .execute_pending_clone_retirement(plan, |stage| async move {
            assert_eq!(stage.workspace, pending);
            Ok::<_, &'static str>("landed")
        })
        .await
        .expect("retire verified pending clone");
    assert_eq!(proof, "landed");
    assert_eq!(retired.workspace().revision(), Revision::new(6));
    let events = host.events();
    assert!(events.iter().any(|event| event == "atomic-retire-to-trash"));
    assert!(!events.iter().any(|event| event == "activate-pending"));
    assert!(!events.iter().any(|event| event.as_str() == "clone"));
}
