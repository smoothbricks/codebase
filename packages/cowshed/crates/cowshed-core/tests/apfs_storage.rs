use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cowshed_core::apfs::{
    ApfsCaseSensitivity, CreateImageRequest, CreatedImage, ImageFormatSelection, MountAccess,
};
use cowshed_core::metadata::{
    GrantSet, ImageFormat, PortBlock, WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::CheckpointLabel;
use cowshed_core::storage::apfs::{
    AdoptExecutionError, ApfsBlockingLane, ApfsExecutionHost, ApfsStorageError, ApfsSubstrate,
    ApfsSubstrateConfig, IncarnationSource, LockMode, MarkerExpectation, MetadataPolicy,
    PublicationError, RestoreStage, volume_name,
};
use cowshed_core::storage::lifecycle::{
    AdoptRequest, CheckpointFact, Destination, ExpectedState, KernelMountFact, LifecyclePlanner,
    LifecycleWorkspace, MountIntent, MountState, ObservedState, OperationIdentity, Pin,
    RestoreMode, Revision, StorageFact, StorageGcReport, Substrate, SubstrateStats,
};
use proptest::prelude::*;

#[derive(Clone, Debug)]
struct FakeAttachment {
    format: ImageFormat,
}

#[derive(Default)]
struct FakeState {
    events: Vec<String>,
    published: BTreeMap<(RepoId, WorkspaceName), StorageFact>,
    mounted: BTreeMap<(RepoId, WorkspaceName), KernelMountFact>,
    formats: BTreeMap<(RepoId, WorkspaceName), ImageFormat>,
    staged: BTreeMap<PathBuf, StorageFact>,
    pending: BTreeMap<PathBuf, StorageFact>,
    checkpoints: Vec<CheckpointFact>,
    next_mount_id: u64,
    paths: Vec<PathBuf>,
    mount_paths: Vec<PathBuf>,
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
        state.formats.insert(key.clone(), workspace.format());
        state.published.insert(
            key,
            StorageFact {
                workspace: workspace.clone(),
                volume_name: volume_name(workspace.repo(), workspace.name()),
            },
        );
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

    fn observe(&self, expected: &[ExpectedState]) -> Result<Vec<ObservedState>, ApfsStorageError> {
        self.record("observe");
        Ok(expected
            .iter()
            .map(|fact| match fact {
                ExpectedState::Exists {
                    repo,
                    name,
                    incarnation,
                    revision,
                    topology_revision,
                    retired,
                } => ObservedState::Exists {
                    repo: repo.clone(),
                    name: name.clone(),
                    incarnation: incarnation.clone(),
                    revision: *revision,
                    topology_revision: *topology_revision,
                    retired: *retired,
                },
                ExpectedState::Absent {
                    repo,
                    name,
                    topology_revision,
                } => ObservedState::Absent {
                    repo: repo.clone(),
                    name: name.clone(),
                    topology_revision: *topology_revision,
                },
                ExpectedState::Checkpoint {
                    repo,
                    workspace,
                    label,
                    revision,
                } => ObservedState::Checkpoint {
                    repo: repo.clone(),
                    workspace: workspace.clone(),
                    label: label.clone(),
                    revision: *revision,
                },
            })
            .collect())
    }

    fn resolve_format(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<ImageFormat, ApfsStorageError> {
        self.record("resolve-format");
        self.state
            .lock()
            .expect("fake state")
            .formats
            .get(&(repo.clone(), workspace.clone()))
            .copied()
            .ok_or_else(|| ApfsStorageError::Host("missing fake format".to_owned()))
    }

    fn create_staged(
        &self,
        request: &CreateImageRequest,
        requested: ImageFormat,
    ) -> Result<CreatedImage, ApfsStorageError> {
        let valid_selection = match requested {
            ImageFormat::Asif => request.image_format == ImageFormatSelection::Auto,
            ImageFormat::Sparse => {
                request.image_format == ImageFormatSelection::Exact(ImageFormat::Sparse)
            }
        };
        if !valid_selection {
            return Err(ApfsStorageError::Host(
                "format selection disagreed with requested lifecycle format".to_owned(),
            ));
        }
        self.record(format!("create:{requested:?}:{}", request.capacity));
        Ok(CreatedImage {
            path: request.staged_stem.with_extension(requested.extension()),
            format: requested,
        })
    }

    fn clone_image(
        &self,
        source: &Path,
        destination: &Path,
        format: ImageFormat,
    ) -> Result<(), ApfsStorageError> {
        if source.extension() != destination.extension()
            || destination.extension().and_then(|value| value.to_str()) != Some(format.extension())
        {
            return Err(ApfsStorageError::Host("clone format changed".to_owned()));
        }
        self.record(format!("clone:{format:?}"));
        Ok(())
    }

    fn attach_verified(
        &self,
        image: &Path,
        format: ImageFormat,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        if image.extension().and_then(|value| value.to_str()) != Some(format.extension()) {
            return Err(ApfsStorageError::Host(
                "format/extension mismatch before attach".to_owned(),
            ));
        }
        self.record(format!("attach-no-mount+fsck:{format:?}"));
        Ok(FakeAttachment { format })
    }

    fn copy_tree(&self, _: &Path, _: &Path) -> Result<(), ApfsStorageError> {
        self.record("copy-until-quiescent");
        Ok(())
    }
    fn mount(
        &self,
        attachment: &Self::Attachment,
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
        self.record(format!("mount:{:?}", attachment.format));
        Ok(())
    }

    fn chown_volume_root(&self, _: &Path) -> Result<(), ApfsStorageError> {
        self.record("chown-root");
        Ok(())
    }

    fn rename_volume(&self, _: &Path, volume_name: &str) -> Result<(), ApfsStorageError> {
        self.record(format!("rename-volume:{volume_name}"));
        Ok(())
    }

    fn mint_workspace_credentials(
        &self,
        _: &LifecycleWorkspace,
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
    fn detach(&self, _: Self::Attachment, force: bool) -> Result<(), ApfsStorageError> {
        self.record(format!("detach:{force}"));
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
                volume_name: volume_name(workspace.repo(), workspace.name()),
            },
        );
        Ok(mount_id)
    }

    fn detach_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        force: bool,
    ) -> Result<(), ApfsStorageError> {
        self.record(format!("detach-mounted:{force}"));
        self.state
            .lock()
            .expect("fake state")
            .mounted
            .remove(&(workspace.repo().clone(), workspace.name().clone()));
        Ok(())
    }
    fn publish_image(&self, staged: &Path, _: &Path) -> Result<(), PublicationError> {
        self.record("atomic-publish-image");
        let mut state = self.state.lock().expect("fake state");
        if let Some(fact) = state.staged.remove(staged) {
            let key = (fact.workspace.repo().clone(), fact.workspace.name().clone());
            state.formats.insert(key.clone(), fact.workspace.format());
            state.published.insert(key, fact);
        }
        Ok(())
    }
    fn publish_adopt(
        &self,
        _: &Path,
        _: &Path,
        staged: &Path,
        _: &Path,
    ) -> Result<(), PublicationError> {
        self.record("atomic-adopt-handoff+publish");
        let mut state = self.state.lock().expect("fake state");
        if let Some(fact) = state.staged.remove(staged) {
            let key = (fact.workspace.repo().clone(), fact.workspace.name().clone());
            state.formats.insert(key.clone(), fact.workspace.format());
            state.published.insert(key, fact);
        }
        Ok(())
    }
    fn publish_metadata(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        policy: MetadataPolicy,
        _: Option<&OperationIdentity>,
        _: Option<&Path>,
    ) -> Result<(), ApfsStorageError> {
        self.record(format!("atomic-metadata+parent-fsync:{policy:?}"));
        if self.fail_metadata_once.swap(false, Ordering::SeqCst) {
            return Err(ApfsStorageError::Host(
                "injected metadata failure".to_owned(),
            ));
        }
        if revision != workspace.revision() && policy == MetadataPolicy::Fresh {
            return Err(ApfsStorageError::Host("fresh revision mismatch".to_owned()));
        }
        let fact = StorageFact {
            workspace: workspace.clone(),
            volume_name: volume_name(workspace.repo(), workspace.name()),
        };
        if policy == MetadataPolicy::PendingFence {
            let key = (workspace.repo().clone(), workspace.name().clone());
            let mut state = self.state.lock().expect("fake state");
            state.published.remove(&key);
            state.pending.insert(image.to_owned(), fact);
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
    ) -> Result<(), ApfsStorageError> {
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
        )
    }

    fn activate_restored_metadata(&self, canonical: &Path) -> Result<(), ApfsStorageError> {
        self.record("activate-restored-metadata");
        let mut state = self.state.lock().expect("fake state");
        let fact = state
            .pending
            .remove(canonical)
            .ok_or_else(|| ApfsStorageError::PendingPublication(canonical.to_owned()))?;
        let key = (fact.workspace.repo().clone(), fact.workspace.name().clone());
        state.formats.insert(key.clone(), fact.workspace.format());
        state.published.insert(key, fact);
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
    fn reclaim_image(&self, image: &Path, _: ImageFormat) -> Result<(), ApfsStorageError> {
        self.record("idempotent-reclaim");
        self.state.lock().expect("fake state").staged.remove(image);
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
                        PathBuf::from(format!("/store/mnt/{}", fact.workspace.name()))
                    },
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
        let pending = std::mem::take(&mut state.pending);
        if !pending.is_empty() {
            state.events.push("recover-pending-publication".to_owned());
        }
        for (_, fact) in pending {
            let key = (fact.workspace.repo().clone(), fact.workspace.name().clone());
            state.formats.insert(key.clone(), fact.workspace.format());
            state.published.insert(key, fact);
        }
        Ok(())
    }

    fn stats(&self, _: &LifecycleWorkspace, _: &Path) -> Result<SubstrateStats, ApfsStorageError> {
        Ok(SubstrateStats {
            logical_bytes: 4096,
            allocated_bytes: 1024,
            checkpoint_count: 3,
        })
    }

    fn compact(&self, _: &Path, format: ImageFormat) -> Result<bool, ApfsStorageError> {
        self.record(format!("compact:{format:?}"));
        Ok(format == ImageFormat::Sparse)
    }

    fn gc(&self, _: &ApfsSubstrateConfig) -> Result<StorageGcReport, ApfsStorageError> {
        self.record("gc-trash+compact-detached");
        Ok(StorageGcReport {
            examined: 2,
            reclaimed: 1,
            retained_pinned: 1,
            retained_recent: 0,
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
        created_trace: "apfs-storage".to_owned(),
        grants: GrantSet::closed_baseline(Some(PortBlock::new(20000, 16).expect("port block")))
            .expect("grants"),
    }
}

fn adopt_request(format: ImageFormat) -> AdoptRequest {
    AdoptRequest {
        repo: repo(),
        format,
        topology_revision: Revision::new(0),
        source_checkout: PathBuf::from("/project"),
        pre_cowshed_checkout: PathBuf::from("/project.pre-cowshed"),
        identity: identity(),
    }
}

fn workspace(name: &str, format: ImageFormat, revision: u64) -> LifecycleWorkspace {
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
        format,
    )
    .expect("workspace ref")
}

fn substrate(host: FakeHost, lane: CountingLane) -> ApfsSubstrate<FakeHost, CountingLane> {
    ApfsSubstrate::with_lane_and_incarnations(
        ApfsSubstrateConfig::new(
            "/store",
            "/store/caches",
            "/project",
            ApfsCaseSensitivity::Insensitive,
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

#[tokio::test]
async fn adopt_uses_exact_format_and_verify_before_mount_order() {
    let host = FakeHost::default();
    let lane = CountingLane::default();
    let substrate = substrate(host.clone(), lane.clone());
    let plan = substrate
        .plan_adopt(adopt_request(ImageFormat::Sparse))
        .expect("adopt plan");

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
                "the mounted stage must not be visible in canonical enumeration"
            );
            assert!(
                !callback_host
                    .events()
                    .contains(&"atomic-adopt-handoff+publish".to_owned())
            );
            callback_host.record("controller-initialize");
            Ok::<(), &'static str>(())
        })
        .await
        .expect("adopt");

    assert_eq!(receipt.workspace.format(), ImageFormat::Sparse);
    assert_eq!(receipt.workspace.revision(), Revision::new(1));
    assert_eq!(receipt.workspace.incarnation(), &incarnation(0));
    assert_eq!(receipt.workspace.topology_revision(), Revision::new(1));
    assert_eq!(
        lane.count(),
        4,
        "lock, authoritative read, staged preparation, and post-callback commit"
    );
    assert_eq!(
        host.events(),
        [
            "lock:1",
            "observe",
            "create:Sparse:100g",
            "atomic-metadata+parent-fsync:Fresh",
            "attach-no-mount+fsck:Sparse",
            "mount:Sparse",
            "copy-until-quiescent",
            "mint-workspace-credentials",
            "marker-identity:apfs-storage",
            "write-marker",
            "validate-marker",
            "controller-initialize",
            "validate-staged-companion",
            "validate-marker",
            "detach:false",
            "atomic-adopt-handoff+publish",
            "attach-no-mount+fsck:Sparse",
            "mount:Sparse",
            "validate-marker",
            "retain-mounted",
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

#[tokio::test]
async fn initializer_failure_detaches_reclaims_and_never_publishes() {
    let host = FakeHost::default();
    let lane = CountingLane::default();
    let substrate = substrate(host.clone(), lane.clone());
    let plan = substrate
        .plan_adopt(adopt_request(ImageFormat::Sparse))
        .expect("adopt plan");
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
    assert!(events.contains(&"detach:false".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"atomic-adopt-handoff+publish".to_owned()));
    assert!(!events.contains(&"retain-mounted".to_owned()));
}

#[tokio::test]
async fn credential_mint_failure_reclaims_adopt_stage_before_publication() {
    let host = FakeHost::default();
    host.fail_next_credentials();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate
        .plan_adopt(adopt_request(ImageFormat::Sparse))
        .expect("adopt plan");

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
    assert!(events.contains(&"detach:false".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"write-marker".to_owned()));
    assert!(!events.contains(&"atomic-adopt-handoff+publish".to_owned()));
    assert!(host.list(&repo()).expect("post-failure listing").is_empty());
}

#[tokio::test]
async fn initializer_and_cleanup_errors_are_both_preserved() {
    let host = FakeHost::default();
    host.fail_next_reclaim();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate
        .plan_adopt(adopt_request(ImageFormat::Sparse))
        .expect("adopt plan");

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
    assert!(
        !host
            .events()
            .contains(&"atomic-adopt-handoff+publish".to_owned())
    );
}

#[tokio::test]
async fn adopt_rejects_each_source_identity_mismatch_before_mutation() {
    for (main_mount, project_root) in [
        ("/project", "/different-project"),
        ("/different-main-mount", "/project"),
    ] {
        let host = FakeHost::default();
        let lane = CountingLane::default();
        let substrate = ApfsSubstrate::with_lane_and_incarnations(
            ApfsSubstrateConfig::new(
                "/store",
                "/store/caches",
                main_mount,
                ApfsCaseSensitivity::Insensitive,
            ),
            host.clone(),
            lane.clone(),
            FixedIncarnations::default(),
        );
        let mut request = adopt_request(ImageFormat::Sparse);
        request.identity.project_root = PathBuf::from(project_root);
        let plan = substrate.plan_adopt(request).expect("adopt plan");

        let error = substrate
            .execute_adopt_staged(plan, |_| async { Ok::<(), &'static str>(()) })
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            AdoptExecutionError::Storage(ApfsStorageError::InvalidPlan(
                "adopt source must equal operation project root and canonical main mount"
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
    let plan = substrate
        .plan_adopt(adopt_request(ImageFormat::Sparse))
        .expect("adopt plan");
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
    let plan = substrate
        .plan_adopt(adopt_request(ImageFormat::Asif))
        .expect("adopt plan");

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
    assert!(!events.contains(&"atomic-publish-image".to_owned()));
    assert!(!events.contains(&"retain-mounted".to_owned()));
}

#[tokio::test]
async fn restore_staging_failure_leaves_the_old_workspace_untouched() {
    let host = FakeHost::default();
    let current = workspace("raven", ImageFormat::Sparse, 7);
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
    assert!(!events.contains(&"detach-mounted:false".to_owned()));
    assert!(!events.contains(&"attach-no-mount+fsck:Sparse".to_owned()));
    assert_eq!(
        host.list(&repo()).expect("active workspace"),
        vec![StorageFact {
            workspace: current,
            volume_name: volume_name(&repo(), &WorkspaceName::new("raven").expect("workspace")),
        }]
    );
}

#[tokio::test]
async fn restore_post_swap_marker_failure_rolls_back_and_remounts_old_image() {
    let host = FakeHost::default();
    let current = workspace("raven", ImageFormat::Sparse, 7);
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
            .any(|event| event == "attach-no-mount+fsck:Sparse")
    );
    assert_eq!(events.last().map(String::as_str), Some("retain-mounted"));
}

#[tokio::test]
async fn restore_metadata_publication_failure_rolls_back_after_verified_mount() {
    let host = FakeHost::default();
    let current = workspace("raven", ImageFormat::Sparse, 7);
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
    let source = workspace("main", ImageFormat::Sparse, 5);
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
        [
            "rename-volume:cowshed.acme--widget.created",
            "rename-volume:cowshed.acme--widget.forked",
            "rename-volume:cowshed.acme--widget.main",
        ],
        "each cloned staging volume is relabeled before publication"
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
        3,
        "create, fork, and restore each mount exactly one hidden staging image"
    );
    let paths = host.paths();
    assert!(
        paths.iter().any(|path| path
            .components()
            .any(|component| component.as_os_str() == ".staging")),
        "create/restore preparation must use a hidden staging mount"
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
    assert_eq!(host.events(), ["lock:1", "detach-mounted:false"]);
}
#[tokio::test]
async fn retire_reclaim_stats_and_gc_cross_only_the_blocking_lane() {
    let host = FakeHost::default();
    let current = workspace("raven", ImageFormat::Asif, 4);
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
        }
    );
    assert_eq!(substrate.gc().await.expect("gc").reclaimed, 1);
    assert!(lane.count() >= 5);
    let events = host.events();
    assert!(events.contains(&"atomic-retire-to-trash".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(events.contains(&"gc-trash+compact-detached".to_owned()));
}

#[tokio::test]
async fn aborting_adopt_callback_detaches_and_reclaims_the_stage() {
    let host = FakeHost::default();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate
        .plan_adopt(adopt_request(ImageFormat::Sparse))
        .expect("adopt plan");
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
    assert!(events.contains(&"detach:false".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"atomic-adopt-handoff+publish".to_owned()));
}

#[tokio::test]
async fn aborting_create_and_fork_callbacks_detaches_and_reclaims_each_stage() {
    for fork in [false, true] {
        let host = FakeHost::default();
        let source = workspace("main", ImageFormat::Sparse, 5);
        host.seed(&source);
        let substrate = substrate(host.clone(), CountingLane::default());
        let destination = Destination {
            repo: repo(),
            name: WorkspaceName::session(if fork {
                "cancelled-fork"
            } else {
                "cancelled-create"
            })
            .expect("destination"),
            topology_revision: Revision::new(8),
            identity: identity(),
        };
        let entered = Arc::new(AtomicBool::new(false));
        let callback_entered = Arc::clone(&entered);
        let task = if fork {
            let plan = substrate
                .plan_fork(&source, destination)
                .expect("fork plan");
            tokio::spawn(async move {
                substrate
                    .execute_fork_staged(plan, move |_| async move {
                        callback_entered.store(true, Ordering::SeqCst);
                        std::future::pending::<Result<(), &'static str>>().await
                    })
                    .await
            })
        } else {
            let plan = substrate
                .plan_create(&source, destination)
                .expect("create plan");
            tokio::spawn(async move {
                substrate
                    .execute_create_staged(plan, move |_| async move {
                        callback_entered.store(true, Ordering::SeqCst);
                        std::future::pending::<Result<(), &'static str>>().await
                    })
                    .await
            })
        };

        abort_at_callback(task, entered).await;

        assert_no_orphan_stage(&host);
        assert_eq!(
            host.list(&repo()).expect("post-cancel listing"),
            vec![StorageFact {
                workspace: source,
                volume_name: volume_name(&repo(), &WorkspaceName::new("main").expect("main")),
            }]
        );
        let events = host.events();
        assert!(events.contains(&"detach:false".to_owned()));
        assert!(events.contains(&"idempotent-reclaim".to_owned()));
        assert!(!events.contains(&"atomic-publish-image".to_owned()));
    }
}

#[tokio::test]
async fn checkpoint_barrier_runs_under_lock_before_snapshot_clone() {
    let host = FakeHost::default();
    let source = workspace("main", ImageFormat::Sparse, 5);
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
                        .any(|event| event.starts_with("clone:")),
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
        .position(|event| event == "clone:Sparse")
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
    let source = workspace("main", ImageFormat::Sparse, 5);
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
    assert!(
        !host
            .events()
            .iter()
            .any(|event| event.starts_with("clone:"))
    );
    assert!(host.checkpoints(&repo()).expect("checkpoints").is_empty());
}

#[tokio::test]
async fn aborting_restore_prepare_callback_cleans_replace_and_verify_mounts() {
    for mode in [RestoreMode::Replace, RestoreMode::VerifyOnly] {
        let host = FakeHost::default();
        let current = workspace("raven", ImageFormat::Sparse, 7);
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
                volume_name: volume_name(&repo(), &WorkspaceName::new("raven").expect("workspace"),),
            }]
        );
        let events = host.events();
        assert!(events.contains(&"detach:false".to_owned()));
        if mode == RestoreMode::Replace {
            assert!(events.contains(&"idempotent-reclaim".to_owned()));
        }
        assert!(!events.contains(&"atomic-restore-swap+undo".to_owned()));
    }
}

#[tokio::test]
async fn aborting_restore_fence_leaves_recoverable_pending_publication() {
    let host = FakeHost::default();
    let current = workspace("raven", ImageFormat::Sparse, 7);
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
            volume_name: volume_name(&repo(), &WorkspaceName::new("raven").expect("workspace"),),
        }]
    );
    assert!(
        host.events()
            .contains(&"recover-pending-publication".to_owned())
    );
}

proptest! {
    #[test]
    fn clone_checkpoint_and_fork_preserve_format_and_extension(
        sparse in any::<bool>(),
        operations in prop::collection::vec(0_u8..=2, 1..20),
    ) {
        let format = if sparse { ImageFormat::Sparse } else { ImageFormat::Asif };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let host = FakeHost::default();
            let source = workspace("main", format, 1);
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
                        let receipt = substrate
                            .execute_create_staged(plan, |_| async {
                                Ok::<(), &'static str>(())
                            })
                            .await
                            .expect("create");
                        prop_assert_eq!(receipt.workspace.format(), format);
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
                        let receipt = substrate
                            .execute_fork_staged(plan, |_| async {
                                Ok::<(), &'static str>(())
                            })
                            .await
                            .expect("fork");
                        prop_assert_eq!(receipt.workspace.format(), format);
                    }
                    _ => {
                        let plan = substrate.plan_checkpoint(
                            &source,
                            CheckpointLabel::new(format!("checkpoint-{index}"))
                                .expect("label"),
                            Pin::Automatic,
                        ).expect("checkpoint plan");
                        let checkpoint = substrate
                            .execute_checkpoint_staged(plan, |_| async {
                                Ok::<(), &'static str>(())
                            })
                            .await
                            .expect("checkpoint");
                        prop_assert_eq!(checkpoint.workspace().format(), format);
                    }
                }
            }
            let expected = format!("clone:{format:?}");
            prop_assert!(host.events().iter().filter(|event| event.starts_with("clone:")).all(|event| event == &expected));
            Ok(())
        })?;
    }
}
