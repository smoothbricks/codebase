use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cowshed_core::apfs::{
    ApfsCaseSensitivity, CreateImageRequest, CreatedImage, ImageFormatSelection,
};
use cowshed_core::metadata::{ImageFormat, WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::CheckpointLabel;
use cowshed_core::storage::apfs::{
    ApfsBlockingLane, ApfsExecutionHost, ApfsStorageError, ApfsSubstrate, ApfsSubstrateConfig,
    IncarnationSource, MarkerExpectation, MetadataPolicy, volume_name,
};
use cowshed_core::storage::lifecycle::{
    AdoptRequest, Destination, ExpectedState, GcReport, KernelMountFact, LifecyclePlanner,
    MountState, ObservedState, Pin, RestoreMode, Revision, StorageFact, Substrate, SubstrateStats,
    WorkspaceRef,
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
}

impl Default for FakeHost {
    fn default() -> Self {
        Self {
            state: Arc::default(),
            marker_validations_before_failure: Arc::new(AtomicUsize::new(usize::MAX)),
            fail_metadata_once: Arc::default(),
            fail_restored_metadata_once: Arc::default(),
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

    fn seed(&self, workspace: &WorkspaceRef) {
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

    fn fail_next_restored_metadata(&self) {
        self.fail_restored_metadata_once
            .store(true, Ordering::SeqCst);
    }
}

impl ApfsExecutionHost for FakeHost {
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

    fn mount(
        &self,
        attachment: &Self::Attachment,
        mount_point: &Path,
        _: bool,
    ) -> Result<(), ApfsStorageError> {
        self.record_path(mount_point);
        self.state
            .lock()
            .expect("fake state")
            .mount_paths
            .push(mount_point.to_owned());
        self.record(format!("mount:{:?}", attachment.format));
        Ok(())
    }

    fn chown_volume_root(&self, _: &Path) -> Result<(), ApfsStorageError> {
        self.record("chown-root");
        Ok(())
    }

    fn write_marker(
        &self,
        _: &Path,
        _: &WorkspaceRef,
        forked_from: Option<&WorkspaceName>,
    ) -> Result<(), ApfsStorageError> {
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

    fn detach(&self, _: Self::Attachment, force: bool) -> Result<(), ApfsStorageError> {
        self.record(format!("detach:{force}"));
        Ok(())
    }

    fn heal_mount(&self, _: &WorkspaceRef, _: &Path) -> Result<(), ApfsStorageError> {
        Ok(())
    }

    fn retain_mounted(
        &self,
        workspace: &WorkspaceRef,
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
        workspace: &WorkspaceRef,
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

    fn publish_image(&self, _: &Path, _: &Path) -> Result<(), ApfsStorageError> {
        self.record("atomic-publish-image");
        Ok(())
    }

    fn publish_metadata(
        &self,
        _: &Path,
        workspace: &WorkspaceRef,
        revision: Revision,
        policy: MetadataPolicy,
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
        self.seed(workspace);
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
        workspace: &WorkspaceRef,
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
            MetadataPolicy::Preserve,
            Some(source_image),
        )
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

    fn reclaim_image(&self, _: &Path, _: ImageFormat) -> Result<(), ApfsStorageError> {
        self.record("idempotent-reclaim");
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

    fn recover_pending(&self, _: &ApfsSubstrateConfig) -> Result<(), ApfsStorageError> {
        Ok(())
    }

    fn stats(&self, _: &WorkspaceRef, _: &Path) -> Result<SubstrateStats, ApfsStorageError> {
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

    fn gc(&self, _: &ApfsSubstrateConfig) -> Result<GcReport, ApfsStorageError> {
        self.record("gc-trash+compact-detached");
        Ok(GcReport {
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

fn workspace(name: &str, format: ImageFormat, revision: u64) -> WorkspaceRef {
    let name = WorkspaceName::new(name).expect("workspace name");
    WorkspaceRef::new(
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

#[tokio::test]
async fn adopt_uses_exact_format_and_verify_before_mount_order() {
    let host = FakeHost::default();
    let lane = CountingLane::default();
    let substrate = substrate(host.clone(), lane.clone());
    let plan = substrate
        .plan_adopt(AdoptRequest {
            repo: repo(),
            format: ImageFormat::Sparse,
            topology_revision: Revision::new(0),
        })
        .expect("adopt plan");

    let receipt = substrate.execute_adopt(plan).await.expect("adopt");

    assert_eq!(receipt.workspace.format(), ImageFormat::Sparse);
    assert_eq!(receipt.workspace.revision(), Revision::new(1));
    assert_eq!(receipt.workspace.incarnation(), &incarnation(0));
    assert_eq!(receipt.workspace.topology_revision(), Revision::new(1));
    assert_eq!(
        lane.count(),
        2,
        "one authoritative read and one blocking apply"
    );
    assert_eq!(
        host.events(),
        [
            "observe",
            "create:Sparse:100g",
            "atomic-metadata+parent-fsync:Fresh",
            "attach-no-mount+fsck:Sparse",
            "mount:Sparse",
            "write-marker",
            "validate-marker",
            "detach:false",
            "atomic-publish-image",
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
async fn ensure_mounted_is_idempotent_for_an_already_mounted_workspace() {
    let host = FakeHost::default();
    let substrate = substrate(host.clone(), CountingLane::default());
    let plan = substrate
        .plan_adopt(AdoptRequest {
            repo: repo(),
            format: ImageFormat::Sparse,
            topology_revision: Revision::new(0),
        })
        .expect("adopt plan");
    let workspace = substrate
        .execute_adopt(plan)
        .await
        .expect("adopt")
        .workspace;
    host.clear_events();

    let path = substrate
        .ensure_mounted(&workspace)
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
        .plan_adopt(AdoptRequest {
            repo: repo(),
            format: ImageFormat::Asif,
            topology_revision: Revision::new(0),
        })
        .expect("adopt plan");

    let error = substrate
        .execute_adopt(plan)
        .await
        .expect_err("marker mismatch");
    assert!(matches!(error, ApfsStorageError::MarkerMismatch(_)));
    let events = host.events();
    assert!(events.iter().any(|event| event.starts_with("detach:")));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(!events.contains(&"atomic-publish-image".to_owned()));
    assert!(!events.contains(&"retain-mounted".to_owned()));
}

#[tokio::test]
async fn restore_staging_failure_remounts_the_old_image_without_swapping() {
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
        )
        .expect("restore plan");
    host.fail_next_metadata();
    host.clear_events();

    substrate
        .execute_restore(plan)
        .await
        .expect_err("staging metadata failure");

    let events = host.events();
    assert!(!events.contains(&"atomic-restore-swap+undo".to_owned()));
    assert!(events.contains(&"idempotent-reclaim".to_owned()));
    assert!(
        events
            .iter()
            .any(|event| event == "attach-no-mount+fsck:Sparse")
    );
    assert_eq!(events.last().map(String::as_str), Some("retain-mounted"));
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
        )
        .expect("restore plan");
    host.fail_marker_after(1);
    host.clear_events();

    substrate
        .execute_restore(plan)
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
        .plan_restore(&current, &checkpoint, RestoreMode::Replace)
        .expect("restore plan");
    host.fail_next_restored_metadata();
    host.clear_events();

    substrate
        .execute_restore(plan)
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
        .execute_create(
            substrate
                .plan_create(
                    &source,
                    Destination {
                        repo: repo(),
                        name: WorkspaceName::session("created").expect("created"),
                        topology_revision: Revision::new(8),
                    },
                )
                .expect("create plan"),
        )
        .await
        .expect("create");
    assert_eq!(created.workspace.revision(), Revision::new(6));
    assert_eq!(created.workspace.topology_revision(), Revision::new(9));

    let forked = substrate
        .execute_fork(
            substrate
                .plan_fork(
                    &source,
                    Destination {
                        repo: repo(),
                        name: WorkspaceName::session("forked").expect("forked"),
                        topology_revision: Revision::new(10),
                    },
                )
                .expect("fork plan"),
        )
        .await
        .expect("fork");
    assert_eq!(forked.workspace.revision(), Revision::new(6));
    assert_eq!(forked.workspace.topology_revision(), Revision::new(11));

    let checkpoint = substrate
        .execute_checkpoint(
            substrate
                .plan_checkpoint(
                    &source,
                    CheckpointLabel::new("exact").expect("label"),
                    Pin::Pinned,
                )
                .expect("checkpoint plan"),
        )
        .await
        .expect("checkpoint");
    assert_eq!(checkpoint.revision(), Revision::new(6));
    assert!(checkpoint.pinned());

    let restored = substrate
        .execute_restore(
            substrate
                .plan_restore(&source, &checkpoint, RestoreMode::Replace)
                .expect("restore plan"),
        )
        .await
        .expect("restore");
    assert_eq!(restored.previous_incarnation, *source.incarnation());
    assert_eq!(restored.workspace.revision(), Revision::new(6));
    assert_eq!(
        restored.workspace.topology_revision(),
        source.topology_revision()
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
    assert!(listed.contains(&created.workspace));
    assert!(listed.contains(&forked.workspace));
    assert!(listed.contains(&restored.workspace));

    host.clear_events();
    substrate
        .unmount(&restored.workspace)
        .await
        .expect("unmount");
    assert_eq!(host.events(), ["detach-mounted:false"]);
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
                            },
                        ).expect("create plan");
                        let receipt = substrate.execute_create(plan).await.expect("create");
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
                            },
                        ).expect("fork plan");
                        let receipt = substrate.execute_fork(plan).await.expect("fork");
                        prop_assert_eq!(receipt.workspace.format(), format);
                    }
                    _ => {
                        let plan = substrate.plan_checkpoint(
                            &source,
                            CheckpointLabel::new(format!("checkpoint-{index}"))
                                .expect("label"),
                            Pin::Automatic,
                        ).expect("checkpoint plan");
                        let checkpoint = substrate.execute_checkpoint(plan).await.expect("checkpoint");
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
