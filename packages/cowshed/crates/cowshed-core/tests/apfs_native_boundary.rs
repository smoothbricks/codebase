use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cowshed_core::apfs::{
    ApfsCaseSensitivity, CommandOutput, CommandRequest, CommandRunError, CommandRunner,
    CreateImageRequest, ImageFormatSelection,
};
use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, GrantSet, ImageFormat, METADATA_VERSION, Platform, PortBlock,
    WorkspaceIncarnation, WorkspaceName, WorkspaceRole, sidecar_path,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::StorageLayout;
use cowshed_core::storage::apfs::native::{MacOsApfsExecutionHost, WorkspaceMetadataTemplate};
use cowshed_core::storage::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, MarkerExpectation, MetadataPolicy,
};
use cowshed_core::storage::lifecycle::{ExpectedState, Revision, WorkspaceRef};
const ATTACH_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
<dict><key>content-hint</key><string>GUID_partition_scheme</string><key>dev-entry</key><string>/dev/disk9</string></dict>
<dict><key>content-hint</key><string>Apple_APFS</string><key>dev-entry</key><string>/dev/disk9s2</string></dict>
<dict><key>content-hint</key><string>41504653-0000-11AA-AA11-00306543ECAC</string><key>dev-entry</key><string>/dev/disk10s1</string><key>potentially-mountable</key><true/><key>volume-kind</key><string>apfs</string></dict>
</array></dict></plist>"#;

const APFS_LIST_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>Containers</key><array><dict>
<key>PhysicalStores</key><array><dict><key>DeviceIdentifier</key><string>disk9s2</string></dict></array>
<key>Volumes</key><array><dict><key>DeviceIdentifier</key><string>disk10s1</string></dict></array>
</dict></array></dict></plist>"#;

fn successful_output(request: &CommandRequest) -> CommandOutput {
    let args: Vec<_> = request
        .args
        .iter()
        .map(|argument| argument.to_string_lossy())
        .collect();
    let stdout = if args.first().is_some_and(|argument| argument == "attach") {
        ATTACH_PLIST.as_bytes().to_vec()
    } else if args.starts_with(&["apfs".into(), "list".into()]) {
        APFS_LIST_PLIST.as_bytes().to_vec()
    } else {
        Vec::new()
    };
    CommandOutput::success(stdout)
}

#[derive(Clone, Default)]
struct RecordingRunner(Arc<AtomicUsize>);

impl RecordingRunner {
    fn calls(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}
impl CommandRunner for RecordingRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(successful_output(request))
    }
}

#[derive(Clone)]
struct FailingDetachRunner {
    calls: Arc<AtomicUsize>,
    failures_remaining: Arc<AtomicUsize>,
}

impl FailingDetachRunner {
    fn new(failures: usize) -> Self {
        Self {
            calls: Arc::default(),
            failures_remaining: Arc::new(AtomicUsize::new(failures)),
        }
    }
}

impl CommandRunner for FailingDetachRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let is_detach = request
            .args
            .first()
            .is_some_and(|argument| argument == "detach");
        if is_detach
            && self
                .failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            Ok(CommandOutput::failure(1, "busy"))
        } else {
            Ok(successful_output(request))
        }
    }
}

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new(test: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "cowshed-apfs-boundary-{}-{test}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).expect("fixture root");
        Self { root }
    }

    fn config(&self) -> ApfsSubstrateConfig {
        ApfsSubstrateConfig::new(
            &self.root,
            self.root.join("caches"),
            self.root.join("mount"),
            cowshed_core::apfs::ApfsCaseSensitivity::Insensitive,
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn repo() -> RepoId {
    RepoId::parse("acme/widget").expect("repo")
}

fn metadata(format: ImageFormat) -> DetachedWorkspaceMetadata {
    DetachedWorkspaceMetadata {
        version: METADATA_VERSION,
        repo_id: repo(),
        workspace: WorkspaceName::new("main").expect("main"),
        workspace_incarnation: WorkspaceIncarnation::new("00000000000000000000000000000001")
            .expect("incarnation"),
        image_format: format,
        platform: Platform::Macos,
        updated_at: "2026-07-13T00:00:00Z".to_owned(),
        grants: GrantSet::closed_baseline(Some(PortBlock::new(20000, 16).expect("port block")))
            .expect("grants"),
        info_snapshot: None,
    }
}

fn host(fixture: &Fixture, runner: RecordingRunner) -> MacOsApfsExecutionHost<RecordingRunner> {
    MacOsApfsExecutionHost::new(
        runner,
        fixture.config(),
        WorkspaceMetadataTemplate {
            project_root: fixture.root.join("project"),
            base_commit: "0123456789abcdef".to_owned(),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            created_trace: "trace-apfs-boundary".to_owned(),
            grants: GrantSet::closed_baseline(Some(PortBlock::new(20000, 16).expect("port block")))
                .expect("grants"),
        },
    )
    .expect("native APFS host")
}

fn create_image(path: &Path, format: ImageFormat) {
    std::fs::create_dir_all(path.parent().expect("parent")).expect("image parent");
    std::fs::write(path, b"fixture").expect("image");
    metadata(format).write_for_image(path).expect("sidecar");
}

fn workspace(format: ImageFormat) -> WorkspaceRef {
    WorkspaceRef::new(
        repo(),
        WorkspaceName::new("main").expect("main"),
        WorkspaceIncarnation::new("00000000000000000000000000000001").expect("incarnation"),
        Revision::new(1),
        Revision::new(1),
        WorkspaceRole::Main,
        format,
    )
    .expect("workspace")
}

#[test]
fn duplicate_asif_sparse_stems_fail_before_any_command() {
    let fixture = Fixture::new("duplicate");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let asif = layout.main_image(ImageFormat::Asif).expect("asif");
    let sparse = layout.main_image(ImageFormat::Sparse).expect("sparse");
    create_image(asif.image(), ImageFormat::Asif);
    create_image(sparse.image(), ImageFormat::Sparse);
    let runner = RecordingRunner::default();
    let host = host(&fixture, runner.clone());

    let error = host
        .resolve_format(&repo(), &WorkspaceName::new("main").expect("main"))
        .expect_err("duplicate formats");

    assert!(error.to_string().contains("duplicate ASIF/SPARSE stem"));
    assert_eq!(runner.calls(), 0);
}

#[test]
fn metadata_extension_mismatch_fails_before_attach_command() {
    let fixture = Fixture::new("mismatch");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Asif).expect("image");
    std::fs::create_dir_all(image.image().parent().expect("parent")).expect("image parent");
    std::fs::write(image.image(), b"fixture").expect("image");
    let mut mismatched = metadata(ImageFormat::Sparse);
    mismatched.workspace_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("incarnation");
    let json = serde_json::to_vec_pretty(&mismatched).expect("json");
    std::fs::write(sidecar_path(image.image()), json).expect("sidecar");
    let runner = RecordingRunner::default();
    let host = host(&fixture, runner.clone());

    let error = host
        .attach_verified(image.image(), ImageFormat::Asif)
        .expect_err("metadata mismatch");

    assert!(
        error
            .to_string()
            .contains("does not agree with imageFormat"),
        "{error}"
    );
    assert_eq!(runner.calls(), 0);
}

#[test]
fn clone_extension_mismatch_and_asif_compaction_fail_before_commands() {
    let fixture = Fixture::new("preflight");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let source = layout.main_image(ImageFormat::Asif).expect("source");
    create_image(source.image(), ImageFormat::Asif);
    let bad_destination = layout.project().sessions.join("wrong.sparseimage");
    let runner = RecordingRunner::default();
    let host = host(&fixture, runner.clone());

    let clone_error = host
        .clone_image(source.image(), &bad_destination, ImageFormat::Asif)
        .expect_err("clone extension mismatch");
    assert!(
        clone_error
            .to_string()
            .contains("does not agree with imageFormat"),
        "{clone_error}"
    );
    assert!(matches!(
        host.compact(source.image(), ImageFormat::Asif),
        Err(ApfsStorageError::InvalidPlan(_))
    ));
    assert_eq!(runner.calls(), 0);
}

#[test]
fn canonical_publication_moves_complete_image_and_sidecar_together() {
    let fixture = Fixture::new("publish");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main.sparseimage");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(&staged, ImageFormat::Sparse);
    let host = host(&fixture, RecordingRunner::default());

    host.publish_image(&staged, canonical.image())
        .expect("publish complete image");

    assert!(!staged.exists());
    assert!(!sidecar_path(&staged).exists());
    assert!(canonical.image().exists());
    assert!(sidecar_path(canonical.image()).exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("canonical metadata")
            .image_format,
        ImageFormat::Sparse
    );
    let facts = host.list(&repo()).expect("published facts");
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].workspace.repo(), &repo());
    assert!(facts[0].workspace.name().is_main());
    assert_eq!(facts[0].workspace.format(), ImageFormat::Sparse);
}

#[test]
fn restore_swap_and_rollback_keep_image_metadata_generations_paired() {
    let fixture = Fixture::new("restore-swap");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-next.sparseimage");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-next.sparseimage");
    create_image(canonical.image(), ImageFormat::Sparse);
    std::fs::write(canonical.image(), b"old generation").expect("old image");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::write(&staged, b"new generation").expect("new image");
    let mut next_metadata = metadata(ImageFormat::Sparse);
    next_metadata.workspace_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("next incarnation");
    next_metadata
        .write_for_image(&staged)
        .expect("next metadata");
    let host = host(&fixture, RecordingRunner::default());

    host.restore_swap(&staged, canonical.image(), &undo)
        .expect("restore swap");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"new generation"
    );
    assert_eq!(std::fs::read(&undo).expect("undo"), b"old generation");
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("new metadata")
            .workspace_incarnation,
        next_metadata.workspace_incarnation
    );
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&undo)
            .expect("undo metadata")
            .workspace_incarnation,
        metadata(ImageFormat::Sparse).workspace_incarnation
    );

    host.rollback_restore(canonical.image(), &undo, &staged)
        .expect("rollback");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"old generation"
    );
    assert!(!staged.exists());
    assert!(!sidecar_path(&staged).exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("restored metadata")
            .workspace_incarnation,
        metadata(ImageFormat::Sparse).workspace_incarnation
    );
}

#[test]
fn stats_count_only_images_and_gc_drains_session_trash_then_compacts_detached_sparse() {
    let fixture = Fixture::new("stats-gc");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let checkpoints = &layout.project().checkpoints.join("main");
    create_image(&checkpoints.join("one.sparseimage"), ImageFormat::Sparse);
    create_image(&checkpoints.join("two.asif"), ImageFormat::Asif);
    std::fs::write(checkpoints.join("not-an-image.txt"), b"ignored").expect("noise");
    let runner = RecordingRunner::default();
    let host = host(&fixture, runner.clone());

    let stats = host
        .stats(&workspace(ImageFormat::Sparse), canonical.image())
        .expect("stats");
    assert_eq!(stats.logical_bytes, b"fixture".len() as u64);
    assert_eq!(stats.checkpoint_count, 2);

    let active = layout
        .session_image(
            &WorkspaceName::session("active").expect("workspace"),
            ImageFormat::Sparse,
        )
        .expect("active");
    create_image(active.image(), ImageFormat::Sparse);
    let trash = layout.project().sessions.join(".trash/retired.sparseimage");
    create_image(&trash, ImageFormat::Sparse);
    let cache_image = fixture.root.join("caches/acme/sessions/cache.sparseimage");
    create_image(&cache_image, ImageFormat::Sparse);

    let report = host.gc(&fixture.config()).expect("gc");
    assert_eq!(report.examined, 2);
    assert_eq!(report.reclaimed, 1);
    assert!(!trash.exists());
    assert!(!sidecar_path(&trash).exists());
    assert!(active.image().exists());
    assert!(
        cache_image.exists(),
        "GC must not descend into the caches volume"
    );
    assert_eq!(runner.calls(), 1, "only the detached session is compacted");
}

#[test]
fn mount_registry_actor_owns_attachment_state_and_blocks_mounted_compaction() {
    let fixture = Fixture::new("mount-registry");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Sparse).expect("image");
    create_image(image.image(), ImageFormat::Sparse);
    let runner = RecordingRunner::default();
    let host = host(&fixture, runner.clone());
    let workspace = workspace(ImageFormat::Sparse);

    let attachment = host
        .attach_verified(image.image(), ImageFormat::Sparse)
        .expect("verified attachment");
    host.mount(&attachment, &fixture.root.join("mounted"), false)
        .expect("mount attachment");
    let mount_id = host
        .retain_mounted(&workspace, attachment)
        .expect("retain attachment");

    assert_eq!(mount_id, 1);
    assert_eq!(
        host.mounts(&repo()).expect("mount facts"),
        [cowshed_core::storage::lifecycle::KernelMountFact {
            mount_id: 1,
            volume_name: "cowshed.acme--widget.main".to_owned(),
        }]
    );
    assert!(
        host.compact(image.image(), ImageFormat::Sparse)
            .expect_err("mounted image must not compact")
            .to_string()
            .contains("cannot compact mounted image")
    );

    host.detach_mounted(&workspace, false)
        .expect("detach retained image");
    assert!(host.mounts(&repo()).expect("mount facts").is_empty());
    assert_eq!(
        runner.calls(),
        5,
        "attach, volume resolution, fsck, mount, and detach cross the command boundary"
    );
}

#[test]
fn marker_validation_checks_every_detached_identity_dimension() {
    let fixture = Fixture::new("marker");
    let host = host(&fixture, RecordingRunner::default());
    let workspace = workspace(ImageFormat::Sparse);
    let mount = fixture.root.join("mounted");
    host.write_marker(&mount, &workspace, None)
        .expect("write marker");
    let expected = MarkerExpectation {
        repo: workspace.repo().clone(),
        workspace: workspace.name().clone(),
        incarnation: workspace.incarnation().clone(),
        format: workspace.format(),
    };
    host.validate_marker(&mount, &expected)
        .expect("matching marker");

    let mismatches = [
        MarkerExpectation {
            repo: RepoId::parse("other/widget").expect("repo"),
            ..expected.clone()
        },
        MarkerExpectation {
            workspace: WorkspaceName::session("other").expect("workspace"),
            ..expected.clone()
        },
        MarkerExpectation {
            incarnation: WorkspaceIncarnation::new("00000000000000000000000000000009")
                .expect("incarnation"),
            ..expected.clone()
        },
        MarkerExpectation {
            format: ImageFormat::Asif,
            ..expected
        },
    ];
    for mismatch in mismatches {
        assert!(matches!(
            host.validate_marker(&mount, &mismatch),
            Err(ApfsStorageError::MarkerMismatch(_))
        ));
    }
}

#[test]
fn retirement_moves_image_and_sidecar_atomically_and_reclaim_is_idempotent() {
    let fixture = Fixture::new("retire");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let trash = layout
        .project()
        .sessions
        .join(".trash/main-retired.sparseimage");
    let host = host(&fixture, RecordingRunner::default());

    host.retire_image(canonical.image(), &trash)
        .expect("retire");
    assert!(!canonical.image().exists());
    assert!(!sidecar_path(canonical.image()).exists());
    assert!(trash.exists());
    assert!(sidecar_path(&trash).exists());

    host.reclaim_image(&trash, ImageFormat::Sparse)
        .expect("first reclaim");
    host.reclaim_image(&trash, ImageFormat::Sparse)
        .expect("idempotent reclaim");
    assert!(!trash.exists());
    assert!(!sidecar_path(&trash).exists());
}

#[test]
fn lifecycle_create_selection_mismatches_fail_before_native_commands() {
    let fixture = Fixture::new("create-selection");
    let runner = RecordingRunner::default();
    let host = host(&fixture, runner.clone());
    let exact_asif = CreateImageRequest {
        staged_stem: fixture.root.join(".staging/main"),
        capacity: "1g".to_owned(),
        volume_name: "cowshed.acme--widget.main".to_owned(),
        case_sensitivity: ApfsCaseSensitivity::Insensitive,
        image_format: ImageFormatSelection::Exact(ImageFormat::Asif),
    };
    assert!(matches!(
        host.create_staged(&exact_asif, ImageFormat::Asif),
        Err(ApfsStorageError::InvalidPlan(_))
    ));
    let auto_sparse = CreateImageRequest {
        image_format: ImageFormatSelection::Auto,
        ..exact_asif
    };
    assert!(matches!(
        host.create_staged(&auto_sparse, ImageFormat::Sparse),
        Err(ApfsStorageError::InvalidPlan(_))
    ));
    assert_eq!(runner.calls(), 0);
}

#[test]
fn canonical_publication_rejects_a_sidecar_only_destination_without_effects() {
    let fixture = Fixture::new("publish-sidecar-conflict");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main.sparseimage");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::create_dir_all(canonical.image().parent().expect("parent")).expect("parent");
    std::fs::write(sidecar_path(canonical.image()), b"occupied").expect("sidecar conflict");
    let host = host(&fixture, RecordingRunner::default());

    host.publish_image(&staged, canonical.image())
        .expect_err("sidecar-only destination is a conflict");

    assert!(staged.exists());
    assert!(sidecar_path(&staged).exists());
    assert!(!canonical.image().exists());
}

#[test]
fn restore_swap_rejects_an_undo_sidecar_without_touching_generations() {
    let fixture = Fixture::new("undo-sidecar-conflict");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-next.sparseimage");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-next.sparseimage");
    create_image(canonical.image(), ImageFormat::Sparse);
    std::fs::write(canonical.image(), b"old").expect("old");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::write(&staged, b"new").expect("new");
    std::fs::create_dir_all(undo.parent().expect("undo parent")).expect("undo parent");
    std::fs::write(sidecar_path(&undo), b"occupied").expect("undo sidecar");
    let host = host(&fixture, RecordingRunner::default());

    host.restore_swap(&staged, canonical.image(), &undo)
        .expect_err("undo sidecar is a conflict");

    assert_eq!(std::fs::read(canonical.image()).expect("canonical"), b"old");
    assert_eq!(std::fs::read(&staged).expect("staged"), b"new");
    assert!(!undo.exists());
}

#[test]
fn metadata_publication_writes_the_requested_identity_and_revision() {
    let fixture = Fixture::new("metadata-publication");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Sparse).expect("image");
    std::fs::create_dir_all(image.image().parent().expect("parent")).expect("parent");
    std::fs::write(image.image(), b"image").expect("image");
    let host = host(&fixture, RecordingRunner::default());
    let workspace = workspace(ImageFormat::Sparse);

    host.publish_metadata(
        image.image(),
        &workspace,
        Revision::new(17),
        MetadataPolicy::Fresh,
        None,
    )
    .expect("publish metadata");

    let published =
        DetachedWorkspaceMetadata::read_for_image(image.image()).expect("published metadata");
    assert_eq!(published.repo_id, *workspace.repo());
    assert_eq!(published.workspace, *workspace.name());
    assert_eq!(published.workspace_incarnation, *workspace.incarnation());
    assert_eq!(published.image_format, workspace.format());
    assert_eq!(published.grants.revision, 17);
}

#[test]
fn canonical_identity_mismatches_are_rejected_one_dimension_at_a_time() {
    for (test, mutate) in [("repo", 0_u8), ("workspace", 1_u8)] {
        let fixture = Fixture::new(test);
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
        create_image(canonical.image(), ImageFormat::Sparse);
        let mut mismatched = metadata(ImageFormat::Sparse);
        if mutate == 0 {
            mismatched.repo_id = RepoId::parse("other/widget").expect("other repo");
        } else {
            mismatched.workspace = WorkspaceName::session("other").expect("other workspace");
        }
        mismatched
            .write_for_image(canonical.image())
            .expect("mismatched metadata");
        let host = host(&fixture, RecordingRunner::default());

        assert!(
            host.list(&repo())
                .expect_err("identity mismatch")
                .to_string()
                .contains("detached metadata identity mismatch")
        );
    }
}

#[test]
fn checkpoint_observation_reads_authoritative_detached_metadata() {
    let fixture = Fixture::new("checkpoint-observe");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let label = cowshed_core::storage::CheckpointLabel::new("ready").expect("label");
    let checkpoint = layout
        .checkpoint_image(
            &WorkspaceName::new("main").expect("main"),
            &label,
            ImageFormat::Sparse,
        )
        .expect("checkpoint");
    create_image(checkpoint.image(), ImageFormat::Sparse);
    let host = host(&fixture, RecordingRunner::default());

    let observed = host
        .observe(&[ExpectedState::Checkpoint {
            repo: repo(),
            workspace: WorkspaceName::new("main").expect("main"),
            label: label.clone(),
            revision: Revision::new(0),
        }])
        .expect("observe checkpoint");

    assert!(matches!(
        observed.as_slice(),
        [cowshed_core::storage::lifecycle::ObservedState::Checkpoint {
            repo: observed_repo,
            workspace,
            label: observed_label,
            revision,
        }] if observed_repo == &repo()
            && workspace.is_main()
            && observed_label == &label
            && *revision == Revision::new(0)
    ));
}

#[test]
fn missing_and_invalid_gc_namespaces_have_distinct_behavior() {
    let fixture = Fixture::new("gc-missing");
    let project = fixture.root.join("acme/widget");
    std::fs::create_dir_all(&project).expect("project");
    let host = host(&fixture, RecordingRunner::default());
    assert_eq!(
        host.gc(&fixture.config()).expect("missing namespaces"),
        Default::default()
    );

    std::fs::create_dir_all(project.join("sessions")).expect("sessions");
    assert_eq!(
        host.gc(&fixture.config()).expect("missing trash"),
        Default::default()
    );
    std::fs::write(project.join("sessions/.trash"), b"not a directory").expect("trash file");
    assert!(host.gc(&fixture.config()).is_err());

    std::fs::remove_file(project.join("sessions/.trash")).expect("remove trash file");
    std::fs::remove_dir(project.join("sessions")).expect("remove sessions");
    std::fs::write(project.join("sessions"), b"not a directory").expect("sessions file");
    assert!(host.gc(&fixture.config()).is_err());
}

#[test]
fn stats_distinguish_a_missing_checkpoint_directory_from_an_invalid_one() {
    let fixture = Fixture::new("stats-missing");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let host = host(&fixture, RecordingRunner::default());
    assert_eq!(
        host.stats(&workspace(ImageFormat::Sparse), canonical.image())
            .expect("missing checkpoint directory")
            .checkpoint_count,
        0
    );

    let checkpoint_path = layout.project().checkpoints.join("main");
    std::fs::create_dir_all(checkpoint_path.parent().expect("parent")).expect("parent");
    std::fs::write(&checkpoint_path, b"not a directory").expect("checkpoint file");
    assert!(
        host.stats(&workspace(ImageFormat::Sparse), canonical.image())
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn chown_rejects_a_nul_path_before_the_native_call() {
    use std::os::unix::ffi::OsStringExt;

    let fixture = Fixture::new("chown-nul");
    let host = host(&fixture, RecordingRunner::default());
    host.chown_volume_root(&fixture.root)
        .expect("chown owned directory to current user");
    let invalid = PathBuf::from(std::ffi::OsString::from_vec(b"invalid\0path".to_vec()));
    assert!(
        host.chown_volume_root(&invalid)
            .expect_err("NUL path")
            .to_string()
            .contains("contains NUL")
    );
}

#[test]
fn reverse_teardown_drains_actor_state_and_detaches_every_attachment() {
    let fixture = Fixture::new("reverse-teardown");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Sparse).expect("image");
    create_image(image.image(), ImageFormat::Sparse);
    let runner = RecordingRunner::default();
    let host = host(&fixture, runner.clone());
    let attachment = host
        .attach_verified(image.image(), ImageFormat::Sparse)
        .expect("attachment");
    host.retain_mounted(&workspace(ImageFormat::Sparse), attachment)
        .expect("retain");

    host.detach_all_reverse().expect("reverse detach");

    assert!(host.mounts(&repo()).expect("mount facts").is_empty());
    assert_eq!(runner.calls(), 4, "attach, resolve, fsck, detach");
}

#[test]
fn sidecar_removal_does_not_hide_non_file_errors() {
    let fixture = Fixture::new("sidecar-error");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Sparse).expect("image");
    std::fs::create_dir_all(image.image().parent().expect("parent")).expect("parent");
    std::fs::write(image.image(), b"image").expect("image");
    std::fs::create_dir_all(sidecar_path(image.image())).expect("sidecar directory");
    let host = host(&fixture, RecordingRunner::default());

    assert!(
        host.reclaim_image(image.image(), ImageFormat::Sparse)
            .is_err()
    );
}

#[test]
fn failed_detach_restores_actor_state_and_force_retries_exactly_once() {
    let fixture = Fixture::new("detach-retry");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Sparse).expect("image");
    create_image(image.image(), ImageFormat::Sparse);
    let runner = FailingDetachRunner::new(1);
    let host = MacOsApfsExecutionHost::new(
        runner.clone(),
        fixture.config(),
        WorkspaceMetadataTemplate {
            project_root: fixture.root.join("project"),
            base_commit: "0123456789abcdef".to_owned(),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            created_trace: "detach-retry".to_owned(),
            grants: GrantSet::closed_baseline(Some(PortBlock::new(20000, 16).expect("port block")))
                .expect("grants"),
        },
    )
    .expect("host");
    let workspace = workspace(ImageFormat::Sparse);
    let attachment = host
        .attach_verified(image.image(), ImageFormat::Sparse)
        .expect("attachment");
    host.retain_mounted(&workspace, attachment).expect("retain");

    host.detach_mounted(&workspace, false)
        .expect_err("busy detach must fail");
    assert_eq!(host.mounts(&repo()).expect("restored fact").len(), 1);

    runner.failures_remaining.store(1, Ordering::SeqCst);
    host.detach_mounted(&workspace, true).expect("forced retry");
    assert!(host.mounts(&repo()).expect("drained fact").is_empty());
}

#[test]
fn direct_detach_crosses_the_backend_boundary() {
    let fixture = Fixture::new("direct-detach");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Sparse).expect("image");
    create_image(image.image(), ImageFormat::Sparse);
    let runner = RecordingRunner::default();
    let host = host(&fixture, runner.clone());
    let attachment = host
        .attach_verified(image.image(), ImageFormat::Sparse)
        .expect("attachment");

    host.detach(attachment, false).expect("detach");

    assert_eq!(runner.calls(), 4, "attach, resolve, fsck, detach");
}

#[test]
fn gc_distinguishes_a_missing_store_from_a_non_directory_store() {
    let fixture = Fixture::new("gc-store");
    let missing_root = fixture.root.join("missing-store");
    let config = ApfsSubstrateConfig::new(
        &missing_root,
        missing_root.join("caches"),
        fixture.root.join("mount"),
        ApfsCaseSensitivity::Insensitive,
    );
    let host = MacOsApfsExecutionHost::new(
        RecordingRunner::default(),
        config.clone(),
        WorkspaceMetadataTemplate {
            project_root: fixture.root.join("project"),
            base_commit: "0123456789abcdef".to_owned(),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            created_trace: "gc-store".to_owned(),
            grants: GrantSet::closed_baseline(Some(PortBlock::new(20000, 16).expect("port block")))
                .expect("grants"),
        },
    )
    .expect("host");
    assert_eq!(host.gc(&config).expect("missing store"), Default::default());

    std::fs::write(&missing_root, b"not a directory").expect("store file");
    assert!(host.gc(&config).is_err());
}

#[test]
fn session_identity_mismatches_are_rejected_one_dimension_at_a_time() {
    for (test, mutate) in [("session-repo", 0_u8), ("session-name", 1_u8)] {
        let fixture = Fixture::new(test);
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let session_name = WorkspaceName::session("raven").expect("session");
        let image = layout
            .session_image(&session_name, ImageFormat::Sparse)
            .expect("session image");
        create_image(image.image(), ImageFormat::Sparse);
        let mut mismatched = metadata(ImageFormat::Sparse);
        mismatched.workspace = session_name;
        if mutate == 0 {
            mismatched.repo_id = RepoId::parse("other/widget").expect("other repo");
        } else {
            mismatched.workspace = WorkspaceName::session("other").expect("other workspace");
        }
        mismatched
            .write_for_image(image.image())
            .expect("mismatched sidecar");
        let host = host(&fixture, RecordingRunner::default());

        assert!(
            host.list(&repo())
                .expect_err("session identity mismatch")
                .to_string()
                .contains("detached metadata identity mismatch")
        );
    }
}

#[test]
fn published_listing_distinguishes_missing_sessions_from_an_invalid_sessions_path() {
    let fixture = Fixture::new("invalid-sessions-list");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let host = host(&fixture, RecordingRunner::default());
    assert_eq!(host.list(&repo()).expect("missing sessions").len(), 1);

    std::fs::write(&layout.project().sessions, b"not a directory").expect("sessions file");
    assert!(host.list(&repo()).is_err());
}
