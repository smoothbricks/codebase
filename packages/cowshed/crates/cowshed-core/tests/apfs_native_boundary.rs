use std::fs::{FileTimes, OpenOptions};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};

use cowshed_core::apfs::{
    ApfsCaseSensitivity, CommandOutput, CommandRequest, CommandRunError, CommandRunner,
    CreateImageRequest, ImageFormatSelection, MountAccess,
};
use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, GrantSet, ImageFormat, METADATA_VERSION, Platform, PortBlock,
    PublicationState, WorkspaceIncarnation, WorkspaceName, WorkspaceRole, sidecar_path,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::apfs::native::{
    KernelMountSnapshot, KernelMountSource, MacOsApfsExecutionHost, RecoveryMarkerSource,
    RestoreFailpoint, SystemKernelMountSource,
};
use cowshed_core::storage::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, LockMode, MarkerExpectation,
    MetadataPolicy, PublicationDisposition,
};
use cowshed_core::storage::lifecycle::{
    ExpectedState, LifecycleWorkspace, OperationIdentity, Pin, Revision, StorageGcReason,
};
use cowshed_core::storage::{CheckpointLabel, StorageLayout, StorageLayoutError};
use cowshed_core::workspace_credentials::mint_workspace_credentials;
const ATTACH_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>system-entities</key><array>
<dict><key>content-hint</key><string>GUID_partition_scheme</string><key>dev-entry</key><string>/dev/disk9</string></dict>
<dict><key>content-hint</key><string>Apple_APFS</string><key>dev-entry</key><string>/dev/disk9s2</string></dict>
<dict><key>content-hint</key><string>41504653-0000-11AA-AA11-00306543ECAC</string><key>dev-entry</key><string>/dev/disk10s1</string><key>potentially-mountable</key><true/><key>volume-kind</key><string>apfs</string></dict>
</array></dict></plist>"#;

const APFS_LIST_PLIST: &str = r#"<?xml version="1.0"?><plist><dict><key>Containers</key><array><dict>
<key>PhysicalStores</key><array><dict><key>DeviceIdentifier</key><string>disk9s2</string></dict></array>
<key>Volumes</key><array><dict><key>DeviceIdentifier</key><string>disk10s1</string></dict></array>
</dict></array></dict></plist>"#;

const EMPTY_ATTACHMENT_INVENTORY: &str =
    r#"<?xml version="1.0"?><plist><dict><key>images</key><array/></dict></plist>"#;

fn successful_output(request: &CommandRequest) -> CommandOutput {
    let args: Vec<_> = request
        .args
        .iter()
        .map(|argument| argument.to_string_lossy())
        .collect();
    let stdout = if request.program == Path::new("/usr/bin/hdiutil") && args == ["info", "-plist"] {
        EMPTY_ATTACHMENT_INVENTORY.as_bytes().to_vec()
    } else if args.first().is_some_and(|argument| argument == "attach") {
        ATTACH_PLIST.as_bytes().to_vec()
    } else if args.starts_with(&["apfs".into(), "list".into()]) {
        APFS_LIST_PLIST.as_bytes().to_vec()
    } else {
        Vec::new()
    };
    CommandOutput::success(stdout)
}

#[derive(Clone)]
struct RecordingRunner {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<CommandRequest>>>,
    volume_name: Arc<Mutex<String>>,
}

impl Default for RecordingRunner {
    fn default() -> Self {
        Self {
            calls: Arc::default(),
            requests: Arc::default(),
            volume_name: Arc::new(Mutex::new("cowshed.acme--widget.main".to_owned())),
        }
    }
}

impl RecordingRunner {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<CommandRequest> {
        self.requests.lock().expect("requests").clone()
    }

    fn set_volume_name(&self, volume_name: &str) {
        *self.volume_name.lock().expect("volume name") = volume_name.to_owned();
    }
}

impl CommandRunner for RecordingRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests
            .lock()
            .expect("requests")
            .push(request.clone());
        let is_info = request.program == Path::new("/usr/sbin/diskutil")
            && request
                .args
                .first()
                .is_some_and(|argument| argument == "info");
        if is_info {
            let device = request
                .args
                .last()
                .expect("device")
                .to_string_lossy()
                .trim_start_matches("/dev/")
                .to_owned();
            let volume_name = self.volume_name.lock().expect("volume name").clone();
            Ok(CommandOutput::success(
                format!(
                    "<?xml version=\"1.0\"?><plist><dict><key>DeviceIdentifier</key><string>{device}</string><key>VolumeName</key><string>{volume_name}</string></dict></plist>"
                )
                .into_bytes(),
            ))
        } else {
            Ok(successful_output(request))
        }
    }
}

#[derive(Clone)]
struct FailingDetachRunner {
    calls: Arc<AtomicUsize>,
    failures_remaining: Arc<AtomicUsize>,
    detach_attempts: Arc<Mutex<Vec<bool>>>,
}

impl FailingDetachRunner {
    fn new(failures: usize) -> Self {
        Self {
            calls: Arc::default(),
            failures_remaining: Arc::new(AtomicUsize::new(failures)),
            detach_attempts: Arc::default(),
        }
    }
}

impl CommandRunner for FailingDetachRunner {
    fn run(&self, request: &CommandRequest) -> Result<CommandOutput, CommandRunError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if request.program == Path::new("/usr/sbin/diskutil")
            && request
                .args
                .first()
                .is_some_and(|argument| argument == "info")
        {
            let device = request
                .args
                .last()
                .expect("device")
                .to_string_lossy()
                .trim_start_matches("/dev/")
                .to_owned();
            return Ok(CommandOutput::success(
                format!(
                    "<?xml version=\"1.0\"?><plist><dict><key>DeviceIdentifier</key><string>{device}</string><key>VolumeName</key><string>cowshed.acme--widget.main</string></dict></plist>"
                )
                .into_bytes(),
            ));
        }
        let is_detach = request
            .args
            .first()
            .is_some_and(|argument| argument == "detach");
        if is_detach {
            self.detach_attempts
                .lock()
                .expect("detach attempts")
                .push(request.args.iter().any(|argument| argument == "-force"));
        }
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

#[derive(Clone, Default)]
struct FakeKernelMountSource(Arc<Mutex<Vec<KernelMountSnapshot>>>);

impl FakeKernelMountSource {
    fn set(&self, mounts: Vec<KernelMountSnapshot>) {
        *self.0.lock().expect("kernel mounts") = mounts;
    }
}

impl KernelMountSource for FakeKernelMountSource {
    fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
        Ok(self.0.lock().expect("kernel mounts").clone())
    }
}

#[derive(Clone, Copy)]
struct ByteRecoveryMarkers;

impl RecoveryMarkerSource for ByteRecoveryMarkers {
    fn incarnation(&self, image: &Path) -> Result<String, ApfsStorageError> {
        match std::fs::read(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?
            .as_slice()
        {
            b"old generation" => Ok("00000000000000000000000000000001".to_owned()),
            b"new generation" => Ok("00000000000000000000000000000002".to_owned()),
            _ => Err(ApfsStorageError::Host(format!(
                "unexpected recovery image bytes: {}",
                image.display()
            ))),
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

fn execute_gc<R>(
    host: &MacOsApfsExecutionHost<R>,
    config: &ApfsSubstrateConfig,
) -> Result<cowshed_core::storage::lifecycle::StorageGcReport, ApfsStorageError>
where
    R: CommandRunner + Send + Sync + 'static,
{
    let plan = host.preview_gc(config, &repo())?;
    host.execute_gc(config, plan)
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
        publication_state: PublicationState::Active,
        updated_at: "2026-07-13T00:00:00Z".to_owned(),
        grants: GrantSet::closed_baseline(Some(PortBlock::new(20000, 16).expect("port block")))
            .expect("grants"),
        info_snapshot: None,
    }
}

fn native_host(
    fixture: &Fixture,
    runner: RecordingRunner,
) -> MacOsApfsExecutionHost<RecordingRunner> {
    MacOsApfsExecutionHost::with_recovery_sources(
        runner,
        fixture.config(),
        SystemKernelMountSource,
        ByteRecoveryMarkers,
    )
    .expect("native APFS host")
}

fn native_host_at(root: &Path) -> MacOsApfsExecutionHost<RecordingRunner> {
    MacOsApfsExecutionHost::with_recovery_sources(
        RecordingRunner::default(),
        ApfsSubstrateConfig::new(
            root,
            root.join("caches"),
            root.join("mount"),
            ApfsCaseSensitivity::Insensitive,
        ),
        SystemKernelMountSource,
        ByteRecoveryMarkers,
    )
    .expect("native APFS host")
}

fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn identity(fixture: &Fixture) -> OperationIdentity {
    OperationIdentity {
        project_root: fixture.root.join("project"),
        base_commit: "0123456789abcdef".to_owned(),
        created_at: "2026-07-13T00:00:00Z".to_owned(),
        created_trace: "trace-apfs-boundary".to_owned(),
        grants: GrantSet::closed_baseline(Some(PortBlock::new(20000, 16).expect("port block")))
            .expect("grants"),
    }
}

fn set_modified(path: &Path, seconds_since_epoch: u64) {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open image for timestamp");
    file.set_times(
        FileTimes::new()
            .set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds_since_epoch)),
    )
    .expect("set image timestamp");
}

fn make_old(path: &Path) {
    set_modified(path, 946_684_800);
}

fn make_future(path: &Path) {
    set_modified(path, 4_102_444_800);
}

fn checkpoint_fact_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(".checkpoint.json");
    PathBuf::from(path)
}

fn restore_recovery_fact_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(".restore.json");
    PathBuf::from(path)
}

fn ca_key_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(".ca.key");
    PathBuf::from(path)
}

fn write_ca_key(image: &Path, contents: &[u8]) {
    let path = ca_key_path(image);
    std::fs::write(&path, contents).expect("CA key");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("CA key mode");
}

fn create_image(path: &Path, format: ImageFormat) {
    std::fs::create_dir_all(path.parent().expect("parent")).expect("image parent");
    std::fs::write(path, b"fixture").expect("image");
    metadata(format).write_for_image(path).expect("sidecar");
    let signing_key =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("fixture P-256 private key");
    write_ca_key(path, signing_key.serialize_pem().as_bytes());
}

fn workspace(format: ImageFormat) -> LifecycleWorkspace {
    LifecycleWorkspace::new(
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
    let host = native_host(&fixture, runner.clone());

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
    let host = native_host(&fixture, runner.clone());

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
    let host = native_host(&fixture, runner.clone());

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
    let host = native_host(&fixture, RecordingRunner::default());

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
fn restore_swap_keeps_old_metadata_until_verified_publication_and_rolls_back_generations() {
    let fixture = Fixture::new("restore-swap");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.sparseimage");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.sparseimage");
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
    let host = native_host(&fixture, RecordingRunner::default());

    host.restore_swap(&staged, canonical.image(), &undo)
        .expect("restore swap");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"new generation"
    );
    assert_eq!(std::fs::read(&undo).expect("undo"), b"old generation");
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("pre-publication metadata")
            .workspace_incarnation,
        metadata(ImageFormat::Sparse).workspace_incarnation
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
    let first = checkpoints.join("one.sparseimage");
    let second = checkpoints.join("two.asif");
    create_image(&first, ImageFormat::Sparse);
    create_image(&second, ImageFormat::Asif);
    std::fs::write(checkpoints.join("not-an-image.txt"), b"ignored").expect("noise");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    host.publish_checkpoint_fact(
        &first,
        &CheckpointLabel::new("one").expect("label"),
        Revision::new(2),
        Pin::Pinned,
    )
    .expect("pinned fact");
    host.publish_checkpoint_fact(
        &second,
        &CheckpointLabel::new("two").expect("label"),
        Revision::new(3),
        Pin::Automatic,
    )
    .expect("automatic fact");

    let stats = host
        .stats(&workspace(ImageFormat::Sparse), canonical.image())
        .expect("stats");
    assert_eq!(stats.logical_bytes, b"fixture".len() as u64);
    assert_eq!(stats.checkpoint_count, 2);
    let first_bytes = std::fs::metadata(&first)
        .expect("first metadata")
        .blocks()
        .saturating_mul(512);
    let second_bytes = std::fs::metadata(&second)
        .expect("second metadata")
        .blocks()
        .saturating_mul(512);
    assert_eq!(stats.checkpoint_bytes, first_bytes + second_bytes);
    assert_eq!(stats.pinned_checkpoint_bytes, first_bytes);

    let active_name = WorkspaceName::session("active").expect("workspace");
    let active = layout
        .session_image(&active_name, ImageFormat::Sparse)
        .expect("active");
    create_image(active.image(), ImageFormat::Sparse);
    let mut active_metadata = metadata(ImageFormat::Sparse);
    active_metadata.workspace = active_name;
    active_metadata
        .write_for_image(active.image())
        .expect("active metadata");
    let trash = layout.project().sessions.join(".trash/retired.sparseimage");
    create_image(&trash, ImageFormat::Sparse);
    let cache_image = fixture.root.join("caches/acme/sessions/cache.sparseimage");
    create_image(&cache_image, ImageFormat::Sparse);

    let report = execute_gc(&host, &fixture.config()).expect("gc");
    assert_eq!(report.examined, 4);
    assert_eq!(report.reclaimed, 2);
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
    let host = native_host(&fixture, runner.clone());
    let workspace = workspace(ImageFormat::Sparse);

    let attachment = host
        .attach_verified(image.image(), ImageFormat::Sparse)
        .expect("verified attachment");
    host.mount(
        &attachment,
        &fixture.root.join("mounted"),
        MountAccess::ReadWrite,
        false,
    )
    .expect("mount attachment");
    let mount_id = host
        .retain_mounted(&workspace, attachment)
        .expect("retain attachment");

    assert_eq!(mount_id, 1);

    host.detach_mounted(&workspace, false)
        .expect("detach retained image");
    assert_eq!(
        runner.calls(),
        6,
        "inventory, attach, volume resolution, fsck, mount, and detach cross the command boundary"
    );
}

#[test]
fn marker_validation_checks_every_detached_identity_dimension() {
    let fixture = Fixture::new("marker");
    let host = native_host(&fixture, RecordingRunner::default());
    let workspace = workspace(ImageFormat::Sparse);
    let mount = fixture.root.join("mounted");
    host.write_marker(&mount, &workspace, None, &identity(&fixture))
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
    let host = native_host(&fixture, RecordingRunner::default());

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
    let host = native_host(&fixture, runner.clone());
    let exact_asif = CreateImageRequest {
        staged_stem: fixture.root.join(".staging/main"),
        capacity: "1g".to_owned(),
        volume_name: "cowshed.acme--widget.main".to_owned(),
        case_sensitivity: ApfsCaseSensitivity::Insensitive,
        owner_uid: unsafe { libc::getuid() },
        owner_gid: unsafe { libc::getgid() },
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
    let host = native_host(&fixture, RecordingRunner::default());

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
        .join(".staging/main-00000000000000000000000000000002.sparseimage");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.sparseimage");
    create_image(canonical.image(), ImageFormat::Sparse);
    std::fs::write(canonical.image(), b"old").expect("old");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::write(&staged, b"new").expect("new");
    std::fs::create_dir_all(undo.parent().expect("undo parent")).expect("undo parent");
    std::fs::write(sidecar_path(&undo), b"occupied").expect("undo sidecar");
    let host = native_host(&fixture, RecordingRunner::default());

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
    let host = native_host(&fixture, RecordingRunner::default());
    let workspace = workspace(ImageFormat::Sparse);

    host.publish_metadata(
        image.image(),
        &workspace,
        Revision::new(17),
        MetadataPolicy::Fresh,
        Some(&identity(&fixture)),
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
        let host = native_host(&fixture, RecordingRunner::default());

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
    let host = native_host(&fixture, RecordingRunner::default());

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
fn checkpoint_enumeration_reads_regular_asif_and_sparseimage_files() {
    let fixture = Fixture::new("checkpoint-regular-files");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let main = WorkspaceName::new("main").expect("main");
    let host = native_host(&fixture, RecordingRunner::default());

    for (name, format, revision, pin) in [
        ("asif", ImageFormat::Asif, 11, Pin::Pinned),
        ("sparse", ImageFormat::Sparse, 12, Pin::Automatic),
    ] {
        let label = CheckpointLabel::new(name).expect("label");
        let image = layout
            .checkpoint_image(&main, &label, format)
            .expect("checkpoint")
            .image()
            .to_owned();
        create_image(&image, format);
        host.publish_checkpoint_fact(&image, &label, Revision::new(revision), pin)
            .expect("checkpoint fact");
    }

    let facts = host.checkpoints(&repo()).expect("checkpoint facts");
    let observed = facts
        .iter()
        .map(|fact| (fact.label.as_str(), fact.revision, fact.pin))
        .collect::<Vec<_>>();
    assert_eq!(
        observed,
        vec![
            ("asif", Revision::new(11), Pin::Pinned),
            ("sparse", Revision::new(12), Pin::Automatic),
        ]
    );
    assert!(
        facts
            .iter()
            .all(|fact| fact.repo == repo() && fact.workspace == main)
    );
}

#[test]
fn checkpoint_gc_reclaims_only_expired_automatic_regular_files_and_sidecars() {
    let fixture = Fixture::new("checkpoint-retention");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let main = WorkspaceName::new("main").expect("main");
    let host = native_host(&fixture, RecordingRunner::default());
    let create_checkpoint = |name: &str, revision: u64, pin: Pin, format: ImageFormat| {
        let label = CheckpointLabel::new(name).expect("label");
        let image = layout
            .checkpoint_image(&main, &label, format)
            .expect("checkpoint")
            .image()
            .to_owned();
        create_image(&image, format);
        host.publish_checkpoint_fact(&image, &label, Revision::new(revision), pin)
            .expect("checkpoint fact");
        image
    };

    let mut newest_five = Vec::new();
    for index in 0..5 {
        let format = if index % 2 == 0 {
            ImageFormat::Asif
        } else {
            ImageFormat::Sparse
        };
        let image = create_checkpoint(
            &format!("future-{index}"),
            index + 1,
            Pin::Automatic,
            format,
        );
        make_future(&image);
        newest_five.push(image);
    }
    let young = create_checkpoint("young", 6, Pin::Automatic, ImageFormat::Asif);
    let expired = create_checkpoint("expired", 7, Pin::Automatic, ImageFormat::Sparse);
    make_old(&expired);
    let pinned = create_checkpoint("pinned-old", 8, Pin::Pinned, ImageFormat::Asif);
    make_old(&pinned);
    drop(host);

    let restarted = native_host(&fixture, RecordingRunner::default());
    assert_eq!(
        restarted.checkpoints(&repo()).expect("restart facts").len(),
        8
    );

    let plan = restarted.preview_gc(&config, &repo()).expect("GC preview");
    let repeated = restarted
        .preview_gc(&config, &repo())
        .expect("repeated preview");
    assert_eq!(plan.candidates().len(), 1);
    assert_eq!(
        plan.candidates()[0].identity(),
        repeated.candidates()[0].identity(),
        "unchanged candidates have stable identities"
    );
    let candidate = &plan.candidates()[0];
    assert_eq!(candidate.path(), expired);
    assert_eq!(candidate.reason(), StorageGcReason::ExpiredCheckpoint);
    let expected_bytes = [
        expired.clone(),
        sidecar_path(&expired),
        ca_key_path(&expired),
        checkpoint_fact_path(&expired),
    ]
    .into_iter()
    .map(|path| {
        std::fs::metadata(path)
            .expect("candidate component")
            .blocks()
            .saturating_mul(512)
    })
    .sum::<u64>();
    assert_eq!(candidate.bytes(), expected_bytes);
    assert!(
        plan.candidates()
            .iter()
            .all(|candidate| candidate.path() != pinned),
        "pinned checkpoints are never candidates"
    );
    assert!(expired.exists(), "preview must not mutate");

    restarted
        .publish_checkpoint_fact(
            &expired,
            &CheckpointLabel::new("expired").expect("label"),
            Revision::new(7),
            Pin::Pinned,
        )
        .expect("pin after preview");
    let stale = restarted
        .execute_gc(&config, plan)
        .expect_err("pin change makes plan stale");
    assert!(matches!(stale, ApfsStorageError::GcPlanStale));
    assert!(expired.exists(), "stale execution must not mutate");
    restarted
        .publish_checkpoint_fact(
            &expired,
            &CheckpointLabel::new("expired").expect("label"),
            Revision::new(7),
            Pin::Automatic,
        )
        .expect("restore automatic pin");
    let plan = restarted
        .preview_gc(&config, &repo())
        .expect("fresh preview");
    let report = restarted.execute_gc(&config, plan).expect("checkpoint gc");
    assert_eq!(report.examined, 8);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.retained_pinned, 1);
    assert_eq!(report.retained_recent, 6);
    assert!(newest_five.iter().all(|image| image.exists()));
    assert!(young.exists(), "young automatic checkpoint must survive");
    assert!(pinned.exists(), "old pinned checkpoint must survive");
    assert!(
        !expired.exists(),
        "expired automatic checkpoint is reclaimed"
    );
    assert!(
        !sidecar_path(&expired).exists(),
        "grants sidecar is reclaimed"
    );
    assert!(
        !checkpoint_fact_path(&expired).exists(),
        "checkpoint fact sidecar is reclaimed"
    );
    let surviving = restarted.checkpoints(&repo()).expect("surviving facts");
    assert_eq!(surviving.len(), 7);
    assert!(
        surviving
            .iter()
            .all(|fact| fact.label.as_str() != "expired")
    );
}

#[test]
fn missing_and_invalid_gc_namespaces_have_distinct_behavior() {
    let fixture = Fixture::new("gc-missing");
    let project = fixture.root.join("acme/widget");
    std::fs::create_dir_all(&project).expect("project");
    let host = native_host(&fixture, RecordingRunner::default());
    assert_eq!(
        execute_gc(&host, &fixture.config()).expect("missing namespaces"),
        Default::default()
    );

    std::fs::create_dir_all(project.join("sessions")).expect("sessions");
    assert_eq!(
        execute_gc(&host, &fixture.config()).expect("missing trash"),
        Default::default()
    );
    std::fs::write(project.join("sessions/.trash"), b"not a directory").expect("trash file");
    assert!(execute_gc(&host, &fixture.config()).is_err());

    std::fs::remove_file(project.join("sessions/.trash")).expect("remove trash file");
    std::fs::remove_dir(project.join("sessions")).expect("remove sessions");
    std::fs::write(project.join("sessions"), b"not a directory").expect("sessions file");
    assert!(execute_gc(&host, &fixture.config()).is_err());
}

#[test]
fn stats_distinguish_a_missing_checkpoint_directory_from_an_invalid_one() {
    let fixture = Fixture::new("stats-missing");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let host = native_host(&fixture, RecordingRunner::default());
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
    let host = native_host(&fixture, RecordingRunner::default());
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
    let host = native_host(&fixture, runner.clone());
    let attachment = host
        .attach_verified(image.image(), ImageFormat::Sparse)
        .expect("attachment");
    host.retain_mounted(&workspace(ImageFormat::Sparse), attachment)
        .expect("retain");

    host.detach_all_reverse().expect("reverse detach");

    assert_eq!(
        runner.calls(),
        5,
        "inventory, attach, resolve, fsck, detach"
    );
}

#[test]
fn sidecar_removal_does_not_hide_non_file_errors() {
    let fixture = Fixture::new("sidecar-error");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Sparse).expect("image");
    std::fs::create_dir_all(image.image().parent().expect("parent")).expect("parent");
    std::fs::write(image.image(), b"image").expect("image");
    std::fs::create_dir_all(sidecar_path(image.image())).expect("sidecar directory");
    let host = native_host(&fixture, RecordingRunner::default());

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
    let host = MacOsApfsExecutionHost::new(runner.clone(), fixture.config()).expect("host");
    let workspace = workspace(ImageFormat::Sparse);
    let attachment = host
        .attach_verified(image.image(), ImageFormat::Sparse)
        .expect("attachment");
    host.retain_mounted(&workspace, attachment).expect("retain");

    host.detach_mounted(&workspace, false)
        .expect_err("busy detach must fail");

    runner.failures_remaining.store(1, Ordering::SeqCst);
    host.detach_mounted(&workspace, true).expect("forced retry");
    assert_eq!(runner.calls.load(Ordering::SeqCst), 7);
    assert_eq!(
        *runner.detach_attempts.lock().expect("detach attempts"),
        [false, false, true],
        "failed detach must restore actor ownership, then force retry must cross normal and forced detach targets"
    );
}

#[test]
fn direct_detach_crosses_the_backend_boundary() {
    let fixture = Fixture::new("direct-detach");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let image = layout.main_image(ImageFormat::Sparse).expect("image");
    create_image(image.image(), ImageFormat::Sparse);
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    let attachment = host
        .attach_verified(image.image(), ImageFormat::Sparse)
        .expect("attachment");

    host.detach(attachment, false).expect("detach");

    assert_eq!(
        runner.calls(),
        5,
        "inventory, attach, resolve, fsck, detach"
    );
}

#[test]
fn native_volume_rename_crosses_the_backend_boundary() {
    let fixture = Fixture::new("rename-volume");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    let mount = fixture.root.join("mounted");

    host.rename_volume(&mount, "cowshed.acme--widget.session")
        .expect("rename volume");

    let requests = runner.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].program, Path::new("/usr/sbin/diskutil"));
    assert_eq!(
        requests[0].args,
        [
            std::ffi::OsString::from("renameVolume"),
            mount.into_os_string(),
            std::ffi::OsString::from("cowshed.acme--widget.session"),
        ]
    );
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
    let host =
        MacOsApfsExecutionHost::new(RecordingRunner::default(), config.clone()).expect("host");
    assert_eq!(
        execute_gc(&host, &config).expect("missing store"),
        Default::default()
    );

    std::fs::write(&missing_root, b"not a directory").expect("store file");
    assert!(execute_gc(&host, &config).is_err());
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
        let host = native_host(&fixture, RecordingRunner::default());

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
    let host = native_host(&fixture, RecordingRunner::default());
    assert_eq!(host.list(&repo()).expect("missing sessions").len(), 1);

    std::fs::write(&layout.project().sessions, b"not a directory").expect("sessions file");
    assert!(host.list(&repo()).is_err());
}

#[test]
fn kernel_mount_facts_survive_host_restart_and_prevent_detached_compaction() {
    let fixture = Fixture::new("kernel-restart");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let source = FakeKernelMountSource::default();
    source.set(vec![KernelMountSnapshot::new(
        42,
        fixture.config().main_mount,
        "/dev/disk10s1",
        true,
        true,
    )]);
    let first = MacOsApfsExecutionHost::with_mount_source(
        RecordingRunner::default(),
        fixture.config(),
        source.clone(),
    )
    .expect("first host");
    assert_eq!(first.mounts(&repo()).expect("first facts")[0].mount_id, 42);
    drop(first);

    let runner = RecordingRunner::default();
    runner.set_volume_name("cowshed.acme--widget.main");
    let restarted =
        MacOsApfsExecutionHost::with_mount_source(runner.clone(), fixture.config(), source)
            .expect("restarted host");
    assert_eq!(
        restarted.mounts(&repo()).expect("restart facts")[0].mount_id,
        42
    );
    assert!(
        restarted
            .compact(canonical.image(), ImageFormat::Sparse)
            .expect_err("kernel-mounted image must not compact")
            .to_string()
            .contains("cannot compact mounted image")
    );
    let requests = runner.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| {
        request.program == Path::new("/usr/sbin/diskutil")
            && request
                .args
                .first()
                .is_some_and(|argument| argument == "info")
    }));
    restarted
        .detach_mounted(&workspace(ImageFormat::Sparse), false)
        .expect("restart-safe detach");
    let detach = runner.requests().last().cloned().expect("detach request");
    assert_eq!(detach.program, Path::new("/usr/bin/hdiutil"));
    assert_eq!(
        detach.args,
        [
            std::ffi::OsString::from("detach"),
            std::ffi::OsString::from("-quiet"),
            fixture.config().main_mount.into_os_string(),
        ]
    );
}

#[test]
fn restart_safe_detach_honors_force_only_after_normal_detach_fails() {
    let fixture = Fixture::new("kernel-restart-force");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let source = FakeKernelMountSource::default();
    source.set(vec![KernelMountSnapshot::new(
        43,
        fixture.config().main_mount,
        "/dev/disk10s1",
        true,
        true,
    )]);
    let runner = FailingDetachRunner::new(2);
    let host = MacOsApfsExecutionHost::with_mount_source(runner.clone(), fixture.config(), source)
        .expect("host");

    host.detach_mounted(&workspace(ImageFormat::Sparse), false)
        .expect_err("non-forced restart detach must not escalate");
    host.detach_mounted(&workspace(ImageFormat::Sparse), true)
        .expect("forced restart detach");
    assert_eq!(
        *runner.detach_attempts.lock().expect("detach attempts"),
        [false, false, true]
    );
}

#[test]
fn canonical_path_with_unrelated_volume_fails_closed_without_detaching() {
    let fixture = Fixture::new("wrong-source");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let source = FakeKernelMountSource::default();
    source.set(vec![KernelMountSnapshot::new(
        8,
        fixture.config().main_mount,
        "/dev/disk10s1",
        true,
        true,
    )]);
    let runner = RecordingRunner::default();
    runner.set_volume_name("unrelated.volume");
    let host = MacOsApfsExecutionHost::with_mount_source(runner.clone(), fixture.config(), source)
        .expect("host");
    let error = host.mounts(&repo()).expect_err("impostor mount");
    assert!(error.to_string().contains("mount source mismatch"));
    let error = host
        .heal_mount(
            &workspace(ImageFormat::Sparse),
            &fixture.config().main_mount,
        )
        .expect_err("impostor must not be healed destructively");
    assert!(
        error
            .to_string()
            .contains("refusing to heal unrelated mount")
    );
    let error = host
        .detach_mounted(&workspace(ImageFormat::Sparse), true)
        .expect_err("restart-safe detach must reject an impostor source");
    assert!(
        error
            .to_string()
            .contains("refusing to detach unrelated mount")
    );
    assert!(
        runner.requests().iter().all(|request| request
            .args
            .first()
            .is_some_and(|argument| argument == "info")),
        "volume resolution may run, but impostor mount must never be detached"
    );
}

#[test]
fn wrong_kernel_mount_flags_are_detected_and_healed_by_mountpoint() {
    let fixture = Fixture::new("wrong-flags");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    let source = FakeKernelMountSource::default();
    source.set(vec![KernelMountSnapshot::new(
        7,
        fixture.config().main_mount,
        "/dev/disk10s1",
        false,
        false,
    )]);
    let runner = RecordingRunner::default();
    let host =
        MacOsApfsExecutionHost::with_mount_source(runner.clone(), fixture.config(), source.clone())
            .expect("host");
    let workspace = workspace(ImageFormat::Sparse);
    assert!(
        host.mounts(&repo())
            .expect_err("wrong flags")
            .to_string()
            .contains("non-canonical flags")
    );

    host.heal_mount(&workspace, &fixture.config().main_mount)
        .expect("heal by mountpoint");
    let requests = runner.requests();
    assert_eq!(
        requests.len(),
        4,
        "three source-bound identity reads and one detach"
    );
    let detach = requests.last().expect("detach request");
    assert_eq!(detach.program, Path::new("/usr/bin/hdiutil"));
    assert_eq!(
        detach.args,
        [
            std::ffi::OsString::from("detach"),
            std::ffi::OsString::from("-quiet"),
            fixture.config().main_mount.into_os_string(),
        ]
    );
    source.set(Vec::new());
    assert!(host.mounts(&repo()).expect("healed facts").is_empty());
}

fn interrupted_restore_image_publication(
    fixture: &Fixture,
    failpoint: RestoreFailpoint,
) -> (
    MacOsApfsExecutionHost<RecordingRunner>,
    PathBuf,
    PathBuf,
    PathBuf,
    DetachedWorkspaceMetadata,
) {
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout
        .main_image(ImageFormat::Sparse)
        .expect("canonical")
        .image()
        .to_owned();
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.sparseimage");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.sparseimage");
    create_image(&canonical, ImageFormat::Sparse);
    std::fs::write(&canonical, b"old generation").expect("old image");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::write(&staged, b"new generation").expect("new image");
    let mut replacement = metadata(ImageFormat::Sparse);
    replacement.workspace_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("next incarnation");
    replacement
        .write_for_image(&staged)
        .expect("replacement metadata");
    let host = native_host(fixture, RecordingRunner::default());
    host.set_restore_failpoint(failpoint);
    host.restore_swap(&staged, &canonical, &undo)
        .expect_err("injected image publication crash");
    (host, canonical, staged, undo, replacement)
}

#[test]
fn sidecar_first_publication_is_invisible_until_image_rename_and_recovers() {
    let fixture = Fixture::new("publish-image-boundary");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::write(&staged, b"published generation").expect("staged bytes");
    let host = native_host(&fixture, RecordingRunner::default());
    host.set_restore_failpoint(RestoreFailpoint::AfterMetadataFsync);
    host.publish_image(&staged, canonical.image())
        .expect_err("recoverable prepublication failure");
    assert!(!canonical.image().exists());
    assert!(!sidecar_path(canonical.image()).exists());
    assert!(staged.exists());
    assert!(sidecar_path(&staged).exists());
    std::fs::rename(sidecar_path(&staged), sidecar_path(canonical.image()))
        .expect("simulate process death after durable sidecar rename");
    assert!(
        host.list(&repo()).expect("sidecar-only listing").is_empty(),
        "readers enumerate images, so sidecar-only publication remains invisible"
    );

    drop(host);
    let restarted = native_host(&fixture, RecordingRunner::default());
    restarted
        .recover_pending(&fixture.config(), &[])
        .expect("complete sidecar publication");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"published generation"
    );
    DetachedWorkspaceMetadata::read_for_image(canonical.image()).expect("canonical metadata");
    assert_eq!(
        std::fs::read(ca_key_path(canonical.image())).expect("canonical CA key"),
        b"fixture-ca-private-key"
    );
    assert!(!sidecar_path(&staged).exists());
    assert!(!ca_key_path(&staged).exists());
    restarted
        .recover_pending(&fixture.config(), &[])
        .expect("repeated recovery is idempotent");
}

#[test]
fn publication_recovery_converges_every_rename_and_fsync_layout_with_its_ca_key() {
    for (boundary, move_companion, move_image) in [
        ("sidecar-rename", false, false),
        ("sidecar-fsync", false, false),
        ("companion-rename", true, false),
        ("image-rename", true, true),
        ("parent-fsync", true, true),
    ] {
        let fixture = Fixture::new(&format!("publication-crash-{boundary}"));
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
        let staged = layout
            .project()
            .project_root
            .join(".staging/main-00000000000000000000000000000001.sparseimage");
        create_image(&staged, ImageFormat::Sparse);
        std::fs::write(&staged, b"new generation").expect("staged image");
        write_ca_key(&staged, b"new-ca-key");
        std::fs::rename(sidecar_path(&staged), sidecar_path(canonical.image()))
            .expect("publish sidecar");
        if move_companion {
            std::fs::rename(ca_key_path(&staged), ca_key_path(canonical.image()))
                .expect("publish CA companion");
        }
        if move_image {
            std::fs::rename(&staged, canonical.image()).expect("publish image");
        }

        let host = native_host(&fixture, RecordingRunner::default());
        host.recover_pending(&fixture.config(), &[])
            .expect("recover publication triple");
        assert_eq!(
            std::fs::read(canonical.image()).expect("canonical image"),
            b"new generation",
            "{boundary}"
        );
        DetachedWorkspaceMetadata::read_for_image(canonical.image()).expect("canonical metadata");
        assert_eq!(
            std::fs::read(ca_key_path(canonical.image())).expect("canonical CA key"),
            b"new-ca-key",
            "{boundary}"
        );
        assert!(!staged.exists(), "{boundary}");
        assert!(!sidecar_path(&staged).exists(), "{boundary}");
        assert!(!ca_key_path(&staged).exists(), "{boundary}");
        host.recover_pending(&fixture.config(), &[])
            .expect("idempotent recovery");
    }
}

#[test]
fn recovery_rejects_missing_or_contradictory_ca_companion_layouts() {
    let missing = Fixture::new("recovery-missing-ca");
    let missing_layout = StorageLayout::new(&missing.root, &repo()).expect("layout");
    let missing_canonical = missing_layout
        .main_image(ImageFormat::Sparse)
        .expect("canonical");
    create_image(missing_canonical.image(), ImageFormat::Sparse);
    std::fs::remove_file(ca_key_path(missing_canonical.image())).expect("remove CA key");
    let error = native_host(&missing, RecordingRunner::default())
        .recover_pending(&missing.config(), &[])
        .expect_err("missing canonical CA key");
    match error {
        ApfsStorageError::MarkerMismatch(message) => {
            assert!(message.contains(&missing_canonical.image().display().to_string()));
            assert!(
                message.contains(&ca_key_path(missing_canonical.image()).display().to_string())
            );
        }
        other => panic!("expected typed integrity error, got {other:?}"),
    }

    let contradictory = Fixture::new("recovery-contradictory-ca");
    let layout = StorageLayout::new(&contradictory.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::rename(sidecar_path(&staged), sidecar_path(canonical.image()))
        .expect("publish sidecar");
    std::fs::copy(ca_key_path(&staged), ca_key_path(canonical.image()))
        .expect("create contradictory CA keys");
    std::fs::set_permissions(
        ca_key_path(canonical.image()),
        std::fs::Permissions::from_mode(0o600),
    )
    .expect("canonical CA mode");
    let error = native_host(&contradictory, RecordingRunner::default())
        .recover_pending(&contradictory.config(), &[])
        .expect_err("contradictory CA keys");
    match error {
        ApfsStorageError::MarkerMismatch(message) => {
            assert!(message.contains(&staged.display().to_string()));
            assert!(message.contains(&canonical.image().display().to_string()));
        }
        other => panic!("expected typed integrity error, got {other:?}"),
    }
}

#[test]
fn gc_reclaims_sidecarless_staging_crash_images_but_preserves_recoverable_pairs() {
    let fixture = Fixture::new("staging-orphan");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let staging = layout.project().project_root.join(".staging");
    let orphan = staging.join("session-a-00000000000000000000000000000002.asif");
    std::fs::create_dir_all(&staging).expect("staging directory");
    std::fs::write(&orphan, b"crashed before metadata").expect("orphan staged image");
    let recoverable = staging.join("main-00000000000000000000000000000001.sparseimage");
    create_image(&recoverable, ImageFormat::Sparse);

    let report = execute_gc(
        &native_host(&fixture, RecordingRunner::default()),
        &fixture.config(),
    )
    .expect("staging gc");

    assert_eq!(report.reclaimed, 1);
    assert!(!orphan.exists(), "sidecarless crash image is reclaimed");
    assert!(recoverable.exists(), "recoverable staged image is retained");
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&recoverable)
            .expect("recoverable staged metadata"),
        metadata(ImageFormat::Sparse)
    );
}

#[cfg(unix)]
#[test]
fn gc_does_not_follow_symlinked_owner_repository_staging_or_image_paths() {
    let fixture = Fixture::new("gc-containment");
    let external = Fixture::new("gc-external-targets");
    let orphan_name = "main-00000000000000000000000000000002.sparseimage";
    let write_orphan = |path: &Path, contents: &[u8]| {
        std::fs::create_dir_all(path.parent().expect("external parent")).expect("external parent");
        std::fs::write(path, contents).expect("external staged image");
    };

    let owner_target = external
        .root
        .join("owner-target/repository/.staging")
        .join(orphan_name);
    write_orphan(&owner_target, b"owner target");
    std::os::unix::fs::symlink(
        external.root.join("owner-target"),
        fixture.root.join("linked-owner"),
    )
    .expect("owner symlink");

    let repository_target = external
        .root
        .join("repository-target/.staging")
        .join(orphan_name);
    write_orphan(&repository_target, b"repository target");
    std::fs::create_dir_all(fixture.root.join("real-owner")).expect("real owner");
    std::os::unix::fs::symlink(
        external.root.join("repository-target"),
        fixture.root.join("real-owner/linked-repository"),
    )
    .expect("repository symlink");

    let staging_target = external.root.join("staging-target").join(orphan_name);
    write_orphan(&staging_target, b"staging target");
    let staging_link = fixture.root.join("staging-owner/repository/.staging");
    std::fs::create_dir_all(staging_link.parent().expect("staging project"))
        .expect("staging project");
    std::os::unix::fs::symlink(external.root.join("staging-target"), &staging_link)
        .expect("staging symlink");

    let image_target = external.root.join("image-target.sparseimage");
    write_orphan(&image_target, b"image target");
    let real_staging = fixture.root.join("image-owner/repository/.staging");
    std::fs::create_dir_all(&real_staging).expect("real staging");
    let image_link = real_staging.join(orphan_name);
    std::os::unix::fs::symlink(&image_target, &image_link).expect("image symlink");
    let invalid_name = real_staging.join("manual-backup.sparseimage");
    std::fs::write(&invalid_name, b"not a transaction").expect("manual staging file");

    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let escaped_label = CheckpointLabel::new("escaped").expect("escaped label");
    let escaped_checkpoint = layout
        .checkpoint_image(
            &WorkspaceName::new("main").expect("main"),
            &escaped_label,
            ImageFormat::Sparse,
        )
        .expect("escaped checkpoint")
        .image()
        .to_owned();
    std::fs::create_dir_all(escaped_checkpoint.parent().expect("checkpoint directory"))
        .expect("checkpoint directory");
    std::os::unix::fs::symlink(&image_target, &escaped_checkpoint).expect("checkpoint symlink");
    metadata(ImageFormat::Sparse)
        .write_for_image(&escaped_checkpoint)
        .expect("escaped checkpoint metadata");
    let host = native_host(&fixture, RecordingRunner::default());
    let error = host
        .publish_checkpoint_fact(
            &escaped_checkpoint,
            &escaped_label,
            Revision::new(99),
            Pin::Automatic,
        )
        .expect_err("symlinked checkpoint publication must fail closed");
    assert!(matches!(
        error,
        ApfsStorageError::Layout(StorageLayoutError::SymlinkComponent(path))
            if path == escaped_checkpoint
    ));
    assert!(
        host.checkpoints(&repo())
            .expect("checkpoint enumeration")
            .is_empty()
    );

    let report = execute_gc(&host, &fixture.config()).expect("contained gc");

    assert_eq!(report, Default::default());
    for (path, contents) in [
        (&owner_target, b"owner target".as_slice()),
        (&repository_target, b"repository target".as_slice()),
        (&staging_target, b"staging target".as_slice()),
        (&image_target, b"image target".as_slice()),
    ] {
        assert_eq!(std::fs::read(path).expect("external target"), contents);
    }
    assert!(
        std::fs::symlink_metadata(&escaped_checkpoint)
            .expect("checkpoint link")
            .file_type()
            .is_symlink()
    );
    assert!(
        std::fs::symlink_metadata(&image_link)
            .expect("image link")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read(&invalid_name).expect("invalid staging name"),
        b"not a transaction"
    );
}

#[test]
fn adopt_publication_moves_source_aside_writes_stub_and_publishes_image_atomically() {
    let fixture = Fixture::new("adopt-publication");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::create_dir_all(&config.main_mount).expect("source");
    std::fs::write(config.main_mount.join("tracked"), b"source").expect("source file");
    let pre_cowshed = PathBuf::from(format!("{}.pre-cowshed", config.main_mount.display()));

    native_host(&fixture, RecordingRunner::default())
        .publish_adopt(&config.main_mount, &pre_cowshed, &staged, canonical.image())
        .expect("publish adopt");

    assert_eq!(
        std::fs::read(pre_cowshed.join("tracked")).expect("retained source"),
        b"source"
    );
    assert_eq!(
        std::fs::read(config.main_mount.join(".envrc")).expect("stub"),
        b"cowshed ensure --attach\n"
    );
    assert!(canonical.image().exists());
    assert!(sidecar_path(canonical.image()).exists());
    assert!(!staged.exists());
}

#[test]
fn adopt_recovery_waits_for_handoff_then_completes_publication_after_restart() {
    let before = Fixture::new("adopt-before-handoff");
    let before_config = before.config();
    let before_layout = StorageLayout::new(&before.root, &repo()).expect("layout");
    let before_canonical = before_layout
        .main_image(ImageFormat::Sparse)
        .expect("canonical");
    let before_staged = before_layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    create_image(&before_staged, ImageFormat::Sparse);
    std::fs::create_dir_all(&before_config.main_mount).expect("source");
    std::fs::write(before_config.main_mount.join("tracked"), b"source").expect("source file");
    let before_host = native_host(&before, RecordingRunner::default());
    before_host
        .recover_pending(&before_config, &[])
        .expect("pre-handoff recovery");
    assert!(before_staged.exists());
    assert!(!before_canonical.image().exists());
    let cleanup = execute_gc(&before_host, &before_config).expect("staging cleanup");
    assert_eq!(cleanup.examined, 1);
    assert_eq!(cleanup.reclaimed, 0);
    assert!(before_staged.exists());
    assert!(sidecar_path(&before_staged).exists());
    assert_eq!(
        std::fs::read(before_config.main_mount.join("tracked")).expect("original source"),
        b"source"
    );

    let after = Fixture::new("adopt-after-handoff");
    let after_config = after.config();
    let after_layout = StorageLayout::new(&after.root, &repo()).expect("layout");
    let after_canonical = after_layout
        .main_image(ImageFormat::Sparse)
        .expect("canonical");
    let after_staged = after_layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    create_image(&after_staged, ImageFormat::Sparse);
    std::fs::create_dir_all(&after_config.main_mount).expect("source");
    std::fs::write(after_config.main_mount.join("tracked"), b"source").expect("source file");
    let after_pre = PathBuf::from(format!("{}.pre-cowshed", after_config.main_mount.display()));
    std::fs::rename(&after_config.main_mount, &after_pre).expect("simulate handoff crash");

    native_host(&after, RecordingRunner::default())
        .recover_pending(&after_config, &[])
        .expect("post-handoff recovery");
    assert!(after_canonical.image().exists());
    assert!(!after_staged.exists());
    assert_eq!(
        std::fs::read(after_config.main_mount.join(".envrc")).expect("stub"),
        b"cowshed ensure --attach\n"
    );
    assert_eq!(
        std::fs::read(after_pre.join("tracked")).expect("retained source"),
        b"source"
    );
}

#[test]
fn checkpoint_pin_facts_survive_restart_and_gc_prunes_only_old_automatic_excess() {
    let fixture = Fixture::new("checkpoint-retention");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let main = WorkspaceName::new("main").expect("main");
    let host = native_host(&fixture, RecordingRunner::default());
    let create_checkpoint = |name: &str, revision: u64, pin: Pin| {
        let label = CheckpointLabel::new(name).expect("label");
        let image = layout
            .checkpoint_image(&main, &label, ImageFormat::Sparse)
            .expect("checkpoint")
            .image()
            .to_owned();
        create_image(&image, ImageFormat::Sparse);
        host.publish_checkpoint_fact(&image, &label, Revision::new(revision), pin)
            .expect("checkpoint fact");
        image
    };

    let mut newest_five = Vec::new();
    for index in 0..5 {
        let image = create_checkpoint(&format!("future-{index}"), index + 1, Pin::Automatic);
        make_future(&image);
        newest_five.push(image);
    }
    let young = create_checkpoint("young", 6, Pin::Automatic);
    let expired = create_checkpoint("expired", 7, Pin::Automatic);
    make_old(&expired);
    let pinned = create_checkpoint("pinned-old", 8, Pin::Pinned);
    make_old(&pinned);
    drop(host);

    let restarted = native_host(&fixture, RecordingRunner::default());
    let facts = restarted.checkpoints(&repo()).expect("restart facts");
    assert_eq!(facts.len(), 8);
    assert!(facts.iter().any(|fact| {
        fact.label.as_str() == "young"
            && fact.revision == Revision::new(6)
            && fact.pin == Pin::Automatic
    }));
    assert!(facts.iter().any(|fact| {
        fact.label.as_str() == "pinned-old"
            && fact.revision == Revision::new(8)
            && fact.pin == Pin::Pinned
    }));

    let report = execute_gc(&restarted, &config).expect("gc");
    assert_eq!(report.examined, 8);
    assert_eq!(report.reclaimed, 1);
    assert_eq!(report.retained_pinned, 1);
    assert_eq!(report.retained_recent, 6);
    assert!(newest_five.iter().all(|image| image.exists()));
    assert!(
        young.exists(),
        "young checkpoint must survive beyond newest five"
    );
    assert!(pinned.exists(), "old pinned checkpoint must survive");
    assert!(
        !expired.exists(),
        "old automatic checkpoint beyond five is pruned"
    );
    assert!(!sidecar_path(&expired).exists(), "grants sidecar is pruned");
    assert!(
        !checkpoint_fact_path(&expired).exists(),
        "checkpoint fact sidecar is pruned"
    );

    let surviving = restarted.checkpoints(&repo()).expect("surviving facts");
    assert_eq!(surviving.len(), 7);
    assert_eq!(
        surviving
            .iter()
            .filter(|fact| fact.pin == Pin::Pinned)
            .count(),
        1
    );
}

#[test]
fn recovery_ignores_unscoped_and_non_internal_restore_lookalikes() {
    let fixture = Fixture::new("restore-lookalikes");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    create_image(canonical.image(), ImageFormat::Sparse);
    std::fs::write(canonical.image(), b"live generation").expect("canonical");

    for root in [
        fixture.root.join("caches"),
        fixture.root.join("mnt"),
        fixture.root.join("telemetry"),
    ] {
        std::fs::create_dir_all(&root).expect("lookalike root");
        std::fs::write(
            root.join("pre-restore-00000000000000000000000000000002.sparseimage.grants.json"),
            b"not metadata",
        )
        .expect("lookalike");
    }
    let invalid_checkpoints = [
        "pre-restore-user.sparseimage",
        "pre-restore-0000000000000000000000000000000.sparseimage",
        "pre-restore-gggggggggggggggggggggggggggggggg.sparseimage",
    ]
    .map(|name| layout.project().checkpoints.join("main").join(name));
    for checkpoint in &invalid_checkpoints {
        create_image(checkpoint, ImageFormat::Sparse);
        std::fs::write(checkpoint, b"user checkpoint").expect("checkpoint");
    }

    let session = WorkspaceName::new("session-a").expect("session");
    let session_canonical = layout
        .session_image(&session, ImageFormat::Sparse)
        .expect("session canonical");
    create_image(session_canonical.image(), ImageFormat::Sparse);
    std::fs::write(session_canonical.image(), b"live session").expect("session");
    let mut session_metadata = metadata(ImageFormat::Sparse);
    session_metadata.workspace = session.clone();
    session_metadata
        .write_for_image(session_canonical.image())
        .expect("session metadata");
    let mismatched_undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.sparseimage");
    create_image(&mismatched_undo, ImageFormat::Sparse);
    std::fs::write(&mismatched_undo, b"unrelated undo").expect("undo");
    session_metadata
        .write_for_image(&mismatched_undo)
        .expect("mismatched undo metadata");

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("ignore lookalikes");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"live generation"
    );
    assert!(
        invalid_checkpoints
            .iter()
            .all(|checkpoint| checkpoint.exists())
    );
    assert_eq!(
        std::fs::read(session_canonical.image()).expect("session"),
        b"live session"
    );
    assert!(mismatched_undo.exists());
}

#[test]
fn recovery_rejects_a_non_directory_store_root_and_ignores_file_children() {
    let file_root = Fixture::new("recovery-file-root");
    let file_config = file_root.config();
    std::fs::remove_dir_all(&file_config.store_root).expect("remove store directory");
    std::fs::write(&file_config.store_root, b"not a directory").expect("file store root");
    assert!(
        native_host(&file_root, RecordingRunner::default())
            .recover_pending(&file_config, &[])
            .is_err(),
        "a non-directory store root is corruption, not an empty store"
    );

    let child_file = Fixture::new("recovery-file-child");
    let child_config = child_file.config();
    std::fs::create_dir_all(&child_config.store_root).expect("store root");
    std::fs::write(child_config.store_root.join("not-a-project"), b"file").expect("child file");
    native_host(&child_file, RecordingRunner::default())
        .recover_pending(&child_config, &[])
        .expect("non-directory children are not project roots");
}

#[test]
fn recovery_never_publishes_metadata_without_its_staged_image() {
    let fixture = Fixture::new("sidecar-without-image");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.sparseimage");
    std::fs::create_dir_all(staged.parent().expect("staging parent")).expect("staging");
    let mut next = metadata(ImageFormat::Sparse);
    next.workspace_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("incarnation");
    next.write_for_image(&staged)
        .expect("orphan staging metadata");

    native_host(&fixture, RecordingRunner::default())
        .recover_pending(&fixture.config(), &[])
        .expect("recovery");
    assert!(sidecar_path(&staged).exists());
    assert!(!sidecar_path(canonical.image()).exists());
}

#[test]
fn recovery_rolls_forward_published_metadata_before_undo_rename() {
    let fixture = Fixture::new("published-before-undo");
    let (host, canonical, staged, undo, replacement) =
        interrupted_restore_image_publication(&fixture, RestoreFailpoint::AfterImageSwap);
    replacement
        .write_for_image(&canonical)
        .expect("simulate committed metadata");

    drop(host);
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    host.recover_pending(&fixture.config(), &[])
        .expect("roll forward");

    assert_eq!(
        std::fs::read(&canonical).expect("canonical"),
        b"new generation"
    );
    assert_eq!(std::fs::read(&undo).expect("undo"), b"old generation");
    assert!(!staged.exists());
    assert!(
        runner.requests().is_empty(),
        "roll-forward rename must preserve the retained undo without deleting an absent staging image"
    );
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&canonical)
            .expect("canonical metadata")
            .workspace_incarnation,
        replacement.workspace_incarnation
    );
}

#[test]
fn restore_recovery_rejects_published_new_image_with_old_ca_key() {
    let fixture = Fixture::new("published-new-image-old-ca");
    let (host, canonical, staged, undo, replacement) =
        interrupted_restore_image_publication(&fixture, RestoreFailpoint::AfterRestoreImageSwap);
    replacement
        .write_for_image(&canonical)
        .expect("simulate published replacement metadata");
    drop(host);

    let error = native_host(&fixture, RecordingRunner::default())
        .recover_pending(&fixture.config(), &[])
        .expect_err("old CA key must not authenticate replacement image");
    match error {
        ApfsStorageError::MarkerMismatch(message) => {
            assert!(message.contains(&canonical.display().to_string()));
            assert!(message.contains(&ca_key_path(&canonical).display().to_string()));
            assert!(message.contains(&ca_key_path(&undo).display().to_string()));
        }
        other => panic!("expected typed restore integrity error, got {other:?}"),
    }
    assert!(canonical.exists());
    assert!(staged.exists());
}

#[test]
fn recovery_rolls_back_when_published_metadata_is_missing() {
    let fixture = Fixture::new("published-metadata-missing");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000002.sparseimage");
    let undo = layout
        .project()
        .checkpoints
        .join("main/pre-restore-00000000000000000000000000000002.sparseimage");
    create_image(canonical.image(), ImageFormat::Sparse);
    std::fs::write(canonical.image(), b"old generation").expect("old image");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::write(&staged, b"new generation").expect("new image");
    let next_incarnation =
        WorkspaceIncarnation::new("00000000000000000000000000000002").expect("next incarnation");
    let mut next_metadata = metadata(ImageFormat::Sparse);
    next_metadata.workspace_incarnation = next_incarnation.clone();
    next_metadata
        .write_for_image(&staged)
        .expect("next metadata");
    let replacement = LifecycleWorkspace::new(
        repo(),
        WorkspaceName::new("main").expect("main"),
        next_incarnation,
        Revision::new(2),
        Revision::new(1),
        WorkspaceRole::Main,
        ImageFormat::Sparse,
    )
    .expect("replacement");
    let host = native_host(&fixture, RecordingRunner::default());
    host.restore_swap(&staged, canonical.image(), &undo)
        .expect("image publication");
    host.set_restore_failpoint(RestoreFailpoint::AfterMetadataPublish);
    host.publish_restored_metadata(
        &staged,
        canonical.image(),
        &replacement,
        replacement.revision(),
        &undo,
    )
    .expect_err("metadata failpoint");
    std::fs::remove_file(sidecar_path(canonical.image())).expect("remove published metadata");

    drop(host);
    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("missing publication evidence rolls back");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical"),
        b"old generation"
    );
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(canonical.image())
            .expect("restored metadata")
            .workspace_incarnation,
        metadata(ImageFormat::Sparse).workspace_incarnation
    );
}
#[test]
fn recovery_accepts_an_already_rolled_back_canonical_layout() {
    let fixture = Fixture::new("already-rolled-back");
    let (host, canonical, staged, _, _) =
        interrupted_restore_image_publication(&fixture, RestoreFailpoint::AfterImageSwap);
    let temporary = canonical.with_extension("swap");
    std::fs::rename(&canonical, &temporary).expect("stage new canonical");
    std::fs::rename(&staged, &canonical).expect("restore old canonical");
    std::fs::rename(&temporary, &staged).expect("restage replacement");

    drop(host);
    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("recognize old canonical");

    assert_eq!(
        std::fs::read(&canonical).expect("canonical"),
        b"old generation"
    );
    assert!(!staged.exists());
}

#[test]
fn recovery_restores_missing_prepublication_canonical_metadata() {
    let fixture = Fixture::new("missing-canonical-metadata");
    let (host, canonical, staged, undo, _) =
        interrupted_restore_image_publication(&fixture, RestoreFailpoint::AfterUndoRename);
    std::fs::remove_file(sidecar_path(&canonical)).expect("remove interrupted sidecar");

    drop(host);
    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&fixture.config(), &[])
        .expect("restore metadata from undo");

    assert_eq!(
        std::fs::read(&canonical).expect("canonical"),
        b"old generation"
    );
    assert!(!staged.exists());
    assert!(!undo.exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&canonical)
            .expect("canonical metadata")
            .workspace_incarnation,
        metadata(ImageFormat::Sparse).workspace_incarnation
    );
}

#[test]
fn stateless_restore_recovery_converges_each_publication_boundary() {
    for failpoint in [
        RestoreFailpoint::AfterUndoSidecar,
        RestoreFailpoint::AfterRestoreImageSwap,
        RestoreFailpoint::AfterImageSwap,
        RestoreFailpoint::AfterRestoreUndoImageRename,
        RestoreFailpoint::AfterUndoRename,
        RestoreFailpoint::AfterRestoreCanonicalParentFsync,
        RestoreFailpoint::AfterRestoreUndoParentFsync,
        RestoreFailpoint::AfterMetadataPublish,
        RestoreFailpoint::AfterMetadataFsync,
        RestoreFailpoint::AfterStagedMetadataRemoval,
        RestoreFailpoint::AfterRestoreMetadataParentFsync,
    ] {
        let fixture = Fixture::new(&format!("stateless-restore-{failpoint:?}"));
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
        let staged = layout
            .project()
            .project_root
            .join(".staging/main-00000000000000000000000000000002.sparseimage");
        let undo = layout
            .project()
            .checkpoints
            .join("main/pre-restore-00000000000000000000000000000002.sparseimage");
        create_image(canonical.image(), ImageFormat::Sparse);
        std::fs::write(canonical.image(), b"old generation").expect("old image");
        write_ca_key(canonical.image(), b"old-ca-key");
        create_image(&staged, ImageFormat::Sparse);
        std::fs::write(&staged, b"new generation").expect("new image");
        write_ca_key(&staged, b"new-ca-key");
        let next_incarnation = WorkspaceIncarnation::new("00000000000000000000000000000002")
            .expect("next incarnation");
        let mut next_metadata = metadata(ImageFormat::Sparse);
        next_metadata.workspace_incarnation = next_incarnation.clone();
        next_metadata
            .write_for_image(&staged)
            .expect("next metadata");
        let replacement = LifecycleWorkspace::new(
            repo(),
            WorkspaceName::new("main").expect("main"),
            next_incarnation.clone(),
            Revision::new(2),
            Revision::new(1),
            WorkspaceRole::Main,
            ImageFormat::Sparse,
        )
        .expect("replacement");
        let old_credential_mount = fixture.root.join("old-credential-mount");
        let new_credential_mount = fixture.root.join("new-credential-mount");
        std::fs::create_dir_all(&old_credential_mount).expect("old credential mount");
        std::fs::create_dir_all(&new_credential_mount).expect("new credential mount");
        mint_workspace_credentials(
            &workspace(ImageFormat::Sparse),
            &old_credential_mount,
            &ca_key_path(canonical.image()),
        )
        .expect("old credentials");
        mint_workspace_credentials(&replacement, &new_credential_mount, &ca_key_path(&staged))
            .expect("replacement credentials");
        let old_ca_key = std::fs::read(ca_key_path(canonical.image())).expect("old CA key");
        let new_ca_key = std::fs::read(ca_key_path(&staged)).expect("new CA key");
        let host = native_host(&fixture, RecordingRunner::default());
        host.set_restore_failpoint(failpoint);
        let image_phase = matches!(
            failpoint,
            RestoreFailpoint::AfterUndoSidecar
                | RestoreFailpoint::AfterRestoreImageSwap
                | RestoreFailpoint::AfterImageSwap
                | RestoreFailpoint::AfterRestoreUndoImageRename
                | RestoreFailpoint::AfterUndoRename
                | RestoreFailpoint::AfterRestoreCanonicalParentFsync
                | RestoreFailpoint::AfterRestoreUndoParentFsync
        );
        if image_phase {
            host.restore_swap(&staged, canonical.image(), &undo)
                .expect_err("injected image publication crash");
        } else {
            host.restore_swap(&staged, canonical.image(), &undo)
                .expect("image publication");
            host.publish_restored_metadata(
                &staged,
                canonical.image(),
                &replacement,
                replacement.revision(),
                &undo,
            )
            .expect_err("injected metadata publication crash");
        }

        drop(host);
        let host = native_host(&fixture, RecordingRunner::default());
        host.recover_pending(&fixture.config(), &[])
            .expect("restart recovery");
        host.recover_pending(&fixture.config(), &[])
            .expect("repeated restart recovery is idempotent");
        let pending = host.pending_publications(&repo()).expect("pending facts");
        let metadata_was_published = !pending.is_empty();
        if metadata_was_published {
            assert_eq!(pending.len(), 1, "{failpoint:?}");
            assert_eq!(
                pending[0].source_incarnation,
                metadata(ImageFormat::Sparse).workspace_incarnation
            );
            assert_eq!(
                pending[0].source_checkpoint,
                format!("pre-restore-{next_incarnation}")
            );
            host.activate_restored_metadata(&pending[0].image)
                .expect("idempotent forward activation");
            assert!(
                host.pending_publications(&repo())
                    .expect("activated pending facts")
                    .is_empty()
            );
        } else {
            assert!(pending.is_empty(), "{failpoint:?}");
        }
        assert!(
            !restore_recovery_fact_path(canonical.image()).exists(),
            "{failpoint:?}"
        );
        assert_eq!(
            std::fs::read(canonical.image()).expect("canonical bytes"),
            if metadata_was_published {
                b"new generation".as_slice()
            } else {
                b"old generation".as_slice()
            },
            "{failpoint:?}"
        );
        assert_eq!(
            std::fs::read(ca_key_path(canonical.image())).expect("canonical CA key"),
            if metadata_was_published {
                new_ca_key.as_slice()
            } else {
                old_ca_key.as_slice()
            },
            "{failpoint:?}"
        );
        if metadata_was_published {
            assert_eq!(std::fs::read(&undo).expect("undo image"), b"old generation");
            assert_eq!(
                std::fs::read(ca_key_path(&undo)).expect("undo CA key"),
                old_ca_key
            );
        } else {
            assert!(!undo.exists(), "{failpoint:?}");
            assert!(!ca_key_path(&undo).exists(), "{failpoint:?}");
        }
        assert!(!staged.exists(), "{failpoint:?}");
        assert!(!sidecar_path(&staged).exists(), "{failpoint:?}");
        assert!(!ca_key_path(&staged).exists(), "{failpoint:?}");
        let canonical_metadata =
            DetachedWorkspaceMetadata::read_for_image(canonical.image()).expect("metadata");
        assert_eq!(
            canonical_metadata.workspace_incarnation,
            if metadata_was_published {
                next_incarnation
            } else {
                metadata(ImageFormat::Sparse).workspace_incarnation
            },
            "{failpoint:?}"
        );
    }
}

#[cfg(target_os = "macos")]
#[test]
fn system_mount_source_observes_the_live_root_mount() {
    let mounts = SystemKernelMountSource.mounts().expect("getmntinfo");
    assert!(!mounts.is_empty());
    let root = mounts
        .iter()
        .find(|mount| mount.mount_point == Path::new("/"))
        .expect("root mount");
    assert!(root.source_device.starts_with("/dev/disk"));
    assert!(mounts.iter().all(|mount| mount.mount_id != 0));
}

#[test]
fn kernel_mount_flag_truth_table_allows_browse_but_requires_owners() {
    for (nobrowse, owners, expected_valid) in [
        (true, true, true),
        (true, false, false),
        (false, true, true),
        (false, false, false),
    ] {
        let fixture = Fixture::new(&format!("flag-table-{nobrowse}-{owners}"));
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
        create_image(canonical.image(), ImageFormat::Sparse);
        let source = FakeKernelMountSource::default();
        source.set(vec![KernelMountSnapshot::new(
            91,
            fixture.config().main_mount,
            "/dev/disk10s1",
            nobrowse,
            owners,
        )]);
        let host = MacOsApfsExecutionHost::with_mount_source(
            RecordingRunner::default(),
            fixture.config(),
            source,
        )
        .expect("host");
        let result = host.mounts(&repo());
        assert_eq!(result.is_ok(), expected_valid, "{nobrowse}/{owners}");
    }
}

#[test]
fn flock_child_helper() {
    let Ok(root) = std::env::var("COWSHED_FLOCK_HELPER_ROOT") else {
        return;
    };
    let lock =
        PathBuf::from(std::env::var_os("COWSHED_FLOCK_HELPER_LOCK").expect("helper lock path"));
    let ready =
        PathBuf::from(std::env::var_os("COWSHED_FLOCK_HELPER_READY").expect("helper ready path"));
    let release = PathBuf::from(
        std::env::var_os("COWSHED_FLOCK_HELPER_RELEASE").expect("helper release path"),
    );
    let host = native_host_at(Path::new(&root));
    let _guard = host
        .lock_images(&[lock], LockMode::Wait)
        .expect("helper lock")
        .expect("blocking helper lock");
    std::fs::write(&ready, b"ready").expect("signal ready");
    wait_for_path(&release);
    if std::env::var_os("COWSHED_FLOCK_HELPER_CRASH").is_some() {
        std::process::abort();
    }
}

#[test]
fn independent_hosts_and_processes_serialize_and_crash_releases_the_lock() {
    let fixture = Fixture::new("flock-process");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let lock = layout
        .main_image(ImageFormat::Sparse)
        .expect("main")
        .lock()
        .to_owned();
    let first = native_host_at(&fixture.root);
    let second = native_host_at(&fixture.root);
    let guard = first
        .lock_images(std::slice::from_ref(&lock), LockMode::Wait)
        .expect("first lock")
        .expect("blocking first lock");
    assert!(
        second
            .lock_images(std::slice::from_ref(&lock), LockMode::Try)
            .expect("second try lock")
            .is_none(),
        "independent hosts must contend through the kernel"
    );
    drop(guard);
    drop(
        second
            .lock_images(std::slice::from_ref(&lock), LockMode::Try)
            .expect("second lock after release")
            .expect("released lock must be available"),
    );

    for crash in [false, true] {
        let ready = fixture.root.join(format!("child-{crash}.ready"));
        let release = fixture.root.join(format!("child-{crash}.release"));
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .arg("--exact")
            .arg("flock_child_helper")
            .env("COWSHED_FLOCK_HELPER_ROOT", &fixture.root)
            .env("COWSHED_FLOCK_HELPER_LOCK", &lock)
            .env("COWSHED_FLOCK_HELPER_READY", &ready)
            .env("COWSHED_FLOCK_HELPER_RELEASE", &release)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if crash {
            command.env("COWSHED_FLOCK_HELPER_CRASH", "1");
        }
        let mut child = command.spawn().expect("spawn lock helper");
        wait_for_path(&ready);
        assert!(
            first
                .lock_images(std::slice::from_ref(&lock), LockMode::Try)
                .expect("parent try while child owns")
                .is_none(),
            "another process must own the lock"
        );
        std::fs::write(&release, b"release").expect("release helper");
        let status = child.wait().expect("wait for helper");
        assert_eq!(status.success(), !crash);
        drop(
            first
                .lock_images(std::slice::from_ref(&lock), LockMode::Try)
                .expect("lock after child exit")
                .expect("process exit must release flock"),
        );
    }
}

#[test]
fn gc_skips_staging_owned_by_an_active_lifecycle_lock() {
    let fixture = Fixture::new("gc-active-staging");
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    std::fs::create_dir_all(staged.parent().expect("staging directory"))
        .expect("staging directory");
    std::fs::write(&staged, b"sidecarless staged orphan").expect("staged orphan");
    let owner = native_host_at(&fixture.root);
    let collector = native_host_at(&fixture.root);
    let guard = owner
        .lock_images(&[canonical.lock().to_owned()], LockMode::Wait)
        .expect("owner lock")
        .expect("blocking owner lock");

    let error = execute_gc(&collector, &fixture.config()).expect_err("contended GC plan");
    assert!(matches!(error, ApfsStorageError::GcPlanStale));
    assert!(staged.exists());
    assert!(!sidecar_path(&staged).exists());

    drop(guard);
    let report = execute_gc(&collector, &fixture.config()).expect("released gc");
    assert_eq!(report.examined, 1);
    assert_eq!(report.reclaimed, 1);
    assert!(!staged.exists());
    assert!(!sidecar_path(&staged).exists());
}

#[test]
fn lock_and_command_targets_reject_intermediate_symlink_ancestors_without_effects() {
    let fixture = Fixture::new("symlink-ancestor");
    let runner = RecordingRunner::default();
    let host = native_host(&fixture, runner.clone());
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let attacker = fixture.root.join("attacker");
    std::fs::create_dir(&attacker).expect("attacker directory");
    std::os::unix::fs::symlink(&attacker, fixture.root.join("acme")).expect("owner symlink");

    assert!(
        host.lock_images(&[canonical.lock().to_owned()], LockMode::Try)
            .is_err(),
        "dirfd traversal must reject a symlinked owner"
    );
    let request = CreateImageRequest {
        staged_stem: layout
            .project()
            .project_root
            .join(".staging/main-00000000000000000000000000000001"),
        capacity: "1g".to_owned(),
        volume_name: "cowshed.acme--widget.main".to_owned(),
        case_sensitivity: ApfsCaseSensitivity::Insensitive,
        image_format: ImageFormatSelection::Exact(ImageFormat::Sparse),
        owner_uid: 501,
        owner_gid: 20,
    };
    assert!(
        host.create_staged(&request, ImageFormat::Sparse).is_err(),
        "command target validation must reject the same ancestor"
    );
    assert!(runner.requests().is_empty(), "no APFS command may spawn");
    assert!(
        std::fs::read_dir(&attacker)
            .expect("attacker directory")
            .next()
            .is_none(),
        "no lock or image may be created through the symlink"
    );
}

#[test]
fn gc_first_recovers_post_handoff_adopt_before_pruning_staging() {
    let fixture = Fixture::new("gc-first-adopt");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    create_image(&staged, ImageFormat::Sparse);
    let credential_mount = fixture.root.join("credential-mount");
    std::fs::create_dir_all(&credential_mount).expect("credential mount");
    mint_workspace_credentials(
        &workspace(ImageFormat::Sparse),
        &credential_mount,
        &ca_key_path(&staged),
    )
    .expect("valid staged credentials");
    std::fs::write(&staged, b"complete adopted image").expect("staged bytes");
    std::fs::create_dir_all(&config.main_mount).expect("source checkout");
    std::fs::write(config.main_mount.join("tracked"), b"original source").expect("source bytes");
    let pre_cowshed = PathBuf::from(format!("{}.pre-cowshed", config.main_mount.display()));
    std::fs::rename(&config.main_mount, &pre_cowshed).expect("simulate completed handoff");

    let host = native_host(&fixture, RecordingRunner::default());
    host.recover_pending(&config, &[])
        .expect("startup recovery before GC");
    execute_gc(&host, &config).expect("post-recovery GC");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical image"),
        b"complete adopted image"
    );
    DetachedWorkspaceMetadata::read_for_image(canonical.image()).expect("canonical metadata");
    assert_eq!(
        std::fs::read(pre_cowshed.join("tracked")).expect("preserved source"),
        b"original source"
    );
    assert!(config.main_mount.is_dir());
    assert!(!staged.exists());
    assert!(!sidecar_path(&staged).exists());

    execute_gc(&host, &config).expect("repeated GC converges");
    host.recover_pending(&config, &[])
        .expect("repeated recovery converges");
    assert!(canonical.image().exists());
    assert!(sidecar_path(canonical.image()).exists());
}

#[test]
fn publication_failpoints_converge_for_clone_and_adopt_callers() {
    for failpoint in [
        RestoreFailpoint::AfterCanonicalSidecarRename,
        RestoreFailpoint::AfterMetadataFsync,
        RestoreFailpoint::AfterCanonicalCompanionRename,
        RestoreFailpoint::AfterCanonicalImageRename,
        RestoreFailpoint::CanonicalParentFsyncFailure,
        RestoreFailpoint::AfterCanonicalParentFsync,
    ] {
        let fixture = Fixture::new(&format!("publish-clone-{failpoint:?}"));
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
        let staged = layout
            .project()
            .project_root
            .join(".staging/main-00000000000000000000000000000001.sparseimage");
        create_image(&staged, ImageFormat::Sparse);
        std::fs::write(&staged, b"clone generation").expect("staged bytes");
        let host = native_host(&fixture, RecordingRunner::default());
        host.set_restore_failpoint(failpoint);
        let result = host.publish_image(&staged, canonical.image());
        if matches!(
            failpoint,
            RestoreFailpoint::AfterCanonicalSidecarRename
                | RestoreFailpoint::AfterMetadataFsync
                | RestoreFailpoint::AfterCanonicalCompanionRename
        ) {
            result.expect_err("prepublication failure");
            assert!(staged.exists());
            assert!(sidecar_path(&staged).exists());
            native_host(&fixture, RecordingRunner::default())
                .publish_image(&staged, canonical.image())
                .expect("retry prepublication");
        } else {
            result.expect("durable pair is recovered as success");
        }
        assert_eq!(
            std::fs::read(canonical.image()).expect("canonical bytes"),
            b"clone generation"
        );
        DetachedWorkspaceMetadata::read_for_image(canonical.image()).expect("canonical metadata");
        assert!(ca_key_path(canonical.image()).exists(), "{failpoint:?}");
        assert!(!staged.exists());
        assert!(!sidecar_path(&staged).exists());
        assert!(!ca_key_path(&staged).exists(), "{failpoint:?}");
    }

    for failpoint in [
        RestoreFailpoint::AfterCanonicalSidecarRename,
        RestoreFailpoint::AfterMetadataFsync,
        RestoreFailpoint::AfterCanonicalCompanionRename,
        RestoreFailpoint::AfterCanonicalImageRename,
        RestoreFailpoint::CanonicalParentFsyncFailure,
        RestoreFailpoint::AfterCanonicalParentFsync,
    ] {
        let fixture = Fixture::new(&format!("publish-adopt-{failpoint:?}"));
        let config = fixture.config();
        let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
        let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
        let staged = layout
            .project()
            .project_root
            .join(".staging/main-00000000000000000000000000000001.sparseimage");
        create_image(&staged, ImageFormat::Sparse);
        std::fs::write(&staged, b"adopted generation").expect("staged bytes");
        std::fs::create_dir_all(&config.main_mount).expect("source");
        std::fs::write(config.main_mount.join("tracked"), b"original").expect("source bytes");
        let pre_cowshed = PathBuf::from(format!("{}.pre-cowshed", config.main_mount.display()));
        let host = native_host(&fixture, RecordingRunner::default());
        host.set_restore_failpoint(failpoint);
        let result =
            host.publish_adopt(&config.main_mount, &pre_cowshed, &staged, canonical.image());
        if matches!(
            failpoint,
            RestoreFailpoint::AfterCanonicalSidecarRename
                | RestoreFailpoint::AfterMetadataFsync
                | RestoreFailpoint::AfterCanonicalCompanionRename
        ) {
            result.expect_err("prepublication adopt failure");
            assert_eq!(
                std::fs::read(config.main_mount.join("tracked")).expect("restored source"),
                b"original"
            );
            assert!(!pre_cowshed.exists());
            native_host(&fixture, RecordingRunner::default())
                .publish_adopt(&config.main_mount, &pre_cowshed, &staged, canonical.image())
                .expect("adopt retry");
        } else {
            result.expect("durable adopt pair recovers as success");
        }
        assert_eq!(
            std::fs::read(canonical.image()).expect("canonical bytes"),
            b"adopted generation"
        );
        DetachedWorkspaceMetadata::read_for_image(canonical.image()).expect("canonical metadata");
        assert_eq!(
            std::fs::read(pre_cowshed.join("tracked")).expect("preserved original"),
            b"original"
        );
        assert!(
            !config.main_mount.join("tracked").exists(),
            "nonempty original must never remain beneath the mountpoint"
        );
        assert!(config.main_mount.join(".envrc").exists());
        let restarted = native_host(&fixture, RecordingRunner::default());
        assert_eq!(restarted.list(&repo()).expect("list").len(), 1);
        execute_gc(&restarted, &config).expect("GC convergence");
        restarted
            .recover_pending(&config, &[])
            .expect("recovery convergence");
    }
}

#[test]
fn persistent_parent_fsync_failure_never_restores_adopt_source_beside_canonical_pair() {
    let fixture = Fixture::new("persistent-adopt-fsync");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::write(&staged, b"durable adopted generation").expect("staged bytes");
    std::fs::create_dir_all(&config.main_mount).expect("source");
    std::fs::write(config.main_mount.join("tracked"), b"irreplaceable original")
        .expect("source bytes");
    let pre_cowshed = PathBuf::from(format!("{}.pre-cowshed", config.main_mount.display()));
    let host = native_host(&fixture, RecordingRunner::default());
    host.set_restore_failpoint(RestoreFailpoint::PersistentCanonicalParentFsyncFailure);
    let error = host
        .publish_adopt(&config.main_mount, &pre_cowshed, &staged, canonical.image())
        .expect_err("persistent fsync remains uncertain");
    assert_eq!(error.disposition(), PublicationDisposition::ForwardOnly);
    assert!(matches!(
        error.into_source(),
        ApfsStorageError::Cleanup { .. }
    ));
    assert!(canonical.image().exists());
    assert!(sidecar_path(canonical.image()).exists());
    assert_eq!(
        std::fs::read(pre_cowshed.join("tracked")).expect("preserved original"),
        b"irreplaceable original"
    );
    assert!(!config.main_mount.join("tracked").exists());
    assert!(config.main_mount.join(".envrc").exists());

    drop(host);
    let restarted = native_host(&fixture, RecordingRunner::default());
    restarted
        .recover_pending(&config, &[])
        .expect("fresh-host recovery");
    execute_gc(&restarted, &config).expect("fresh-host GC");
    for _ in 0..2 {
        let facts = restarted.list(&repo()).expect("idempotent list");
        assert_eq!(facts.len(), 1);
        assert_eq!(
            std::fs::read(canonical.image()).expect("canonical bytes"),
            b"durable adopted generation"
        );
        assert_eq!(
            std::fs::read(pre_cowshed.join("tracked")).expect("preserved original"),
            b"irreplaceable original"
        );
        assert!(!config.main_mount.join("tracked").exists());
        assert!(config.main_mount.join(".envrc").exists());
        restarted
            .recover_pending(&config, &[])
            .expect("repeated recovery");
    }
}

#[test]
fn sidecar_primary_and_rollback_double_failure_retains_every_forward_artifact() {
    let fixture = Fixture::new("sidecar-double-failure");
    let config = fixture.config();
    let layout = StorageLayout::new(&fixture.root, &repo()).expect("layout");
    let canonical = layout.main_image(ImageFormat::Sparse).expect("canonical");
    let staged = layout
        .project()
        .project_root
        .join(".staging/main-00000000000000000000000000000001.sparseimage");
    create_image(&staged, ImageFormat::Sparse);
    std::fs::write(&staged, b"complete forward image").expect("staged bytes");
    std::fs::create_dir_all(&config.main_mount).expect("source");
    std::fs::write(config.main_mount.join("tracked"), b"irreplaceable source")
        .expect("source bytes");
    let pre_cowshed = PathBuf::from(format!("{}.pre-cowshed", config.main_mount.display()));
    let host = native_host(&fixture, RecordingRunner::default());
    host.set_restore_failpoint(RestoreFailpoint::CanonicalSidecarRollbackFailure);
    let error = host
        .publish_adopt(&config.main_mount, &pre_cowshed, &staged, canonical.image())
        .expect_err("compound publication failure");
    assert_eq!(error.disposition(), PublicationDisposition::ForwardOnly);
    assert!(matches!(
        error.into_source(),
        ApfsStorageError::Cleanup { .. }
    ));
    assert!(staged.exists(), "full staged image must be retained");
    assert!(!sidecar_path(&staged).exists());
    assert!(!canonical.image().exists());
    assert!(
        sidecar_path(canonical.image()).exists(),
        "canonical sidecar remains the forward reference"
    );
    assert_eq!(
        std::fs::read(pre_cowshed.join("tracked")).expect("preserved source"),
        b"irreplaceable source"
    );
    assert!(!config.main_mount.join("tracked").exists());
    assert!(config.main_mount.join(".envrc").exists());

    drop(host);
    let restarted = native_host(&fixture, RecordingRunner::default());
    restarted
        .recover_pending(&config, &[])
        .expect("fresh recovery completes image-last publication");
    assert_eq!(
        std::fs::read(canonical.image()).expect("canonical bytes"),
        b"complete forward image"
    );
    DetachedWorkspaceMetadata::read_for_image(canonical.image()).expect("canonical metadata");
    assert!(!staged.exists());
    assert_eq!(restarted.list(&repo()).expect("list").len(), 1);
    execute_gc(&restarted, &config).expect("GC convergence");
    restarted
        .recover_pending(&config, &[])
        .expect("idempotent recovery");
    assert_eq!(
        std::fs::read(pre_cowshed.join("tracked")).expect("preserved source"),
        b"irreplaceable source"
    );
    assert!(!config.main_mount.join("tracked").exists());
}
