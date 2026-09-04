//! SliceE forensic acceptance: deletion tombstones, stale mountpoint sweep, fence sub-steps.
//!
//! Temp fixtures only — no live store is ever touched.

#![cfg(target_os = "macos")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use cowshed_core::apfs::{ApfsCaseSensitivity, CommandOutput, CommandRequest, CommandRunner};
use cowshed_core::metadata::{ImageFormat, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_core::storage::apfs::{ApfsExecutionHost, ApfsSubstrateConfig, CheckoutLayout};
use cowshed_core::storage::deletion_log::{
    CompanionAbsence, DELETION_LOG_FILE, classify_missing_companion,
};
use cowshed_core::storage::lifecycle::StorageGcReason;
use cowshed_core::storage::recovery::{
    FenceStep, FenceSteps, LifecycleIntent, LifecycleIntentJournal,
};

#[derive(Clone, Default)]
struct StubRunner;

impl CommandRunner for StubRunner {
    fn run(
        &self,
        _request: &CommandRequest,
    ) -> Result<CommandOutput, cowshed_core::apfs::CommandRunError> {
        Ok(CommandOutput::success(Vec::new()))
    }
}

struct Fixture {
    root: PathBuf,
}

impl Fixture {
    fn new(test: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "cowshed-deletion-log-{}-{test}-{}",
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
            CheckoutLayout::Symlink,
            ApfsCaseSensitivity::Insensitive,
        )
    }

    fn project_root(&self) -> PathBuf {
        cowshed_core::storage::StorageLayout::new(&self.root, &repo())
            .expect("layout")
            .project()
            .project_root
            .clone()
    }

    fn mount_root(&self) -> PathBuf {
        cowshed_core::storage::StorageLayout::new(&self.root, &repo())
            .expect("layout")
            .project()
            .mount_root
            .clone()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
        // The default mount root for a temp store lives under the store itself, but drop
        // defensively in case host config ever resolves elsewhere: only inside temp.
        let _ = std::fs::remove_dir_all(self.mount_root());
    }
}

fn repo() -> RepoId {
    RepoId::parse("acme/widget").expect("repo")
}

fn host(fixture: &Fixture) -> MacOsApfsExecutionHost<StubRunner> {
    MacOsApfsExecutionHost::new(StubRunner, fixture.config()).expect("host")
}

fn companion_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(".ca.key");
    PathBuf::from(path)
}

const INCARNATION: &str = "00000000000000000000000000000001";

/// (a) An orphan-GC run over a stranded staging image writes a tombstone line, and the
/// doctor-side lookup classifies the missing companion as controller-removed (with the
/// op and timestamp) versus externally removed when no tombstone exists.
#[test]
fn orphan_gc_run_writes_tombstone_and_classifies_missing_companion() {
    let fixture = Fixture::new("orphan-tombstone");
    let project = fixture.project_root();
    let staging = project.join(".staging");
    let image = staging.join(format!("orphan-{INCARNATION}.sparseimage"));
    std::fs::create_dir_all(&staging).expect("staging dir");
    std::fs::write(&image, b"orphan payload").expect("orphan image");
    let companion = companion_path(&image);
    std::fs::write(&companion, b"ca key").expect("orphan companion");
    std::fs::create_dir_all(project.join("sessions")).expect("sessions dir");

    let host = host(&fixture);
    let config = fixture.config();
    let plan = host.preview_gc(&config, &repo()).expect("preview");
    assert!(
        plan.candidates().iter().any(|candidate| candidate.reason()
            == StorageGcReason::OrphanStagingImage
            && candidate.path() == image),
        "stranded staging image must be GC-planned"
    );
    host.execute_gc(&config, plan).expect("execute");

    assert!(!image.exists(), "orphan image is reclaimed");
    assert!(!companion.exists(), "orphan companion is reclaimed");

    let log = std::fs::read_to_string(project.join(DELETION_LOG_FILE)).expect("deletion log");
    assert!(
        log.lines().any(|line| line.contains("orphan")
            && (line.contains("reclaim-image") || line.contains("remove-companion"))),
        "orphan-GC run must write a tombstone line, got: {log}"
    );

    match classify_missing_companion(&project, &image, None, false) {
        CompanionAbsence::RemovedByController(removal) => {
            assert!(!removal.at.is_empty(), "tombstone carries the removal time");
        }
        other => panic!("expected controller tombstone, got {other:?}"),
    }

    // A companion with no tombstone anywhere is an external deletion, not ours.
    let foreign = staging.join(format!("foreign-{INCARNATION}.sparseimage"));
    match classify_missing_companion(&project, &foreign, None, false) {
        CompanionAbsence::RemovedExternally => {}
        other => panic!("expected external deletion, got {other:?}"),
    }
}

/// (b) A stale, empty session mountpoint directory — no image names it, no volume is
/// mounted at it — is removed by the sweep instead of lingering under the mount root.
#[test]
fn sweep_removes_stale_empty_session_mountpoint() {
    let fixture = Fixture::new("stale-mountpoint");
    let mount_root = fixture.mount_root();
    let stale = mount_root.join("stale-ws");
    std::fs::create_dir_all(&stale).expect("stale mountpoint");
    std::fs::create_dir_all(fixture.project_root().join("sessions")).expect("sessions");

    let host = host(&fixture);
    let config = fixture.config();
    let plan = host.preview_gc(&config, &repo()).expect("preview");
    assert!(
        plan.candidates().iter().any(|candidate| candidate.reason()
            == StorageGcReason::OrphanMountpoint
            && candidate.path() == stale),
        "stale mountpoint must be sweep-planned"
    );
    host.execute_gc(&config, plan).expect("execute");
    assert!(!stale.exists(), "stale empty session mountpoint is removed");
}

/// (c) Fence sub-steps on the intent record let the classifier tell a mid-fence crash
/// (steps incomplete: the companion may never have existed) from an external deletion
/// after a completed fence (all steps done, no tombstone).
#[test]
fn fence_sub_steps_distinguish_crash_window_from_external_deletion() {
    let workspace = WorkspaceName::new("demo").expect("workspace");
    let image = PathBuf::from("/store/acme/widget/sessions/demo.sparseimage");
    let project = PathBuf::from("/store/acme/widget");

    let mut journal = LifecycleIntentJournal::default();
    journal.begin(LifecycleIntent::Create {
        workspace: workspace.clone(),
        options: cowshed_core::api::dto::CreateOptions {
            revision: None,
            from_workspace: None,
            browse: false,
            slot: None,
            register: false,
            git_worktree: false,
        },
    });

    // A fresh intent carries no step evidence: unknown, treated as an open fence.
    let record = journal.get(&workspace).expect("record");
    assert_eq!(record.fence_steps, None);

    // Mid-fence crash: sidecar durable, companion and image not yet.
    journal
        .mark_fence_step(&workspace, FenceStep::Sidecar)
        .expect("mark sidecar");
    let steps: FenceSteps = journal
        .get(&workspace)
        .expect("record")
        .fence_steps
        .expect("steps");
    assert!(!steps.is_complete());
    match classify_missing_companion(&project, &image, Some(steps), true) {
        CompanionAbsence::CrashWindow => {}
        other => panic!("expected crash window, got {other:?}"),
    }

    // Fence completed, companion later vanished with no controller tombstone: external.
    journal
        .mark_fence_step(&workspace, FenceStep::Companion)
        .expect("mark companion");
    journal
        .mark_fence_step(&workspace, FenceStep::Image)
        .expect("mark image");
    let steps: FenceSteps = journal
        .get(&workspace)
        .expect("record")
        .fence_steps
        .expect("steps");
    assert!(steps.is_complete());
    match classify_missing_companion(&project, &image, Some(steps), true) {
        CompanionAbsence::RemovedExternally => {}
        other => panic!("expected external deletion, got {other:?}"),
    }
}

/// Log-write chaos: when the deletion log cannot be written, the destructive op still
/// succeeds. Evidence is best-effort; the op is not.
#[test]
fn unreadable_log_never_fails_the_op() {
    let fixture = Fixture::new("log-chaos");
    let project = fixture.project_root();
    let staging = project.join(".staging");
    // A directory where the log file should be makes every log open fail.
    std::fs::create_dir_all(project.join(DELETION_LOG_FILE)).expect("block the log");
    let image = staging.join(format!("chaos-{INCARNATION}.sparseimage"));
    std::fs::create_dir_all(&staging).expect("staging dir");
    std::fs::write(&image, b"chaos payload").expect("chaos image");
    std::fs::create_dir_all(project.join("sessions")).expect("sessions dir");

    let host = host(&fixture);
    let config = fixture.config();
    let plan = host.preview_gc(&config, &repo()).expect("preview");
    host.execute_gc(&config, plan)
        .expect("op succeeds despite log failure");
    assert!(!image.exists(), "orphan image is still reclaimed");
}
