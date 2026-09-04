//! Contract tests for `cowshed mount main --repo-id <owner/repo>`.
//!
//! Everything asserted here is behaviour a caller can observe: the parsed
//! grammar, store-record resolution from an empty stub directory with no git
//! repository present (the exact post-outage state), the mount attempt going
//! through the gateway path with `browse: false` (the gateway-canonical
//! `nobrowse` + `owners` flags live in core's mount constructor and are
//! covered by core's own argv tests), and the verb attempting the mount
//! rather than refusing when the checkout is a stale stub.

use async_trait::async_trait;
use cowshed_cli::args::{Command, MountTarget, ProjectDiscovery, parse_args};
use cowshed_cli::mount_main::{
    MainMountBackend, ResolvedMainMount, dispatch_mount_main, resolve_main_mount,
};
use cowshed_cli::output::Output;
use cowshed_core::metadata::{
    CheckoutLayout, DetachedWorkspaceMetadata, GrantSet, ImageFormat, Platform, PublicationState,
    SIDECAR_VERSION, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceName, WorkspaceRole,
    read_json, write_json,
};
use cowshed_core::repository::{BoundIdentity, RepoId, RepositoryBinding};
use cowshed_core::storage::StorageLayout;
use cowshed_core::storage::lifecycle::MountIntent;
use cowshed_core::{ErrorCode, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

fn repo() -> RepoId {
    RepoId::parse("acme/widget").expect("fixture repo")
}

fn scratch_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "cowshed-cli-mount-main-{label}-{}-{nonce}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("scratch root");
    root
}

struct Fixture {
    root: PathBuf,
    store: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root = scratch_root(label);
        let store = root.join("store");
        fs::create_dir_all(&store).expect("fixture store");
        Self { root, store }
    }

    /// An adopted project whose checkout is the given directory: binding,
    /// layout record, and the main image sidecar carrying the checkout-path
    /// record. The checkout directory itself is left exactly as the caller
    /// made it — usually an empty stub with no git repository.
    fn bind_main(&self, repo: &RepoId, checkout: &Path, layout: CheckoutLayout) {
        let paths = StorageLayout::new(&self.store, repo)
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
        StorageLayout::new(&self.store, repo)
            .expect("layout")
            .record_checkout_layout(layout)
            .expect("checkout layout record");
        // The binding just written is the one resolution must read back.
        let round_trip: RepositoryBinding =
            read_json(&paths.repository_binding).expect("binding round trip");
        assert_eq!(
            round_trip.primary().expect("primary").repo_id,
            *repo,
            "fixture binding must own the fixture repo"
        );
        let image = StorageLayout::new(&self.store, repo)
            .expect("layout")
            .main_image(ImageFormat::Sparse)
            .expect("main image paths")
            .image()
            .to_owned();
        fs::create_dir_all(image.parent().expect("image parent")).expect("image parent");
        fs::write(&image, b"fixture").expect("image");
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: repo.clone(),
            workspace: WorkspaceName::new("main").expect("fixed main"),
            workspace_incarnation: WorkspaceIncarnation::new("0123456789abcdef0123456789abcdef")
                .expect("incarnation"),
            image_format: ImageFormat::Sparse,
            platform: Platform::Linux,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-14T00:00:00Z".to_owned(),
            grants: GrantSet::closed_baseline(None).expect("grants"),
            info_snapshot: Some(WorkspaceInfoSnapshot {
                project_root: checkout.to_owned(),
                role: WorkspaceRole::Main,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: Some("main".to_owned()),
                created_at: "2026-07-14T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-14T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            }),
        }
        .write_for_image(&image)
        .expect("sidecar");
    }

    /// Bind the repository record and layout only: no main image, so no
    /// checkout-path record.
    fn bind_without_checkout_record(&self, repo: &RepoId) {
        let paths = StorageLayout::new(&self.store, repo)
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
        StorageLayout::new(&self.store, repo)
            .expect("layout")
            .record_checkout_layout(CheckoutLayout::DirectMount)
            .expect("checkout layout record");
    }
}

struct FakeBackend {
    calls: Mutex<Vec<String>>,
    stale: bool,
    store: PathBuf,
}

impl FakeBackend {
    fn fresh(store: PathBuf) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            stale: false,
            store,
        }
    }

    fn stale(store: PathBuf) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            stale: true,
            store,
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().expect("calls").clone()
    }
}

#[async_trait]
impl MainMountBackend for FakeBackend {
    fn store_root(&self) -> &Path {
        &self.store
    }

    async fn ensure_main_mounted(
        &self,
        resolved: &ResolvedMainMount,
        intent: MountIntent,
    ) -> Result<PathBuf> {
        let mut calls = self.calls.lock().expect("calls");
        if self.stale {
            calls.push("saw-stale-mount".to_owned());
        }
        calls.push(format!("browse:{}", intent.browse));
        calls.push(format!("mount:{}", resolved.repo_id));
        Ok(resolved.mountpoint.clone())
    }
}

fn output() -> Output<Vec<u8>, Vec<u8>> {
    Output::new(Vec::new(), Vec::new(), false)
}

#[test]
fn mount_main_parses_with_required_repo_id() {
    let Command::Mount(args) = parse_args(["mount", "main", "--repo-id", "acme/widget"])
        .expect("mount main parses")
        .command
    else {
        panic!("expected mount");
    };
    assert_eq!(args.target, MountTarget::Main);
    assert_eq!(args.repo_id, repo());
}

#[test]
fn mount_main_requires_repo_id_and_main_target() {
    let missing_flag = parse_args(["mount", "main"]).unwrap_err();
    assert!(
        missing_flag.message.contains("--repo-id"),
        "unexpected message: {}",
        missing_flag.message
    );

    let missing_target = parse_args(["mount"]).unwrap_err();
    assert!(
        missing_target.message.contains("mount requires a target"),
        "unexpected message: {}",
        missing_target.message
    );

    let sibling = parse_args(["mount", "session", "--repo-id", "acme/widget"]).unwrap_err();
    assert!(
        sibling.message.contains("only `main`"),
        "unexpected message: {}",
        sibling.message
    );

    let bad_id = parse_args(["mount", "main", "--repo-id", "not-a-repo-id"]).unwrap_err();
    assert!(
        bad_id.message.contains("invalid --repo-id"),
        "unexpected message: {}",
        bad_id.message
    );
}

#[test]
fn mount_main_needs_no_project_discovery() {
    let parsed =
        parse_args(["mount", "main", "--repo-id", "acme/widget"]).expect("mount main parses");
    assert_eq!(
        parsed.command.project_discovery(),
        ProjectDiscovery::NotUsed
    );
}

#[tokio::test]
async fn resolves_main_by_repo_id_from_empty_stub_without_git() {
    let fixture = Fixture::new("stub");
    // The exact post-outage state: an empty directory, no `.git`, no live
    // repository. Resolution must never consult it.
    let stub = fixture.root.join("checkout");
    fs::create_dir_all(&stub).expect("stub checkout");
    assert!(
        !stub.join(".git").exists(),
        "the stub must carry no git repository"
    );
    fixture.bind_main(&repo(), &stub, CheckoutLayout::DirectMount);

    // Run from inside the stub so any cwd- or git-dependent resolution fails.
    let restore = std::env::current_dir().expect("a current directory");
    std::env::set_current_dir(&stub).expect("chdir into the stub");
    let resolved = resolve_main_mount(&fixture.store, &repo());
    std::env::set_current_dir(&restore).expect("chdir back");
    let resolved = resolved.expect("resolution from the stub");

    assert_eq!(resolved.repo_id, repo());
    assert_eq!(resolved.checkout_path, stub);
    assert_eq!(
        resolved.mountpoint, stub,
        "a direct mount lives at its checkout"
    );
    assert_eq!(resolved.checkout_layout, CheckoutLayout::DirectMount);
    assert!(
        !resolved.images.is_empty(),
        "resolution reports the canonical main image it read"
    );
}

#[test]
fn unknown_repo_id_names_expected_project_path() {
    let fixture = Fixture::new("unknown");
    let error = resolve_main_mount(&fixture.store, &repo()).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotFound);
    assert!(
        error.message.contains("acme/widget"),
        "unexpected message: {}",
        error.message
    );
    assert!(
        error.message.contains(&fixture.store.display().to_string()),
        "the error must name the store it searched: {}",
        error.message
    );
}

#[test]
fn missing_checkout_record_names_expected_path() {
    let fixture = Fixture::new("no-checkout-record");
    let stub = fixture.root.join("checkout");
    fs::create_dir_all(&stub).expect("stub checkout");
    fixture.bind_without_checkout_record(&repo());

    let error = resolve_main_mount(&fixture.store, &repo()).unwrap_err();
    assert!(
        error.message.contains("records no adopted checkout path"),
        "unexpected message: {}",
        error.message
    );
    let project_root = StorageLayout::new(&fixture.store, &repo())
        .expect("layout")
        .project()
        .project_root
        .clone();
    assert!(
        error.message.contains(&project_root.display().to_string()),
        "the error must name where the checkout path was expected: {}",
        error.message
    );
}

#[test]
fn foreign_binding_is_refused() {
    let fixture = Fixture::new("foreign");
    let other = RepoId::parse("other/repo").expect("other repo");
    // The binding lives at acme/widget's store path but names another
    // identity: resolution must refuse it, not adopt the stranger's name.
    let paths = StorageLayout::new(&fixture.store, &repo())
        .expect("project paths")
        .project()
        .clone();
    fs::create_dir_all(&paths.project_root).expect("project root");
    let binding = RepositoryBinding::new(vec![BoundIdentity {
        repo_id: other,
        remote_name: None,
        remote_url: None,
        primary: true,
    }])
    .expect("binding");
    write_json(&paths.repository_binding, &binding).expect("binding file");

    let error = resolve_main_mount(&fixture.store, &repo()).unwrap_err();
    assert!(
        error.message.contains("other/repo"),
        "unexpected message: {}",
        error.message
    );
}

#[test]
fn symlink_layout_mounts_under_mount_root() {
    let fixture = Fixture::new("symlink");
    let stub = fixture.root.join("checkout");
    fs::create_dir_all(&stub).expect("stub checkout");
    fixture.bind_main(&repo(), &stub, CheckoutLayout::Symlink);

    let resolved = resolve_main_mount(&fixture.store, &repo()).expect("resolution");
    assert_eq!(resolved.checkout_path, stub);
    assert!(
        resolved.mountpoint.ends_with(Path::new("acme/widget/main")),
        "a symlink-layout main mounts under the mount root: {}",
        resolved.mountpoint.display()
    );
}

#[tokio::test]
async fn dispatch_mounts_and_reports_mountpoint() {
    let fixture = Fixture::new("dispatch");
    let stub = fixture.root.join("checkout");
    fs::create_dir_all(&stub).expect("stub checkout");
    fixture.bind_main(&repo(), &stub, CheckoutLayout::DirectMount);
    let backend = FakeBackend::fresh(fixture.store.clone());

    let mut out = output();
    let exit = dispatch_mount_main(&backend, &repo(), false, &mut out)
        .await
        .expect("dispatch");
    assert_eq!(exit.code, 0);
    let calls = backend.calls();
    assert!(
        calls.contains(&"browse:false".to_owned()),
        "the verb mounts through the gateway path with browse disabled, calls: {calls:?}"
    );
    assert!(calls.contains(&"mount:acme/widget".to_owned()));
    let (stdout, stderr) = out.into_inner();
    assert_eq!(stdout, format!("{}\n", stub.display()).as_bytes());
    let stderr = String::from_utf8(stderr).expect("stderr");
    assert!(
        stderr.contains("acme/widget"),
        "the verb says what it mounted: {stderr}"
    );
}

#[tokio::test]
async fn dispatch_attempts_mount_rather_than_refusing_stale_state() {
    let fixture = Fixture::new("stale");
    let stub = fixture.root.join("checkout");
    fs::create_dir_all(&stub).expect("stub checkout");
    fixture.bind_main(&repo(), &stub, CheckoutLayout::DirectMount);
    // A flags-mismatched volume is still present at the mountpoint. The verb
    // must proceed to the gateway mount path — which remounts — rather than
    // refusing the stale state.
    let backend = FakeBackend::stale(fixture.store.clone());

    let mut out = output();
    let exit = dispatch_mount_main(&backend, &repo(), false, &mut out)
        .await
        .expect("stale state still mounts");
    assert_eq!(exit.code, 0);
    let calls = backend.calls();
    assert_eq!(
        calls,
        vec![
            "saw-stale-mount".to_owned(),
            "browse:false".to_owned(),
            "mount:acme/widget".to_owned(),
        ]
    );
    let (stdout, _) = out.into_inner();
    assert_eq!(stdout, format!("{}\n", stub.display()).as_bytes());
}

#[tokio::test]
async fn dispatch_json_reports_mount_result() {
    let fixture = Fixture::new("json");
    let stub = fixture.root.join("checkout");
    fs::create_dir_all(&stub).expect("stub checkout");
    fixture.bind_main(&repo(), &stub, CheckoutLayout::DirectMount);
    let backend = FakeBackend::fresh(fixture.store.clone());

    let mut out = output();
    dispatch_mount_main(&backend, &repo(), true, &mut out)
        .await
        .expect("dispatch");
    let (stdout, _) = out.into_inner();
    let envelope: serde_json::Value = serde_json::from_slice(&stdout).expect("json envelope");
    assert_eq!(envelope["result"]["workspace"], "main");
    assert_eq!(
        envelope["result"]["mount"],
        stub.display().to_string().as_str()
    );
}
