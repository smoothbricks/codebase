//! SliceBRekey fixtures: a keyless workspace must rekey to attachable.
//!
//! Layouts covered:
//!
//! * quarantined: canonical image + quarantine entry (`tombstone.json` +
//!   quarantined grants sidecar), no canonical sidecar/companion;
//! * live: canonical sidecar present, companion missing (the recovery pass
//!   has not quarantined it yet).
//!
//! A live mount carrying the workspace marker stands in for the degraded
//! mount (the marker needs no CA). After [`rekey_workspace`] the workspace
//! is `active`, the companion is present mode `0600`, and the inventory
//! admission predicate (active sidecar naming this exact incarnation)
//! passes, so a subsequent attach/inspect succeeds.

use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, GrantSet, ImageFormat, MACOS_PORT_BLOCK_MIN, MARKER_VERSION,
    PORT_BLOCK_SIZE, Platform, PortBlock, PublicationState, SIDECAR_VERSION, WorkspaceIncarnation,
    WorkspaceInfoSnapshot, WorkspaceMarker, WorkspaceName, WorkspaceRole, sidecar_path, write_json,
};
use cowshed_core::repository::RepoId;
use cowshed_core::storage::StorageLayout;
use cowshed_core::storage::WORKSPACE_MARKER_PATH;
use cowshed_core::storage::apfs::rekey::{RekeyError, rekey_workspace};
use cowshed_core::storage::lifecycle::{LifecycleWorkspace, Revision};
use cowshed_core::workspace_credentials::{mint_workspace_credentials, validate_private_key};
const INCARNATION: &str = "0198f2c0b7e34dc795f17b238b331c80";

const QUARANTINED_REVISION: u64 = 7;
const LIVE_REVISION: u64 = 5;

struct Fixture {
    layout: StorageLayout,
    workspace: WorkspaceName,
    incarnation: WorkspaceIncarnation,
    image: PathBuf,
    mount_point: PathBuf,
    entry: Option<PathBuf>,
}

fn temp_root(case: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "cowshed-rekey-{case}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    fs::create_dir_all(&root).expect("temp root");
    fs::canonicalize(&root).expect("canonical temp root")
}

fn sidecar(
    repo: &RepoId,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
    revision: u64,
    checkout: &Path,
) -> DetachedWorkspaceMetadata {
    let block = PortBlock::new(MACOS_PORT_BLOCK_MIN, PORT_BLOCK_SIZE).expect("port block");
    let mut grants = GrantSet::closed_baseline(Some(block)).expect("closed baseline grants");
    grants.revision = revision;
    DetachedWorkspaceMetadata {
        version: SIDECAR_VERSION,
        repo_id: repo.clone(),
        workspace: workspace.clone(),
        workspace_incarnation: incarnation.clone(),
        image_format: ImageFormat::Sparse,
        platform: Platform::Macos,
        publication_state: PublicationState::Active,
        updated_at: "2026-07-14T00:00:00Z".to_owned(),
        grants,
        info_snapshot: Some(WorkspaceInfoSnapshot {
            project_root: checkout.to_owned(),
            role: WorkspaceRole::Workspace,
            base_commit: "8f31c2d".to_owned(),
            branch: Some("raven".to_owned()),
            created_at: "2026-07-14T00:00:00Z".to_owned(),
            forked_from: None,
            captured_at: "2026-07-14T00:00:00Z".to_owned(),
            stale: false,
            git_worktree: false,
        }),
    }
}

fn base_fixture(case: &str) -> (PathBuf, Fixture) {
    let root = temp_root(case);
    let store = root.join("store");
    let mounts = root.join("mnt");
    fs::create_dir_all(&store).expect("store");
    fs::create_dir_all(&mounts).expect("mounts");

    let repo = RepoId::parse("acme/widget").expect("repo");
    let workspace = WorkspaceName::new("raven").expect("workspace");
    let incarnation = WorkspaceIncarnation::new(INCARNATION).expect("incarnation");
    let layout = StorageLayout::with_mount_root(&store, &mounts, &repo).expect("project layout");
    let paths = layout.project().clone();

    fs::create_dir_all(&paths.sessions).expect("sessions");
    let image = paths.sessions.join("raven.sparseimage");
    fs::write(&image, b"image").expect("image");

    // Degraded live mount: the marker carries identity and needs no CA.
    let mount_point = mounts.join("acme").join("widget").join("raven");
    fs::create_dir_all(mount_point.join(".cowshed")).expect("credential dir");
    write_json(
        &mount_point.join(WORKSPACE_MARKER_PATH),
        &WorkspaceMarker {
            version: MARKER_VERSION,
            repo_id: repo.clone(),
            project_root: root.join("checkout"),
            workspace: workspace.clone(),
            workspace_incarnation: incarnation.clone(),
            role: WorkspaceRole::Workspace,
            image_format: ImageFormat::Sparse,
            base_commit: "8f31c2d".to_owned(),
            created_at: "2026-07-14T00:00:00Z".to_owned(),
            forked_from: None,
            created_trace: "rekey-fixture".to_owned(),
            lineage: None,
        },
    )
    .expect("marker");

    (
        root,
        Fixture {
            layout,
            workspace,
            incarnation,
            image,
            mount_point,
            entry: None,
        },
    )
}

fn quarantined_fixture(case: &str) -> Fixture {
    let (root, mut fixture) = base_fixture(case);
    let paths = fixture.layout.project().clone();
    assert!(
        !sidecar_path(&fixture.image).exists(),
        "fixture must start with no canonical sidecar"
    );

    let entry = paths.quarantine.join("raven-1756944000");
    fs::create_dir_all(&entry).expect("quarantine entry");
    sidecar(
        &RepoId::parse("acme/widget").expect("repo"),
        &fixture.workspace,
        &fixture.incarnation,
        QUARANTINED_REVISION,
        &root.join("checkout"),
    )
    .write_for_image(&entry.join("raven.sparseimage"))
    .expect("quarantined sidecar");
    let quarantined_sidecar_path = sidecar_path(&entry.join("raven.sparseimage"));
    assert!(quarantined_sidecar_path.is_file());
    let tombstone = serde_json::json!({
        "version": 1,
        "repoId": "acme/widget",
        "workspace": "raven",
        "workspaceIncarnation": INCARNATION,
        "revision": QUARANTINED_REVISION,
        "reason": "missing-ca-companion",
        "image": fixture.image.to_str().expect("image utf8"),
        "companion": paths.sessions.join("raven.sparseimage.ca.key").to_str().expect("companion utf8"),
        "quarantinedAt": "2026-07-14T00:00:00Z",
        "sidecar": quarantined_sidecar_path.to_str().expect("sidecar utf8"),
    });
    fs::write(
        entry.join("tombstone.json"),
        serde_json::to_string_pretty(&tombstone).expect("tombstone json"),
    )
    .expect("tombstone");
    fixture.entry = Some(entry);
    fixture
}

fn live_keyless_fixture(case: &str) -> Fixture {
    let (root, fixture) = base_fixture(case);
    sidecar(
        &RepoId::parse("acme/widget").expect("repo"),
        &fixture.workspace,
        &fixture.incarnation,
        LIVE_REVISION,
        &root.join("checkout"),
    )
    .write_for_image(&fixture.image)
    .expect("live sidecar");
    fixture
}

fn assert_attachable(fixture: &Fixture, expected_revision: u64) {
    // The republished sidecar admits the workspace: active, same incarnation,
    // expected revision.
    let metadata = DetachedWorkspaceMetadata::read_for_image(&fixture.image)
        .expect("republished sidecar reads");
    assert_eq!(metadata.publication_state, PublicationState::Active);
    assert_eq!(metadata.workspace, fixture.workspace);
    assert_eq!(metadata.workspace_incarnation, fixture.incarnation);
    assert_eq!(metadata.grants.revision, expected_revision);

    // The companion is present, private, and a valid key: the
    // MissingCaCompanion condition (missing companion) no longer holds.
    let companion = PathBuf::from(format!("{}.ca.key", fixture.image.display()));
    let mode = fs::symlink_metadata(&companion)
        .expect("companion exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "companion must be mode 0600");
    validate_private_key(&companion).expect("companion is a valid private key");

    // In-image assets pair with the new companion through the existing mint path.
    assert!(fixture.mount_point.join(".cowshed/ca.pem").is_file());
    assert!(fixture.mount_point.join(".cowshed/token").is_file());
}

#[test]
fn rekey_converts_keyless_quarantined_workspace_to_attachable() {
    let fixture = quarantined_fixture("quarantined");
    let report = rekey_workspace(&fixture.layout, &fixture.workspace, &fixture.mount_point)
        .expect("rekey converts to active");

    // Revision bumps exactly once over the quarantined generation; identity is preserved.
    assert_eq!(report.revision, QUARANTINED_REVISION + 1);
    assert_eq!(report.workspace, fixture.workspace);
    assert_eq!(report.incarnation, fixture.incarnation);

    assert_attachable(&fixture, QUARANTINED_REVISION + 1);

    // The tombstone is consumed, not left behind.
    let entry = fixture.entry.expect("quarantine entry");
    assert!(!entry.exists(), "quarantine entry must be removed");
    assert_eq!(report.tombstone_removed, Some(entry));
}

#[test]
fn rekey_preserves_revision_when_the_sidecar_never_left() {
    let fixture = live_keyless_fixture("live");
    let report = rekey_workspace(&fixture.layout, &fixture.workspace, &fixture.mount_point)
        .expect("rekey heals the live sidecar");

    // Nothing was removed, so there is nothing to supersede: the revision is
    // preserved while the CA still rotates.
    assert_eq!(report.revision, LIVE_REVISION);
    assert_eq!(report.tombstone_removed, None);

    assert_attachable(&fixture, LIVE_REVISION);
}

#[test]
fn rekey_refuses_a_workspace_that_is_already_keyed() {
    let (root, fixture) = base_fixture("keyed");
    let repo = RepoId::parse("acme/widget").expect("repo");
    let block = PortBlock::new(MACOS_PORT_BLOCK_MIN, PORT_BLOCK_SIZE).expect("port block");
    sidecar(
        &repo,
        &fixture.workspace,
        &fixture.incarnation,
        LIVE_REVISION,
        &root.join("checkout"),
    )
    .write_for_image(&fixture.image)
    .expect("live sidecar");
    let workspace = LifecycleWorkspace::new(
        repo,
        fixture.workspace.clone(),
        fixture.incarnation.clone(),
        Revision::new(LIVE_REVISION),
        Revision::new(LIVE_REVISION),
        WorkspaceRole::Workspace,
        ImageFormat::Sparse,
    )
    .expect("lifecycle workspace");
    let companion = PathBuf::from(format!("{}.ca.key", fixture.image.display()));
    mint_workspace_credentials(
        &workspace,
        &fixture.mount_point,
        &fixture.mount_point,
        Platform::Macos,
        Some(block),
        &companion,
    )
    .expect("pre-existing companion");

    let error = rekey_workspace(&fixture.layout, &fixture.workspace, &fixture.mount_point)
        .expect_err("healthy workspace must refuse rekey");
    assert!(
        matches!(error, RekeyError::AlreadyKeyed { .. }),
        "unexpected error: {error:?}"
    );
}

#[test]
fn rekey_refuses_main_and_an_unmounted_workspace() {
    let fixture = live_keyless_fixture("refusals");
    let main = WorkspaceName::new("main").expect("main");
    assert!(matches!(
        rekey_workspace(&fixture.layout, &main, &fixture.mount_point),
        Err(RekeyError::MainWorkspace)
    ));

    let missing_mount = fixture.mount_point.join("nowhere");
    assert!(matches!(
        rekey_workspace(&fixture.layout, &fixture.workspace, &missing_mount),
        Err(RekeyError::NotMounted { .. })
    ));
}
