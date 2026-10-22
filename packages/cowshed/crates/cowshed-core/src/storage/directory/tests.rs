use std::fs;
use std::path::{Path, PathBuf};

use cowshed_apfs::DetachIntent;

use super::{
    MountPointState, Payload, bind, mount_point_state, move_sibling, observed_mount, settle,
    tree_usage, unbind,
};

const SOURCE: &[u8] = b"pub fn answer() -> u8 { 42 }\n";

struct Scratch(PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "cowshed-directory-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        // The paths a mounted tree resolves to are compared exactly, so the scratch root is
        // spelled the way the kernel resolves it (`/var` is `/private/var` on macOS).
        Self(fs::canonicalize(root).unwrap())
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn payload(&self, name: &str) -> PathBuf {
        let image = self.0.join(name);
        fs::create_dir_all(image.join("src")).unwrap();
        fs::write(image.join("src/lib.rs"), SOURCE).unwrap();
        image
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn a_mounted_payload_resolves_to_its_mountpoint_not_the_store() {
    let scratch = Scratch::new("identity");
    let image = scratch.payload("main.dmg");
    let mount = scratch.path().join("checkout");
    fs::create_dir(&mount).unwrap();

    bind(&image, &mount).unwrap();
    // The tree physically lives at the mountpoint: realpath (what getcwd, git and Seatbelt
    // see) is the mountpoint itself.
    assert_eq!(
        fs::canonicalize(mount.join("src")).unwrap(),
        mount.join("src")
    );
    assert!(fs::symlink_metadata(&mount).unwrap().is_dir());
    assert_eq!(fs::read(mount.join("src/lib.rs")).unwrap(), SOURCE);
    // The image path is the mount record naming it.
    assert_eq!(fs::read_link(&image).unwrap(), mount);
    assert_eq!(settle(&image).unwrap(), Payload::Mounted(mount.clone()));
    assert_eq!(observed_mount(&image).unwrap(), Some(mount.clone()));
    // Idempotent where it already is.
    bind(&image, &mount).unwrap();

    unbind(&image, &mount, DetachIntent::Release).unwrap();
    assert_eq!(mount_point_state(&mount).unwrap(), MountPointState::Vacant);
    assert!(fs::symlink_metadata(&image).unwrap().is_dir());
    assert_eq!(fs::read(image.join("src/lib.rs")).unwrap(), SOURCE);
    assert!(!move_sibling(&image).exists());
}

#[test]
fn mounting_creates_an_absent_mountpoint() {
    let scratch = Scratch::new("absent");
    let image = scratch.payload("feature.dmg");
    let mount = scratch.path().join("mnt/owner/repo/feature");

    bind(&image, &mount).unwrap();
    assert_eq!(fs::read(mount.join("src/lib.rs")).unwrap(), SOURCE);
    assert_eq!(settle(&image).unwrap(), Payload::Mounted(mount));
}

#[test]
fn mounting_refuses_an_occupied_mountpoint_and_a_second_mountpoint() {
    let scratch = Scratch::new("occupied");
    let image = scratch.payload("a.dmg");
    let occupied = scratch.path().join("occupied");
    fs::create_dir(&occupied).unwrap();
    fs::write(occupied.join("work.txt"), b"unsaved").unwrap();
    assert!(bind(&image, &occupied).is_err());
    assert_eq!(fs::read(occupied.join("work.txt")).unwrap(), b"unsaved");
    assert_eq!(settle(&image).unwrap(), Payload::Home);

    let mount = scratch.path().join("mount");
    bind(&image, &mount).unwrap();
    let elsewhere = scratch.path().join("elsewhere");
    assert!(bind(&image, &elsewhere).is_err());
    assert!(
        unbind(&image, &elsewhere, DetachIntent::Release).is_err(),
        "unmounting names its own mountpoint"
    );
    assert_eq!(settle(&image).unwrap(), Payload::Mounted(mount));
}

#[test]
fn a_mount_interrupted_between_its_steps_is_finished() {
    let scratch = Scratch::new("interrupted");
    let image = scratch.payload("a.dmg");
    let mount = scratch.path().join("mount");
    fs::create_dir(&mount).unwrap();
    // The state a crash between the exchange and the rename leaves.
    let sibling = move_sibling(&image);
    fs::rename(&image, &sibling).unwrap();
    std::os::unix::fs::symlink(&mount, &image).unwrap();
    assert_eq!(
        observed_mount(&image).unwrap(),
        None,
        "a fact pass sees no mount yet"
    );

    assert_eq!(settle(&image).unwrap(), Payload::Mounted(mount.clone()));
    assert_eq!(fs::read(mount.join("src/lib.rs")).unwrap(), SOURCE);
    assert!(!sibling.exists());
}

#[test]
fn unmounting_nothing_mounted_changes_nothing() {
    let scratch = Scratch::new("unmounted");
    let image = scratch.payload("a.dmg");
    let tree = scratch.payload("user-checkout");
    unbind(&image, &tree, DetachIntent::Release).unwrap();
    assert!(tree.join("src/lib.rs").exists());
    assert!(image.join("src/lib.rs").exists());
}

#[test]
fn tree_usage_counts_hard_links_once_and_never_follows_links() {
    let scratch = Scratch::new("usage");
    let outside = scratch.payload("outside");
    fs::write(outside.join("big"), vec![7_u8; 1 << 20]).unwrap();
    let tree = scratch.payload("tree");
    fs::write(tree.join("data"), vec![1_u8; 8192]).unwrap();
    fs::hard_link(tree.join("data"), tree.join("data-alias")).unwrap();
    std::os::unix::fs::symlink(&outside, tree.join("link")).unwrap();

    let usage = tree_usage(&tree).unwrap();
    assert_eq!(usage.logical, 8192 + SOURCE.len() as u64);
    assert!(usage.allocated < 1 << 20, "the linked tree is not counted");
}

fn published_directory(
    scratch: &Scratch,
    name: &str,
    incarnation: u128,
) -> (
    super::DirectoryExecutionHost,
    PathBuf,
    crate::metadata::DetachedWorkspaceMetadata,
) {
    use super::*;
    use crate::metadata::{GrantSet, PortBlock};
    let config = ApfsSubstrateConfig::new(
        scratch.path().join("store"),
        scratch.path().join("checkout"),
    );
    let repo = RepoId::parse("fixture/directory").unwrap();
    let workspace = WorkspaceName::session(name).unwrap();
    let storage = layout(&config, &repo).unwrap();
    // Adoption's own provisioning, which marks the owner store recovery reads.
    fs::create_dir_all(&config.store_root).unwrap();
    storage.provision_project().unwrap();
    let image = storage
        .session_image(&workspace)
        .unwrap()
        .image()
        .to_owned();
    fs::create_dir_all(&image).unwrap();
    let metadata = DetachedWorkspaceMetadata {
        version: SIDECAR_VERSION,
        repo_id: repo,
        workspace,
        workspace_incarnation: WorkspaceIncarnation::new(format!("{incarnation:032x}")).unwrap(),
        platform: Platform::Macos,
        publication_state: PublicationState::Active,
        updated_at: "2026-10-14T00:00:00Z".to_owned(),
        grants: GrantSet::closed_baseline(Some(PortBlock::new(32768, 64).unwrap())).unwrap(),
        info_snapshot: WorkspaceInfoSnapshot {
            project_root: config.checkout_path.clone(),
            role: WorkspaceRole::Workspace,
            base_commit: "0123456789abcdef".to_owned(),
            branch: Some(format!("cowshed/{name}")),
            created_at: "2026-10-14T00:00:00Z".to_owned(),
            forked_from: None,
            captured_at: "2026-10-14T00:00:00Z".to_owned(),
            stale: false,
            git_worktree: false,
        },
    };
    metadata.write_for_image(&image).unwrap();
    let identity = metadata_workspace_ref(&metadata).unwrap();
    mint_workspace_credentials(
        &identity,
        &image,
        metadata.platform,
        metadata.grants.port_block,
        &companion_path(&image),
    )
    .unwrap();
    let marker = WorkspaceMarker {
        version: MARKER_VERSION,
        repo_id: metadata.repo_id.clone(),
        project_root: metadata.info_snapshot.project_root.clone(),
        workspace: metadata.workspace.clone(),
        workspace_incarnation: metadata.workspace_incarnation.clone(),
        role: WorkspaceRole::Workspace,
        base_commit: metadata.info_snapshot.base_commit.clone(),
        created_at: metadata.info_snapshot.created_at.clone(),
        forked_from: None,
        created_trace: "directory-fixture".to_owned(),
        lineage: None,
        owned_links_revision: 0,
    };
    crate::metadata::write_json(&image.join(WORKSPACE_MARKER_PATH), &marker).unwrap();
    (
        DirectoryExecutionHost::new(config).unwrap(),
        image,
        metadata,
    )
}

#[test]
fn directory_named_snapshot_and_mount_fence_ignore_broken_siblings() {
    use super::*;
    let scratch = Scratch::new("named-record");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let (_, missing, _) = published_directory(&scratch, "missing", 2);
    fs::remove_file(sidecar_path(&missing)).unwrap();
    let (_, corrupt, _) = published_directory(&scratch, "corrupt", 3);
    fs::write(sidecar_path(&corrupt), b"{ corrupt").unwrap();
    let store = layout(&host.config, &metadata.repo_id).unwrap();
    let label = CheckpointLabel::new("ready").unwrap();
    let checkpoint = store
        .checkpoint_image(&metadata.workspace, &label)
        .unwrap()
        .image()
        .to_owned();
    fs::create_dir_all(&checkpoint).unwrap();
    metadata.write_for_image(&checkpoint).unwrap();
    host.publish_checkpoint_fact(&checkpoint, &label, Revision::new(3), Pin::Pinned)
        .unwrap();
    let poisoned_checkpoint = store.project().checkpoints.join("corrupt/bad.dmg");
    fs::create_dir_all(&poisoned_checkpoint).unwrap();
    fs::write(checkpoint_fact_path(&poisoned_checkpoint), b"{ corrupt").unwrap();
    assert!(host.list(&metadata.repo_id).is_err());
    assert!(host.checkpoints(&metadata.repo_id).is_err());
    let (detached, _) = host
        .workspace_snapshot(&metadata.repo_id, &metadata.workspace)
        .unwrap()
        .unwrap();
    assert_eq!(detached.mount_state, MountState::Detached);
    assert_eq!(detached.checkpoints.len(), 1);
    assert_eq!(detached.checkpoints[0].revision, Revision::new(3));
    assert_eq!(detached.checkpoints[0].pin, Pin::Pinned);
    let mount =
        main_aware_mount_point(&host.config, &metadata.repo_id, &metadata.workspace).unwrap();
    bind(&image, &mount).unwrap();
    let (mounted, current) = host
        .workspace_snapshot(&metadata.repo_id, &metadata.workspace)
        .unwrap()
        .unwrap();
    assert_eq!(current, metadata);
    assert!(matches!(mounted.mount_state, MountState::Mounted { .. }));
    assert_eq!(
        host.workspace_mount_state(&mounted.workspace).unwrap(),
        mounted.mount_state
    );
    let mut stale = WorkspaceMarker::read_from(&mount.join(WORKSPACE_MARKER_PATH)).unwrap();
    stale.workspace_incarnation = WorkspaceIncarnation::new(format!("{:032x}", 99)).unwrap();
    crate::metadata::write_json(&mount.join(WORKSPACE_MARKER_PATH), &stale).unwrap();
    assert!(host.workspace_mount_state(&mounted.workspace).is_err());
    assert!(
        host.workspace_snapshot(
            &metadata.repo_id,
            &WorkspaceName::session("missing").unwrap()
        )
        .is_err()
    );
    assert!(
        host.workspace_snapshot(
            &metadata.repo_id,
            &WorkspaceName::session("corrupt").unwrap()
        )
        .is_err()
    );
    unbind(&image, &mount, DetachIntent::Release).unwrap();
}

#[test]
fn directory_source_release_keeps_canonical_authority_on_cleanup_refusal() {
    use super::*;
    let scratch = Scratch::new("direct-source-release");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let workspace = metadata_workspace_ref(&metadata).unwrap();
    let retired = RetiredRef::new(workspace, Revision::new(1));
    let storage = layout(&host.config, &metadata.repo_id).unwrap();
    let checkpoints = storage.project().checkpoints.join("selected");
    fs::create_dir_all(&checkpoints).unwrap();
    let foreign = checkpoints.join("only-copy");
    fs::write(&foreign, b"preserve checkpoint evidence").unwrap();
    assert!(host.release_workspace(&host.config, &retired).is_err());
    assert!(image.exists());
    assert!(sidecar_path(&image).exists());
    assert!(companion_path(&image).exists());
    assert_eq!(fs::read(&foreign).unwrap(), b"preserve checkpoint evidence");
    fs::remove_file(&foreign).unwrap();
    host.release_workspace(&host.config, &retired).unwrap();
    assert!(!image.exists());
    assert!(!sidecar_path(&image).exists());
    assert!(!companion_path(&image).exists());
    assert!(!storage.project().sessions.join(".trash").exists());
}

#[test]
fn directory_retirement_retry_and_held_recovery_keep_complete_authority() {
    use super::*;
    let scratch = Scratch::new("retirement-scope");
    let (host, image, mut metadata) = published_directory(&scratch, "selected", 1);
    let storage = layout(&host.config, &metadata.repo_id).unwrap();
    let trash = storage.project().sessions.join(format!(
        ".trash/selected-{}.dmg",
        metadata.workspace_incarnation
    ));
    fs::create_dir_all(trash.parent().unwrap()).unwrap();
    let misnamed = trash.with_file_name(format!("selected-{:032x}.dmg", 99));
    assert!(matches!(
        host.retire_image(&image, &misnamed),
        Err(ApfsStorageError::MarkerMismatch(_))
    ));
    assert!(image.exists());
    assert!(!sidecar_path(&misnamed).exists());
    let key = fs::read(companion_path(&image)).unwrap();
    fs::hard_link(sidecar_path(&image), sidecar_path(&trash)).unwrap();
    fs::hard_link(companion_path(&image), companion_path(&trash)).unwrap();
    assert!(
        host.workspace_snapshot(&metadata.repo_id, &metadata.workspace)
            .unwrap()
            .is_some()
    );
    metadata.updated_at = "2026-10-14T01:00:00Z".to_owned();
    metadata.write_for_image(&image).unwrap();
    host.retire_image(&image, &trash).unwrap();
    assert!(!image.exists());
    assert!(!sidecar_path(&image).exists());
    assert!(!companion_path(&image).exists());
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&trash).unwrap(),
        metadata
    );
    assert_eq!(fs::read(companion_path(&trash)).unwrap(), key);
    assert!(
        host.workspace_snapshot(&metadata.repo_id, &metadata.workspace)
            .unwrap()
            .is_none()
    );
    assert!(host.retire_image(&image, &trash).is_err());

    let (_, committed, victim) = published_directory(&scratch, "victim", 2);
    let committed_trash = storage.project().sessions.join(format!(
        ".trash/victim-{}.dmg",
        victim.workspace_incarnation
    ));
    fs::hard_link(sidecar_path(&committed), sidecar_path(&committed_trash)).unwrap();
    fs::hard_link(companion_path(&committed), companion_path(&committed_trash)).unwrap();
    fs::rename(&committed, &committed_trash).unwrap();
    let poisoned = [
        sidecar_path(
            storage
                .session_image(&WorkspaceName::session("broken").unwrap())
                .unwrap()
                .image(),
        ),
        sidecar_path(
            &storage
                .project()
                .project_root
                .join(format!(".staging/broken-{:032x}.dmg", 3)),
        ),
        sidecar_path(
            &storage
                .project()
                .checkpoints
                .join(format!("broken/pre-restore-{:032x}.dmg", 3)),
        ),
        sidecar_path(&host.config.store_root.join("foreign/project/main.dmg")),
    ];
    for record in &poisoned {
        fs::create_dir_all(record.parent().unwrap()).unwrap();
        fs::write(record, b"{ corrupt").unwrap();
    }
    let lock = storage
        .session_image(&victim.workspace)
        .unwrap()
        .lock()
        .to_owned();
    let _held = host
        .lock_images(std::slice::from_ref(&lock), LockMode::Wait)
        .unwrap()
        .unwrap();
    for _ in 0..2 {
        host.recover_pending(&host.config, std::slice::from_ref(&lock))
            .unwrap();
        assert!(!sidecar_path(&committed).exists());
        assert!(!companion_path(&committed).exists());
        assert_eq!(
            DetachedWorkspaceMetadata::read_for_image(&committed_trash).unwrap(),
            victim
        );
        assert!(companion_path(&committed_trash).exists());
        for record in &poisoned {
            assert_eq!(fs::read(record).unwrap(), b"{ corrupt");
        }
    }
    assert!(host.recover_pending(&host.config, &[committed]).is_err());
    assert!(host.recover_pending(&host.config, &[]).is_err());
}
