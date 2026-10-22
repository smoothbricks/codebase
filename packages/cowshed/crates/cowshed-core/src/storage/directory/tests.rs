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

/// `gc --recover` takes a recovery image a retirement left in a directory store out of the store:
/// the image, its grants sidecar and CA companion move, never copied or deleted, beside a manifest
/// naming the workspace, its incarnation and why the retirement kept it, and the trash keeps
/// nothing a terminal removal would count (02_workspaces.md, `rm` step 5). A path that is not a
/// recovery image, a destination inside the store and an occupied one refuse before anything
/// moves; a move interrupted before the image went is finished by the same request.
#[test]
fn a_recovery_image_moves_out_of_the_store_with_its_companions_and_a_manifest() {
    use super::*;
    use crate::api::dto::{RecoveryCause, RecoveryRetention};
    use crate::error::ErrorCode;
    use crate::storage::deletion_log::{self, Caller, KeptRecovery, RetiredBy};
    use crate::storage::recovery_image::{MANIFEST_FILE, recover};

    let scratch = Scratch::new("recover-image");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let storage = layout(&host.config, &metadata.repo_id).unwrap();
    let project = storage.project();
    let repo = &metadata.repo_id;
    let trash = project.sessions.join(format!(
        ".trash/selected-{}.dmg",
        metadata.workspace_incarnation
    ));
    fs::create_dir_all(trash.parent().unwrap()).unwrap();
    fs::hard_link(sidecar_path(&image), sidecar_path(&trash)).unwrap();
    fs::hard_link(companion_path(&image), companion_path(&trash)).unwrap();
    host.retire_image(&image, &trash).unwrap();
    let key = fs::read(companion_path(&trash)).unwrap();
    let because = vec![RecoveryCause::OnlyCopy {
        path: PathBuf::from("notes/only.txt"),
    }];
    deletion_log::log_retirement(
        &project.project_root,
        "selected",
        &scratch.path().join("mnt/selected"),
        None,
        RetiredBy {
            caller: Caller {
                pid: 1,
                argv: vec!["cowshed".to_owned(), "rm".to_owned()],
                parent: None,
            },
            forced: None,
        },
        Some(KeptRecovery {
            image: trash.clone(),
            because: because.clone(),
        }),
    );

    let to = scratch.path().join("recovered");
    let inside = project.store_root.join("kept");
    assert_eq!(
        recover(&storage, repo, &trash, &inside).unwrap_err().code,
        ErrorCode::Usage
    );
    assert!(
        !inside.exists(),
        "a destination inside the store is never made"
    );
    assert_eq!(
        recover(&storage, repo, &sidecar_path(&trash), &to)
            .unwrap_err()
            .code,
        ErrorCode::Usage,
        "a companion is not a recovery image"
    );
    assert_eq!(
        recover(&storage, repo, &image, &to).unwrap_err().code,
        ErrorCode::Usage,
        "the canonical name is not the trash"
    );
    let directory = to.join(format!("selected-{}", metadata.workspace_incarnation));
    fs::create_dir_all(&directory).unwrap();
    assert_eq!(
        recover(&storage, repo, &trash, &to).unwrap_err().code,
        ErrorCode::Conflict,
        "an occupied destination is never merged into"
    );
    fs::remove_dir(&directory).unwrap();
    assert_eq!(
        recover(
            &storage,
            repo,
            Path::new("sessions/.trash/selected.dmg"),
            &to
        )
        .unwrap_err()
        .code,
        ErrorCode::Usage,
        "a relative path is refused, whatever the caller's working directory"
    );
    let lock = storage
        .session_image(&metadata.workspace)
        .unwrap()
        .lock()
        .to_owned();
    let held = host
        .lock_images(std::slice::from_ref(&lock), LockMode::Wait)
        .unwrap()
        .unwrap();
    assert_eq!(
        recover(&storage, repo, &trash, &to).unwrap_err().code,
        ErrorCode::Conflict,
        "another operation holding the workspace's lifecycle lock refuses the move"
    );
    drop(held);
    assert!(!directory.exists());
    assert!(trash.join(WORKSPACE_MARKER_PATH).exists());
    assert!(sidecar_path(&trash).exists());

    let recovered = recover(&storage, repo, &trash, &to).unwrap();
    let moved = directory.join(trash.file_name().unwrap());
    assert_eq!(recovered.workspace, metadata.workspace);
    assert_eq!(recovered.incarnation, metadata.workspace_incarnation);
    assert_eq!(recovered.source, trash);
    assert_eq!(recovered.directory, directory);
    assert_eq!(recovered.manifest, directory.join(MANIFEST_FILE));
    assert_eq!(
        recovered.moved,
        [sidecar_path(&moved), companion_path(&moved), moved.clone()]
    );
    assert!(recovered.grants);
    assert!(matches!(
        &recovered.retained,
        RecoveryRetention::Recorded { because: recorded, .. } if *recorded == because
    ));
    assert!(moved.join(WORKSPACE_MARKER_PATH).exists());
    assert_eq!(fs::read(companion_path(&moved)).unwrap(), key);
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(&moved).unwrap(),
        metadata
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&recovered.manifest).unwrap()).unwrap();
    assert_eq!(manifest["workspace"], "selected");
    assert_eq!(
        manifest["incarnation"],
        metadata.workspace_incarnation.as_str()
    );
    assert_eq!(manifest["source"], trash.to_str().unwrap());
    assert_eq!(manifest["retained"]["because"][0]["kind"], "onlyCopy");
    assert_eq!(
        fs::read_dir(trash.parent().unwrap()).unwrap().count(),
        0,
        "the trash keeps nothing of the image"
    );
    let log =
        fs::read_to_string(project.project_root.join(deletion_log::DELETION_LOG_FILE)).unwrap();
    assert_eq!(
        log.lines()
            .filter(|line| line.contains("\"recover-retired-image\""))
            .count(),
        3
    );

    // Interrupted after its companions went and before the image did: the same request finishes.
    fs::rename(&moved, &trash).unwrap();
    assert_eq!(recover(&storage, repo, &trash, &to).unwrap(), recovered);
    assert!(!trash.exists());
    assert!(moved.join(WORKSPACE_MARKER_PATH).exists());
    // Interrupted after everything went: the same request answers the finished move.
    assert_eq!(recover(&storage, repo, &trash, &to).unwrap(), recovered);

    // A manifest that is ordered but short, or whose grants flag contradicts its list, moves
    // nothing: the image would otherwise leave its companions behind.
    let image_name = trash.file_name().unwrap().to_str().unwrap().to_owned();
    for path in [sidecar_path(&moved), companion_path(&moved), moved.clone()] {
        fs::rename(&path, trash.with_file_name(path.file_name().unwrap())).unwrap();
    }
    let written = fs::read(&recovered.manifest).unwrap();
    for (files, grants) in [
        (serde_json::json!([image_name]), false),
        (
            serde_json::json!([
                format!("{image_name}.grants.json"),
                format!("{image_name}.ca.key"),
                image_name
            ]),
            false,
        ),
    ] {
        let mut tampered: serde_json::Value = serde_json::from_slice(&written).unwrap();
        tampered["files"] = files;
        tampered["grants"] = serde_json::Value::Bool(grants);
        fs::write(&recovered.manifest, serde_json::to_vec(&tampered).unwrap()).unwrap();
        assert_eq!(
            recover(&storage, repo, &trash, &to).unwrap_err().code,
            ErrorCode::Integrity
        );
        assert!(trash.join(WORKSPACE_MARKER_PATH).exists());
        assert!(sidecar_path(&trash).exists());
        assert!(companion_path(&trash).exists());
    }
    fs::write(&recovered.manifest, &written).unwrap();
    assert_eq!(recover(&storage, repo, &trash, &to).unwrap(), recovered);

    // A link where the recovered directory belongs is never followed, even to this image's own
    // manifest: it could aim the move anywhere, back into the store included.
    let elsewhere = scratch.path().join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    let relocated = elsewhere.join(directory.file_name().unwrap());
    fs::rename(&directory, &relocated).unwrap();
    std::os::unix::fs::symlink(&relocated, &directory).unwrap();
    fs::rename(relocated.join(trash.file_name().unwrap()), &trash).unwrap();
    assert!(!sidecar_path(&trash).exists());
    assert_eq!(
        recover(&storage, repo, &trash, &to).unwrap_err().code,
        ErrorCode::Conflict
    );
    assert!(trash.join(WORKSPACE_MARKER_PATH).exists());
}
