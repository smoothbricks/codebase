use cowshed_os::test_backstop::CommandBackstop as _;
use std::fs;
use std::path::{Path, PathBuf};

use cowshed_apfs::DetachIntent;

use super::{
    MountPointState, Payload, bind, mount_point_state, move_sibling, observed_mount, settle,
    tree_usage, unbind,
};

const SOURCE: &[u8] = b"pub fn answer() -> u8 { 42 }\n";

pub(crate) struct Scratch(PathBuf);

impl Scratch {
    pub(crate) fn new(label: &str) -> Self {
        Self::new_in(&std::env::temp_dir(), label)
    }

    pub(crate) fn new_in(parent: &Path, label: &str) -> Self {
        let root = parent.join(format!(
            "cowshed-directory-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        // The paths a mounted tree resolves to are compared exactly, so the scratch root is
        // spelled the way the kernel resolves it (`/var` is `/private/var` on macOS).
        Self(fs::canonicalize(root).unwrap())
    }

    pub(crate) fn path(&self) -> &Path {
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

pub(crate) fn published_directory(
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
    use cowshed_os::namespace::{HostNamespace, TestNamespace};
    let config = ApfsSubstrateConfig::new(
        HostNamespace::Test(TestNamespace::mint().expect("a test namespace")),
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

/// The store paths a published fixture's release and a project's terminal-storage guard
/// both name: its project root, and the per-workspace temp dir below it. Shared so a test
/// outside this module composes the actual release with that guard on one store.
pub(crate) fn published_directory_paths(
    host: &super::DirectoryExecutionHost,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
) -> (PathBuf, PathBuf) {
    use super::*;
    let storage = layout(&host.config, &metadata.repo_id).unwrap();
    (
        storage.project().project_root.clone(),
        storage.exec_temp_dir(&metadata.workspace).unwrap(),
    )
}

/// Release a published fixture through the same RetiredRef authority its retiring image
/// carries in production.
pub(crate) fn release_published_directory(
    host: &super::DirectoryExecutionHost,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
) {
    use super::*;
    let workspace = metadata_workspace_ref(metadata).unwrap();
    let retired = RetiredRef::new(workspace, Revision::new(1));
    host.release_workspace(&host.config, &retired).unwrap();
}

/// The published fixture's storage layout, for tests outside this module composing the
/// fixture with store-level operations a layout names (recovery export above all).
pub(crate) fn published_directory_layout(
    host: &super::DirectoryExecutionHost,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
) -> crate::storage::StorageLayout {
    use super::*;
    layout(&host.config, &metadata.repo_id).unwrap()
}

/// Where a published fixture's payload retires to: the trash image a recovery retention
/// keeps, named after the workspace and its incarnation.
pub(crate) fn published_directory_trash(
    host: &super::DirectoryExecutionHost,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
) -> PathBuf {
    use super::*;
    layout(&host.config, &metadata.repo_id)
        .unwrap()
        .project()
        .sessions
        .join(format!(
            ".trash/{}-{}.dmg",
            metadata.workspace.as_str(),
            metadata.workspace_incarnation
        ))
}

/// Reclaim a published fixture's retained trash image through its exact RetiredRef, the
/// authority gc's collection of a retirement carries: image disposal included, unlike
/// the recovery export, which must preserve the image.
pub(crate) fn reclaim_retired_published_directory(
    host: &super::DirectoryExecutionHost,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
) {
    use super::*;
    let workspace = metadata_workspace_ref(metadata).unwrap();
    let revision = Revision::new(workspace.revision().get().saturating_add(1));
    let retired = RetiredRef::new(workspace, revision);
    host.reclaim_retired(&host.config, &retired).unwrap();
}
#[test]
fn a_retired_workspaces_read_only_compile_cache_is_reclaimed_with_its_temp_dir() {
    use super::*;
    use crate::capabilities::CapabilityContribution;
    use crate::workspace_environment::{
        EnvironmentMount, NODE_COMPILE_CACHE_DIRECTORY, NODE_COMPILE_CACHE_ENV,
        WORKSPACE_ENVIRONMENT_PATH, write_workspace_environment,
    };

    fn files_under(root: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            let entries = match fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => panic!("read {}: {error}", directory.display()),
            };
            for entry in entries {
                let entry = entry.unwrap_or_else(|error| panic!("read entry: {error}"));
                if entry.file_type().expect("entry type").is_dir() {
                    pending.push(entry.path());
                } else {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        files
    }

    fn host_program(name: &str) -> PathBuf {
        let path = std::env::var_os("PATH").expect("the gate PATH names the host toolchain");
        std::env::split_paths(&path)
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
            .unwrap_or_else(|| panic!("host {name} is required for this proof"))
    }

    // The producer's actual call, then a real module load whose bytecode the runtime
    // flushes into the resolved cache before it exits.
    const COMPILE: &str = r#"const m = require("node:module");
m.enableCompileCache();
const dir = m.getCompileCacheDir();
if (!dir) { console.error("compile cache is not enabled"); process.exit(3); }
console.log(dir);
import("./compile-cache-lib.mjs").then((lib) => {
  if (lib.answer() !== 42) { process.exit(4); }
  if (m.flushCompileCache) { m.flushCompileCache(); }
});
"#;

    let scratch = Scratch::new("compile-cache-retire");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let storage = layout(&host.config, &metadata.repo_id).unwrap();
    let (_, exec_temp) = published_directory_paths(&host, &metadata);
    fs::create_dir_all(&exec_temp).unwrap();
    // Another workspace's temp dir and the caller's own cache are foreign to this release.
    let sibling = storage
        .exec_temp_dir(&WorkspaceName::session("sibling").unwrap())
        .unwrap()
        .join("keep");
    fs::create_dir_all(sibling.parent().unwrap()).unwrap();
    fs::write(&sibling, b"a sibling's own").unwrap();
    let caller_sentinel = scratch.path().join("caller-cache/sentinel");
    fs::create_dir_all(caller_sentinel.parent().unwrap()).unwrap();
    fs::write(&caller_sentinel, b"the caller's own").unwrap();

    // Serve the fixture's image the way a supervisor start does, then let an activated
    // read-only shell (its XDG_CACHE_HOME is the exec-temp cache) compile into it.
    write_workspace_environment(
        EnvironmentMount::Served {
            workspace_mount: &image,
            host_home: scratch.path(),
            temp_dir: &exec_temp,
            capability_env: &CapabilityContribution::default(),
        },
        metadata.platform,
        metadata.grants.port_block,
    )
    .unwrap();
    let work = scratch.path().join("work");
    fs::create_dir_all(&work).unwrap();
    fs::write(
        work.join("compile-cache-lib.mjs"),
        "export function answer() { return 42; }\n",
    )
    .unwrap();
    let script = work.join("compile.cjs");
    fs::write(&script, COMPILE).unwrap();
    let owned = exec_temp.join("cache").join(NODE_COMPILE_CACHE_DIRECTORY);
    for program in [host_program("node"), host_program("bun")] {
        let mut command = std::process::Command::new("/bin/sh");
        command
            .env_clear()
            .args(["-c", ". \"$1\"; exec \"$2\" \"$3\"", "read-only-shell"])
            .arg(image.join(WORKSPACE_ENVIRONMENT_PATH))
            .arg(&program)
            .arg(&script)
            .current_dir(&work)
            .env("HOME", scratch.path())
            .env("XDG_CACHE_HOME", exec_temp.join("cache"))
            .env(NODE_COMPILE_CACHE_ENV, scratch.path().join("caller-cache"));
        let output = command.output_bounded().unwrap();
        assert!(output.status.success(), "{}: {output:?}", program.display());
        let stdout = String::from_utf8(output.stdout).unwrap();
        let reported = PathBuf::from(stdout.lines().next().expect("cache dir"));
        assert!(
            reported.starts_with(&owned),
            "compiled into {}, not {}",
            reported.display(),
            owned.display()
        );
        assert!(
            files_under(&reported)
                .iter()
                .any(|file| fs::metadata(file).expect("cache file").len() > 0),
            "no bytecode under {}",
            reported.display()
        );
    }
    assert!(
        !exec_temp.join(NODE_COMPILE_CACHE_DIRECTORY).exists(),
        "a loose compile cache beside the owned read-only cache"
    );

    // The supported release reclaims the whole per-workspace temp dir, compile cache
    // included, and nothing foreign with it.
    release_published_directory(&host, &metadata);
    assert!(
        !exec_temp.exists(),
        "the retired workspace's temp dir, compile cache included, is reclaimed"
    );
    assert!(!image.exists());
    assert_eq!(fs::read(&sibling).unwrap(), b"a sibling's own");
    assert_eq!(fs::read(&caller_sentinel).unwrap(), b"the caller's own");
}

/// Backend scope proof only — not the recovery acceptance, which must preserve the
/// exported image: reclaiming a retained trash image through its RetiredRef disposes of
/// the image and removes the workspace's whole exec-temp tree, the read-only compile
/// cache a real Bun child wrote into it included, and nothing of a sibling's.
#[test]
fn reclaiming_a_retained_image_removes_the_whole_exec_temp_tree() {
    use super::*;
    use crate::capabilities::CapabilityContribution;
    use crate::workspace_environment::{
        EnvironmentMount, NODE_COMPILE_CACHE_DIRECTORY, NODE_COMPILE_CACHE_ENV,
        WORKSPACE_ENVIRONMENT_PATH, write_workspace_environment,
    };

    const COMPILE: &str = r#"const m = require("node:module");
m.enableCompileCache();
const dir = m.getCompileCacheDir();
if (!dir) { console.error("compile cache is not enabled"); process.exit(3); }
console.log(dir);
import("./compile-cache-lib.mjs").then((lib) => {
  if (lib.answer() !== 42) { process.exit(4); }
  if (m.flushCompileCache) { m.flushCompileCache(); }
});
"#;

    let scratch = Scratch::new("compile-cache-reclaim");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let storage = layout(&host.config, &metadata.repo_id).unwrap();
    let (_, exec_temp) = published_directory_paths(&host, &metadata);
    fs::create_dir_all(&exec_temp).unwrap();
    let sibling = storage
        .exec_temp_dir(&WorkspaceName::session("sibling").unwrap())
        .unwrap()
        .join("keep");
    fs::create_dir_all(sibling.parent().unwrap()).unwrap();
    fs::write(&sibling, b"a sibling's own").unwrap();

    write_workspace_environment(
        EnvironmentMount::Served {
            workspace_mount: &image,
            host_home: scratch.path(),
            temp_dir: &exec_temp,
            capability_env: &CapabilityContribution::default(),
        },
        metadata.platform,
        metadata.grants.port_block,
    )
    .unwrap();
    let work = scratch.path().join("work");
    fs::create_dir_all(&work).unwrap();
    fs::write(
        work.join("compile-cache-lib.mjs"),
        "export function answer() { return 42; }\n",
    )
    .unwrap();
    let script = work.join("compile.cjs");
    fs::write(&script, COMPILE).unwrap();
    let path = std::env::var_os("PATH").expect("the gate PATH names the host toolchain");
    let bun = std::env::split_paths(&path)
        .map(|directory| directory.join("bun"))
        .find(|candidate| candidate.is_file())
        .expect("host bun is required for this proof");
    let mut command = std::process::Command::new("/bin/sh");
    command
        .env_clear()
        .args(["-c", ". \"$1\"; exec \"$2\" \"$3\"", "read-only-shell"])
        .arg(image.join(WORKSPACE_ENVIRONMENT_PATH))
        .arg(&bun)
        .arg(&script)
        .current_dir(&work)
        .env("HOME", scratch.path())
        .env("XDG_CACHE_HOME", exec_temp.join("cache"))
        .env(NODE_COMPILE_CACHE_ENV, scratch.path().join("caller-cache"));
    let output = command.output_bounded().unwrap();
    assert!(output.status.success(), "bun: {output:?}");
    let owned = exec_temp.join("cache").join(NODE_COMPILE_CACHE_DIRECTORY);
    assert!(owned.is_dir(), "no read-only compile cache was written");

    // Retire as a recovery retention does: payload, sidecar and companion move to the
    // trash and the temp dir stands, until a reclaim is authorized for the exact ref.
    let trash = published_directory_trash(&host, &metadata);
    fs::create_dir_all(trash.parent().unwrap()).unwrap();
    fs::hard_link(sidecar_path(&image), sidecar_path(&trash)).unwrap();
    fs::hard_link(companion_path(&image), companion_path(&trash)).unwrap();
    host.retire_image(&image, &trash).unwrap();
    assert!(trash.is_dir(), "the retained payload left the trash");
    assert!(
        exec_temp.is_dir(),
        "retirement alone reclaimed the temp dir"
    );

    reclaim_retired_published_directory(&host, &metadata);
    assert!(!trash.exists(), "the retained payload outlived its reclaim");
    assert!(
        !sidecar_path(&trash).exists(),
        "the retained record outlived its reclaim"
    );
    assert!(
        !exec_temp.exists(),
        "the reclaimed workspace's temp dir remains"
    );
    assert_eq!(fs::read(&sibling).unwrap(), b"a sibling's own");
}

/// The name is the scope: when a newer incarnation already owns the name, reclaiming
/// the old incarnation's retained image disposes of that image alone and keeps the
/// name-scoped temp dir the new owner's jobs write into, until the new owner's own
/// release removes it whole.
#[test]
fn reclaiming_an_old_incarnation_keeps_the_names_new_owners_temp_dir() {
    use super::*;
    let scratch = Scratch::new("compile-cache-foreign");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let (_, exec_temp) = published_directory_paths(&host, &metadata);
    fs::create_dir_all(&exec_temp).unwrap();
    fs::write(exec_temp.join("old-scratch"), b"the old incarnation's").unwrap();
    let trash = published_directory_trash(&host, &metadata);
    fs::create_dir_all(trash.parent().unwrap()).unwrap();
    fs::hard_link(sidecar_path(&image), sidecar_path(&trash)).unwrap();
    fs::hard_link(companion_path(&image), companion_path(&trash)).unwrap();
    host.retire_image(&image, &trash).unwrap();

    // The name again, a new incarnation with its own canonical image and temp dir.
    let (host, image, current) = published_directory(&scratch, "selected", 2);
    let (_, current_temp) = published_directory_paths(&host, &current);
    assert_eq!(current_temp, exec_temp, "the temp dir is keyed on the name");
    fs::write(exec_temp.join("new-scratch"), b"the new owner's").unwrap();

    reclaim_retired_published_directory(&host, &metadata);
    assert!(!trash.exists(), "the old payload outlived its reclaim");
    assert!(image.is_dir(), "the new owner's image was reclaimed");
    assert_eq!(
        fs::read(exec_temp.join("new-scratch")).unwrap(),
        b"the new owner's",
        "the old reclaim touched the new owner's temp dir"
    );

    release_published_directory(&host, &current);
    assert!(
        !exec_temp.exists(),
        "the new owner's release left its temp dir"
    );
}

/// The recovery export under a reused name: the old incarnation's image leaves with
/// its bytes exact, the pre-export ephemeral cleanup computes the name Foreign and
/// does not touch the temp dir the new incarnation's jobs write into, and the new
/// canonical image stands. The new owner's own release still removes the temp dir
/// whole.
#[test]
fn recovering_an_old_incarnation_keeps_the_names_new_owners_temp_dir() {
    use super::*;
    use crate::storage::recovery_image::recover;

    fn tree_bytes(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        let mut files = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    pending.push(entry.path());
                } else {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        files
            .into_iter()
            .map(|file| {
                let relative = file
                    .strip_prefix(root)
                    .expect("a listed file is under its root")
                    .to_path_buf();
                let bytes = fs::read(&file)
                    .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
                (relative, bytes)
            })
            .collect()
    }

    let scratch = Scratch::new("recover-foreign");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let storage = published_directory_layout(&host, &metadata);
    let (_, exec_temp) = published_directory_paths(&host, &metadata);
    fs::create_dir_all(&exec_temp).unwrap();
    fs::write(exec_temp.join("old-scratch"), b"the old incarnation's").unwrap();
    let trash = published_directory_trash(&host, &metadata);
    fs::create_dir_all(trash.parent().unwrap()).unwrap();
    fs::hard_link(sidecar_path(&image), sidecar_path(&trash)).unwrap();
    fs::hard_link(companion_path(&image), companion_path(&trash)).unwrap();
    host.retire_image(&image, &trash).unwrap();

    let (host, image, current) = published_directory(&scratch, "selected", 2);
    fs::write(exec_temp.join("new-scratch"), b"the new owner's").unwrap();
    let payload = tree_bytes(&trash);
    assert!(!payload.is_empty(), "the oracle snapshot is empty");
    let sidecar = fs::read(sidecar_path(&trash)).unwrap();
    let companion = fs::read(companion_path(&trash)).unwrap();

    let to = scratch.path().join("recovered");
    let recovered = recover(&storage, &metadata.repo_id, &trash, &to, &host).unwrap();
    let moved = recovered.directory.join(trash.file_name().unwrap());
    assert_eq!(
        tree_bytes(&moved),
        payload,
        "the export changed payload bytes"
    );
    assert_eq!(fs::read(sidecar_path(&moved)).unwrap(), sidecar);
    assert_eq!(fs::read(companion_path(&moved)).unwrap(), companion);
    assert!(image.is_dir(), "the new owner's image went with the export");
    assert_eq!(
        fs::read(exec_temp.join("new-scratch")).unwrap(),
        b"the new owner's"
    );
    assert_eq!(
        fs::read(exec_temp.join("old-scratch")).unwrap(),
        b"the old incarnation's",
        "Foreign cleanup touched the shared name-scoped temp dir"
    );

    release_published_directory(&host, &current);
    assert!(
        !exec_temp.exists(),
        "the new owner's release left its temp dir"
    );
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
        .lock_images(
            std::slice::from_ref(&lock),
            LockMode::Wait,
            &crate::api::held_lock::LockHolding::of(
                crate::api::held_lock::LockOperation::Retire,
                &victim.workspace,
            ),
        )
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
    use crate::storage::recovery_image::MANIFEST_FILE;

    let scratch = Scratch::new("recover-image");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    // Every recovery below names the fixture host as its backend environment owner.
    let recover = |storage: &crate::storage::StorageLayout,
                   repo: &crate::repository::RepoId,
                   image: &Path,
                   to: &Path| {
        crate::storage::recovery_image::recover(storage, repo, image, to, &host)
    };
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
        .lock_images(
            std::slice::from_ref(&lock),
            LockMode::Wait,
            &crate::api::held_lock::LockHolding::of(
                crate::api::held_lock::LockOperation::Recover,
                &metadata.workspace,
            ),
        )
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

/// The pre-export cleanup's refusal, with a real mounted filesystem inside the temp
/// dir (host lane: attaching one is host-controller authority): the recovery refuses
/// before any source transfer, the trash image and its companions keep their exact
/// bytes and authority, the mounted payload is untouched, and detaching the mount
/// lets the same recovery finish — exported bytes exact, temp dir gone whole.
#[cfg(target_os = "macos")]
#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_recovery_refuses_a_mounted_temp_subtree_and_keeps_the_image() {
    use super::*;
    use crate::error::ErrorCode;
    use crate::storage::recovery_image::{MANIFEST_FILE, recover};
    use cowshed_apfs::{ApfsBackend, MacOsApfsBackend, MountAccess, SystemCommandRunner};
    use cowshed_os::namespace::HostNamespace;

    fn tree_bytes(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        let mut files = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    pending.push(entry.path());
                } else {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        files
            .into_iter()
            .map(|file| {
                let relative = file
                    .strip_prefix(root)
                    .expect("a listed file is under its root")
                    .to_path_buf();
                let bytes = fs::read(&file)
                    .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
                (relative, bytes)
            })
            .collect()
    }

    let scratch = Scratch::new("recover-mounted");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let storage = published_directory_layout(&host, &metadata);
    let (_, exec_temp) = published_directory_paths(&host, &metadata);
    fs::create_dir_all(&exec_temp).unwrap();
    fs::write(exec_temp.join("job-scratch"), b"a job's own").unwrap();

    // A real blank image mounted inside the temp dir: removing the tree would empty
    // it through its mountpoint, which is exactly what the cleanup must refuse.
    let image_root = crate::scratch_apfs::ScratchRoot::new("recover-mounted")
        .expect("real APFS backing-image root");
    let namespace = image_root.namespace();
    let mount_image = image_root
        .path()
        .join("mounted")
        .with_extension(cowshed_apfs::image::IMAGE_EXTENSION);
    crate::blank_image::blank_image(namespace, &mount_image);
    let mount = exec_temp.join("mounted");
    fs::create_dir_all(&mount).unwrap();
    let backend = MacOsApfsBackend::new(SystemCommandRunner::new(HostNamespace::Test(namespace)));
    let attachment = backend
        .attach_mounted(&mount_image, &mount, MountAccess::ReadWrite, false)
        .unwrap();
    assert_ne!(
        fs::metadata(&exec_temp).unwrap().dev(),
        fs::metadata(&mount).unwrap().dev(),
        "the mounted subtree is not on another device"
    );
    fs::write(mount.join("payload"), b"mounted bytes").unwrap();

    let trash = published_directory_trash(&host, &metadata);
    fs::create_dir_all(trash.parent().unwrap()).unwrap();
    fs::hard_link(sidecar_path(&image), sidecar_path(&trash)).unwrap();
    fs::hard_link(companion_path(&image), companion_path(&trash)).unwrap();
    host.retire_image(&image, &trash).unwrap();
    let payload = tree_bytes(&trash);
    assert!(!payload.is_empty(), "the oracle snapshot is empty");
    let sidecar = fs::read(sidecar_path(&trash)).unwrap();
    let companion = fs::read(companion_path(&trash)).unwrap();

    let to = scratch.path().join("recovered");
    let error = recover(&storage, &metadata.repo_id, &trash, &to, &host).unwrap_err();
    assert_eq!(error.code, ErrorCode::Integrity);
    assert!(
        error.message.contains("mounted filesystem"),
        "{}",
        error.message
    );
    // Original authority retained: nothing of the source moved or changed, and the
    // mounted payload was not emptied through its mountpoint.
    assert_eq!(
        tree_bytes(&trash),
        payload,
        "the refusal moved source payload bytes"
    );
    assert_eq!(fs::read(sidecar_path(&trash)).unwrap(), sidecar);
    assert_eq!(fs::read(companion_path(&trash)).unwrap(), companion);
    assert_eq!(fs::read(mount.join("payload")).unwrap(), b"mounted bytes");
    assert!(exec_temp.is_dir(), "the refusal removed the temp dir");
    // Destination bookkeeping from the preflight may stand; no source file may.
    let directory = to.join(format!("selected-{}", metadata.workspace_incarnation));
    if directory.exists() {
        assert_eq!(
            tree_bytes(&directory)
                .keys()
                .map(|path| path.as_path())
                .collect::<Vec<_>>(),
            [Path::new(MANIFEST_FILE)],
            "the refusal moved source files into the destination"
        );
    }

    // Detached, the same recovery finishes the move its manifest describes: the
    // cleanup now removes the temp dir whole and the payload arrives exact.
    backend
        .unmount_verified(&attachment, cowshed_apfs::DetachIntent::WhenIdle)
        .unwrap();
    backend
        .detach(&attachment, cowshed_apfs::DetachIntent::WhenIdle)
        .unwrap();
    let recovered = recover(&storage, &metadata.repo_id, &trash, &to, &host).unwrap();
    let moved = recovered.directory.join(trash.file_name().unwrap());
    assert_eq!(
        tree_bytes(&moved),
        payload,
        "the export changed payload bytes"
    );
    assert_eq!(fs::read(sidecar_path(&moved)).unwrap(), sidecar);
    assert_eq!(fs::read(companion_path(&moved)).unwrap(), companion);
    assert!(!trash.exists(), "the payload did not leave the trash");
    assert!(
        !exec_temp.exists(),
        "the recovered workspace's temp dir outlived its export"
    );
}

/// The mounted-root boundary of the same refusal (host lane): the temp dir itself is
/// the mountpoint, so a device walk that only compares children against the root
/// would see one device and start deleting. Admission by mount identity refuses
/// before any entry is removed — image, sidecar and companion keep their bytes and
/// authority, the mounted payload is untouched — and detaching lets the same
/// recovery finish with the temp dir gone whole.
#[cfg(target_os = "macos")]
#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_recovery_refuses_a_mounted_temp_root_and_keeps_the_image() {
    use super::*;
    use crate::error::ErrorCode;
    use crate::storage::recovery_image::{MANIFEST_FILE, recover};
    use cowshed_apfs::{ApfsBackend, MacOsApfsBackend, MountAccess, SystemCommandRunner};
    use cowshed_os::namespace::HostNamespace;

    fn tree_bytes(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        let mut files = Vec::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    pending.push(entry.path());
                } else {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        files
            .into_iter()
            .map(|file| {
                let relative = file
                    .strip_prefix(root)
                    .expect("a listed file is under its root")
                    .to_path_buf();
                let bytes = fs::read(&file)
                    .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
                (relative, bytes)
            })
            .collect()
    }

    let scratch = Scratch::new("recover-mounted-root");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let storage = published_directory_layout(&host, &metadata);
    let (_, exec_temp) = published_directory_paths(&host, &metadata);
    fs::create_dir_all(&exec_temp).unwrap();

    // The temp dir itself mounted: every entry inside shares its device, so only
    // mount identity against the parent outside it can refuse this.
    let image_root = crate::scratch_apfs::ScratchRoot::new("recover-mounted-root")
        .expect("real APFS backing-image root");
    let namespace = image_root.namespace();
    let mount_image = image_root
        .path()
        .join("root-volume")
        .with_extension(cowshed_apfs::image::IMAGE_EXTENSION);
    crate::blank_image::blank_image(namespace, &mount_image);
    let backend = MacOsApfsBackend::new(SystemCommandRunner::new(HostNamespace::Test(namespace)));
    let attachment = backend
        .attach_mounted(&mount_image, &exec_temp, MountAccess::ReadWrite, false)
        .unwrap();
    assert_ne!(
        fs::metadata(exec_temp.parent().unwrap()).unwrap().dev(),
        fs::metadata(&exec_temp).unwrap().dev(),
        "the temp root is not itself a mounted filesystem"
    );
    fs::write(exec_temp.join("payload"), b"mounted bytes").unwrap();

    let trash = published_directory_trash(&host, &metadata);
    fs::create_dir_all(trash.parent().unwrap()).unwrap();
    fs::hard_link(sidecar_path(&image), sidecar_path(&trash)).unwrap();
    fs::hard_link(companion_path(&image), companion_path(&trash)).unwrap();
    host.retire_image(&image, &trash).unwrap();
    let payload = tree_bytes(&trash);
    assert!(!payload.is_empty(), "the oracle snapshot is empty");
    let sidecar = fs::read(sidecar_path(&trash)).unwrap();
    let companion = fs::read(companion_path(&trash)).unwrap();

    let to = scratch.path().join("recovered");
    let error = recover(&storage, &metadata.repo_id, &trash, &to, &host).unwrap_err();
    assert_eq!(error.code, ErrorCode::Integrity);
    assert!(
        error.message.contains("mounted filesystem"),
        "{}",
        error.message
    );
    assert_eq!(
        tree_bytes(&trash),
        payload,
        "the refusal moved source payload bytes"
    );
    assert_eq!(fs::read(sidecar_path(&trash)).unwrap(), sidecar);
    assert_eq!(fs::read(companion_path(&trash)).unwrap(), companion);
    assert_eq!(
        fs::read(exec_temp.join("payload")).unwrap(),
        b"mounted bytes"
    );
    let directory = to.join(format!("selected-{}", metadata.workspace_incarnation));
    if directory.exists() {
        assert_eq!(
            tree_bytes(&directory)
                .keys()
                .map(|path| path.as_path())
                .collect::<Vec<_>>(),
            [Path::new(MANIFEST_FILE)],
            "the refusal moved source files into the destination"
        );
    }

    backend
        .unmount_verified(&attachment, cowshed_apfs::DetachIntent::WhenIdle)
        .unwrap();
    backend
        .detach(&attachment, cowshed_apfs::DetachIntent::WhenIdle)
        .unwrap();
    let recovered = recover(&storage, &metadata.repo_id, &trash, &to, &host).unwrap();
    let moved = recovered.directory.join(trash.file_name().unwrap());
    assert_eq!(
        tree_bytes(&moved),
        payload,
        "the export changed payload bytes"
    );
    assert_eq!(fs::read(sidecar_path(&moved)).unwrap(), sidecar);
    assert_eq!(fs::read(companion_path(&moved)).unwrap(), companion);
    assert!(!trash.exists(), "the payload did not leave the trash");
    assert!(
        !exec_temp.exists(),
        "the recovered workspace's temp dir outlived its export"
    );
}
