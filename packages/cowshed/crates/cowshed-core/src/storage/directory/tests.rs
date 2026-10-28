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

    unbind(
        &crate::census::this_test(),
        &image,
        &mount,
        DetachIntent::Release,
    )
    .unwrap();
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
        unbind(
            &crate::census::this_test(),
            &image,
            &elsewhere,
            DetachIntent::Release
        )
        .is_err(),
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
    unbind(
        &crate::census::this_test(),
        &image,
        &tree,
        DetachIntent::Release,
    )
    .unwrap();
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
    let (detached, _) = host
        .workspace_snapshot(&metadata.repo_id, &metadata.workspace)
        .unwrap()
        .unwrap();
    assert_eq!(detached.mount_state, MountState::Detached);
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
    unbind(
        &crate::census::this_test(),
        &image,
        &mount,
        DetachIntent::Release,
    )
    .unwrap();
}

#[test]
fn directory_source_release_keeps_canonical_authority_on_cleanup_refusal() {
    use super::*;
    let scratch = Scratch::new("direct-source-release");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let workspace = metadata_workspace_ref(&metadata).unwrap();
    let retired = RetiredRef::new(workspace, Revision::new(1));
    let storage = layout(&host.config, &metadata.repo_id).unwrap();
    let mountpoint = storage.project().mount_root.join("selected");
    fs::create_dir_all(&mountpoint).unwrap();
    let foreign = mountpoint.join("late-write");
    fs::write(&foreign, b"preserve late writes").unwrap();
    assert!(host.release_workspace(&host.config, &retired).is_err());
    assert!(image.exists());
    assert!(sidecar_path(&image).exists());
    assert!(companion_path(&image).exists());
    assert_eq!(fs::read(&foreign).unwrap(), b"preserve late writes");
    fs::remove_file(&foreign).unwrap();
    host.release_workspace(&host.config, &retired).unwrap();
    assert!(!image.exists());
    assert!(!sidecar_path(&image).exists());
    assert!(!companion_path(&image).exists());
    assert!(!storage.project().sessions.join(".trash").exists());
}

#[test]
fn directory_gc_observes_retired_owners_once_per_phase_not_per_candidate() {
    use super::*;
    use crate::storage::apfs::retired_trash_observations;
    use crate::storage::lifecycle::StorageGcReason;

    for candidates in [1_usize, 8, 64, 128] {
        let scratch = Scratch::new("gc-retired-owner-count");
        let (host, _, metadata) = published_directory(&scratch, "kept", 1);
        let storage = layout(&host.config, &metadata.repo_id).unwrap();
        let sessions = &storage.project().sessions;
        fs::create_dir_all(sessions.join(".trash")).unwrap();
        let mut retired = Vec::new();
        for index in 0..candidates {
            let mut record = metadata.clone();
            record.workspace = WorkspaceName::session(format!("gone-{index}")).unwrap();
            record.workspace_incarnation =
                WorkspaceIncarnation::new(format!("{:032x}", index + 2)).unwrap();
            let image = crate::storage::apfs::retired_image_below(
                sessions,
                &record.workspace,
                &record.workspace_incarnation,
            );
            record.write_for_image(&image).unwrap();
            retired.push(image);
        }
        let mut record = metadata.clone();
        record.workspace = WorkspaceName::session("retained").unwrap();
        let retained = crate::storage::apfs::retired_image_below(
            sessions,
            &record.workspace,
            &record.workspace_incarnation,
        );
        fs::create_dir(&retained).unwrap();
        fs::write(retained.join("only-copy"), b"retained recovery evidence").unwrap();
        record.write_for_image(&retained).unwrap();

        let before = retired_trash_observations();
        let plan = host
            .preview_gc(&host.config, &metadata.repo_id, StorageGcOptions::default())
            .unwrap();
        assert_eq!(
            plan.candidates()
                .iter()
                .filter(|candidate| candidate.reason() == StorageGcReason::RetiredWorkspace)
                .count(),
            candidates + 1
        );
        assert_eq!(
            retired_trash_observations() - before,
            1,
            "one read-only preview observation"
        );
        let report = host.execute_gc(&host.config, plan).unwrap();
        assert_eq!(report.reclaimed, candidates + 1);
        assert!(report.deferred.is_empty(), "{:?}", report.deferred);
        assert_eq!(
            retired_trash_observations() - before,
            2,
            "one fresh locked revalidation, no per-candidate census; candidates={candidates}"
        );
        for image in retired {
            assert!(!sidecar_path(&image).exists());
        }
        assert!(
            !retained.exists(),
            "the payload an earlier build kept as recovery is released too"
        );
        assert!(!sidecar_path(&retained).exists());
        cowshed_os::eprintln!(
            "retired-owner observations: candidates={candidates} preview=1 execution=1"
        );
    }
}

#[test]
fn retired_owner_completion_uses_residual_authority_even_when_cleanup_failed() {
    use super::*;
    use crate::storage::apfs::RetiredNameOwners;

    for (payload, record, failed) in [
        (true, true, true),
        (false, true, true),
        (true, false, true),
        (false, false, true),
        (false, false, false),
    ] {
        let scratch = Scratch::new("retired-owner-partial-cleanup");
        let (host, _, metadata) = published_directory(&scratch, "selected", 2);
        let storage = layout(&host.config, &metadata.repo_id).unwrap();
        let trash = crate::storage::apfs::retired_image_below(
            &storage.project().sessions,
            &metadata.workspace,
            &metadata.workspace_incarnation,
        );
        fs::create_dir_all(trash.parent().unwrap()).unwrap();
        let older = WorkspaceIncarnation::new(format!("{:032x}", 1)).unwrap();
        let older_image = crate::storage::apfs::retired_image_below(
            &storage.project().sessions,
            &metadata.workspace,
            &older,
        );
        let mut owners = RetiredNameOwners::from_images(&[older_image, trash.clone()]);
        if payload {
            fs::create_dir(&trash).unwrap();
        }
        if record {
            metadata.write_for_image(&trash).unwrap();
        }
        let retired = RetiredRef::new(metadata_workspace_ref(&metadata).unwrap(), Revision::new(1));
        let cleanup = if failed {
            Err(ApfsStorageError::Host(
                "cleanup's original failure".to_owned(),
            ))
        } else {
            Ok(())
        };
        let result = owners.after_reclaim(&retired, &trash, |kind| kind.is_dir(), cleanup);
        if failed {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("cleanup's original failure")
            );
        } else {
            result.unwrap();
        }
        assert_eq!(
            owners.get(&metadata.workspace),
            Some(if payload || record {
                &metadata.workspace_incarnation
            } else {
                &older
            }),
            "payload={payload} record={record} failed={failed}",
        );
    }
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
        None,
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
        None,
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

#[cfg(target_os = "macos")]
#[test]
fn directory_fixture_retirement_moves_the_inode_once_for_every_payload_size() {
    use crate::scratch_directory::{retire_directory_tree, retirement_rename_attempts};
    use std::os::unix::fs::MetadataExt as _;

    let scratch =
        crate::temp_root::TempRoot::new_in(&std::env::temp_dir(), "directory-retirement-rename");
    for entries in [0_usize, 1, 32, 128] {
        let source = scratch.join(format!("source-{entries}"));
        let destination = scratch.join(format!("retired-{entries}"));
        fs::create_dir(&source).unwrap();
        for index in 0..entries {
            fs::write(source.join(format!("entry-{index}")), index.to_le_bytes()).unwrap();
        }
        let inode = fs::metadata(&source).unwrap().ino();
        let before = retirement_rename_attempts();
        retire_directory_tree(&source, &destination).unwrap();
        assert_eq!(retirement_rename_attempts() - before, 1);
        assert!(!source.exists());
        assert_eq!(fs::metadata(&destination).unwrap().ino(), inode);
        for index in 0..entries {
            assert_eq!(
                fs::read(destination.join(format!("entry-{index}"))).unwrap(),
                index.to_le_bytes(),
            );
        }
        cowshed_os::eprintln!(
            "directory-retirement counts: entries={entries} rename_attempts=1 inode_preserved=true"
        );
    }
    let source = scratch.join("source-stays");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("kept"), b"kept").unwrap();
    let missing_destination = scratch.join("missing-parent/retired");
    assert_eq!(
        retire_directory_tree(&source, &missing_destination)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::NotFound,
    );
    assert_eq!(fs::read(source.join("kept")).unwrap(), b"kept");
    retire_directory_tree(&scratch.join("already-gone"), &missing_destination).unwrap();
}
