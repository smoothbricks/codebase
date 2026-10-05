use super::*;
use crate::build_volume::BuildVolumeRole;
use crate::metadata::WorkspaceName;
use crate::repository::{ProjectPaths, RepoId};
use crate::sandbox::{RunSandboxMode, SandboxConfig, SandboxGrants};
use crate::storage::apfs::ApfsSubstrateConfig;

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "cowshed-build-migrate-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(root.join("checkout")).unwrap();
        fs::create_dir_all(root.join("volume")).unwrap();
        Self(fs::canonicalize(root).unwrap())
    }

    fn checkout(&self) -> PathBuf {
        self.0.join("checkout")
    }
    fn volume(&self) -> PathBuf {
        self.0.join("volume")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn write(checkout: &Path, relative: &str, bytes: &str) {
    let path = checkout.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
}

fn git(checkout: &Path, args: &[&str]) {
    let output = crate::git::git_command_at(checkout)
        .args(args)
        .output_locked()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn paths() -> Vec<BuildStatePath> {
    vec![
        BuildStatePath::new("target", "target").unwrap(),
        BuildStatePath::new(".nx/cache", "nx/cache").unwrap(),
        BuildStatePath::new(".nx/workspace-data", "nx/workspace-data").unwrap(),
        BuildStatePath::new(".codegraph", "codegraph").unwrap(),
    ]
}

fn linked_record() -> BuildVolumeRecord {
    BuildVolumeRecord::new(
        None,
        BuildVolumeRole::Linked {
            checkout: WorkspaceName::main(),
        },
    )
}

fn host(
    root: &Path,
) -> (
    MacOsApfsExecutionHost<SystemCommandRunner>,
    BuildVolumeLayout,
) {
    let store = root.join("store");
    fs::create_dir_all(&store).unwrap();
    let project = ProjectPaths::with_mount_root(
        &store,
        root.join("mnt"),
        &RepoId::parse("example-org/example-app").unwrap(),
    )
    .unwrap();
    let host = MacOsApfsExecutionHost::new(
        SystemCommandRunner,
        ApfsSubstrateConfig::new(&store, root.join("caches"), root.join("checkout")),
    )
    .unwrap();
    (host, BuildVolumeLayout::new(&project).unwrap())
}

fn sandbox(checkout: &Path) -> SandboxConfig {
    SandboxConfig {
        home: PathBuf::from(std::env::var_os("HOME").unwrap()),
        mount_root: checkout.join("other-mounts"),
        workspace_mount: checkout.to_owned(),
        exec_temp_dir: checkout.join("exec-temp"),
        port_block: crate::metadata::PortBlock::new(49_184, 16).unwrap(),
        retained_port_blocks: Vec::new(),
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants::default(),
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        shed_links: Vec::new(),
        git_worktree_repository: None,
        build_volume_mount: None,
        capabilities: Default::default(),
    }
}

#[test]
fn migration_discards_only_contributed_real_state_and_keeps_one_fixed_link_per_path() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    write(&checkout, "source.rs", "tracked source");
    git(&checkout, &["add", "source.rs"]);
    for state in paths() {
        write(
            &checkout,
            &format!("{}/old", state.checkout.as_path().display()),
            "discard me",
        );
    }
    write(&scratch.volume(), "target/warm", "volume bytes stay");
    link::point(&checkout, &scratch.volume()).unwrap();
    let findings = adopt_paths(&checkout, &scratch.volume(), &paths())
        .unwrap()
        .unwrap();
    assert_eq!(findings.len(), 4);
    assert_eq!(findings[0].likely_tool, BuildStateTool::Cargo);
    assert_eq!(findings[1].likely_tool, BuildStateTool::Nx);
    assert_eq!(findings[3].likely_tool, BuildStateTool::Codegraph);
    for state in paths() {
        assert_eq!(
            fs::read_link(checkout.join(state.checkout.as_path())).unwrap(),
            link::relative_target(state.checkout.as_path(), state.volume.as_path())
        );
        assert!(
            !scratch
                .volume()
                .join(state.volume.as_path())
                .join("old")
                .exists(),
            "migration never copies old bytes"
        );
    }
    assert_eq!(
        fs::read_to_string(checkout.join("source.rs")).unwrap(),
        "tracked source"
    );
    assert_eq!(
        fs::read_to_string(checkout.join("target/warm")).unwrap(),
        "volume bytes stay"
    );
    assert!(
        adopt_paths(&checkout, &scratch.volume(), &paths())
            .unwrap()
            .unwrap()
            .is_empty()
    );
    fs::remove_file(checkout.join("target")).unwrap();
    write(&checkout, "target/displaced", "cargo clean rebuilt this");
    let findings = adopt_paths(&checkout, &scratch.volume(), &paths())
        .unwrap()
        .unwrap();
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].path, Path::new("target"));
    assert!(!checkout.join("target/displaced").exists());
    assert_eq!(
        fs::read_to_string(checkout.join("target/warm")).unwrap(),
        "volume bytes stay"
    );
}

#[test]
fn exact_links_need_no_git_query_or_discovery() {
    let scratch = Scratch::new();
    link::point(&scratch.checkout(), &scratch.volume()).unwrap();
    link::link_paths(&scratch.checkout(), &scratch.volume(), &paths()).unwrap();
    assert!(
        adopt_paths(&scratch.checkout(), &scratch.volume(), &paths())
            .unwrap()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn foreign_entries_refuse_before_removing_any_real_directory() {
    for foreign_link in [false, true] {
        let scratch = Scratch::new();
        write(&scratch.checkout(), "target/old", "still here");
        fs::create_dir_all(scratch.checkout().join(".nx")).unwrap();
        if foreign_link {
            std::os::unix::fs::symlink("elsewhere", scratch.checkout().join(".nx/cache")).unwrap();
        } else {
            write(&scratch.checkout(), ".nx/cache", "foreign file");
        }
        assert!(adopt_paths(&scratch.checkout(), &scratch.volume(), &paths()).is_err());
        assert_eq!(
            fs::read_to_string(scratch.checkout().join("target/old")).unwrap(),
            "still here"
        );
    }
}

#[test]
fn tracked_source_in_any_real_path_refuses_before_deleting_all_the_others() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    write(&checkout, "target/untracked", "other build state");
    write(
        &checkout,
        ".nx/cache/tracked source",
        "source, never delete",
    );
    git(&checkout, &["add", "--", ".nx/cache/tracked source"]);
    let refusal = adopt_paths(&checkout, &scratch.volume(), &paths())
        .unwrap()
        .unwrap_err();
    assert_eq!(refusal.path, Path::new(".nx/cache"));
    assert_eq!(
        refusal.tracked_files,
        [Path::new(".nx/cache/tracked source")]
    );
    assert_eq!(
        fs::read_to_string(checkout.join("target/untracked")).unwrap(),
        "other build state"
    );
    assert_eq!(
        fs::read_to_string(checkout.join(".nx/cache/tracked source")).unwrap(),
        "source, never delete"
    );
}

#[test]
fn overlapping_paths_and_symlinked_parents_refuse_without_deletion() {
    let scratch = Scratch::new();
    write(&scratch.checkout(), "target/nested/old", "still here");
    let overlap = vec![
        BuildStatePath::new("target", "target").unwrap(),
        BuildStatePath::new("target/nested", "nested").unwrap(),
    ];
    assert!(adopt_paths(&scratch.checkout(), &scratch.volume(), &overlap).is_err());
    assert_eq!(
        fs::read_to_string(scratch.checkout().join("target/nested/old")).unwrap(),
        "still here"
    );
    std::os::unix::fs::symlink(scratch.volume(), scratch.checkout().join(".nx")).unwrap();
    assert!(adopt_paths(&scratch.checkout(), &scratch.volume(), &paths()).is_err());
    assert_eq!(
        fs::read_to_string(scratch.checkout().join("target/nested/old")).unwrap(),
        "still here"
    );
}

#[test]
fn configured_cargo_target_containing_tracked_source_refuses_before_mint_or_delete() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    write(
        &checkout,
        "Cargo.toml",
        "[package]\nname = \"guard_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    );
    write(&checkout, "src/lib.rs", "pub fn value() -> u8 { 1 }\n");
    write(
        &checkout,
        ".cargo/config.toml",
        "[build]\ntarget-dir = \"source-output\"\n",
    );
    write(
        &checkout,
        "source-output/source.rs",
        "tracked, not a disposable artifact",
    );
    git(
        &checkout,
        &[
            "add",
            "Cargo.toml",
            "src/lib.rs",
            ".cargo/config.toml",
            "source-output/source.rs",
        ],
    );
    let discovery = sandbox(&checkout)
        .with_detection_context(&checkout, |context| {
            crate::capabilities::discover_build_state(context, &mut crate::capabilities::HostCargo)
        })
        .unwrap();
    assert!(discovery.findings.is_empty(), "{:?}", discovery.findings);
    assert_eq!(
        discovery.paths,
        [BuildStatePath::new("source-output", "source-output").unwrap()]
    );
    let (host, layout) = host(&scratch.0);
    let refusal = first_touch(
        &host,
        &layout,
        FirstTouch {
            checkout: &checkout,
            paths: &discovery.paths,
            fingerprint: "fixture".to_owned(),
            capacity: ImageCapacity::from_gibibytes(1),
            record: linked_record(),
        },
    )
    .unwrap()
    .unwrap_err();
    assert_eq!(refusal.path, Path::new("source-output"));
    assert_eq!(
        refusal.tracked_files,
        [Path::new("source-output/source.rs")]
    );
    assert!(link::linked(&checkout).unwrap().is_none());
    assert!(
        !layout.images().exists(),
        "tracked source refuses before creating an image"
    );
    assert_eq!(
        fs::read_to_string(checkout.join("source-output/source.rs")).unwrap(),
        "tracked, not a disposable artifact"
    );
}

#[test]
fn real_apfs_first_touch_resumes_the_early_pointer_and_publishes_record_last() {
    let root = crate::scratch_apfs::ScratchRoot::new("build-migration").unwrap();
    let checkout = root.path().join("checkout");
    fs::create_dir_all(&checkout).unwrap();
    git(&checkout, &["init", "--quiet"]);
    for state in paths() {
        write(
            &checkout,
            &format!("{}/old", state.checkout.as_path().display()),
            "discard me",
        );
    }
    write(&checkout, "source.rs", "source stays");
    git(&checkout, &["add", "source.rs"]);
    let (host, layout) = host(root.path());
    // The mint clones the store's blank template: the run's, seeded here.
    crate::blank_image::blank_image(&crate::storage::apfs::native::blank_template_path(
        &root.path().join("store"),
        crate::blank_image::CAPACITY,
    ));
    let id = BuildVolumeId::mint();
    let mount = host
        .create_build_volume(&layout, &id, ImageCapacity::from_gibibytes(1))
        .unwrap();
    link::point(&checkout, &mount).unwrap();
    assert!(
        !layout.record(&id).exists(),
        "creation must not publish a sidecar"
    );
    fs::write(mount.join(super::super::STATE_FILE), "broken state").unwrap();
    assert!(
        first_touch(
            &host,
            &layout,
            FirstTouch {
                checkout: &checkout,
                paths: &paths(),
                fingerprint: "fixture".to_owned(),
                capacity: ImageCapacity::from_gibibytes(1),
                record: linked_record(),
            }
        )
        .is_err()
    );
    assert!(
        !layout.record(&id).exists(),
        "failure before linking leaves the sidecar absent"
    );
    assert_eq!(
        fs::read_to_string(checkout.join("target/old")).unwrap(),
        "discard me",
        "state is validated before deletion"
    );
    fs::remove_file(mount.join(super::super::STATE_FILE)).unwrap();
    for _ in 0..2 {
        let (linked, _) = first_touch(
            &host,
            &layout,
            FirstTouch {
                checkout: &checkout,
                paths: &paths(),
                fingerprint: "fixture".to_owned(),
                capacity: ImageCapacity::from_gibibytes(1),
                record: linked_record(),
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            linked, id,
            "a retry keeps the early pointer, never mints another image"
        );
        assert_eq!(layout.read_record(&id).unwrap().role, linked_record().role);
        let state = BuildVolumeState::read(&mount).unwrap();
        assert_eq!(state.paths, paths());
        assert_eq!(state.fingerprint.as_deref(), Some("fixture"));
        for path in paths() {
            assert!(
                fs::symlink_metadata(checkout.join(path.checkout.as_path()))
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert!(!mount.join(path.volume.as_path()).join("old").exists());
        }
    }
    assert_eq!(layout.list().unwrap().as_slice(), std::slice::from_ref(&id));
    assert_eq!(
        fs::read_to_string(checkout.join("source.rs")).unwrap(),
        "source stays"
    );
    assert_eq!(
        host.release_build_volume(&layout, &id).unwrap(),
        crate::storage::apfs::native::BuildVolumeRelease::Deleted
    );
}
