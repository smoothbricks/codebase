use super::*;
use crate::build_volume::{BuildVolumeRecord, BuildVolumeRole, Registrant};
use crate::metadata::WorkspaceName;
use crate::repository::{ProjectPaths, RepoId};
use crate::sandbox::{RunSandboxMode, SandboxConfig, SandboxGrants};
use crate::storage::apfs::ApfsSubstrateConfig;
use crate::storage::directory::DirectoryExecutionHost;
use cowshed_os::namespace::{HostNamespace, TestNamespace};
use cowshed_os::test_backstop::CommandBackstop as _;
use std::path::PathBuf;

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
        .output_bounded()
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
    ]
}

fn linked_registration() -> Registration {
    Registration::new(
        BuildVolumeRecord::new(
            None,
            BuildVolumeRole::Linked {
                checkout: WorkspaceName::main(),
            },
        ),
        Registrant::Workspace {
            incarnation: crate::metadata::WorkspaceIncarnation::new("0".repeat(32)).unwrap(),
        },
    )
}

fn host(root: &Path) -> (DirectoryExecutionHost, BuildVolumeLayout) {
    let store = root.join("store");
    fs::create_dir_all(&store).unwrap();
    let project = ProjectPaths::with_mount_root(
        &store,
        root.join("mnt"),
        &RepoId::parse("example-org/example-app").unwrap(),
    )
    .unwrap();
    let host = DirectoryExecutionHost::new(ApfsSubstrateConfig::new(
        HostNamespace::Test(TestNamespace::mint().expect("a test namespace")),
        &store,
        root.join("checkout"),
    ))
    .unwrap();
    (host, BuildVolumeLayout::new(&project).unwrap())
}

fn sandbox(checkout: &Path) -> SandboxConfig {
    SandboxConfig {
        home: PathBuf::from(std::env::var_os("HOME").unwrap()),
        mount_root: checkout.join("other-mounts"),
        workspace_mount: checkout.to_owned(),
        exec_temp_dir: checkout.join("exec-temp"),
        store_root: checkout.join("store"),
        port_block: crate::metadata::PortBlock::new(49_184, 16).unwrap(),
        retained_port_blocks: Vec::new(),
        cpu_pool_directory: None,
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants::default(),
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        shed_links: Vec::new(),
        git_worktree_repository: None,
        main_mount: None,
        build_volume_mount: None,
        repository_caches: Vec::new(),
        program_directories: Vec::new(),
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
    let findings = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &paths(),
        &[],
    )
    .unwrap()
    .unwrap()
    .discarded;
    assert_eq!(findings.len(), 3);
    assert_eq!(findings[0].tool, BuildStateTool::Cargo);
    assert_eq!(findings[1].tool, BuildStateTool::Nx);
    assert!(
        findings
            .iter()
            .all(|finding| finding.origin == DiscardOrigin::Predated),
        "a volume that held none of the paths linked none of them: {findings:?}"
    );
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
        adopt_paths(
            &crate::census::this_test(),
            &checkout,
            &scratch.volume(),
            &paths(),
            &paths()
        )
        .unwrap()
        .unwrap()
            == Adopted::default()
    );
    fs::remove_file(checkout.join("target")).unwrap();
    write(&checkout, "target/displaced", "cargo clean rebuilt this");
    let findings = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &paths(),
        &paths(),
    )
    .unwrap()
    .unwrap()
    .discarded;
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].path, Path::new("target"));
    assert_eq!(
        (findings[0].tool, findings[0].origin),
        (BuildStateTool::Cargo, DiscardOrigin::Displaced)
    );
    assert!(
        findings[0]
            .to_string()
            .contains("cargo clean or another Cargo command may have displaced it"),
        "{}",
        findings[0]
    );
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
        adopt_paths(
            &crate::census::this_test(),
            &scratch.checkout(),
            &scratch.volume(),
            &paths(),
            &paths()
        )
        .unwrap()
        .unwrap()
            == Adopted::default()
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
        assert!(
            adopt_paths(
                &crate::census::this_test(),
                &scratch.checkout(),
                &scratch.volume(),
                &paths(),
                &paths()
            )
            .is_err()
        );
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
    let refusal = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &paths(),
        &paths(),
    )
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
    assert!(
        adopt_paths(
            &crate::census::this_test(),
            &scratch.checkout(),
            &scratch.volume(),
            &overlap,
            &overlap
        )
        .is_err()
    );
    assert_eq!(
        fs::read_to_string(scratch.checkout().join("target/nested/old")).unwrap(),
        "still here"
    );
    std::os::unix::fs::symlink(scratch.volume(), scratch.checkout().join(".nx")).unwrap();
    assert!(
        adopt_paths(
            &crate::census::this_test(),
            &scratch.checkout(),
            &scratch.volume(),
            &paths(),
            &paths()
        )
        .is_err()
    );
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
            crate::capabilities::discover_build_state(
                context,
                &mut crate::capabilities::HostCargo::default(),
            )
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
            mbx: None,
            capacity: ImageCapacity::from_gibibytes(1),
            beside: MintedBeside::Nothing,
            registration: linked_registration(),
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
fn directory_first_touch_resumes_the_early_pointer_and_publishes_record_last() {
    let root = crate::scratch_directory::ScratchRoot::new("build-migration").unwrap();
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
    let id = BuildVolumeId::mint();
    let (mount, creating) = host
        .create_build_volume(
            &layout,
            &id,
            ImageCapacity::from_gibibytes(1),
            &linked_registration(),
        )
        .unwrap();
    link::point(&checkout, &mount).unwrap();
    drop(creating);
    let registered = layout.read_record(&id).unwrap();
    assert_eq!(&registered.role, linked_registration().role());
    assert!(
        !registered.is_published(),
        "creation registers the owner before the image and publishes nothing"
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
                mbx: None,
                capacity: ImageCapacity::from_gibibytes(1),
                beside: MintedBeside::Nothing,
                registration: linked_registration(),
            }
        )
        .is_err()
    );
    assert!(
        !layout.read_record(&id).unwrap().is_published(),
        "failure before linking leaves the record registered, never published"
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
                mbx: None,
                capacity: ImageCapacity::from_gibibytes(1),
                beside: MintedBeside::Nothing,
                registration: linked_registration(),
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            linked, id,
            "a retry keeps the early pointer, never mints another image"
        );
        let record = layout.read_record(&id).unwrap();
        assert_eq!(record.role, linked_registration().role().clone());
        assert!(record.is_published());
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
    let release = host.release_build_volume(&layout, &id).unwrap();
    assert!(
        matches!(
            release,
            crate::storage::apfs::native::BuildVolumeRelease::Deleted { .. }
        ),
        "{release:?}"
    );
}

fn target_only() -> Vec<BuildStatePath> {
    vec![BuildStatePath::new("target", "target").unwrap()]
}

fn status(checkout: &Path) -> String {
    let output = crate::git::git_command_at(checkout)
        .args(["status", "--porcelain", "--untracked-files=all"])
        .output_bounded()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

/// The background delete is observed, never assumed: a native watch on the discard
/// directory is armed before the first pending read, and each directory event -- a
/// discard leaf leaving it -- earns exactly one re-read, until none are pending.
fn discards_drain(checkout: &Path) {
    let directory = checkout.join(discard::DISCARD_DIRECTORY);
    let Some(watch) = DiscardWatch::arm(&directory) else {
        assert!(
            discard::pending(checkout).unwrap().is_empty(),
            "discards are pending under {} with no directory to watch",
            directory.display()
        );
        return;
    };
    while !discard::pending(checkout).unwrap().is_empty() {
        watch.wait();
    }
}

/// One blocking native watch on a checkout's discard directory: a leaf's removal is a
/// directory write there. Armed before the first read it waits on, so no removal that
/// the read could miss is lost between the read and the wait.
#[cfg(target_os = "macos")]
struct DiscardWatch {
    queue: std::os::fd::OwnedFd,
    _directory: fs::File,
}

#[cfg(target_os = "macos")]
impl DiscardWatch {
    fn arm(directory: &Path) -> Option<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        let directory = match fs::File::open(directory) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => panic!("open the discard directory: {error}"),
        };
        // SAFETY: `kqueue` takes no arguments; a negative answer is an error.
        let queue = unsafe { libc::kqueue() };
        assert!(queue >= 0, "kqueue: {}", io::Error::last_os_error());
        // SAFETY: `queue` is a fresh descriptor only this value owns.
        let queue = unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) };
        let change = libc::kevent {
            ident: usize::try_from(directory.as_raw_fd()).expect("a descriptor"),
            filter: libc::EVFILT_VNODE,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: libc::NOTE_WRITE,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: one valid change, no events requested back.
        let added = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        assert!(added == 0, "kevent: {}", io::Error::last_os_error());
        Some(Self {
            queue,
            _directory: directory,
        })
    }

    fn wait(&self) {
        use std::os::fd::AsRawFd as _;
        let mut event = std::mem::MaybeUninit::<libc::kevent>::zeroed();
        loop {
            // SAFETY: room for one event; no timeout.
            let ready = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    event.as_mut_ptr(),
                    1,
                    std::ptr::null(),
                )
            };
            if ready == 1 {
                return;
            }
            let error = io::Error::last_os_error();
            assert_eq!(error.kind(), io::ErrorKind::Interrupted, "kevent: {error}");
        }
    }
}

#[cfg(target_os = "linux")]
struct DiscardWatch {
    inotify: std::os::fd::OwnedFd,
}

#[cfg(target_os = "linux")]
impl DiscardWatch {
    fn arm(directory: &Path) -> Option<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        use std::os::unix::ffi::OsStrExt as _;
        if !directory.exists() {
            return None;
        }
        // SAFETY: inotify_init1 takes flags only; a negative answer is an error.
        let raw = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
        assert!(raw >= 0, "inotify_init1: {}", io::Error::last_os_error());
        // SAFETY: `raw` is a fresh descriptor only this value owns.
        let inotify = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) };
        let path =
            std::ffi::CString::new(directory.as_os_str().as_bytes()).expect("a directory path");
        // SAFETY: `path` is NUL-terminated and outlives the call.
        let watch = unsafe {
            libc::inotify_add_watch(
                inotify.as_raw_fd(),
                path.as_ptr(),
                libc::IN_DELETE | libc::IN_MOVED_FROM | libc::IN_CREATE | libc::IN_MOVED_TO,
            )
        };
        assert!(
            watch >= 0,
            "inotify_add_watch {}: {}",
            directory.display(),
            io::Error::last_os_error()
        );
        Some(Self { inotify })
    }

    fn wait(&self) {
        use std::os::fd::AsRawFd as _;
        let mut buffer = [0_u8; 4096];
        loop {
            // SAFETY: `buffer` is writable for its full length; the descriptor is owned
            // and blocking, so the read returns only with events or an error.
            let read = unsafe {
                libc::read(
                    self.inotify.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            if read > 0 {
                return;
            }
            let error = io::Error::last_os_error();
            assert_eq!(
                error.kind(),
                io::ErrorKind::Interrupted,
                "inotify read: {error}"
            );
        }
    }
}

/// A refresh that died after moving old build state aside -- a crash, or a setup that exited
/// mid-delete -- left it in `.cowshed/discard`, invisible to Git; the next refresh deletes it.
#[test]
fn a_discard_a_crash_left_pending_is_finished_by_the_next_refresh() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    write(&checkout, ".gitignore", "target/\n");
    write(&checkout, "target/debug/unit", "old build state");
    git(&checkout, &["add", ".gitignore"]);
    link::point(&checkout, &scratch.volume()).unwrap();
    // The crash: excluded and moved aside, then nothing more.
    link::exclude_links(&checkout, &target_only()).unwrap();
    let moved = discard::move_aside(&checkout, Path::new("target")).unwrap();
    assert!(moved.starts_with(checkout.join(discard::DISCARD_DIRECTORY)));
    assert!(fs::read(moved.join("debug/unit")).is_ok());
    assert_eq!(
        status(&checkout),
        "A  .gitignore\n",
        "a pending discard is not source"
    );

    assert!(
        adopt_paths(
            &crate::census::this_test(),
            &checkout,
            &scratch.volume(),
            &target_only(),
            &target_only()
        )
        .unwrap()
        .unwrap()
            == Adopted::default(),
        "nothing real was at the path, so nothing new is displaced"
    );
    assert!(
        fs::symlink_metadata(checkout.join("target"))
            .unwrap()
            .is_symlink()
    );
    discards_drain(&checkout);
    assert_eq!(status(&checkout), "A  .gitignore\n");
}

/// A refresh returns once the link is in place; the delete of what it moved aside runs behind
/// it. A delete that cannot proceed stands in for a target directory of any size: a `gc` of the
/// checkout is deleting what an earlier refresh left, and holds there until the test lets it
/// go. The refresh returns all the same, with its own discard pending. Once that `gc` is done,
/// the refresh's own background delete takes what the refresh moved aside, which the `gc` never
/// listed.
#[test]
fn a_refresh_returns_while_the_delete_of_what_it_moved_aside_waits() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    write(&checkout, "target/debug/unit", "old build state");
    write(
        &checkout,
        "earlier/unit",
        "what an earlier refresh moved aside",
    );
    link::point(&checkout, &scratch.volume()).unwrap();
    let earlier = discard::move_aside(&checkout, Path::new("earlier")).unwrap();

    let (holding, held) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let collecting = std::thread::spawn({
        let checkout = checkout.clone();
        move || {
            discard::finish(&checkout, |discard| {
                holding.send(discard.to_owned()).unwrap();
                released.recv().unwrap();
            })
        }
    });
    assert_eq!(
        held.recv().unwrap(),
        earlier,
        "gc holds the earlier discard"
    );

    let displaced = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &target_only(),
        &target_only(),
    )
    .unwrap()
    .unwrap()
    .discarded;
    assert_eq!(displaced.len(), 1);
    assert!(
        fs::symlink_metadata(checkout.join("target"))
            .unwrap()
            .is_symlink()
    );
    let pending = discard::pending(&checkout).unwrap();
    assert_eq!(
        pending.len(),
        2,
        "the refresh returned while every delete of the checkout waited: {pending:?}"
    );

    release.send(()).unwrap();
    collecting.join().unwrap().unwrap();
    discards_drain(&checkout);
    assert_eq!(status(&checkout), "", "{}", status(&checkout));
}

/// Declared build state goes onto the volume like any build state: a real directory at a declared
/// path -- here a nested Git checkout of an upstream project and its build tree -- leaves its path
/// in one rename, never copied, and the path becomes its fixed link into the volume's `declared/`
/// namespace. Cowshed cannot rebuild what a declared directory holds, so the directory is kept
/// whole, at the location the refresh names, and nothing deletes it. What is written through the
/// link afterwards lives on the volume.
#[test]
fn a_declared_checkout_is_kept_whole_and_its_path_linked() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    // Every cowshed checkout excludes its own namespace (02_workspaces.md).
    fs::write(checkout.join(".git/info/exclude"), "/.cowshed/\n").unwrap();
    write(&checkout, "vendor/upstream/build/build.ninja", "stale tree");
    git(&checkout.join("vendor/upstream"), &["init", "--quiet"]);
    link::point(&checkout, &scratch.volume()).unwrap();
    let declared = vec![BuildStatePath::declared("vendor/upstream").unwrap()];
    let adopted = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &declared,
        &declared,
    )
    .unwrap()
    .unwrap();
    assert_eq!(adopted.discarded, []);
    let [kept] = adopted.kept.as_slice() else {
        panic!("one directory is kept: {adopted:?}");
    };
    assert_eq!(
        (kept.path.as_path(), kept.origin),
        (Path::new("vendor/upstream"), DiscardOrigin::Predated)
    );
    assert!(
        kept.kept.starts_with(checkout.join(kept::KEPT_DIRECTORY)),
        "{}",
        kept.kept.display()
    );
    assert_eq!(
        fs::read_to_string(kept.kept.join("build/build.ninja")).unwrap(),
        "stale tree"
    );
    assert!(
        kept.kept.join(".git").is_dir(),
        "the nested checkout moves whole"
    );
    let report = kept.to_string();
    assert!(
        report.contains(&kept.kept.display().to_string()) && report.contains("never deletes"),
        "{report}"
    );
    assert_eq!(
        fs::read_link(checkout.join("vendor/upstream")).unwrap(),
        Path::new("../.cowshed/build/declared/vendor/upstream")
    );
    assert!(
        fs::read_dir(scratch.volume().join("declared/vendor/upstream"))
            .unwrap()
            .next()
            .is_none(),
        "migration never copies the old checkout"
    );
    write(&checkout, "vendor/upstream/build/build.ninja", "rebuilt");
    assert_eq!(
        fs::read_to_string(
            scratch
                .volume()
                .join("declared/vendor/upstream/build/build.ninja")
        )
        .unwrap(),
        "rebuilt"
    );
    assert!(
        adopt_paths(
            &crate::census::this_test(),
            &checkout,
            &scratch.volume(),
            &declared,
            &declared
        )
        .unwrap()
        .unwrap()
            == Adopted::default(),
        "the exact link is left alone"
    );
    discards_drain(&checkout);
    assert_eq!(
        kept::list(&checkout).unwrap().len(),
        1,
        "nothing deletes a kept directory"
    );
    assert_eq!(status(&checkout), "", "the link is excluded, never source");
}

/// A reconstruct script that wants to fill the volume on its first run makes the empty
/// directory and lets a refresh link it (16_build_volumes.md, "Declared build state"). An empty
/// directory holds nothing to lose, so the refresh removes it and links the path, keeping
/// nothing.
#[test]
fn an_empty_declared_directory_is_linked_in_its_place() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    fs::create_dir_all(checkout.join("vendor/upstream")).unwrap();
    link::point(&checkout, &scratch.volume()).unwrap();
    let declared = vec![BuildStatePath::declared("vendor/upstream").unwrap()];
    let adopted = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &declared,
        &declared,
    )
    .unwrap()
    .unwrap();
    assert_eq!(adopted.kept, []);
    assert_eq!(
        adopted.discarded,
        [DiscardedBuildStateFinding {
            path: PathBuf::from("vendor/upstream"),
            tool: BuildStateTool::Declared,
            origin: DiscardOrigin::Predated,
        }]
    );
    assert!(
        adopted.discarded[0]
            .to_string()
            .contains("nothing was lost"),
        "{}",
        adopted.discarded[0]
    );
    assert_eq!(
        fs::read_link(checkout.join("vendor/upstream")).unwrap(),
        Path::new("../.cowshed/build/declared/vendor/upstream")
    );
    assert_eq!(kept::list(&checkout).unwrap(), []);
    discards_drain(&checkout);
}

/// Existence carries meaning: a tool asks whether a declared directory is there (a pin worktree
/// or none). So a refresh never makes an absent declared path -- no link, no volume directory,
/// no parent -- while every capability's path is linked at once. Once a tool makes the
/// directory, the next refresh adopts it.
#[test]
fn an_absent_declared_path_stays_absent_until_a_tool_makes_it() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    link::point(&checkout, &scratch.volume()).unwrap();
    let paths = vec![
        BuildStatePath::new("target", "target").unwrap(),
        BuildStatePath::declared("vendor/upstream").unwrap(),
    ];
    for _ in 0..2 {
        assert!(
            adopt_paths(
                &crate::census::this_test(),
                &checkout,
                &scratch.volume(),
                &paths,
                &paths
            )
            .unwrap()
            .unwrap()
                == Adopted::default()
        );
        assert!(
            fs::symlink_metadata(checkout.join("target"))
                .unwrap()
                .is_symlink()
        );
        assert!(!checkout.join("vendor").exists(), "nothing made its parent");
        assert!(!scratch.volume().join("declared").exists());
    }
    write(
        &checkout,
        "vendor/upstream/build/build.ninja",
        "made by a tool",
    );
    let adopted = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &paths,
        &paths,
    )
    .unwrap()
    .unwrap();
    assert_eq!(adopted.discarded, []);
    assert_eq!(adopted.kept.len(), 1);
    assert_eq!(
        adopted.kept[0].origin,
        DiscardOrigin::Predated,
        "a declared path is held before its first link"
    );
    assert_eq!(
        fs::read_to_string(adopted.kept[0].kept.join("build/build.ninja")).unwrap(),
        "made by a tool"
    );
    assert_eq!(
        fs::read_link(checkout.join("vendor/upstream")).unwrap(),
        Path::new("../.cowshed/build/declared/vendor/upstream")
    );
    assert!(scratch.volume().join("declared/vendor/upstream").is_dir());
}

/// The `.bun-pin` incident, in a downstream repository's shed: a tool cloned a patch worktree
/// into a declared path before its link existed, an agent edited a file inside it, and the next
/// `cowshed exec`'s refresh linked the path and deleted the directory, edit included, saying so
/// only on stderr. Cowshed cannot rebuild what it does not know, so the refresh keeps every byte
/// of it, intact, at a location the refresh names, and still links the path.
#[test]
fn a_declared_directory_survives_its_relink_at_a_location_the_refresh_names() {
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    write(
        &checkout,
        "packages/pin/.bun-pin/src/edited.zig",
        "an uncommitted edit",
    );
    link::point(&checkout, &scratch.volume()).unwrap();
    let declared = vec![BuildStatePath::declared("packages/pin/.bun-pin").unwrap()];
    let adopted = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &declared,
        &declared,
    )
    .unwrap()
    .unwrap();
    let told = adopted
        .kept
        .iter()
        .map(ToString::to_string)
        .chain(adopted.discarded.iter().map(ToString::to_string))
        .collect::<Vec<_>>();
    discards_drain(&checkout);
    assert_eq!(
        fs::read_link(checkout.join("packages/pin/.bun-pin")).unwrap(),
        Path::new("../../.cowshed/build/declared/packages/pin/.bun-pin")
    );
    let survivors = files_holding(&checkout, b"an uncommitted edit");
    let [survivor] = survivors.as_slice() else {
        panic!("the edit survives exactly once, not {survivors:?}; the refresh said {told:?}");
    };
    let kept = survivor.parent().unwrap().parent().unwrap();
    assert!(
        told.iter()
            .any(|line| line.contains(&kept.display().to_string())),
        "the refresh names {} where it kept the directory: {told:?}",
        kept.display()
    );
}

/// Every regular file under `root` whose bytes are `bytes`, never following a link.
fn files_holding(root: &Path, bytes: &[u8]) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            let metadata = fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() && fs::read(&path).unwrap() == bytes {
                found.push(path);
            }
        }
    }
    found
}

/// The module compile cache a checkout kept before discovery contributed it (its first touch, or
/// the refresh after an upgrade that newly detects it) is a real directory where the new link
/// belongs. It predates the link, so its discard says so and names no displacement: nothing
/// removed a link that never existed. Once the volume holds the path and its directory, a real
/// directory there again did displace the link, and says which tool's state it is.
#[test]
fn a_compile_cache_kept_before_its_link_existed_is_reported_as_predating_it() {
    use crate::workspace_environment::{NODE_COMPILE_CACHE_CHECKOUT, NODE_COMPILE_CACHE_DIRECTORY};
    let scratch = Scratch::new();
    let checkout = scratch.checkout();
    git(&checkout, &["init", "--quiet"]);
    write(
        &checkout,
        &format!("{NODE_COMPILE_CACHE_CHECKOUT}/v22/module"),
        "compiled before the link",
    );
    link::point(&checkout, &scratch.volume()).unwrap();
    let compile_cache =
        [BuildStatePath::new(NODE_COMPILE_CACHE_CHECKOUT, NODE_COMPILE_CACHE_DIRECTORY).unwrap()];
    let discarded = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &compile_cache,
        &[],
    )
    .unwrap()
    .unwrap()
    .discarded;
    assert_eq!(
        discarded,
        [DiscardedBuildStateFinding {
            path: PathBuf::from(NODE_COMPILE_CACHE_CHECKOUT),
            tool: BuildStateTool::JavaScript,
            origin: DiscardOrigin::Predated,
        }]
    );
    let report = discarded[0].to_string();
    assert!(!report.contains("displaced"), "{report}");
    assert!(report.contains("predates its link"), "{report}");

    fs::remove_file(checkout.join(NODE_COMPILE_CACHE_CHECKOUT)).unwrap();
    write(
        &checkout,
        &format!("{NODE_COMPILE_CACHE_CHECKOUT}/v22/module"),
        "written after a cleanup removed the link",
    );
    let displaced = adopt_paths(
        &crate::census::this_test(),
        &checkout,
        &scratch.volume(),
        &compile_cache,
        &compile_cache,
    )
    .unwrap()
    .unwrap()
    .discarded;
    assert_eq!(
        displaced,
        [DiscardedBuildStateFinding {
            path: PathBuf::from(NODE_COMPILE_CACHE_CHECKOUT),
            tool: BuildStateTool::JavaScript,
            origin: DiscardOrigin::Displaced,
        }]
    );
    assert!(
        displaced[0]
            .to_string()
            .contains("before a Node or Bun process wrote there may have displaced it"),
        "{}",
        displaced[0]
    );
    discards_drain(&checkout);
}

/// The decision's invariant over every tool and everything a path can hold: a file and a link
/// cowshed did not make are never removed or linked over, only a tool's own directory is
/// discarded, a declared directory holding anything is moved aside intact, only an empty
/// directory is removed outright, and every path ends at its fixed link but a declared one
/// nothing occupies.
#[test]
fn the_decision_deletes_nothing_cowshed_cannot_rebuild() {
    use BuildStateTool::{Cargo, Declared, JavaScript, Nx};
    for tool in [Cargo, Nx, Declared, JavaScript] {
        for occupant in [
            Occupant::Absent,
            Occupant::FixedLink,
            Occupant::ForeignLink,
            Occupant::EmptyDirectory,
            Occupant::Directory,
            Occupant::File,
        ] {
            let decided = decide(tool, occupant);
            if matches!(occupant, Occupant::ForeignLink | Occupant::File) {
                assert_eq!(decided, Err(Refuse), "{tool:?} {occupant:?}");
                continue;
            }
            let Ok(action) = decided else {
                panic!("{tool:?} {occupant:?} refused");
            };
            assert_eq!(
                action == Action::Discard,
                tool != Declared && occupant == Occupant::Directory,
                "{tool:?} {occupant:?}: {action:?}"
            );
            assert_eq!(
                action == Action::MoveAside,
                tool == Declared && occupant == Occupant::Directory,
                "{tool:?} {occupant:?}: {action:?}"
            );
            assert_eq!(
                action == Action::ReplaceEmpty,
                occupant == Occupant::EmptyDirectory,
                "{tool:?} {occupant:?}: {action:?}"
            );
            assert_eq!(
                action == Action::LeaveAbsent,
                tool == Declared && occupant == Occupant::Absent,
                "{tool:?} {occupant:?}: {action:?}"
            );
        }
    }
}

/// The refresh as a whole -- observation, decision and the shell that performs it -- over random
/// occupants of two capability paths and two declared ones.
mod property {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    /// What one case plants at one build-state path.
    #[derive(Clone, Debug)]
    enum Planted {
        Absent,
        /// The path's own link, onto a volume directory that holds a file or is missing.
        FixedLink {
            dangling: bool,
        },
        /// A link cowshed did not make, onto a file or onto nothing.
        ForeignLink {
            dangling: bool,
        },
        EmptyDirectory,
        /// The bytes of each file, which sits at [`planted_file`].
        Directory(Vec<Vec<u8>>),
        File(Vec<u8>),
    }

    /// Weighted so that a case with six paths is a successful refresh about two times in five:
    /// one file or foreign link anywhere refuses the whole refresh.
    fn planted() -> impl Strategy<Value = Planted> {
        prop_oneof![
            3 => Just(Planted::Absent),
            3 => any::<bool>().prop_map(|dangling| Planted::FixedLink { dangling }),
            1 => any::<bool>().prop_map(|dangling| Planted::ForeignLink { dangling }),
            2 => Just(Planted::EmptyDirectory),
            4 => prop::collection::vec(prop::collection::vec(any::<u8>(), 0..16), 1..4)
                .prop_map(Planted::Directory),
            1 => prop::collection::vec(any::<u8>(), 0..16).prop_map(Planted::File),
        ]
    }

    /// Three capability paths and three declared ones, disjoint. None is Nx's, whose real
    /// directories a refresh first closes against live database holders.
    fn paths() -> [BuildStatePath; 6] {
        [
            BuildStatePath::new("target", "target").unwrap(),
            BuildStatePath::new("crates/tool/target", "crates-tool-target").unwrap(),
            BuildStatePath::new("web/node_modules/.cache", "web-cache").unwrap(),
            BuildStatePath::declared("vendor/upstream").unwrap(),
            BuildStatePath::declared("packages/a/.cache/lmao").unwrap(),
            BuildStatePath::declared("packages/runtime/.bun-pin").unwrap(),
        ]
    }

    fn planted_file(index: usize) -> String {
        if index % 2 == 1 {
            format!("nested/f{index}")
        } else {
            format!("f{index}")
        }
    }

    /// Bytes no other planted file holds: the path and index lead, so survival can be checked
    /// by content wherever the bytes went.
    fn planted_bytes(path: &Path, index: usize, bytes: &[u8]) -> Vec<u8> {
        let mut content = format!("{}#{index}:", path.display()).into_bytes();
        content.extend_from_slice(bytes);
        content
    }

    fn plant(checkout: &Path, volume: &Path, state: &BuildStatePath, planted: &Planted) {
        let path = state.checkout.as_path();
        let at = checkout.join(path);
        if !matches!(planted, Planted::Absent) {
            fs::create_dir_all(at.parent().unwrap()).unwrap();
        }
        match planted {
            Planted::Absent => {}
            Planted::FixedLink { dangling } => {
                std::os::unix::fs::symlink(
                    link::relative_target(path, state.volume.as_path()),
                    &at,
                )
                .unwrap();
                if !dangling {
                    let directory = volume.join(state.volume.as_path());
                    fs::create_dir_all(&directory).unwrap();
                    fs::write(directory.join("warm"), planted_bytes(path, 99, b"warm")).unwrap();
                }
            }
            Planted::ForeignLink { dangling } => {
                let target = if *dangling {
                    checkout.join("nowhere")
                } else {
                    checkout.join("README")
                };
                std::os::unix::fs::symlink(target, &at).unwrap();
            }
            Planted::EmptyDirectory => fs::create_dir(&at).unwrap(),
            Planted::Directory(files) => {
                for (index, bytes) in files.iter().enumerate() {
                    let file = at.join(planted_file(index));
                    fs::create_dir_all(file.parent().unwrap()).unwrap();
                    fs::write(file, planted_bytes(path, index, bytes)).unwrap();
                }
            }
            Planted::File(bytes) => fs::write(&at, planted_bytes(path, 0, bytes)).unwrap(),
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Entry {
        File(Vec<u8>),
        Link(PathBuf),
    }

    /// Every file and link under `root` by its path relative to `root`, never following a link,
    /// skipping the relative paths `skip`.
    fn entries(root: &Path, skip: &[&str]) -> BTreeMap<PathBuf, Entry> {
        let mut found = BTreeMap::new();
        let mut pending = vec![PathBuf::new()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(root.join(&directory)).unwrap() {
                let relative = directory.join(entry.unwrap().file_name());
                if skip.iter().any(|skip| relative == Path::new(skip)) {
                    continue;
                }
                let path = root.join(&relative);
                let metadata = fs::symlink_metadata(&path).unwrap();
                if metadata.file_type().is_symlink() {
                    found.insert(relative, Entry::Link(fs::read_link(&path).unwrap()));
                } else if metadata.is_dir() {
                    pending.push(relative);
                } else {
                    found.insert(relative, Entry::File(fs::read(&path).unwrap()));
                }
            }
        }
        found
    }

    /// The discards a background delete is still taking.
    const NOT_CHECKOUT_DATA: [&str; 1] = [discard::DISCARD_DIRECTORY];

    /// A refresh never deletes user data. Every file and link outside a tool's own cache
    /// directory is still in the checkout afterwards, byte for byte; a file or a foreign link at
    /// any path refuses before any path changes; otherwise every path but an unoccupied declared
    /// one is its fixed link, and each declared directory's files are where the refresh says it
    /// kept them. Every case's checkout is a fresh directory of one Git repository, made once:
    /// a Git process costs seconds on a loaded macOS host, and the tracked-source guard needs one.
    #[test]
    fn a_refresh_never_deletes_user_data() {
        let scratch = Scratch::new();
        git(&scratch.0, &["init", "--quiet"]);
        let cases = std::sync::atomic::AtomicUsize::new(0);
        proptest!(ProptestConfig::with_cases(32), |(planted in prop::array::uniform6(planted()))| {
            let case = scratch
                .0
                .join(format!("case-{}", cases.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
            let (checkout, volume) = (case.join("checkout"), case.join("volume"));
            fs::create_dir_all(&checkout).unwrap();
            fs::create_dir_all(&volume).unwrap();
            fs::write(checkout.join("README"), b"outside every build-state path").unwrap();
            link::point(&checkout, &volume).unwrap();
            let paths = paths();
            for (state, planted) in paths.iter().zip(&planted) {
                plant(&checkout, &volume, state, planted);
            }
            let tool_cache = |relative: &Path| {
                paths.iter().zip(&planted).any(|(state, planted)| {
                    !state.is_declared()
                        && matches!(planted, Planted::Directory(_))
                        && relative.starts_with(state.checkout.as_path())
                })
            };
            let before = entries(&checkout, &NOT_CHECKOUT_DATA);
            let volume_before = entries(&volume, &[]);
            let refuses = planted
                .iter()
                .any(|planted| matches!(planted, Planted::File(_) | Planted::ForeignLink { .. }));

            let result = adopt_paths(&crate::census::this_test(), &checkout, &volume, &paths, &paths);
            discards_drain(&checkout);

            let after = entries(&checkout, &NOT_CHECKOUT_DATA);
            let volume_after = entries(&volume, &[]);
            for (relative, entry) in &volume_before {
                prop_assert_eq!(volume_after.get(relative), Some(entry), "{}", relative.display());
            }
            if refuses {
                prop_assert!(result.is_err(), "a file or a foreign link refuses: {:?}", result);
                prop_assert_eq!(&after, &before, "a refusal changes no path");
                return Ok(());
            }
            let adopted = match result {
                Ok(Ok(adopted)) => adopted,
                other => return Err(TestCaseError::fail(format!("refused: {other:?}"))),
            };
            for (relative, entry) in before.iter().filter(|(relative, _)| !tool_cache(relative.as_path())) {
                prop_assert!(
                    after.values().any(|survivor| survivor == entry),
                    "{} was deleted: {:?}",
                    relative.display(),
                    adopted
                );
            }
            for (state, planted) in paths.iter().zip(&planted) {
                let path = state.checkout.as_path();
                if state.is_declared() && matches!(planted, Planted::Absent) {
                    prop_assert!(
                        fs::symlink_metadata(checkout.join(path)).is_err(),
                        "{} stays absent",
                        path.display()
                    );
                    continue;
                }
                prop_assert!(link::is_fixed_link(&checkout, state), "{} is its link", path.display());
                if let (true, Planted::Directory(files)) = (state.is_declared(), planted) {
                    let kept = adopted.kept.iter().find(|kept| kept.path == path);
                    prop_assert!(kept.is_some(), "{} is kept: {:?}", path.display(), adopted);
                    let kept = &kept.unwrap().kept;
                    for (index, bytes) in files.iter().enumerate() {
                        prop_assert_eq!(
                            fs::read(kept.join(planted_file(index))).unwrap(),
                            planted_bytes(path, index, bytes)
                        );
                    }
                }
            }
        });
    }
}
