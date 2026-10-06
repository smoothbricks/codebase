use super::*;
use std::os::unix::fs::symlink;

/// A host HOME and a retired volume (with its marker) side by side in a temp directory.
struct Fixture {
    root: PathBuf,
    home: PathBuf,
    volume: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("cowshed-retire-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let home = root.join("home");
        let volume = root.join("volume");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&volume).unwrap();
        fs::write(volume.join(VOLUME_MARKER_FILE), "{}").unwrap();
        Self { root, home, volume }
    }

    fn layout(&self, declared: &[&str]) -> RetirementLayout {
        let declared = declared.iter().map(PathBuf::from).collect::<Vec<_>>();
        RetirementLayout::new(&self.home, &self.volume, &declared)
    }

    /// Write `bytes` bytes at `path`, creating its parents.
    fn file(&self, path: &Path, bytes: usize) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, vec![b'x'; bytes]).unwrap();
    }

    fn run(&self, layout: &RetirementLayout) -> RetirementReport {
        let plan = plan(layout, &observe(layout).unwrap());
        execute(layout, &plan).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn outcome<'a>(report: &'a RetirementReport, label: &str) -> &'a CacheOutcome {
    report
        .caches
        .iter()
        .find(|cache| cache.placement.label == label)
        .unwrap_or_else(|| panic!("no outcome for {label}: {report:?}"))
}

/// A host link into the volume is replaced by the tool's own directory holding the same bytes,
/// and the volume keeps nothing but its marker; cargo's two move under cargo's own locks.
#[test]
fn a_host_link_into_the_volume_becomes_the_tools_own_directory() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    let registry = fixture.volume.join("cargo/registry");
    fixture.file(&registry.join("index/config.json"), 10);
    fixture.file(&registry.join("cache/serde.crate"), 90);
    fs::create_dir_all(fixture.volume.join("cargo/git")).unwrap();
    fs::create_dir_all(fixture.home.join(".cargo")).unwrap();
    symlink(&registry, fixture.home.join(".cargo/registry")).unwrap();
    symlink(
        fixture.volume.join("cargo/git"),
        fixture.home.join(".cargo/git"),
    )
    .unwrap();

    let observation = observe(&layout).unwrap();
    assert_eq!(
        observation.hosts[&fixture.home.join(".cargo/registry")],
        HostState::LinkIntoVolume
    );
    let planned = plan(&layout, &observation);
    assert_eq!(planned.owners(), BTreeSet::from([Owner::Cargo]));
    assert!(planned.leftovers.is_empty());
    assert!(matches!(
        &planned.steps[..],
        [
            Step::Move {
                replace_link: true,
                bytes: 0,
                ..
            },
            Step::Move {
                replace_link: true,
                bytes: 100,
                ..
            },
            Step::RemoveEmpty(cargo),
        ] if cargo == &fixture.volume.join("cargo")
    ));

    let report = execute(&layout, &planned).unwrap();
    let host = fixture.home.join(".cargo/registry");
    assert!(
        !fs::symlink_metadata(&host)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read(host.join("cache/serde.crate")).unwrap().len(), 90);
    assert!(fixture.home.join(".cargo/git").is_dir());
    assert_eq!(outcome(&report, "cargo registry").moved_bytes, 100);
    assert!(report.leftovers.is_empty());
    assert!(holds_only_marker(&fixture.volume).unwrap());
}

/// A content-addressed cache in both places is merged: every entry the host lacks moves in and
/// every entry it already holds is dropped from the volume as a duplicate.
#[test]
fn a_content_addressed_cache_in_both_places_is_merged_entry_by_entry() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    let volume = fixture.volume.join("uv");
    let host = fixture.home.join(".cache/uv");
    fixture.file(&volume.join("archive/a"), 5);
    fixture.file(&host.join("archive/a"), 5);
    fixture.file(&volume.join("archive/b"), 7);
    fixture.file(&volume.join("wheels/c"), 11);
    fixture.file(&host.join("wheels/d"), 13);

    let planned = plan(&layout, &observe(&layout).unwrap());
    assert!(matches!(
        &planned.steps[..],
        [Step::Merge { placement, bytes: 23 }] if placement.host == host
    ));
    let report = execute(&layout, &planned).unwrap();
    let uv = outcome(&report, "uv cache");
    assert_eq!((uv.moved_bytes, uv.dropped_bytes), (18, 5));
    for entry in ["archive/a", "archive/b", "wheels/c", "wheels/d"] {
        assert!(host.join(entry).is_file(), "{entry}");
    }
    assert!(!volume.exists());
    assert!(holds_only_marker(&fixture.volume).unwrap());
}

/// Entries the host already holds under the same key are dropped, never kept twice and never
/// overwriting the host's bytes.
#[test]
fn duplicates_are_dropped_and_the_host_bytes_are_kept() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    let volume = fixture.volume.join("go/mod");
    let host = fixture.home.join("go/pkg/mod");
    fixture.file(&volume.join("cache/download/x.zip"), 8);
    fs::create_dir_all(host.join("cache/download")).unwrap();
    fs::write(host.join("cache/download/x.zip"), "host").unwrap();

    let report = fixture.run(&layout);
    let go = outcome(&report, "go module cache");
    assert_eq!((go.moved_bytes, go.dropped_bytes), (0, 8));
    assert_eq!(
        fs::read_to_string(host.join("cache/download/x.zip")).unwrap(),
        "host"
    );
    assert!(holds_only_marker(&fixture.volume).unwrap());
}

/// Nix's client state is not content-addressed: the host's copy wins whole and the volume's is
/// deleted, entries the host lacks included.
#[test]
fn nix_state_keeps_the_host_copy() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    fixture.file(
        &fixture.volume.join("nix/cache/fetcher-cache-v4.sqlite"),
        30,
    );
    fixture.file(&fixture.home.join(".cache/nix/eval-cache-v5/a.sqlite"), 3);

    let report = fixture.run(&layout);
    let nix = outcome(&report, "nix fetcher cache");
    assert_eq!((nix.moved_bytes, nix.dropped_bytes), (0, 30));
    assert!(
        !fixture
            .home
            .join(".cache/nix/fetcher-cache-v4.sqlite")
            .exists()
    );
    assert!(
        fixture
            .home
            .join(".cache/nix/eval-cache-v5/a.sqlite")
            .is_file()
    );
    assert!(holds_only_marker(&fixture.volume).unwrap());
}

/// A cargo process holding cargo's package-cache lock delays the run until it lets go; nothing
/// moves while the lock is held, and the run completes once it is released.
#[test]
fn a_held_cargo_lock_is_waited_out_before_anything_moves() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    fixture.file(&fixture.volume.join("cargo/registry/index/x"), 4);
    fixture.file(&fixture.volume.join("zig/o/h"), 4);
    let cargo_home = fixture.home.join(".cargo");
    fs::create_dir_all(&cargo_home).unwrap();
    let held = cargo::try_lock_caches(&cargo_home)
        .unwrap()
        .expect("unlocked");

    let planned = plan(&layout, &observe(&layout).unwrap());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            started_tx.send(()).unwrap();
            done_tx.send(execute(&layout, &planned)).unwrap();
        });
        started_rx.recv().unwrap();
        // The run cannot finish while the lock is held: its result is not ready, and nothing
        // has moved yet.
        assert!(done_rx.try_recv().is_err());
        assert!(fixture.volume.join("cargo/registry/index/x").is_file());
        drop(held);
        let report = done_rx.recv().unwrap().unwrap();
        assert!(report.leftovers.is_empty());
    });
    assert!(fixture.home.join(".cargo/registry/index/x").is_file());
}

/// A directory no detector names and no main declares stays, named with its size; an empty one
/// is removed, and an interior directory holding a leftover stays too.
#[test]
fn undeclared_directories_are_left_in_place_and_named_with_their_size() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    fixture.file(&fixture.volume.join("ttsc/plugin/out.js"), 42);
    fs::create_dir_all(fixture.volume.join("devenv")).unwrap();
    fixture.file(&fixture.volume.join("cargo/stray/file"), 6);
    fixture.file(&fixture.volume.join("notes.txt"), 2);
    for bookkeeping in [".fseventsd/x", ".Spotlight-V100/y"] {
        fixture.file(&fixture.volume.join(bookkeeping), 1);
    }
    fixture.file(&fixture.volume.join(".DS_Store"), 1);

    let report = fixture.run(&layout);
    let mut leftovers = report
        .leftovers
        .iter()
        .map(|leftover| {
            (
                leftover.path.clone(),
                leftover.bytes,
                leftover.reason.clone(),
            )
        })
        .collect::<Vec<_>>();
    leftovers.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        leftovers,
        [
            (
                fixture.volume.join("cargo/stray"),
                6,
                LeftoverReason::Undeclared
            ),
            (
                fixture.volume.join("notes.txt"),
                2,
                LeftoverReason::Undeclared
            ),
            (fixture.volume.join("ttsc"), 42, LeftoverReason::Undeclared),
        ]
    );
    assert!(!fixture.volume.join("devenv").exists());
    assert!(fixture.volume.join("cargo").is_dir());
    assert!(!holds_only_marker(&fixture.volume).unwrap());
}

/// A repository-placed directory goes to the `[caches] home` path an adopted main declares,
/// matched by final component; two declarations with its name leave it in place as ambiguous.
#[test]
fn repository_caches_go_to_the_declared_home_path_matched_by_name() {
    let fixture = Fixture::new();
    fixture.file(&fixture.volume.join("ttsc/plugin/out.js"), 42);
    let report = fixture.run(&fixture.layout(&[".cache/ttsc"]));
    assert_eq!(outcome(&report, "ttsc (repository cache)").moved_bytes, 42);
    assert!(fixture.home.join(".cache/ttsc/plugin/out.js").is_file());
    assert!(holds_only_marker(&fixture.volume).unwrap());

    fixture.file(&fixture.volume.join("ttsc/plugin/again.js"), 1);
    let layout = fixture.layout(&[".cache/ttsc", "Library/Caches/ttsc"]);
    let planned = plan(&layout, &observe(&layout).unwrap());
    assert_eq!(
        planned.leftovers,
        [Leftover {
            path: fixture.volume.join("ttsc"),
            bytes: 1,
            reason: LeftoverReason::Ambiguous(vec![
                fixture.home.join(".cache/ttsc"),
                fixture.home.join("Library/Caches/ttsc"),
            ]),
        }]
    );
    assert!(planned.steps.is_empty());
}

/// Gateway mirrors move to cowshed's own cache directory and sccache's store to sccache's
/// default, each naming the writer setup stops around the move.
#[test]
fn mirrors_and_the_sccache_store_go_to_their_own_directories() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    fixture.file(&fixture.volume.join("mirror/obj-1"), 3);
    fixture.file(&fixture.volume.join("sccache/a/b"), 4);
    let planned = plan(&layout, &observe(&layout).unwrap());
    assert_eq!(
        planned.owners(),
        BTreeSet::from([Owner::Gateway, Owner::Sccache])
    );
    execute(&layout, &planned).unwrap();
    assert!(
        crate::host_dirs::gateway_mirror(&fixture.home)
            .join("obj-1")
            .is_file()
    );
    assert!(
        sccache::cache_directory(&fixture.home)
            .join("a/b")
            .is_file()
    );
}

/// A link a declarative module owns is a conflict naming the module option; setup never
/// rewrites it and leaves the volume directory where it is.
#[test]
fn a_module_owned_link_is_a_conflict_and_is_left_alone() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    fixture.file(&fixture.volume.join("zig/o/h"), 4);
    fs::create_dir_all(fixture.home.join(".cache")).unwrap();
    let target = Path::new("/nix/store/0000-home-manager-files/.cache/zig");
    symlink(target, fixture.home.join(".cache/zig")).unwrap();

    let planned = plan(&layout, &observe(&layout).unwrap());
    assert!(planned.steps.is_empty());
    let [leftover] = &planned.leftovers[..] else {
        panic!("{planned:?}");
    };
    let LeftoverReason::Conflict(reason) = &leftover.reason else {
        panic!("{leftover:?}");
    };
    assert!(reason.contains("home.file.\".cache/zig\""), "{reason}");
    assert_eq!(
        fs::read_link(fixture.home.join(".cache/zig")).unwrap(),
        target
    );
}

/// A run that died after its copy but before publishing leaves a staging copy and the host link;
/// one that died after publishing leaves the source too. Either resumes, and a finished
/// retirement plans nothing at all.
#[test]
fn a_partial_run_resumes_and_a_finished_one_plans_nothing() {
    let fixture = Fixture::new();
    let layout = fixture.layout(&[]);
    // Died before publishing: a partial staging copy beside the host link.
    let volume = fixture.volume.join("cargo/git");
    fixture.file(&volume.join("db/x/HEAD"), 9);
    fs::create_dir_all(fixture.home.join(".cargo")).unwrap();
    symlink(&volume, fixture.home.join(".cargo/git")).unwrap();
    let staging = fixture.home.join(".cargo/.git.cowshed-retiring");
    fixture.file(&staging.join("db/partial"), 1);
    // Died after publishing: the host already holds the whole copy, the source remains.
    fixture.file(&fixture.volume.join("zig/o/h"), 4);
    fixture.file(&fixture.home.join(".cache/zig/o/h"), 4);

    let planned = plan(&layout, &observe(&layout).unwrap());
    assert_eq!(
        planned.steps.first(),
        Some(&Step::CleanStaging(staging.clone()))
    );
    let report = execute(&layout, &planned).unwrap();
    assert!(!staging.exists());
    assert!(fixture.home.join(".cargo/git/db/x/HEAD").is_file());
    assert!(!fixture.home.join(".cargo/git/db/partial").exists());
    let zig = outcome(&report, "zig global cache");
    assert_eq!((zig.moved_bytes, zig.dropped_bytes), (0, 4));
    assert!(holds_only_marker(&fixture.volume).unwrap());

    let again = plan(&layout, &observe(&layout).unwrap());
    assert_eq!(again, Plan::default());
    assert!(execute(&layout, &again).unwrap().caches.is_empty());
}

/// The marker is required: a directory without it is not the retired volume.
#[test]
fn only_a_marked_volume_with_nothing_else_holds_only_its_marker() {
    let fixture = Fixture::new();
    assert!(holds_only_marker(&fixture.volume).unwrap());
    fs::remove_file(fixture.volume.join(VOLUME_MARKER_FILE)).unwrap();
    assert!(!holds_only_marker(&fixture.volume).unwrap());
}
