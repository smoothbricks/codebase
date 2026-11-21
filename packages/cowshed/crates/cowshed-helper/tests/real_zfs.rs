//! The helper against real ZFS: the dataset lifecycle cowshed drives through it, and the
//! refusals that only a real host can prove (the root's owner property, a mountpoint outside the
//! caller's tree, the handover of a new dataset's root).
//!
//! A host proof: it needs `sudo -n`, and `COWSHED_ZFS_TEST_PARENT` naming a dataset below which
//! root may create (specs/cowshed/08_testing.md, "Real ZFS"). Everything it makes lives in one
//! dataset of its own below that parent, destroyed on every exit path.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const HELPER: &str = env!("CARGO_BIN_EXE_cowshed-helper");

fn sudo(program: &str, arguments: &[&str]) -> Output {
    Command::new("sudo")
        .arg("-n")
        .arg(program)
        .args(arguments)
        .output()
        .expect("sudo runs")
}

fn zfs_as_root(arguments: &[&str]) {
    let output = sudo("zfs", arguments);
    assert!(
        output.status.success(),
        "zfs {arguments:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A property of a dataset or snapshot, read without privilege.
fn property(name: &str, property: &str) -> String {
    let output = Command::new("zfs")
        .args(["get", "-H", "-p", "-o", "value", property, name])
        .output()
        .expect("zfs runs");
    assert!(output.status.success(), "zfs get {property} {name}");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn exists(name: &str) -> bool {
    Command::new("zfs")
        .args(["list", "-H", "-t", "all", "-o", "name", name])
        .output()
        .expect("zfs runs")
        .status
        .success()
}

/// One scratch tree: `<parent>/cowshed-itest-<pid>` holding a cowshed root that serves this
/// user, a sibling root that serves nobody, and a directory for mountpoints.
struct Fixture {
    tree: String,
    root: String,
    directory: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let parent = std::env::var("COWSHED_ZFS_TEST_PARENT")
            .expect("COWSHED_ZFS_TEST_PARENT names the dataset this host proof may create below");
        let tree = format!("{parent}/cowshed-itest-{}", std::process::id());
        let directory = std::env::temp_dir()
            .canonicalize()
            .expect("the temporary directory resolves")
            .join(format!("cowshed-itest-{}", std::process::id()));
        std::fs::create_dir(&directory).expect("the scratch directory is new");
        let uid = std::fs::metadata(&directory).expect("it exists").uid();
        let fixture = Self {
            root: format!("{tree}/cowshed"),
            tree,
            directory,
        };
        zfs_as_root(&["create", "-o", "mountpoint=none", &fixture.tree]);
        zfs_as_root(&[
            "create",
            "-o",
            &format!("cowshed:owner={uid}"),
            &fixture.root,
        ]);
        zfs_as_root(&["create", &format!("{}/unowned", fixture.tree)]);
        zfs_as_root(&["create", &format!("{}/unowned/cowshed", fixture.tree)]);
        fixture
    }

    fn helper_at(&self, root: &str, arguments: &[&str]) -> Output {
        let mut all = vec![root];
        all.extend(arguments);
        sudo(HELPER, &all)
    }

    fn run(&self, arguments: &[&str]) {
        let output = self.helper_at(&self.root, arguments);
        assert!(
            output.status.success(),
            "{arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// The helper refuses with status 2, saying `reason`.
    fn refused(&self, root: &str, arguments: &[&str], reason: &str) {
        let output = self.helper_at(root, arguments);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(2), "{arguments:?}: {stderr}");
        assert!(stderr.contains(reason), "{arguments:?}: {stderr}");
    }

    fn dataset(&self, below: &str) -> String {
        format!("{}/{below}", self.root)
    }

    fn path(&self, below: &str) -> String {
        self.directory
            .join(below)
            .to_str()
            .expect("UTF-8")
            .to_owned()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // `-R` reaches clones, which all lie inside the tree; `-f` unmounts what a failed step
        // left mounted.
        let destroyed = sudo("zfs", &["destroy", "-R", "-f", &self.tree]);
        let removed = sudo(
            "rm",
            &["-rf", "--", self.directory.to_str().expect("UTF-8")],
        );
        if !std::thread::panicking() {
            assert!(destroyed.status.success(), "the scratch tree is destroyed");
            assert!(removed.status.success(), "the scratch directory is removed");
        }
    }
}

#[test]
#[ignore = "host proof: needs sudo and COWSHED_ZFS_TEST_PARENT (the live host check of specs/cowshed/08_testing.md, Real ZFS)"]
fn the_helper_drives_a_workspace_and_a_build_volume_through_their_lives() {
    let fixture = Fixture::new();
    let repo = "projects/acme/widget";
    let main = fixture.dataset(&format!("{repo}/main"));
    let ws = fixture.dataset(&format!("{repo}/ws"));
    let raven = format!("{ws}/raven");
    let trash = format!("{ws}/.trash");
    let retired = format!("{trash}/raven-1");
    let fork = format!("{main}@cowshed:fork-raven");
    let build = fixture.dataset(&format!("{repo}/build"));

    // Adopt: containers that never mount, then main at a path of the caller's.
    for container in ["projects", "projects/acme", repo] {
        fixture.run(&["create", &fixture.dataset(container), "mountpoint=none"]);
    }
    let checkout = fixture.path("checkout");
    fixture.run(&["create", &main, &format!("mountpoint={checkout}")]);
    assert_eq!(property(&main, "mounted"), "yes");
    // A new dataset's root is root's until the helper hands it over.
    std::fs::write(Path::new(&checkout).join("tracked"), "main").expect("the caller owns main");

    // Fork: a snapshot of main, cloned below a container that passes its mountpoint down.
    let mounts = fixture.path("mnt");
    fixture.run(&[
        "create",
        &ws,
        "canmount=off",
        &format!("mountpoint={mounts}"),
    ]);
    fixture.run(&["create", &trash, "canmount=off", "mountpoint=none"]);
    fixture.run(&["snapshot", &fork]);
    fixture.run(&[
        "clone",
        &fork,
        &raven,
        "refquota=1073741824",
        "cowshed:role=ws",
    ]);
    let workspace = Path::new(&mounts).join("raven");
    assert_eq!(property(&raven, "mountpoint"), workspace.to_str().unwrap());
    assert_eq!(property(&raven, "refquota"), "1073741824");
    assert_eq!(
        std::fs::read_to_string(workspace.join("tracked")).unwrap(),
        "main"
    );
    std::fs::write(workspace.join("tracked"), "raven").expect("a clone keeps its origin's owner");
    assert_eq!(
        std::fs::read_to_string(Path::new(&checkout).join("tracked")).unwrap(),
        "main"
    );

    // What only this host can refuse.
    fixture.refused(
        &fixture.root,
        &["set", &raven, "mountpoint=/etc/cowshed-itest"],
        "belongs to uid 0",
    );
    fixture.refused(
        &format!("{}/unowned/cowshed", fixture.tree),
        &["create", &format!("{}/unowned/cowshed/x", fixture.tree)],
        "cowshed:owner",
    );
    // `check` proves the same two things and changes nothing: root, and a root naming the caller.
    fixture.run(&["check"]);
    fixture.refused(
        &format!("{}/unowned/cowshed", fixture.tree),
        &["check"],
        "cowshed:owner",
    );
    fixture.refused(&fixture.root, &["check", &raven], "usage:");
    fixture.refused(
        &fixture.root,
        &["destroy", &format!("{}/unowned", fixture.tree)],
        "not below the cowshed root",
    );
    assert!(exists(&format!("{}/unowned", fixture.tree)));
    assert_eq!(property(&raven, "mountpoint"), workspace.to_str().unwrap());

    // The origin pins its clone's source: ZFS itself refuses, and the helper never recurses.
    let pinned = fixture.helper_at(&fixture.root, &["destroy", &fork]);
    assert!(!pinned.status.success() && exists(&raven));

    // rm: unmount, hold the dataset unmountable, retire it by rename, then clone before origin.
    fixture.run(&["unmount", &raven]);
    fixture.run(&["set", &raven, "canmount=off"]);
    fixture.run(&["rename", &raven, &retired]);
    assert_eq!(property(&retired, "mounted"), "no");
    fixture.run(&["destroy", &retired]);
    fixture.run(&["destroy", &fork]);
    assert!(!exists(&retired) && !exists(&fork));

    // Land: the landing build dataset, a clone of the target's seed, takes over its lineage.
    let (target, landing) = (format!("{build}/1"), format!("{build}/2"));
    let seed = format!("{target}@seed-1");
    fixture.run(&[
        "create",
        &build,
        "canmount=off",
        &format!("mountpoint={}", fixture.path("build")),
    ]);
    fixture.run(&["create", &target, "compression=zstd-1", "sync=disabled"]);
    fixture.run(&["snapshot", &seed]);
    fixture.run(&["clone", &seed, &landing]);
    assert_eq!(property(&landing, "origin"), seed);
    fixture.refused(&fixture.root, &["promote", &main], "not a build dataset");
    fixture.run(&["promote", &landing]);
    assert_eq!(property(&target, "origin"), format!("{landing}@seed-1"));
    // The old build dataset is now a clone of the adopted one and goes at once; a superseded
    // seed is destroyed deferred.
    fixture.run(&["destroy", &target]);
    fixture.run(&["destroy", &format!("{landing}@seed-1"), "--defer"]);
    assert!(!exists(&target) && !exists(&format!("{landing}@seed-1")));
}
