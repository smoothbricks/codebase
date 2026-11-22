//! The helper against real ZFS: the dataset lifecycle cowshed drives through it, and the
//! refusals that only a real host can prove (the root's owner property, a mountpoint outside the
//! caller's tree, the handover of a new dataset's root).
//!
//! An explicit host check, never part of an ordinary test run (specs/cowshed/08_testing.md, "Real
//! ZFS"): it drives the helper `cowshed setup` installed, through `sudo -n` as the product does,
//! below the cowshed root that names this user, `${COWSHED_ZFS_TEST_PARENT}/cowshed`. Nothing asks
//! for a password: a host where that root or that helper is not proven skips, saying why.
//! Everything it makes lives in a project namespace of its own, destroyed leaf first through the
//! helper on every exit path.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The helper `cowshed setup` installed, where `sudo` admits it without a password.
/// Where this run's mountpoints live: the per-user fixture directory beside cowshed's mount root
/// (08_testing.md, "Fixtures"), a directory of this user's on a filesystem that keeps ownership.
/// A temporary directory is no such place: a `cowshed exec` job's lies in the store, whose volume
/// ignores ownership, and the helper refuses a mountpoint whose owner it cannot read as the
/// caller's.
#[cfg(target_os = "macos")]
fn scratch_parent() -> PathBuf {
    // SAFETY: reads this process's real uid.
    let uid = unsafe { libc::getuid() };
    let parent = PathBuf::from(format!("/private/var/folders/cowshed/{uid}/t"));
    std::fs::create_dir_all(&parent).expect("the per-user fixture directory");
    parent
}

#[cfg(target_os = "linux")]
fn scratch_parent() -> PathBuf {
    let parent = PathBuf::from(std::env::var_os("HOME").expect("HOME")).join(".cowshed/t");
    std::fs::create_dir_all(&parent).expect("the per-user fixture directory");
    parent
}
fn installed_helper() -> PathBuf {
    Path::new(cowshed_helper::host::SYSTEM_DIRECTORY).join("cowshed-helper")
}

fn zfs(arguments: &[&str]) -> Output {
    Command::new("zfs")
        .args(arguments)
        .env("PATH", cowshed_helper::host::TRUSTED_PATH)
        .output()
        .expect("zfs runs")
}

/// A property of a dataset or snapshot, read without privilege.
fn property(name: &str, property: &str) -> String {
    let output = zfs(&["get", "-H", "-p", "-o", "value", property, name]);
    assert!(output.status.success(), "zfs get {property} {name}");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn exists(name: &str) -> bool {
    zfs(&["list", "-H", "-t", "all", "-o", "name", name])
        .status
        .success()
}

/// The names below `name`, itself included, parents before children.
fn listed(name: &str) -> Vec<String> {
    let output = zfs(&["list", "-H", "-r", "-t", "all", "-o", "name", name]);
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}

/// One run's namespace below the cowshed root that names this user: the project owner
/// `cowshed-itest-<pid>`, a root inside it that names nobody, and a directory of this user's for
/// mountpoints.
struct Fixture {
    root: String,
    parent: String,
    tree: String,
    /// Whether this run made `<root>/projects`, which it then removes when it is empty.
    projects: bool,
    directory: PathBuf,
}

impl Fixture {
    /// The fixture, or `None` with the reason this host cannot run the check printed.
    fn new() -> Option<Self> {
        let skip = |reason: String| {
            eprintln!("skipped: {reason}");
            None
        };
        let Ok(parent) = std::env::var("COWSHED_ZFS_TEST_PARENT") else {
            return skip(String::from(
                "COWSHED_ZFS_TEST_PARENT names no pool or dataset whose cowshed root to use",
            ));
        };
        let root = format!("{parent}/cowshed");
        // SAFETY: reads this process's real uid.
        let uid = unsafe { libc::getuid() };
        let owner = zfs(&["get", "-H", "-o", "value,source", "cowshed:owner", &root]);
        let owner = String::from_utf8_lossy(&owner.stdout).trim().to_owned();
        if owner != format!("{uid}\tlocal") {
            return skip(format!(
                "{root} is not a cowshed root whose owner property names uid {uid} locally (read: {owner:?})"
            ));
        }
        let helper = installed_helper();
        let proven = Command::new("sudo")
            .args(["-n", "-k"])
            .arg(&helper)
            .args([root.as_str(), "check"])
            .output();
        match &proven {
            Ok(output) if output.status.success() => {}
            other => {
                return skip(format!(
                    "{} does not act as root for {root} without a password: {other:?}",
                    helper.display()
                ));
            }
        }
        let directory = scratch_parent().join(format!("cowshed-itest-{}", std::process::id()));
        std::fs::create_dir(&directory).expect("the scratch directory is new");
        let projects = !exists(&format!("{root}/projects"));
        let fixture = Self {
            tree: format!("{root}/projects/cowshed-itest-{}", std::process::id()),
            root,
            parent,
            projects,
            directory,
        };
        if projects {
            fixture.run(&[
                "create",
                &format!("{}/projects", fixture.root),
                "mountpoint=none",
            ]);
        }
        fixture.run(&["create", &fixture.tree, "mountpoint=none"]);
        fixture.run(&[
            "create",
            &format!("{}/unowned", fixture.tree),
            "mountpoint=none",
        ]);
        fixture.run(&[
            "create",
            &format!("{}/unowned/cowshed", fixture.tree),
            "mountpoint=none",
        ]);
        Some(fixture)
    }

    fn helper_at(&self, root: &str, arguments: &[&str]) -> Output {
        Command::new("sudo")
            .arg("-n")
            .arg(installed_helper())
            .arg(root)
            .args(arguments)
            .output()
            .expect("sudo runs")
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
        format!("{}/{below}", self.tree)
    }

    /// A mountpoint of this run, made by this user before anything mounts there.
    fn path(&self, below: &str) -> String {
        let path = self.directory.join(below);
        std::fs::create_dir_all(&path).expect("the mountpoint");
        path.to_str().expect("UTF-8").to_owned()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Leaf first through the helper, which never recurses: a clone goes before its origin,
        // so every pass destroys what nothing depends on until nothing is left.
        let mut left = listed(&self.tree);
        while !left.is_empty() {
            for name in left.iter().rev() {
                let _ = self.helper_at(&self.root, &["destroy", name]);
            }
            let after = listed(&self.tree);
            if after.len() == left.len() {
                break;
            }
            left = after;
        }
        let projects = format!("{}/projects", self.root);
        if self.projects && left.is_empty() {
            let _ = self.helper_at(&self.root, &["destroy", &projects]);
        }
        let removed = std::fs::remove_dir_all(&self.directory);
        if !std::thread::panicking() {
            assert_eq!(
                left,
                Vec::<String>::new(),
                "the run's datasets are destroyed"
            );
            assert!(
                !self.projects || !exists(&projects),
                "the projects container this run made is destroyed"
            );
            assert!(
                removed.is_ok(),
                "the scratch directory is removed: {removed:?}"
            );
        }
    }
}

#[test]
#[ignore = "explicit host check: real ZFS through the installed helper (specs/cowshed/08_testing.md, Real ZFS)"]
fn the_helper_drives_a_workspace_and_a_build_volume_through_their_lives() {
    let Some(fixture) = Fixture::new() else {
        return;
    };
    let repo = "widget";
    let main = fixture.dataset(&format!("{repo}/main"));
    let ws = fixture.dataset(&format!("{repo}/ws"));
    let raven = format!("{ws}/raven");
    let trash = format!("{ws}/.trash");
    let retired = format!("{trash}/raven-1");
    let fork = format!("{main}@cowshed:fork-raven");
    let build = fixture.dataset(&format!("{repo}/build"));

    // Adopt: a container that never mounts, then main at a path of the caller's.
    fixture.run(&["create", &fixture.dataset(repo), "mountpoint=none"]);
    let checkout = fixture.path("checkout");
    fixture.run(&["create", &main, &format!("mountpoint={checkout}")]);
    assert_eq!(property(&main, "mounted"), "yes");
    // A new dataset's root is root's until the helper hands it over.
    std::fs::write(Path::new(&checkout).join("tracked"), "main").expect("the caller owns main");

    // Fork: a snapshot of main, cloned below a container that passes its mountpoint down.
    let mounts = fixture.path("mnt");
    fixture.path("mnt/raven");
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
        &["set", &raven, "mountpoint=/usr/cowshed-itest"],
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
    // A dataset outside the root, here one beside it that does not exist, is refused by name
    // before anything runs.
    let outside = format!(
        "{}/cowshed-itest-{}-outside",
        fixture.parent,
        std::process::id()
    );
    fixture.refused(
        &fixture.root,
        &["destroy", &outside],
        "not below the cowshed root",
    );
    assert!(!exists(&outside));
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
    fixture.path("build/1");
    fixture.path("build/2");
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
