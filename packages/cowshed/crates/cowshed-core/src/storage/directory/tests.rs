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
    assert_eq!(fs::canonicalize(mount.join("src")).unwrap(), mount.join("src"));
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
    assert_eq!(observed_mount(&image).unwrap(), None, "a fact pass sees no mount yet");

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
