use std::fs;
use std::path::{Path, PathBuf};

use super::{MountPointState, bind, mount_point_state, tree_usage, unbind};

const SOURCE: &[u8] = b"pub fn answer() -> u8 { 42 }\n";

struct Scratch(PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "cowshed-directory-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        Self(root)
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
fn binding_replaces_a_vacant_mountpoint_and_unbinding_restores_it() {
    let scratch = Scratch::new("vacant");
    let image = scratch.payload("main.dmg");
    let mount = scratch.path().join("checkout");
    fs::create_dir(&mount).unwrap();

    bind(&image, &mount).unwrap();
    assert_eq!(
        mount_point_state(&mount).unwrap(),
        MountPointState::Bound(image.clone())
    );
    assert_eq!(fs::read(mount.join("src/lib.rs")).unwrap(), SOURCE);
    // Idempotent for its own payload.
    bind(&image, &mount).unwrap();

    unbind(&image, &mount).unwrap();
    assert_eq!(mount_point_state(&mount).unwrap(), MountPointState::Vacant);
    assert!(image.join("src/lib.rs").exists(), "unbinding never touches the payload");
    assert!(!scratch.path().join("checkout.cowshed-bind").exists());
    assert!(!scratch.path().join("checkout.cowshed-unbind").exists());
}

#[test]
fn binding_creates_an_absent_mountpoint() {
    let scratch = Scratch::new("absent");
    let image = scratch.payload("feature.dmg");
    let mount = scratch.path().join("mnt/owner/repo/feature");

    bind(&image, &mount).unwrap();
    assert_eq!(
        mount_point_state(&mount).unwrap(),
        MountPointState::Bound(image)
    );
}

#[test]
fn binding_refuses_an_occupied_mountpoint_and_another_payload() {
    let scratch = Scratch::new("occupied");
    let image = scratch.payload("a.dmg");
    let other = scratch.payload("b.dmg");
    let occupied = scratch.path().join("occupied");
    fs::create_dir(&occupied).unwrap();
    fs::write(occupied.join("work.txt"), b"unsaved").unwrap();
    assert!(bind(&image, &occupied).is_err());
    assert_eq!(fs::read(occupied.join("work.txt")).unwrap(), b"unsaved");

    let mount = scratch.path().join("mount");
    bind(&image, &mount).unwrap();
    assert!(bind(&other, &mount).is_err());
    assert!(unbind(&other, &mount).is_err(), "unbinding names its own payload");
    assert_eq!(
        mount_point_state(&mount).unwrap(),
        MountPointState::Bound(image)
    );
}

#[test]
fn unbinding_nothing_bound_changes_nothing() {
    let scratch = Scratch::new("unbound");
    let image = scratch.payload("a.dmg");
    let tree = scratch.payload("user-checkout");
    unbind(&image, &tree).unwrap();
    assert!(tree.join("src/lib.rs").exists());
    unbind(&image, &scratch.path().join("absent")).unwrap();
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
