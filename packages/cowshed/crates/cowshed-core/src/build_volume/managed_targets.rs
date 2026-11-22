//! The Cargo target links mbx manages in a checkout (16_build_volumes.md, "Fork" step 4): mbx
//! creates `<Cargo workspace>/target -> <build cache>/mbx/targets/v1/<checkout digest>` at the
//! checkout's first build of that workspace, and a clone of the checkout inherits every one of
//! them, naming the source's managed targets. A fork drops each inherited link that names a
//! managed target, so its first build places its own, and the source's retirement (`mbx clean
//! --under`, which removes what the source's links name) cannot break the fork's builds.
//!
//! The candidates are the Cargo targets the source's build-state discovery recorded as mbx's
//! ([`crate::build_volume::BuildVolumeState::mbx_targets`]), nested Cargo workspaces included: no
//! walk of the tree finds them. Only the final symlink is unlinked, never what it names: a real
//! directory, a link to anything but a managed target, and a candidate reached through a symlinked
//! directory are left as they are.

use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use crate::capabilities::RelPath;
use crate::{CowshedError, Result};

/// Where every managed target mbx creates lies: `targets` in the one store every job's mbx shares
/// on the build-cache volume `build_cache` (03_caches.md, "mbx"; `MBX_TARGET_ROOT` unset).
pub fn managed_root(build_cache: &Path) -> PathBuf {
    crate::capabilities::mbx::store(build_cache).join("targets")
}

/// Unlink each `candidate` of the cloned checkout `tree_root` that is a symlink whose target,
/// resolved lexically against its own directory and never followed, is one checkout's managed
/// target: `<managed_root>/v1/<checkout digest>`, exactly one name below `v1`.
/// Returns the unlinked paths, relative to `tree_root`. A candidate that is absent, not a symlink,
/// a link to anything else, or reached through a symlink or a non-directory is left as it is.
pub fn drop_inherited(
    tree_root: &Path,
    candidates: &[RelPath],
    managed_root: &Path,
) -> Result<Vec<PathBuf>> {
    let mut dropped = Vec::new();
    for candidate in candidates {
        let relative = candidate.as_path();
        let Some(leaf) = relative.file_name() else {
            continue;
        };
        let parent = relative.parent().unwrap_or_else(|| Path::new(""));
        if !real_directories(tree_root, parent)? {
            continue;
        }
        let at = tree_root.join(parent).join(leaf);
        let metadata = match fs::symlink_metadata(&at) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
            Err(source) => return Err(inspect_error(&at, &source)),
        };
        if !metadata.is_symlink() {
            continue;
        }
        let target = fs::read_link(&at).map_err(|source| inspect_error(&at, &source))?;
        let resolved = crate::inherited_links::resolve_in_source(tree_root, parent, &target);
        if !is_managed_target(&resolved, managed_root) {
            continue;
        }
        // The link is a name, not content: unlinking it never touches the managed target.
        match fs::remove_file(&at) {
            Ok(()) => dropped.push(relative.to_path_buf()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(CowshedError::integrity(
                    format!(
                        "cannot drop the inherited mbx target link {}: {source}",
                        at.display()
                    ),
                    "check the workspace mount is writable, then retry",
                ));
            }
        }
    }
    Ok(dropped)
}

/// [`drop_inherited`], run off the async runtime: each candidate's checks and unlink block on the
/// workspace mount.
pub async fn drop_inherited_in(
    tree_root: PathBuf,
    candidates: Vec<RelPath>,
    managed_root: PathBuf,
) -> Result<Vec<PathBuf>> {
    tokio::task::spawn_blocking(move || drop_inherited(&tree_root, &candidates, &managed_root))
        .await
        .map_err(|source| {
            CowshedError::integrity(
                format!("dropping inherited mbx target links panicked: {source}"),
                "retry the operation and report the failure if it repeats",
            )
        })?
}
/// Whether every component of `parent` under `tree_root` is a real directory: a symlinked ancestor
/// would direct the unlink outside the clone, so such a candidate is left alone.
fn real_directories(tree_root: &Path, parent: &Path) -> Result<bool> {
    let mut path = tree_root.to_path_buf();
    for component in parent.components() {
        let Component::Normal(name) = component else {
            return Ok(false);
        };
        path.push(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !metadata.is_symlink() => {}
            Ok(_) => return Ok(false),
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(source) => return Err(inspect_error(&path, &source)),
        }
    }
    Ok(true)
}

/// Whether `resolved` is one checkout's managed target: `<managed_root>/v1/<checkout digest>`,
/// the layout mbx documents ("Managed target directories"; 16_build_volumes.md) with the digest
/// spelled as mbx 1.30.0 names it, never the store's root, `v1` itself, another child of `v1` or
/// a path below a target.
fn is_managed_target(resolved: &Path, managed_root: &Path) -> bool {
    resolved.parent() == Some(managed_root.join("v1").as_path())
        && resolved
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(is_checkout_digest)
}

/// A checkout digest as mbx names its managed targets: 64 lowercase hexadecimal digits.
fn is_checkout_digest(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}
fn inspect_error(path: &Path, source: &io::Error) -> CowshedError {
    CowshedError::integrity(
        format!(
            "could not inspect the inherited mbx target link {}: {source}",
            path.display()
        ),
        "check the workspace mount is readable, then retry",
    )
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use super::*;

    struct Fixture {
        tree: PathBuf,
        managed: PathBuf,
        elsewhere: PathBuf,
    }

    fn fixture() -> Fixture {
        let scratch = std::env::temp_dir().join(format!(
            "cowshed-managed-targets-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&scratch).unwrap();
        let root = scratch.canonicalize().unwrap();
        let tree = root.join("checkout");
        let managed = managed_root(&root.join("build-cache"));
        let elsewhere = root.join("elsewhere");
        for directory in [&tree, &managed, &elsewhere] {
            fs::create_dir_all(directory).unwrap();
        }
        Fixture {
            tree,
            managed,
            elsewhere,
        }
    }

    /// A checkout digest as mbx spells one.
    fn digest(seed: u8) -> String {
        format!("{seed:064x}")
    }
    fn rel(path: &str) -> RelPath {
        RelPath::new(path).unwrap()
    }

    /// The source's managed target of one workspace, which the source's retirement removes.
    fn source_target(fixture: &Fixture, digest: &str) -> PathBuf {
        let target = fixture.managed.join("v1").join(digest);
        fs::create_dir_all(&target).unwrap();
        target
    }

    #[test]
    fn drops_the_root_and_nested_links_to_managed_targets_and_never_their_payload() {
        let fixture = fixture();
        let root = source_target(&fixture, &digest(1));
        let nested = source_target(&fixture, &digest(2));
        fs::create_dir_all(fixture.tree.join("packages/fixture")).unwrap();
        symlink(&root, fixture.tree.join("target")).unwrap();
        symlink(&nested, fixture.tree.join("packages/fixture/target")).unwrap();
        let dropped = drop_inherited(
            &fixture.tree,
            &[rel("target"), rel("packages/fixture/target")],
            &fixture.managed,
        )
        .unwrap();
        assert_eq!(
            dropped,
            [
                PathBuf::from("target"),
                PathBuf::from("packages/fixture/target")
            ]
        );
        assert!(fs::symlink_metadata(fixture.tree.join("target")).is_err());
        assert!(fs::symlink_metadata(fixture.tree.join("packages/fixture/target")).is_err());
        assert!(root.is_dir() && nested.is_dir(), "the managed targets stay");
    }

    #[test]
    fn drops_a_dangling_link_the_sources_retirement_left() {
        let fixture = fixture();
        let gone = fixture.managed.join("v1").join(digest(3));
        fs::create_dir_all(fixture.tree.join("fuzz")).unwrap();
        symlink(&gone, fixture.tree.join("fuzz/target")).unwrap();
        let dropped =
            drop_inherited(&fixture.tree, &[rel("fuzz/target")], &fixture.managed).unwrap();
        assert_eq!(dropped, [PathBuf::from("fuzz/target")]);
    }

    #[test]
    fn leaves_real_directories_foreign_links_and_escapes() {
        let fixture = fixture();
        fs::create_dir_all(fixture.tree.join("real/target/debug")).unwrap();
        fs::create_dir_all(fixture.tree.join("foreign")).unwrap();
        symlink(&fixture.elsewhere, fixture.tree.join("foreign/target")).unwrap();
        fs::create_dir_all(fixture.tree.join("relative")).unwrap();
        symlink("../real/target", fixture.tree.join("relative/target")).unwrap();
        // Spelled to pass through the managed root and leave it again.
        fs::create_dir_all(fixture.tree.join("escape")).unwrap();
        symlink(
            fixture.managed.join("v1/../../../elsewhere"),
            fixture.tree.join("escape/target"),
        )
        .unwrap();
        // Neither the managed root, nor v1 itself, nor a path below one target is a managed target.
        fs::create_dir_all(fixture.tree.join("root")).unwrap();
        symlink(&fixture.managed, fixture.tree.join("root/target")).unwrap();
        fs::create_dir_all(fixture.tree.join("versioned")).unwrap();
        symlink(
            fixture.managed.join("v1"),
            fixture.tree.join("versioned/target"),
        )
        .unwrap();
        fs::create_dir_all(fixture.tree.join("named")).unwrap();
        symlink(
            fixture.managed.join("v1/not-a-checkout-digest"),
            fixture.tree.join("named/target"),
        )
        .unwrap();
        fs::create_dir_all(fixture.tree.join("deeper")).unwrap();
        symlink(
            fixture.managed.join("v1").join(digest(4)).join("debug"),
            fixture.tree.join("deeper/target"),
        )
        .unwrap();
        let candidates = [
            rel("real/target"),
            rel("foreign/target"),
            rel("relative/target"),
            rel("escape/target"),
            rel("root/target"),
            rel("versioned/target"),
            rel("deeper/target"),
            rel("named/target"),
            rel("absent/target"),
        ];
        let dropped = drop_inherited(&fixture.tree, &candidates, &fixture.managed).unwrap();
        assert!(dropped.is_empty(), "{dropped:?}");
        assert!(fixture.tree.join("real/target/debug").is_dir());
        for kept in [
            "foreign/target",
            "relative/target",
            "escape/target",
            "root/target",
            "versioned/target",
            "deeper/target",
            "named/target",
        ] {
            assert!(
                fs::symlink_metadata(fixture.tree.join(kept))
                    .unwrap()
                    .is_symlink(),
                "{kept}"
            );
        }
    }

    /// Adoption's guarded root cleanup: a known managed link at the root goes; a normal `target`
    /// directory and a link naming anything else stay.
    #[test]
    fn adoptions_root_cleanup_drops_only_a_managed_link() {
        let root = [rel("target")];
        let managed = fixture();
        let target = source_target(&managed, &digest(5));
        symlink(&target, managed.tree.join("target")).unwrap();
        assert_eq!(
            drop_inherited(&managed.tree, &root, &managed.managed).unwrap(),
            [PathBuf::from("target")]
        );
        assert!(target.is_dir(), "the managed payload stays");

        let normal = fixture();
        fs::create_dir_all(normal.tree.join("target/debug")).unwrap();
        assert!(
            drop_inherited(&normal.tree, &root, &normal.managed)
                .unwrap()
                .is_empty()
        );
        assert!(normal.tree.join("target/debug").is_dir());

        let foreign = fixture();
        symlink(&foreign.elsewhere, foreign.tree.join("target")).unwrap();
        assert!(
            drop_inherited(&foreign.tree, &root, &foreign.managed)
                .unwrap()
                .is_empty()
        );
        assert!(
            fs::symlink_metadata(foreign.tree.join("target"))
                .unwrap()
                .is_symlink()
        );
    }

    #[test]
    fn never_unlinks_through_a_symlinked_ancestor() {
        let fixture = fixture();
        let target = source_target(&fixture, &digest(6));
        // `linked` is a link to a directory outside the clone that holds a managed-target link.
        let outside = fixture.elsewhere.join("crate");
        fs::create_dir_all(&outside).unwrap();
        symlink(&target, outside.join("target")).unwrap();
        symlink(&outside, fixture.tree.join("linked")).unwrap();
        let dropped =
            drop_inherited(&fixture.tree, &[rel("linked/target")], &fixture.managed).unwrap();
        assert!(dropped.is_empty());
        assert!(
            fs::symlink_metadata(outside.join("target"))
                .unwrap()
                .is_symlink()
        );
    }
}
