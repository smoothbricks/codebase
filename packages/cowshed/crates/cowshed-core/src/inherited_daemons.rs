//! Drop the daemon rendezvous state a new tree inherited from the tree that produced it.
//!
//! A workspace is materialized by cloning one image, so a build daemon's private directory
//! arrives byte-identical — including the file that says *where that daemon is*. Nx writes
//! `.nx/workspace-data/d/server-process.json` naming the pid and socket of the server that owns
//! the workspace it was started in; a clone that carries it points every `nx` invocation in the
//! new tree at the daemon still serving the old one, which then answers about a workspace root
//! that is not the caller's. The failure surfaces as a workspace mismatch rather than as
//! anything that names the copied file, and the same directory is where that daemon's log
//! accumulates: 563MB of it in one repository measured here, cloned into every workspace.
//!
//! This is the same class of repair as an inherited Git remote or an escaping symlink
//! ([`crate::inherited_links`]) and it runs in the same place, at mint, where nothing in the
//! tree is the user's yet: generated state that encodes the *source tree's position* — its URLs,
//! its depth, its running processes — is not content, and a clone that keeps it is wrong in a
//! way the user cannot see.
//!
//! The rule is deliberately narrow, and narrower than "clear the caches". A cache is exactly
//! what a copy-on-write workspace is for: the project graph, file map and hash databases beside
//! this directory are warm, correct at any path, and re-earning them costs the seconds the
//! clone exists to save. Only the daemon's own rendezvous directory goes.

use std::fs;
use std::io;
use std::path::Path;

use crate::error::{CowshedError, Result};

/// Repository-relative directories whose contents name a *running daemon* rather than data.
///
/// One entry per daemon that rendezvouses through a file in the tree. Anything a fresh daemon
/// regenerates from the tree belongs here; anything that would have to be recomputed from
/// sources does not.
const INHERITED_DAEMON_STATE: &[&str] = &[".nx/workspace-data/d"];

/// Discard every inherited daemon rendezvous directory in `tree_root`, reporting what went.
///
/// Idempotent: an entry that is absent — a tree that never ran the daemon, or one already
/// minted — is the state this establishes, so it is not a finding and not an error.
pub fn discard(tree_root: &Path) -> Result<Vec<&'static str>> {
    let mut discarded = Vec::new();
    for state in INHERITED_DAEMON_STATE {
        if discard_one(tree_root, state)? {
            discarded.push(*state);
        }
    }
    Ok(discarded)
}

/// [`discard`] for the async mint path. The walk is a handful of `lstat` calls plus one
/// removal, but the removal can be a large directory, so it does not run on the reactor.
pub async fn discard_in(tree_root: &Path) -> Result<Vec<&'static str>> {
    let root = tree_root.to_path_buf();
    tokio::task::spawn_blocking(move || discard(&root))
        .await
        .map_err(|source| {
            CowshedError::integrity(
                format!("discarding inherited daemon state panicked: {source}"),
                "retry the operation and report the failure if it repeats",
            )
        })?
}

/// `Ok(true)` when this entry was present and is now gone.
fn discard_one(tree_root: &Path, state: &str) -> Result<bool> {
    let mut path = tree_root.to_path_buf();
    let mut components = Path::new(state).components().peekable();
    while let Some(component) = components.next() {
        path.push(component);
        // Top-down, one component at a time, because `symlink_metadata` declines to follow only
        // its FINAL component: asked for the whole path at once it would have resolved a
        // symlinked `.nx` on the way in and reported on a directory in another tree.
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(source) => {
                return Err(CowshedError::integrity(
                    format!(
                        "could not inspect inherited daemon state at {}: {source}",
                        path.display()
                    ),
                    "check the workspace mount is readable, then retry",
                ));
            }
        };
        let leaf = components.peek().is_none();
        if metadata.is_symlink() {
            if !leaf {
                // Removing anything under here would delete from whatever tree the link names.
                return Err(CowshedError::integrity(
                    format!(
                        "{} is a symlink, so the inherited daemon state {state} cannot be dropped without writing outside this workspace",
                        path.display()
                    ),
                    "replace the symlink with a real directory in the source checkout, then retry",
                ));
            }
            // The entry itself is a link. Unlink the name and never touch what it names: the
            // clone must stop reaching a foreign daemon, and the target is not cowshed's.
            return remove(&path, false).map(|()| true);
        }
        if leaf {
            return remove(&path, metadata.is_dir()).map(|()| true);
        }
    }
    // Only an entry that names no component at all, which the table has none of and a test
    // holds it to. Nothing was named, so nothing was discarded.
    Ok(false)
}

fn remove(path: &Path, directory: bool) -> Result<()> {
    let removed = if directory {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };
    removed.map_err(|source| {
        CowshedError::integrity(
            format!(
                "could not drop the inherited daemon state {}: {source}",
                path.display()
            ),
            "check the workspace mount is writable, then retry",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tree(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-inherited-daemons-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).expect("tree root");
        root
    }

    /// A clone as the substrate hands it over: the daemon's rendezvous directory, and beside it
    /// the warm state that is the reason to clone at all.
    fn nx_workspace(root: &Path) {
        let data = root.join(".nx/workspace-data");
        fs::create_dir_all(data.join("d")).expect("daemon directory");
        fs::write(data.join("d/server-process.json"), b"{\"processId\":4242}")
            .expect("server process");
        fs::write(data.join("d/daemon.log"), b"the host daemon's log").expect("daemon log");
        fs::write(data.join("file-map.json"), b"{}").expect("file map");
        fs::write(data.join("project-graph.db"), b"graph").expect("graph database");
        fs::create_dir_all(root.join(".nx/cache/1234")).expect("task cache");
        fs::write(root.join(".nx/cache/1234/terminalOutput"), b"cached").expect("cached output");
        fs::create_dir_all(root.join("packages/app/src")).expect("source directory");
        fs::write(root.join("packages/app/src/main.ts"), b"export {};").expect("source file");
    }

    /// The daemon directory goes and nothing else does. The neighbours are the assertion that
    /// matters: they are what makes a clone warm, and clearing them would trade one bug for a
    /// slower workspace.
    #[test]
    fn a_mint_drops_the_daemon_directory_and_keeps_the_warm_cache_beside_it() {
        let root = tree("scope");
        nx_workspace(&root);

        assert_eq!(
            discard(&root).expect("discard inherited daemon state"),
            [".nx/workspace-data/d"]
        );

        assert!(
            !root.join(".nx/workspace-data/d").exists(),
            "the daemon rendezvous directory must be gone"
        );
        for kept in [
            ".nx/workspace-data/file-map.json",
            ".nx/workspace-data/project-graph.db",
            ".nx/cache/1234/terminalOutput",
            "packages/app/src/main.ts",
        ] {
            assert!(root.join(kept).exists(), "{kept} must survive the mint");
        }
        assert!(
            root.join(".nx/workspace-data").is_dir(),
            "the daemon's parent directory is not the daemon's to take with it"
        );

        fs::remove_dir_all(&root).ok();
    }

    /// Every mint runs this, and the overwhelming majority of trees have no daemon state at all.
    /// Absence is the goal state, so it reports nothing rather than failing.
    #[test]
    fn a_tree_that_never_ran_the_daemon_is_untouched_and_reports_nothing() {
        let root = tree("absent");
        fs::create_dir_all(root.join(".nx/cache")).expect("cache only");

        assert!(
            discard(&root)
                .expect("an absent entry is not a failure")
                .is_empty()
        );
        assert!(root.join(".nx/cache").is_dir());

        // And again on a tree that has just been cleaned: minting is retried after a crash.
        let cleaned = tree("cleaned");
        nx_workspace(&cleaned);
        assert_eq!(discard(&cleaned).expect("first mint").len(), 1);
        assert!(discard(&cleaned).expect("retried mint").is_empty());

        fs::remove_dir_all(&root).ok();
        fs::remove_dir_all(&cleaned).ok();
    }

    /// A symlinked ancestor is the one shape where dropping the entry means writing into a tree
    /// cowshed was not handed. It refuses by name instead, and proves it never followed the link
    /// by checking the pointed-at directory is still whole.
    #[test]
    fn a_symlinked_ancestor_is_refused_rather_than_followed_out_of_the_tree() {
        let root = tree("escape");
        let outside = tree("escape-target");
        fs::create_dir_all(outside.join("workspace-data/d")).expect("outside daemon directory");
        fs::write(outside.join("workspace-data/d/server-process.json"), b"{}")
            .expect("outside server process");
        std::os::unix::fs::symlink(&outside, root.join(".nx")).expect("symlinked .nx");

        let error = discard(&root).expect_err("an escaping ancestor must refuse");
        assert_eq!(error.code.as_str(), "integrity");
        assert!(
            error.message.contains(".nx") && error.message.contains("symlink"),
            "the refusal names the link: {}",
            error.message
        );
        assert!(
            outside
                .join("workspace-data/d/server-process.json")
                .exists(),
            "nothing outside the workspace may be removed"
        );

        fs::remove_dir_all(&root).ok();
        fs::remove_dir_all(&outside).ok();
    }

    /// The entry itself being a link is not an escape: unlinking the name reaches no other tree,
    /// and leaving it would keep the clone pointed at a daemon it does not own.
    #[test]
    fn a_symlinked_entry_is_unlinked_without_touching_its_target() {
        let root = tree("leaf-link");
        let outside = tree("leaf-link-target");
        fs::write(outside.join("server-process.json"), b"{}").expect("outside server process");
        fs::create_dir_all(root.join(".nx/workspace-data")).expect("workspace data");
        std::os::unix::fs::symlink(&outside, root.join(".nx/workspace-data/d"))
            .expect("symlinked entry");

        assert_eq!(
            discard(&root).expect("discard the link"),
            [".nx/workspace-data/d"]
        );
        assert!(
            fs::symlink_metadata(root.join(".nx/workspace-data/d")).is_err(),
            "the link must be gone"
        );
        assert!(
            outside.join("server-process.json").exists(),
            "the link's target belongs to whoever made it"
        );

        fs::remove_dir_all(&root).ok();
        fs::remove_dir_all(&outside).ok();
    }

    /// The walk's leaf handling is derived from the table, so an entry that named nothing would
    /// silently do nothing. There is no such entry, and this is what keeps it that way.
    #[test]
    fn every_table_entry_names_a_relative_path() {
        for state in INHERITED_DAEMON_STATE {
            let path = Path::new(state);
            assert!(
                path.components().count() > 0,
                "{state} must name at least one component"
            );
            assert!(
                path.is_relative(),
                "{state} must be repository-relative, never absolute"
            );
            assert!(
                path.components()
                    .all(|component| matches!(component, std::path::Component::Normal(_))),
                "{state} must not climb or re-root"
            );
        }
    }
}
