//! Drop the daemon rendezvous state a new tree inherited from the tree that produced it.
//!
//! A workspace is materialized by cloning one image, so a build daemon's private directory
//! arrives byte-identical — including the file that says *where that daemon is*. Nx, for one,
//! writes `server-process.json` naming the pid and socket of the server that owns the workspace
//! it was started in; a clone that carries it points every client in the new tree at the daemon
//! still serving the old one, and the same directory is where that daemon's log accumulates. The
//! directories come from the detected capabilities' `daemon_isolation.discard_at_mint`
//! (15_capabilities.md): this module knows no tool, only how to drop a named directory safely.
//!
//! This is the same class of repair as an inherited Git remote or an escaping symlink
//! ([`crate::inherited_links`]) and it runs in the same place, at mint, where nothing in the
//! tree is the user's yet: generated state that encodes the *source tree's position* — its URLs,
//! its depth, its running processes — is not content, and a clone that keeps it is wrong in a
//! way the user cannot see.
//!
//! The rule is deliberately narrow, and narrower than "clear the caches". A cache is exactly
//! what a copy-on-write workspace is for: the project graph, file map and hash databases beside
//! these directories are warm, correct at any path, and re-earning them costs the seconds the
//! clone exists to save. Only the daemons' own rendezvous directories go.

use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use crate::error::{CowshedError, Result};

/// Discard every named inherited daemon rendezvous directory in `tree_root`. Each state is
/// workspace-relative and names at least one plain component; anything else is refused before
/// the tree is touched.
///
/// Idempotent: an entry that is absent — a tree that never ran the daemon, or one already
/// minted — is the state this establishes, so it is not a finding and not an error. Nothing is
/// reported on success because nothing consumes a report; what a caller needs to know is whether
/// the tree is now clean, and that is the `Ok`.
fn discard(tree_root: &Path, states: &[PathBuf]) -> Result<()> {
    if let Some(state) = states.iter().find(|state| {
        state.as_os_str().is_empty()
            || !state
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
    }) {
        return Err(CowshedError::integrity(
            format!(
                "inherited daemon state {} is not a plain workspace-relative path",
                state.display()
            ),
            "repair the capability contribution that names it",
        ));
    }
    for state in states {
        discard_one(tree_root, state)?;
    }
    Ok(())
}

/// [`discard`] for the async mint path. The walk is a handful of `lstat` calls plus one
/// removal per state, but a removal can be a large directory, so it does not run on the reactor.
pub(crate) async fn discard_in(tree_root: &Path, states: Vec<PathBuf>) -> Result<()> {
    let root = tree_root.to_path_buf();
    tokio::task::spawn_blocking(move || discard(&root, &states))
        .await
        .map_err(|source| {
            CowshedError::integrity(
                format!("discarding inherited daemon state panicked: {source}"),
                "retry the operation and report the failure if it repeats",
            )
        })?
}

fn discard_one(tree_root: &Path, state: &Path) -> Result<()> {
    let mut path = tree_root.to_path_buf();
    let mut components = state.components().peekable();
    while let Some(component) = components.next() {
        path.push(component);
        // Top-down, one component at a time, because `symlink_metadata` declines to follow only
        // its FINAL component: asked for the whole path at once it would have resolved a
        // symlinked `.nx` on the way in and reported on a directory in another tree.
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
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
                        "{} is a symlink, so the inherited daemon state {} cannot be dropped without writing outside this workspace",
                        path.display(),
                        state.display()
                    ),
                    "replace the symlink with a real directory in the source checkout, then retry",
                ));
            }
            // The entry itself is a link: a NAME, not content. Unlink it and never touch what it
            // names — the clone must stop reaching a foreign daemon, and the target is not
            // cowshed's to delete.
            return unlink(&path, false);
        }
        if !leaf {
            continue;
        }
        if metadata.is_dir() {
            return unlink(&path, true);
        }
        // A regular file at a path a daemon owns as a directory is not that daemon's state, and
        // this module knows nothing about what it is instead. It may be tracked content. Refused
        // rather than removed: deleting a file to satisfy a guess is the failure being avoided.
        return Err(CowshedError::integrity(
            format!(
                "{} is a file, not the daemon directory the inherited state {} names",
                path.display(),
                state.display()
            ),
            "move or remove that file in the source checkout if it is not wanted, then retry",
        ));
    }
    // Only an entry naming no component at all, which `discard` refuses before it gets here.
    Ok(())
}

fn unlink(path: &Path, directory: bool) -> Result<()> {
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

    /// The Nx capability's rendezvous directory (`capabilities::nx`): one per checkout, shared by
    /// every boundary.
    fn nx_states() -> Vec<PathBuf> {
        vec![PathBuf::from(".nx/workspace-data/d")]
    }

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

        discard(&root, &nx_states()).expect("discard inherited daemon state");

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
    /// Absence is the goal state, so it succeeds and changes nothing.
    #[test]
    fn a_tree_that_never_ran_the_daemon_is_untouched() {
        let root = tree("absent");
        fs::create_dir_all(root.join(".nx/cache")).expect("cache only");

        discard(&root, &nx_states()).expect("an absent entry is not a failure");
        assert!(root.join(".nx/cache").is_dir());

        // And again on a tree that has just been cleaned: minting is retried after a crash.
        let cleaned = tree("cleaned");
        nx_workspace(&cleaned);
        discard(&cleaned, &nx_states()).expect("first mint");
        discard(&cleaned, &nx_states()).expect("retried mint");
        assert!(!cleaned.join(".nx/workspace-data/d").exists());
        assert!(cleaned.join(".nx/workspace-data/file-map.json").exists());

        fs::remove_dir_all(&root).ok();
        fs::remove_dir_all(&cleaned).ok();
    }

    /// Nx owns that path as a directory. A regular file there is something else — possibly
    /// tracked content — and this module has no basis for deleting it.
    #[test]
    fn a_regular_file_at_the_entry_is_refused_and_left_exactly_as_it_was() {
        let root = tree("file-leaf");
        fs::create_dir_all(root.join(".nx/workspace-data")).expect("workspace data");
        let entry = root.join(".nx/workspace-data/d");
        fs::write(&entry, b"someone's file").expect("regular file at the entry");

        let error = discard(&root, &nx_states()).expect_err("a regular file must refuse");
        assert_eq!(error.code.as_str(), "integrity");
        assert!(
            error.message.contains("is a file"),
            "the refusal says what it found: {}",
            error.message
        );
        assert_eq!(
            fs::read(&entry).expect("the file must still be there"),
            b"someone's file"
        );

        fs::remove_dir_all(&root).ok();
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

        let error = discard(&root, &nx_states()).expect_err("an escaping ancestor must refuse");
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

        discard(&root, &nx_states()).expect("discard the link");
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

    /// A contributed state that names nothing, climbs or re-roots is refused before anything in
    /// the tree is touched: the walk would otherwise silently do nothing or leave the workspace.
    #[test]
    fn a_state_that_is_not_plain_and_relative_is_refused_before_the_walk() {
        let root = tree("invalid-state");
        nx_workspace(&root);
        for state in ["", "/tmp/elsewhere", "../sibling/d", ".nx/../d"] {
            let mut states = nx_states();
            states.push(PathBuf::from(state));
            let error = discard(&root, &states).expect_err("an invalid state must refuse");
            assert_eq!(error.code.as_str(), "integrity", "{state:?}");
            assert!(
                root.join(".nx/workspace-data/d/server-process.json")
                    .exists(),
                "{state:?} was refused only after the walk touched the tree"
            );
        }
        fs::remove_dir_all(&root).ok();
    }
}
