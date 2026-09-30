//! Drop the Git lock files a new tree inherited from the tree it was cloned from.
//!
//! Git serializes every write to a repository file through `<file>.lock`: the writer creates the
//! lock exclusively beside the file, writes the new contents into it, and renames it over the
//! file to commit. A clone taken while a writer in the source is between those two steps carries
//! the lock, but no process in the new tree ever held it — the writer is writing to the source,
//! and its rename lands there. Git reads an existing lock as a writer in progress, so every later
//! write to that file in the clone fails (`could not lock config file .git/config`) until someone
//! deletes the lock by hand. The window is not rare: a checkout's config and index are rewritten
//! by shell-entry hooks and by `git status`, and a busy source is cloned while it is in use.
//!
//! The same class as inherited daemon state ([`crate::inherited_daemons`]) and it runs in the same
//! place, at mint, before any Git command runs in the new tree: a lock names a running process in
//! the source tree, not content.
//!
//! Only the tree's own repository directory is walked. A `.git` that is a file (a linked worktree
//! or a submodule) or a symlink names a Git directory in another tree, whose locks may be held at
//! this moment by a writer that is still running.

use std::fs;
use std::io;
use std::path::Path;

use crate::error::{CowshedError, Result};

/// The suffix Git's lockfile convention appends to the file a write replaces.
const LOCK_SUFFIX: &str = ".lock";

/// Discard every Git lock file in `tree_root`'s own repository directory.
///
/// Idempotent: a tree with no `.git`, or with no locks, is the state this establishes.
fn discard(tree_root: &Path) -> Result<()> {
    let git_dir = tree_root.join(".git");
    let metadata = match fs::symlink_metadata(&git_dir) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(inspect_error(&git_dir, &source)),
    };
    // `symlink_metadata` reports a symlinked `.git` as a link, never as the directory it names.
    if !metadata.is_dir() {
        return Ok(());
    }
    let mut pending = vec![git_dir];
    while let Some(directory) = pending.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(source) if source.kind() == io::ErrorKind::NotFound => continue,
            Err(source) => return Err(inspect_error(&directory, &source)),
        };
        let loose_objects = directory.file_name().is_some_and(|name| name == "objects");
        for entry in entries {
            let entry = entry.map_err(|source| inspect_error(&directory, &source))?;
            let file_type = entry
                .file_type()
                .map_err(|source| inspect_error(&entry.path(), &source))?;
            let name = entry.file_name();
            if file_type.is_dir() {
                // Loose objects are written through temporary files and renamed into place, never
                // through a lock, and their fan-out directories are most of a repository's
                // entries.
                if !(loose_objects && is_object_fan_out(&name)) {
                    pending.push(entry.path());
                }
                continue;
            }
            // Any entry at a lock's name blocks the write it guards, a symlink included: Git
            // creates the lock exclusively. Removing the entry unlinks the name and never follows
            // it.
            if name.as_encoded_bytes().ends_with(LOCK_SUFFIX.as_bytes()) {
                let path = entry.path();
                match fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(CowshedError::integrity(
                            format!(
                                "could not drop the inherited Git lock {}: {source}",
                                path.display()
                            ),
                            "check the workspace mount is writable, then retry",
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// [`discard`] for the async mint path: a directory walk does not run on the reactor.
pub(crate) async fn discard_in(tree_root: &Path) -> Result<()> {
    let root = tree_root.to_path_buf();
    tokio::task::spawn_blocking(move || discard(&root))
        .await
        .map_err(|source| {
            CowshedError::integrity(
                format!("discarding inherited Git locks panicked: {source}"),
                "retry the operation and report the failure if it repeats",
            )
        })?
}

/// `objects/xx`: two lowercase hex digits, Git's loose-object fan-out.
fn is_object_fan_out(name: &std::ffi::OsStr) -> bool {
    let name = name.as_encoded_bytes();
    name.len() == 2
        && name
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}

fn inspect_error(path: &Path, source: &io::Error) -> CowshedError {
    CowshedError::integrity(
        format!(
            "could not inspect the inherited Git state at {}: {source}",
            path.display()
        ),
        "check the workspace mount is readable, then retry",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::process::Command;

    fn tree(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "cowshed-inherited-git-locks-{label}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).expect("tree root");
        root
    }

    fn git(root: &Path, args: &[&str]) -> std::process::Output {
        Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()
            .expect("run git")
    }

    fn repository(label: &str) -> PathBuf {
        let root = tree(label);
        assert!(
            git(&root, &["init", "--quiet", "--initial-branch=main"])
                .status
                .success()
        );
        assert!(
            git(&root, &["remote", "add", "backup", "/nowhere/backup.git"])
                .status
                .success()
        );
        root
    }

    fn plant(path: &Path) {
        fs::create_dir_all(path.parent().expect("lock parent")).expect("lock directory");
        fs::write(path, b"").expect("plant lock");
    }

    /// The failure a clone inherits, reproduced with Git itself: the planted lock is what Git
    /// honours, so the same removal that fails before the discard succeeds after it.
    #[test]
    fn a_clone_writes_the_config_a_source_writer_held_locked_when_it_was_cloned() {
        let root = repository("config");
        plant(&root.join(".git/config.lock"));
        let refused = git(&root, &["remote", "remove", "backup"]);
        assert!(
            !refused.status.success()
                && String::from_utf8_lossy(&refused.stderr).contains("could not lock config file"),
            "the fixture reproduces the inherited lock: {refused:?}"
        );

        discard(&root).expect("discard inherited Git locks");

        let removed = git(&root, &["remote", "remove", "backup"]);
        assert!(removed.status.success(), "{removed:?}");
        fs::remove_dir_all(&root).ok();
    }

    /// Every lock Git takes in the repository directory, wherever it sits, and nothing that is
    /// not one: a lockfile in the working tree is content, and the repository's other files are
    /// its state.
    #[test]
    fn every_git_lock_goes_and_nothing_else_does() {
        let root = repository("scope");
        let locks = [
            ".git/index.lock",
            ".git/HEAD.lock",
            ".git/packed-refs.lock",
            ".git/refs/heads/topic.lock",
            ".git/refs/remotes/backup/main.lock",
            ".git/objects/info/commit-graph.lock",
            ".git/objects/pack/multi-pack-index.lock",
            ".git/modules/vendored/config.lock",
            ".git/worktrees/side/index.lock",
        ];
        for lock in locks {
            plant(&root.join(lock));
        }
        let outside = tree("scope-outside");
        fs::write(outside.join("held.lock"), b"another tree's lock").expect("outside lock");
        std::os::unix::fs::symlink(outside.join("held.lock"), root.join(".git/config.lock"))
            .expect("symlinked lock");
        let working_tree = ["bun.lock", "target/debug/.cargo-lock", "vendor/cache.lock"];
        for file in working_tree {
            plant(&root.join(file));
        }
        plant(&root.join(".git/hooks/pre-commit"));
        let repository_state = [".git/config", ".git/HEAD", ".git/hooks/pre-commit"];

        discard(&root).expect("discard inherited Git locks");

        for lock in locks.iter().chain([&".git/config.lock"]) {
            assert!(
                fs::symlink_metadata(root.join(lock)).is_err(),
                "{lock} must be gone"
            );
        }
        for file in working_tree.iter().chain(&repository_state) {
            assert!(root.join(file).exists(), "{file} must survive the mint");
        }
        assert_eq!(
            fs::read(outside.join("held.lock")).expect("the link target is untouched"),
            b"another tree's lock"
        );
        fs::remove_dir_all(&root).ok();
        fs::remove_dir_all(&outside).ok();
    }

    /// A `.git` file or symlink names a Git directory in another tree — main's, for a linked
    /// worktree — whose writer may hold its lock right now. Neither is walked.
    #[test]
    fn a_git_directory_in_another_tree_keeps_its_live_locks() {
        let source = repository("source");
        plant(&source.join(".git/config.lock"));
        plant(&source.join(".git/worktrees/linked/index.lock"));

        let linked = tree("linked");
        fs::write(
            linked.join(".git"),
            format!(
                "gitdir: {}\n",
                source.join(".git/worktrees/linked").display()
            ),
        )
        .expect("gitfile");
        discard(&linked).expect("a gitfile tree has nothing of its own to discard");

        let symlinked = tree("symlinked");
        std::os::unix::fs::symlink(source.join(".git"), symlinked.join(".git"))
            .expect("symlinked .git");
        discard(&symlinked).expect("a symlinked .git is not walked");

        assert!(source.join(".git/config.lock").exists());
        assert!(source.join(".git/worktrees/linked/index.lock").exists());
        for root in [source, linked, symlinked] {
            fs::remove_dir_all(&root).ok();
        }
    }

    /// Most trees carry no lock, some carry no repository, and a mint is retried after a crash.
    #[test]
    fn a_tree_without_locks_or_without_a_repository_is_untouched() {
        let bare_tree = tree("no-repository");
        fs::write(bare_tree.join("notes.lock"), b"content").expect("tree file");
        discard(&bare_tree).expect("no .git is not a failure");
        assert!(bare_tree.join("notes.lock").exists());

        let root = repository("retried");
        plant(&root.join(".git/index.lock"));
        discard(&root).expect("first mint");
        discard(&root).expect("retried mint");
        assert!(!root.join(".git/index.lock").exists());
        assert!(root.join(".git/config").exists());
        fs::remove_dir_all(&bare_tree).ok();
        fs::remove_dir_all(&root).ok();
    }
}
