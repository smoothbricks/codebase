//! The `cowshed` an operator's shell runs, and pointing it at the installed copy
//! (05_gateway.md "The installed cowshed").
//!
//! The installed CLI and the gateway daemon are one artifact: the stable name under
//! `~/Library/Application Support/dev.cowshed/bin`, a link to a content-addressed copy that only
//! `cowshed setup` replaces. That holds for the `cowshed` on `PATH` only while it reaches that
//! name. Left as a link into a checkout — which is what `bun link` of the package, or a link
//! made by hand, leaves — every rebuild of the checkout changes the CLI under a daemon that still
//! runs the build `setup` installed, and the two refuse each other.
//!
//! So `setup` points the first `cowshed` on `PATH` at the stable name, when that entry is a
//! symbolic link. An entry that is not one — a script or binary somebody placed there — is not
//! ours to replace, and is reported with the command that would do it.

use crate::launchd::replace_symlink;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

/// The file name a shell looks for.
pub const ENTRY_NAME: &str = "cowshed";

/// The first `cowshed` that the directories of a `PATH` name, as a shell would reach it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PathEntry {
    /// No directory of the `PATH` holds a `cowshed`.
    Missing,
    /// A symbolic link; `text` is what it says.
    Link { entry: PathBuf, text: PathBuf },
    /// A program that is not a link.
    Program { entry: PathBuf },
}

/// What pointing the `PATH`'s `cowshed` at the installed copy did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PathEntryOutcome {
    /// The entry already reaches the installed copy, and was left as it is.
    AlreadyInstalled { entry: PathBuf },
    /// The entry was a link saying `was`; it now names the installed copy.
    Repointed { entry: PathBuf, was: PathBuf },
    /// The entry is a program rather than a link, so it was not replaced.
    NotALink { entry: PathBuf },
    /// No `cowshed` is on the `PATH`.
    Missing,
    /// The entry is a link, and replacing it failed.
    Failed { entry: PathBuf, reason: String },
}

/// The first `cowshed` on `path`. Relative directories are skipped — they name the current
/// directory, which is nobody's install — and so are directories that hold a directory or a file
/// that cannot run by that name.
pub fn find(path: &OsStr) -> PathEntry {
    for directory in std::env::split_paths(path) {
        if !directory.is_absolute() {
            continue;
        }
        let entry = directory.join(ENTRY_NAME);
        let Ok(metadata) = fs::symlink_metadata(&entry) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            return match fs::read_link(&entry) {
                Ok(text) => PathEntry::Link { entry, text },
                Err(_) => PathEntry::Program { entry },
            };
        }
        if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
            return PathEntry::Program { entry };
        }
    }
    PathEntry::Missing
}

/// Point the first `cowshed` on `path` at `stable`, unless it already reaches it.
///
/// Reaching is judged by where each ends, not by what the link says, so an entry the operator
/// chained through their own links to the installed copy is left alone.
pub fn point_at(path: &OsStr, stable: &Path) -> PathEntryOutcome {
    match find(path) {
        PathEntry::Missing => PathEntryOutcome::Missing,
        PathEntry::Program { entry } => PathEntryOutcome::NotALink { entry },
        PathEntry::Link { entry, text } => {
            if reaches(&entry, stable) {
                return PathEntryOutcome::AlreadyInstalled { entry };
            }
            match replace_symlink(stable, &entry) {
                Ok(()) => PathEntryOutcome::Repointed { entry, was: text },
                Err(error) => PathEntryOutcome::Failed {
                    entry,
                    reason: error.to_string(),
                },
            }
        }
    }
}

/// Remove every `cowshed` in a directory of `path` that is a link saying exactly `stable`: the
/// entries [`point_at`] made, and no other link that happens to arrive there. Answers the
/// entries it removed.
pub fn remove_pointing_at(path: &OsStr, stable: &Path) -> io::Result<Vec<PathBuf>> {
    let mut removed: Vec<PathBuf> = Vec::new();
    for directory in std::env::split_paths(path) {
        if !directory.is_absolute() {
            continue;
        }
        let entry = directory.join(ENTRY_NAME);
        if removed.contains(&entry) {
            continue;
        }
        let is_ours = fs::symlink_metadata(&entry)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
            && fs::read_link(&entry).is_ok_and(|text| text == stable);
        if is_ours {
            fs::remove_file(&entry)?;
            removed.push(entry);
        }
    }
    Ok(removed)
}

fn reaches(entry: &Path, stable: &Path) -> bool {
    match (fs::canonicalize(entry), fs::canonicalize(stable)) {
        (Ok(entry), Ok(stable)) => entry == stable,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "cowshed-path-entry-{label}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("scratch root");
            Self(root)
        }

        fn directory(&self, name: &str) -> PathBuf {
            let directory = self.0.join(name);
            fs::create_dir_all(&directory).expect("directory");
            directory
        }

        /// The installed copy's stable name: a link to a stored copy, as `setup` leaves it.
        fn stable(&self) -> PathBuf {
            let bin = self.directory("bin");
            fs::write(bin.join("cowshed-stored"), b"#!/bin/sh\n").expect("stored copy");
            let stable = bin.join("cowshed");
            std::os::unix::fs::symlink("cowshed-stored", &stable).expect("stable name");
            stable
        }

        fn path(&self, directories: &[&Path]) -> OsString {
            std::env::join_paths(directories).expect("a PATH")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// The incident: `bun link` left `~/.bun/bin/cowshed` pointing into a checkout, so every
    /// rebuild there changed the CLI under a daemon that still ran the installed build. The entry
    /// is repointed at the installed copy, and a second run finds nothing to do.
    #[test]
    fn a_link_into_a_checkout_is_repointed_at_the_installed_copy() {
        let scratch = Scratch::new("repoint");
        let stable = scratch.stable();
        let bun = scratch.directory("bun-bin");
        let checkout = scratch.directory("checkout/packages/cowshed/bin");
        fs::write(checkout.join("cowshed"), b"#!/bin/sh\necho checkout\n").unwrap();
        std::os::unix::fs::symlink(checkout.join("cowshed"), bun.join("cowshed")).unwrap();
        let path = scratch.path(&[&bun]);

        assert_eq!(
            point_at(&path, &stable),
            PathEntryOutcome::Repointed {
                entry: bun.join("cowshed"),
                was: checkout.join("cowshed"),
            }
        );
        assert_eq!(fs::read_link(bun.join("cowshed")).unwrap(), stable);
        assert_eq!(
            fs::read(bun.join("cowshed")).unwrap(),
            b"#!/bin/sh\n",
            "the entry runs the installed copy, not the checkout's trampoline"
        );
        assert_eq!(
            point_at(&path, &stable),
            PathEntryOutcome::AlreadyInstalled {
                entry: bun.join("cowshed")
            }
        );
        assert!(
            fs::read_dir(&bun)
                .unwrap()
                .all(|entry| entry.unwrap().file_name() == "cowshed"),
            "no temporary link is left beside the entry"
        );
    }

    #[test]
    fn only_the_first_cowshed_on_the_path_is_the_entry() {
        let scratch = Scratch::new("first");
        let stable = scratch.stable();
        let first = scratch.directory("first");
        let second = scratch.directory("second");
        std::os::unix::fs::symlink("/nowhere/one", first.join("cowshed")).unwrap();
        std::os::unix::fs::symlink("/nowhere/two", second.join("cowshed")).unwrap();

        point_at(&scratch.path(&[&first, &second]), &stable);

        assert_eq!(fs::read_link(first.join("cowshed")).unwrap(), stable);
        assert_eq!(
            fs::read_link(second.join("cowshed")).unwrap(),
            Path::new("/nowhere/two"),
            "a shadowed entry is nobody's cowshed"
        );
    }

    #[test]
    fn an_entry_chained_to_the_installed_copy_by_the_operator_is_left_alone() {
        let scratch = Scratch::new("chained");
        let stable = scratch.stable();
        let local = scratch.directory("local-bin");
        std::os::unix::fs::symlink(&stable, scratch.directory("hop").join("cowshed")).unwrap();
        std::os::unix::fs::symlink(scratch.0.join("hop/cowshed"), local.join("cowshed")).unwrap();

        assert_eq!(
            point_at(&scratch.path(&[&local]), &stable),
            PathEntryOutcome::AlreadyInstalled {
                entry: local.join("cowshed")
            }
        );
        assert_eq!(
            fs::read_link(local.join("cowshed")).unwrap(),
            scratch.0.join("hop/cowshed")
        );
    }

    #[test]
    fn a_program_that_is_not_a_link_is_reported_and_never_replaced() {
        let scratch = Scratch::new("program");
        let stable = scratch.stable();
        let local = scratch.directory("local-bin");
        fs::write(local.join("cowshed"), b"#!/bin/sh\necho mine\n").unwrap();
        fs::set_permissions(local.join("cowshed"), fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(
            point_at(&scratch.path(&[&local]), &stable),
            PathEntryOutcome::NotALink {
                entry: local.join("cowshed")
            }
        );
        assert_eq!(
            fs::read(local.join("cowshed")).unwrap(),
            b"#!/bin/sh\necho mine\n"
        );
    }

    #[test]
    fn a_path_without_a_cowshed_reports_none_and_skips_what_a_shell_would() {
        let scratch = Scratch::new("missing");
        let stable = scratch.stable();
        let empty = scratch.directory("empty");
        let directory_named_cowshed = scratch.directory("shadow");
        fs::create_dir_all(directory_named_cowshed.join("cowshed")).unwrap();
        let unrunnable = scratch.directory("unrunnable");
        fs::write(unrunnable.join("cowshed"), b"data").unwrap();
        fs::set_permissions(
            unrunnable.join("cowshed"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        let mut path = scratch.path(&[&empty, &directory_named_cowshed, &unrunnable]);
        path.push(":relative/bin");

        assert_eq!(point_at(&path, &stable), PathEntryOutcome::Missing);
    }

    #[test]
    fn removal_takes_only_the_links_that_say_the_installed_name() {
        let scratch = Scratch::new("remove");
        let stable = scratch.stable();
        let ours = scratch.directory("ours");
        let theirs = scratch.directory("theirs");
        std::os::unix::fs::symlink(&stable, ours.join("cowshed")).unwrap();
        std::os::unix::fs::symlink("/nowhere/else", theirs.join("cowshed")).unwrap();

        let removed = remove_pointing_at(&scratch.path(&[&ours, &theirs, &ours]), &stable).unwrap();

        assert_eq!(removed, [ours.join("cowshed")]);
        assert!(fs::symlink_metadata(ours.join("cowshed")).is_err());
        assert!(fs::symlink_metadata(theirs.join("cowshed")).is_ok());
    }
}
