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

/// What the filesystem holds at `<directory>/cowshed`, as the shell observed it. Every field is a
/// value, so the decisions made over these are pure; the shell asks `reaches` once per link, so no
/// decision touches the disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Observed {
    /// Nothing there, or nothing that could be read.
    Absent,
    /// A symbolic link. `text` is what it says, or `None` when it cannot be read, and such an
    /// entry is reported as a program, as it always was. `reaches` is whether it ends at the
    /// installed copy.
    Link {
        text: Option<PathBuf>,
        reaches: bool,
    },
    /// A regular file, which a shell runs only when it is executable.
    File { executable: bool },
    /// A directory, or anything else a shell cannot run by that name.
    Other,
}

/// One directory of a `PATH`, with what it holds under the name a shell looks for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate {
    pub directory: PathBuf,
    pub observed: Observed,
}

/// What pointing the `PATH`'s `cowshed` at the installed copy will do. [`plan_point_at`] decides it
/// from values; [`point_at`] is the only caller that executes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PointPlan {
    /// No directory of the `PATH` holds a `cowshed` a shell would run.
    Missing,
    /// The entry is a program rather than a link, so it is never replaced.
    NotALink { entry: PathBuf },
    /// The entry already reaches the installed copy, and is left as it is.
    AlreadyInstalled { entry: PathBuf },
    /// The entry is a link saying `was`, and it is to be repointed at the installed copy.
    Repoint { entry: PathBuf, was: PathBuf },
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

/// Decide what pointing the first `cowshed` on a `PATH` at the installed copy does. The first
/// directory that holds a `cowshed` a shell would run is the entry. A relative directory names the
/// current directory, which is nobody's install, so it is skipped; so is a directory that holds a
/// directory or a file that cannot run by that name.
pub fn plan_point_at(candidates: Vec<Candidate>) -> PointPlan {
    for Candidate {
        directory,
        observed,
    } in candidates
    {
        if !directory.is_absolute() {
            continue;
        }
        let entry = directory.join(ENTRY_NAME);
        match observed {
            Observed::Absent | Observed::Other | Observed::File { executable: false } => {}
            Observed::File { executable: true } | Observed::Link { text: None, .. } => {
                return PointPlan::NotALink { entry };
            }
            Observed::Link {
                text: Some(_),
                reaches: true,
            } => {
                return PointPlan::AlreadyInstalled { entry };
            }
            Observed::Link {
                text: Some(was),
                reaches: false,
            } => {
                return PointPlan::Repoint { entry, was };
            }
        }
    }
    PointPlan::Missing
}

/// The entries [`remove_pointing_at`] removes: each `cowshed` in an absolute directory of a `PATH`
/// that is a link saying exactly `stable`, named once however many times its directory is listed.
pub fn removal_plan(candidates: Vec<Candidate>, stable: &Path) -> Vec<PathBuf> {
    let mut removed: Vec<PathBuf> = Vec::new();
    for Candidate {
        directory,
        observed,
    } in candidates
    {
        if !directory.is_absolute() {
            continue;
        }
        let entry = directory.join(ENTRY_NAME);
        if removed.contains(&entry) {
            continue;
        }
        let says_stable = matches!(
            &observed,
            Observed::Link { text: Some(text), .. } if text.as_path() == stable
        );
        if says_stable {
            removed.push(entry);
        }
    }
    removed
}

/// Point the first `cowshed` on `path` at `stable`, unless it already reaches it.
///
/// Reaching is judged by where each ends, not by what the link says, so an entry the operator
/// chained through their own links to the installed copy is left alone.
pub fn point_at(path: &OsStr, stable: &Path) -> PathEntryOutcome {
    match plan_point_at(observe(path, stable)) {
        PointPlan::Missing => PathEntryOutcome::Missing,
        PointPlan::NotALink { entry } => PathEntryOutcome::NotALink { entry },
        PointPlan::AlreadyInstalled { entry } => PathEntryOutcome::AlreadyInstalled { entry },
        PointPlan::Repoint { entry, was } => match replace_symlink(stable, &entry) {
            Ok(()) => PathEntryOutcome::Repointed { entry, was },
            Err(error) => PathEntryOutcome::Failed {
                entry,
                reason: error.to_string(),
            },
        },
    }
}

/// Remove every `cowshed` in a directory of `path` that is a link saying exactly `stable`: the
/// entries [`point_at`] made, and no other link that happens to arrive there. Answers the
/// entries it removed.
pub fn remove_pointing_at(path: &OsStr, stable: &Path) -> io::Result<Vec<PathBuf>> {
    let removed = removal_plan(observe(path, stable), stable);
    for entry in &removed {
        fs::remove_file(entry)?;
    }
    Ok(removed)
}

/// What the shell sees of every directory of `path`, in `PATH` order. This is the only place the
/// decisions above touch the disk. `reaches` is asked here, once per link, because it is the one
/// question `symlink_metadata` cannot answer.
fn observe(path: &OsStr, stable: &Path) -> Vec<Candidate> {
    std::env::split_paths(path)
        .map(|directory| {
            let observed = observe_entry(&directory.join(ENTRY_NAME), stable);
            Candidate {
                directory,
                observed,
            }
        })
        .collect()
}

fn observe_entry(entry: &Path, stable: &Path) -> Observed {
    let Ok(metadata) = fs::symlink_metadata(entry) else {
        return Observed::Absent;
    };
    if metadata.file_type().is_symlink() {
        return Observed::Link {
            text: fs::read_link(entry).ok(),
            reaches: reaches(entry, stable),
        };
    }
    if metadata.is_file() {
        return Observed::File {
            executable: metadata.permissions().mode() & 0o111 != 0,
        };
    }
    Observed::Other
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

        /// The installed copy's stable name: a link to a stored copy, as `setup` leaves it. The
        /// stored copy is an inert placeholder, because the tests judge where links end and never
        /// run what they reach.
        fn stable(&self) -> PathBuf {
            let bin = self.directory("bin");
            fs::write(bin.join("cowshed-stored"), b"installed").expect("stored copy");
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

    /// A directory of a `PATH` and what it holds, as values: the pure decisions take nothing else.
    fn candidate(directory: &str, observed: Observed) -> Candidate {
        Candidate {
            directory: PathBuf::from(directory),
            observed,
        }
    }

    /// A link saying `text`, with `reaches` as the shell would have precomputed it.
    fn link(text: impl Into<PathBuf>, reaches: bool) -> Observed {
        Observed::Link {
            text: Some(text.into()),
            reaches,
        }
    }

    /// The incident: `bun link` left `~/.bun/bin/cowshed` pointing into a checkout, so every
    /// rebuild there changed the CLI under a daemon that still ran the installed build. The entry
    /// is repointed at the installed copy, and once it reaches that copy a second run finds nothing
    /// to do.
    #[test]
    fn a_link_into_a_checkout_is_repointed_at_the_installed_copy() {
        let checkout = "/home/dev/checkout/packages/cowshed/bin/cowshed";
        assert_eq!(
            plan_point_at(vec![candidate("/home/dev/.bun/bin", link(checkout, false))]),
            PointPlan::Repoint {
                entry: PathBuf::from("/home/dev/.bun/bin/cowshed"),
                was: PathBuf::from(checkout),
            }
        );
        assert_eq!(
            plan_point_at(vec![candidate(
                "/home/dev/.bun/bin",
                link("/home/dev/installed/cowshed", true),
            )]),
            PointPlan::AlreadyInstalled {
                entry: PathBuf::from("/home/dev/.bun/bin/cowshed"),
            }
        );
    }

    #[test]
    fn only_the_first_cowshed_on_the_path_is_the_entry() {
        assert_eq!(
            plan_point_at(vec![
                candidate("/first", link("/nowhere/one", false)),
                candidate("/second", link("/nowhere/two", false)),
            ]),
            PointPlan::Repoint {
                entry: PathBuf::from("/first/cowshed"),
                was: PathBuf::from("/nowhere/one"),
            },
            "a shadowed entry is nobody's cowshed"
        );
        assert_eq!(
            plan_point_at(vec![
                candidate("/first", Observed::Absent),
                candidate("/second", link("/nowhere/two", false)),
            ]),
            PointPlan::Repoint {
                entry: PathBuf::from("/second/cowshed"),
                was: PathBuf::from("/nowhere/two"),
            },
            "a directory with no cowshed does not shadow the next"
        );
    }

    #[test]
    fn an_entry_chained_to_the_installed_copy_by_the_operator_is_left_alone() {
        assert_eq!(
            plan_point_at(vec![candidate(
                "/home/dev/local-bin",
                link("/home/dev/hop/cowshed", true),
            )]),
            PointPlan::AlreadyInstalled {
                entry: PathBuf::from("/home/dev/local-bin/cowshed"),
            }
        );
    }

    #[test]
    fn a_program_that_is_not_a_link_is_reported_and_never_replaced() {
        assert_eq!(
            plan_point_at(vec![candidate(
                "/home/dev/local-bin",
                Observed::File { executable: true },
            )]),
            PointPlan::NotALink {
                entry: PathBuf::from("/home/dev/local-bin/cowshed"),
            }
        );
    }

    /// A link whose text cannot be read is not one this decision can judge, so it is reported the
    /// way a program is: named, and never replaced.
    #[test]
    fn a_link_whose_text_cannot_be_read_is_reported_like_a_program() {
        assert_eq!(
            plan_point_at(vec![candidate(
                "/home/dev/local-bin",
                Observed::Link {
                    text: None,
                    reaches: false,
                },
            )]),
            PointPlan::NotALink {
                entry: PathBuf::from("/home/dev/local-bin/cowshed"),
            }
        );
    }

    #[test]
    fn a_path_without_a_cowshed_reports_none_and_skips_what_a_shell_would() {
        assert_eq!(
            plan_point_at(vec![
                candidate("/home/dev/empty", Observed::Absent),
                candidate("/home/dev/shadow", Observed::Other),
                candidate("/home/dev/unrunnable", Observed::File { executable: false }),
                candidate("relative/bin", Observed::File { executable: true }),
                candidate("relative/link", link("/nowhere", false)),
            ]),
            PointPlan::Missing
        );
    }

    /// A relative directory names the current directory, which is nobody's install: it is skipped,
    /// and it does not stop the scan from reaching the directories after it.
    #[test]
    fn a_relative_directory_is_skipped_not_taken_as_the_entry() {
        assert_eq!(
            plan_point_at(vec![
                candidate("relative/bin", Observed::File { executable: true }),
                candidate("/home/dev/.bun/bin", link("/nowhere", false)),
            ]),
            PointPlan::Repoint {
                entry: PathBuf::from("/home/dev/.bun/bin/cowshed"),
                was: PathBuf::from("/nowhere"),
            }
        );
    }

    #[test]
    fn removal_takes_only_the_links_that_say_the_installed_name() {
        let stable = Path::new("/home/dev/installed/cowshed");
        assert_eq!(
            removal_plan(
                vec![
                    candidate("/home/dev/ours", link(stable, true)),
                    candidate("/home/dev/theirs", link("/nowhere/else", false)),
                    candidate("/home/dev/program", Observed::File { executable: true }),
                    candidate("/home/dev/ours", link(stable, true)),
                ],
                stable,
            ),
            [PathBuf::from("/home/dev/ours/cowshed")]
        );
    }

    /// On disk, the repoint is one rename: the entry ends at the installed copy afterwards, a
    /// second run finds nothing to do, and no temporary link is left beside the entry.
    #[test]
    fn a_link_into_a_checkout_is_repointed_on_disk() {
        let scratch = Scratch::new("repoint");
        let stable = scratch.stable();
        let bun = scratch.directory("bun-bin");
        let checkout = scratch.directory("checkout/packages/cowshed/bin");
        fs::write(checkout.join("cowshed"), b"").unwrap();
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
            fs::canonicalize(bun.join("cowshed")).unwrap(),
            fs::canonicalize(&stable).unwrap(),
            "the entry ends at the installed copy, not the checkout's build"
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

    /// On disk, a chain of the operator's own links is judged by where it ends, and left as it is.
    #[test]
    fn an_entry_chained_to_the_installed_copy_is_left_alone_on_disk() {
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

    /// On disk, a real executable that is not a link is reported, and its bytes are left alone.
    #[test]
    fn a_program_that_is_not_a_link_is_never_replaced_on_disk() {
        let scratch = Scratch::new("program");
        let stable = scratch.stable();
        let local = scratch.directory("local-bin");
        fs::write(local.join("cowshed"), b"mine").unwrap();
        fs::set_permissions(local.join("cowshed"), fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(
            point_at(&scratch.path(&[&local]), &stable),
            PathEntryOutcome::NotALink {
                entry: local.join("cowshed")
            }
        );
        assert_eq!(fs::read(local.join("cowshed")).unwrap(), b"mine");
    }

    /// On disk, a directory named `cowshed` and a non-executable file are skipped as a shell skips
    /// them, so the path reports none.
    #[test]
    fn a_path_without_a_cowshed_reports_none_on_disk() {
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
        let path = scratch.path(&[&empty, &directory_named_cowshed, &unrunnable]);

        assert_eq!(point_at(&path, &stable), PathEntryOutcome::Missing);
    }

    /// On disk, removal deletes the link that names the installed copy and leaves the other one.
    #[test]
    fn removal_deletes_only_the_links_on_disk_that_say_the_installed_name() {
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
