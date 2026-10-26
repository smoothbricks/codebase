//! The throwaway signing keychain of a test (08_testing.md "Fixtures"). A keychain a test makes
//! can reach this user's search list: code_signing::prepare lists it, and security documents
//! create-keychain as doing the same. security delete-keychain takes exactly that keychain off
//! the list as it stands and deletes its file, so the list is never read, edited and written back
//! over what another tool added meanwhile. A test that makes a keychain owns this fixture before
//! its first security call; retire, which dropping runs too, deletes the keychain and then reads
//! the list and the disk again: it succeeds only when the list names the keychain no more and
//! neither its file nor its -db twin remains. A run killed outright leaves a scratch root named
//! for its creator's dead pid, and possibly its entry on the list; reclaim_dead removes both at
//! the next start, and touches nothing a live sibling owns or the user listed.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use cowshed_os::scratch::{Disposition, PASS_ENTRIES, Reaper, named_owner_for};
use cowshed_os::test_backstop::CommandBackstop as _;

use super::{Keychain, SECURITY};
use crate::temp_root::TempRoot;

/// A scratch root of a test, owned from its creation, and the keychain the test makes in it.
pub(crate) struct ThrowawayKeychain {
    keychain: Keychain,
    retired: bool,
    root: TempRoot,
}

impl ThrowawayKeychain {
    /// A fresh scratch root named for prefix (which has to start with cowshed-, as the sweep
    /// knows its roots) and owned by this process, and the keychain keychain_in places under it.
    /// Runs of this prefix that were killed earlier are reclaimed first. Nothing is created and
    /// nothing listed yet: the caller makes the keychain, through security, at keychain().
    pub(crate) fn new(prefix: &str, keychain_in: impl FnOnce(&Path) -> Keychain) -> Self {
        if let Err(error) = reclaim_dead(prefix) {
            panic!("reclaim the {prefix} keychains of runs killed earlier: {error}");
        }
        let parent = fs::canonicalize(std::env::temp_dir()).expect("canonical temporary directory");
        let root = TempRoot::new_in(&parent, prefix);
        let keychain = {
            let home: &Path = &root;
            keychain_in(home)
        };
        Self {
            keychain,
            retired: false,
            root,
        }
    }

    /// The scratch root: where the test puts its keychain.
    pub(crate) fn home(&self) -> &Path {
        &self.root
    }

    pub(crate) fn keychain(&self) -> &Keychain {
        &self.keychain
    }

    /// Have security take the keychain off this user's search list as it stands now and delete its
    /// file, then read the list and the disk again: an Err names what remains, an entry still
    /// listed or a file still there, whatever the delete's status was. The root goes when the
    /// fixture drops.
    pub(crate) fn retire(&mut self) -> Result<(), String> {
        if !self.retired {
            delete_keychain(self.keychain.path())?;
            self.retired = true;
        }
        Ok(())
    }
}

impl Drop for ThrowawayKeychain {
    fn drop(&mut self) {
        let Err(error) = self.retire() else {
            return;
        };
        let message = format!(
            "throwaway keychain {}: {error}",
            self.keychain.path().display()
        );
        if std::thread::panicking() {
            cowshed_os::eprintln!("{message}");
        } else {
            panic!("{message}");
        }
    }
}

/// The user-domain keychain search list as security prints it now. Bytes that are not UTF-8 are
/// an error: a lossy read would name a keychain by a path it does not have.
pub(crate) fn search_list() -> Result<String, String> {
    let output = Command::new(SECURITY)
        .args(["list-keychains", "-d", "user"])
        .output_bounded()
        .map_err(|error| format!("run security list-keychains: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "security list-keychains failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|error| {
        format!("security list-keychains printed bytes that are not UTF-8: {error}")
    })
}

/// The keychains a search listing names, in its order: one quoted path to a line. A line that is
/// neither blank nor one quoted path is an error naming it, never a keychain quietly left out.
pub(crate) fn listed(listing: &str) -> Result<Vec<PathBuf>, String> {
    listing
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
                .filter(|path| !path.is_empty())
                .map(PathBuf::from)
                .ok_or_else(|| {
                    format!("security list-keychains printed {line:?}, not one quoted path")
                })
        })
        .collect()
}

/// Whether a listed entry names keychain: security lists the file it resolved, with -db appended.
pub(crate) fn names(entry: &Path, keychain: &Path) -> bool {
    entry == keychain || entry == keychain.with_extension("keychain-db").as_path()
}

/// What still names keychain, as reasons: its entry on the listing, and its file or -db twin on
/// disk. Empty means it is gone. A listing line that cannot be read, or a stat that cannot tell
/// present from absent, is an error, never a reason to believe the keychain is gone.
fn left_of(
    listing: &str,
    keychain: &Path,
    exists: impl Fn(&Path) -> Result<bool, String>,
) -> Result<Vec<String>, String> {
    let mut left: Vec<String> = listed(listing)?
        .iter()
        .filter(|entry| names(entry, keychain))
        .map(|entry| format!("{} is still listed", entry.display()))
        .collect();
    for file in [
        keychain.to_path_buf(),
        keychain.with_extension("keychain-db"),
    ] {
        if exists(&file)? {
            left.push(format!("{} is still on disk", file.display()));
        }
    }
    Ok(left)
}

/// Whether anything is at path: an absent path is Ok(false); a stat that fails any other way
/// cannot say, and is an error.
fn present(path: &Path) -> Result<bool, String> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("stat {}: {error}", path.display())),
    }
}

/// Have security take keychain off this user's search list as it stands now and delete its file.
/// The verdict is what is left afterwards, not the status: a keychain whose file is already gone
/// answers "could not be found" yet still leaves the list, while a delete that leaves the entry
/// or a file behind fails with the status it ended with.
fn delete_keychain(keychain: &Path) -> Result<(), String> {
    let deleted = Command::new(SECURITY)
        .arg("delete-keychain")
        .arg(keychain)
        .output_bounded()
        .map_err(|error| format!("run security delete-keychain: {error}"))?;
    let left = left_of(&search_list()?, keychain, present)?;
    if left.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{} remains after security delete-keychain ({}: {}): {}",
        keychain.display(),
        deleted.status,
        String::from_utf8_lossy(&deleted.stderr).trim(),
        left.join("; ")
    ))
}

/// What the next start finds of one scratch root that a listed entry sits under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Root {
    /// The directory exists and the sweep production runs judges its owners dead, as a dry run.
    Dead,
    /// The directory exists and the sweep keeps it: a live owner, or one it cannot tell.
    Kept,
    /// The directory is not there: a sweep archived it, or its owner removed it.
    Gone,
    /// A symlink, or a stat that failed any way but absence: never taken for absence.
    Unread,
}

/// What the kernel says of the process a root's name records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Owner {
    Running,
    Dead,
    /// The check itself failed: not a death.
    Unobserved,
}

/// Whether the run that made root is over. An existing root is over only when the sweep's dry run
/// judged it dead. A missing one is over only when its whole name is a named scratch root of
/// prefix and the pid that name records is observed dead. Anything unread, unparsed, of a longer
/// prefix that merely begins with this one, or whose process is not observed dead is not over.
fn run_over(root: &Path, prefix: &str, seen: Root, owner: impl Fn(i32) -> Owner) -> bool {
    match seen {
        Root::Dead => true,
        Root::Kept | Root::Unread => false,
        Root::Gone => named_owner_for(root, prefix).is_some_and(|pid| owner(pid) == Owner::Dead),
    }
}

/// The listed keychains under a scratch root of prefix whose run is over. The entry names its own
/// root, so it says whose it is even when the root is gone; an entry under no named root of
/// prefix is never one of these, and a listing line that cannot be read fails the whole read.
fn orphaned(
    listing: &str,
    prefix: &str,
    seen: impl Fn(&Path) -> Root,
    owner: impl Fn(i32) -> Owner,
) -> Result<Vec<PathBuf>, String> {
    Ok(listed(listing)?
        .into_iter()
        .filter(|entry| {
            entry
                .ancestors()
                .find(|ancestor| named_owner_for(ancestor, prefix).is_some())
                .is_some_and(|root| run_over(root, prefix, seen(root), &owner))
        })
        .collect())
}

/// The roots of prefix directly under parent that the sweep production runs over the same
/// directory judges dead, taken as a dry run that archives nothing.
fn dead_roots(parent: &Path, prefix: &str) -> Result<Vec<PathBuf>, String> {
    let mut reaper = Reaper::new([parent.to_path_buf()])
        .map_err(|error| format!("sweep {}: {error}", parent.display()))?;
    let mut dead = Vec::new();
    loop {
        let report = reaper.sweep(parent, true);
        dead.extend(report.findings.into_iter().filter_map(|finding| {
            let proven_dead = matches!(
                finding.disposition,
                Disposition::WouldArchive | Disposition::WouldDelete
            );
            (proven_dead && named_owner_for(&finding.path, prefix).is_some())
                .then_some(finding.path)
        }));
        // A pass that examined fewer entries than its bound reached the end of the directory.
        if report.examined < PASS_ENTRIES {
            return Ok(dead);
        }
    }
}

/// What a stat of root finds, given the roots the sweep judged dead. Only a path that is not
/// there is gone; a symlink, or a stat that fails any other way, is unread.
fn root_state(root: &Path, dead: &[PathBuf]) -> Root {
    match fs::symlink_metadata(root) {
        Ok(found) if found.file_type().is_symlink() => Root::Unread,
        Ok(_) if dead.iter().any(|dead_root| dead_root == root) => Root::Dead,
        Ok(_) => Root::Kept,
        Err(error) if error.kind() == io::ErrorKind::NotFound => Root::Gone,
        Err(_) => Root::Unread,
    }
}

/// What the kernel says of a process: only a check that answers no is a death.
fn owner_state(pid: i32) -> Owner {
    match cowshed_os::process::running(pid) {
        Ok(true) => Owner::Running,
        Ok(false) => Owner::Dead,
        Err(_) => Owner::Unobserved,
    }
}

/// What killed runs of prefix left: their entries on this user's search list, taken off through
/// security as the list stands now, and then their scratch roots. An entry is theirs when its run
/// is over (run_over); a live sibling's root and entry, an entry under no named root of prefix,
/// and a root or process that cannot be read are never touched, and a listing line that cannot
/// be read fails the start.
fn reclaim_dead(prefix: &str) -> Result<(), String> {
    let parent = fs::canonicalize(std::env::temp_dir())
        .map_err(|error| format!("resolve the temporary directory: {error}"))?;
    let dead = dead_roots(&parent, prefix)?;
    let orphans = orphaned(
        &search_list()?,
        prefix,
        |root| root_state(root, &dead),
        owner_state,
    )?;
    for entry in orphans {
        delete_keychain(&entry)?;
    }
    for root in dead {
        match fs::remove_dir_all(&root) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("remove {}: {error}", root.display())),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A named scratch root of prefix as the sweep makes one: the prefix, -p, the owner's pid in
    /// eight hex digits and a seven digit nonce.
    fn root(prefix: &str, name: &str) -> PathBuf {
        PathBuf::from(format!("/private/var/folders/zz/T/{prefix}-p{name}"))
    }

    /// Where a test's root holds its keychain.
    fn keychain_in(home: &Path) -> PathBuf {
        home.join("Library/Keychains/cowshed-signing.keychain-db")
    }

    /// A listing of the keychains under the roots, after the user's own.
    fn listing(roots: &[&Path]) -> String {
        let user = "    \"/Users/dev/Library/Keychains/login.keychain-db\"\n".to_owned();
        roots.iter().fold(user, |text, home| {
            format!("{text}    \"{}\"\n", keychain_in(home).display())
        })
    }

    /// A listing is the quoted paths of its lines. A blank line carries nothing; any other line
    /// is an error naming it, since a keychain dropped from the read could be the one that stays.
    #[test]
    fn a_listing_is_read_whole_or_fails_naming_its_bad_line() {
        let clean = "\n    \"/a/login.keychain-db\"\n\n    \"/b c/x.keychain-db\"\n";
        assert_eq!(
            listed(clean),
            Ok(vec![
                PathBuf::from("/a/login.keychain-db"),
                PathBuf::from("/b c/x.keychain-db")
            ])
        );
        assert_eq!(listed(""), Ok(Vec::new()));
        for bad in [
            "/a/unquoted.keychain-db",
            "\"/a/unterminated",
            "\"\"",
            "warning: cannot read a keychain",
        ] {
            let error = listed(&format!("{clean}    {bad}\n")).expect_err(bad);
            assert!(error.contains(&format!("{bad:?}")), "{error}");
        }
    }

    /// A keychain is gone only when the listing names it no more and neither its file nor its -db
    /// twin is on disk, whatever the delete's status was: an entry that left while a file stayed
    /// is not gone, and neither is a keychain whose listing or stat cannot be read.
    #[test]
    fn a_keychain_is_gone_only_when_unlisted_and_neither_file_remains() {
        let keychain = PathBuf::from("/h/Library/Keychains/cowshed-signing.keychain");
        let twin = keychain.with_extension("keychain-db");
        let others = "    \"/u/Library/Keychains/login.keychain-db\"\n";
        let with = |entry: &Path| format!("{others}    \"{}\"\n", entry.display());
        let disk = |files: Vec<PathBuf>| {
            move |path: &Path| Ok::<bool, String>(files.iter().any(|file| file == path))
        };
        let listed_twin = format!("{} is still listed", twin.display());
        let on_disk_twin = format!("{} is still on disk", twin.display());

        for (listing, files, left) in [
            (others.to_owned(), vec![], vec![]),
            (with(&twin), vec![], vec![listed_twin.clone()]),
            (
                with(&keychain),
                vec![],
                vec![format!("{} is still listed", keychain.display())],
            ),
            (
                others.to_owned(),
                vec![keychain.clone()],
                vec![format!("{} is still on disk", keychain.display())],
            ),
            (
                others.to_owned(),
                vec![twin.clone()],
                vec![on_disk_twin.clone()],
            ),
            (
                with(&twin),
                vec![twin.clone()],
                vec![listed_twin, on_disk_twin],
            ),
        ] {
            assert_eq!(
                left_of(&listing, &keychain, disk(files)),
                Ok(left),
                "{listing}"
            );
        }

        let error = left_of("not a listing line\n", &keychain, disk(vec![]))
            .expect_err("an unreadable listing");
        assert!(error.contains("\"not a listing line\""), "{error}");
        let error = left_of(others, &keychain, |path: &Path| {
            Err(format!("stat {}: permission denied", path.display()))
        })
        .expect_err("a stat that cannot tell");
        assert!(error.contains("permission denied"), "{error}");
    }

    /// The next start reclaims the listed keychains whose run is over and no others. A root that is
    /// there is over only when the sweep's dry run judged it dead, whatever its name's pid reads; a
    /// root that is gone only when its whole name is a named scratch root of the prefix and that
    /// pid is observed dead. Planted against it: a root kept by the sweep whose pid reads dead, a
    /// running pid, a failed liveness check, a symlink, a name with no pid, a name too short, a
    /// zero pid, another prefix and a longer one, each of which has to be refused.
    #[test]
    fn the_next_start_reclaims_only_entries_whose_run_is_over() {
        let family = "cowshed-keychain-proof";
        let swept = root(family, "0000007b0000001");
        let live = root(family, "000000de0000002");
        let kept_dead = root(family, "000000ff000000a");
        let dead_gone = root(family, "000000ff0000003");
        let running_gone = root(family, "0000010a0000004");
        let unobserved_gone = root(family, "0000010b0000005");
        let linked = root(family, "0000010c0000006");
        let unparsed = root(family, "XYZ");
        let short = root(family, "000000ff");
        let no_pid = root(family, "000000000000007");
        let other = root("cowshed-signing", "000000ff0000008");
        let longer = root("cowshed-keychain-proof-extra", "000000ff0000009");
        let named = [
            &swept,
            &live,
            &kept_dead,
            &dead_gone,
            &running_gone,
            &unobserved_gone,
            &linked,
        ];
        let unnamed = [&unparsed, &short, &no_pid, &other, &longer];
        let homes: Vec<&Path> = named
            .iter()
            .chain(&unnamed)
            .map(|home| home.as_path())
            .collect();
        let text = listing(&homes);
        let keychains = |homes: &[&PathBuf]| -> Vec<PathBuf> {
            homes.iter().map(|home| keychain_in(home)).collect()
        };

        let seen = |home: &Path| {
            if home == swept {
                Root::Dead
            } else if home == live || home == kept_dead {
                Root::Kept
            } else if home == linked {
                Root::Unread
            } else {
                Root::Gone
            }
        };
        let owner = |pid: i32| match pid {
            255 => Owner::Dead,
            267 => Owner::Unobserved,
            _ => Owner::Running,
        };
        assert_eq!(
            orphaned(&text, family, seen, owner),
            Ok(keychains(&[&swept, &dead_gone]))
        );

        // Whatever the verdicts, only the named roots of the family are ever candidates.
        assert_eq!(
            orphaned(&text, family, |_| Root::Dead, |_| Owner::Dead),
            Ok(keychains(&named))
        );
        assert_eq!(
            orphaned(&text, family, |_| Root::Gone, |_| Owner::Dead),
            Ok(keychains(&named))
        );
        for (state, observed) in [
            (Root::Kept, Owner::Dead),
            (Root::Unread, Owner::Dead),
            (Root::Gone, Owner::Running),
            (Root::Gone, Owner::Unobserved),
        ] {
            assert_eq!(
                orphaned(&text, family, |_| state, |_| observed),
                Ok(Vec::new()),
                "{state:?} {observed:?}"
            );
        }

        let error = orphaned("junk\n", family, |_| Root::Dead, |_| Owner::Dead)
            .expect_err("a listing line that is no keychain");
        assert!(error.contains("\"junk\""), "{error}");
    }

    /// Only a path that is not there is gone. A symlink, and a path under a file (a stat that
    /// fails with something other than absence), are unread; an existing root is dead when the
    /// sweep said so and kept when it did not.
    #[test]
    fn only_an_absent_root_is_gone() {
        let scratch = TempRoot::new("cowshed-keychain-states");
        let there = scratch.join("there");
        let file = scratch.join("file");
        let link = scratch.join("link");
        fs::create_dir(&there).expect("a root");
        fs::write(&file, b"not a directory").expect("a file");
        std::os::unix::fs::symlink(&there, &link).expect("a symlink to the root");

        assert_eq!(root_state(&scratch.join("absent"), &[]), Root::Gone);
        assert_eq!(root_state(&there, &[]), Root::Kept);
        assert_eq!(root_state(&there, std::slice::from_ref(&there)), Root::Dead);
        assert_eq!(root_state(&link, std::slice::from_ref(&link)), Root::Unread);
        assert_eq!(root_state(&file.join("inside"), &[]), Root::Unread);
    }
}
