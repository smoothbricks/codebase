//! Declared build state a refresh moved out of its path, kept intact until the operator deletes
//! it (16_build_volumes.md, "What a refresh does at each path").
//!
//! Cowshed cannot rebuild what a declared directory holds. The motivating case is a
//! patch-development checkout, whose patches are uncommitted edits, and a refresh that discarded
//! one deleted an agent's edit inside it (a `.bun-pin` worktree in a downstream repository's
//! shed). So a refresh that links a declared path where a directory with entries stands renames
//! that directory, in one `rename(2)` on the same volume, into [`KEPT_DIRECTORY`]: cowshed's
//! excluded namespace, never the source tree or its status. No refresh and no `gc` deletes it,
//! and `rm` and land refuse its workspace while it is there; only `rm --abandon` takes it, with
//! the image. A fork drops its copy ([`release_inherited`]).
//!
//! Each entry's name is its whole record: the UTC second it was moved, a random id, and the
//! checkout-relative path it came from, with `%` and `/` percent-encoded, so the name decodes
//! back to exactly that path. [`list`] reads them for `cowshed doctor`, which reports each one
//! until it is gone, and no second record can disagree with what is there.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io;
use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Where a checkout keeps the declared directories a refresh moved aside, relative to the
/// checkout.
pub const KEPT_DIRECTORY: &str = ".cowshed/kept";

/// RFC 3339 UTC to the second, as [`crate::storage::deletion_log::rfc3339_utc`] writes it.
const AT_LEN: usize = "2026-10-30T19:41:10Z".len();
/// The random id that keeps two moves of one path within one second apart.
const ID_LEN: usize = 8;

/// One entry of a checkout's [`KEPT_DIRECTORY`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Kept {
    /// The entry itself, absolute.
    pub location: PathBuf,
    /// What its name records, or `None` for an entry no refresh named.
    pub moved: Option<Moved>,
}

/// What a kept entry's name records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Moved {
    /// The checkout-relative path the directory was moved from.
    pub path: PathBuf,
    /// When, RFC 3339 UTC to the second.
    pub at: String,
}

/// Move the checkout-relative `relative`, intact, into the checkout's kept directory in one
/// `rename(2)`, named for `now`, a random id and `relative`. Returns where it went.
pub fn keep_aside(checkout: &Path, relative: &Path, now: SystemTime) -> io::Result<PathBuf> {
    let directory = super::discard::owned_directory(checkout, KEPT_DIRECTORY, true)?;
    let id = uuid::Uuid::new_v4().simple().to_string();
    let kept = directory.join(entry_name(
        &crate::storage::deletion_log::rfc3339_utc(now),
        &id[..ID_LEN],
        relative,
    ));
    fs::rename(checkout.join(relative), &kept)?;
    Ok(kept)
}

/// Every entry of `checkout`'s kept directory, oldest first; none when it has none.
pub fn list(checkout: &Path) -> io::Result<Vec<Kept>> {
    let directory = super::discard::owned_directory(checkout, KEPT_DIRECTORY, false)?;
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut kept = entries
        .map(|entry| {
            entry.map(|entry| Kept {
                moved: decode(&entry.file_name()),
                location: entry.path(),
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    kept.sort_by(|left, right| left.location.cmp(&right.location));
    Ok(kept)
}

/// `<at>-<id>-<relative>`, with `%` and `/` in `relative` percent-encoded.
fn entry_name(at: &str, id: &str, relative: &Path) -> OsString {
    let mut name = format!("{at}-{id}-").into_bytes();
    for &byte in relative.as_os_str().as_bytes() {
        match byte {
            b'%' => name.extend_from_slice(b"%25"),
            b'/' => name.extend_from_slice(b"%2F"),
            byte => name.push(byte),
        }
    }
    OsString::from_vec(name)
}

/// Drop a fork's copy of its source's kept entries (16_build_volumes.md, "Fork" step 4). The
/// source still holds every one, so the copy loses nothing, and kept it would make the fork's own
/// `rm` and land refuse for state that is not the fork's. One rename into the discard directory,
/// deleted in the background like any discard, so the fork never waits on the delete. A
/// checkout with no kept directory has nothing to drop.
pub fn release_inherited(checkout: &Path) -> io::Result<()> {
    match fs::symlink_metadata(checkout.join(KEPT_DIRECTORY)) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
        Ok(_) => {}
    }
    super::discard::move_aside(checkout, Path::new(KEPT_DIRECTORY))?;
    super::discard::reap(checkout);
    Ok(())
}

/// The shell command that deletes the kept entry at `location`, quoted for any path.
pub fn removal(location: &Path) -> String {
    format!(
        "rm -rf '{}'",
        location.display().to_string().replace('\'', "'\\''")
    )
}

/// The record [`entry_name`] wrote into `name`, or `None` for a name it did not write.
fn decode(name: &OsStr) -> Option<Moved> {
    let (at, rest) = name.as_bytes().split_at_checked(AT_LEN)?;
    let at = std::str::from_utf8(at).ok()?;
    crate::api::dto::UtcTimestamp::new(at).ok()?;
    let (id, rest) = rest.strip_prefix(b"-")?.split_at_checked(ID_LEN)?;
    if !id
        .iter()
        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    let mut encoded = rest.strip_prefix(b"-")?.iter();
    let mut path = Vec::with_capacity(encoded.len());
    while let Some(&byte) = encoded.next() {
        path.push(match byte {
            b'%' => match (encoded.next(), encoded.next()) {
                (Some(b'2'), Some(b'5')) => b'%',
                (Some(b'2'), Some(b'F')) => b'/',
                _ => return None,
            },
            byte => byte,
        });
    }
    // Only a name `entry_name` wrote: a relative path of plain components, none empty.
    let plain = path
        .split(|byte| *byte == b'/')
        .all(|component| !matches!(component, b"" | b"." | b".."));
    plain.then(|| Moved {
        path: PathBuf::from(OsString::from_vec(path)),
        at: at.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Every checkout-relative path, `%` and `/` included, comes back from the name it was
        /// kept under exactly as it went in, with the time it was kept.
        #[test]
        fn an_entry_name_decodes_to_the_path_and_time_it_records(
            components in prop::collection::vec("[a-zA-Z0-9%_-][a-zA-Z0-9%._-]{0,7}", 1..5),
            seconds in 0u64..4_102_444_800,
        ) {
            let relative = PathBuf::from(components.join("/"));
            let at = crate::storage::deletion_log::rfc3339_utc(
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds),
            );
            prop_assert_eq!(
                decode(&entry_name(&at, "0123abcd", &relative)),
                Some(Moved { path: relative, at })
            );
        }
    }

    #[test]
    fn a_name_no_refresh_wrote_records_nothing() {
        for name in [
            "notes",
            "2026-10-30T19:41:10Z-0123abcd-",
            "2026-10-30T19:41:10Z-0123abcz-a",
            "2026-13-30T19:41:10Z-0123abcd-a",
            "2026-10-30T19:41:10Z-0123abcd-a%2Gb",
            "2026-10-30T19:41:10Z-0123abcd-a%2",
            "2026-10-30T19:41:10Z-0123ABCD-a",
            "2026-10-30T19:41:10Z-0123abcd-%2Fa",
            "2026-10-30T19:41:10Z-0123abcd-a%2F",
            "2026-10-30T19:41:10Z-0123abcd-a%2F%2Fb",
            "2026-10-30T19:41:10Z-0123abcd-a%2F..%2Fb",
            "2026-10-30T19:41:10Z-0123abcd-.",
        ] {
            assert_eq!(decode(OsStr::new(name)), None, "{name}");
        }
    }

    /// `%2F` written in a path stays those three bytes: only `entry_name`'s own escapes decode.
    #[test]
    fn an_escaped_escape_decodes_to_the_literal_text() {
        let at = "2026-10-30T19:41:10Z";
        for path in ["a%2Fb", "a/b", "a%25b", "a%b"] {
            assert_eq!(
                decode(&entry_name(at, "0123abcd", Path::new(path))),
                Some(Moved {
                    path: PathBuf::from(path),
                    at: at.to_owned(),
                }),
                "{path}"
            );
        }
    }

    #[test]
    fn the_removal_command_quotes_any_path() {
        assert_eq!(
            removal(Path::new("/x/it's kept")),
            "rm -rf '/x/it'\\''s kept'"
        );
    }

    /// The directory moves whole, entries and all, and the listing names where it went and
    /// where it came from until something deletes it.
    #[test]
    fn a_kept_directory_is_listed_until_it_is_deleted() {
        let root =
            std::env::temp_dir().join(format!("cowshed-kept-{}", uuid::Uuid::new_v4().simple()));
        let checkout = root.join("checkout");
        fs::create_dir_all(checkout.join("vendor/up%stream/src")).unwrap();
        fs::write(checkout.join("vendor/up%stream/src/edit.c"), b"edit").unwrap();
        assert_eq!(list(&checkout).unwrap(), []);
        let now = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_793_390_470);
        let kept = keep_aside(&checkout, Path::new("vendor/up%stream"), now).unwrap();
        assert!(!checkout.join("vendor/up%stream").exists());
        assert_eq!(fs::read(kept.join("src/edit.c")).unwrap(), b"edit");
        assert_eq!(
            list(&checkout).unwrap(),
            [Kept {
                location: kept.clone(),
                moved: Some(Moved {
                    path: PathBuf::from("vendor/up%stream"),
                    at: crate::storage::deletion_log::rfc3339_utc(now),
                }),
            }]
        );
        fs::remove_dir_all(&kept).unwrap();
        assert_eq!(list(&checkout).unwrap(), []);
        fs::remove_dir_all(&root).unwrap();
    }
}
