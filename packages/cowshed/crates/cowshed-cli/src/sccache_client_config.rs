//! sccache's own config file, so a client that inherited no cowshed environment still lands in
//! the shared store.
//!
//! Every workspace supervisor exports `SCCACHE_DIR` and `SCCACHE_SERVER_UDS`, so a build run
//! inside a workspace already reaches the host-owned daemon. A build run anywhere else does not:
//! `RUSTC_WRAPPER=sccache` survives in any shell that once loaded a project environment, and a
//! wrapped `cargo` with no `SCCACHE_DIR` falls back to sccache's private per-user default
//! directory. `cowshed_core::sandbox::sccache_cache_directory` already states the requirement this
//! module makes true — "a client that finds no daemon spawns its own server, and that fallback
//! must land in this cache rather than sccache's user-default directory" — and the only host state
//! a store-less client reads is sccache's config file.
//!
//! Three properties of sccache 0.17 shape everything here, all source-verified:
//!
//! 1. **`SCCACHE_DIR` beats the file.** The config governs exactly the store-less case, which is
//!    the case being fixed, and never overrides a workspace.
//! 2. **Resolution can shadow.** sccache reads `SCCACHE_CONF`, else `config_dir()/config` if it
//!    exists, else `preference_dir()/config` if it exists, else `config_dir()/config`. A
//!    user-authored config may therefore live at the legacy `preference_dir` location, where
//!    writing the new path would not overwrite it but *shadow* it — silently moving that user's
//!    cache. Editing whatever sccache would actually read makes shadowing unrepresentable.
//! 3. **Unknown keys are rejected.** `FileConfig` is `deny_unknown_fields`, so a "written by
//!    cowshed" *key* would make sccache exit 2 and start no server at all. Ownership is therefore
//!    recorded as a TOML comment, which is legible to a reader and invisible to the parser. That
//!    same strictness is why writing this file cannot become a silent lie: a config cowshed
//!    corrupted would fail every wrapped build loudly, with a file, line, and column.
//!
//! `SCCACHE_CONF` is deliberately not honoured when choosing where to write. It is one process's
//! environment rather than host state, so a file written there would be read by exactly the shell
//! that ran `setup`.

use cowshed_core::metadata::ImageCapacity;
use cowshed_core::{CowshedError, Result};
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

mod native;
use native::config_directories;

/// The leaf name sccache looks for in its config directory.
const CONFIG_LEAF: &str = "config";

/// Readable and writable only by the user whose clients read it, like every other per-user
/// artifact cowshed installs (`launchd::PRIVATE_PLIST_MODE`).
const PRIVATE_CONFIG_MODE: u32 = 0o600;

/// The first line of the block cowshed owns, and the whole of its ownership claim.
///
/// Matched at a line start, so it is also the truncation point when the block is refreshed. It
/// must not name a path or a size: a marker that drifted with the content it introduces could not
/// identify the block it belongs to.
const OWNERSHIP_MARKER: &str = "# managed by `cowshed setup`";

/// The store a store-less client must reach, and the cap it must respect while doing so.
///
/// The cap is part of the destination rather than an option: a client that finds no daemon starts
/// a server of its own over this same directory, and sccache's 10 GiB default is smaller than what
/// the shared store already holds — so a `dir` written without a `size` turns a misdirected-cache
/// defect into a cache-destroying one. The number is the daemon's own, so both servers manage the
/// store to the same bound.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SharedStore {
    directory: PathBuf,
    capacity: ImageCapacity,
}

impl SharedStore {
    pub const fn new(directory: PathBuf, capacity: ImageCapacity) -> Self {
        Self {
            directory,
            capacity,
        }
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub const fn capacity(&self) -> ImageCapacity {
        self.capacity
    }
}

/// What cowshed did, or deliberately did not do, to the config file sccache reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigReport {
    /// The file sccache would load, whether or not it was written.
    pub path: PathBuf,
    /// The directory that file has to name. Reported even when nothing was written, because
    /// "cowshed did not point clients at X" is only actionable when X is named.
    pub store: PathBuf,
    pub outcome: ConfigOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigOutcome {
    /// The file already directs a store-less client at the shared store, to the byte.
    AlreadyCurrent,
    Written(ConfigChange),
    /// The file is somebody else's and was left exactly as it was found.
    Refused(ConfigConflict),
    /// There was no shared store to point at, so nothing was written.
    ///
    /// A host whose caches volume is absent or unmounted would otherwise get a config naming a
    /// directory that resolves onto the boot disk under the empty mountpoint — a fourth orphaned
    /// store, created by the command whose job is to prevent them.
    NoSharedStore {
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigChange {
    /// sccache had no config at any location it reads.
    Created,
    /// A config existed with no `cache.disk` table; cowshed's block was appended below it and
    /// every byte the user wrote was preserved.
    Appended,
    /// cowshed's own block was there and no longer said the right thing — the derived cap grows as
    /// projects are adopted, and a stale cap is an eviction bound nobody chose.
    Refreshed,
}

/// Why cowshed left the file alone. Every variant names what was found, because a refusal a person
/// cannot act on is worse than no refusal at all.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigConflict {
    /// `cache.disk.dir` names a different directory, and cowshed did not write it.
    ForeignDirectory { found: String },
    /// `cache.disk` exists without a `dir`, so cowshed's block cannot be appended (a second
    /// `[cache.disk]` header is not valid TOML) and cannot be merged without rewriting a table it
    /// does not own.
    ForeignDiskTable,
    /// `cache.disk.dir` is not a string, so sccache is refusing this file too.
    MalformedDirectory,
    /// The file is not TOML, so sccache is refusing it and cowshed cannot reason about it.
    Unparsable { reason: String },
    /// cowshed's marker is present but the block below it is no longer only cowshed's, so
    /// refreshing it would delete something a person put there.
    DisturbedBlock,
    /// Appending cowshed's block would not produce a file sccache can load — an inline `cache`
    /// table cannot be extended by a `[cache.disk]` header. Caught by re-parsing the candidate
    /// rather than by enumerating TOML shapes.
    UnmergeableShape,
}

impl fmt::Display for ConfigConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignDirectory { found } => {
                write!(formatter, "it already sets cache.disk.dir to {found}")
            }
            Self::ForeignDiskTable => write!(
                formatter,
                "it already has a [cache.disk] table that cowshed did not write"
            ),
            Self::MalformedDirectory => {
                write!(formatter, "its cache.disk.dir is not a string")
            }
            Self::Unparsable { reason } => write!(formatter, "it is not valid TOML: {reason}"),
            Self::DisturbedBlock => write!(
                formatter,
                "the block below cowshed's marker has been edited by hand"
            ),
            Self::UnmergeableShape => write!(
                formatter,
                "its cache table is written inline and cannot be extended by a [cache.disk] header"
            ),
        }
    }
}

/// The config file sccache itself would load, and therefore the only one worth writing.
///
/// Mirrors `directories::ProjectDirs::from("", "Mozilla", "sccache")` per platform, which is what
/// sccache builds its own paths from. The legacy `preference_dir` candidate is macOS-only: the
/// `directories` split that created it exists nowhere else, and on XDG platforms `preference_dir`
/// and `config_dir` are the same directory.
pub fn client_config_path(home: &Path) -> PathBuf {
    let candidates = config_directories(home);
    candidates
        .iter()
        .map(|directory| directory.join(CONFIG_LEAF))
        .find(|candidate| candidate.exists())
        .unwrap_or_else(|| {
            candidates
                .first()
                .expect("every platform has at least one sccache config directory")
                .join(CONFIG_LEAF)
        })
}

/// Read the file sccache would load, decide, and act — the only function here that touches disk.
///
/// A refusal is a report rather than an error. `setup`'s subject is host storage, and a user's own
/// sccache config is neither cowshed's to overwrite nor a reason to fail the command that found it;
/// what the caller owes the reader is the conflict, named.
pub fn apply(path: &Path, store: &SharedStore) -> Result<ConfigReport> {
    let report = |outcome| {
        Ok(ConfigReport {
            path: path.to_owned(),
            store: store.directory().to_owned(),
            outcome,
        })
    };
    let existing = match fs::read_to_string(path) {
        Ok(text) => Some(text),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        // A config that is not UTF-8 is one sccache cannot read either, and cowshed will not
        // replace it sight unseen.
        Err(error) if error.kind() == io::ErrorKind::InvalidData => {
            return report(ConfigOutcome::Refused(ConfigConflict::Unparsable {
                reason: error.to_string(),
            }));
        }
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not read {}: {error}",
                path.display()
            )));
        }
    };
    match plan(existing.as_deref(), store) {
        ConfigPlan::AlreadyCurrent => report(ConfigOutcome::AlreadyCurrent),
        ConfigPlan::Refuse(conflict) => report(ConfigOutcome::Refused(conflict)),
        ConfigPlan::Write { change, contents } => {
            write_atomically(path, &contents, existing.is_some())?;
            report(ConfigOutcome::Written(change))
        }
    }
}

/// Replace the config in one step, so no client ever reads a half-written one.
///
/// A temporary beside the target keeps the rename within one filesystem, which is what makes it
/// atomic. A file cowshed creates is private to the user launchd runs their agents as, matching
/// every other per-user artifact cowshed installs; a file that already existed keeps the mode its
/// author gave it.
fn write_atomically(path: &Path, contents: &str, existed: bool) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        CowshedError::internal(format!(
            "{} is not a path a config file can live at",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        CowshedError::internal(format!("could not create {}: {error}", parent.display()))
    })?;
    let mode = if existed {
        fs::metadata(path)
            .map(|metadata| metadata.permissions().mode() & 0o7777)
            .unwrap_or(PRIVATE_CONFIG_MODE)
    } else {
        PRIVATE_CONFIG_MODE
    };
    let temporary = path.with_extension("cowshed-tmp");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode)
        .open(&temporary)
        .map_err(|error| {
            CowshedError::internal(format!("could not create {}: {error}", temporary.display()))
        })?;
    let written = file
        .write_all(contents.as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| fs::set_permissions(&temporary, fs::Permissions::from_mode(mode)))
        .and_then(|()| fs::rename(&temporary, path));
    if let Err(error) = written {
        let _ = fs::remove_file(&temporary);
        return Err(CowshedError::internal(format!(
            "could not write {}: {error}",
            path.display()
        )));
    }
    Ok(())
}

/// What to do about `existing`, decided without touching the filesystem.
///
/// Total in the shapes a config can take, because every answer is read off a parsed document
/// rather than off the text: a `dir` set as `[cache.disk]`, as `cache = { disk = { … } }`, or as a
/// dotted `cache.disk.dir` key are one case here, and none of them can be mistaken for absence.
pub fn plan(existing: Option<&str>, store: &SharedStore) -> ConfigPlan {
    let block = render_block(store);
    let Some(text) = existing else {
        return ConfigPlan::Write {
            change: ConfigChange::Created,
            contents: block,
        };
    };
    let document = match text.parse::<toml::Table>() {
        Ok(document) => document,
        Err(error) => {
            return ConfigPlan::Refuse(ConfigConflict::Unparsable {
                reason: error.message().to_owned(),
            });
        }
    };
    // Ownership is settled before the content is inspected: cowshed's own block is the one thing
    // it may rewrite, and a marker over a block somebody has edited is a refusal rather than a
    // licence.
    if let Some(marker) = marker_offset(text) {
        if !owns_tail(text, marker) {
            return ConfigPlan::Refuse(ConfigConflict::DisturbedBlock);
        }
        let contents = format!("{}{block}", &text[..marker]);
        if contents == text {
            return ConfigPlan::AlreadyCurrent;
        }
        return ConfigPlan::Write {
            change: ConfigChange::Refreshed,
            contents,
        };
    }
    match disk_directory(&document) {
        DiskDirectory::TableAbsent => {
            // A blank line between their last setting and cowshed's marker, so the boundary
            // between the two authors is visible at a glance.
            let contents = format!("{}\n\n{block}", text.trim_end());
            // The one shape check that matters, made by construction rather than by enumeration:
            // if the merged file does not parse into the directory that was wanted, it is not
            // written. An inline `cache` table lands here.
            if !directs_to(&contents, store) {
                return ConfigPlan::Refuse(ConfigConflict::UnmergeableShape);
            }
            ConfigPlan::Write {
                change: ConfigChange::Appended,
                contents,
            }
        }
        DiskDirectory::Missing => ConfigPlan::Refuse(ConfigConflict::ForeignDiskTable),
        DiskDirectory::Malformed => ConfigPlan::Refuse(ConfigConflict::MalformedDirectory),
        // A hand-written config that already does the right thing is right, whoever wrote it. Its
        // cap is its author's business: cowshed asserts the destination, not the policy.
        DiskDirectory::Set(directory) if Path::new(&directory) == store.directory() => {
            ConfigPlan::AlreadyCurrent
        }
        DiskDirectory::Set(found) => ConfigPlan::Refuse(ConfigConflict::ForeignDirectory { found }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigPlan {
    AlreadyCurrent,
    Write {
        change: ConfigChange,
        contents: String,
    },
    Refuse(ConfigConflict),
}

/// Where cowshed's block starts, recognised only at the beginning of a line.
///
/// Matching the marker anywhere would let a config that merely quotes it inside a value be read as
/// a claim, and the reader would get "the block below cowshed's marker has been edited by hand"
/// instead of the conflict that is actually there. The first line-start occurrence wins: a second
/// block below it lands inside the tail, where the tail parse rejects it as a duplicate table.
fn marker_offset(text: &str) -> Option<usize> {
    text.match_indices(OWNERSHIP_MARKER)
        .map(|(index, _)| index)
        .find(|&index| index == 0 || text.as_bytes()[index - 1] == b'\n')
}

/// Whether everything from `marker` to the end of the file is still only cowshed's block.
///
/// Proven by parsing the tail on its own: it has to be exactly a `cache.disk` table carrying
/// nothing but `dir` and `size`. Anything a person added below the marker — another table, another
/// key, a stray line — makes the tail not-ours, so refreshing it can never delete their work.
fn owns_tail(text: &str, marker: usize) -> bool {
    let Ok(tail) = text[marker..].parse::<toml::Table>() else {
        return false;
    };
    let mut keys = tail.keys();
    if keys.next().map(String::as_str) != Some("cache") || keys.next().is_some() {
        return false;
    }
    let Some(cache) = tail["cache"].as_table() else {
        return false;
    };
    let mut cache_keys = cache.keys();
    if cache_keys.next().map(String::as_str) != Some("disk") || cache_keys.next().is_some() {
        return false;
    }
    cache["disk"].as_table().is_some_and(|disk| {
        disk.keys()
            .all(|key| matches!(key.as_str(), "dir" | "size"))
    })
}

/// What a parsed config says about `cache.disk.dir`.
enum DiskDirectory {
    /// There is no `cache.disk` at all, so a `[cache.disk]` header can be appended.
    TableAbsent,
    /// `cache.disk` exists but sets no `dir`.
    Missing,
    /// `dir` is present but not a string.
    Malformed,
    Set(String),
}

fn disk_directory(document: &toml::Table) -> DiskDirectory {
    let disk = document
        .get("cache")
        .and_then(toml::Value::as_table)
        .and_then(|cache| cache.get("disk"));
    let Some(disk) = disk else {
        return DiskDirectory::TableAbsent;
    };
    // `cache.disk` set to a non-table is a config sccache rejects; treat it as a foreign table
    // rather than as an absent one, because appending a header would collide with it.
    let Some(table) = disk.as_table() else {
        return DiskDirectory::Missing;
    };
    match table.get("dir") {
        None => DiskDirectory::Missing,
        Some(toml::Value::String(directory)) => DiskDirectory::Set(directory.clone()),
        Some(_) => DiskDirectory::Malformed,
    }
}

/// Whether `contents` really is a config that sends a store-less client to `store`.
///
/// The candidate is re-parsed rather than trusted: it is assembled by concatenation, so this is
/// the step that turns "the text looks right" into "sccache will read this directory".
fn directs_to(contents: &str, store: &SharedStore) -> bool {
    contents
        .parse::<toml::Table>()
        .is_ok_and(|document| match disk_directory(&document) {
            DiskDirectory::Set(directory) => Path::new(&directory) == store.directory(),
            _ => false,
        })
}

/// The block cowshed owns, rendered identically for identical inputs so that a second `setup` run
/// is a byte comparison rather than a judgement.
fn render_block(store: &SharedStore) -> String {
    format!(
        "{OWNERSHIP_MARKER}\n\
         #\n\
         # cowshed owns the [cache.disk] table below and keeps it current; it touches no other\n\
         # table and no other file. It exists so that an sccache client which inherited no cowshed\n\
         # environment — a cargo build outside a workspace, where SCCACHE_DIR is unset — reads and\n\
         # writes the store the dev.cowshed.sccache daemon serves, instead of a private per-user\n\
         # cache nobody shares. Without it, cross-workspace compile reuse silently does not happen\n\
         # and the symptom is a hit rate of zero rather than a misdirected path.\n\
         #\n\
         # size matches the daemon's cap deliberately: a client that finds no daemon starts a\n\
         # server of its own over this same directory, and sccache's 10 GiB default would evict\n\
         # the shared cache down to it. It is written as a byte count because sccache accepts\n\
         # only an integer or a bare k/m/g/t suffix and rejects anything else — `\"200 GiB\"`\n\
         # fails its TOML load with exit 2 — so a byte count cannot drift with how cowshed\n\
         # chooses to render a capacity for humans elsewhere.\n\
         #\n\
         # Delete this block to opt out. `cowshed setup` never rewrites a [cache.disk] it did not\n\
         # write, and reports what it found instead.\n\
         [cache.disk]\n\
         dir = {}\n\
         size = {}\n",
        basic_string(store.directory()),
        store.capacity().bytes(),
    )
}

/// A path as a TOML basic string.
///
/// Escapes rather than assumes: the destination is a constant today, but a rendering that would
/// corrupt the file on an unexpected byte is a trap for whoever makes it configurable. A path that
/// is not UTF-8 renders as an empty string, which [`directs_to`] then refuses — the write never
/// happens, and no lossy spelling of the path reaches the file.
fn basic_string(path: &Path) -> String {
    let Some(text) = path.to_str() else {
        return String::from("\"\"");
    };
    let mut rendered = String::with_capacity(text.len() + 2);
    rendered.push('"');
    for character in text.chars() {
        match character {
            '"' => rendered.push_str("\\\""),
            '\\' => rendered.push_str("\\\\"),
            '\n' => rendered.push_str("\\n"),
            '\r' => rendered.push_str("\\r"),
            '\t' => rendered.push_str("\\t"),
            control if control.is_control() => {
                rendered.push_str(&format!("\\u{:04X}", control as u32));
            }
            other => rendered.push(other),
        }
    }
    rendered.push('"');
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> SharedStore {
        SharedStore::new(
            PathBuf::from("/private/cowshed/caches/sccache"),
            ImageCapacity::from_gibibytes(200),
        )
    }

    fn written(existing: Option<&str>) -> (ConfigChange, String) {
        match plan(existing, &store()) {
            ConfigPlan::Write { change, contents } => (change, contents),
            other => panic!("expected a write, got {other:?}"),
        }
    }

    /// The defect, as the file that fixes it: a host with no sccache config gets one naming the
    /// shared store and the daemon's cap.
    #[test]
    fn a_host_with_no_config_gets_one_naming_the_shared_store() {
        let (change, contents) = written(None);
        assert_eq!(change, ConfigChange::Created);
        assert!(contents.starts_with(OWNERSHIP_MARKER));
        assert!(contents.contains("[cache.disk]\ndir = \"/private/cowshed/caches/sccache\"\n"));
        assert!(contents.contains("size = 214748364800"));
        assert!(directs_to(&contents, &store()));
    }

    /// sccache's `FileConfig` is `deny_unknown_fields`: the ownership claim has to survive its
    /// parser, which is why it is a comment and not a key.
    #[test]
    fn the_ownership_claim_is_a_comment_and_leaves_no_key_behind() {
        let (_, contents) = written(None);
        let document = contents.parse::<toml::Table>().expect("valid TOML");
        assert_eq!(document.keys().collect::<Vec<_>>(), vec!["cache"]);
        let disk = document["cache"].as_table().expect("cache table")["disk"]
            .as_table()
            .expect("disk table");
        assert_eq!(disk.keys().collect::<Vec<_>>(), vec!["dir", "size"]);
    }

    /// Idempotent in both shapes cowshed can produce: the file it created, and the block it
    /// appended below somebody else's settings.
    #[test]
    fn a_second_run_over_cowsheds_own_block_changes_nothing() {
        let (_, created) = written(None);
        assert_eq!(plan(Some(&created), &store()), ConfigPlan::AlreadyCurrent);

        let (_, appended) = written(Some("[dist]\ntoolchain_cache_size = 1073741824\n"));
        assert_eq!(plan(Some(&appended), &store()), ConfigPlan::AlreadyCurrent);
    }

    /// A user's own settings are preserved byte for byte, because they are never rewritten: the
    /// block is appended below them.
    #[test]
    fn a_config_without_a_disk_table_keeps_every_byte_and_gains_the_block() {
        let existing = "# my own notes\n[dist]\nscheduler_url = \"http://build.invalid\"\n";
        let (change, contents) = written(Some(existing));
        assert_eq!(change, ConfigChange::Appended);
        assert!(contents.starts_with(existing));
        assert!(directs_to(&contents, &store()));
        let document = contents.parse::<toml::Table>().expect("valid TOML");
        assert_eq!(
            document["dist"].as_table().expect("dist")["scheduler_url"]
                .as_str()
                .expect("url"),
            "http://build.invalid"
        );
    }

    /// The conflict that must never be resolved by clobbering: somebody else already chose a
    /// directory. The diagnostic names it, so the reader knows what cowshed disagreed with.
    #[test]
    fn a_foreign_cache_directory_is_refused_and_named() {
        let existing = "[cache.disk]\ndir = \"/somewhere/else\"\nsize = \"5g\"\n";
        let conflict = match plan(Some(existing), &store()) {
            ConfigPlan::Refuse(conflict) => conflict,
            other => panic!("expected a refusal, got {other:?}"),
        };
        assert_eq!(
            conflict,
            ConfigConflict::ForeignDirectory {
                found: String::from("/somewhere/else")
            }
        );
        assert_eq!(
            conflict.to_string(),
            "it already sets cache.disk.dir to /somewhere/else"
        );
    }

    /// A hand-written config that already points at the shared store is correct, so cowshed
    /// leaves it — including its author's own cap.
    #[test]
    fn a_hand_written_config_that_already_points_at_the_store_is_left_alone() {
        let existing = "[cache.disk]\ndir = \"/private/cowshed/caches/sccache\"\nsize = \"12g\"\n";
        assert_eq!(plan(Some(existing), &store()), ConfigPlan::AlreadyCurrent);
    }

    /// `cache.disk` without a `dir` cannot take an appended header — a duplicate `[cache.disk]` is
    /// not valid TOML — and cowshed will not rewrite a table it did not write.
    #[test]
    fn a_foreign_disk_table_without_a_directory_is_refused() {
        let existing = "[cache.disk]\nsize = \"5g\"\n";
        assert_eq!(
            plan(Some(existing), &store()),
            ConfigPlan::Refuse(ConfigConflict::ForeignDiskTable)
        );
    }

    /// Every spelling of the same key is one case, so no shape slips past as "absent".
    #[test]
    fn inline_and_dotted_spellings_are_read_as_the_same_setting() {
        for existing in [
            "cache.disk.dir = \"/somewhere/else\"\n",
            "[cache]\ndisk = { dir = \"/somewhere/else\" }\n",
            "[cache.disk]\ndir = \"/somewhere/else\"\n",
        ] {
            assert_eq!(
                plan(Some(existing), &store()),
                ConfigPlan::Refuse(ConfigConflict::ForeignDirectory {
                    found: String::from("/somewhere/else")
                }),
                "{existing}"
            );
        }
    }

    /// An inline `cache` table cannot be extended by a `[cache.disk]` header. The candidate is
    /// re-parsed, so this is caught as a refusal instead of written as a file sccache would reject.
    #[test]
    fn an_inline_cache_table_is_refused_rather_than_corrupted() {
        let existing = "cache = { s3 = { bucket = \"b\" } }\n";
        assert_eq!(
            plan(Some(existing), &store()),
            ConfigPlan::Refuse(ConfigConflict::UnmergeableShape)
        );
    }

    #[test]
    fn a_file_that_is_not_toml_is_refused_with_the_parser_s_reason() {
        match plan(Some("this is not toml\n"), &store()) {
            ConfigPlan::Refuse(ConfigConflict::Unparsable { reason }) => {
                assert!(!reason.is_empty(), "the parser's reason is reported");
            }
            other => panic!("expected an unparsable refusal, got {other:?}"),
        }
    }

    /// The cap drifts upward as projects are adopted, and a stale cap is an eviction bound nobody
    /// chose — so cowshed's own block is refreshed rather than left.
    #[test]
    fn a_stale_cap_in_cowsheds_own_block_is_refreshed() {
        let stale = render_block(&SharedStore::new(
            PathBuf::from("/private/cowshed/caches/sccache"),
            ImageCapacity::from_gibibytes(40),
        ));
        let (change, contents) = written(Some(&stale));
        assert_eq!(change, ConfigChange::Refreshed);
        assert!(contents.contains("size = 214748364800"));
        assert!(!contents.contains("40g"));
    }

    /// A refresh may never delete something a person put below the marker, so a disturbed block is
    /// a refusal.
    #[test]
    fn a_hand_edited_block_below_the_marker_is_refused() {
        let (_, mine) = written(None);
        let disturbed = format!("{mine}\n[dist]\nscheduler_url = \"http://build.invalid\"\n");
        assert_eq!(
            plan(Some(&disturbed), &store()),
            ConfigPlan::Refuse(ConfigConflict::DisturbedBlock)
        );

        let extra_key = mine.replace(
            "size = 214748364800\n",
            "size = 214748364800\nrw_mode = \"READ_ONLY\"\n",
        );
        assert_eq!(
            plan(Some(&extra_key), &store()),
            ConfigPlan::Refuse(ConfigConflict::DisturbedBlock)
        );
    }

    /// A user's settings above cowshed's appended block survive a refresh, because only the tail
    /// cowshed owns is rewritten.
    #[test]
    fn a_refresh_rewrites_only_the_tail_cowshed_owns() {
        let existing = "[dist]\nscheduler_url = \"http://build.invalid\"\n";
        let stale = format!(
            "{existing}\n{}",
            render_block(&SharedStore::new(
                PathBuf::from("/private/cowshed/caches/sccache"),
                ImageCapacity::from_gibibytes(40),
            ))
        );
        let (change, contents) = written(Some(&stale));
        assert_eq!(change, ConfigChange::Refreshed);
        assert!(contents.starts_with(existing));
        assert!(contents.contains("size = 214748364800"));
        assert!(directs_to(&contents, &store()));
    }

    /// `apply` writes what `plan` decided, and does it to a real file.
    ///
    /// Every other test here exercises `plan`, which is pure and hands back a `String`. That
    /// leaves the whole write path — the temp-and-rename, the mode, and the read-modify-write
    /// round trip — asserted by inspection rather than demonstrated, which is exactly where a
    /// config that is correct in memory and wrong on disk would hide. This drives the real
    /// filesystem and reads the bytes back.
    #[test]
    fn apply_appends_to_a_users_config_on_disk_and_leaves_their_lines_intact() {
        let directory =
            std::env::temp_dir().join(format!("cowshed-sccache-apply-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("temp directory");
        let path = directory.join("config");
        let authored = "[dist]\ntoolchain_cache_size = 1073741824\n";
        std::fs::write(&path, authored).expect("author a config");

        let report = apply(&path, &store()).expect("apply");
        assert_eq!(
            report.outcome,
            ConfigOutcome::Written(ConfigChange::Appended)
        );

        let written_back = std::fs::read_to_string(&path).expect("read back");
        assert!(
            written_back.starts_with(authored),
            "the user's own lines must survive verbatim, got {written_back:?}"
        );
        assert!(directs_to(&written_back, &store()));

        // A second run must change nothing: idempotence is the property that makes `setup` safe
        // to run repeatedly, and only a real file can prove the bytes are identical.
        let again = apply(&path, &store()).expect("apply twice");
        assert_eq!(again.outcome, ConfigOutcome::AlreadyCurrent);
        assert_eq!(
            std::fs::read_to_string(&path).expect("read back twice"),
            written_back
        );

        std::fs::remove_dir_all(&directory).ok();
    }

    /// The marker only claims a block it introduces at a line start; one quoted inside a value
    /// claims nothing.
    #[test]
    fn a_marker_inside_a_value_claims_nothing() {
        let existing = format!("[cache.disk]\ndir = \"/x/{OWNERSHIP_MARKER}\"\n");
        assert!(matches!(
            plan(Some(&existing), &store()),
            ConfigPlan::Refuse(ConfigConflict::ForeignDirectory { .. })
        ));
    }

    #[test]
    fn paths_are_escaped_so_no_byte_can_break_the_file() {
        let store = SharedStore::new(
            PathBuf::from("/tmp/quote\"and\\backslash"),
            ImageCapacity::from_gibibytes(40),
        );
        let ConfigPlan::Write { contents, .. } = plan(None, &store) else {
            panic!("expected a write");
        };
        assert!(directs_to(&contents, &store));
    }

    /// The path cowshed writes is the path sccache reads, including the legacy location sccache
    /// still prefers when a config already lives there. Writing the new path over an existing old
    /// one would shadow it rather than replace it, and silently move that user's cache.
    #[cfg(target_os = "macos")]
    #[test]
    fn resolution_follows_sccache_and_prefers_an_existing_legacy_config() {
        let home = std::env::temp_dir().join(format!(
            "cowshed-sccache-config-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let modern = home.join("Library/Application Support/Mozilla.sccache");
        let legacy = home.join("Library/Preferences/Mozilla.sccache");
        std::fs::create_dir_all(&legacy).expect("legacy directory");

        assert_eq!(client_config_path(&home), modern.join(CONFIG_LEAF));

        std::fs::write(legacy.join(CONFIG_LEAF), b"").expect("legacy config");
        assert_eq!(client_config_path(&home), legacy.join(CONFIG_LEAF));

        std::fs::create_dir_all(&modern).expect("modern directory");
        std::fs::write(modern.join(CONFIG_LEAF), b"").expect("modern config");
        assert_eq!(client_config_path(&home), modern.join(CONFIG_LEAF));

        std::fs::remove_dir_all(&home).expect("clean up");
    }
}
