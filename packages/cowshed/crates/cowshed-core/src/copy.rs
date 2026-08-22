//! Materialize a live repository checkout onto a freshly attached image volume.
//!
//! Adopt has two jobs: reproduce the checkout on the image with full fidelity,
//! and refuse to adopt a tree that is being written underneath it. Both are done
//! here, in-process. Shelling out to rsync failed at both, and not for
//! stylistic reasons:
//!
//! - `/usr/bin/rsync` on macOS 15+ is openrsync, whose itemized output cannot
//!   report an unchanged tree as unchanged once extended attributes are
//!   preserved. Measuring quiescence therefore cost a second full pass over the
//!   tree, and the cheap path existed only when some other rsync happened to
//!   shadow the system one on `PATH`.
//! - rsync grants owner-search to each destination directory it creates so it
//!   can populate it, and does not take it away again. A source directory
//!   without `u+x` therefore reports a permission difference on every pass, and
//!   the copy can never converge no matter how quiet the repository is.
//!
//! Both failures are one mistake: inferring *did the repository change?* from a
//! copier's log of *what the copier did*. Here the two questions are separate.
//! Quiescence is decided by comparing two stat snapshots of the source alone, so
//! it is independent of how the copy behaved. Fidelity is `copyfile(3)` — the
//! primitive behind `cp -c`, carrying data, mode, timestamps, ACLs, extended
//! attributes and resource forks — plus one ordering rule the copier owns:
//! directory metadata is applied after the directory's subtree is complete,
//! which is what lets a directory without owner-search be reproduced exactly.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::error::{CowshedError, Result};

const DEFAULT_PASS_BUDGET: usize = 6;
const CHURN_SAMPLE_LIMIT: usize = 8;
const CHURN_PATH_LIMIT: usize = 120;

/// Per-volume stores that APFS maintains for itself. They describe the volume
/// they live on, not the repository, and the image volume grows its own, so they
/// are neither copied from the source nor deleted from the destination.
const APFS_VOLUME_METADATA: [&str; 5] = [
    ".DocumentRevisions-V100",
    ".Spotlight-V100",
    ".TemporaryItems",
    ".Trashes",
    ".fseventsd",
];

/// Mode a destination directory holds while its subtree is being written.
///
/// The source mode is applied afterwards, so this never leaks into the adopted
/// image: it exists only so that populating a subtree does not depend on the
/// source directory granting owner write and search.
const DIRECTORY_STAGING_MODE: u32 = 0o700;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyReport {
    /// Source snapshots compared. One means the tree never moved.
    pub passes: usize,
    /// Source entries observed changing across passes.
    pub changed_entries: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum EntryKind {
    Directory,
    File,
    Symlink,
}

/// Everything about a source entry that a repository write would move.
///
/// `ctime` earns its place: a permission or extended-attribute write leaves
/// `size` and `mtime` untouched, and those are precisely the writes a
/// content-shaped comparison misses. `inode` catches replace-by-rename, where a
/// new file can land with a copied `mtime`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Entry {
    kind: EntryKind,
    mode: u32,
    size: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
    device: u64,
    inode: u64,
    links: u64,
}

/// The part of an entry the destination can be made to match.
///
/// Device, inode and change time belong to the volume an entry lives on, so they
/// can never agree across the two trees. Comparing only what a copy can actually
/// equalize is what lets a resumed adopt skip work instead of recopying
/// everything.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Mirrored {
    kind: EntryKind,
    mode: u32,
    size: u64,
    mtime: (i64, i64),
}

type Snapshot = BTreeMap<PathBuf, Entry>;
type MirrorState = BTreeMap<PathBuf, Mirrored>;

impl Entry {
    fn new(kind: EntryKind, metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt as _;
        Self {
            kind,
            mode: metadata.mode() & 0o7777,
            size: metadata.size(),
            mtime: (metadata.mtime(), metadata.mtime_nsec()),
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
            device: metadata.dev(),
            inode: metadata.ino(),
            links: metadata.nlink(),
        }
    }

    /// Whether this file is one of several names for a single inode.
    const fn is_hard_linked(&self) -> bool {
        matches!(self.kind, EntryKind::File) && self.links > 1
    }
}

impl From<&Entry> for Mirrored {
    fn from(entry: &Entry) -> Self {
        Self {
            kind: entry.kind,
            mode: entry.mode,
            size: entry.size,
            mtime: entry.mtime,
        }
    }
}

/// Copy `source` onto `destination` until a full pass observes an unchanged source.
pub fn copy_until_quiescent_blocking(source: &Path, destination: &Path) -> Result<CopyReport> {
    copy_with_budget_blocking(source, destination, DEFAULT_PASS_BUDGET)
}

pub fn copy_with_budget_blocking(
    source: &Path,
    destination: &Path,
    pass_budget: usize,
) -> Result<CopyReport> {
    if pass_budget == 0 {
        return Err(CowshedError::usage(
            "copy pass budget must be positive",
            "retry cowshed adopt without overriding the pass budget",
        ));
    }
    let (source, destination) = validate_copy_roots(source, destination)?;
    converge(&source, &destination, pass_budget, &mut snapshot)
}

/// Copy a live repository into an attached image until a pass observes no changes.
pub async fn copy_until_quiescent(source: &Path, destination: &Path) -> Result<CopyReport> {
    copy_with_budget(source, destination, DEFAULT_PASS_BUDGET).await
}

pub async fn copy_with_budget(
    source: &Path,
    destination: &Path,
    pass_budget: usize,
) -> Result<CopyReport> {
    let source = source.to_owned();
    let destination = destination.to_owned();
    tokio::task::spawn_blocking(move || {
        copy_with_budget_blocking(&source, &destination, pass_budget)
    })
    .await
    .map_err(|error| CowshedError::internal(format!("repository copy worker failed: {error}")))?
}

/// The convergence loop, with the source observation injected.
///
/// Taking `observe` as a parameter keeps the budget-exhaustion path testable
/// without a live writer racing the assertions: a churning repository is just an
/// observation that never repeats itself.
fn converge(
    source: &Path,
    destination: &Path,
    pass_budget: usize,
    observe: &mut dyn FnMut(&Path) -> Result<Snapshot>,
) -> Result<CopyReport> {
    let mut mirrored: MirrorState = snapshot(destination)?
        .iter()
        .map(|(path, entry)| (path.clone(), Mirrored::from(entry)))
        .collect();
    let mut observed = observe(source)?;
    let mut changed_entries = 0usize;
    let mut last_changes = Vec::new();

    for pass in 1..=pass_budget {
        reconcile(source, destination, &mut mirrored, &observed)?;
        let current = observe(source)?;
        let changes = describe_churn(&observed, &current);
        if changes.is_empty() {
            return Ok(CopyReport {
                passes: pass,
                changed_entries,
            });
        }
        changed_entries = changed_entries.saturating_add(changes.len());
        observed = current;
        last_changes = changes;
    }

    let sample = last_changes
        .iter()
        .take(CHURN_SAMPLE_LIMIT)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    Err(CowshedError::conflict(
        format!(
            "repository did not quiesce after {pass_budget} copy passes; still changing: {sample}"
        ),
        "stop the processes writing those paths, then retry cowshed adopt",
    ))
}

/// Bring `destination` into agreement with the `target` snapshot of `source`.
///
/// The four stages exist to make the orderings explicit, because every one of
/// them is load-bearing:
///
/// 1. Grant staging modes, so a directory that is about to be written into is
///    writable even if the source denies it.
/// 2. Remove what the source no longer has, deepest first, so directories are
///    empty when they are removed and a path that changed kind is gone before
///    its replacement is created.
/// 3. Create and copy, shallowest first, so a parent exists before its children.
/// 4. Apply directory metadata, deepest first, so a directory's mode and
///    timestamps are written after the last change inside it.
fn reconcile(
    source: &Path,
    destination: &Path,
    mirrored: &mut MirrorState,
    target: &Snapshot,
) -> Result<()> {
    let obsolete = obsolete_paths(mirrored, target);
    let outdated = outdated_paths(mirrored, target);
    let staging = staging_directories(target, &obsolete, &outdated);

    for relative in &staging {
        let path = destination.join(relative);
        if path.is_dir() {
            set_mode(&path, DIRECTORY_STAGING_MODE)?;
            if let Some(state) = mirrored.get_mut(relative) {
                state.mode = DIRECTORY_STAGING_MODE;
            }
        }
    }

    for relative in obsolete.iter().rev() {
        let kind = mirrored
            .get(relative)
            .map_or(EntryKind::File, |state| state.kind);
        remove(&destination.join(relative), kind)?;
        mirrored.remove(relative);
    }

    let mut hard_links: BTreeMap<(u64, u64), PathBuf> = BTreeMap::new();
    for (relative, entry) in target {
        if entry.is_hard_linked()
            && let Some(primary) = hard_links.get(&(entry.device, entry.inode))
        {
            // A second name for an inode already materialized: link it instead
            // of copying it. rsync was never asked to preserve hard links, so
            // adopt used to expand a `.git` object store or a package store into
            // independent copies, inflating the image against no benefit.
            if link_leaf(&destination.join(primary), &destination.join(relative))? {
                mirrored.insert(relative.clone(), Mirrored::from(entry));
            }
            continue;
        }
        if entry.is_hard_linked() {
            hard_links.insert((entry.device, entry.inode), relative.clone());
        }
        if !outdated.contains(relative) {
            continue;
        }
        let materialized = match entry.kind {
            EntryKind::Directory => {
                let path = destination.join(relative);
                if relative.as_os_str().is_empty() || path.is_dir() {
                    true
                } else {
                    create_directory(&path)?
                }
            }
            EntryKind::File | EntryKind::Symlink => {
                copy_leaf(&source.join(relative), &destination.join(relative))?
            }
        };
        if !materialized {
            // The entry vanished from the source mid-pass. That is churn, and
            // the next observation reports it; nothing here needs to fail.
            mirrored.remove(relative);
            continue;
        }
        let state = match entry.kind {
            EntryKind::Directory => Mirrored {
                mode: DIRECTORY_STAGING_MODE,
                ..Mirrored::from(entry)
            },
            EntryKind::File | EntryKind::Symlink => Mirrored::from(entry),
        };
        mirrored.insert(relative.clone(), state);
    }

    for relative in staging.iter().rev() {
        let Some(entry) = target.get(relative) else {
            continue;
        };
        if !apply_directory_metadata(&source.join(relative), &destination.join(relative))? {
            mirrored.remove(relative);
            continue;
        }
        mirrored.insert(relative.clone(), Mirrored::from(entry));
    }
    Ok(())
}

/// Destination paths the source no longer has, or has under a different kind.
fn obsolete_paths(mirrored: &MirrorState, target: &Snapshot) -> Vec<PathBuf> {
    mirrored
        .iter()
        .filter(|(path, state)| {
            target
                .get(*path)
                .is_none_or(|entry| entry.kind != state.kind)
        })
        .map(|(path, _)| path.clone())
        .collect()
}

/// Source paths the destination does not already reproduce.
fn outdated_paths(mirrored: &MirrorState, target: &Snapshot) -> BTreeSet<PathBuf> {
    target
        .iter()
        .filter(|(path, entry)| mirrored.get(*path) != Some(&Mirrored::from(*entry)))
        .map(|(path, _)| path.clone())
        .collect()
}

/// Directories that must be made writable, and whose metadata must be
/// reapplied: every directory that is itself out of date, plus every ancestor of
/// a path this pass will write or remove.
fn staging_directories(
    target: &Snapshot,
    obsolete: &[PathBuf],
    outdated: &BTreeSet<PathBuf>,
) -> BTreeSet<PathBuf> {
    let mut staging = BTreeSet::new();
    let mut note_ancestors = |path: &Path| {
        let mut current = path.parent();
        while let Some(ancestor) = current {
            if !staging.insert(ancestor.to_path_buf()) {
                break;
            }
            current = ancestor.parent();
        }
    };
    for path in obsolete {
        note_ancestors(path);
    }
    for path in outdated {
        note_ancestors(path);
    }
    for path in outdated {
        if target
            .get(path)
            .is_some_and(|entry| entry.kind == EntryKind::Directory)
        {
            staging.insert(path.clone());
        }
    }
    staging.retain(|path| {
        target
            .get(path)
            .is_some_and(|entry| entry.kind == EntryKind::Directory)
    });
    staging
}

/// Snapshot every entry under `root`, keyed by path relative to it.
///
/// The root itself is included under the empty path: its mode and timestamps are
/// part of the checkout too. Entries that disappear mid-walk are skipped rather
/// than failed — a vanishing path means the tree is moving, which is what the
/// next comparison is for.
fn snapshot(root: &Path) -> Result<Snapshot> {
    let metadata = fs::symlink_metadata(root).map_err(|error| read_error(root, &error))?;
    if !metadata.is_dir() {
        return Err(CowshedError::usage(
            format!("{} is not a directory", root.display()),
            "cowshed adopt <git-root>",
        ));
    }
    let mut entries = Snapshot::new();
    entries.insert(PathBuf::new(), Entry::new(EntryKind::Directory, &metadata));

    let mut pending = vec![PathBuf::new()];
    while let Some(relative) = pending.pop() {
        let directory = root.join(&relative);
        let reader = match fs::read_dir(&directory) {
            Ok(reader) => reader,
            Err(error) if vanished(&error) => continue,
            Err(error) => return Err(read_error(&directory, &error)),
        };
        for child in reader {
            let child = match child {
                Ok(child) => child,
                Err(error) if vanished(&error) => continue,
                Err(error) => return Err(read_error(&directory, &error)),
            };
            let name = child.file_name();
            if relative.as_os_str().is_empty() && is_apfs_volume_metadata(&name) {
                continue;
            }
            // `DirEntry::metadata` does not traverse symlinks, so a link is
            // recorded as a link rather than as whatever it points at.
            let metadata = match child.metadata() {
                Ok(metadata) => metadata,
                Err(error) if vanished(&error) => continue,
                Err(error) => return Err(read_error(&directory.join(&name), &error)),
            };
            // Sockets, FIFOs and device nodes are runtime artifacts of whatever
            // was running in the checkout, never repository content, and an
            // image volume mounted `nodev` could not carry them anyway.
            let Some(kind) = classify(&metadata) else {
                continue;
            };
            let path = relative.join(&name);
            if matches!(kind, EntryKind::Directory) {
                pending.push(path.clone());
            }
            entries.insert(path, Entry::new(kind, &metadata));
        }
    }
    Ok(entries)
}

fn classify(metadata: &fs::Metadata) -> Option<EntryKind> {
    let kind = metadata.file_type();
    if kind.is_dir() {
        Some(EntryKind::Directory)
    } else if kind.is_file() {
        Some(EntryKind::File)
    } else if kind.is_symlink() {
        Some(EntryKind::Symlink)
    } else {
        None
    }
}

fn is_apfs_volume_metadata(name: &OsStr) -> bool {
    APFS_VOLUME_METADATA
        .iter()
        .any(|reserved| name == OsStr::new(reserved))
}

/// Describe every entry that moved between two observations of the same tree.
fn describe_churn(previous: &Snapshot, current: &Snapshot) -> Vec<String> {
    let mut changes = Vec::new();
    for (path, entry) in current {
        match previous.get(path) {
            None => changes.push(format!("added {}", render_path(path))),
            Some(before) if before != entry => {
                changes.push(format!("changed {}", render_path(path)));
            }
            Some(_) => {}
        }
    }
    for path in previous.keys() {
        if !current.contains_key(path) {
            changes.push(format!("removed {}", render_path(path)));
        }
    }
    changes
}

/// Render a path for an error message: printable, bounded, and never empty.
fn render_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    if text.is_empty() {
        return ".".to_owned();
    }
    let mut rendered: String = text
        .chars()
        .take(CHURN_PATH_LIMIT)
        .map(|character| {
            if character.is_control() {
                '\u{fffd}'
            } else {
                character
            }
        })
        .collect();
    if text.chars().count() > CHURN_PATH_LIMIT {
        rendered.push('…');
    }
    rendered
}

fn validate_copy_roots(source: &Path, destination: &Path) -> Result<(PathBuf, PathBuf)> {
    let source = source.canonicalize().map_err(|error| {
        CowshedError::not_found(
            format!("cannot open source tree {}: {error}", source.display()),
            "cowshed adopt <existing-git-root>",
        )
    })?;
    let destination = destination.canonicalize().map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot open image mount {}: {error}", destination.display()),
            "cowshed doctor --json",
        )
    })?;

    if !source.is_dir() || !destination.is_dir() {
        return Err(CowshedError::usage(
            "adopt source and image destination must both be directories",
            "cowshed adopt <git-root>",
        ));
    }
    if destination.starts_with(&source) || source.starts_with(&destination) {
        return Err(CowshedError::conflict(
            "adopt copy roots overlap",
            "choose a cowshed store outside the repository tree",
        ));
    }
    Ok((source, destination))
}

/// Whether an error means the path is no longer there.
fn vanished(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::NotFound)
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    match fs::set_permissions(path, fs::Permissions::from_mode(mode)) {
        Ok(()) => Ok(()),
        Err(error) if vanished(&error) => Ok(()),
        Err(error) => Err(write_error(path, &error)),
    }
}

fn remove(path: &Path, kind: EntryKind) -> Result<()> {
    let removed = match kind {
        EntryKind::Directory => fs::remove_dir_all(path),
        EntryKind::File | EntryKind::Symlink => fs::remove_file(path),
    };
    match removed {
        Ok(()) => Ok(()),
        Err(error) if vanished(&error) => Ok(()),
        Err(error) => Err(write_error(path, &error)),
    }
}

/// Create a destination directory. Reports `false` if its parent vanished.
fn create_directory(path: &Path) -> Result<bool> {
    match fs::create_dir(path) {
        Ok(()) => set_mode(path, DIRECTORY_STAGING_MODE).map(|()| true),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            set_mode(path, DIRECTORY_STAGING_MODE).map(|()| true)
        }
        Err(error) if vanished(&error) => Ok(false),
        Err(error) => Err(write_error(path, &error)),
    }
}

/// Replace `destination` with a hard link to `primary`. Reports `false` if the
/// link target vanished.
fn link_leaf(primary: &Path, destination: &Path) -> Result<bool> {
    remove(destination, EntryKind::File)?;
    match fs::hard_link(primary, destination) {
        Ok(()) => Ok(true),
        Err(error) if vanished(&error) => Ok(false),
        Err(error) => Err(write_error(destination, &error)),
    }
}

/// Copy one file or symlink with every attribute it carries. Reports `false` if
/// the source vanished mid-pass.
fn copy_leaf(source: &Path, destination: &Path) -> Result<bool> {
    remove(destination, EntryKind::File)?;
    match copy_leaf_native(source, destination) {
        Ok(()) => Ok(true),
        Err(error) if vanished(&error) => Ok(false),
        Err(error) => Err(copy_error(source, destination, &error)),
    }
}

/// Apply a source directory's mode, timestamps, ACL and extended attributes to
/// the matching destination directory. Reports `false` if the source vanished.
fn apply_directory_metadata(source: &Path, destination: &Path) -> Result<bool> {
    match directory_metadata_native(source, destination) {
        Ok(()) => Ok(true),
        Err(error) if vanished(&error) => Ok(false),
        Err(error) => Err(copy_error(source, destination, &error)),
    }
}

#[cfg(target_os = "macos")]
mod native {
    use std::ffi::CString;
    use std::io;
    use std::path::Path;

    /// Data plus every attribute `copyfile(3)` knows how to carry.
    const COPYFILE_ALL: libc::copyfile_flags_t = libc::COPYFILE_METADATA | libc::COPYFILE_DATA;

    fn c_path(path: &Path) -> io::Result<CString> {
        use std::os::unix::ffi::OsStrExt as _;
        CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("path contains an interior NUL: {}", path.display()),
            )
        })
    }

    fn copyfile(
        source: &Path,
        destination: &Path,
        flags: libc::copyfile_flags_t,
    ) -> io::Result<()> {
        let source = c_path(source)?;
        let destination = c_path(destination)?;
        // SAFETY: both C strings outlive the call, and a null state requests the
        // default one-shot copy rather than a caller-managed one.
        let status = unsafe {
            libc::copyfile(
                source.as_ptr(),
                destination.as_ptr(),
                std::ptr::null_mut(),
                flags,
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// `COPYFILE_CLONE` asks for a copy-on-write clone and falls back to a full
    /// copy when the two paths are on different volumes, which they are during
    /// adopt. Keeping it costs nothing and makes a same-volume copy free.
    /// `COPYFILE_NOFOLLOW` copies a symlink as a symlink.
    pub fn copy_leaf(source: &Path, destination: &Path) -> io::Result<()> {
        copyfile(
            source,
            destination,
            COPYFILE_ALL | libc::COPYFILE_CLONE | libc::COPYFILE_NOFOLLOW,
        )
    }

    /// Metadata only: the directory already exists and its contents were written
    /// by this copier, so nothing but the directory's own attributes moves.
    pub fn directory_metadata(source: &Path, destination: &Path) -> io::Result<()> {
        copyfile(
            source,
            destination,
            libc::COPYFILE_METADATA | libc::COPYFILE_NOFOLLOW,
        )
    }
}

/// Elsewhere there is no `copyfile(3)`; carry what POSIX exposes. cowshed only
/// provisions APFS, so this path exists to keep the crate buildable and testable
/// off macOS, not to adopt a repository on another platform.
#[cfg(not(target_os = "macos"))]
mod native {
    use std::fs;
    use std::io;
    use std::path::Path;

    fn set_times(path: &Path, metadata: &fs::Metadata) -> io::Result<()> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        use std::os::unix::fs::MetadataExt as _;
        let target = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "interior NUL in path"))?;
        let times = [
            libc::timespec {
                tv_sec: metadata.atime(),
                tv_nsec: metadata.atime_nsec(),
            },
            libc::timespec {
                tv_sec: metadata.mtime(),
                tv_nsec: metadata.mtime_nsec(),
            },
        ];
        // SAFETY: the path string and the two-element array both outlive the call.
        let status = unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                target.as_ptr(),
                times.as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub fn copy_leaf(source: &Path, destination: &Path) -> io::Result<()> {
        let metadata = fs::symlink_metadata(source)?;
        if metadata.file_type().is_symlink() {
            std::os::unix::fs::symlink(fs::read_link(source)?, destination)?;
        } else {
            fs::copy(source, destination)?;
            fs::set_permissions(destination, metadata.permissions())?;
        }
        set_times(destination, &metadata)
    }

    pub fn directory_metadata(source: &Path, destination: &Path) -> io::Result<()> {
        let metadata = fs::symlink_metadata(source)?;
        fs::set_permissions(destination, metadata.permissions())?;
        set_times(destination, &metadata)
    }
}

fn copy_leaf_native(source: &Path, destination: &Path) -> io::Result<()> {
    native::copy_leaf(source, destination)
}

fn directory_metadata_native(source: &Path, destination: &Path) -> io::Result<()> {
    native::directory_metadata(source, destination)
}

fn read_error(path: &Path, error: &io::Error) -> CowshedError {
    CowshedError::conflict(
        format!("cannot read {}: {error}", path.display()),
        "make the path readable, then retry cowshed adopt",
    )
}

fn write_error(path: &Path, error: &io::Error) -> CowshedError {
    CowshedError::conflict(
        format!("cannot write {}: {error}", path.display()),
        "resolve the filesystem error and retry cowshed adopt",
    )
}

fn copy_error(source: &Path, destination: &Path, error: &io::Error) -> CowshedError {
    CowshedError::conflict(
        format!(
            "cannot copy {} to {}: {error}",
            source.display(),
            destination.display()
        ),
        "resolve the filesystem error and retry cowshed adopt",
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::MetadataExt as _;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        CopyReport, DIRECTORY_STAGING_MODE, EntryKind, Snapshot, converge, copy_with_budget,
        describe_churn, render_path, snapshot, validate_copy_roots,
    };

    fn temp_root(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cowshed-copy-{label}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create fixture root");
        root
    }

    fn copy_roots(label: &str) -> (PathBuf, PathBuf, PathBuf) {
        let root = temp_root(label);
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir(&source).expect("create source");
        fs::create_dir(&destination).expect("create destination");
        (root, source, destination)
    }

    /// Create a FIFO without shelling out: `mkfifo(1)` is not at a fixed path on
    /// every platform the tests run on, and NixOS has no `/usr/bin` at all.
    fn make_fifo(path: &Path) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        let c_path = CString::new(path.as_os_str().as_bytes()).expect("fifo path");
        // SAFETY: the C string outlives the call.
        let created = unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) };
        assert_eq!(created, 0, "mkfifo: {}", std::io::Error::last_os_error());
    }

    fn mode_of(path: &Path) -> u32 {
        fs::symlink_metadata(path).expect("stat").mode() & 0o7777
    }

    fn run(source: &Path, destination: &Path, budget: usize) -> CopyReport {
        converge(source, destination, budget, &mut snapshot).expect("copy converges")
    }

    #[cfg(target_os = "macos")]
    const TEST_XATTR: &str = "com.cowshed.test";

    #[cfg(target_os = "macos")]
    fn write_xattr(path: &Path, name: &str, value: &[u8]) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        let path = CString::new(path.as_os_str().as_bytes()).expect("path");
        let name = CString::new(name).expect("name");
        // SAFETY: both C strings outlive the call, and the value slice is passed
        // with its own length.
        let written = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
                0,
            )
        };
        assert_eq!(written, 0, "setxattr: {}", std::io::Error::last_os_error());
    }

    #[cfg(target_os = "macos")]
    fn read_xattr(path: &Path, name: &str) -> Option<Vec<u8>> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        let path = CString::new(path.as_os_str().as_bytes()).expect("path");
        let name = CString::new(name).expect("name");
        let mut buffer = vec![0u8; 64];
        // SAFETY: the buffer is valid for `buffer.len()` bytes for the call.
        let read = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                0,
                0,
            )
        };
        if read < 0 {
            return None;
        }
        buffer.truncate(usize::try_from(read).expect("non-negative length"));
        Some(buffer)
    }

    /// A directory whose mode denies owner-search is the case that made adopt
    /// unconvergeable: the copier must populate it and still leave the source
    /// mode behind, in one pass.
    #[test]
    fn reproduces_a_directory_that_denies_owner_search() {
        let (root, source, destination) = copy_roots("no-owner-search");
        fs::create_dir(source.join("sealed")).expect("create sealed directory");
        fs::write(source.join("sealed/inner"), b"inside\n").expect("write inside");
        fs::create_dir(source.join("empty")).expect("create empty directory");
        // 0o644 on a directory: readable, but not searchable.
        fs::set_permissions(source.join("empty"), fs::Permissions::from_mode(0o644))
            .expect("seal empty directory");
        fs::set_permissions(source.join("sealed"), fs::Permissions::from_mode(0o500))
            .expect("seal populated directory");

        let report = run(&source, &destination, 6);

        assert_eq!(
            report.passes, 1,
            "a still tree converges on the first observation"
        );
        assert_eq!(report.changed_entries, 0);
        assert_eq!(mode_of(&destination.join("empty")), 0o644);
        assert_eq!(
            mode_of(&destination.join("sealed")),
            0o500,
            "a populated directory keeps the source mode, not the staging mode"
        );
        assert_ne!(DIRECTORY_STAGING_MODE, 0o500);
        fs::set_permissions(
            destination.join("sealed"),
            fs::Permissions::from_mode(0o700),
        )
        .expect("unseal for teardown");
        fs::set_permissions(source.join("sealed"), fs::Permissions::from_mode(0o700))
            .expect("unseal for teardown");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// Re-running the copier over its own output must move nothing. Under
    /// openrsync this was the assertion that failed, because the copy pass
    /// reported every regular file as freshly transferred forever.
    #[test]
    fn an_already_mirrored_destination_converges_without_recopying() {
        let (root, source, destination) = copy_roots("already-mirrored");
        fs::create_dir_all(source.join("nested/deeper")).expect("create nested");
        fs::write(source.join("nested/deeper/file"), b"warm\n").expect("write file");

        let first = run(&source, &destination, 6);
        assert_eq!(first.passes, 1);

        let inode_before = fs::symlink_metadata(destination.join("nested/deeper/file"))
            .expect("stat copied file")
            .ino();
        let repeat = run(&source, &destination, 6);

        assert_eq!(repeat.passes, 1);
        assert_eq!(repeat.changed_entries, 0);
        assert_eq!(
            fs::symlink_metadata(destination.join("nested/deeper/file"))
                .expect("stat copied file")
                .ino(),
            inode_before,
            "an up-to-date file is left alone rather than recopied"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// `.git` object stores and package stores are full of hard links. Expanding
    /// them into independent copies inflates the image for nothing.
    #[test]
    fn preserves_hard_linked_files_as_a_single_inode() {
        let (root, source, destination) = copy_roots("hard-links");
        fs::create_dir(source.join("store")).expect("create store");
        fs::write(source.join("store/original"), b"shared bytes\n").expect("write original");
        fs::hard_link(source.join("store/original"), source.join("alias"))
            .expect("hard link the source");

        run(&source, &destination, 6);

        let original = fs::symlink_metadata(destination.join("store/original")).expect("stat");
        let alias = fs::symlink_metadata(destination.join("alias")).expect("stat");
        assert_eq!(
            (original.dev(), original.ino()),
            (alias.dev(), alias.ino()),
            "both names must resolve to one inode"
        );
        assert_eq!(alias.nlink(), 2);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// A live socket in the checkout — a dev server, a language server, a nix
    /// daemon — used to abort the whole adopt: rsync copies specials under `-a`,
    /// and recreating one under the staging mount root overruns `sun_path`'s 104
    /// bytes, so the copy died with `mkstempsock: Invalid argument`. Runtime
    /// artifacts are skipped, and their presence is not a reason to fail.
    #[test]
    fn skips_sockets_and_fifos_instead_of_failing_the_copy() {
        use std::os::unix::net::UnixListener;

        let (root, source, destination) = copy_roots("runtime-artifacts");
        fs::create_dir(source.join("run")).expect("create run directory");
        fs::write(source.join("run/kept"), b"content\n").expect("write regular file");
        // `bind` is the one filesystem call with a hard path-length ceiling:
        // `sun_path` is 104 bytes, and a fixture root under the temp directory
        // already spends most of them — the same ceiling that made copying a
        // socket into the staging mount impossible. So bind under a short
        // sibling of the fixture, which shares its filesystem, and rename in.
        // A rename from a fixed `/tmp` would cross devices wherever `TMPDIR`
        // points somewhere else.
        let staging = std::env::temp_dir().join(format!("cs{}", std::process::id()));
        fs::create_dir_all(&staging).expect("create socket staging directory");
        let staged_socket = staging.join("s");
        let _ = fs::remove_file(&staged_socket);
        let listener = UnixListener::bind(&staged_socket).expect("bind fixture socket");
        let socket = source.join("run/daemon.sock");
        fs::rename(&staged_socket, &socket).expect("move socket into the fixture");
        fs::remove_dir_all(&staging).expect("remove socket staging directory");
        make_fifo(&source.join("run/pipe"));

        let report = run(&source, &destination, 6);

        assert_eq!(report.passes, 1);
        assert_eq!(report.changed_entries, 0, "a still tree reports no churn");
        assert!(destination.join("run/kept").is_file());
        assert!(
            !destination.join("run/daemon.sock").exists(),
            "a socket is a runtime artifact, never repository content"
        );
        assert!(!destination.join("run/pipe").exists());
        drop(listener);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[tokio::test]
    async fn copies_content_symlinks_and_attributes_and_deletes_stale_destination_entries() {
        let (root, source, destination) = copy_roots("full-tree");
        fs::create_dir_all(source.join(".git/objects")).expect("create source git directory");
        fs::create_dir(source.join("nested")).expect("create source directory");
        fs::write(source.join(".git/HEAD"), b"ref: refs/heads/main\n")
            .expect("write source git metadata");
        fs::write(source.join("nested/file"), b"warm state\n").expect("write source file");
        fs::set_permissions(
            source.join("nested/file"),
            fs::Permissions::from_mode(0o640),
        )
        .expect("set source file mode");
        std::os::unix::fs::symlink("nested/file", source.join("link")).expect("create symlink");
        #[cfg(target_os = "macos")]
        write_xattr(&source.join("nested/file"), TEST_XATTR, b"warm");

        fs::write(destination.join("stale-secret"), b"remove me\n").expect("write stale file");
        fs::create_dir(destination.join("stale-directory")).expect("create stale directory");
        fs::write(destination.join("stale-directory/child"), b"also stale\n")
            .expect("write stale child");
        let volume_metadata = destination.join(".fseventsd");
        fs::create_dir(&volume_metadata).expect("create destination volume metadata");

        let report = copy_with_budget(&source, &destination, 3)
            .await
            .expect("copy reaches quiescence");

        assert_eq!(report.passes, 1);
        assert_eq!(
            fs::read(destination.join("nested/file")).expect("read copied file"),
            b"warm state\n"
        );
        assert_eq!(mode_of(&destination.join("nested/file")), 0o640);
        assert_eq!(
            fs::read(destination.join(".git/HEAD")).expect("read copied git metadata"),
            b"ref: refs/heads/main\n"
        );
        assert_eq!(
            fs::read_link(destination.join("link")).expect("read copied symlink"),
            Path::new("nested/file"),
            "a symlink is copied as a symlink, not as its target"
        );
        assert!(!destination.join("stale-secret").exists());
        assert!(!destination.join("stale-directory").exists());
        assert!(
            volume_metadata.is_dir(),
            "the image volume keeps its own APFS metadata"
        );

        #[cfg(target_os = "macos")]
        assert_eq!(
            read_xattr(&destination.join("nested/file"), TEST_XATTR).as_deref(),
            Some(&b"warm"[..]),
            "extended attributes ride along with the data"
        );

        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// The image volume's own root mode belongs to the checkout being adopted.
    #[test]
    fn applies_the_source_root_mode_to_the_destination_root() {
        let (root, source, destination) = copy_roots("root-mode");
        fs::set_permissions(&source, fs::Permissions::from_mode(0o750)).expect("set source root");

        run(&source, &destination, 6);

        assert_eq!(mode_of(&destination), 0o750);
        fs::set_permissions(&destination, fs::Permissions::from_mode(0o700))
            .expect("restore for teardown");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn a_tree_that_never_settles_is_a_conflict_naming_the_paths() {
        let (root, source, destination) = copy_roots("never-settles");
        fs::write(source.join("churn"), b"first\n").expect("write churn file");

        // Every observation reports a different tree, which is exactly what a
        // repository under active write looks like.
        let mut observation = 0usize;
        let mut observe = move |root: &Path| -> super::Result<Snapshot> {
            observation += 1;
            let mut tree = snapshot(root)?;
            if let Some(entry) = tree.get_mut(Path::new("churn")) {
                entry.size = entry.size.wrapping_add(observation as u64);
            }
            Ok(tree)
        };

        let error = converge(&source, &destination, 2, &mut observe)
            .expect_err("a churning tree cannot be adopted");

        assert_eq!(error.code.as_str(), "conflict");
        assert!(
            error
                .message
                .contains("did not quiesce after 2 copy passes"),
            "{}",
            error.message
        );
        assert!(error.message.contains("changed churn"), "{}", error.message);
        assert!(
            error
                .hint
                .contains("stop the processes writing those paths"),
            "{}",
            error.hint
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn churn_distinguishes_additions_removals_and_changes() {
        let root = temp_root("churn-shape");
        fs::write(root.join("kept"), b"same\n").expect("write kept");
        fs::write(root.join("edited"), b"before\n").expect("write edited");
        fs::write(root.join("gone"), b"doomed\n").expect("write gone");
        let before = snapshot(&root).expect("first observation");

        fs::write(root.join("edited"), b"after the edit\n").expect("rewrite edited");
        fs::remove_file(root.join("gone")).expect("remove gone");
        fs::write(root.join("fresh"), b"new\n").expect("write fresh");
        let after = snapshot(&root).expect("second observation");

        let changes = describe_churn(&before, &after);

        assert!(
            changes.contains(&"changed edited".to_owned()),
            "{changes:?}"
        );
        assert!(changes.contains(&"added fresh".to_owned()), "{changes:?}");
        assert!(changes.contains(&"removed gone".to_owned()), "{changes:?}");
        assert!(
            !changes.iter().any(|change| change.ends_with("kept")),
            "{changes:?}"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// A permission-only write moves neither size nor mtime, so a comparison
    /// shaped like a content diff would call a churning repository quiet.
    #[test]
    fn metadata_only_writes_count_as_churn() {
        let root = temp_root("metadata-churn");
        fs::write(root.join("file"), b"stable\n").expect("write file");
        let before = snapshot(&root).expect("first observation");

        fs::set_permissions(root.join("file"), fs::Permissions::from_mode(0o600))
            .expect("change mode");
        let after = snapshot(&root).expect("second observation");

        assert_eq!(describe_churn(&before, &after), ["changed file"]);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn snapshot_keys_the_root_under_the_empty_path_and_skips_volume_metadata() {
        let root = temp_root("snapshot-shape");
        fs::create_dir(root.join(".fseventsd")).expect("create volume metadata");
        fs::create_dir(root.join("nested")).expect("create nested");
        fs::write(root.join("nested/.fseventsd"), b"not volume metadata\n")
            .expect("write shadowed name");

        let tree = snapshot(&root).expect("snapshot");

        assert_eq!(
            tree.get(Path::new("")).map(|entry| entry.kind),
            Some(EntryKind::Directory)
        );
        assert!(!tree.contains_key(Path::new(".fseventsd")));
        assert!(
            tree.contains_key(Path::new("nested/.fseventsd")),
            "the exclusion is anchored at the volume root"
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn renders_the_root_and_bounds_long_paths() {
        assert_eq!(render_path(Path::new("")), ".");
        assert_eq!(render_path(Path::new("nested/file")), "nested/file");
        let long = "a".repeat(super::CHURN_PATH_LIMIT + 10);
        let rendered = render_path(Path::new(&long));
        assert!(rendered.ends_with('…'));
        assert_eq!(rendered.chars().count(), super::CHURN_PATH_LIMIT + 1);
    }

    #[test]
    fn rejects_a_zero_pass_budget() {
        let (root, source, destination) = copy_roots("zero-budget");
        let error = super::copy_with_budget_blocking(&source, &destination, 0)
            .expect_err("a zero budget cannot converge");
        assert_eq!(error.code.as_str(), "usage");
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn rejects_each_overlapping_root_boundary_and_accepts_disjoint_roots() {
        let root = temp_root("root-boundaries");
        let source = root.join("source");
        let source_child = source.join("child");
        let destination = root.join("destination");
        fs::create_dir(&source).expect("create source");
        fs::create_dir(&source_child).expect("create source child");
        fs::create_dir(&destination).expect("create destination");

        for (candidate_source, candidate_destination) in [
            (&source, &source),
            (&source, &source_child),
            (&source_child, &source),
        ] {
            let error = validate_copy_roots(candidate_source, candidate_destination)
                .expect_err("overlapping roots must fail");
            assert_eq!(error.code.as_str(), "conflict");
        }

        let (canonical_source, canonical_destination) =
            validate_copy_roots(&source, &destination).expect("siblings are disjoint");
        assert_eq!(
            canonical_source,
            source.canonicalize().expect("source root")
        );
        assert_eq!(
            canonical_destination,
            destination.canonicalize().expect("destination root")
        );
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn rejects_either_copy_root_when_it_is_not_a_directory() {
        let root = temp_root("file-boundaries");
        let directory = root.join("directory");
        let file = root.join("file");
        fs::create_dir(&directory).expect("create directory");
        fs::write(&file, b"not a directory").expect("create file");

        for (candidate_source, candidate_destination) in [(&file, &directory), (&directory, &file)]
        {
            let error = validate_copy_roots(candidate_source, candidate_destination)
                .expect_err("both roots must be directories");
            assert_eq!(error.code.as_str(), "usage");
        }
        fs::remove_dir_all(root).expect("remove fixture");
    }
}
