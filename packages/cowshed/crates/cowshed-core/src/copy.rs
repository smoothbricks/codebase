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
use std::thread;

use crate::error::{CowshedError, Result};

const DEFAULT_PASS_BUDGET: usize = 6;
const CHURN_SAMPLE_LIMIT: usize = 8;
const CHURN_PATH_LIMIT: usize = 120;

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
    /// Regular files materialized with copy-on-write.
    pub cloned_files: usize,
    /// Leaves materialized by the explicit data-copy fallback.
    pub copied_files: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CopyCounts {
    cloned_files: usize,
    copied_files: usize,
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
    allocated_bytes: u64,
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
    /// Destination identity observed in the paid snapshot; unknown after a fresh leaf copy
    /// until a hard-link operation needs it. This is never compared to the source's inode.
    identity: Option<(u64, u64)>,
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
            allocated_bytes: metadata.blocks().saturating_mul(512),
            mtime: (metadata.mtime(), metadata.mtime_nsec()),
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
            device: metadata.dev(),
            inode: metadata.ino(),
            links: metadata.nlink(),
        }
    }

    /// Writeback and compression can move st_blocks without a repository write.
    /// Keep allocation for fidelity/accounting, but judge quiescence by source identity and metadata.
    fn same_source(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.mode == other.mode
            && self.size == other.size
            && self.mtime == other.mtime
            && self.ctime == other.ctime
            && self.device == other.device
            && self.inode == other.inode
            && self.links == other.links
    }

    /// Whether this file is one of several names for a single inode.
    const fn is_hard_linked(&self) -> bool {
        matches!(self.kind, EntryKind::File) && self.links > 1
    }
}

/// Whether two observations of one tree name the same entries with the same source identity.
/// Allocation is left out: writeback and compression move st_blocks with no repository write
/// (02_workspaces.md, adoption step 3), so a tree that only gained a block has not changed.
fn same_tree(left: &Snapshot, right: &Snapshot) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left_path, left), (right_path, right))| {
                left_path == right_path && left.same_source(right)
            })
}

impl From<&Entry> for Mirrored {
    fn from(entry: &Entry) -> Self {
        Self {
            kind: entry.kind,
            mode: entry.mode,
            size: entry.size,
            mtime: entry.mtime,
            identity: None,
        }
    }
}

impl Mirrored {
    fn from_destination(entry: &Entry) -> Self {
        Self {
            identity: (entry.kind == EntryKind::File).then_some((entry.device, entry.inode)),
            ..Self::from(entry)
        }
    }

    fn matches(&self, entry: &Entry) -> bool {
        self.kind == entry.kind
            && self.mode == entry.mode
            && self.size == entry.size
            && self.mtime == entry.mtime
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

/// Content evidence and regular-file allocations observed in the same verification walk.
/// Hard-link aliases are counted once. Separate CoW-sharing inodes still have their own
/// st_blocks values; these are file allocations, not an estimate of unique host-device blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CopyVerification {
    pub digest: crate::api::Sha256Digest,
    pub source_allocated_bytes: u64,
    pub copied_allocated_bytes: u64,
}

/// A regular file's identity and mutation witness. Allocation changes alone are not mutations.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CopyFileIdentity {
    device: u64,
    inode: u64,
    size: u64,
    mode: u32,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

impl CopyFileIdentity {
    pub(crate) fn from_metadata(metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt as _;
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            size: metadata.len(),
            mode: metadata.mode(),
            mtime: (metadata.mtime(), metadata.mtime_nsec()),
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
    pub(crate) fn read(file: &fs::File) -> io::Result<Self> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cross-filesystem recovery requires a regular file",
            ));
        }
        Ok(Self::from_metadata(&metadata))
    }

    pub(crate) fn same_inode(&self, other: &Self) -> bool {
        self.device == other.device && self.inode == other.inode
    }

    /// A rename may change ctime; byte verification is still required after this comparison.
    pub(crate) fn after_rename(&self, other: &Self) -> bool {
        self.same_inode(other)
            && self.size == other.size
            && self.mode == other.mode
            && self.mtime == other.mtime
    }
}

/// Copy only into a newly created, held regular file. The caller owns publication and durability.
/// Both descriptors bypass the page cache: a recovery image passes through once, and caching it
/// evicts what the rest of the host is using. The native copy writes the source's data regions
/// only, so a hole is neither read nor written.
pub(crate) fn copy_held_file(source: &fs::File, destination: &fs::File) -> io::Result<()> {
    CopyFileIdentity::read(source)?;
    CopyFileIdentity::read(destination)?;
    native::bypass_cache(source)?;
    native::bypass_cache(destination)?;
    native::copy_regular(source, destination)
}

/// Compare held bytes, ownership and applicable ACL/xattrs; retain exact mutation witnesses.
pub(crate) fn verify_held_file(
    source: &fs::File,
    destination: &fs::File,
) -> io::Result<crate::api::Sha256Digest> {
    use std::os::unix::fs::MetadataExt as _;
    let before = CopyFileIdentity::read(source)?;
    let copied = CopyFileIdentity::read(destination)?;
    if before.size != copied.size || before.mode != copied.mode || before.mtime != copied.mtime {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "copied file metadata differs",
        ));
    }
    let original_metadata = source.metadata()?;
    let copied_metadata = destination.metadata()?;
    if original_metadata.uid() != copied_metadata.uid()
        || original_metadata.gid() != copied_metadata.gid()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "copied ownership differs: source uid {} gid {}, destination uid {} gid {}",
                original_metadata.uid(),
                original_metadata.gid(),
                copied_metadata.uid(),
                copied_metadata.gid(),
            ),
        ));
    }
    native::verify_metadata(source, destination)?;
    if copied_metadata.blocks().saturating_mul(512)
        > original_metadata
            .blocks()
            .saturating_mul(512)
            .saturating_add(65536)
        && original_metadata.blocks().saturating_mul(512) < before.size
        && native::has_holes(source, before.size)?
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sparse holes were materialized",
        ));
    }
    native::bypass_cache(source)?;
    native::bypass_cache(destination)?;
    let digest = compare_file_bytes(source, destination, before.size)?;
    if CopyFileIdentity::read(source)? != before || CopyFileIdentity::read(destination)? != copied {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source or destination changed during verification",
        ));
    }
    Ok(digest)
}

/// The bytes `compare_file_bytes` reads, compares and hashes per positional transfer.
const COMPARE_CHUNK_BYTES: usize = 1 << 20;

/// What a hole reads as, compared and hashed without a read.
static ZEROS: [u8; COMPARE_CHUNK_BYTES] = [0; COMPARE_CHUNK_BYTES];

/// What one file has over a [`Run`]: data to read, or a hole, which reads as zeros.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Content {
    Data,
    Hole,
}

/// A stretch `[start, end)` of the compared range over which each file is all data or all hole.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Run {
    start: u64,
    end: u64,
    source: Content,
    destination: Content,
}

/// One file's place among its data regions: the first region ending past the walk's offset, or
/// `None` once only a hole remains.
pub(crate) struct RegionCursor<'a> {
    file: &'a fs::File,
    region: Option<(u64, u64)>,
}

impl<'a> RegionCursor<'a> {
    pub(crate) fn new(file: &'a fs::File, length: u64) -> io::Result<Self> {
        Ok(Self {
            file,
            region: native::next_data_region(file, 0, length)?,
        })
    }

    /// The current or next data region for a monotonically increasing offset and fixed length.
    pub(crate) fn region_at(&mut self, offset: u64, length: u64) -> io::Result<Option<(u64, u64)>> {
        advance_data_region(&mut self.region, offset, |offset| {
            native::next_data_region(self.file, offset, length)
        })
    }
    /// What the file has from `offset` on, and where that ends.
    fn at(&mut self, offset: u64, length: u64) -> io::Result<(Content, u64)> {
        Ok(match self.region_at(offset, length)? {
            None => (Content::Hole, length),
            Some((start, _)) if start > offset => (Content::Hole, start),
            Some((_, end)) => (Content::Data, end),
        })
    }
}

fn advance_data_region(
    region: &mut Option<(u64, u64)>,
    offset: u64,
    probe: impl FnOnce(u64) -> io::Result<Option<(u64, u64)>>,
) -> io::Result<Option<(u64, u64)>> {
    if region.is_some_and(|(_, end)| end <= offset) {
        *region = probe(offset)?;
    }
    Ok(*region)
}

/// The runs of `[0, length)` in ascending order. Each ends at the nearer of the two files' next
/// data or hole boundary, so one SEEK_DATA/SEEK_HOLE pair per data region of either file plans the
/// whole comparison. A failed observation ends the walk: nothing after it is known.
struct Runs<'a> {
    source: RegionCursor<'a>,
    destination: RegionCursor<'a>,
    offset: u64,
    length: u64,
}

impl<'a> Runs<'a> {
    fn new(source: &'a fs::File, destination: &'a fs::File, length: u64) -> io::Result<Self> {
        Ok(Self {
            source: RegionCursor::new(source, length)?,
            destination: RegionCursor::new(destination, length)?,
            offset: 0,
            length,
        })
    }

    fn run_at(&mut self, start: u64) -> io::Result<Run> {
        let (source, source_end) = self.source.at(start, self.length)?;
        let (destination, destination_end) = self.destination.at(start, self.length)?;
        Ok(Run {
            start,
            end: source_end.min(destination_end),
            source,
            destination,
        })
    }
}

impl Iterator for Runs<'_> {
    type Item = io::Result<Run>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.length {
            return None;
        }
        let run = self.run_at(self.offset);
        self.offset = run.as_ref().map_or(self.length, |run| run.end);
        Some(run)
    }
}

/// The bytes `file` has over `[offset, offset + buffer.len())` inside one run: read when it has
/// data there, the static zeros of its hole otherwise.
fn run_bytes<'a>(
    file: &fs::File,
    content: Content,
    buffer: &'a mut [u8],
    offset: u64,
) -> io::Result<&'a [u8]> {
    use std::os::unix::fs::FileExt as _;
    match content {
        Content::Data => {
            file.read_exact_at(buffer, offset)?;
            Ok(buffer)
        }
        Content::Hole => Ok(&ZEROS[..buffer.len()]),
    }
}

/// SHA-256 of the `length` logical bytes both files must hold identically, reading only where a
/// file has data. A hole is compared and hashed from static zeros and read from neither file, so
/// a run both files hold as holes costs no read at all. The digest still covers every logical
/// byte, holes included: it is recorded evidence (the recovery journal's `published` digest and
/// adopt's verified-copy content line). Every transfer is positional, so the descriptors' offsets
/// are irrelevant; callers have matched both lengths to `length` and recheck identities after.
fn compare_file_bytes(
    source: &fs::File,
    destination: &fs::File,
    length: u64,
) -> io::Result<crate::api::Sha256Digest> {
    use sha2::{Digest as _, Sha256};
    use std::os::unix::fs::FileExt as _;
    let mut digest = Sha256::new();
    let mut source_bytes = vec![0_u8; COMPARE_CHUNK_BYTES];
    let mut destination_bytes = vec![0_u8; COMPARE_CHUNK_BYTES];
    for run in Runs::new(source, destination, length)? {
        let run = run?;
        let mut position = run.start;
        while position < run.end {
            let count = usize::try_from(run.end - position)
                .map_or(COMPARE_CHUNK_BYTES, |rest| rest.min(COMPARE_CHUNK_BYTES));
            let held = run_bytes(source, run.source, &mut source_bytes[..count], position)?;
            let copied = run_bytes(
                destination,
                run.destination,
                &mut destination_bytes[..count],
                position,
            )?;
            if held != copied {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "file content differs",
                ));
            }
            digest.update(held);
            position +=
                u64::try_from(count).map_err(|_| io::Error::other("read length exceeds u64"))?;
        }
    }
    let mut extra = [0];
    if source.read_at(&mut extra, length)? != 0 || destination.read_at(&mut extra, length)? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file grew during verification",
        ));
    }
    Ok(crate::api::Sha256Digest::from_bytes(
        digest.finalize().into(),
    ))
}
/// One hard-linked inode already byte-compared in this walk: the destination inode it was
/// compared against, and the digest of its payload, reused for every other name of that inode.
struct VerifiedAlias {
    copied_inode: (u64, u64),
    digest: crate::api::Sha256Digest,
}

/// Verify the copied namespace, metadata, symlink targets and every regular-file byte before
/// adoption stamps its own marker and credentials. Reuse the copier's volume-metadata and
/// runtime-artifact exclusions; never follow a copied symlink while reading file content.
pub(crate) fn verify_tree_blocking(source: &Path, destination: &Path) -> Result<CopyVerification> {
    verify_with_snapshot(source, destination, snapshot)
}

/// Compare raw Git-listed paths without running attributes or external filters. A listed
/// directory (a Git link/submodule) includes its complete subtree, including ignored bytes.
#[cfg(any(target_os = "macos", test))]
pub(crate) fn verify_relative_paths_blocking(
    source: &Path,
    destination: &Path,
    paths: &[PathBuf],
) -> Result<()> {
    verify_with_snapshot(source, destination, |root| selected_snapshot(root, paths)).map(|_| ())
}

#[cfg(any(target_os = "macos", test))]
fn selected_snapshot(root: &Path, paths: &[PathBuf]) -> Result<Snapshot> {
    let mut entries = Snapshot::new();
    for relative in paths {
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(CowshedError::integrity(
                "Git-listed content path is not a relative repository path",
                "repair the retained index before retrying",
            ));
        }
        let mut ancestor = PathBuf::new();
        let mut parts = relative.components().peekable();
        let mut missing_ancestor = false;
        while let Some(part) = parts.next() {
            if parts.peek().is_none() {
                break;
            }
            ancestor.push(part.as_os_str());
            let path = root.join(&ancestor);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_dir() => {}
                Ok(_) => {
                    return Err(CowshedError::integrity(
                        format!(
                            "Git-listed content path traverses a non-directory: {}",
                            path.display()
                        ),
                        "repair the retained worktree before retrying",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    missing_ancestor = true;
                    break;
                }
                Err(error) => return Err(read_error(&path, &error)),
            }
        }
        if missing_ancestor {
            continue;
        }
        let path = root.join(relative);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(read_error(&path, &error)),
        };
        let kind = classify(&metadata).ok_or_else(|| {
            CowshedError::integrity(
                format!(
                    "Git-listed content is not a file, link or directory: {}",
                    path.display()
                ),
                "repair the retained worktree before retrying",
            )
        })?;
        if kind == EntryKind::Directory {
            for (child, entry) in snapshot(&path)? {
                entries.insert(relative.join(child), entry);
            }
        } else {
            entries.insert(relative.clone(), Entry::new(kind, &metadata));
        }
    }
    Ok(entries)
}

fn verify_with_snapshot(
    source: &Path,
    destination: &Path,
    observe: impl Fn(&Path) -> Result<Snapshot>,
) -> Result<CopyVerification> {
    use sha2::{Digest, Sha256};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;

    let before = observe(source)?;
    let copied = observe(destination)?;
    let mismatch = |relative: &Path, reason: &str| {
        CowshedError::integrity(
            format!("copied tree differs at {}: {reason}", render_path(relative)),
            "keep the source tree and repair the copy before re-running adoption",
        )
    };
    if before.keys().ne(copied.keys()) {
        return Err(CowshedError::integrity(
            "copied tree has a different path set",
            "keep the source tree and repair the copy before re-running adoption",
        ));
    }
    let mut digest = Sha256::new();
    let mut source_allocated_bytes = 0_u64;
    let mut copied_allocated_bytes = 0_u64;
    let mut aliases: BTreeMap<(u64, u64), VerifiedAlias> = BTreeMap::new();
    for (relative, original) in &before {
        let copy = copied
            .get(relative)
            .ok_or_else(|| mismatch(relative, "missing path"))?;
        if original.kind != copy.kind || original.mode != copy.mode || original.mtime != copy.mtime
        {
            return Err(mismatch(relative, "type, mode or modification time"));
        }
        if original.kind != EntryKind::Directory && original.size != copy.size {
            return Err(mismatch(relative, "file length"));
        }
        let path_bytes = relative.as_os_str().as_bytes();
        let path_length = u64::try_from(path_bytes.len())
            .map_err(|_| CowshedError::internal("path length exceeds the digest framing"))?;
        digest.update(path_length.to_le_bytes());
        digest.update(path_bytes);
        digest.update(original.mode.to_le_bytes());
        digest.update(original.mtime.0.to_le_bytes());
        digest.update(original.mtime.1.to_le_bytes());
        let source_path = source.join(relative);
        let destination_path = destination.join(relative);
        match original.kind {
            EntryKind::Directory => digest.update([0]),
            EntryKind::Symlink => {
                digest.update([1]);
                let target = fs::read_link(&source_path)
                    .map_err(|error| read_error(&source_path, &error))?;
                let copied_target = fs::read_link(&destination_path)
                    .map_err(|error| read_error(&destination_path, &error))?;
                if target != copied_target {
                    return Err(mismatch(relative, "symlink target"));
                }
                digest.update(original.size.to_le_bytes());
                digest.update(target.as_os_str().as_bytes());
            }
            EntryKind::File => {
                digest.update([2]);
                digest.update(original.size.to_le_bytes());
                let source_inode = (original.device, original.inode);
                let copied_inode = (copy.device, copy.inode);
                if let Some(VerifiedAlias {
                    copied_inode: verified_inode,
                    digest: payload_digest,
                }) = aliases.get(&source_inode)
                {
                    if *verified_inode != copied_inode {
                        return Err(mismatch(relative, "hard-link relationship"));
                    }
                    // This same immutable inode was already compared byte-for-byte. Record its
                    // digest for this name without rereading either hard-link alias.
                    digest.update(payload_digest.as_bytes());
                    continue;
                }
                source_allocated_bytes = source_allocated_bytes
                    .checked_add(original.allocated_bytes)
                    .ok_or_else(|| CowshedError::internal("source allocation sum overflow"))?;
                copied_allocated_bytes =
                    copied_allocated_bytes
                        .checked_add(copy.allocated_bytes)
                        .ok_or_else(|| CowshedError::internal("copied allocation sum overflow"))?;
                let open = |path: &Path| {
                    fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                        .open(path)
                        .map_err(|error| read_error(path, &error))
                };
                let source_file = open(&source_path)?;
                let destination_file = open(&destination_path)?;
                native::bypass_cache(&source_file)
                    .map_err(|error| read_error(&source_path, &error))?;
                native::bypass_cache(&destination_file)
                    .map_err(|error| read_error(&destination_path, &error))?;
                if copy.allocated_bytes > original.allocated_bytes.saturating_add(65536)
                    && original.allocated_bytes < original.size
                    && native::has_holes(&source_file, original.size)
                        .map_err(|error| read_error(&source_path, &error))?
                {
                    return Err(mismatch(relative, "sparse holes were materialized"));
                }
                let payload_digest =
                    compare_file_bytes(&source_file, &destination_file, original.size)
                        .map_err(|error| mismatch(relative, &error.to_string()))?;
                digest.update(payload_digest.as_bytes());
                if original.is_hard_linked() {
                    aliases.insert(
                        source_inode,
                        VerifiedAlias {
                            copied_inode,
                            digest: payload_digest,
                        },
                    );
                }
            }
        }
    }
    if !same_tree(&observe(source)?, &before) || !same_tree(&observe(destination)?, &copied) {
        return Err(CowshedError::conflict(
            "source or copied tree changed during verification",
            "stop the writers and re-run adoption",
        ));
    }
    Ok(CopyVerification {
        digest: crate::api::Sha256Digest::from_bytes(digest.finalize().into()),
        source_allocated_bytes,
        copied_allocated_bytes,
    })
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
        .map(|(path, entry)| (path.clone(), Mirrored::from_destination(entry)))
        .collect();
    let mut observed = observe(source)?;
    let mut changed_entries = 0usize;
    let mut last_changes = Vec::new();
    let mut materialized = CopyCounts::default();

    for pass in 1..=pass_budget {
        let copied = reconcile(source, destination, &mut mirrored, &observed)?;
        materialized.cloned_files = materialized
            .cloned_files
            .saturating_add(copied.cloned_files);
        materialized.copied_files = materialized
            .copied_files
            .saturating_add(copied.copied_files);
        let current = observe(source)?;
        let changes = describe_churn(&observed, &current);
        if changes.is_empty() {
            return Ok(CopyReport {
                passes: pass,
                changed_entries,
                cloned_files: materialized.cloned_files,
                copied_files: materialized.copied_files,
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
) -> Result<CopyCounts> {
    let obsolete = obsolete_paths(mirrored, target);
    let outdated = outdated_paths(mirrored, target);

    // Plan every leaf mutation before staging directory metadata. Correct alias groups are
    // already known from the destination snapshot and need neither stat nor relink.
    let mut hard_link_primaries: BTreeMap<(u64, u64), PathBuf> = BTreeMap::new();
    let mut hard_link_aliases = Vec::new();
    let mut leaf_copies = Vec::new();
    for (relative, entry) in target {
        if entry.kind == EntryKind::Directory {
            continue;
        }
        if entry.is_hard_linked() {
            let key = (entry.device, entry.inode);
            if let Some(primary) = hard_link_primaries.get(&key) {
                let primary_identity = mirrored.get(primary).and_then(|state| state.identity);
                let alias_identity = mirrored.get(relative).and_then(|state| state.identity);
                if outdated.contains(primary)
                    || outdated.contains(relative)
                    || primary_identity.is_none()
                    || primary_identity != alias_identity
                {
                    hard_link_aliases.push(HardLinkAlias {
                        primary: primary.clone(),
                        relative: relative.clone(),
                        state: Mirrored::from(entry),
                    });
                }
                continue;
            }
            hard_link_primaries.insert(key, relative.clone());
        }
        if outdated.contains(relative) {
            leaf_copies.push(LeafCopy {
                relative: relative.clone(),
                state: Mirrored::from(entry),
            });
        }
    }
    let staging = staging_directories(target, &obsolete, &outdated, &hard_link_aliases);
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
    for (relative, entry) in target {
        if entry.kind != EntryKind::Directory || !outdated.contains(relative) {
            continue;
        }
        let path = destination.join(relative);
        let materialized =
            relative.as_os_str().is_empty() || path.is_dir() || create_directory(&path)?;
        if materialized {
            mirrored.insert(
                relative.clone(),
                Mirrored {
                    mode: DIRECTORY_STAGING_MODE,
                    ..Mirrored::from(entry)
                },
            );
        } else {
            mirrored.remove(relative);
        }
    }
    let mut copied = CopyCounts::default();
    for outcome in copy_leaves_parallel(source, destination, &leaf_copies)? {
        if let Some(materialization) = outcome.materialization {
            match materialization {
                native::LeafMaterialization::Cloned => copied.cloned_files += 1,
                native::LeafMaterialization::Copied => copied.copied_files += 1,
            }
            mirrored.insert(outcome.relative, outcome.state);
        } else {
            mirrored.remove(&outcome.relative);
        }
    }
    // A recopied primary changes identity. Repair its aliases after workers settle, and
    // observe that primary only once; subsequent aliases share the same known inode.
    for alias in hard_link_aliases {
        if link_leaf(
            &destination.join(&alias.primary),
            &destination.join(&alias.relative),
        )? {
            let identity = match mirrored
                .get(&alias.primary)
                .and_then(|state| state.identity)
            {
                Some(identity) => identity,
                None => {
                    use std::os::unix::fs::MetadataExt as _;
                    let primary = destination.join(&alias.primary);
                    let metadata = fs::symlink_metadata(&primary)
                        .map_err(|error| read_error(&primary, &error))?;
                    let identity = (metadata.dev(), metadata.ino());
                    if let Some(state) = mirrored.get_mut(&alias.primary) {
                        state.identity = Some(identity);
                    }
                    identity
                }
            };
            mirrored.insert(
                alias.relative,
                Mirrored {
                    identity: Some(identity),
                    ..alias.state
                },
            );
        } else {
            mirrored.remove(&alias.relative);
        }
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
    Ok(copied)
}

struct LeafCopy {
    relative: PathBuf,
    state: Mirrored,
}

struct LeafCopyOutcome {
    relative: PathBuf,
    state: Mirrored,
    materialization: Option<native::LeafMaterialization>,
}

struct HardLinkAlias {
    primary: PathBuf,
    relative: PathBuf,
    state: Mirrored,
}

fn copy_worker_count(job_count: usize) -> usize {
    if job_count == 0 {
        return 0;
    }
    thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(job_count)
}

/// Clone independent leaves with one bounded worker per available core.
///
/// Each worker gets a stable strided slice of the snapshot rather than taking work through a
/// mutex. The source and destination paths are immutable, and aliases are deliberately excluded:
/// their cross-subtree inode dependency is resolved after all workers join.
fn copy_leaves_parallel(
    source: &Path,
    destination: &Path,
    leaves: &[LeafCopy],
) -> Result<Vec<LeafCopyOutcome>> {
    let worker_count = copy_worker_count(leaves.len());
    if worker_count == 0 {
        return Ok(Vec::new());
    }

    thread::scope(|scope| {
        let mut workers = Vec::with_capacity(worker_count);
        for worker in 0..worker_count {
            workers.push(scope.spawn(move || {
                let mut outcomes = Vec::with_capacity(leaves.len().div_ceil(worker_count));
                for leaf in leaves.iter().skip(worker).step_by(worker_count) {
                    outcomes.push(LeafCopyOutcome {
                        relative: leaf.relative.clone(),
                        state: leaf.state,
                        materialization: copy_leaf(
                            &source.join(&leaf.relative),
                            &destination.join(&leaf.relative),
                        )?,
                    });
                }
                Ok::<_, CowshedError>(outcomes)
            }));
        }

        let mut outcomes = Vec::with_capacity(leaves.len());
        for worker in workers {
            let batch = worker
                .join()
                .map_err(|_| CowshedError::internal("repository copy worker panicked"))??;
            outcomes.extend(batch);
        }
        Ok(outcomes)
    })
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
        .filter(|(path, entry)| {
            mirrored
                .get(*path)
                .is_none_or(|state| !state.matches(entry))
        })
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
    aliases: &[HardLinkAlias],
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
    for alias in aliases {
        note_ancestors(&alias.relative);
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

/// The volume's own bookkeeping ([`cowshed_apfs::VOLUME_METADATA`], and the Spotlight opt-out
/// [`cowshed_apfs::SPOTLIGHT_OPT_OUT`] older volumes carry): it is neither copied from the source
/// nor deleted from the destination.
fn is_apfs_volume_metadata(name: &OsStr) -> bool {
    cowshed_apfs::VOLUME_METADATA
        .iter()
        .chain([&cowshed_apfs::SPOTLIGHT_OPT_OUT])
        .any(|reserved| name == OsStr::new(reserved))
}

/// Describe every entry that moved between two observations of the same tree.
fn describe_churn(previous: &Snapshot, current: &Snapshot) -> Vec<String> {
    let mut changes = Vec::new();
    for (path, entry) in current {
        match previous.get(path) {
            None => changes.push(format!("added {}", render_path(path))),
            Some(before) if !before.same_source(entry) => {
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

/// Copy one file or symlink with every attribute it carries. `None` means the source vanished
/// mid-pass.
fn copy_leaf(source: &Path, destination: &Path) -> Result<Option<native::LeafMaterialization>> {
    remove(destination, EntryKind::File)?;
    match copy_leaf_native(source, destination) {
        Ok(materialization) => Ok(Some(materialization)),
        Err(error) if vanished(&error) => Ok(None),
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

pub(crate) mod native;

fn copy_leaf_native(source: &Path, destination: &Path) -> io::Result<native::LeafMaterialization> {
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

    #[test]
    fn region_probes_follow_regions_not_extents() {
        use std::cell::Cell;

        for extents in [1_u64, 16, 4096] {
            let regions = [(2, extents + 2), (extents + 4, 2 * extents + 4)];
            let layouts: [&[(u64, u64)]; 3] = [&regions[..1], &regions, &[]];
            for layout in layouts {
                let probes = Cell::new(0_usize);
                let probe = |offset| {
                    probes.set(probes.get() + 1);
                    Ok(layout
                        .iter()
                        .copied()
                        .find(|&(_, end)| end > offset)
                        .map(|(start, end)| (start.max(offset), end)))
                };
                let mut region = probe(0).expect("initial region");
                // Each unit step represents a separate physical extent, including steps in holes.
                for offset in 0..2 * extents + 8 {
                    let actual = super::advance_data_region(&mut region, offset, probe)
                        .expect("advance region")
                        .map(|(start, end)| (start.max(offset), end));
                    let expected = layout
                        .iter()
                        .copied()
                        .find(|&(_, end)| end > offset)
                        .map(|(start, end)| (start.max(offset), end));
                    assert_eq!(actual, expected, "layout {layout:?}, offset {offset}");
                }
                assert_eq!(
                    probes.get(),
                    layout.len() + 1,
                    "one probe per region plus EOF, independent of {extents} extents: {layout:?}"
                );
            }
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!(
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

    #[cfg(target_os = "macos")]
    #[test]
    fn same_volume_regular_file_copy_is_a_clone() {
        let (root, source, destination) = copy_roots("clone-guard");
        fs::write(source.join("large"), vec![0x5a; 1024 * 1024]).expect("write source");

        let report = run(&source, &destination, 2);

        assert_eq!(report.cloned_files, 1);
        assert_eq!(report.copied_files, 0);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// `.git` object stores and package stores are full of hard links. Expanding
    /// them into independent copies inflates the image for nothing.
    #[cfg(not(target_os = "macos"))]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_preserves_hard_linked_files_as_a_single_inode() {
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
        // Bind through the admitted short runtime spelling, then rename on the
        // same filesystem into the long checkout path the copier must skip.
        let staging = root
            .parent()
            .expect("fixture runtime directory")
            .join(format!("cs{}", std::process::id()));
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
    fn allocation_only_changes_converge_in_one_pass() {
        let (root, source, destination) = copy_roots("allocation-only");
        fs::write(source.join("file"), b"unchanged\n").expect("write file");
        let initial = snapshot(&source).expect("source snapshot");
        let mut observation = 0;
        let mut observe = |_: &Path| -> super::Result<Snapshot> {
            let mut tree = initial.clone();
            tree.get_mut(Path::new("file"))
                .expect("file")
                .allocated_bytes += observation * 4096;
            observation += 1;
            Ok(tree)
        };
        let report = converge(&source, &destination, 1, &mut observe)
            .expect("allocation writeback is not a repository write");
        assert_eq!(report.passes, 1);
        assert_eq!(report.changed_entries, 0);
        assert_eq!(
            fs::read(destination.join("file")).expect("copy"),
            b"unchanged\n"
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

    #[test]
    fn resumed_parallel_copy_keeps_completed_leaves() {
        let (root, source, destination) = copy_roots("parallel-resume");
        let mut initial_inodes = Vec::new();

        // Grow one real tree instead of rebuilding 24 randomly sized fixtures.
        // Cross the eight-subtree boundary, retain the old maximum's 39 completed
        // and 40 missing leaves, then resume once more with no work left.
        for file_count in [1_usize, 2, 8, 9, 39, 79, 79] {
            let completed = initial_inodes.len();
            for index in completed..file_count {
                let subtree = source.join(format!("subtree-{}", index % 8));
                fs::create_dir_all(&subtree).expect("create resumed subtree");
                fs::write(subtree.join(format!("file-{index}")), index.to_le_bytes())
                    .expect("write resumed file");
            }
            let report = run(&source, &destination, 6);
            assert_eq!(report.passes, 1, "a still source needs one pass");
            assert_eq!(report.changed_entries, 0);
            assert_eq!(
                report.cloned_files + report.copied_files,
                file_count - completed,
                "a resumed copy must materialize only missing leaves"
            );

            for index in 0..file_count {
                let path = destination
                    .join(format!("subtree-{}", index % 8))
                    .join(format!("file-{index}"));
                assert_eq!(
                    fs::read(&path).expect("read copied file"),
                    index.to_le_bytes()
                );
                let inode = fs::symlink_metadata(&path).expect("stat copied file").ino();
                if index < completed {
                    assert_eq!(
                        inode, initial_inodes[index],
                        "a resumed copy must not reclone completed leaves"
                    );
                } else {
                    initial_inodes.push(inode);
                }
            }
        }
        fs::remove_dir_all(root).expect("remove fixture");
    }

    /// A copy is verified against its source by identity and content. Writeback giving a file a
    /// block between the walk's first and last observation is no change (02_workspaces.md,
    /// adoption step 3); a modification time that moves still is.
    #[test]
    fn verification_ignores_allocation_that_moves_during_the_walk() {
        use std::cell::Cell;
        let (root, source, destination) = copy_roots("allocation-moves");
        fs::write(source.join("payload"), b"original").unwrap();
        run(&source, &destination, 6);
        let expected = super::verify_tree_blocking(&source, &destination)
            .unwrap()
            .digest;
        // The walk observes source and copy before it reads them, then both again after.
        let observed = |late: fn(&mut super::Entry)| {
            let calls = Cell::new(0_u32);
            move |tree: &Path| -> crate::Result<Snapshot> {
                let mut entries = snapshot(tree)?;
                calls.set(calls.get() + 1);
                if calls.get() > 2 {
                    entries.values_mut().for_each(late);
                }
                Ok(entries)
            }
        };
        let grown = super::verify_with_snapshot(
            &source,
            &destination,
            observed(|entry| entry.allocated_bytes += 4096),
        )
        .expect("a block that writeback added is not a change");
        assert_eq!(grown.digest, expected);
        let error = super::verify_with_snapshot(
            &source,
            &destination,
            observed(|entry| entry.mtime.1 += 1),
        )
        .expect_err("a moved modification time is a change");
        assert_eq!(error.code, crate::ErrorCode::Conflict);
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn verification_walk_checks_content_namespace_and_link_targets() {
        use std::os::unix::fs::symlink;
        let (root, source, destination) = copy_roots("verified-walk");
        let relative = Path::new("payload");
        fs::write(source.join(relative), b"original").unwrap();
        symlink("payload", source.join("link")).unwrap();
        run(&source, &destination, 6);
        let original = super::verify_tree_blocking(&source, &destination).unwrap();

        let source_time = fs::metadata(source.join(relative))
            .unwrap()
            .modified()
            .unwrap();
        fs::write(destination.join(relative), b"tampered").unwrap();
        fs::File::open(destination.join(relative))
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(source_time))
            .unwrap();
        let error = super::verify_tree_blocking(&source, &destination).unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Integrity);
        assert!(error.message.contains("file content"), "{}", error.message);
        fs::write(destination.join(relative), b"original").unwrap();
        fs::File::open(destination.join(relative))
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(source_time))
            .unwrap();
        // Content evidence, not allocation: writeback can give the copied payload its block
        // between two walks with no repository write (specs/cowshed/02_workspaces.md, adoption
        // step 3: allocation is fidelity and space-accounting evidence, not quiescence).
        assert_eq!(
            super::verify_tree_blocking(&source, &destination)
                .unwrap()
                .digest,
            original.digest
        );

        fs::write(destination.join("extra"), b"not in source").unwrap();
        let error = super::verify_tree_blocking(&source, &destination).unwrap_err();
        assert_eq!(error.code, crate::ErrorCode::Integrity);
        assert!(error.message.contains("path set"), "{}", error.message);
        fs::remove_file(destination.join("extra")).unwrap();
        super::native::directory_metadata(&source, &destination).unwrap();
        assert_eq!(
            super::verify_tree_blocking(&source, &destination)
                .unwrap()
                .digest,
            original.digest
        );

        #[cfg(target_os = "macos")]
        {
            fs::remove_file(destination.join("link")).unwrap();
            symlink("changed", destination.join("link")).unwrap();
            super::native::directory_metadata(&source.join("link"), &destination.join("link"))
                .unwrap();
            super::native::directory_metadata(&source, &destination).unwrap();
            assert_eq!(
                fs::read_link(destination.join("link")).unwrap(),
                PathBuf::from("changed")
            );
            let source_link = fs::symlink_metadata(source.join("link")).unwrap();
            let destination_link = fs::symlink_metadata(destination.join("link")).unwrap();
            assert_eq!(source_link.len(), destination_link.len());
            assert_eq!(source_link.mode(), destination_link.mode());
            assert_eq!(
                source_link.modified().unwrap(),
                destination_link.modified().unwrap()
            );
            let error = super::verify_tree_blocking(&source, &destination).unwrap_err();
            assert_eq!(error.code, crate::ErrorCode::Integrity);
            assert!(
                error.message.contains("symlink target"),
                "{}",
                error.message
            );
        }
        assert_eq!(fs::read(source.join(relative)).unwrap(), b"original");
        assert_eq!(
            fs::read_link(source.join("link")).unwrap(),
            PathBuf::from("payload")
        );
        fs::remove_dir_all(root).unwrap();
    }

    /// One cross-volume copy walk covers dense bytes, sparse allocation, links, metadata, resumed
    /// verification and a same-length corruption control without paying another volume setup.
    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_copy_preserves_holes_and_verifies_each_lifecycle_step() {
        use cowshed_apfs::{ApfsBackend, MacOsApfsBackend, MountAccess, SystemCommandRunner};
        use cowshed_os::namespace::HostNamespace;
        use std::io::{Seek, SeekFrom, Write};
        use std::os::unix::fs::symlink;

        let root = crate::scratch_apfs::ScratchRoot::new("verified-sparse-copy").unwrap();
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("dense"), b"original").unwrap();
        let sparse_path = source.join("sparse");
        let mut sparse = fs::File::create(&sparse_path).unwrap();
        sparse.set_len(64 * 1024 * 1024).unwrap();
        sparse.seek(SeekFrom::Start(32 * 1024 * 1024)).unwrap();
        sparse.write_all(b"nonzero sparse payload").unwrap();
        sparse.sync_all().unwrap();
        drop(sparse);
        symlink("sparse", source.join("link")).unwrap();
        fs::hard_link(source.join("dense"), source.join("dense-alias")).unwrap();
        fs::create_dir_all(source.join("nested/out")).unwrap();
        fs::hard_link(source.join("dense"), source.join("nested/out/alias")).unwrap();
        write_xattr(&source.join("dense"), TEST_XATTR, b"verified attribute");

        let image = root
            .path()
            .join("destination")
            .with_extension(cowshed_apfs::image::IMAGE_EXTENSION);
        crate::blank_image::blank_image(root.namespace(), &image);
        let backend = MacOsApfsBackend::new(SystemCommandRunner::new(HostNamespace::Test(
            root.namespace(),
        )));
        let (attachment, _) = backend
            .attach_mounted(&image, &destination, MountAccess::ReadWrite, false)
            .unwrap();
        assert_ne!(
            fs::metadata(&source).unwrap().dev(),
            fs::metadata(&destination).unwrap().dev()
        );
        let report = run(&source, &destination, 6);
        assert!(
            report.copied_files > 0,
            "cross-volume copy exercised the native data path"
        );
        let before = super::verify_tree_blocking(&source, &destination).unwrap();
        let source_sparse = fs::metadata(&sparse_path).unwrap();
        let copied_sparse = fs::metadata(destination.join("sparse")).unwrap();
        assert_eq!(copied_sparse.len(), source_sparse.len());
        assert!(
            copied_sparse.blocks() * 512 <= source_sparse.blocks() * 512 + 65536,
            "sparse holes were materialized: source {} bytes, copied {} bytes",
            source_sparse.blocks() * 512,
            copied_sparse.blocks() * 512
        );
        assert_eq!(
            read_xattr(&destination.join("dense"), TEST_XATTR),
            Some(b"verified attribute".to_vec())
        );
        assert_eq!(
            fs::metadata(destination.join("dense")).unwrap().ino(),
            fs::metadata(destination.join("dense-alias")).unwrap().ino()
        );
        assert_eq!(
            fs::metadata(destination.join("dense-alias"))
                .unwrap()
                .nlink(),
            3
        );
        assert_eq!(
            fs::read_link(destination.join("link")).unwrap(),
            PathBuf::from("sparse")
        );
        let alias_dir_before = fs::metadata(destination.join("nested/out")).unwrap();
        let primary_before = fs::metadata(destination.join("dense")).unwrap();
        let resumed = run(&source, &destination, 6);
        assert_eq!(resumed.cloned_files + resumed.copied_files, 0);
        let alias_dir_after = fs::metadata(destination.join("nested/out")).unwrap();
        let primary_after = fs::metadata(destination.join("dense")).unwrap();
        assert_eq!(
            (alias_dir_after.mtime(), alias_dir_after.mtime_nsec()),
            (alias_dir_before.mtime(), alias_dir_before.mtime_nsec()),
            "a correct nested hard-link alias is not relinked on replay"
        );
        assert_eq!(
            (primary_after.ctime(), primary_after.ctime_nsec()),
            (primary_before.ctime(), primary_before.ctime_nsec()),
            "a correct hard-link inode has no relink churn"
        );
        assert_eq!(
            super::verify_tree_blocking(&source, &destination).unwrap(),
            before
        );

        // Break only topology, preserving bytes and source-comparable metadata. A replay
        // must repair the alias and restore every ancestor's mtime without recopying data.
        let alias = destination.join("nested/out/alias");
        fs::remove_file(&alias).unwrap();
        fs::write(&alias, b"original").unwrap();
        super::native::directory_metadata(&source.join("nested/out/alias"), &alias).unwrap();
        for relative in ["nested/out", "nested", ""] {
            super::native::directory_metadata(&source.join(relative), &destination.join(relative))
                .unwrap();
        }
        assert_eq!(
            super::verify_tree_blocking(&source, &destination)
                .unwrap_err()
                .code,
            crate::ErrorCode::Integrity
        );
        let repaired = run(&source, &destination, 6);
        assert_eq!(repaired.cloned_files + repaired.copied_files, 0);
        assert_eq!(
            fs::metadata(&alias).unwrap().ino(),
            fs::metadata(destination.join("dense")).unwrap().ino()
        );
        assert_eq!(
            super::verify_tree_blocking(&source, &destination).unwrap(),
            before
        );

        let primary_inode = fs::metadata(destination.join("dense")).unwrap().ino();
        fs::write(source.join("dense"), b"updated!").unwrap();
        let recopy = run(&source, &destination, 6);
        assert_eq!(
            recopy.cloned_files + recopy.copied_files,
            1,
            "a changed primary is copied once, not once per alias"
        );
        let new_primary = fs::metadata(destination.join("dense")).unwrap().ino();
        assert_ne!(new_primary, primary_inode);
        assert_eq!(
            fs::metadata(&alias).unwrap().ino(),
            new_primary,
            "a recopied primary must relink its previously correct nested alias"
        );
        super::verify_tree_blocking(&source, &destination).unwrap();

        fs::write(destination.join("dense"), b"tampered").unwrap();
        super::native::directory_metadata(&source.join("dense"), &destination.join("dense"))
            .unwrap();
        let refused = super::verify_tree_blocking(&source, &destination).unwrap_err();
        assert_eq!(refused.code, crate::ErrorCode::Integrity);
        assert!(
            refused.message.contains("file content"),
            "{}",
            refused.message
        );
        assert_eq!(fs::read(source.join("dense")).unwrap(), b"updated!");
        backend
            .unmount_verified(&attachment, cowshed_apfs::DetachIntent::WhenIdle)
            .unwrap();
        backend
            .detach(&attachment, cowshed_apfs::DetachIntent::WhenIdle)
            .unwrap();
    }
}

#[cfg(test)]
mod metadata_tests;

#[cfg(test)]
mod sparse_tests;
