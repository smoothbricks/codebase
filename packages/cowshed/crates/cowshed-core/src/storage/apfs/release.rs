//! Releasing an image file's storage in bounded steps (16_build_volumes.md, "Garbage
//! collection").
//!
//! One `unlink(2)` of a written image frees its whole extent tree as one operation, and APFS
//! frees extents one at a time: measured 2026-10-22 on this host's container (a clone of a 2 GiB
//! file with a 4 KiB write every 32 KiB, 131,072 extents), one unlink ran 7.6 s to 29 s, and a
//! trashed 300 GiB build image holds millions. So a release cuts the file from its end, at most
//! [`STEP_EXTENTS`] extents per `ftruncate(2)`, and unlinks it only once it is empty. Other
//! writers can progress during and between cuts; this does not bound their latency.
//!
//! The file's length is the release's journal. A step cuts at the start of a run, so an
//! interrupted release leaves a shorter file with whole runs before its end; opening it again
//! walks what is left and plans the rest. Nothing else records progress.
//!
//! The extent limit bounds reclamation work, not syscall latency. A fresh clone may copy its
//! whole shared extent map inside the first cut: measured 34.8 s and 50.1 s for 485,830 runs.
//! A yield between cuts cannot split that call. A shared-map flag alone cannot justify an
//! unbounded unlink: the other map owner can disappear after the observation. See
//! 01a_apfs.md, "Freeing an image", for the measurements and remaining options.
//!
//! The plan is a pure function of the run starts ([`ReleasePlan`]); [`Release`] is the shell that
//! walks the runs, takes the file's lock and makes the cuts.
//!
//! An image a lifecycle operation owns is released through a journal ([`release_owned`]): one
//! rename into the [`JOURNAL_DIRECTORY`] beside it commits the release, so no cut ever lands
//! on an image anything can still find, and the same operation then frees it before it answers.
//! A crash leaves the entry, which recovery ([`drain`]) finishes.

use std::ffi::{CStr, CString};
use std::fs::{self, File};
use std::io;
use std::num::NonZeroU64;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use cowshed_os::fork_lock::Fenced;
use cowshed_os::fsio::{
    open_directory_nofollow, open_or_create_child_directory, rename_noreplace, sync_held_directory,
};

use cowshed_apfs::SECTOR_BYTES;

use super::extents::for_each_run;

/// The most extents one step of a release frees.
///
/// Measured 2026-10-22 with `ftruncate` steps over the 131,072-extent clone above, on the same
/// container as the store (`/private/tmp`, disk3), normal disk priority, while the host's other
/// agents were retiring workspaces, beside a probe that creates, writes 4 KiB to, fsyncs and
/// unlinks a file in a loop (`/private/tmp/trash-release-measure/measure.py`):
///
/// | extents/step | runs | step p50     | step max   | total    |
/// | -----------: | ---: | -----------: | ---------: | -------: |
/// |          256 |    1 | 18 ms        | 3.6 s      | 62 s     |
/// |        1,024 |    3 | 99–302 ms    | 1.4–3.7 s  | 35–47 s  |
/// |        2,048 |    2 | 59–497 ms    | 0.6–6.0 s  | 7.5–44 s |
/// |        4,096 |    3 | 369–1,433 ms | 2.5–7.4 s  | 19–54 s  |
/// |       16,384 |    1 | 2.1 s        | 6.0 s      | 23 s     |
/// |  one unlink  |    2 | —            | 7.6–29 s   | 7.6–29 s |
///
/// The totals moved sevenfold between runs of one size with the host's own load, so they rank
/// nothing; the step's typical length is what a step size decides, and from 1,024 on it grows
/// with the step, at about 100 to 300 µs per extent. Probe writers waited 0.2 to 0.8 ms at the
/// median with nothing freed, up to 16 ms under any free. 1,024 is the largest step whose typical
/// free stays near a tenth of a second; a smaller one only adds steps.
///
/// Measured again 2026-10-23 on a materialized 485,830-run clone: 475 cuts at 44 ms p50,
/// 113 ms p90 and 217 ms maximum; independent store/Data/attached-image probes waited at most
/// 62 ms. Two releases at once reached 1.444 s per cut and 0.972 s per probe. These are
/// observations under that load, not wall-time guarantees. A fresh shared-map clone took
/// 50.074 s for its first 454-run cut; reducing reclamation work cannot bound map splitting.
pub const STEP_EXTENTS: StepExtents = StepExtents(NonZeroU64::MIN.saturating_add(1023));

/// How many extents one step frees at most: never zero, so every plan ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StepExtents(NonZeroU64);

impl StepExtents {
    pub const fn new(extents: NonZeroU64) -> Self {
        Self(extents)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// The lengths a release cuts a file to, longest first, ending at zero; then the file is
/// unlinked.
///
/// Built from the starts of the file's physical runs in ascending order: every
/// [`StepExtents`]-th start is a cut, so cutting to it frees the runs from that start on, never
/// more than one step's worth. Only those starts are kept, one per step, so a file of millions
/// of extents plans in a few thousand words.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleasePlan {
    /// Ascending, so the next cut is the last.
    cuts: Vec<u64>,
}

impl ReleasePlan {
    /// The plan for a file of `length` bytes whose data runs start at `starts`, ascending and
    /// each below `length`.
    pub fn new(starts: impl IntoIterator<Item = u64>, length: u64, step: StepExtents) -> Self {
        let mut planner = Planner::new(step);
        for start in starts {
            planner.run(start);
        }
        planner.plan(length)
    }

    /// The next length to cut the file to, if any cut is left.
    pub fn next(&self) -> Option<u64> {
        self.cuts.last().copied()
    }

    /// The cuts left, in the order they are made.
    pub fn cuts(&self) -> impl Iterator<Item = u64> + '_ {
        self.cuts.iter().rev().copied()
    }

    fn advance(&mut self) {
        self.cuts.pop();
    }
}

/// [`ReleasePlan::new`] fed one run start at a time, as the walk finds them.
struct Planner {
    step: u64,
    seen: u64,
    /// The start of every `step`-th run after the first, ascending.
    cuts: Vec<u64>,
}

impl Planner {
    fn new(step: StepExtents) -> Self {
        Self {
            step: step.get(),
            seen: 0,
            cuts: Vec::new(),
        }
    }

    fn run(&mut self, start: u64) {
        // Run 0's group is freed by the final cut to zero, so it needs no cut of its own.
        if self.seen > 0 && self.seen.is_multiple_of(self.step) {
            self.cuts.push(start);
        }
        self.seen += 1;
    }

    fn plan(self, length: u64) -> ReleasePlan {
        let mut cuts = Vec::with_capacity(self.cuts.len() + 1);
        // An empty file has nothing to free; anything longer ends cut to zero, holes included.
        if length > 0 {
            cuts.push(0);
        }
        cuts.extend(self.cuts);
        ReleasePlan { cuts }
    }
}

/// What releasing one file did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Released {
    /// The file is gone: it held `bytes` on disk when its release opened it, and `steps` cuts
    /// freed them before the unlink.
    Freed { bytes: u64, steps: u64 },
    /// No file was there.
    Absent,
    /// Another process is releasing it; it is left to that release.
    Busy,
}

/// A release in progress: the file, locked exclusively, the directory it is unlinked from, held
/// by descriptor, and the cuts left.
#[derive(Debug)]
pub struct Release {
    directory: File,
    name: CString,
    /// Fenced: a peer's try-lock observes its release (16_build_volumes.md, "Garbage
    /// collection"), so no spawn in flight may carry the lock past this release's drop.
    file: Fenced<File>,
    plan: ReleasePlan,
    bytes: u64,
    steps: u64,
}

/// What opening a release found.
#[derive(Debug)]
pub enum Opened {
    Release(Release),
    Absent,
    Busy,
}

impl Release {
    /// Open the file at `path` for release: take its lock without waiting, so two processes never
    /// cut one file, and plan from the runs its data occupies now. Only a regular file is opened,
    /// never through a link, in its directory held by descriptor; anything else refuses.
    pub fn open(path: &Path, step: StepExtents) -> io::Result<Opened> {
        let (directory, name) = anchor(path)?;
        let file = match open_entry(&directory, &name)? {
            Entry::Absent => return Ok(Opened::Absent),
            Entry::NotImage => return Err(not_image(path.display())),
            Entry::Image(file) => file,
        };
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => return Ok(Opened::Busy),
            Err(fs::TryLockError::Error(error)) => return Err(error),
        }
        Self::plan(directory, name, file, step).map(Opened::Release)
    }

    /// Open the file at `path` for release on behalf of the operation that owns it, which
    /// answers only once the file is gone: a release another process holds is waited out, and
    /// what it left is planned. `None` when no file is there.
    pub fn open_waiting(path: &Path, step: StepExtents) -> io::Result<Option<Self>> {
        let (directory, name) = anchor(path)?;
        let file = match open_entry(&directory, &name)? {
            Entry::Absent => return Ok(None),
            Entry::NotImage => return Err(not_image(path.display())),
            Entry::Image(file) => file,
        };
        file.lock()?;
        Self::plan(directory, name, file, step).map(Some)
    }

    /// Plan the cuts of `file`, locked, from the runs its data occupies now.
    fn plan(
        directory: File,
        name: CString,
        file: Fenced<File>,
        step: StepExtents,
    ) -> io::Result<Self> {
        let metadata = file.metadata()?;
        let length = metadata.len();
        let mut planner = Planner::new(step);
        for_each_run(&file, length, |start| {
            planner.run(start);
            std::ops::ControlFlow::Continue(())
        })?;
        Ok(Self {
            directory,
            name,
            plan: planner.plan(length),
            bytes: metadata.blocks().saturating_mul(SECTOR_BYTES),
            file,
            steps: 0,
        })
    }

    /// Make the next cut, returning the length it cut the file to, or `None` once every cut is
    /// made.
    pub fn step(&mut self) -> io::Result<Option<u64>> {
        let Some(cut) = self.plan.next() else {
            return Ok(None);
        };
        self.file.set_len(cut)?;
        self.plan.advance();
        self.steps += 1;
        Ok(Some(cut))
    }

    /// Make every cut left and unlink the empty file from its held directory.
    pub fn finish(mut self) -> io::Result<Released> {
        while self.step()?.is_some() {}
        // SAFETY: the held directory descriptor and the NUL-terminated name outlive the call.
        if unsafe { libc::unlinkat(self.directory.as_raw_fd(), self.name.as_ptr(), 0) } != 0 {
            let error = io::Error::last_os_error();
            // Unlinked by hand under the lock: what it held is freed all the same.
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
        }
        Ok(Released::Freed {
            bytes: self.bytes,
            steps: self.steps,
        })
    }
}

/// Release the file at `path` in bounded steps, then unlink it.
pub fn release_file(path: &Path, step: StepExtents) -> io::Result<Released> {
    match Release::open(path, step)? {
        Opened::Release(release) => release.finish(),
        Opened::Absent => Ok(Released::Absent),
        Opened::Busy => Ok(Released::Busy),
    }
}

/// What a directory entry a release would open turned out to be.
enum Entry {
    Absent,
    /// A regular file, opened for writing, fenced from its open on.
    Image(Fenced<File>),
    /// A link, a directory or anything else that is not a regular file: never opened through,
    /// never freed.
    NotImage,
}

/// Open the entry `name` of `directory` for release without following a link at it.
fn open_entry(directory: &File, name: &CStr) -> io::Result<Entry> {
    // SAFETY: the held directory descriptor and the NUL-terminated name outlive the call.
    // NOFOLLOW refuses a link at the name; NONBLOCK keeps a planted FIFO from holding the open.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ENOENT) => Ok(Entry::Absent),
            Some(libc::ELOOP | libc::EISDIR) => Ok(Entry::NotImage),
            _ => Err(error),
        };
    }
    // SAFETY: a successful openat returned a new owned descriptor, close-on-exec from that call.
    let file = Fenced::new(unsafe { File::from_raw_fd(fd) });
    Ok(if file.metadata()?.is_file() {
        Entry::Image(file)
    } else {
        Entry::NotImage
    })
}

fn not_image(name: impl std::fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{name} is not a regular image file; a release frees nothing else, and it is left"),
    )
}

/// The directory holding `path`, held by descriptor without following a link at its last
/// component, and the entry's name in it.
fn anchor(path: &Path) -> io::Result<(File, CString)> {
    let directory = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} has no directory", path.display()),
        )
    })?;
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} names no entry", path.display()),
        )
    })?;
    Ok((
        open_directory_nofollow(directory)?,
        c_name(name.as_bytes())?,
    ))
}

fn c_name(name: &[u8]) -> io::Result<CString> {
    CString::new(name).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "name contains NUL"))
}

/// The directory, beside the images it journals, that holds each release an operation has
/// committed and not yet finished: `build/.releasing/<id>.dmg`, `sessions/.releasing/<ws>-<inc>.dmg`
/// (16_build_volumes.md, "Garbage collection"; 02_workspaces.md, `rm` step 5). A crash-recovery
/// journal, never a queue: an entry outlives its operation only when a crash cut it short.
pub const JOURNAL_DIRECTORY: &str = ".releasing";

/// Release `image` inside the operation that owns it, crash-safely, and say whether an image was
/// released.
///
/// The commit is one rename of `image` to `entry`, in its directory's [`JOURNAL_DIRECTORY`],
/// with every directory it touched synced: before it the image is its owner's and untouched,
/// after it nothing finds the image by its name, so a cut never lands on an image that can still
/// be mounted, forked or read. The image is opened and locked before the rename and stays locked
/// to its unlink, so a recovery that finds the entry leaves it until this operation is done: no
/// cut lands before the rename is durable. This operation frees it in bounded steps, unlinks it
/// and syncs the journal, all before it returns, so its caller removes the owner record only once
/// the bytes are gone. Every directory is held by descriptor and no entry is opened through a
/// link: a link or any other non-regular file at either name refuses, untouched.
///
/// An entry a crash left under `entry` is the same image's committed release (an id and an
/// incarnation are never reused), so it is finished first, waiting out a recovery that has it:
/// renaming over it would free it in one go. A retry after such a crash therefore finds no
/// `image` and still answers `true`.
pub fn release_owned(image: &Path, entry: &Path) -> io::Result<bool> {
    let anchors = Anchors::open(image, entry)?;
    let left = anchors.finish_left()?;
    let Some(release) = anchors.commit(STEP_EXTENTS)? else {
        return Ok(left);
    };
    release.finish()?;
    sync_held_directory(&anchors.journal)?;
    Ok(true)
}

/// The directories one owned release works in, each held by descriptor: the image's, the
/// journal, and the journal's parent, which gains the journal's entry when it is first made.
struct Anchors {
    image_directory: File,
    image_name: CString,
    journal_parent: File,
    journal: File,
    entry_name: CString,
}

impl Anchors {
    fn open(image: &Path, entry: &Path) -> io::Result<Self> {
        let (image_directory, image_name) = anchor(image)?;
        let journal = entry.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} has no journal", entry.display()),
            )
        })?;
        let (journal_parent, journal_name) = anchor(journal)?;
        let (journal, _) = open_or_create_child_directory(&journal_parent, &journal_name)?;
        let entry_name = c_name(
            entry
                .file_name()
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("{} names no entry", entry.display()),
                    )
                })?
                .as_bytes(),
        )?;
        Ok(Self {
            image_directory,
            image_name,
            journal_parent,
            journal,
            entry_name,
        })
    }

    /// Finish the entry a crash left under this release's name: `false` when none is there.
    fn finish_left(&self) -> io::Result<bool> {
        let file = match open_entry(&self.journal, &self.entry_name)? {
            Entry::Absent => return Ok(false),
            Entry::NotImage => return Err(not_image(self.entry_name.to_string_lossy())),
            Entry::Image(file) => file,
        };
        file.lock()?;
        // Its owner may have died between the rename and its syncs: made durable before a cut.
        sync_held_directory(&self.journal)?;
        sync_held_directory(&self.image_directory)?;
        sync_held_directory(&self.journal_parent)?;
        Release::plan(
            self.journal.try_clone()?,
            self.entry_name.clone(),
            file,
            STEP_EXTENTS,
        )?
        .finish()?;
        sync_held_directory(&self.journal)?;
        Ok(true)
    }

    /// Commit the image: open and lock it, rename it into the journal and make the rename
    /// durable, then plan its release, still locked. `None` when no image is there. An image
    /// another process has locked refuses rather than waits: nothing but a release locks one.
    fn commit(&self, step: StepExtents) -> io::Result<Option<Release>> {
        let file = match open_entry(&self.image_directory, &self.image_name)? {
            Entry::Absent => return Ok(None),
            Entry::NotImage => return Err(not_image(self.image_name.to_string_lossy())),
            Entry::Image(file) => file,
        };
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::ResourceBusy,
                    format!(
                        "{} is locked by another process, so it is not released",
                        self.image_name.to_string_lossy()
                    ),
                ));
            }
            Err(fs::TryLockError::Error(error)) => return Err(error),
        }
        rename_noreplace(
            self.image_directory.as_raw_fd(),
            &self.image_name,
            self.journal.as_raw_fd(),
            &self.entry_name,
        )?;
        // The entry, the image's departure, and the journal's own name, which an earlier attempt
        // may have made without syncing.
        sync_held_directory(&self.journal)?;
        sync_held_directory(&self.image_directory)?;
        sync_held_directory(&self.journal_parent)?;
        Release::plan(
            self.journal.try_clone()?,
            self.entry_name.clone(),
            file,
            step,
        )
        .map(Some)
    }
}

/// What draining a directory of committed releases freed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Drained {
    pub images: usize,
    /// The bytes those images held on disk, read when each release opened it.
    pub bytes: u64,
}

impl Drained {
    pub fn add(&mut self, other: Self) {
        self.images += other.images;
        self.bytes += other.bytes;
    }
}

/// Finish every committed release in `directory` (a [`JOURNAL_DIRECTORY`], or a historical
/// trash) whose name `selected` accepts, one at a time on the calling thread, each in bounded
/// steps at normal disk priority. The directory is held by descriptor and refused when it is a
/// link; each entry is opened in it without following a link. Only regular files whose names
/// `is_entry` accepts are released; anything else is said and left, untouched. An entry another
/// process is releasing is left to it, and one a crash left half-released is finished from its
/// length. A directory that does not exist is empty; one that cannot be read refuses.
///
/// Before its first cut the journal and its parent are synced: an entry's owner may have died
/// between its rename and that rename's syncs, and a cut that outlived a rename a crash then
/// undid would leave the image's own name on shortened bytes. A rename is one APFS transaction,
/// which a full sync of the directory it entered makes durable; every entry listed was renamed
/// before the listing, so one sync after it covers them all.
pub fn drain(
    directory: &Path,
    is_entry: impl Fn(&str) -> bool,
    selected: impl Fn(&str) -> bool,
) -> io::Result<Drained> {
    let held = match open_directory_nofollow(directory) {
        Ok(held) => held,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Drained::default()),
        Err(error) => return Err(error),
    };
    let entries = fs::read_dir(directory)?.collect::<io::Result<Vec<_>>>()?;
    if !entries.is_empty() {
        sync_held_directory(&held)?;
        if let Some(parent) = directory.parent() {
            sync_held_directory(&open_directory_nofollow(parent)?)?;
        }
    }
    let mut drained = Drained::default();
    for entry in entries {
        let path = entry.path();
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            cowshed_os::eprintln!(
                "cowshed: {} is not a released image; it is left",
                path.display()
            );
            continue;
        };
        if !selected(name) {
            continue;
        }
        let file = match is_entry(name).then(|| open_entry(&held, &c_name(name.as_bytes())?)) {
            Some(Ok(Entry::Absent)) => continue,
            Some(Ok(Entry::Image(file))) => file,
            Some(Ok(Entry::NotImage)) | None => {
                cowshed_os::eprintln!(
                    "cowshed: {} is not a released image; it is left",
                    path.display()
                );
                continue;
            }
            Some(Err(error)) => return Err(error),
        };
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                cowshed_os::eprintln!(
                    "cowshed: released image {} is being freed by another process; it is left to that release",
                    path.display()
                );
                continue;
            }
            Err(fs::TryLockError::Error(error)) => return Err(error),
        }
        let release = Release::plan(
            held.try_clone()?,
            c_name(name.as_bytes())?,
            file,
            STEP_EXTENTS,
        )?;
        if let Released::Freed { bytes, .. } = release.finish()? {
            drained.add(Drained { images: 1, bytes });
        }
    }
    if drained.images > 0 {
        sync_held_directory(&held)?;
    }
    Ok(drained)
}

/// [`drain`] the directory `<project>/<images>/<namespace>` of every project in the store at
/// `store` (`<store>/<owner>/<repo>`), summed. A project whose directory cannot be drained is
/// said and skipped, so one unreadable project never holds back the rest.
pub fn drain_store(
    store: &Path,
    images: &str,
    namespace: &str,
    is_entry: impl Fn(&str) -> bool,
) -> Drained {
    let mut drained = Drained::default();
    let owners = match fs::read_dir(store) {
        Ok(owners) => owners,
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: cannot list the store {} for released images: {error}",
                store.display()
            );
            return drained;
        }
    };
    let skipped = |what: &str, error: io::Error| {
        cowshed_os::eprintln!(
            "cowshed: released images under {what} stay: it cannot be listed: {error}"
        );
    };
    for owner in owners {
        let owner = match owner {
            Ok(owner) => owner.path(),
            Err(error) => {
                skipped(&store.display().to_string(), error);
                continue;
            }
        };
        let repos = match fs::read_dir(&owner) {
            Ok(repos) => repos,
            // A file beside the owners (`host.json`) holds no projects.
            Err(error) if error.raw_os_error() == Some(libc::ENOTDIR) => continue,
            Err(error) => {
                skipped(&owner.display().to_string(), error);
                continue;
            }
        };
        for repo in repos {
            let repo = match repo {
                Ok(repo) => repo.path(),
                Err(error) => {
                    skipped(&owner.display().to_string(), error);
                    continue;
                }
            };
            let directory = repo.join(images).join(namespace);
            match drain(&directory, &is_entry, |_| true) {
                Ok(project) => drained.add(project),
                Err(error) => cowshed_os::eprintln!(
                    "cowshed: released images in {} stay: {error}",
                    directory.display()
                ),
            }
        }
    }
    drained
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Runs as `(hole before, length)` pairs laid out from offset 0, and a trailing hole.
    fn layout(runs: &[(u64, u64)], trailing: u64) -> (Vec<(u64, u64)>, u64) {
        let mut offset = 0;
        let mut placed = Vec::with_capacity(runs.len());
        for &(hole, length) in runs {
            offset += hole;
            placed.push((offset, offset + length));
            offset += length;
        }
        (placed, offset + trailing)
    }

    /// The runs a cut to `cut` frees from a file of `length`: those starting in `[cut, length)`.
    fn freed(runs: &[(u64, u64)], cut: u64, length: u64) -> usize {
        runs.iter()
            .filter(|(start, _)| (cut..length).contains(start))
            .count()
    }

    fn step(extents: u64) -> StepExtents {
        StepExtents::new(NonZeroU64::new(extents).expect("nonzero step"))
    }

    fn runs_strategy() -> impl Strategy<Value = (Vec<(u64, u64)>, u64, u64)> {
        (
            proptest::collection::vec((0_u64..4, 1_u64..4), 0..600),
            0_u64..4,
            1_u64..70,
        )
    }

    proptest! {
        /// Every cut frees at most one step of runs, the cuts only shorten the file and end at
        /// zero, every run is freed by exactly one cut, and the cuts are as few as the bound
        /// allows.
        #[test]
        fn every_cut_frees_at_most_one_step_and_together_they_free_every_run(
            (runs, trailing, extents) in runs_strategy()
        ) {
            let (runs, length) = layout(&runs, trailing);
            let plan = ReleasePlan::new(runs.iter().map(|(start, _)| *start), length, step(extents));
            let cuts: Vec<u64> = plan.cuts().collect();

            let mut current = length;
            let mut total = 0;
            for &cut in &cuts {
                prop_assert!(cut < current, "cut {cut} does not shorten {current}");
                let count = freed(&runs, cut, current);
                prop_assert!(u64::try_from(count).unwrap() <= extents, "cut to {cut} frees {count} runs");
                total += count;
                current = cut;
            }
            prop_assert_eq!(total, runs.len());
            prop_assert_eq!(cuts.last().copied(), (length > 0).then_some(0));
            let n = u64::try_from(runs.len()).unwrap();
            let fewest = if length == 0 { 0 } else { n.div_ceil(extents).max(1) };
            prop_assert_eq!(u64::try_from(cuts.len()).unwrap(), fewest);
        }

        /// A release interrupted after any cut, opened again on what that cut left, plans exactly
        /// the cuts the interrupted one had left: the length is the whole journal.
        #[test]
        fn a_release_resumed_after_any_cut_plans_the_cuts_left(
            (runs, trailing, extents) in runs_strategy(),
            made in any::<prop::sample::Index>(),
        ) {
            let (runs, length) = layout(&runs, trailing);
            let plan = ReleasePlan::new(runs.iter().map(|(start, _)| *start), length, step(extents));
            let cuts: Vec<u64> = plan.cuts().collect();
            if cuts.is_empty() {
                return Ok(());
            }
            let made = made.index(cuts.len());
            let cut = cuts[made];

            let left = runs
                .iter()
                .filter(|(start, _)| *start < cut)
                .map(|(start, _)| *start);
            let resumed: Vec<u64> = ReleasePlan::new(left, cut, step(extents)).cuts().collect();

            prop_assert_eq!(resumed, cuts[made + 1..].to_vec());
        }
    }

    /// A real file on the host's filesystem: fragmented by rewriting every other page while a
    /// clone shares it, released a few steps at a time, interrupted, and finished by a second
    /// release that walks only what the first left.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_release_frees_a_fragmented_file_stepwise_and_resumes_from_its_length() {
        use crate::storage::apfs::extents::count_extents;
        use crate::storage::apfs::extents::tests::{fragmented_sparse_file, scratch};

        let root = scratch("release");
        let (image, _) = fragmented_sparse_file(&root.0);
        let extents = count_extents(&image).expect("count").get();
        assert!(
            extents >= 256,
            "the fixture did not fragment: {extents} extents"
        );
        let allocated = |path: &Path| fs::metadata(path).expect("stat").blocks() * SECTOR_BYTES;
        let held = allocated(&image);
        let bound = step(32);

        let Opened::Release(mut first) = Release::open(&image, bound).expect("open") else {
            panic!("a fresh file opens for release");
        };
        assert!(
            matches!(
                Release::open(&image, bound).expect("open twice"),
                Opened::Busy
            ),
            "a second release of the same file waits for none and cuts nothing"
        );
        let mut before = (extents, held);
        for _ in 0..3 {
            let cut = first.step().expect("cut").expect("a cut is left");
            let after = (
                count_extents(&image).expect("count").get(),
                allocated(&image),
            );
            assert_eq!(fs::metadata(&image).expect("stat").len(), cut);
            assert!(
                after.0 < before.0,
                "a cut frees extents: {before:?} -> {after:?}"
            );
            assert!(
                before.0 - after.0 <= bound.get(),
                "a cut frees at most one step: {before:?} -> {after:?}"
            );
            assert!(
                after.1 < before.1,
                "a cut frees bytes: {before:?} -> {after:?}"
            );
            before = after;
        }
        drop(first);

        let Opened::Release(second) = Release::open(&image, bound).expect("reopen") else {
            panic!("an interrupted release opens again");
        };
        let left: Vec<u64> = second.plan.cuts().collect();
        assert_eq!(
            u64::try_from(left.len()).expect("count"),
            before.0.div_ceil(bound.get()),
            "the resumed release plans from what is left: {left:?}"
        );
        let Released::Freed { bytes, steps } = second.finish().expect("finish") else {
            panic!("the release frees the file");
        };
        assert_eq!(bytes, before.1, "the resumed release counts what was left");
        assert_eq!(steps, u64::try_from(left.len()).expect("count"));
        assert!(!image.exists(), "the empty file is unlinked");
        assert_eq!(
            release_file(&image, bound).expect("again"),
            Released::Absent
        );
    }

    /// The crash the journal exists for: an owner commits its image, makes the first cut and
    /// dies. Nothing finds the image by its name any more; recovery finishes the release from
    /// what the cut left, and so does the owner's own retry, which still answers that it
    /// released the image. While the owner holds the committed entry, from before its rename,
    /// recovery leaves it uncut.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_release_cut_short_after_its_first_cut_is_finished_by_recovery_or_its_retry() {
        use crate::storage::apfs::extents::count_extents;
        use crate::storage::apfs::extents::tests::{fragmented_sparse_file, scratch};

        let any = |name: &str| name.ends_with(".dmg");
        let crash_after_first_cut = |test: &str| {
            let root = scratch(test);
            let (image, _) = fragmented_sparse_file(&root.0);
            assert!(count_extents(&image).expect("count").get() >= 256);
            let entry = root.0.join(JOURNAL_DIRECTORY).join("image.dmg");
            let anchors = Anchors::open(&image, &entry).expect("anchors");
            let mut owner = anchors
                .commit(step(32))
                .expect("commit")
                .expect("the image");
            assert!(!image.exists(), "the commit takes the image off its name");
            let length = fs::metadata(&entry).expect("stat").len();
            let journal = entry.parent().expect("journal");
            assert_eq!(
                drain(journal, any, any).expect("a recovery beside the owner"),
                Drained::default()
            );
            assert_eq!(
                fs::metadata(&entry).expect("stat").len(),
                length,
                "recovery never cuts an entry its owner holds"
            );
            let cut = owner.step().expect("cut").expect("a cut is left");
            assert!(cut < length, "the first cut shortens the entry");
            drop(owner);
            assert!(entry.exists(), "the crash leaves the entry");
            (root, image, entry)
        };

        let (_recovered, image, entry) = crash_after_first_cut("journal-recovery");
        let journal = entry.parent().expect("journal");
        let drained = drain(journal, any, any).expect("recover");
        assert_eq!(drained.images, 1);
        assert!(drained.bytes > 0, "recovery frees what the cut left");
        assert!(!entry.exists(), "recovery finishes the release");
        assert!(
            !release_owned(&image, &entry).expect("a later retry"),
            "nothing is left for a retry after recovery"
        );

        let (_retried, image, entry) = crash_after_first_cut("journal-retry");
        assert!(
            release_owned(&image, &entry).expect("retry"),
            "the owner's retry finishes its own committed release"
        );
        assert!(!entry.exists() && !image.exists());
    }

    /// A link at the entry's name, at the image's name, or in place of the journal never leads a
    /// release to another file: each refuses, or is left by recovery, and the file it names keeps
    /// every byte. Nor does anything but a regular file get committed.
    #[test]
    fn a_release_never_frees_through_a_link_or_anything_but_a_regular_file() {
        let root =
            std::env::temp_dir().join(format!("cowshed-release-links-{}", uuid::Uuid::new_v4()));
        let images = root.join("images");
        let journal = images.join(JOURNAL_DIRECTORY);
        fs::create_dir_all(&journal).expect("journal");
        let foreign = root.join("foreign.dmg");
        fs::write(&foreign, vec![7_u8; 64 * 1024]).expect("foreign file");
        let image = images.join("image.dmg");
        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        let entry = journal.join("image.dmg");
        let intact = |path: &Path, byte: u8| {
            assert_eq!(fs::read(path).expect("read"), vec![byte; 64 * 1024]);
        };

        // A link where a crash's entry would be.
        std::os::unix::fs::symlink(&foreign, &entry).expect("entry link");
        assert!(release_owned(&image, &entry).is_err());
        intact(&foreign, 7);
        intact(&image, 9);
        let any = |name: &str| name.ends_with(".dmg");
        assert_eq!(
            drain(&journal, any, any).expect("recovery"),
            Drained::default()
        );
        intact(&foreign, 7);
        assert!(
            fs::symlink_metadata(&entry).is_ok(),
            "recovery leaves the link"
        );
        fs::remove_file(&entry).expect("drop the entry link");

        // A link at the image's name.
        fs::remove_file(&image).expect("drop the image");
        std::os::unix::fs::symlink(&foreign, &image).expect("image link");
        assert!(release_owned(&image, &entry).is_err());
        intact(&foreign, 7);
        assert!(!entry.exists(), "nothing was committed");
        fs::remove_file(&image).expect("drop the image link");

        // A directory at the image's name.
        fs::create_dir(&image).expect("image directory");
        assert!(release_owned(&image, &entry).is_err());
        assert!(image.is_dir() && !entry.exists());
        fs::remove_dir(&image).expect("drop the image directory");

        // A link in place of the journal.
        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        let elsewhere = root.join("elsewhere");
        fs::create_dir(&elsewhere).expect("elsewhere");
        fs::remove_dir(&journal).expect("drop the journal");
        std::os::unix::fs::symlink(&elsewhere, &journal).expect("journal link");
        assert!(release_owned(&image, &entry).is_err());
        intact(&image, 9);
        assert_eq!(fs::read_dir(&elsewhere).expect("elsewhere").count(), 0);
        fs::write(elsewhere.join("other.dmg"), b"kept").expect("other");
        assert!(
            drain(&journal, any, any).is_err(),
            "a linked journal refuses"
        );
        assert_eq!(
            fs::read(elsewhere.join("other.dmg")).expect("other"),
            b"kept"
        );
        fs::remove_dir_all(&root).expect("cleanup");
    }

    /// An owner that died between its commit's rename and that rename's syncs leaves an entry a
    /// crash could still put back under the image's name. Recovery and the owner's own retry each
    /// sync the journal and the directories around it, under the entry's lock, before their first
    /// cut, and the journal again once the entry is unlinked.
    #[test]
    fn a_rename_its_owner_never_synced_is_made_durable_before_the_first_cut() {
        use cowshed_os::fsio::take_syncs;

        let root =
            std::env::temp_dir().join(format!("cowshed-release-unsynced-{}", uuid::Uuid::new_v4()));
        let images = root.join("images");
        let journal = images.join(JOURNAL_DIRECTORY);
        fs::create_dir_all(&journal).expect("journal");
        let image = images.join("image.dmg");
        let entry = journal.join("image.dmg");
        let any = |name: &str| name.ends_with(".dmg");

        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        fs::rename(&image, &entry).expect("the owner's rename, never synced");
        take_syncs();
        let drained = drain(&journal, any, any).expect("recover");
        assert_eq!(drained.images, 1);
        assert!(!entry.exists());
        let syncs = take_syncs();
        assert_eq!(
            syncs.full, 3,
            "the journal and its parent before the cut, the journal after the unlink: {syncs:?}"
        );

        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        fs::rename(&image, &entry).expect("the owner's rename, never synced");
        take_syncs();
        assert!(release_owned(&image, &entry).expect("the owner's retry"));
        assert!(!entry.exists());
        let syncs = take_syncs();
        assert_eq!(
            syncs.full, 4,
            "the journal, the image's directory and the journal's parent before the cut, the journal after: {syncs:?}"
        );
        fs::remove_dir_all(&root).expect("cleanup");
    }
}
