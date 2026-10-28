//! Releasing an image file's storage in bounded steps (16_build_volumes.md, "Garbage
//! collection").
//!
//! One `unlink(2)` of a written image frees its whole extent tree as one operation, and APFS
//! frees extents one at a time: measured 2026-10-22 on this host's container (a clone of a 2 GiB
//! file with a 4 KiB write every 32 KiB, 131,072 extents), one unlink ran 7.6 s to 29 s, and a
//! trashed 300 GiB build image holds millions. So a release cuts the file from its end in bounded
//! steps, [`STEP_EXTENTS`] runs a cut, or in the gateway's cleanup a size the last cut's rate
//! sets ([`next_cut`]), and unlinks it only once it is empty. Other writers can progress during
//! and between cuts; this does not bound their latency.
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
//! An image a lifecycle operation owns is released by one rename into the [`DELETING_DIRECTORY`]
//! beside it ([`commit_release`]), so no cut ever lands on an image anything can still find, and
//! the operation answers once that rename is durable. Its bytes are freed off every verb's path,
//! by the gateway's cleanup, one entry at a time, which the releasing process nudges
//! ([`super::deleter`]); each of its passes lists every `.deleting` directory
//! ([`released_in`], 05_gateway.md "Deleting released images").

use std::ffi::{CStr, CString};
use std::fs::{self, File};
use std::io;
use std::num::NonZeroU64;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cowshed_os::fork_lock::Fenced;
use cowshed_os::fsio::{
    open_directory_nofollow, open_or_create_child_directory, rename_noreplace, sync_held_directory,
};

use cowshed_apfs::SECTOR_BYTES;

use super::extents::for_each_run;

/// The extents one fixed step of a release frees, and the first cut of each entry the gateway's
/// cleanup frees (later cuts follow [`next_cut`]).
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

/// The fewest extents a timed cut asks to free ([`next_cut`]; a short tail frees fewer), and the
/// step the gateway's cleanup plans in: a cut of the last 64 runs still frees whole runs from the
/// end.
pub const STEP_FLOOR: StepExtents = StepExtents(NonZeroU64::MIN.saturating_add(63));

/// The wall time the gateway's cleanup sizes each cut toward ([`next_cut`]), under the 500 ms a
/// fork may wait behind one cut (Danny, 2026-10-26).
///
/// Measured 2026-10-26 freeing real ~220 GB released build volumes (~0.9M runs each) beside
/// the running gateway's own cuts: one cut's time varies about 1.8x around the size the last
/// cut's rate predicts, so a 400 ms target put the p90 cut at 672 and 719 ms, past the ceiling,
/// and 250 ms puts it at 379, 448 and 415 ms. Totals per entry were 43.9, 117.8 and 253.1 s at
/// 250 ms against 116.8, 54.0, 75.2 and 120.8 s at the earlier fixed 1,024-run cap: the host's
/// own load moved them more than the sizing did. Cuts of 3.6 to 5.7 s occurred under every
/// sizing, and a probe's create+unlink beside them stalled 2.7 to 5.0 s under both.
pub const CUT_TARGET: Duration = Duration::from_millis(250);

/// The store's free-space floor, in percent of its capacity: below it a land gate fails for want of
/// space. cowshed owns the number; `gateway status` reports it (`imageCleanup.floorPercent`), and
/// a watchdog that judges the store reads it from there.
pub const STORE_FLOOR_PERCENT: u64 = 10;

/// How far above [`STORE_FLOOR_PERCENT`] the cleanup already drains under pressure ([`drain_pace`]).
pub const PRESSURE_MARGIN_PERCENT: u64 = 5;

/// How the gateway's cleanup frees its next entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainPace {
    /// Cuts sized toward [`CUT_TARGET`], so a fork or a git call waits at most about one cut.
    Paced,
    /// One unlink of the whole entry. Measured on the 131,072-extent clone above, one unlink freed
    /// it in 7.6 to 29 s against 35 to 62 s in cuts, while other writers stalled for seconds: under
    /// the floor every gate fails anyway, so freeing faster is worth the stalls.
    Pressure,
}

/// The pace for the next entry when the store has `free` of `capacity` bytes and `queued` entries
/// wait: pressure once free space is under [`STORE_FLOOR_PERCENT`] plus [`PRESSURE_MARGIN_PERCENT`]
/// of capacity and something waits, paced otherwise (and for a capacity nothing reported).
pub fn drain_pace(free: u64, capacity: u64, queued: u64) -> DrainPace {
    let threshold =
        u128::from(capacity) * u128::from(STORE_FLOOR_PERCENT + PRESSURE_MARGIN_PERCENT);
    if queued > 0 && capacity > 0 && u128::from(free) * 100 < threshold {
        DrainPace::Pressure
    } else {
        DrainPace::Paced
    }
}

/// Whether writing `bytes` more into a store with `free` of `capacity` bytes leaves it at or above
/// [`STORE_FLOOR_PERCENT`] plus [`PRESSURE_MARGIN_PERCENT`] of capacity: what a seed compaction's copy
/// needs before it starts and at each check (16_build_volumes.md, "Seed compaction"). Its old
/// seed's blocks stay shared with every volume cloned from it, so the old seed's retirement frees
/// none of what the copy wrote. A capacity nothing reported admits nothing.
pub fn writes_leave_floor(free: u64, capacity: u64, bytes: u64) -> bool {
    capacity > 0
        && u128::from(free.saturating_sub(bytes)) * 100
            >= u128::from(capacity) * u128::from(STORE_FLOOR_PERCENT + PRESSURE_MARGIN_PERCENT)
}
/// The free bytes an unprivileged writer has, and the capacity, of the filesystem holding `path`.
pub fn store_space(path: &Path) -> io::Result<(u64, u64)> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    // SAFETY: `statfs` is plain old data; all-zero is a valid value for the kernel to overwrite.
    let mut stats: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `path` is NUL-terminated and `stats` is a valid, writable `statfs`.
    if unsafe { libc::statfs(path.as_ptr(), &raw mut stats) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // The fields' integer types differ by platform (Darwin's are unsigned, glibc's `f_bsize` is
    // signed), so each converts checked; a value that does not fit reads as no space.
    fn field<T: TryInto<u64>>(value: T) -> u64 {
        value.try_into().unwrap_or(0)
    }
    let block = field(stats.f_bsize);
    Ok((
        field(stats.f_bavail).saturating_mul(block),
        field(stats.f_blocks).saturating_mul(block),
    ))
}

/// The size of the cut after one of `runs` extents that took `took`: the rate it observed carried
/// to `target`, `runs × target / took`, so a cut past the target is followed at once by one the
/// rate puts under it, however much smaller; never more than twice `runs`, since a rate measured
/// on a few runs is no promise for many; never below [`STEP_FLOOR`].
pub fn next_cut(runs: StepExtents, took: Duration, target: Duration) -> StepExtents {
    let grown = runs.get().saturating_mul(2);
    let carried = match u64::try_from(took.as_nanos()) {
        Ok(0) => grown,
        Ok(took) => u64::try_from(u128::from(runs.get()) * target.as_nanos() / u128::from(took))
            .unwrap_or(u64::MAX),
        Err(_) => STEP_FLOOR.get(),
    };
    StepExtents(NonZeroU64::new(carried.min(grown).max(STEP_FLOOR.get())).unwrap_or(STEP_FLOOR.0))
}

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
    /// The length a cut freeing the plan's next `steps` steps cuts the file to, and how many cuts
    /// the plan keeps once that cut is made; `None` once every cut is made.
    fn reach(&self, steps: NonZeroU64) -> Option<(u64, usize)> {
        let kept = self
            .cuts
            .len()
            .saturating_sub(usize::try_from(steps.get()).unwrap_or(usize::MAX));
        self.cuts.get(kept).map(|&cut| (cut, kept))
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
        while self.plan.next().is_some() {
            self.step()?;
        }
        self.unlink()
    }

    /// Make every cut left, each sized from the rate the one before it in this entry took
    /// ([`next_cut`] toward [`CUT_TARGET`]), the first freeing [`STEP_EXTENTS`] runs, and unlink
    /// the empty file. The plan's steps are [`STEP_FLOOR`] runs, so each size is rounded down to a
    /// whole number of them, and that rounded size is the one whose time the rate is taken from.
    fn finish_timed(mut self) -> io::Result<Released> {
        let mut size = STEP_EXTENTS;
        loop {
            let steps = NonZeroU64::new(size.get() / STEP_FLOOR.get()).unwrap_or(NonZeroU64::MIN);
            let Some((cut, kept)) = self.plan.reach(steps) else {
                break;
            };
            let started = Instant::now();
            self.file.set_len(cut)?;
            // The plan drops the cuts this one made only once it is made: the length is the journal.
            self.plan.cuts.truncate(kept);
            let made = StepExtents(steps.saturating_mul(STEP_FLOOR.0));
            size = next_cut(made, started.elapsed(), CUT_TARGET);
            self.steps += 1;
        }
        self.unlink()
    }

    /// Unlink the emptied file from its held directory.
    fn unlink(self) -> io::Result<Released> {
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
/// Free the one committed release at `entry` and unlink it, at `pace`: in bounded cuts, or under
/// pressure by one unlink ([`DrainPace`]). What the gateway's cleanup does with each entry it picks
/// (05_gateway.md, "Deleting released images"). An entry
/// another process holds is left to it ([`Released::Busy`]): another cleanup still cutting it, or
/// an earlier deleter. One another deletion unlinked, or nothing there, is
/// [`Released::Absent`]. The directory and its parent are synced before the first cut, since the
/// releaser may have died between its rename and that rename's syncs, and the directory after the
/// unlink. A link or any other non-regular file at `entry` refuses, untouched.
pub fn free_committed(entry: &Path, pace: DrainPace) -> io::Result<Released> {
    let (directory, name) = match anchor(entry) {
        Ok(anchored) => anchored,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Released::Absent),
        Err(error) => return Err(error),
    };
    let file = match open_entry(&directory, &name)? {
        Entry::Absent => return Ok(Released::Absent),
        Entry::NotImage => return Err(not_image(entry.display())),
        Entry::Image(file) => file,
    };
    match file.try_lock() {
        Ok(()) => {}
        Err(fs::TryLockError::WouldBlock) => return Ok(Released::Busy),
        Err(fs::TryLockError::Error(error)) => return Err(error),
    }
    if file.metadata()?.nlink() == 0 {
        return Ok(Released::Absent);
    }
    sync_held_directory(&directory)?;
    if let Some(parent) = entry.parent().and_then(Path::parent) {
        sync_held_directory(&open_directory_nofollow(parent)?)?;
    }
    let released = match pace {
        DrainPace::Paced => {
            Release::plan(directory.try_clone()?, name, file, STEP_FLOOR)?.finish_timed()?
        }
        DrainPace::Pressure => {
            let bytes = file.metadata()?.blocks().saturating_mul(SECTOR_BYTES);
            // SAFETY: the held directory descriptor and the NUL-terminated name outlive the call.
            if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } != 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::NotFound {
                    return Err(error);
                }
            }
            Released::Freed { bytes, steps: 0 }
        }
    };
    sync_held_directory(&directory)?;
    Ok(released)
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

/// What the name `name` in `directory` is, read with `fstatat(2)` without following a link and
/// without opening it: a release's commit needs no access to an image's bytes.
fn classify(directory: &File, name: &CStr) -> io::Result<Kind> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: the held directory descriptor and the NUL-terminated name outlive the call, and
    // `stat` is written whole on success.
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ENOENT) => Ok(Kind::Absent),
            _ => Err(error),
        };
    }
    // SAFETY: fstatat succeeded, so it filled `stat`.
    let stat = unsafe { stat.assume_init() };
    Ok(if stat.st_mode & libc::S_IFMT == libc::S_IFREG {
        Kind::Image {
            writable: stat.st_mode & (libc::S_IRUSR | libc::S_IWUSR)
                == libc::S_IRUSR | libc::S_IWUSR,
        }
    } else {
        Kind::NotImage
    })
}

/// What a name a release reads turned out to be, without opening it.
enum Kind {
    Absent,
    /// A regular file; `writable` when its owner may read and write it.
    Image {
        writable: bool,
    },
    /// A link, a directory or anything else that is not a regular file.
    NotImage,
}

/// Open the committed entry `name` of `directory` to free it, without following a link at it.
/// An entry its owner may not read and write (an image made unreadable while it was live) is made
/// so first: it is the deleter's to free, and the commit that put it here never opened it.
fn open_entry(directory: &File, name: &CStr) -> io::Result<Entry> {
    match classify(directory, name)? {
        Kind::Absent => return Ok(Entry::Absent),
        Kind::NotImage => return Ok(Entry::NotImage),
        Kind::Image { writable: true } => {}
        Kind::Image { writable: false } => {
            // SAFETY: the held directory descriptor and the NUL-terminated name outlive the call;
            // NOFOLLOW changes a link planted since, never its target, and the open below refuses it.
            if unsafe {
                libc::fchmodat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::S_IRUSR | libc::S_IWUSR,
                    libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                return Err(io::Error::last_os_error());
            }
        }
    }
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

/// The directory, beside the images it holds, where each released image waits for
/// the gateway's cleanup: `build/.deleting/<id>.dmg`, `sessions/.deleting/<name>.dmg`
/// (16_build_volumes.md, "Garbage collection"; 02_workspaces.md, `rm` step 5; 05_gateway.md,
/// "Deleting released images"). An entry exists from its operation's rename to its unlink; nothing
/// mounts, forks, reads or names it in between.
pub const DELETING_DIRECTORY: &str = ".deleting";

/// Commit the release of `image`, owned by the calling operation, to `entry` in its directory's
/// [`DELETING_DIRECTORY`], crash-safely, and say whether an image was released. Its caller then
/// nudges the gateway's cleanup ([`super::deleter::nudge`]).
///
/// The commit is one rename of `image` to `entry`, with every directory it touched synced: before
/// it the image is its owner's and untouched, after it nothing finds the image by its name, so a
/// cut never lands on an image that can still be mounted, forked or read. The image is never
/// opened: the commit reads none of its bytes and needs no access to it, and every deleter makes
/// the rename durable itself before its first cut. Once the syncs are done this returns: the
/// caller removes the owner record at once, and the bytes wait for the gateway's cleanup. Every directory is held by descriptor and no entry is opened through a link: a link
/// or any other non-regular file at either name refuses, untouched.
///
/// An entry already under `entry` is the same image's committed release (an id and an incarnation
/// are never reused): a retry that finds no `image` makes that rename durable and still answers
/// `true`, freeing nothing. The rename never replaces a file, because a rename over one frees the
/// file it replaces in one go.
pub fn commit_release(image: &Path, entry: &Path) -> io::Result<bool> {
    let anchors = Anchors::open(image, entry)?;
    if anchors.commit()? {
        Ok(true)
    } else {
        anchors.confirm_left()
    }
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

    /// Make durable, without freeing it, the entry an earlier attempt committed under this
    /// release's name: `false` when none is there. That attempt may have died, or failed a sync,
    /// between its rename and the rename's syncs, and its caller removes the owner record once
    /// this answers. So this syncs those directories itself, whoever holds the entry: another
    /// process's lock says it is releasing the entry, not that the rename is durable yet. Syncing
    /// a directory cuts nothing, so it needs no lock and waits for no releaser.
    fn confirm_left(&self) -> io::Result<bool> {
        match classify(&self.journal, &self.entry_name)? {
            Kind::Absent => return Ok(false),
            Kind::NotImage => return Err(not_image(self.entry_name.to_string_lossy())),
            Kind::Image { .. } => {}
        }
        sync_held_directory(&self.journal)?;
        sync_held_directory(&self.image_directory)?;
        sync_held_directory(&self.journal_parent)?;
        Ok(true)
    }

    /// Commit the image: rename it into the journal, never replacing a file there, and make the
    /// rename durable. `false` when no image is there. The image is never opened, so the commit
    /// reads none of its bytes and needs no access to it; a cleanup that lists the entry before
    /// these syncs makes the rename durable itself before its first cut ([`free_committed`]).
    fn commit(&self) -> io::Result<bool> {
        match classify(&self.image_directory, &self.image_name)? {
            Kind::Absent => return Ok(false),
            Kind::NotImage => return Err(not_image(self.image_name.to_string_lossy())),
            Kind::Image { .. } => {}
        }
        match rename_noreplace(
            self.image_directory.as_raw_fd(),
            &self.image_name,
            self.journal.as_raw_fd(),
            &self.entry_name,
        ) {
            Ok(()) => {}
            // Another release of the same image renamed it first.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        }
        // The entry, the image's departure, and the journal's own name, which an earlier attempt
        // may have made without syncing.
        sync_held_directory(&self.journal)?;
        sync_held_directory(&self.image_directory)?;
        sync_held_directory(&self.journal_parent)?;
        Ok(true)
    }
}

/// What freeing released images freed.
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

/// The released images a listing of deleting directories found, in the order it found them, and
/// each entry or directory it left where it was, with why. Values, not lines on stderr: the
/// gateway's cleanup says the ones it leaves and frees the rest.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Listing {
    pub entries: Vec<PathBuf>,
    pub left: Vec<Left>,
}

impl Listing {
    pub fn add(&mut self, other: Self) {
        self.entries.extend(other.entries);
        self.left.extend(other.left);
    }

    fn leave(&mut self, path: PathBuf, why: Why) {
        self.left.push(Left { path, why });
    }
}

/// An entry, or a whole directory of them, a listing or a free left untouched.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Left {
    pub path: PathBuf,
    pub why: Why,
}

/// Why a listing or a free left [`Left::path`].
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Why {
    /// Not a regular file named as a released image: never opened through a link, never freed.
    NotImage,
    /// Another process holds the entry's lock: another cleanup or deleter is still cutting it.
    Busy,
    /// The directory could not be read; its entries stay for the next pass.
    Failed(String),
}

impl std::fmt::Display for Left {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let path = self.path.display();
        match &self.why {
            Why::NotImage => write!(formatter, "{path} is not a released image; it is left"),
            Why::Busy => write!(
                formatter,
                "released image {path} is held by another process; it is left to that process"
            ),
            Why::Failed(error) => write!(formatter, "released images in {path} stay: {error}"),
        }
    }
}

/// The committed releases in `directory` (a [`DELETING_DIRECTORY`]): every regular file whose
/// name `is_entry` accepts; anything else is left, untouched, and named. Each entry's type is the
/// one the directory read reports (`d_type`), so the listing never stats or opens an entry: a
/// stat of the entry the gateway's cleanup is cutting waits for that whole cut, and the first cut
/// of a clone still sharing its extent map runs 32 to 81 s in one call (measured 2026-10-26).
/// The directory is held by descriptor and refused when it is a link. One that does not exist is
/// empty. Listing syncs nothing and locks nothing: an entry another process holds is found busy
/// when its free comes ([`free_committed`]).
pub fn released_in(directory: &Path, is_entry: impl Fn(&str) -> bool) -> io::Result<Listing> {
    // Held only to refuse a link in place of the directory.
    let _held = match open_directory_nofollow(directory) {
        Ok(held) => held,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Listing::default()),
        Err(error) => return Err(error),
    };
    let mut listing = Listing::default();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let image =
            entry.file_name().to_str().is_some_and(&is_entry) && entry.file_type()?.is_file();
        if image {
            listing.entries.push(path);
        } else {
            listing.leave(path, Why::NotImage);
        }
    }
    Ok(listing)
}

/// Whether the store entry at `path` holds projects: an encoded owner, or
/// [`crate::storage::bootstrap::RETIRED_PROJECTS_NAMESPACE`], whose entries are archived projects.
fn holds_projects(path: &Path) -> bool {
    is_encoded_entry(path)
        || path.file_name().is_some_and(|name| {
            name == std::ffi::OsStr::new(crate::storage::bootstrap::RETIRED_PROJECTS_NAMESPACE)
        })
}

/// Whether the store entry at `path` is named as an encoded owner or repository component.
fn is_encoded_entry(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(crate::repository::is_encoded_component)
}

/// [`released_in`] the directory `<project>/<images>/<namespace>` of every project in the store at
/// `store` (`<store>/<owner>/<repo>`), together. A directory that cannot be listed is left in the
/// listing, so one unreadable project never holds back the rest.
pub fn released_in_store(
    store: &Path,
    images: &str,
    namespace: &str,
    is_entry: impl Fn(&str) -> bool,
) -> Listing {
    let mut pass = Listing::default();
    let failed = |path: &Path, error: io::Error| Left {
        path: path.to_owned(),
        why: Why::Failed(error.to_string()),
    };
    let owners = match fs::read_dir(store) {
        Ok(owners) => owners,
        Err(error) => {
            pass.left.push(failed(store, error));
            return pass;
        }
    };
    for owner in owners {
        let owner = match owner {
            // Only an encoded owner, or the archive of retired projects, holds projects: the volume's
            // system directories do not.
            Ok(owner) if !holds_projects(&owner.path()) => continue,
            Ok(owner) => owner.path(),
            Err(error) => {
                pass.left.push(failed(store, error));
                continue;
            }
        };
        let repos = match fs::read_dir(&owner) {
            Ok(repos) => repos,
            // A file beside the owners (`host.json`) holds no projects.
            Err(error) if error.raw_os_error() == Some(libc::ENOTDIR) => continue,
            Err(error) => {
                pass.left.push(failed(&owner, error));
                continue;
            }
        };
        for repo in repos {
            let repo = match repo.and_then(|repo| Ok((repo.file_type()?, repo.path()))) {
                // A file beside an owner's projects (`.cowshed-owner`) holds no project.
                Ok((kind, repo)) if !kind.is_dir() || !is_encoded_entry(&repo) => continue,
                Ok((_, repo)) => repo,
                Err(error) => {
                    pass.left.push(failed(&owner, error));
                    continue;
                }
            };
            let directory = repo.join(images).join(namespace);
            match released_in(&directory, &is_entry) {
                Ok(project) => pass.add(project),
                Err(error) => pass.left.push(failed(&directory, error)),
            }
        }
    }
    pass
}

#[cfg(test)]
mod tests {
    use cowshed_os::test_backstop::FileBackstop as _;
    use proptest::prelude::*;

    /// The cleanup drains by unlink once the store is under its 10% floor plus the 5% margin and
    /// something waits; paced at or above that line, with nothing queued, or with no capacity read.
    #[test]
    fn the_drain_pace_follows_free_space_against_floor_plus_margin() {
        use super::{DrainPace, drain_pace};
        const TIB: u64 = 1 << 40;
        let capacity = 100 * TIB;
        for (free, queued, expected) in [
            (50 * TIB, 30, DrainPace::Paced),
            (16 * TIB, 30, DrainPace::Paced),
            (15 * TIB, 30, DrainPace::Paced),
            (15 * TIB - 1, 30, DrainPace::Pressure),
            (12 * TIB, 1, DrainPace::Pressure),
            (9 * TIB, 30, DrainPace::Pressure),
            (0, 1, DrainPace::Pressure),
            (9 * TIB, 0, DrainPace::Paced),
            (0, 0, DrainPace::Paced),
        ] {
            assert_eq!(
                drain_pace(free, capacity, queued),
                expected,
                "free {free} of {capacity}, {queued} queued"
            );
        }
        assert_eq!(drain_pace(0, 0, 30), DrainPace::Paced, "no capacity read");
        assert_eq!(
            drain_pace(u64::MAX / 200, u64::MAX, 3),
            DrainPace::Pressure,
            "no overflow at the largest sizes"
        );
    }

    /// A seed compaction writes a whole copy whose old blocks stay shared with every volume
    /// cloned from the seed, so retiring the old seed frees none of it: a copy starts, and goes on,
    /// only while what it still writes leaves the store at or above the floor plus the margin.
    #[test]
    fn a_compaction_writes_only_what_leaves_the_floor_plus_margin() {
        use super::writes_leave_floor;
        const GIB: u64 = 1 << 30;
        let capacity = 7_760 * GIB;
        let line = capacity * 15 / 100;
        for (free, bytes, fits) in [
            // The 2026-10-28 copy: 415 GB into a store at 10.5% free, which it took to 8.6%.
            (815 * GIB, 387 * GIB, false),
            (line + 387 * GIB, 387 * GIB, true),
            (line + 387 * GIB - 1, 387 * GIB, false),
            (line, 0, true),
            (line - 1, 0, false),
            (100 * GIB, 400 * GIB, false),
        ] {
            assert_eq!(
                writes_leave_floor(free, capacity, bytes),
                fits,
                "{bytes} more bytes into {free} free of {capacity}"
            );
        }
        assert!(
            !writes_leave_floor(u64::MAX, 0, 0),
            "no capacity read admits nothing"
        );
        assert!(
            writes_leave_floor(u64::MAX, u64::MAX, 0),
            "no overflow at the largest sizes"
        );
    }
    use super::*;

    /// What the gateway's cleanup did over one deleting directory, in the shape these tests read.
    #[derive(Debug, Default, Eq, PartialEq)]
    struct Pass {
        drained: Drained,
        left: Vec<Left>,
    }

    /// The cleanup over `directory` alone: list it, then free each entry, as the gateway's queue
    /// does.
    fn drain(directory: &Path, is_entry: impl Fn(&str) -> bool) -> io::Result<Pass> {
        let Listing { entries, left } = released_in(directory, is_entry)?;
        let mut pass = Pass {
            drained: Drained::default(),
            left,
        };
        for entry in entries {
            match free_committed(&entry, DrainPace::Paced)? {
                Released::Freed { bytes, .. } => pass.drained.add(Drained { images: 1, bytes }),
                Released::Busy => pass.left.push(Left {
                    path: entry,
                    why: Why::Busy,
                }),
                Released::Absent => {}
            }
        }
        Ok(pass)
    }

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

    /// The next cut's size is the observed rate times the target (05_gateway.md "Deleting released
    /// images"): `runs × target / observed`, at most twice `runs` and at least the 64-run floor.
    /// An overshoot shrinks it at once, however far; an undershoot grows it at most twofold.
    #[test]
    fn the_next_cut_is_the_observed_rate_times_the_target() {
        let ms = std::time::Duration::from_millis;
        let target = ms(400);
        // (this cut's runs, its wall time, the next cut's runs)
        let rows: [(u64, std::time::Duration, u64); 9] = [
            (1024, ms(400), 1024),
            (1024, ms(200), 2048),
            (1024, ms(100), 2048),
            (1024, std::time::Duration::ZERO, 2048),
            (4096, ms(500), 3276),
            (40_000, ms(5_000), 3200),
            (1024, ms(57_352), 64),
            (64, ms(1_000), 64),
            (30_000, ms(390), 30_769),
        ];
        for (runs, took, next) in rows {
            assert_eq!(
                next_cut(step(runs), took, target),
                step(next),
                "{runs} runs in {took:?}"
            );
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

    /// The crash the deleting directory exists for. While another deleter holds a committed
    /// entry, a pass leaves it uncut and names it
    /// busy. Then a deleter makes the first cut and stops (a gateway restart): nothing finds the
    /// image by its name, and the next pass finishes the release from what the cut left. The
    /// owner's retry after that finds nothing to release.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_release_a_stopped_deleter_cut_short_is_finished_by_the_next_pass() {
        use crate::storage::apfs::extents::count_extents;
        use crate::storage::apfs::extents::tests::{fragmented_sparse_file, scratch};

        let any = |name: &str| name.ends_with(".dmg");
        let root = scratch("deleting-restart");
        let (image, _) = fragmented_sparse_file(&root.0);
        assert!(count_extents(&image).expect("count").get() >= 256);
        let entry = root.0.join(DELETING_DIRECTORY).join("image.dmg");
        let journal = entry.parent().expect("deleting directory").to_owned();
        assert!(commit_release(&image, &entry).expect("commit"));
        let committed = fs::File::open(&entry).expect("another deleter's descriptor");
        committed
            .lock_bounded()
            .expect("another deleter holds the entry");
        assert!(!image.exists(), "the commit takes the image off its name");
        let length = fs::metadata(&entry).expect("stat").len();
        let beside_owner = drain(&journal, any).expect("a deleter beside the owner");
        assert_eq!(beside_owner.drained, Drained::default());
        assert_eq!(
            beside_owner.left,
            vec![Left {
                path: entry.clone(),
                why: Why::Busy
            }]
        );
        assert_eq!(
            fs::metadata(&entry).expect("stat").len(),
            length,
            "a deleter never cuts an entry another holds"
        );
        drop(committed);

        let Opened::Release(mut stopped) = Release::open(&entry, step(32)).expect("open") else {
            panic!("the committed entry opens for a deleter once the other lets go");
        };
        let cut = stopped.step().expect("cut").expect("a cut is left");
        assert!(cut < length, "the first cut shortens the entry");
        drop(stopped);
        assert!(entry.exists(), "the stop leaves the entry");

        let pass = drain(&journal, any).expect("the next pass");
        assert_eq!(pass.drained.images, 1);
        assert!(
            pass.drained.bytes > 0,
            "the next pass frees what the cut left"
        );
        assert!(pass.left.is_empty(), "{pass:?}");
        assert!(!entry.exists(), "the next pass finishes the release");
        assert!(
            !commit_release(&image, &entry).expect("a later retry"),
            "nothing is left for the owner's retry"
        );
    }

    /// A release is committed when it returns and freed later: the image is off its name, the
    /// entry keeps every byte and its lock is free, so the deleter's next pass frees it rather than
    /// leaving it to an owner that has moved on. A retry finds the commit and frees nothing.
    #[test]
    fn a_release_commits_and_leaves_the_entry_whole_for_the_deleter() {
        let root =
            std::env::temp_dir().join(format!("cowshed-release-commit-{}", uuid::Uuid::new_v4()));
        let images = root.join("images");
        fs::create_dir_all(&images).expect("images");
        let image = images.join("image.dmg");
        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        let entry = images.join(DELETING_DIRECTORY).join("image.dmg");

        assert!(commit_release(&image, &entry).expect("commit"));
        assert!(!image.exists(), "the commit takes the image off its name");
        assert_eq!(
            fs::read(&entry).expect("the entry"),
            vec![9_u8; 64 * 1024],
            "a release frees nothing before it returns"
        );
        fs::File::open(&entry)
            .expect("open the entry")
            .try_lock()
            .expect("the entry is left unlocked for the deleter");
        assert!(
            commit_release(&image, &entry).expect("retry"),
            "a retry answers the commit already made"
        );
        assert!(entry.exists(), "a retry frees nothing either");

        let any = |name: &str| name.ends_with(".dmg");
        let pass = drain(entry.parent().expect("deleting directory"), any).expect("drain");
        assert_eq!(pass.drained.images, 1);
        assert!(!entry.exists(), "the deleter frees the committed release");
        fs::remove_dir_all(&root).expect("clean up");
    }

    /// A release commits an image its owner made unreadable (gc never reads an image it
    /// reclaims): the commit opens nothing. The deleter, whose entry it now is, frees it.
    #[test]
    fn a_release_commits_an_unreadable_image_and_the_deleter_frees_it() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = std::env::temp_dir().join(format!(
            "cowshed-release-unreadable-{}",
            uuid::Uuid::new_v4()
        ));
        let images = root.join("images");
        fs::create_dir_all(&images).expect("images");
        let image = images.join("image.dmg");
        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        fs::set_permissions(&image, fs::Permissions::from_mode(0o000)).expect("unreadable");
        let entry = images.join(DELETING_DIRECTORY).join("image.dmg");

        assert!(commit_release(&image, &entry).expect("a commit needs no access to the image"));
        assert!(!image.exists());
        assert_eq!(
            fs::metadata(&entry)
                .expect("the entry")
                .permissions()
                .mode()
                & 0o777,
            0,
            "the commit leaves the image as it was"
        );
        let pass = drain(entry.parent().expect("deleting directory"), |name| {
            name.ends_with(".dmg")
        })
        .expect("drain");
        assert_eq!(pass.drained.images, 1, "{pass:?}");
        assert!(!entry.exists(), "the deleter frees the unreadable entry");
        fs::remove_dir_all(&root).expect("clean up");
    }

    /// A link at the entry's name, at the image's name, or in place of the deleting directory never
    /// leads a release to another file: each refuses, or is left by the deleter and named, and the
    /// file it names keeps every byte. Nor does anything but a regular file get committed. A pass
    /// that frees nothing syncs nothing, so a deleter passing over what it leaves costs a listing.
    #[test]
    fn a_release_never_frees_through_a_link_or_anything_but_a_regular_file() {
        use cowshed_os::fsio::take_syncs;

        let root =
            std::env::temp_dir().join(format!("cowshed-release-links-{}", uuid::Uuid::new_v4()));
        let images = root.join("images");
        let journal = images.join(DELETING_DIRECTORY);
        fs::create_dir_all(&journal).expect("deleting directory");
        let foreign = root.join("foreign.dmg");
        fs::write(&foreign, vec![7_u8; 64 * 1024]).expect("foreign file");
        let image = images.join("image.dmg");
        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        let entry = journal.join("image.dmg");
        let intact = |path: &Path, byte: u8| {
            assert_eq!(fs::read(path).expect("read"), vec![byte; 64 * 1024]);
        };

        // A link where an earlier commit's entry would be.
        std::os::unix::fs::symlink(&foreign, &entry).expect("entry link");
        assert!(commit_release(&image, &entry).is_err());
        intact(&foreign, 7);
        intact(&image, 9);
        let any = |name: &str| name.ends_with(".dmg");
        take_syncs();
        assert_eq!(
            drain(&journal, any).expect("the deleter"),
            Pass {
                drained: Drained::default(),
                left: vec![Left {
                    path: entry.clone(),
                    why: Why::NotImage
                }],
            }
        );
        assert_eq!(
            take_syncs().full,
            0,
            "a pass that frees nothing syncs nothing"
        );
        intact(&foreign, 7);
        assert!(
            fs::symlink_metadata(&entry).is_ok(),
            "the deleter leaves the link"
        );
        fs::remove_file(&entry).expect("drop the entry link");

        // A link at the image's name.
        fs::remove_file(&image).expect("drop the image");
        std::os::unix::fs::symlink(&foreign, &image).expect("image link");
        assert!(commit_release(&image, &entry).is_err());
        intact(&foreign, 7);
        assert!(!entry.exists(), "nothing was committed");
        fs::remove_file(&image).expect("drop the image link");

        // A directory at the image's name.
        fs::create_dir(&image).expect("image directory");
        assert!(commit_release(&image, &entry).is_err());
        assert!(image.is_dir() && !entry.exists());
        fs::remove_dir(&image).expect("drop the image directory");

        // A link in place of the deleting directory.
        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        let elsewhere = root.join("elsewhere");
        fs::create_dir(&elsewhere).expect("elsewhere");
        fs::remove_dir(&journal).expect("drop the deleting directory");
        std::os::unix::fs::symlink(&elsewhere, &journal).expect("deleting directory link");
        assert!(commit_release(&image, &entry).is_err());
        intact(&image, 9);
        assert_eq!(fs::read_dir(&elsewhere).expect("elsewhere").count(), 0);
        fs::write(elsewhere.join("other.dmg"), b"kept").expect("other");
        assert!(
            drain(&journal, any).is_err(),
            "a linked deleting directory refuses"
        );
        assert_eq!(
            fs::read(elsewhere.join("other.dmg")).expect("other"),
            b"kept"
        );
        fs::remove_dir_all(&root).expect("cleanup");
    }

    /// The cleanup cuts an image of many runs in steps, and never cuts an entry another process
    /// holds: that entry is left busy, whole, until its releaser lets go.
    #[cfg(target_os = "macos")]
    #[test]
    fn the_cleanup_cuts_in_steps_and_never_cuts_a_held_entry() {
        use crate::storage::apfs::extents::tests::{fragmented_sparse_file, scratch};

        let root = scratch("cleanup-held");
        let (image, _) = fragmented_sparse_file(&root.0);
        let entry = root.0.join(DELETING_DIRECTORY).join("image.dmg");
        assert!(commit_release(&image, &entry).expect("commit"));
        let length = fs::metadata(&entry).expect("the entry").len();
        let held = fs::File::open(&entry).expect("a releaser's descriptor");
        held.try_lock().expect("the releaser holds the entry");
        assert_eq!(
            free_committed(&entry, DrainPace::Paced).expect("a held entry"),
            Released::Busy
        );
        assert_eq!(
            fs::metadata(&entry).expect("the held entry").len(),
            length,
            "a held entry is never cut"
        );
        drop(held);
        let Released::Freed { steps, .. } =
            free_committed(&entry, DrainPace::Paced).expect("the free")
        else {
            panic!("the entry is freed once its releaser lets go");
        };
        assert!(steps >= 1, "an image is freed by cuts");
        assert!(!entry.exists(), "the freed entry is unlinked");
    }

    /// An owner that died between its commit's rename and that rename's syncs leaves an entry a
    /// crash could still put back under the image's name. The deleter syncs the deleting directory
    /// and its parent, under the entry's lock, before its first cut, and the directory again once
    /// the entry is unlinked. The owner's retry answers its earlier commit only once that rename
    /// is durable, and frees none of it.
    #[test]
    fn a_rename_its_owner_never_synced_is_made_durable_before_the_first_cut() {
        use cowshed_os::fsio::take_syncs;

        let root =
            std::env::temp_dir().join(format!("cowshed-release-unsynced-{}", uuid::Uuid::new_v4()));
        let images = root.join("images");
        let journal = images.join(DELETING_DIRECTORY);
        fs::create_dir_all(&journal).expect("deleting directory");
        let image = images.join("image.dmg");
        let entry = journal.join("image.dmg");
        let any = |name: &str| name.ends_with(".dmg");

        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        fs::rename(&image, &entry).expect("the owner's rename, never synced");
        take_syncs();
        let pass = drain(&journal, any).expect("the deleter");
        assert_eq!(pass.drained.images, 1);
        assert!(!entry.exists());
        let syncs = take_syncs();
        assert_eq!(
            syncs.full, 3,
            "the directory and its parent before the cut, the directory after the unlink: {syncs:?}"
        );

        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        fs::rename(&image, &entry).expect("the owner's rename, never synced");
        take_syncs();
        assert!(commit_release(&image, &entry).expect("the owner's retry"));
        let syncs = take_syncs();
        assert_eq!(
            syncs.full, 3,
            "the deleting directory, the image's directory and their parent: {syncs:?}"
        );
        assert_eq!(fs::read(&entry).expect("the entry"), vec![9_u8; 64 * 1024]);

        // A deleter holding the entry, paused before its own syncs, is no proof the rename is
        // durable: the retry still makes it so before it answers, and cuts nothing.
        fs::remove_file(&entry).expect("drop the entry");
        fs::write(&image, vec![9_u8; 64 * 1024]).expect("image");
        fs::rename(&image, &entry).expect("a rename nobody synced");
        let releaser = fs::File::open(&entry).expect("the deleter's descriptor");
        releaser
            .lock_bounded()
            .expect("the deleter holds the entry");
        take_syncs();
        assert!(commit_release(&image, &entry).expect("the retry beside a held entry"));
        let syncs = take_syncs();
        assert_eq!(
            syncs.full, 3,
            "a held entry's rename is made durable before the retry answers: {syncs:?}"
        );
        assert_eq!(fs::read(&entry).expect("the entry"), vec![9_u8; 64 * 1024]);
        drop(releaser);
        fs::remove_dir_all(&root).expect("cleanup");
    }
}
