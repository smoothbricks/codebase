//! Releasing an image file's storage in bounded steps (16_build_volumes.md, "Garbage
//! collection").
//!
//! One `unlink(2)` of a written image frees its whole extent tree as one operation, and APFS
//! frees extents one at a time: measured 2026-10-22 on this host's container (a clone of a 2 GiB
//! file with a 4 KiB write every 32 KiB, 131,072 extents), one unlink ran 7.6 s to 29 s, and a
//! trashed 300 GiB build image holds millions. So a release cuts the file from its end, at most
//! [`STEP_EXTENTS`] extents per `ftruncate(2)`, and unlinks it only once it is empty: every
//! step is one small free, and the writers of the container get their turn between steps.
//!
//! The file's length is the release's journal. A step cuts at the start of a run, so an
//! interrupted release leaves a shorter file with whole runs before its end; opening it again
//! walks what is left and plans the rest. Nothing else records progress.
//!
//! The plan is a pure function of the run starts ([`ReleasePlan`]); [`Release`] is the shell that
//! walks the runs, takes the file's lock and makes the cuts.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

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

/// A release in progress: the file, locked exclusively, and the cuts left.
#[derive(Debug)]
pub struct Release {
    path: PathBuf,
    file: File,
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
    /// cut one file, and plan from the runs its data occupies now.
    pub fn open(path: &Path, step: StepExtents) -> io::Result<Opened> {
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Opened::Absent),
            Err(error) => return Err(error),
        };
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => return Ok(Opened::Busy),
            Err(fs::TryLockError::Error(error)) => return Err(error),
        }
        let metadata = file.metadata()?;
        let length = metadata.len();
        let mut planner = Planner::new(step);
        for_each_run(&file, length, |start| planner.run(start))?;
        Ok(Opened::Release(Self {
            path: path.to_owned(),
            plan: planner.plan(length),
            bytes: metadata.blocks().saturating_mul(SECTOR_BYTES),
            file,
            steps: 0,
        }))
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

    /// Make every cut left and unlink the empty file.
    pub fn finish(mut self) -> io::Result<Released> {
        while self.step()?.is_some() {}
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            // Unlinked by hand under the lock: what it held is freed all the same.
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
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
}
