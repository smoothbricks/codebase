//! The released-image cleanup (05_gateway.md, "Deleting released images").
//!
//! A release is one rename into a `.deleting` directory: that rename is the commit, and the
//! `.deleting` directories are the queue. This gateway frees what they hold on one thread of its
//! own, `image-cleanup`, one entry at a time, so one release cut runs on the store's container at
//! once. Its only inputs are a listing of those directories and a nudge: each pass lists them and
//! frees every entry, oldest first, and a releaser's `released` request, which carries nothing,
//! starts the next pass. The thread keeps no record of what an entry holds, so nothing it knows can
//! disagree with the directories across a crash or a restart. Measured 2026-10-25: nine deletion
//! children each truncating a released image at once saturated the container, and lsof, git,
//! forks and execs sat in uninterruptible waits behind them.

use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use cowshed_core::storage::apfs::release::{
    DrainPace, Released, Why, drain_pace, free_committed, store_space,
};

/// A handle to the cleanup thread: nudges it to list the store again.
#[derive(Clone, Debug)]
pub(crate) struct ImageCleanup {
    nudges: mpsc::Sender<()>,
}

/// The nudges an [`ImageCleanup`] handle sends, heard by the thread [`ImageCleanup::start`] starts.
pub(crate) struct Heard(mpsc::Receiver<()>);

impl ImageCleanup {
    /// A handle the gateway publishes before the thread starts, and what the thread hears: every
    /// nudge sent once the handle is published reaches a pass that lists after it.
    pub(crate) fn new() -> (Self, Heard) {
        let (nudges, heard) = mpsc::channel();
        (Self { nudges }, Heard(heard))
    }

    /// Start the cleanup of the store at `store`: its thread lists what is released there, frees
    /// it, and lists again at every nudge its handle sends.
    pub(crate) fn start(store: PathBuf, heard: Heard) -> std::io::Result<()> {
        std::thread::Builder::new()
            .name("image-cleanup".to_owned())
            .spawn(move || run(&store, &heard.0))
            .map(|_| ())
    }

    /// Something was released: list the store again once the pass under way ends.
    pub(crate) fn nudge(&self) -> Result<(), String> {
        self.nudges
            .send(())
            .map_err(|_| "the released-image cleanup has stopped".to_owned())
    }
}

fn run(store: &Path, heard: &mpsc::Receiver<()>) {
    let mut first = true;
    let mut pace = DrainPace::Paced;
    loop {
        let listing = cowshed_core::storage::apfs::deleter::leftovers(store);
        // A stray is named once, at the gateway's start; a directory that cannot be read, at every
        // pass that finds it so.
        for left in listing
            .left
            .iter()
            .filter(|left| first || matches!(left.why, Why::Failed(_)))
        {
            cowshed_os::eprintln!("cowshed: {left}");
        }
        first = false;
        let entries = oldest_first(listing.entries);
        let total = entries.len();
        for (index, entry) in entries.into_iter().enumerate() {
            // The store's free space is read again before every entry: an entry freed, or anything
            // else written, moves it. A store whose space cannot be read is drained paced.
            let next = store_space(store).map_or(DrainPace::Paced, |(free, capacity)| {
                drain_pace(free, capacity, (total - index) as u64)
            });
            if next != pace {
                cowshed_os::eprintln!(
                    "cowshed: image cleanup drains {}",
                    match next {
                        DrainPace::Pressure =>
                            "under pressure: the store is under its free-space floor plus margin, so each released image goes by one unlink",
                        DrainPace::Paced =>
                            "paced again: the store is above its free-space floor plus margin",
                    }
                );
                pace = next;
            }
            match free_committed(&entry, pace) {
                Ok(Released::Freed { bytes, steps }) => cowshed_os::eprintln!(
                    "cowshed: freed released image {} ({bytes} bytes, {steps} cuts)",
                    entry.display()
                ),
                Ok(Released::Absent) => {}
                Ok(Released::Busy) => cowshed_os::eprintln!(
                    "cowshed: released image {} is held by another process (its releaser, or a gateway still cutting it); it waits for the next pass",
                    entry.display()
                ),
                Err(error) => cowshed_os::eprintln!(
                    "cowshed: released image {} stays in .deleting for the next pass, which the next release or gateway start begins: {error}",
                    entry.display()
                ),
            }
        }
        // Every nudge heard during the pass is one more pass.
        if heard.recv().is_err() {
            return;
        }
        while heard.try_recv().is_ok() {}
    }
}

/// `entries` oldest released first: by the change time the release's rename set, then by path.
/// An entry that cannot be read goes last, where its free says why.
fn oldest_first(entries: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut dated: Vec<((i64, i64), PathBuf)> = entries
        .into_iter()
        .map(|entry| {
            let changed = std::fs::symlink_metadata(&entry)
                .map_or((i64::MAX, i64::MAX), |at| (at.ctime(), at.ctime_nsec()));
            (changed, entry)
        })
        .collect();
    dated.sort();
    dated.into_iter().map(|(_, entry)| entry).collect()
}
