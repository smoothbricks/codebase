//! The released-image cleanup (05_gateway.md, "Deleting released images").
//!
//! Every release is a hand-off: its process renames the image into a `.deleting` directory and
//! hands the entry here with one `released` request, then returns. This gateway frees the entries
//! on one thread of its own, `image-cleanup`, one at a time, so one release cut runs on the
//! store's container at once. It picks the next by [`cleanup_order::next`] over what each
//! releaser recorded and whether the container is under pressure, and cuts it at once: nothing
//! waits for gate work. Measured 2026-10-25: nine deletion children each truncating a released
//! image at once saturated the container, and lsof, git, forks and execs sat in uninterruptible
//! waits behind them.

use std::path::{Path, PathBuf};
use std::sync::mpsc;

use cowshed_core::storage::apfs::cleanup_order::{self, Pending, Pressure};
use cowshed_core::storage::apfs::release::{Released, free_committed};
use cowshed_gateway_types::ReleaseRecord;

/// The container is under pressure while its free space is below one part in this many of its
/// capacity: below an eighth, the queue frees the most unique bytes per extent first.
pub(crate) const PRESSURE_FREE_FRACTION: u64 = 8;

/// A handle to the cleanup thread: hands it entries. Dropping every handle ends the thread once
/// its queue is empty.
#[derive(Clone, Debug)]
pub(crate) struct ImageCleanup {
    store: std::sync::Arc<Path>,
    entries: mpsc::Sender<(PathBuf, ReleaseRecord)>,
}

impl ImageCleanup {
    /// Start the cleanup of the store at `store`: its thread lists what an earlier gateway left,
    /// queues it with no record, and serves hand-overs from then on.
    pub(crate) fn start(store: PathBuf) -> std::io::Result<Self> {
        let (entries, handed) = mpsc::channel();
        let shared: std::sync::Arc<Path> = std::sync::Arc::from(store.as_path());
        let again = entries.clone();
        std::thread::Builder::new()
            .name("image-cleanup".to_owned())
            .spawn(move || run(&store, &handed, &again))?;
        Ok(Self {
            store: shared,
            entries,
        })
    }

    /// Queue `entry`, handed over with what its releaser knew; refused, saying why, when it is no
    /// entry of this store's deleting directories or the thread is gone.
    pub(crate) fn take(&self, entry: PathBuf, record: ReleaseRecord) -> Result<(), String> {
        if !admissible(&self.store, &entry) || !really_in(&self.store, &entry) {
            return Err(format!(
                "{} is no released image of the store {}: an entry is an absolute path in one of its .deleting directories",
                entry.display(),
                self.store.display()
            ));
        }
        self.entries
            .send((entry, record))
            .map_err(|_| "the released-image cleanup has stopped".to_owned())
    }
}

/// One queued entry and its place in [`cleanup_order`].
struct Queued {
    entry: PathBuf,
    pending: Pending,
}

fn run(
    store: &Path,
    handed: &mpsc::Receiver<(PathBuf, ReleaseRecord)>,
    again: &mpsc::Sender<(PathBuf, ReleaseRecord)>,
) {
    let mut queue: Vec<Queued> = Vec::new();
    let mut seq = 0_u64;
    let mut said_space = false;
    let mut push = |queue: &mut Vec<Queued>, entry: PathBuf, record: ReleaseRecord| {
        // An entry handed over while the listing found it too is queued once.
        if queue.iter().any(|queued| queued.entry == entry) {
            return;
        }
        queue.push(Queued {
            entry,
            pending: Pending {
                seq,
                unique_bytes: record.unique_bytes,
                extents: record.extents,
            },
        });
        seq += 1;
    };
    let leftovers = cowshed_core::storage::apfs::deleter::leftovers(store);
    for left in &leftovers.left {
        cowshed_os::eprintln!("cowshed: {left}");
    }
    // With no record, the oldest leftover first.
    let mut leftovers: Vec<(Option<std::time::SystemTime>, PathBuf)> = leftovers
        .entries
        .into_iter()
        .map(|entry| {
            (
                std::fs::symlink_metadata(&entry)
                    .and_then(|at| at.modified())
                    .ok(),
                entry,
            )
        })
        .collect();
    leftovers.sort();
    for (_, entry) in leftovers {
        push(&mut queue, entry, ReleaseRecord::default());
    }
    loop {
        while let Ok((entry, record)) = handed.try_recv() {
            push(&mut queue, entry, record);
        }
        let pending: Vec<Pending> = queue.iter().map(|queued| queued.pending).collect();
        let Some(position) = cleanup_order::next(&pending, pressure(store, &mut said_space)) else {
            // Nothing to free: wait for the next hand-over, or end with the gateway.
            match handed.recv() {
                Ok((entry, record)) => {
                    push(&mut queue, entry, record);
                    continue;
                }
                Err(_) => return,
            }
        };
        let Queued { entry, pending } = queue.swap_remove(position);
        match free_committed(&entry) {
            Ok(Released::Freed { bytes, steps }) => cowshed_os::eprintln!(
                "cowshed: freed released image {} ({bytes} bytes, {steps} cuts)",
                entry.display()
            ),
            Ok(Released::Absent) => {}
            Ok(Released::Busy) => {
                cowshed_os::eprintln!(
                    "cowshed: released image {} is held by another process (an earlier deleter, or a gateway still cutting it); it is queued again once that lets go",
                    entry.display()
                );
                requeue_when_free(
                    entry,
                    ReleaseRecord {
                        unique_bytes: pending.unique_bytes,
                        extents: pending.extents,
                    },
                    again.clone(),
                );
            }
            Err(error) => cowshed_os::eprintln!(
                "cowshed: released image {} stays in .deleting for the gateway's next start: {error}",
                entry.display()
            ),
        }
    }
}

/// Queue `entry` again once the process holding its lock lets go: a thread of its own waits in
/// `flock(2)` and hands the entry back, so nothing polls and the cleanup frees others meanwhile.
/// An entry gone by then is found absent.
fn requeue_when_free(
    entry: PathBuf,
    record: ReleaseRecord,
    again: mpsc::Sender<(PathBuf, ReleaseRecord)>,
) {
    let named = entry.clone();
    let waiting = std::thread::Builder::new()
        .name("image-cleanup-held".to_owned())
        .spawn(move || {
            if let Ok(file) = std::fs::File::open(&entry) {
                let _ = file.lock_shared();
            }
            let _ = again.send((entry, record));
        });
    if let Err(error) = waiting {
        cowshed_os::eprintln!(
            "cowshed: released image {} waits in .deleting for the gateway's next start: no thread waits for its holder: {error}",
            named.display()
        );
    }
}

/// Whether `entry`'s directory, every link in its path resolved, lies in the store at `store`:
/// the path is opened through its directories, so a link among them must not lead out.
fn really_in(store: &Path, entry: &Path) -> bool {
    let (Some(directory), Ok(store)) = (entry.parent(), std::fs::canonicalize(store)) else {
        return false;
    };
    std::fs::canonicalize(directory).is_ok_and(|directory| directory.starts_with(&store))
}

/// Whether the store's container is below its free-space floor, [`PRESSURE_FREE_FRACTION`] of its
/// capacity. A store whose space cannot be read is under none: the queue then keeps each cut short,
/// which is said once.
fn pressure(store: &Path, said: &mut bool) -> Pressure {
    match free_and_capacity(store) {
        Ok((free, capacity)) if free < capacity / PRESSURE_FREE_FRACTION => Pressure::Below,
        Ok(_) => Pressure::None,
        Err(_) if *said => Pressure::None,
        Err(error) => {
            *said = true;
            cowshed_os::eprintln!(
                "cowshed: the free space of {} cannot be read, so the released-image cleanup frees the cheapest cut first: {error}",
                store.display()
            );
            Pressure::None
        }
    }
}

/// The container's free bytes available to this user and its capacity, from `statvfs(3)`.
// `fsblkcnt_t` is 32 bits on macOS and 64 on Linux: the conversion widens on one, not the other.
#[allow(clippy::useless_conversion)]
fn free_and_capacity(store: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt as _;
    let path =
        std::ffi::CString::new(store.as_os_str().as_bytes()).map_err(std::io::Error::other)?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stat` is written whole on success.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: statvfs succeeded, so it filled `stat`.
    let stat = unsafe { stat.assume_init() };
    let fragment = stat.f_frsize;
    Ok((
        u64::from(stat.f_bavail).saturating_mul(fragment),
        u64::from(stat.f_blocks).saturating_mul(fragment),
    ))
}

/// Whether `entry` may be handed to the cleanup of the store at `store`: an absolute path below
/// the store, in a `.deleting` directory, naming a file there.
pub(crate) fn admissible(store: &Path, entry: &Path) -> bool {
    entry.is_absolute()
        && entry.starts_with(store)
        && entry
            .components()
            .all(|component| !matches!(component, std::path::Component::ParentDir))
        && entry.file_name().is_some()
        && entry
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|directory| {
                directory == cowshed_core::storage::apfs::release::DELETING_DIRECTORY
            })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only an absolute entry below the store, in a `.deleting` directory, is taken.
    #[test]
    fn only_an_entry_in_a_deleting_directory_of_the_store_is_taken() {
        let store = Path::new("/s/store");
        for (entry, taken) in [
            ("/s/store/acme/widget/build/.deleting/a.dmg", true),
            ("/s/store/.deleting/a.dmg", true),
            ("/s/store/acme/widget/build/a.dmg", false),
            ("/s/other/acme/widget/build/.deleting/a.dmg", false),
            ("s/store/.deleting/a.dmg", false),
            ("/s/store/.deleting/../main.dmg", false),
            ("/s/store/.deleting", false),
        ] {
            assert_eq!(admissible(store, Path::new(entry)), taken, "{entry}");
        }
    }

    /// An entry another process holds goes back to the queue when its lock is let go, not before:
    /// an earlier deleter still cutting it, or a gateway still in its last cut.
    #[test]
    fn a_held_entry_is_queued_again_once_its_holder_lets_go() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-cleanup-held-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        ));
        std::fs::create_dir_all(&root).expect("root");
        let entry = root.join("0123.dmg");
        std::fs::write(&entry, b"released").expect("entry");
        let holder = std::fs::File::open(&entry).expect("the holder's descriptor");
        holder.lock().expect("the holder takes the entry");
        let (again, queue) = mpsc::channel();
        requeue_when_free(entry.clone(), ReleaseRecord::default(), again);
        assert!(queue.try_recv().is_err(), "not queued while it is held");
        drop(holder);
        assert_eq!(queue.recv().expect("queued again").0, entry);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    /// The pressure test reads the store's real container: free space is at most its capacity.
    #[test]
    fn the_free_space_read_is_within_the_capacity() {
        let (free, capacity) = free_and_capacity(&std::env::temp_dir()).expect("statvfs");
        assert!(capacity > 0 && free <= capacity, "{free} of {capacity}");
    }
}
