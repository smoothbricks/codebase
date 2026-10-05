//! Old build state a migration moved out of a tool's path, deleted after the link took it.
//!
//! A contributed directory is renamed into [`DISCARD_DIRECTORY`] (inside the checkout's own
//! `.cowshed/`, so on the same volume, in cowshed's namespace, and never in the source tree or
//! its status), then deleted. The delete can be large, 83 GiB of Cargo target directory on one
//! host, so nothing that hands a checkout to a job waits on it: [`reap`] deletes in the
//! background and the refresh returns once the link is in place. A process that ends before the
//! delete finishes, a crash included, leaves the rest pending; the next refresh of that checkout
//! reaps it again and `gc` [`finish`]es it.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex};

/// Where a checkout's moved-aside build state waits to be deleted, relative to the checkout.
pub const DISCARD_DIRECTORY: &str = ".cowshed/discard";

/// Move the checkout-relative `relative` out of the tree into the checkout's discard directory,
/// in one `rename(2)`. The name keeps the path it came from, readable in a listing.
pub fn move_aside(checkout: &Path, relative: &Path) -> io::Result<PathBuf> {
    let directory = discard_directory(checkout, true)?;
    let mut name = relative.to_string_lossy().replace('/', "%");
    name.push_str(&format!(
        "-{}-{}",
        std::process::id(),
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    let moved = directory.join(name);
    fs::rename(checkout.join(relative), &moved)?;
    Ok(moved)
}

/// Every discard of `checkout` not yet deleted.
pub fn pending(checkout: &Path) -> io::Result<Vec<PathBuf>> {
    let directory = discard_directory(checkout, false)?;
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut pending = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()?;
    pending.sort();
    Ok(pending)
}

/// Delete every pending discard of `checkout` now, after any background [`reap`] of it in this
/// process has finished. `report` hears each discard before its delete starts, so a caller that
/// waits can say what it waits on.
pub fn finish(checkout: &Path, mut report: impl FnMut(&Path)) -> io::Result<()> {
    let lock = checkout_lock(checkout);
    let _held = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for discard in pending(checkout)? {
        report(&discard);
        remove(&discard)?;
    }
    Ok(())
}

/// Delete `checkout`'s pending discards on a background thread, unless one in this process
/// already is. Never blocks the caller. The thread says what it deletes and any failure on
/// stderr; a failure leaves the discard pending for the next refresh or `gc`.
pub fn reap(checkout: &Path) {
    match pending(checkout) {
        Ok(pending) if pending.is_empty() => return,
        Ok(_) => {}
        Err(error) => {
            eprintln!(
                "cowshed: cannot list old build state under {}: {error}",
                checkout.join(DISCARD_DIRECTORY).display()
            );
            return;
        }
    }
    let lock = checkout_lock(checkout);
    let reaped = checkout.to_owned();
    let spawned = std::thread::Builder::new()
        .name("cowshed-discard".to_owned())
        .spawn(move || {
            let checkout = reaped;
            // Another reaper of this checkout is deleting; it takes what this one would.
            let Ok(_held) = lock.try_lock() else {
                return;
            };
            loop {
                let pending = match pending(&checkout) {
                    Ok(pending) => pending,
                    Err(error) => {
                        eprintln!(
                            "cowshed: cannot list old build state under {}: {error}",
                            checkout.join(DISCARD_DIRECTORY).display()
                        );
                        return;
                    }
                };
                if pending.is_empty() {
                    return;
                }
                for discard in pending {
                    eprintln!(
                        "cowshed: deleting old build state {} in the background",
                        discard.display()
                    );
                    if let Err(error) = remove(&discard) {
                        eprintln!(
                            "cowshed: cannot delete old build state {}: {error}; the next \
                             refresh or `cowshed gc` retries it",
                            discard.display()
                        );
                        return;
                    }
                }
            }
        });
    if let Err(error) = spawned {
        eprintln!(
            "cowshed: cannot start deleting old build state under {}: {error}; the next refresh \
             or `cowshed gc` retries it",
            checkout.join(DISCARD_DIRECTORY).display()
        );
    }
}

fn remove(discard: &Path) -> io::Result<()> {
    match fs::symlink_metadata(discard) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
        Ok(metadata) if metadata.is_dir() => match fs::remove_dir_all(discard) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            result => result,
        },
        Ok(_) => fs::remove_file(discard),
    }
}

/// The checkout's discard directory, which must be a real directory under a real `.cowshed`: a
/// link planted at either would aim the rename and the delete outside the checkout.
fn discard_directory(checkout: &Path, create: bool) -> io::Result<PathBuf> {
    let mut directory = checkout.to_owned();
    for component in Path::new(DISCARD_DIRECTORY).components() {
        directory.push(component);
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(io::Error::other(format!(
                    "{} is not a real directory",
                    directory.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
                fs::create_dir(&directory)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(directory),
            Err(error) => return Err(error),
        }
    }
    Ok(directory)
}

/// One lock per checkout in this process: a reaper holds it while it deletes.
fn checkout_lock(checkout: &Path) -> Arc<Mutex<()>> {
    static LOCKS: LazyLock<Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>> =
        LazyLock::new(Mutex::default);
    LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(checkout.to_owned())
        .or_default()
        .clone()
}
