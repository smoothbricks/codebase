//! The land target's ref lock (02_workspaces.md "`cowshed land`" step 3): the caller's lock file,
//! which every writer that moves the target's branch outside cowshed holds while it does, taken as
//! the exclusive `flock(2)` lockf(1) takes. A land holds it from re-reading the target head through
//! the fast-forward and for nothing else, so a writer queued on it waits for that fence, never for
//! a land's checks, cache initialization, adoption or retirement.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::api::dto::RefLockPath;
use crate::{CowshedError, Result};

/// How often a land that waits for the ref lock says who holds it.
const WAIT_REPORT: Duration = Duration::from_secs(10);

/// The held ref lock. Dropping it closes the descriptor, which releases the lock.
pub struct RefLock {
    _file: File,
}

impl RefLock {
    /// Take the ref lock at `path`, creating its file if no writer has yet. An uncontended lock is
    /// taken at once; a held one is waited for on a helper thread, and the wait says every
    /// [`WAIT_REPORT`] who holds the file. Nothing bounds the wait: the holder is a writer moving
    /// the same branch, which a land must follow, not overtake.
    pub async fn acquire(path: &RefLockPath) -> Result<Self> {
        let file = open(path.as_path()).map_err(|error| failure(path, "open", &error))?;
        match flock(&file, libc::LOCK_EX | libc::LOCK_NB) {
            Ok(()) => return Ok(Self { _file: file }),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(failure(path, "lock", &error)),
        }
        let (locked, waiter) = tokio::sync::oneshot::channel();
        std::thread::Builder::new()
            .name("cowshed-ref-lock".into())
            .spawn(move || {
                // A land dropped while it waited has dropped `waiter`: the failed send returns the
                // descriptor inside its error, and dropping that releases the lock just taken.
                let _ = locked.send(flock(&file, libc::LOCK_EX).map(|()| file));
            })
            .map_err(|error| failure(path, "start the waiter for", &error))?;
        tokio::pin!(waiter);
        let started = Instant::now();
        let mut report = tokio::time::interval_at(tokio::time::Instant::now(), WAIT_REPORT);
        loop {
            tokio::select! {
                answer = &mut waiter => {
                    let file = answer
                        .map_err(|_| CowshedError::internal(format!(
                            "the waiter for ref lock {} exited without an answer",
                            path.as_str()
                        )))?
                        .map_err(|error| failure(path, "lock", &error))?;
                    return Ok(Self { _file: file });
                }
                _ = report.tick() => eprintln!(
                    "cowshed: land has waited {}s for the ref lock {}: {}",
                    started.elapsed().as_secs(),
                    path.as_str(),
                    holder_text(path.as_path()),
                ),
            }
        }
    }
}

fn open(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o644)
        // A process the land starts while it holds the lock (git) must not inherit it.
        .custom_flags(libc::O_CLOEXEC)
        .open(path)
}

fn flock(file: &File, operation: libc::c_int) -> io::Result<()> {
    loop {
        // SAFETY: `file` owns this descriptor for the whole call; flock does not consume it.
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn holder_text(path: &Path) -> String {
    match crate::build_volume::nx::holders(path) {
        Ok(holders) if holders.is_empty() => {
            "no process has it open now, so its holder just let go".to_owned()
        }
        Ok(holders) => format!("held by {}", crate::build_volume::nx::Holders(&holders)),
        Err(error) => format!("its holders cannot be read: {error}"),
    }
}

fn failure(path: &RefLockPath, operation: &str, error: &io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot {operation} the ref lock {}: {error}", path.as_str()),
        "name a ref lock file this user can create and write, the one the target's other branch writers hold",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lock_path(directory: &crate::temp_root::TempRoot) -> RefLockPath {
        RefLockPath::new(directory.join("ref.lock")).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_land_takes_a_free_ref_lock_and_releases_it_when_dropped() {
        let directory = crate::temp_root::TempRoot::new("ref-lock");
        let path = lock_path(&directory);
        let held = RefLock::acquire(&path).await.unwrap();
        let other = open(path.as_path()).unwrap();
        assert_eq!(
            flock(&other, libc::LOCK_EX | libc::LOCK_NB)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock,
            "a held ref lock admitted a second holder"
        );
        drop(held);
        flock(&other, libc::LOCK_EX | libc::LOCK_NB).expect("a dropped ref lock stayed held");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_land_that_asks_for_a_held_ref_lock_holds_it_once_released() {
        let directory = crate::temp_root::TempRoot::new("ref-lock");
        let path = lock_path(&directory);
        let holder = open(path.as_path()).unwrap();
        flock(&holder, libc::LOCK_EX).unwrap();
        let waiting = tokio::spawn({
            let path = path.clone();
            async move { RefLock::acquire(&path).await }
        });
        drop(holder);
        let held = waiting
            .await
            .unwrap()
            .expect("the waiter never took the released lock");
        let other = open(path.as_path()).unwrap();
        assert_eq!(
            flock(&other, libc::LOCK_EX | libc::LOCK_NB)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(held);
    }
}
